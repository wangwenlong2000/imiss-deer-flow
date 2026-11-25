#!/usr/bin/env python3
"""Measure real OneCity model/tool workloads using disposable test identities."""

from __future__ import annotations

import argparse
import csv
import hashlib
import http.client
import io
import json
import math
import re
import statistics
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from uuid import uuid4

import verify_onecity_production as smoke


def native_run(args, account, thread_id, run_id, *, cancel=False):
    """Inspect or cancel only a run whose IDs were returned to this test user."""
    if not thread_id or not run_id:
        return {"verified": False}
    program = """
import json,sys,uuid,requests
data=json.load(sys.stdin)
thread=str(uuid.UUID(data['thread_id'])); run=str(uuid.UUID(data['run_id']))
url='http://'+data['graph_container']+':3329/threads/'+thread+'/runs/'+run
session=requests.Session(); session.trust_env=False
headers={'Authorization':'Bearer '+data['token']}
if data['cancel']:
    response=session.post(url+'/cancel',headers=headers,params={'wait':'true'},timeout=30)
    result={'cancel_http_status':response.status_code}
else:
    response=session.get(url,headers=headers,timeout=15)
    result={'http_status':response.status_code}
    if response.status_code==200:
        record=response.json()
        result.update({key:record.get(key) for key in ['status','created_at','updated_at']})
        if record.get('status') not in {'pending','running'}:
            state=session.get('http://'+data['graph_container']+':3329/threads/'+thread+'/state',headers=headers,timeout=15)
            if state.status_code==200:
                values=state.json().get('values') or {}
                intent=values.get('intent_context') or {}
                result['routed_scene']=intent.get('scene')
                result['routed_scenes']=intent.get('scenes')
                result['tool_names']=sorted({call.get('name') for message in values.get('messages',[]) for call in message.get('tool_calls',[]) if call.get('name')})
print(json.dumps(result))
"""
    process = subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            args.container,
            "/app/.venv/bin/python",
            "-c",
            program,
        ],
        input=json.dumps(
            {
                "thread_id": thread_id,
                "run_id": run_id,
                "token": account["token"],
                "cancel": cancel,
                "graph_container": args.graph_container,
            }
        ),
        text=True,
        capture_output=True,
        timeout=40,
    )
    if process.returncode:
        return {"verified": False, "error_type": "native_probe_failed"}
    return json.loads(process.stdout)


def make_case(wave, index, *, oversize=False):
    """Create independent synthetic inputs with exact, reproducible answers."""
    sizes = [16, 4096, 16000]
    if oversize:
        sizes[-1] = 32768
    count = sizes[wave % len(sizes)]
    if index % 4 == 0:
        return {
            "scene": "question",
            "query": "某社区有12个小组，每组12人，请计算总人数，用一句中文回答。",
            "expected": "144",
        }
    if index % 4 == 3:
        text = "本材料为虚构政策演练。开放时间为每周18小时，试行期为36天。\n"
        text += (
            "服务说明：本项目为市民提供公开咨询，使用汇总数据，不涉及个人信息。\n"
            * [4, 128, 512][wave % 3]
        )
        return {
            "scene": "policy_document",
            "filename": "policy.txt",
            "content": text.encode(),
            "query": "请阅读本会话上传的虚构政策材料policy.txt，仅回答每周开放小时数和试行天数，给出材料依据。无需检索外部政策。",
            "expected": ["18", "36"],
        }
    scene = "road_traffic" if index % 4 == 1 else "statistics_table"
    table = io.StringIO()
    writer = csv.writer(table)
    writer.writerow(["group", "value"])
    totals = {"sampleA": 0, "sampleB": 0}
    for row in range(count):
        group = "sampleA" if row % 2 == 0 else "sampleB"
        value = row % 10 + 1
        totals[group] += value
        writer.writerow([group, value])
    label = "道路交通流量" if scene == "road_traffic" else "城市统计观测"
    query = (
        f"请分析本会话上传的合成{label}数据same.csv。仅使用这份文件，调用本地Python或bash读取全部行，"
        "按group分组计算value总和，写入/mnt/user-data/outputs/report.csv，列名为group,total。"
        "最终回答给出汇总和report.csv下载链接。不联网、不安装依赖、不读取其他数据。"
    )
    return {
        "scene": scene,
        "filename": "same.csv",
        "content": table.getvalue().encode(),
        "query": query,
        "expected": totals,
        "rows": count,
    }


def quantiles(values):
    if not values:
        return {"count": 0}
    ordered = sorted(values)
    return {
        "count": len(values),
        "min": round(min(values), 3),
        "mean": round(statistics.mean(values), 3),
        "p50": round(ordered[(len(ordered) - 1) // 2], 3),
        "p95": round(ordered[math.ceil(len(ordered) * 0.95) - 1], 3),
        "max": round(max(values), 3),
    }


def resource_monitor(stop, records, args):
    service_names = [args.container, args.graph_container, args.gateway_container]
    while not stop.is_set():
        try:
            names = list(service_names)
            if args.sandbox_prefix:
                sandboxes = subprocess.run(
                    [
                        "docker",
                        "ps",
                        "--filter",
                        "name=" + args.sandbox_prefix,
                        "--format",
                        "{{.Names}}",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                names.extend(
                    name
                    for name in sandboxes.stdout.splitlines()
                    if name.startswith(args.sandbox_prefix)
                )
            process = subprocess.run(
                ["docker", "stats", "--no-stream", "--format", "{{json .}}", *names],
                capture_output=True,
                text=True,
                timeout=10,
            )
            for line in process.stdout.splitlines():
                data = json.loads(line)
                records.append(
                    {
                        "elapsed": round(time.monotonic(), 3),
                        **{
                            key: data.get(key)
                            for key in [
                                "Name",
                                "CPUPerc",
                                "MemUsage",
                                "MemPerc",
                                "NetIO",
                                "BlockIO",
                                "PIDs",
                            ]
                        },
                    }
                )
        except (OSError, ValueError, subprocess.TimeoutExpired):
            records.append({"sampling_error": True})
        try:
            gpu = subprocess.run(
                [
                    "nvidia-smi",
                    "--query-gpu=index,utilization.gpu,memory.used,memory.total",
                    "--format=csv,noheader,nounits",
                ],
                capture_output=True,
                text=True,
                timeout=5,
            )
            for line in gpu.stdout.splitlines():
                index, utilization, used, total = [
                    float(value.strip()) for value in line.split(",")
                ]
                records.append(
                    {
                        "elapsed": round(time.monotonic(), 3),
                        "gpu": int(index),
                        "utilization_percent": utilization,
                        "memory_used_mib": used,
                        "memory_total_mib": total,
                    }
                )
        except (OSError, ValueError, subprocess.TimeoutExpired):
            pass
        stop.wait(3)


def run_case(args, case, account, label):
    started = time.monotonic()
    dialog = "citybrain-" + uuid4().hex[:22]
    record = {
        "case": label,
        "scene": case["scene"],
        "account": account["test_label"],
        "dialog_id": dialog,
        "input_bytes": len(case.get("content", b"")),
        "rows": case.get("rows"),
        "submitted_clock": started,
        "statuses": [],
        "errors": [],
        "passed": False,
    }
    thread_id = None
    try:
        response = smoke.request(
            args,
            "/query/deerflow/dialog",
            token=account["token"],
            method="POST",
            data={
                "conversation_id": dialog,
                "name": "Synthetic load validation " + label,
            },
        )
        if not smoke.success(response):
            raise smoke.SmokeFailure("dialog_" + str(response["status"]))
        attachments = []
        if case.get("content"):
            boundary = "onecityload" + uuid4().hex
            body = (
                f'--{boundary}\r\nContent-Disposition: form-data; name="files"; filename="{case["filename"]}"\r\nContent-Type: application/octet-stream\r\n\r\n'.encode()
                + case["content"]
                + f"\r\n--{boundary}--\r\n".encode()
            )
            upload_start = time.monotonic()
            uploaded = smoke.request(
                args,
                "/query/deerflow/upload?" + urlencode({"conversation_id": dialog}),
                token=account["token"],
                method="POST",
                body=body,
                content_type="multipart/form-data; boundary=" + boundary,
            )
            record["upload_seconds"] = round(time.monotonic() - upload_start, 3)
            record["upload_status"] = uploaded["status"]
            data = (uploaded.get("json") or {}).get("data") or {}
            attachments = data.get("files") or []
            thread_id = data.get("thread_id")
            if not smoke.success(uploaded) or not attachments:
                record["upload_rejected"] = True
                raise smoke.SmokeFailure("upload_rejected")
        parsed = urlsplit(args.onecity_url)
        connection = http.client.HTTPConnection(
            parsed.hostname, parsed.port or 80, timeout=args.deadline
        )
        query_start = time.monotonic()
        record["query_clock"] = query_start
        query = {
            "conversation_id": dialog,
            "query": case["query"],
            "attachments": json.dumps(attachments, ensure_ascii=False),
        }
        final = []
        finished = False
        try:
            connection.request(
                "GET",
                "/query/deerflow?" + urlencode(query),
                headers={"Authorization": "Bearer " + account["token"]},
            )
            stream = connection.getresponse()
            record["query_http_status"] = stream.status
            record["headers_seconds"] = round(time.monotonic() - query_start, 3)
            if stream.status != 200:
                raise smoke.SmokeFailure("query_" + str(stream.status))
            while True:
                if time.monotonic() - query_start > args.deadline:
                    raise TimeoutError("deadline")
                line = stream.readline(2 * 1024 * 1024)
                if not line:
                    break
                if not line.startswith(b"data:"):
                    continue
                event = json.loads(line[5:].strip())
                elapsed = time.monotonic() - query_start
                record.setdefault("first_event_seconds", round(elapsed, 3))
                metadata = event.get("metadata") or {}
                thread_id = (
                    metadata.get("thread_id") or event.get("thread_id") or thread_id
                )
                record["run_id"] = metadata.get("run_id") or record.get("run_id")
                source = metadata.get("source_event")
                if source in {"status", "heartbeat", "accepted", "start"}:
                    status = event.get("status")
                    if status and (
                        not record["statuses"]
                        or record["statuses"][-1]["status"] != status
                    ):
                        record["statuses"].append(
                            {"status": status, "seconds": round(elapsed, 3)}
                        )
                    if status == "running":
                        record.setdefault("queue_seconds", round(elapsed, 3))
                if source in {
                    "task_planning",
                    "final_answer",
                    "intent",
                    "todo_routing_guidance",
                }:
                    record.setdefault("first_answer_seconds", round(elapsed, 3))
                if source in {"final_answer", "execution_failure"}:
                    final.append(str(event.get("content", "")))
                if event.get("type") == "error":
                    record["errors"].append(
                        {
                            "code": event.get("code") or event.get("http_status"),
                            "status": event.get("status"),
                        }
                    )
                if event.get("type") == "compliance":
                    record["compliance_events"] = record.get("compliance_events", 0) + 1
                if event.get("type") == "[DONE]":
                    finished = event.get("finished") is True
                    record["done_finished"] = finished
                    break
        finally:
            connection.close()
        text = "\n".join(final)
        smoke.write_private(
            args.output / (label + "-answer.json"),
            {"content": text, "expected": case["expected"]},
        )
        record["final_answer_sha256"] = hashlib.sha256(text.encode()).hexdigest()
        record["final_answer_chars"] = len(text)
        record["model_finished_seconds"] = round(time.monotonic() - query_start, 3)
        if isinstance(case["expected"], dict):
            downloaded = smoke.request(
                args,
                "/query/deerflow/file/download?"
                + urlencode(
                    {
                        "conversation_id": dialog,
                        "url": f"/api/threads/{thread_id}/artifacts/mnt/user-data/outputs/report.csv",
                    }
                ),
                token=account["token"],
            )
            record["download_http_status"] = downloaded["status"]
            if downloaded["status"] == 200:
                rows = list(
                    csv.DictReader(io.StringIO(downloaded["body"].decode("utf-8-sig")))
                )
                record["actual"] = {row["group"]: float(row["total"]) for row in rows}
                record["answer_correct"] = (
                    len(rows) == len(case["expected"])
                    and record["actual"] == case["expected"]
                )
        else:
            values = (
                case["expected"]
                if isinstance(case["expected"], list)
                else [case["expected"]]
            )
            record["answer_correct"] = all(
                re.search(r"(?<!\d)" + re.escape(value) + r"(?!\d)", text)
                for value in values
            )
        record["delivery_complete"] = bool(text.strip()) and (
            not isinstance(case["expected"], dict) or "report.csv" in text
        )
        record["passed"] = (
            finished
            and not record["errors"]
            and record.get("answer_correct", False)
            and record["delivery_complete"]
        )
        settling = time.monotonic()
        record["native_run"] = native_run(
            args, account, thread_id, record.get("run_id")
        )
        while (
            record["native_run"].get("status") in {"pending", "running"}
            and time.monotonic() - settling < 30
        ):
            time.sleep(0.25)
            record["native_run"] = native_run(
                args, account, thread_id, record.get("run_id")
            )
        record["terminal_settle_seconds"] = round(time.monotonic() - settling, 3)
        native = record["native_run"]
        if native.get("created_at") and native.get("updated_at"):
            record["native_lifetime_seconds"] = round(
                (
                    datetime.fromisoformat(native["updated_at"])
                    - datetime.fromisoformat(native["created_at"])
                ).total_seconds(),
                3,
            )
        if record["native_run"].get("status") != "success":
            record["passed"] = False
    except Exception as exc:
        record["exception_type"] = (
            str(exc) if isinstance(exc, smoke.SmokeFailure) else type(exc).__name__
        )
        if record.get("run_id"):
            try:
                record["recovery"] = native_run(
                    args, account, thread_id, record["run_id"], cancel=True
                )
                record["native_run"] = native_run(
                    args, account, thread_id, record["run_id"]
                )
            except (OSError, ValueError, subprocess.TimeoutExpired):
                record["recovery"] = {"verified": False}
    record["thread_id"] = thread_id
    record["finished_clock"] = time.monotonic()
    record["total_seconds"] = round(record["finished_clock"] - started, 3)
    print(
        json.dumps(
            {
                key: record.get(key)
                for key in [
                    "case",
                    "scene",
                    "input_bytes",
                    "passed",
                    "total_seconds",
                    "exception_type",
                ]
            }
        ),
        flush=True,
    )
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--onecity-url", default="http://127.0.0.1:8086")
    parser.add_argument("--gateway-url", default="http://onecity-deerflow-gateway:3330")
    parser.add_argument("--container", default="onecityagent-a-dev")
    parser.add_argument("--graph-container", default="onecity-deerflow-langgraph")
    parser.add_argument("--gateway-container", default="onecity-deerflow-gateway")
    parser.add_argument(
        "--sandbox-prefix",
        default="",
        help="Collect resource samples for this test's own sandboxes.",
    )
    parser.add_argument("--waves", type=int, default=3)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--deadline", type=float, default=600)
    parser.add_argument(
        "--include-oversize",
        action="store_true",
        help="Include files exceeding the complete-scan bound to verify rejection.",
    )
    args = parser.parse_args()
    target = urlsplit(args.onecity_url)
    if (
        target.scheme != "http"
        or target.hostname not in {"127.0.0.1", "localhost"}
        or target.path not in {"", "/"}
        or target.query
        or target.fragment
        or target.username
    ):
        parser.error("OneCity target must be a loopback HTTP endpoint")
    if any(
        not re.fullmatch(r"[A-Za-z0-9_.-]+", name)
        for name in [
            args.container,
            args.graph_container,
            args.gateway_container,
            *([args.sandbox_prefix] if args.sandbox_prefix else []),
        ]
    ):
        parser.error("invalid container name")
    states = [smoke.read_private(path) for path in args.state]
    accounts = []
    for state, path in zip(states, args.state):
        for account in state["accounts"]:
            login = smoke.request(
                args,
                "/v1/user/login",
                method="POST",
                data={
                    "email": account["email"],
                    "password": account["encrypted_password"],
                },
            )
            token = next(
                (
                    value
                    for key, value in login["headers"].items()
                    if key.lower() == "authorization"
                ),
                None,
            )
            if not smoke.success(login) or not token:
                raise smoke.SmokeFailure("synthetic_login_failed")
            account["token"] = token
            account["test_label"] = "account-" + str(len(accounts) + 1)
            accounts.append(account)
        smoke.write_private(path, state)
    if not 1 <= args.concurrency <= len(accounts) * 2 or not 1 <= args.waves <= 20:
        parser.error(
            "concurrency must fit two tasks per disposable account; waves must be 1..20"
        )
    args.output.mkdir(parents=True, exist_ok=True, mode=0o700)
    args.output.chmod(0o700)
    stop = threading.Event()
    resources = []
    monitor = threading.Thread(
        target=resource_monitor, args=(stop, resources, args), daemon=True
    )
    monitor.start()
    records = []
    started = time.monotonic()
    try:
        for wave in range(args.waves):
            with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
                futures = [
                    pool.submit(
                        run_case,
                        args,
                        make_case(wave, index, oversize=args.include_oversize),
                        accounts[index % len(accounts)],
                        f"w{wave + 1}-c{index + 1}",
                    )
                    for index in range(args.concurrency)
                ]
                records.extend(future.result() for future in futures)
            smoke.write_private(
                args.output / "progress.json",
                {"cases": records, "resources": resources},
            )
    finally:
        stop.set()
        monitor.join(timeout=15)
        for state, path in zip(states, args.state):
            for account in state["accounts"]:
                dialogs = set(account.get("validation_dialog_ids", []))
                dialogs.update(
                    row["dialog_id"]
                    for row in records
                    if row["account"] == account["test_label"]
                )
                account["validation_dialog_ids"] = sorted(dialogs)
            smoke.write_private(path, state)
    rejected = [
        r
        for r in records
        if r.get("query_http_status") == 429
        or any(error.get("code") == 429 for error in r["errors"])
    ]
    accepted = [
        r for r in records if r.get("query_http_status") == 200 and r not in rejected
    ]
    summary = {
        "scope": "real registered OneCity users, configured business model, original skills/tools and sandbox",
        "waves": args.waves,
        "concurrency": args.concurrency,
        "account_count": len(accounts),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "case_count": len(records),
        "passed": sum(r["passed"] for r in records),
        "success_rate": sum(r["passed"] for r in records) / len(records),
        "accepted_tasks": len(accepted),
        "accepted_task_success_rate": sum(r["passed"] for r in accepted) / len(accepted)
        if accepted
        else 0,
        "upload_rejections": sum(bool(r.get("upload_rejected")) for r in records),
        "capacity_rejections": len(rejected),
        "timings": {
            key: quantiles(
                [r[key] for r in records if isinstance(r.get(key), (int, float))]
            )
            for key in [
                "queue_seconds",
                "first_event_seconds",
                "first_answer_seconds",
                "model_finished_seconds",
                "total_seconds",
                "upload_seconds",
                "native_lifetime_seconds",
                "terminal_settle_seconds",
            ]
        },
        "cases": records,
        "resources": resources,
    }
    smoke.write_private(args.output / "summary.json", summary)
    print(
        json.dumps(
            {
                key: value
                for key, value in summary.items()
                if key not in ["cases", "resources"]
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    return 0 if all(r["passed"] for r in records) else 1


if __name__ == "__main__":
    raise SystemExit(main())
