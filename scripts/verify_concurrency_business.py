"""Opt-in real business check: three new conversations, real model/tools/files.

Calls the configured model through the existing website APIs. Raw SSE/state stay
in a private local directory; only synthetic CSVs are suitable
for a review artifact. Existing conversations and configuration are untouched.
"""

import argparse
import asyncio
import csv
import gzip
import hashlib
import io
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx


def sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def final_table_correct(messages: list[dict], expected: dict) -> bool:
    final = next((message for message in reversed(messages) if message.get("type") == "ai" and not message.get("tool_calls")), {})
    text = final.get("content", "")
    if isinstance(text, list):
        text = "\n".join(part.get("text", "") for part in text if isinstance(part, dict))
    actual = {}
    for line in text.splitlines():
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) == 4 and cells[0] in expected:
            try:
                actual[cells[0]] = {key: float(value.replace(",", "")) for key, value in zip(["sum_flow", "mean_flow", "mean_speed_kmh"], cells[1:])}
            except ValueError:
                return False
    return actual == expected


def make_case(index: int) -> dict:
    marker = "ABC"[index]
    scale = index + 1
    rows = [
        ["2026-10-26 08:00:00", f"样例{marker}主路", 100 * scale, 30 + index],
        ["2026-10-26 08:05:00", f"样例{marker}主路", 300 * scale, 10 + index],
        ["2026-10-26 08:00:00", f"样例{marker}支路", 200 * scale, 40 + index],
        ["2026-10-26 08:05:00", f"样例{marker}支路", 200 * scale, 20 + index],
    ]
    text = io.StringIO()
    writer = csv.writer(text)
    writer.writerow(["timestamp", "road", "flow", "speed_kmh"])
    writer.writerows(rows)
    expected = {
        f"样例{marker}主路": {"sum_flow": 400 * scale, "mean_flow": 200 * scale, "mean_speed_kmh": 20 + index},
        f"样例{marker}支路": {"sum_flow": 400 * scale, "mean_flow": 200 * scale, "mean_speed_kmh": 30 + index},
    }
    query = (
        "请分析本会话上传的合成道路交通观测 same.csv。按road分组计算flow总和、flow算术平均值、speed_kmh算术平均值。"
        "将完整汇总写入 /mnt/user-data/outputs/report.csv，列名依次为road,sum_flow,mean_flow,mean_speed_kmh。"
        "最终回答简要列出每条道路的三个数值，并给出该CSV的下载链接。"
        "只使用本会话上传的数据和会话工作区；不联网、不读取平台其他数据、不安装依赖、不修改技能。"
    )
    return {"case": marker, "csv": text.getvalue(), "query": query, "expected": expected}


async def verify(base_url: str, output: Path, deadline: float) -> dict:
    output.mkdir(parents=True, exist_ok=True, mode=0o700)
    output.chmod(0o700)
    cases = [make_case(index) for index in range(3)]
    group = f"concurrency-business-{uuid4().hex[:12]}"
    manifest = {"group": group, "created_at": datetime.now(UTC).isoformat(), "cases": cases}
    (output / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2))
    records = [{"case": case["case"], "input_sha256": sha256(case["csv"].encode()), "expected": case["expected"]} for case in cases]
    samples = []
    collecting = True

    async with httpx.AsyncClient(base_url=base_url, timeout=httpx.Timeout(30, read=deadline)) as client:
        response = await client.get("/api/models")
        response.raise_for_status()
        model = response.json()["models"][0]["name"]

        async def prepare(case, record):
            response = await client.post("/api/langgraph/threads", json={"metadata": {"source": "concurrency_verification", "test_group": group, "test_case": case["case"]}})
            response.raise_for_status()
            thread_id = response.json()["thread_id"]
            record["thread_id"] = thread_id
            response = await client.post(f"/api/threads/{thread_id}/uploads", files={"files": ("same.csv", case["csv"].encode(), "text/csv")})
            response.raise_for_status()
            data = response.json()
            if not data.get("success"):
                raise RuntimeError("Upload was not accepted")
            file = data["files"][0]
            record["upload_succeeded"] = True
            file["path"] = file["virtual_path"]
            file["status"] = "uploaded"
            return {
                "assistant_id": "lead_agent",
                "input": {"messages": [{"type": "human", "id": str(uuid4()), "content": [{"type": "text", "text": case["query"]}], "additional_kwargs": {"files": [file]}}]},
                "context": {"thread_id": thread_id, "model_name": model, "thinking_enabled": True, "is_plan_mode": False, "subagent_enabled": False},
                "config": {"recursion_limit": 1000},
                "stream_mode": ["updates", "values", "messages-tuple", "custom"],
                "stream_resumable": True,
            }

        payloads = await asyncio.gather(*(prepare(case, record) for case, record in zip(cases, records)))
        # Freeze the exact inputs before starting any graph run.
        (output / "requests.json").write_text(json.dumps(payloads, ensure_ascii=False, indent=2))

        async def collect_status():
            while collecting:
                sample = {"time": time.monotonic(), "statuses": {}}
                for record in records:
                    if record.get("run_id"):
                        try:
                            response = await client.get(f"/api/langgraph/threads/{record['thread_id']}/runs/{record['run_id']}")
                            if response.is_success:
                                sample["statuses"][record["case"]] = response.json()["status"]
                        except httpx.HTTPError:
                            pass
                if sample["statuses"]:
                    samples.append(sample)
                await asyncio.sleep(0.25)

        async def execute(payload, record):
            thread_id = record["thread_id"]
            record["submitted_at"] = time.monotonic()
            events = 0
            try:
                async with asyncio.timeout(deadline):
                    async with client.stream("POST", f"/api/langgraph/threads/{thread_id}/runs/stream", json=payload) as stream:
                        record["http_status"] = stream.status_code
                        stream.raise_for_status()
                        location = stream.headers.get("content-location", "")
                        if "/runs/" in location:
                            record["run_id"] = location.rsplit("/", 1)[-1]
                        with gzip.open(output / f"{record['case']}.sse.gz", "wt") as raw:
                            async for line in stream.aiter_lines():
                                raw.write(line + "\n")
                                if line.startswith("event: "):
                                    events += 1
                response = await client.get(f"/api/langgraph/threads/{thread_id}/state")
                response.raise_for_status()
                state = response.json()
                (output / f"{record['case']}.state.json").write_text(json.dumps(state, ensure_ascii=False))
                record["next_nodes"] = state.get("next")
                record["scene"] = state.get("values", {}).get("intent_context", {}).get("scene")
                record["answer_correct"] = final_table_correct(state.get("values", {}).get("messages", []), record["expected"])
                if record.get("run_id"):
                    response = await client.get(f"/api/langgraph/threads/{thread_id}/runs/{record['run_id']}")
                    response.raise_for_status()
                    run = response.json()
                    record["run_status"] = run["status"]
                    record["run_created_at"] = run.get("created_at")
                    record["run_updated_at"] = run.get("updated_at")
                response = await client.get(f"/api/threads/{thread_id}/artifacts/mnt/user-data/outputs/report.csv", params={"download": "true"})
                record["download_http_status"] = response.status_code
                if response.is_success:
                    content = response.content
                    (output / f"{record['case']}.report.csv").write_bytes(content)
                    rows = list(csv.DictReader(io.StringIO(content.decode("utf-8-sig"))))
                    actual = {row["road"]: {key: float(row[key]) for key in ["sum_flow", "mean_flow", "mean_speed_kmh"]} for row in rows}
                    record["actual"] = actual
                    record["csv_sha256"] = sha256(content)
                    record["csv_correct"] = len(rows) == 2 and actual == record["expected"]
                record["passed"] = record.get("run_status") == "success" and not record["next_nodes"] and record.get("csv_correct", False) and record["answer_correct"]
            except Exception as exc:
                record["exception_type"] = type(exc).__name__
                record["passed"] = False
                if isinstance(exc, TimeoutError) and record.get("run_id"):
                    response = await client.post(f"/api/langgraph/threads/{thread_id}/runs/{record['run_id']}/cancel", params={"wait": "true"})
                    record["timeout_cancel_http_status"] = response.status_code
            finally:
                record["finished_at"] = time.monotonic()
                record["elapsed_seconds"] = round(record["finished_at"] - record["submitted_at"], 3)
                record["stream_event_count"] = events
                print(json.dumps({key: record.get(key) for key in ["case", "run_status", "passed", "elapsed_seconds", "exception_type"]}), flush=True)

        monitor = asyncio.create_task(collect_status())
        try:
            await asyncio.gather(*(execute(payload, record) for payload, record in zip(payloads, records)))
        finally:
            collecting = False
            await monitor
        summary = {
            "group": group, "scope": "real website APIs, configured model, original lead_agent, actual tools and sandbox",
            "model": model, "cases": records,
            "observed_two_running": any(list(s["statuses"].values()).count("running") == 2 for s in samples),
            "observed_third_queued_while_two_running": any(list(s["statuses"].values()).count("running") == 2 and list(s["statuses"].values()).count("pending") == 1 for s in samples),
            "observed_max_running": max((list(s["statuses"].values()).count("running") for s in samples), default=0),
            "all_business_cases_passed": all(record.get("passed") for record in records),
        }
        (output / "status-samples.json").write_text(json.dumps(samples, ensure_ascii=False, indent=2))
        (output / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
        print(json.dumps({key: value for key, value in summary.items() if key != "cases"}, ensure_ascii=False), flush=True)
        return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:3328")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--deadline", type=float, default=240)
    args = parser.parse_args()
    summary = asyncio.run(verify(args.base_url, args.output, args.deadline))
    raise SystemExit(0 if summary["all_business_cases_passed"] and summary["observed_third_queued_while_two_running"] else 1)
