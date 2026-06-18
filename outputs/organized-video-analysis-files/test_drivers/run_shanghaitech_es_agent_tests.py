#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import shutil
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_API = "http://localhost:3230/api/langgraph"
ASSISTANT_ID = "lead_agent"

SOURCE_SAMPLE = Path("/home/huangxiao/City_brain/shanghaitech/shanghaitech/training/videos/07_007.avi")
DATASET_SAMPLE = REPO_ROOT / "datasets" / "shanghaitech-agent-eval" / "07_007.avi"
SANDBOX_VIDEO = "/mnt/datasets/shanghaitech-agent-eval/07_007.avi"
ORIGINAL_SAMPLE_LABEL = "/home/huangxiao/City_brain/shanghaitech/shanghaitech/training/videos/07_007.avi"

CONFIG = "/mnt/skills/custom/configs/deerflow_config.json"
SOURCE_INDEX = "huangxiao-shanghaitech-es-agent-test"
VECTOR_INDEX = "huangxiao-shanghaitech-vector-agent-test"
CAMERA_ID = "CAM_SHANGHAITECH_07_007"
VIDEO_ID = "shanghaitech-07-007-agent-test"


def json_dump(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def text_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def ensure_sample() -> dict[str, Any]:
    if not SOURCE_SAMPLE.is_file():
        raise FileNotFoundError(f"ShanghaiTech sample not found: {SOURCE_SAMPLE}")
    DATASET_SAMPLE.parent.mkdir(parents=True, exist_ok=True)
    if not DATASET_SAMPLE.exists() or sha256(DATASET_SAMPLE) != sha256(SOURCE_SAMPLE):
        shutil.copy2(SOURCE_SAMPLE, DATASET_SAMPLE)
    return {
        "source_sample": str(SOURCE_SAMPLE),
        "dataset_sample": str(DATASET_SAMPLE),
        "sandbox_video": SANDBOX_VIDEO,
        "source_sha256": sha256(SOURCE_SAMPLE),
        "dataset_sha256": sha256(DATASET_SAMPLE),
        "bytes": DATASET_SAMPLE.stat().st_size,
    }


def post_json(url: str, payload: dict[str, Any], timeout: int = 30) -> tuple[int, str]:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")


def walk_json(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk_json(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk_json(child)


def parse_events(raw_sse: str) -> list[dict[str, str]]:
    events: list[dict[str, str]] = []
    for block in raw_sse.replace("\r\n", "\n").replace("\r", "\n").split("\n\n"):
        event = "message"
        data_lines: list[str] = []
        for line in block.splitlines():
            line = line.strip()
            if line.startswith("event:"):
                event = line.split(":", 1)[1].strip()
            elif line.startswith("data:"):
                data_lines.append(line.split(":", 1)[1].lstrip())
        if data_lines:
            events.append({"event": event, "data": "\n".join(data_lines)})
    return events


def parse_tool_calls(events: list[dict[str, str]]) -> list[dict[str, Any]]:
    calls: dict[str, dict[str, Any]] = {}
    for ev in events:
        try:
            payload = json.loads(ev["data"])
        except Exception:
            continue
        for blob in walk_json(payload):
            for call in blob.get("tool_calls", []) or []:
                if not isinstance(call, dict):
                    continue
                call_id = call.get("id") or f"anonymous-{len(calls)}"
                item = calls.setdefault(call_id, {"id": call_id, "name": "", "args": {}})
                if call.get("name"):
                    item["name"] = call["name"]
                if isinstance(call.get("args"), dict):
                    item["args"] = call["args"]
    return list(calls.values())


def final_agent_text(events: list[dict[str, str]]) -> str:
    latest = ""
    for ev in events:
        if ev["event"] not in {"values", "messages"}:
            continue
        try:
            payload = json.loads(ev["data"])
        except Exception:
            continue
        messages = payload.get("messages", []) if isinstance(payload, dict) else payload if isinstance(payload, list) else []
        for msg in messages:
            if isinstance(msg, dict) and msg.get("type") in {"ai", "AIMessageChunk"}:
                content = msg.get("content")
                if isinstance(content, str) and content.strip():
                    latest = content
    return latest


def extract_run_id(events: list[dict[str, str]]) -> str | None:
    for ev in events:
        if ev["event"] != "metadata":
            continue
        try:
            return json.loads(ev["data"]).get("run_id")
        except Exception:
            return None
    for ev in events:
        try:
            payload = json.loads(ev["data"])
        except Exception:
            continue
        for blob in walk_json(payload):
            run_id = blob.get("run_id")
            if run_id:
                return run_id
    return None


def error_events(events: list[dict[str, str]]) -> list[dict[str, Any]]:
    errors = []
    for ev in events:
        if ev["event"] != "error":
            continue
        try:
            errors.append(json.loads(ev["data"]))
        except Exception:
            errors.append({"raw": ev["data"]})
    return errors


def collect_thread_artifacts(thread_id: str, dest: Path) -> None:
    source = REPO_ROOT / "backend" / ".deer-flow" / "threads" / thread_id / "user-data"
    if not source.exists():
        return
    for name in ["outputs", "workspace"]:
        src = source / name
        if src.exists():
            dst = dest / name
            if dst.exists():
                shutil.rmtree(dst)
            shutil.copytree(src, dst)


def manifest_writer_command(ws: str) -> str:
    manifest = {
        "videos": [
            {
                "video_id": VIDEO_ID,
                "camera_id": CAMERA_ID,
                "file_path": SANDBOX_VIDEO,
                "source_type": "local_file",
                "capture_seconds": 2,
                "interval_seconds": 1,
                "analysis_mode": "object_detection",
                "labels": ["person", "bicycle"],
                "location": {
                    "city": "上海市",
                    "district": "浦东新区",
                    "address": "ShanghaiTech campus surveillance dataset sample",
                },
                "metadata": {
                    "filename": "07_007.avi",
                    "dataset": "ShanghaiTech",
                    "split": "training",
                    "scene": "campus_pedestrian_surveillance",
                    "camera_name": "ShanghaiTech 07 scene camera",
                    "original_host_path": ORIGINAL_SAMPLE_LABEL,
                    "description": "ShanghaiTech真实样例07_007，校园行人监控短视频，用于ES skill Agent真链路测试",
                },
                "tags": ["ShanghaiTech", "campus", "pedestrian", "surveillance", "agent-es-skill-test"],
                "description": "ShanghaiTech campus pedestrian surveillance sample 07_007 from the real dataset.",
            }
        ]
    }
    return "\n".join(
        [
            "python - <<'PY'",
            "import json, pathlib",
            f"manifest = {json.dumps(manifest, ensure_ascii=False)}",
            f"p = pathlib.Path({json.dumps(ws + '/shanghaitech_manifest.json')})",
            "p.parent.mkdir(parents=True, exist_ok=True)",
            "json.dump(manifest, p.open('w'), ensure_ascii=False, indent=2)",
            "PY",
        ]
    )


def event_writer_command(ws: str) -> str:
    event = {
        "event_id": "EVT_SHANGHAITECH_07_007_AGENT_TEST",
        "video_id": VIDEO_ID,
        "camera_id": CAMERA_ID,
        "event_type": "manual_evidence",
        "event_time": "2026-06-02T00:00:01+08:00",
        "event_elapsed_seconds": 1,
        "confidence": 0.8,
        "description": "Evidence package smoke event for ShanghaiTech sample 07_007.",
    }
    return "\n".join(
        [
            "python - <<'PY'",
            "import json, pathlib",
            f"event = {json.dumps(event, ensure_ascii=False)}",
            f"p = pathlib.Path({json.dumps(ws + '/event.json')})",
            "p.parent.mkdir(parents=True, exist_ok=True)",
            "json.dump(event, p.open('w'), ensure_ascii=False, indent=2)",
            "PY",
        ]
    )


def test_cases() -> list[dict[str, str]]:
    cases: list[dict[str, str]] = []

    def add(case_id: str, skill: str, command: str, note: str) -> None:
        cases.append({"case_id": case_id, "skill": skill, "command": command, "note": note})

    out = "/mnt/user-data/outputs/batch-video-ingestion-shanghaitech"
    ws = "/mnt/user-data/workspace/batch-video-ingestion-shanghaitech"
    add(
        "batch-video-ingestion-shanghaitech",
        "batch-video-ingestion",
        f"""mkdir -p {out} {ws}
{manifest_writer_command(ws)}
python /mnt/skills/custom/batch-video-ingestion/scripts/run.py --manifest-json {ws}/shanghaitech_manifest.json --index {SOURCE_INDEX} --analysis-mode object_detection --config {CONFIG} --refresh --output {out}/result.json""",
        "Ingest the real ShanghaiTech 07_007 sample into a personal ES test index.",
    )

    out = "/mnt/user-data/outputs/video-search-shanghaitech"
    add(
        "video-search-shanghaitech",
        "video-search",
        f"""mkdir -p {out}
python /mnt/skills/custom/video-search/scripts/run.py --query ShanghaiTech --camera-id {CAMERA_ID} --top-k 5 --index {SOURCE_INDEX} --config {CONFIG} --output {out}/result.json""",
        "Search the indexed ShanghaiTech sample by metadata and camera filter.",
    )

    out = "/mnt/user-data/outputs/object-statistics-shanghaitech"
    add(
        "object-statistics-shanghaitech",
        "object-statistics",
        f"""mkdir -p {out}
python /mnt/skills/custom/object-statistics/scripts/run.py --video-id {VIDEO_ID} --index {SOURCE_INDEX} --config {CONFIG} --output {out}/result.json""",
        "Aggregate object statistics for the ingested ShanghaiTech video document.",
    )

    out = "/mnt/user-data/outputs/evidence-package-generation-shanghaitech"
    ws = "/mnt/user-data/workspace/evidence-package-generation-shanghaitech"
    add(
        "evidence-package-generation-shanghaitech",
        "evidence-package-generation",
        f"""mkdir -p {out} {ws}
{event_writer_command(ws)}
python /mnt/skills/custom/evidence-package-generation/scripts/run.py --video-id {VIDEO_ID} --event-json {ws}/event.json --output-dir {out}/package --index {SOURCE_INDEX} --config {CONFIG} --output {out}/result.json""",
        "Build an evidence package by retrieving the ShanghaiTech video from ES.",
    )

    out = "/mnt/user-data/outputs/video-embedding-index-deterministic-shanghaitech"
    add(
        "video-embedding-index-deterministic-shanghaitech",
        "video-embedding-index",
        f"""mkdir -p {out}
python /mnt/skills/custom/video-embedding-index/scripts/run.py --source-index {SOURCE_INDEX} --target-index {VECTOR_INDEX} --owner huangxiao --video-id {VIDEO_ID} --limit 1 --embedding-provider deterministic-hash --dimensions 1024 --config {CONFIG} --output {out}/result.json""",
        "Create a deterministic vector copy of the ShanghaiTech ES document.",
    )

    out = "/mnt/user-data/outputs/video-embedding-index-streetmodel-query"
    add(
        "video-embedding-index-streetmodel-query",
        "video-embedding-index",
        f"""mkdir -p {out}
python /mnt/skills/custom/video-embedding-index/scripts/run.py --embedding-provider streetmodel --query 上海科技大学校园监控中有人在路面行走 --query-vector-output {out}/query_vector.json --config {CONFIG} --output {out}/result.json""",
        "Call the StreetModel/Qwen3-VL text embedding path used for semantic video retrieval.",
    )

    out = "/mnt/user-data/outputs/video-embedding-index-streetmodel-video"
    add(
        "video-embedding-index-streetmodel-video",
        "video-embedding-index",
        f"""mkdir -p {out}
python /mnt/skills/custom/video-embedding-index/scripts/run.py --embedding-provider streetmodel --video-id {VIDEO_ID}-streetmodel --video-uri {SANDBOX_VIDEO} --copy-video-to-shared --target-index {VECTOR_INDEX} --owner huangxiao --timeout-seconds 120 --config {CONFIG} --output {out}/result.json""",
        "Attempt a real video embedding call for the ShanghaiTech sample; record path/model failures.",
    )

    out = "/mnt/user-data/outputs/video-embedding-index-streetmodel-video-workspace-prefix"
    add(
        "video-embedding-index-streetmodel-video-workspace-prefix",
        "video-embedding-index",
        f"""mkdir -p {out} /mnt/user-data/workspace/streetmodel_shared
python /mnt/skills/custom/video-embedding-index/scripts/run.py --embedding-provider streetmodel --video-id {VIDEO_ID}-streetmodel-workspace --video-uri {SANDBOX_VIDEO} --copy-video-to-shared --deerflow-path-prefix /mnt/user-data/workspace/streetmodel_shared --streetmodel-path-prefix /nfsdat2/home/xhuangslm/shared_videos/codex_unmounted_workspace --target-index {VECTOR_INDEX} --owner huangxiao --timeout-seconds 120 --config {CONFIG} --output {out}/result.json""",
        "Force the video path to a writable local prefix so the skill reaches StreetModel /embed with a GPU-side path that is expected to be inaccessible.",
    )
    return cases


def prompt_for(case: dict[str, str]) -> str:
    skill = case["skill"]
    case_id = case["case_id"]
    out = f"/mnt/user-data/outputs/{case_id}"
    return f"""请测试 `{skill}` skill 的 Agent 真链路，测试用例：`{case_id}`。

测试说明：{case["note"]}

必须严格执行：
1. 先调用 read_file 读取 `/mnt/skills/custom/{skill}/SKILL.md`。
2. 再调用 bash 执行下面这段命令，不要改路径，不要省略任何输出文件。
3. 执行完成后读取 `{out}/result.json`，并在最终回复中返回这个 JSON 的核心字段和简短结论；如果失败，保留 error_code/message/detail。

```bash
{case["command"]}
```

成功判定：必须能看到 read_file 读取该 SKILL.md、bash 执行该 skill 入口、并生成 `{out}/result.json`。"""


def validate(case: dict[str, str], case_dir: Path, tool_calls: list[dict[str, Any]]) -> dict[str, Any]:
    case_id = case["case_id"]
    skill = case["skill"]
    outputs_result = case_dir / "artifacts" / "outputs" / case_id / "result.json"
    local_result = case_dir / "result.json"
    result_path = outputs_result if outputs_result.exists() else local_result
    result: dict[str, Any] | None = None
    if result_path.exists():
        try:
            result = json.loads(result_path.read_text(encoding="utf-8"))
            json_dump(local_result, result)
        except Exception as exc:
            result = {"status": "failed", "message": f"result JSON parse failed: {exc}"}
    args_blob = [json.dumps(c.get("args", {}), ensure_ascii=False) for c in tool_calls]
    read_ok = any(c.get("name") == "read_file" and f"/mnt/skills/custom/{skill}/SKILL.md" in blob for c, blob in zip(tool_calls, args_blob))
    bash_ok = any(c.get("name") == "bash" and f"/mnt/skills/custom/{skill}" in blob for c, blob in zip(tool_calls, args_blob))
    result_exists = result_path.exists()
    status = result.get("status") if result else None
    data = result.get("data") if isinstance(result, dict) and isinstance(result.get("data"), dict) else {}
    return {
        "case_id": case_id,
        "skill": skill,
        "result_path": str(result_path) if result_exists else None,
        "read_skill_md": read_ok,
        "bash_entrypoint": bash_ok,
        "result_exists": result_exists,
        "agent_chain_passed": bool(read_ok and bash_ok and result_exists),
        "result_status": status,
        "error_code": result.get("error_code") if result else "missing result.json",
        "message": result.get("message") if result else "missing result.json",
        "data_summary": summarize_data(data),
    }


def summarize_data(data: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for key in [
        "ingested_count",
        "failed_count",
        "total",
        "query_mode",
        "index",
        "matched_videos",
        "total_objects",
        "total_tracks",
        "package_id",
        "manifest_uri",
        "source_index",
        "target_index",
        "embedded_count",
        "embedding_provider",
        "embedding_model",
        "dimensions",
        "vector_field",
    ]:
        if key in data:
            summary[key] = data[key]
    if isinstance(data.get("documents"), list):
        summary["documents"] = data["documents"][:3]
    if isinstance(data.get("failures"), list):
        summary["failures"] = data["failures"][:3]
    if isinstance(data.get("hits"), list):
        summary["hits"] = data["hits"][:3]
    if isinstance(data.get("by_label"), dict):
        summary["by_label"] = data["by_label"]
    if isinstance(data.get("query_embedding"), dict):
        summary["query_embedding"] = data["query_embedding"]
    if isinstance(data.get("evidence"), list):
        summary["evidence"] = data["evidence"][:2]
    return summary


def run_one(api: str, run_root: Path, case: dict[str, str], timeout: int) -> dict[str, Any]:
    case_dir = run_root / case["case_id"]
    case_dir.mkdir(parents=True, exist_ok=True)
    (case_dir / "artifacts").mkdir(exist_ok=True)
    prompt = prompt_for(case)
    text_write(case_dir / "prompt.txt", prompt)
    status, body = post_json(f"{api}/threads", {"metadata": {"purpose": f"{case['case_id']}-agent-test", "created_by": "codex"}})
    text_write(case_dir / "thread-create-response.json", body)
    if status >= 300:
        result = {"case_id": case["case_id"], "skill": case["skill"], "agent_chain_passed": False, "error": f"thread create failed: {status}", "body": body}
        json_dump(case_dir / "validation.json", result)
        return result
    thread = json.loads(body)
    thread_id = thread["thread_id"]
    json_dump(case_dir / "thread.json", thread)
    run_payload = {
        "assistant_id": ASSISTANT_ID,
        "input": {"messages": [{"role": "user", "content": prompt}]},
        "context": {"thread_id": thread_id},
        "config": {"recursion_limit": 80},
        "stream_mode": ["values"],
    }
    req = urllib.request.Request(
        f"{api}/threads/{thread_id}/runs/stream",
        data=json.dumps(run_payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    raw_sse = ""
    stream_error = None
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw_sse = resp.read().decode("utf-8", errors="replace")
    except Exception as exc:
        stream_error = repr(exc)
    text_write(case_dir / "raw_sse.log", raw_sse)
    events = parse_events(raw_sse)
    tool_calls = parse_tool_calls(events)
    json_dump(case_dir / "tool_calls.json", tool_calls)
    text_write(case_dir / "agent_final.md", final_agent_text(events) or "")
    collect_thread_artifacts(thread_id, case_dir / "artifacts")
    validation = validate(case, case_dir, tool_calls)
    validation.update(
        {
            "thread_id": thread_id,
            "run_id": extract_run_id(events),
            "stream_error": stream_error,
            "sse_errors": error_events(events),
        }
    )
    json_dump(case_dir / "validation.json", validation)
    readme = f"""# {case['case_id']}

- skill: `{case['skill']}`
- thread_id: `{thread_id}`
- run_id: `{validation.get('run_id')}`
- agent chain passed: `{validation['agent_chain_passed']}`
- result status: `{validation.get('result_status')}`
- error code: `{validation.get('error_code')}`
- result path: `{validation.get('result_path')}`
- stream error: `{stream_error}`
"""
    text_write(case_dir / "README.md", readme)
    return validation


def write_summary(run_root: Path, sample_info: dict[str, Any], results: list[dict[str, Any]], started_at: str, finished_at: str) -> None:
    chain_passed = sum(1 for r in results if r.get("agent_chain_passed"))
    functional_success = sum(1 for r in results if r.get("result_status") == "success")
    lines = [
        "# ShanghaiTech ES Skill Agent Test Summary",
        "",
        f"- started_at: `{started_at}`",
        f"- finished_at: `{finished_at}`",
        f"- sample_source: `{sample_info['source_sample']}`",
        f"- sandbox_video: `{sample_info['sandbox_video']}`",
        f"- sample_sha256: `{sample_info['source_sha256']}`",
        f"- source_index: `{SOURCE_INDEX}`",
        f"- vector_index: `{VECTOR_INDEX}`",
        f"- agent_chain_passed: `{chain_passed}/{len(results)}`",
        f"- functional_success: `{functional_success}/{len(results)}`",
        "",
        "## Results",
        "",
        "| Case | Skill | Agent Chain | Result | Error | Notes |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for r in results:
        note = json.dumps(r.get("data_summary", {}), ensure_ascii=False)
        if len(note) > 240:
            note = note[:237] + "..."
        lines.append(
            f"| `{r.get('case_id')}` | `{r.get('skill')}` | `{r.get('agent_chain_passed')}` | `{r.get('result_status')}` | `{r.get('error_code') or ''}` | {note} |"
        )
    text_write(run_root / "SUMMARY.md", "\n".join(lines) + "\n")
    json_dump(run_root / "manifest.json", {"started_at": started_at, "finished_at": finished_at, "sample": sample_info, "results": results})


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Agent true-chain ES skill tests with a real ShanghaiTech sample.")
    parser.add_argument("--api", default=DEFAULT_API)
    parser.add_argument("--root", default=str(REPO_ROOT / "outputs" / "skill-tests"))
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--cases", nargs="*", default=[])
    args = parser.parse_args()

    sample_info = ensure_sample()
    started = dt.datetime.now().astimezone().strftime("%Y-%m-%dT%H:%M:%S%z")
    run_id = dt.datetime.now().strftime("%Y%m%d-%H%M%S-shanghaitech-es")
    run_root = Path(args.root) / run_id
    run_root.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(Path(__file__), run_root / "run_shanghaitech_es_agent_tests.py")
    json_dump(run_root / "sample-info.json", sample_info)

    selected = test_cases()
    if args.cases:
        wanted = set(args.cases)
        selected = [case for case in selected if case["case_id"] in wanted or case["skill"] in wanted]
    results = []
    for index, case in enumerate(selected, start=1):
        print(f"[{index}/{len(selected)}] {case['case_id']}", flush=True)
        result = run_one(args.api.rstrip("/"), run_root, case, args.timeout)
        results.append(result)
        write_summary(run_root, sample_info, results, started, dt.datetime.now().astimezone().strftime("%Y-%m-%dT%H:%M:%S%z"))
        time.sleep(1)
    finished = dt.datetime.now().astimezone().strftime("%Y-%m-%dT%H:%M:%S%z")
    write_summary(run_root, sample_info, results, started, finished)
    print(run_root)
    return 0 if all(r.get("agent_chain_passed") for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
