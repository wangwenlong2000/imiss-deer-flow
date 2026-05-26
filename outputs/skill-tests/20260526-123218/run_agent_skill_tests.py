#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_API = "http://localhost:3230/api/langgraph"
ASSISTANT_ID = "lead_agent"
CAMERA_ID = "CAM_DEERFLOW_001"
TRAFFIC_VIDEO = "/mnt/datasets/Vedio-demo/Trafic.mp4"
FIGHT_VIDEO = "/mnt/datasets/Vedio-demo/fight.mp4"
CONFIG = "/mnt/skills/custom/configs/deerflow_config.json"


def json_dump(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def text_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def post_json(url: str, payload: dict[str, Any], timeout: int = 30) -> tuple[int, str]:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")


def docker_status() -> str:
    cmd = ["docker", "ps", "--format", "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    lines = result.stdout.splitlines()
    keep = [line for line in lines if "NAMES" in line or "huangxiao-deer-flow" in line or "deer-flow-sandbox" in line]
    return "\n".join(keep) + ("\n" if keep else "")


def sse_events(response) -> list[dict[str, str]]:
    events: list[dict[str, str]] = []
    current = {"event": "message", "data": ""}
    for raw in response:
        line = raw.decode("utf-8", errors="replace").rstrip("\n")
        if not line:
            if current["data"]:
                events.append(current)
            current = {"event": "message", "data": ""}
            continue
        if line.startswith("event:"):
            current["event"] = line.split(":", 1)[1].strip()
        elif line.startswith("data:"):
            data = line.split(":", 1)[1].lstrip()
            current["data"] = data if not current["data"] else current["data"] + "\n" + data
    if current["data"]:
        events.append(current)
    return events


def walk_json(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk_json(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk_json(child)


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
        if isinstance(payload, dict):
            messages = payload.get("messages", [])
        elif isinstance(payload, list):
            messages = payload
        else:
            messages = []
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


def prompt_for(skill: str) -> str:
    out = f"/mnt/user-data/outputs/{skill}"
    ws = f"/mnt/user-data/workspace/{skill}"
    skill_path = f"/mnt/skills/custom/{skill}"
    command_by_skill = {
        "video-stream-ingestion": f"""mkdir -p {out}
python {skill_path}/scripts/run.py --video {TRAFFIC_VIDEO} --camera-id {CAMERA_ID} --source-type local_file --capture-seconds 10 --config {CONFIG} --output {out}/result.json""",
        "frame-sampling": f"""mkdir -p {out}
python {skill_path}/scripts/run.py --video {TRAFFIC_VIDEO} --camera-id {CAMERA_ID} --capture-seconds 10 --interval-seconds 1 --output-dir {out}/artifacts --config {CONFIG} --output {out}/result.json""",
        "camera-health-check": f"""mkdir -p {out} {ws}
python /mnt/skills/custom/frame-sampling/scripts/run.py --video {TRAFFIC_VIDEO} --camera-id {CAMERA_ID} --capture-seconds 5 --interval-seconds 1 --output-dir {ws}/frames --config {CONFIG} --output {ws}/frames.json
python {skill_path}/scripts/run.py --frames-json {ws}/frames.json --camera-id {CAMERA_ID} --config {CONFIG} --output {out}/result.json""",
        "object-detection": f"""mkdir -p {out} {ws}
python /mnt/skills/custom/frame-sampling/scripts/run.py --video {TRAFFIC_VIDEO} --camera-id {CAMERA_ID} --capture-seconds 2 --interval-seconds 1 --output-dir {ws}/frames --config {CONFIG} --output {ws}/frames.json
python {skill_path}/scripts/run.py --frames-json {ws}/frames.json --labels person,car,bus,truck,motorcycle,bicycle --provider ultralytics --config {CONFIG} --output {out}/result.json""",
        "object-tracking": f"""mkdir -p {out} {ws}
python - <<'PY'
import json, pathlib
detections = {{"detections":[
  {{"frame_id":"frame_001","timestamp":"2026-05-20T10:00:00+08:00","objects":[{{"object_id":"det_001","label":"person","confidence":0.91,"bbox":[100,100,180,260]}}]}},
  {{"frame_id":"frame_002","timestamp":"2026-05-20T10:00:02+08:00","objects":[{{"object_id":"det_002","label":"person","confidence":0.89,"bbox":[120,105,200,265]}}]}}
]}}
p = pathlib.Path('{ws}/detections.json')
p.parent.mkdir(parents=True, exist_ok=True)
json.dump(detections, p.open('w'), ensure_ascii=False, indent=2)
PY
python {skill_path}/scripts/run.py --detections-json {ws}/detections.json --camera-id {CAMERA_ID} --config {CONFIG} --output {out}/result.json""",
        "roi-mapping": f"""mkdir -p {out} {ws}
python - <<'PY'
import json, pathlib
tracks = {{"tracks":[{{"track_id":"track_0001","camera_id":"{CAMERA_ID}","label":"person","last_bbox":[100,100,180,260],"confidence":0.9,"evidence_frame_ids":["frame_001"]}}]}}
p = pathlib.Path('{ws}/tracks.json')
p.parent.mkdir(parents=True, exist_ok=True)
json.dump(tracks, p.open('w'), ensure_ascii=False, indent=2)
PY
python {skill_path}/scripts/run.py --tracks-json {ws}/tracks.json --camera-id {CAMERA_ID} --position-strategy bbox_overlap --config {CONFIG} --output {out}/result.json""",
        "analyze-video": f"""mkdir -p {out}/artifacts
python {skill_path}/scripts/extract_frames.py {TRAFFIC_VIDEO} --output-dir {out}/artifacts/video_review --coarse-fps 0.2 --dense-fps 1 --scene-threshold 0.3 --dense-window 1 --max-frames 12 > {out}/result.json""",
        "single-video-event-analysis": f"""mkdir -p {out} {ws}
python - <<'PY'
import json, pathlib
payload = {{"source_type":"local_file","video_path":"{FIGHT_VIDEO}","camera_id":"{CAMERA_ID}","target_events":["fight","fall_down","other_abnormal_event"],"output_dir":"{out}/artifacts/review","coarse_fps":0.2,"dense_fps":1.0,"scene_threshold":0.3,"dense_window":1.0,"max_frames":12}}
p = pathlib.Path('{ws}/input.json')
p.parent.mkdir(parents=True, exist_ok=True)
json.dump(payload, p.open('w'), ensure_ascii=False, indent=2)
PY
python {skill_path}/scripts/run.py --input {ws}/input.json --config {CONFIG} --output {out}/result.json""",
        "evidence-snapshot": f"""mkdir -p {out} {ws}
python /mnt/skills/custom/frame-sampling/scripts/run.py --video {TRAFFIC_VIDEO} --camera-id {CAMERA_ID} --capture-seconds 2 --interval-seconds 1 --output-dir {ws}/frames --config {CONFIG} --output {ws}/frames.json
python - <<'PY'
import json, pathlib
event = {{"event_id":"EVT_TEST_001","event_type":"traffic_congestion","camera_id":"{CAMERA_ID}","confidence":0.8,"evidence_frame_ids":["CAM_DEERFLOW_001_000001"]}}
p = pathlib.Path('{ws}/event.json')
p.parent.mkdir(parents=True, exist_ok=True)
json.dump(event, p.open('w'), ensure_ascii=False, indent=2)
PY
python {skill_path}/scripts/run.py --event-json {ws}/event.json --frames-json {ws}/frames.json --output-dir {out}/artifacts --config {CONFIG} --output {out}/result.json""",
        "video-segment-extraction": f"""mkdir -p {out}
python {skill_path}/scripts/run.py --event-id EVT_TEST_SEGMENT_001 --raw-segment-uri {TRAFFIC_VIDEO} --event-time 2026-05-20T10:00:05+08:00 --event-elapsed-seconds 5 --pre-seconds 2 --post-seconds 3 --output-dir {out}/artifacts --config {CONFIG} --output {out}/result.json""",
        "privacy-masking": f"""mkdir -p {out} {ws}
python /mnt/skills/custom/ffmpeg-utils/scripts/run.py --operation keyframe --input-path {TRAFFIC_VIDEO} --timestamp 1 --output-path {ws}/source.jpg --output {ws}/keyframe_result.json
python - <<'PY'
import json, pathlib
regions = {{"sensitive_regions":[{{"type":"face","bbox":[20,20,180,160]}}]}}
p = pathlib.Path('{ws}/regions.json')
p.parent.mkdir(parents=True, exist_ok=True)
json.dump(regions, p.open('w'), ensure_ascii=False, indent=2)
PY
python {skill_path}/scripts/run.py --image-uri {ws}/source.jpg --sensitive-regions-json {ws}/regions.json --method gaussian_blur --config {CONFIG} --output {out}/result.json""",
        "human-review-routing": f"""mkdir -p {out} {ws}
python - <<'PY'
import json, pathlib
event = {{"event_id":"EVT_REVIEW_001","event_type":"fight","confidence":0.62,"severity":"high","enforcement_impact":True}}
health = {{"health_status":"degraded","health_score":0.55,"issues":["blurred"]}}
pathlib.Path('{ws}').mkdir(parents=True, exist_ok=True)
json.dump(event, open('{ws}/event.json','w'), ensure_ascii=False, indent=2)
json.dump(health, open('{ws}/health.json','w'), ensure_ascii=False, indent=2)
PY
python {skill_path}/scripts/run.py --event-json {ws}/event.json --camera-health-json {ws}/health.json --config {CONFIG} --output {out}/result.json""",
        "duplicate-event-merge": f"""mkdir -p {out} {ws}
python - <<'PY'
import json, pathlib
events = {{"events":[
  {{"event_id":"EVT_DUP_001","event_type":"traffic_congestion","camera_id":"{CAMERA_ID}","start_time":"2026-05-20T10:00:01+08:00","end_time":"2026-05-20T10:00:06+08:00","confidence":0.75,"roi_id":"ROI_FULL_FRAME","evidence_frame_ids":["frame_001"]}},
  {{"event_id":"EVT_DUP_002","event_type":"traffic_congestion","camera_id":"{CAMERA_ID}","start_time":"2026-05-20T10:00:03+08:00","end_time":"2026-05-20T10:00:08+08:00","confidence":0.8,"roi_id":"ROI_FULL_FRAME","evidence_frame_ids":["frame_002"]}}
]}}
p = pathlib.Path('{ws}/events.json')
p.parent.mkdir(parents=True, exist_ok=True)
json.dump(events, p.open('w'), ensure_ascii=False, indent=2)
PY
python {skill_path}/scripts/run.py --events-json {ws}/events.json --output {out}/result.json""",
        "ffmpeg-utils": f"""mkdir -p {out}/artifacts
python {skill_path}/scripts/run.py --operation segment --input-path {TRAFFIC_VIDEO} --start-time 1 --duration 2 --output-path {out}/artifacts/segment.mp4 --output {out}/segment_result.json
python {skill_path}/scripts/run.py --operation keyframe --input-path {TRAFFIC_VIDEO} --timestamp 1 --output-path {out}/artifacts/keyframe.jpg --output {out}/keyframe_result.json
python - <<'PY'
import json
segment = json.load(open('{out}/segment_result.json'))
keyframe = json.load(open('{out}/keyframe_result.json'))
json.dump({{"skill":"ffmpeg-utils","version":"1.0.0","status":"success" if segment.get("status") == "success" and keyframe.get("status") == "success" else "failed","data":{{"segment":segment,"keyframe":keyframe}}}}, open('{out}/result.json','w'), ensure_ascii=False, indent=2)
PY""",
    }
    command = command_by_skill[skill]
    return f"""请测试 `{skill}` skill 的 Agent 真链路。

必须严格执行：
1. 先调用 read_file 读取 `{skill_path}/SKILL.md`。
2. 再调用 bash 执行下面这段命令，不要改路径，不要省略任何输出文件。
3. 执行完成后读取 `{out}/result.json`，并在最终回复中返回这个 JSON 的内容和简短结论。

```bash
{command}
```

成功判定：必须能看到 read_file 读取该 SKILL.md、bash 执行该 skill 入口、并生成 `{out}/result.json`。"""


SKILLS = [
    "video-stream-ingestion",
    "frame-sampling",
    "camera-health-check",
    "object-detection",
    "object-tracking",
    "roi-mapping",
    "analyze-video",
    "single-video-event-analysis",
    "evidence-snapshot",
    "video-segment-extraction",
    "privacy-masking",
    "human-review-routing",
    "duplicate-event-merge",
    "ffmpeg-utils",
]


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


def validate(skill: str, skill_dir: Path, tool_calls: list[dict[str, Any]]) -> dict[str, Any]:
    outputs_result = skill_dir / "artifacts" / "outputs" / skill / "result.json"
    local_result = skill_dir / "result.json"
    result_path = outputs_result if outputs_result.exists() else local_result
    result: dict[str, Any] | None = None
    if result_path.exists():
        try:
            result = json.loads(result_path.read_text(encoding="utf-8"))
            json_dump(local_result, result)
        except Exception as exc:
            result = {"status": "failed", "message": f"result JSON parse failed: {exc}"}
    read_ok = any(c.get("name") == "read_file" and f"/mnt/skills/custom/{skill}/SKILL.md" in json.dumps(c.get("args", {}), ensure_ascii=False) for c in tool_calls)
    bash_ok = any(c.get("name") == "bash" and f"/mnt/skills/custom/{skill}" in json.dumps(c.get("args", {}), ensure_ascii=False) for c in tool_calls)
    status_ok = bool(result and result.get("status") == "success")
    if skill == "single-video-event-analysis" and result:
        status_ok = isinstance(result.get("video"), dict) and isinstance(result.get("visual_timeline"), list) and isinstance(result.get("events"), list)
    return {
        "result_path": str(result_path) if result_path.exists() else None,
        "read_skill_md": read_ok,
        "bash_entrypoint": bash_ok,
        "status_success": status_ok,
        "passed": read_ok and bash_ok and status_ok,
        "result_status": (result.get("status") or ("analysis_json" if status_ok else None)) if result else None,
        "result_error": result.get("error_code") or result.get("message") if result else "missing result.json",
    }


def run_one(api: str, run_root: Path, skill: str, timeout: int) -> dict[str, Any]:
    skill_dir = run_root / skill
    skill_dir.mkdir(parents=True, exist_ok=True)
    (skill_dir / "artifacts").mkdir(exist_ok=True)
    prompt = prompt_for(skill)
    text_write(skill_dir / "prompt.txt", prompt)
    thread_payload = {"metadata": {"purpose": f"{skill}-agent-skill-test", "created_by": "codex"}}
    status, body = post_json(f"{api}/threads", thread_payload)
    text_write(skill_dir / "thread-create-response.json", body)
    if status >= 300:
        summary = {"skill": skill, "passed": False, "error": f"thread create failed: {status}"}
        json_dump(skill_dir / "validation.json", summary)
        return summary
    thread = json.loads(body)
    thread_id = thread["thread_id"]
    json_dump(skill_dir / "thread.json", thread)
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
    events: list[dict[str, str]]
    raw_sse = ""
    error = None
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            raw_sse = raw.decode("utf-8", errors="replace")
    except Exception as exc:
        error = repr(exc)
    raw_sse = raw_sse.replace("\r\n", "\n").replace("\r", "\n")
    text_write(skill_dir / "raw_sse.log", raw_sse)
    events = []
    for block in raw_sse.split("\n\n"):
        event = "message"
        data_lines = []
        for line in block.splitlines():
            line = line.strip()
            if line.startswith("event:"):
                event = line.split(":", 1)[1].strip()
            elif line.startswith("data:"):
                data_lines.append(line.split(":", 1)[1].lstrip())
        if data_lines:
            events.append({"event": event, "data": "\n".join(data_lines)})
    tool_calls = parse_tool_calls(events)
    json_dump(skill_dir / "tool_calls.json", tool_calls)
    text_write(skill_dir / "agent_final.md", final_agent_text(events) or "")
    collect_thread_artifacts(thread_id, skill_dir / "artifacts")
    validation = validate(skill, skill_dir, tool_calls)
    validation.update({"skill": skill, "thread_id": thread_id, "run_id": extract_run_id(events), "stream_error": error, "sse_errors": error_events(events)})
    json_dump(skill_dir / "validation.json", validation)
    readme = f"""# {skill}

- thread_id: `{thread_id}`
- run_id: `{validation.get("run_id")}`
- passed: `{validation["passed"]}`
- read SKILL.md: `{validation["read_skill_md"]}`
- bash entrypoint: `{validation["bash_entrypoint"]}`
- result status: `{validation["result_status"]}`
- result path: `{validation["result_path"]}`
- stream error: `{error}`
- sse errors: `{validation["sse_errors"]}`
"""
    text_write(skill_dir / "README.md", readme)
    return validation


def write_summary(run_root: Path, results: list[dict[str, Any]], started_at: str, finished_at: str) -> None:
    passed = sum(1 for r in results if r.get("passed"))
    lines = [
        "# Video Monitoring Skills Agent Chain Test Summary",
        "",
        f"- started_at: `{started_at}`",
        f"- finished_at: `{finished_at}`",
        f"- total: `{len(results)}`",
        f"- passed: `{passed}`",
        f"- failed: `{len(results) - passed}`",
        "",
        "## Docker Services",
        "",
        "```text",
        (run_root / "docker-before.txt").read_text(encoding="utf-8") if (run_root / "docker-before.txt").exists() else "",
        "```",
        "",
        "## Results",
        "",
        "| Skill | Pass | Thread | Run | Notes |",
        "| --- | --- | --- | --- | --- |",
    ]
    for r in results:
        note = "ok" if r.get("passed") else (r.get("result_error") or r.get("stream_error") or "failed")
        lines.append(f"| `{r['skill']}` | `{r.get('passed')}` | `{r.get('thread_id')}` | `{r.get('run_id')}` | {note} |")
    text_write(run_root / "SUMMARY.md", "\n".join(lines) + "\n")
    json_dump(run_root / "manifest.json", {"started_at": started_at, "finished_at": finished_at, "results": results})


def main() -> int:
    parser = argparse.ArgumentParser(description="Run DeerFlow Agent true-chain tests for custom video monitoring skills.")
    parser.add_argument("--api", default=DEFAULT_API)
    parser.add_argument("--root", default=str(REPO_ROOT / "outputs" / "skill-tests"))
    parser.add_argument("--skills", nargs="*", default=SKILLS)
    parser.add_argument("--timeout", type=int, default=420)
    args = parser.parse_args()
    started = dt.datetime.now().astimezone().strftime("%Y-%m-%dT%H:%M:%S%z")
    run_id = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    run_root = Path(args.root) / run_id
    run_root.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(Path(__file__), run_root / "run_agent_skill_tests.py")
    text_write(run_root / "docker-before.txt", docker_status())
    results = []
    for index, skill in enumerate(args.skills, start=1):
        print(f"[{index}/{len(args.skills)}] {skill}", flush=True)
        result = run_one(args.api.rstrip("/"), run_root, skill, args.timeout)
        results.append(result)
        write_summary(run_root, results, started, dt.datetime.now().astimezone().strftime("%Y-%m-%dT%H:%M:%S%z"))
        time.sleep(1)
    text_write(run_root / "docker-after.txt", docker_status())
    finished = dt.datetime.now().astimezone().strftime("%Y-%m-%dT%H:%M:%S%z")
    write_summary(run_root, results, started, finished)
    print(run_root)
    return 0 if all(r.get("passed") for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
