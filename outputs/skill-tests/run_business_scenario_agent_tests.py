#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import shutil
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_API = "http://localhost:3230/api/langgraph"
ASSISTANT_ID = "lead_agent"


SCENARIOS: list[dict[str, Any]] = [
    {
        "id": "traffic-police-evidence",
        "prompt": "我是交警队的，需要调取奉化区大成路路口今天上午 8-10 点的监控。",
        "expected_capability": "video_asset_retrieval",
        "expected_scenario": "traffic_police_video_review",
        "expected_chain": ["video-search"],
    },
    {
        "id": "semantic-congestion-search",
        "prompt": "帮我找一段交通拥堵的视频。",
        "expected_capability": "semantic_video_retrieval",
        "expected_scenario": "generic_video_intelligence",
        "expected_chain": ["video-search"],
    },
    {
        "id": "single-video-traffic-accident",
        "prompt": "检测 /mnt/datasets/Vedio-demo/Trafic.mp4 里有没有交通事故。",
        "expected_capability": "single_video_event_understanding",
        "expected_scenario": "generic_video_intelligence",
        "expected_chain": ["single-video-event-analysis"],
    },
    {
        "id": "vehicle-statistics-report",
        "prompt": "统计一下摄像头 CAM_DEERFLOW_001 今天检测到了多少辆车。",
        "expected_capability": "object_statistics",
        "expected_scenario": "generic_statistics_workflow",
        "expected_chain": ["object-statistics"],
    },
    {
        "id": "legal-evidence-package",
        "prompt": "帮我准备一份法庭证据：从 /mnt/datasets/Vedio-demo/Trafic.mp4 的 00:00:01 前后各 2 秒导出事件证据包，包括视频片段和完整性哈希。",
        "expected_capability": "evidence_preservation",
        "expected_scenario": "legal_evidence_package",
        "expected_chain": ["evidence-package-generation"],
    },
    {
        "id": "video-ingestion-embedding",
        "prompt": "把 /mnt/datasets/Vedio-demo/Trafic.mp4 这个监控视频入库到 Elasticsearch，并生成 StreetModel 向量嵌入。",
        "expected_capability": "video_library_governance",
        "expected_scenario": "generic_video_intelligence",
        "expected_chain": ["video-stream-ingestion", "batch-video-ingestion", "video-embedding-index"],
    },
    {
        "id": "camera-health-quality",
        "prompt": "检查摄像头 CAM_DEERFLOW_001 的画面质量怎么样，有没有模糊、遮挡、黑屏或冻结？",
        "expected_capability": "camera_health_operations",
        "expected_scenario": "camera_health_ops",
        "expected_chain": ["frame-sampling", "camera-health-check"],
    },
    {
        "id": "property-security-review",
        "prompt": "物业公司要求查看小区门口昨晚的录像，有没有陌生人进入？",
        "expected_scenario": "property_security_review",
        "expected_chain": ["video-search", "single-video-event-analysis", "human-review-routing"],
    },
    {
        "id": "emergency-fire-risk",
        "prompt": "应急管理局要求搜索所有可能出现烟雾或火灾风险的监控点。",
        "expected_scenario": "emergency_fire_risk_search",
        "expected_chain": ["video-search", "single-video-event-analysis", "evidence-package-generation"],
    },
    {
        "id": "urban-management-flow",
        "prompt": "城管部门需要统计这个商圈的人流量峰值时段。",
        "expected_capability": "object_statistics",
        "expected_scenario": "urban_management_flow_statistics",
        "expected_chain": ["object-statistics"],
    },
    {
        "id": "cross-camera-gap",
        "prompt": "找出所有在同一时间出现在不同摄像头的可疑人员。",
        "expected_capability": "cross_video_investigation",
        "expected_scenario": "generic_video_intelligence",
        "expected_chain": ["video-search", "object-statistics", "object-tracking"],
        "expect_gap": True,
    },
    {
        "id": "streetmodel-development-test",
        "prompt": "测试一下 StreetModel 的嵌入效果怎么样，对比一下关键词搜索和向量搜索的结果差异。",
        "expected_capability": "development_test",
        "expected_scenario": "generic_video_intelligence",
        "expected_chain": ["video-search", "video-embedding-index"],
        "expected_mode": "development_test",
    },
]


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
        data_lines = []
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
        if ev["event"] == "metadata":
            try:
                return json.loads(ev["data"]).get("run_id")
            except Exception:
                return None
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


def extract_plan_from_artifacts(case_dir: Path) -> dict[str, Any] | None:
    candidates = [
        case_dir / "artifacts" / "outputs" / "city-video-intelligence" / "result.json",
        case_dir / "artifacts" / "workspace" / "city-video-intelligence" / "result.json",
        case_dir / "artifacts" / "workspace" / "orchestration_plan.json",
        case_dir / "result.json",
    ]
    candidates.extend(sorted((case_dir / "artifacts").glob("**/*.json")))
    for path in candidates:
        if path.exists():
            try:
                result = json.loads(path.read_text(encoding="utf-8"))
                if result.get("skill") != "city-video-intelligence" and "recommended_skill_chain" not in result:
                    continue
                json_dump(case_dir / "result.json", result)
                return result
            except Exception:
                continue
    return None


def extract_plan_from_text(text: str) -> dict[str, Any] | None:
    blocks = re.findall(r"```(?:json)?\s*(\{.*?\})\s*```", text, flags=re.DOTALL)
    blocks.append(text)
    for block in blocks:
        start = block.find("{")
        end = block.rfind("}")
        if start == -1 or end == -1 or end <= start:
            continue
        try:
            value = json.loads(block[start : end + 1])
        except Exception:
            continue
        if isinstance(value, dict):
            return value
    return None


def tool_text(tool_calls: list[dict[str, Any]]) -> str:
    return json.dumps(tool_calls, ensure_ascii=False)


def skill_script_observed(tool_calls: list[dict[str, Any]], skill: str) -> bool:
    text = tool_text(tool_calls)
    return (
        f"/mnt/skills/custom/{skill}/scripts/" in text
        or f"/mnt/skills/custom/{skill}/scripts/run.py" in text
        or (f"/mnt/skills/custom/{skill}" in text and "scripts/run.py" in text)
    )


def skill_loaded_or_executed(tool_calls: list[dict[str, Any]], skill: str) -> bool:
    text = tool_text(tool_calls)
    return f"/mnt/skills/custom/{skill}/SKILL.md" in text or skill_script_observed(tool_calls, skill)


def is_plain_question(text: str) -> bool:
    stripped = text.strip()
    if not stripped or "\n" in stripped:
        return False
    if not (stripped.endswith("？") or stripped.endswith("?")):
        return False
    if len(stripped) > 90:
        return False
    disallowed = ["如果", "可以", "根据", "为了", "例如", "如下", "我需要", "请提供这些信息", "**", "1.", "2.", "- "]
    return not any(token in stripped for token in disallowed)


def plain_clarification_question(tool_calls: list[dict[str, Any]]) -> str | None:
    for call in reversed(tool_calls):
        if call.get("name") != "ask_clarification":
            continue
        args = call.get("args") if isinstance(call.get("args"), dict) else {}
        question = str(args.get("question") or "").strip()
        has_extra = bool(args.get("context") or args.get("options"))
        if question and not has_extra and is_plain_question(question):
            return question
    return None


def validate_case(case: dict[str, Any], case_dir: Path, tool_calls: list[dict[str, Any]], final_text: str, stream_error: str | None, sse_errors: list[dict[str, Any]]) -> dict[str, Any]:
    plan = extract_plan_from_artifacts(case_dir) or extract_plan_from_text(final_text) or {}
    chain = plan.get("recommended_skill_chain") or []
    if not isinstance(chain, list):
        chain = []
    text = tool_text(tool_calls)
    clarification_question = plain_clarification_question(tool_calls)
    city_read = "/mnt/skills/custom/city-video-intelligence/SKILL.md" in text
    city_run = (
        "/mnt/skills/custom/city-video-intelligence/scripts/run.py" in text
        or "city-video-intelligence/scripts/run.py" in text
        or ("/mnt/skills/custom/city-video-intelligence" in text and "scripts/run.py" in text)
    )
    expected_chain = case.get("expected_chain", [])
    missing_chain = [skill for skill in expected_chain if skill not in chain]
    missing_direct_execution = [skill for skill in expected_chain if not skill_script_observed(tool_calls, skill)]
    actual_downstream = {
        skill: skill_loaded_or_executed(tool_calls, skill)
        for skill in expected_chain
    }
    actual_downstream_executed = {
        skill: skill_script_observed(tool_calls, skill)
        for skill in expected_chain
    }
    route_checks = {
        "city_skill_loaded": city_read,
        "city_skill_executed": city_run,
        "plan_status_success": plan.get("status") == "success",
        "capability_ok": not case.get("expected_capability") or plan.get("capability") == case.get("expected_capability"),
        "scenario_ok": not case.get("expected_scenario") or plan.get("business_scenario") == case.get("expected_scenario"),
        "mode_ok": not case.get("expected_mode") or plan.get("mode") == case.get("expected_mode"),
        "chain_ok": not missing_chain,
        "gap_ok": not case.get("expect_gap") or bool(plan.get("capability_gap")),
        "no_stream_error": not stream_error and not sse_errors,
        "follow_up_question_ok": not plan.get("follow_up_question")
        or final_text.strip() == plan.get("follow_up_question")
        or is_plain_question(final_text)
        or clarification_question == plan.get("follow_up_question"),
        "no_think_marker": "<think" not in final_text.lower() and "</think" not in final_text.lower(),
    }
    direct_execution_ok = bool(expected_chain) and not missing_direct_execution and not stream_error
    checks = dict(route_checks)
    checks["direct_downstream_execution_ok"] = direct_execution_ok or bool(plan.get("follow_up_question"))
    passed = all(checks.values())
    return {
        "id": case["id"],
        "passed": passed,
        "checks": checks,
        "expected": {
            "capability": case.get("expected_capability"),
            "scenario": case.get("expected_scenario"),
            "chain": expected_chain,
            "mode": case.get("expected_mode"),
            "expect_gap": case.get("expect_gap", False),
        },
        "actual": {
            "capability": plan.get("capability"),
            "scenario": plan.get("business_scenario"),
            "chain": chain,
            "mode": plan.get("mode"),
            "capability_gap": plan.get("capability_gap"),
            "missing_fields": plan.get("missing_fields"),
            "follow_up_question": plan.get("follow_up_question"),
            "clarification_question": clarification_question,
            "downstream_tool_observed": actual_downstream,
            "downstream_script_executed": actual_downstream_executed,
        },
        "missing_chain": missing_chain,
        "missing_direct_execution": missing_direct_execution,
        "thread_id": None,
        "run_id": None,
        "stream_error": stream_error,
        "sse_errors": sse_errors,
    }


def prompt_for(case: dict[str, Any]) -> str:
    return case["prompt"]


def run_one(api: str, run_root: Path, case: dict[str, Any], timeout: int, recursion_limit: int) -> dict[str, Any]:
    case_dir = run_root / case["id"]
    case_dir.mkdir(parents=True, exist_ok=True)
    prompt = prompt_for(case)
    text_write(case_dir / "prompt.txt", prompt)
    thread_payload = {"metadata": {"purpose": f"business-scenario-{case['id']}", "created_by": "codex"}}
    status, body = post_json(f"{api}/threads", thread_payload)
    text_write(case_dir / "thread-create-response.json", body)
    if status >= 300:
        validation = {"id": case["id"], "passed": False, "error": f"thread create failed: {status}", "body": body}
        json_dump(case_dir / "validation.json", validation)
        return validation
    thread = json.loads(body)
    thread_id = thread["thread_id"]
    json_dump(case_dir / "thread.json", thread)
    run_payload = {
        "assistant_id": ASSISTANT_ID,
        "input": {"messages": [{"role": "user", "content": prompt}]},
        "context": {"thread_id": thread_id},
        "config": {"recursion_limit": recursion_limit},
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
    final_text = final_agent_text(events)
    text_write(case_dir / "agent_final.md", final_text)
    json_dump(case_dir / "tool_calls.json", tool_calls)
    collect_thread_artifacts(thread_id, case_dir / "artifacts")
    validation = validate_case(case, case_dir, tool_calls, final_text, stream_error, error_events(events))
    validation["thread_id"] = thread_id
    validation["run_id"] = extract_run_id(events)
    json_dump(case_dir / "validation.json", validation)
    return validation


def write_summary(run_root: Path, results: list[dict[str, Any]], started_at: str, finished_at: str) -> None:
    passed = sum(1 for result in results if result.get("passed"))
    lines = [
        "# Business Scenario Agent Test Summary",
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
        "| Case | Pass | Capability | Scenario | Chain | Notes |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for result in results:
        actual = result.get("actual", {})
        checks = result.get("checks", {})
        failed_checks = [name for name, ok in checks.items() if not ok]
        note = "ok" if result.get("passed") else "failed: " + ", ".join(failed_checks)
        chain = ", ".join(actual.get("chain") or [])
        lines.append(
            f"| `{result['id']}` | `{result.get('passed')}` | `{actual.get('capability')}` | `{actual.get('scenario')}` | `{chain}` | {note} |"
        )
    text_write(run_root / "SUMMARY.md", "\n".join(lines) + "\n")
    json_dump(run_root / "manifest.json", {"started_at": started_at, "finished_at": finished_at, "results": results})


def main() -> int:
    parser = argparse.ArgumentParser(description="Run natural-language business scenario tests against DeerFlow lead_agent.")
    parser.add_argument("--api", default=DEFAULT_API)
    parser.add_argument("--root", default=str(REPO_ROOT / "outputs" / "skill-tests" / "business-scenarios"))
    parser.add_argument("--cases", nargs="*", default=[case["id"] for case in SCENARIOS])
    parser.add_argument("--timeout", type=int, default=150)
    parser.add_argument("--recursion-limit", type=int, default=80)
    args = parser.parse_args()
    selected = [case for case in SCENARIOS if case["id"] in set(args.cases)]
    if not selected:
        raise SystemExit("No matching cases selected")
    started = dt.datetime.now().astimezone().strftime("%Y-%m-%dT%H:%M:%S%z")
    run_id = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    run_root = Path(args.root) / run_id
    run_root.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(Path(__file__), run_root / "run_business_scenario_agent_tests.py")
    text_write(run_root / "docker-before.txt", docker_status())
    results = []
    for index, case in enumerate(selected, start=1):
        print(f"[{index}/{len(selected)}] {case['id']}", flush=True)
        result = run_one(args.api.rstrip("/"), run_root, case, args.timeout, args.recursion_limit)
        results.append(result)
        write_summary(run_root, results, started, dt.datetime.now().astimezone().strftime("%Y-%m-%dT%H:%M:%S%z"))
        time.sleep(1)
    text_write(run_root / "docker-after.txt", docker_status())
    finished = dt.datetime.now().astimezone().strftime("%Y-%m-%dT%H:%M:%S%z")
    write_summary(run_root, results, started, finished)
    print(run_root)
    return 0 if all(result.get("passed") for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
