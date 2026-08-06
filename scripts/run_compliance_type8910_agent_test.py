#!/usr/bin/env python3
"""Run real lead_agent requests for synthetic compliance type 8/9/10 cases."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CASES = REPO_ROOT / "tests/fixtures/compliance_type8910_agent_cases.json"


def post_json(url: str, payload: dict[str, Any], timeout: int) -> tuple[int, str]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")


def parse_sse(raw: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    event_name: str | None = None
    data_lines: list[str] = []

    def flush() -> None:
        nonlocal event_name, data_lines
        if not data_lines:
            return
        data = "\n".join(data_lines)
        try:
            parsed: Any = json.loads(data)
        except json.JSONDecodeError:
            parsed = data
        events.append({"event": event_name, "data": parsed})
        event_name = None
        data_lines = []

    for line in raw.splitlines():
        if line.startswith("event:"):
            event_name = line.split(":", 1)[1].strip()
        elif line.startswith("data:"):
            data_lines.append(line.split(":", 1)[1].lstrip())
        elif not line.strip():
            flush()
    flush()
    return events


def text_of(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "".join(text_of(item) for item in value)
    if isinstance(value, dict):
        return str(value.get("text") or value.get("content") or "")
    return ""


def message_type(message: dict[str, Any]) -> str:
    return str(message.get("type") or message.get("role") or "")


def extract_skill_names(calls: list[dict[str, Any]]) -> list[str]:
    """Extract only skill names from the real invoke_skill tool calls.

    A lead-agent trace also contains read_file/ls/bash calls and sometimes
    echoes shell snippets or skill paths.  Those are useful trace data, but
    they are not skills actually invoked by the agent.
    """
    names: set[str] = set()
    for call in calls:
        args = call.get("args")
        if isinstance(args, dict):
            name = args.get("skill_name")
            if isinstance(name, str) and name.strip():
                names.add(name.strip())
        function = call.get("function")
        if isinstance(function, dict):
            raw_args = function.get("arguments")
            if isinstance(raw_args, str):
                try:
                    parsed = json.loads(raw_args)
                except json.JSONDecodeError:
                    parsed = None
                if isinstance(parsed, dict):
                    name = parsed.get("skill_name")
                    if isinstance(name, str) and name.strip():
                        names.add(name.strip())
    return sorted(names)


def read_audit_records(thread_id: str) -> list[dict[str, Any]]:
    """Read service-side compliance audit records for one real thread."""
    audit_dir = REPO_ROOT / "backend/.deer-flow/compliance/audit"
    records: list[dict[str, Any]] = []
    if not audit_dir.is_dir():
        return records
    for path in sorted(audit_dir.glob("compliance-*.jsonl")):
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line in lines:
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(record, dict) and record.get("thread_id") == thread_id:
                records.append(record)
    records.sort(key=lambda item: str(item.get("timestamp") or ""))
    return records


def observe(events: list[dict[str, Any]]) -> dict[str, Any]:
    messages: list[dict[str, Any]] = []
    calls: list[dict[str, Any]] = []
    tool_results: list[dict[str, Any]] = []
    ai_texts: list[str] = []
    models: set[str] = set()
    custom_events: list[dict[str, Any]] = []
    errors: list[Any] = []

    for event in events:
        data = event.get("data")
        if event.get("event") == "error":
            errors.append(data)
        if event.get("event") == "custom":
            custom_events.append(data if isinstance(data, dict) else {"value": data})
        if not isinstance(data, dict):
            continue
        if event.get("event") == "values":
            batch = data.get("messages") or []
            if isinstance(batch, list):
                messages.extend(item for item in batch if isinstance(item, dict))
        # Some server versions emit messages in updates rather than values.
        for value in data.values():
            if isinstance(value, dict) and isinstance(value.get("messages"), list):
                messages.extend(item for item in value["messages"] if isinstance(item, dict))

    seen_messages: set[str] = set()
    unique_messages: list[dict[str, Any]] = []
    for message in messages:
        key = json.dumps(
            [message.get("id"), message_type(message), text_of(message.get("content")), message.get("tool_calls")],
            ensure_ascii=False,
            sort_keys=True,
        )
        if key not in seen_messages:
            seen_messages.add(key)
            unique_messages.append(message)
        metadata = message.get("response_metadata") or {}
        for field in ("model_name", "model"):
            if isinstance(metadata.get(field), str):
                models.add(metadata[field])
        raw_calls = message.get("tool_calls") or (message.get("additional_kwargs") or {}).get("tool_calls") or []
        if isinstance(raw_calls, list):
            for call in raw_calls:
                if isinstance(call, dict):
                    calls.append(call)
        if message_type(message) in {"tool", "tool_result"}:
            tool_results.append({"name": message.get("name"), "content": message.get("content")})
        if message_type(message) in {"ai", "assistant"}:
            text = text_of(message.get("content"))
            if text:
                ai_texts.append(text)

    skill_names = extract_skill_names(calls)

    compliance: list[dict[str, Any]] = []
    for message in unique_messages:
        metadata = message.get("response_metadata") or {}
        item = metadata.get("compliance")
        if isinstance(item, dict):
            compliance.append(item)

    return {
        "messages_seen": len(unique_messages),
        "tool_calls": calls,
        "tool_results": tool_results,
        "skills_touched": skill_names,
        "models": sorted(models),
        "final": ai_texts[-1] if ai_texts else "",
        "compliance": compliance,
        "custom_events": custom_events,
        "errors": errors,
        "event_count": len(events),
    }


def create_assistant(api: str, model: str, stamp: str, timeout: int) -> str:
    status, body = post_json(
        f"{api.rstrip('/')}/assistants",
        {
            "graph_id": "lead_agent",
            "name": f"compliance-type8910-{stamp}",
            "config": {"configurable": {"model_name": model, "thinking_enabled": False}},
            "metadata": {"purpose": "compliance-type8910-real-agent-test", "synthetic_only": True},
        },
        timeout,
    )
    if status >= 300:
        raise RuntimeError(f"assistant create HTTP {status}: {body[:500]}")
    return str(json.loads(body)["assistant_id"])


def run_case(api: str, assistant_id: str, case: dict[str, Any], scene: str, output_dir: Path, timeout: int, recursion_limit: int) -> dict[str, Any]:
    case_dir = output_dir / f"{case['id']}__{scene}"
    case_dir.mkdir(parents=True, exist_ok=True)
    status, body = post_json(
        f"{api.rstrip('/')}/threads",
        {"metadata": {"purpose": "compliance-type8910-real-agent-test", "case_id": case["id"], "scene": scene, "synthetic_only": True}},
        timeout,
    )
    (case_dir / "thread-create-response.json").write_text(body, encoding="utf-8")
    if status >= 300:
        return {"case_id": case["id"], "scene": scene, "status": "error", "error": f"thread create HTTP {status}: {body[:500]}"}
    thread_id = str(json.loads(body)["thread_id"])
    context = {
        "thread_id": thread_id,
        "compliance_context": {"source": "manual_ui", "enabled": True, "scene": scene},
    }
    payload = {
        "assistant_id": assistant_id,
        "input": {"messages": [{"role": "user", "content": case["question"]}]},
        "context": context,
        "config": {"recursion_limit": recursion_limit},
        "stream_mode": ["values", "custom", "updates"],
    }
    request = urllib.request.Request(
        f"{api.rstrip('/')}/threads/{thread_id}/runs/stream",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "text/event-stream"},
        method="POST",
    )
    started = time.monotonic()
    raw = ""
    error: str | None = None
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            chunks: list[bytes] = []
            while True:
                if time.monotonic() - started > timeout:
                    error = f"wall-clock timeout after {timeout}s"
                    break
                chunk = response.read1(65536) if hasattr(response, "read1") else response.read(65536)
                if not chunk:
                    break
                chunks.append(chunk)
            raw = b"".join(chunks).decode("utf-8", errors="replace")
    except Exception as exc:
        error = repr(exc)
    (case_dir / "turn-1.sse").write_text(raw, encoding="utf-8")
    observation = observe(parse_sse(raw))
    audit_records = read_audit_records(thread_id)
    audit_actions = sorted({action for item in audit_records for action in item.get("actions", [])})
    audit_violation_types = sorted(
        {
            hit.get("violation_type")
            for item in audit_records
            for hit in item.get("hits", [])
            if isinstance(hit, dict) and isinstance(hit.get("violation_type"), str)
        }
    )
    (case_dir / "observation.json").write_text(json.dumps(observation, ensure_ascii=False, indent=2), encoding="utf-8")
    (case_dir / "agent_final.md").write_text(observation["final"], encoding="utf-8")
    result = {
        "case_id": case["id"],
        "type_no": case["type_no"],
        "violation_type": case["violation_type"],
        "scene": scene,
        "thread_id": thread_id,
        "title": case["title"],
        "expected_skills": case.get("expected_skills", []),
        "actual_skills": observation["skills_touched"],
        "expected_action": case.get("expected_scene_action", {}).get(scene),
        "compliance": observation["compliance"],
        "actual_actions": audit_actions,
        "detected_violation_types": audit_violation_types,
        "audit": [
            {
                "audit_ref": item.get("audit_ref"),
                "gate": item.get("gate"),
                "scene_key": item.get("scene_key"),
                "actions": item.get("actions", []),
                "per_violation_actions": item.get("per_violation_actions", {}),
                "hits": [
                    {
                        "detector_id": hit.get("detector_id"),
                        "violation_type": hit.get("violation_type"),
                        "confidence": hit.get("confidence"),
                        "severity": hit.get("severity"),
                        "reason_code": hit.get("reason_code"),
                        "evidence": hit.get("evidence"),
                    }
                    for hit in item.get("hits", [])
                    if isinstance(hit, dict)
                ],
            }
            for item in audit_records
        ],
        "custom_event_count": len(observation["custom_events"]),
        "final_chars": len(observation["final"]),
        "models": observation["models"],
        "error": error,
        "agent_errors": observation["errors"],
    }
    (case_dir / "validation.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api", default="http://127.0.0.1:3538/api/langgraph")
    parser.add_argument("--model", default="qwen3.6-35b-a3b")
    parser.add_argument("--cases", nargs="*", default=None)
    parser.add_argument("--scenes", nargs="*", default=["public_release"])
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--recursion-limit", type=int, default=500)
    parser.add_argument("--cases-file", type=Path, default=DEFAULT_CASES)
    parser.add_argument("--output-root", type=Path, default=REPO_ROOT / "outputs" / "compliance-type8910-agent")
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()
    cases = json.loads(args.cases_file.read_text(encoding="utf-8"))
    if args.list:
        for case in cases:
            print(f"{case['id']} type={case['type_no']} {case['violation_type']} {case['skill']}")
        return 0
    selected = [case for case in cases if not args.cases or case["id"] in args.cases]
    if not selected:
        raise SystemExit(f"no cases matched: {args.cases}")
    stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    output_dir = args.output_root / stamp
    output_dir.mkdir(parents=True, exist_ok=True)
    assistant_id = create_assistant(args.api, args.model, stamp, args.timeout)
    results: list[dict[str, Any]] = []
    print(f"assistant={assistant_id} output={output_dir}")
    for index, case in enumerate(selected, start=1):
        for scene in args.scenes:
            print(f"[{index}/{len(selected)} {scene}] {case['id']} {case['title']}", flush=True)
            result = run_case(args.api, assistant_id, case, scene, output_dir, args.timeout, args.recursion_limit)
            results.append(result)
            print(json.dumps({k: result.get(k) for k in ("case_id", "scene", "actual_skills", "compliance", "error")}, ensure_ascii=False), flush=True)
    summary = {"assistant_id": assistant_id, "model": args.model, "scenes": args.scenes, "results": results}
    (output_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = ["# 第 8/9/10 类真实 Agent 合规测试", "", f"assistant: `{assistant_id}`", "", "| 用例 | 场景 | 类型 | Skill | 审计闸门/命中/动作 | 最终回答字符数 | 错误 |", "|---|---|---|---|---|---:|---|"]
    for result in results:
        audit = "; ".join(
            f"{item.get('gate')}: {','.join(item.get('hits') and sorted({hit.get('violation_type') for hit in item['hits']} - {None}) or []) or '未命中'} / {','.join(item.get('actions') or []) or '无动作'}"
            for item in result.get("audit", [])
        ) or "未落盘"
        audit = audit.replace("|", "/")[:240]
        lines.append(f"| {result['case_id']} | {result['scene']} | {result['violation_type']} | {', '.join(result.get('actual_skills') or []) or '无'} | {audit} | {result.get('final_chars', 0)} | {result.get('error') or ''} |")
    (output_dir / "REPORT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0 if all(not item.get("error") and not item.get("agent_errors") for item in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
