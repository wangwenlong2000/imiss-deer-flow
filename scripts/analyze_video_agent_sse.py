#!/usr/bin/env python3
"""从视频监控 Agent 批次的原始 SSE 中提取"实际发生的工具调用"。

`run_video_surveillance_qwen35b_test.py` 的 `actual_skills` 会把场景过滤器注入的
`<available_skills>` 候选集一起统计进来，导致路由判定虚高。本脚本只统计：

  - 真实的 tool_calls（工具名 + 关键参数）
  - 工具调用中出现的 skill 路径 / skill 名
  - 结构化工具 `video_object_analytics` 的 operation
  - 错误事件与递归上限

用法：

    python3 scripts/analyze_video_agent_sse.py outputs/skill-tests/video-surveillance-qwen35b/<批次目录>
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

SKILL_PATH_RE = re.compile(r"/mnt/skills/custom/video_surveillance/([a-z0-9-]+)")
SKILL_NAME_RE = re.compile(r"\b(city-video-intelligence|single-video-event-analysis|video-stream-ingestion|"
                           r"frame-sampling|object-detection|object-tracking|roi-mapping|evidence-snapshot|"
                           r"video-segment-extraction|privacy-masking|human-review-routing|camera-health-check|"
                           r"duplicate-event-merge|video-search|batch-video-ingestion|object-statistics|"
                           r"evidence-package-generation|video-embedding-index|ffmpeg-utils|analyze-video)\b")


def parse_sse(raw: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    name: str | None = None
    lines: list[str] = []

    def flush() -> None:
        nonlocal name, lines
        if not lines:
            return
        text = "\n".join(lines)
        try:
            payload: Any = json.loads(text)
        except json.JSONDecodeError:
            payload = text
        events.append({"event": name, "data": payload})
        name, lines = None, []

    for line in raw.splitlines():
        if line.startswith("event:"):
            name = line.split(":", 1)[1].strip()
        elif line.startswith("data:"):
            lines.append(line.split(":", 1)[1].lstrip())
        elif not line.strip():
            flush()
    flush()
    return events


def analyze_case(case_dir: Path) -> dict[str, Any]:
    sse_files = sorted(case_dir.glob("*.sse"))
    calls: list[dict[str, Any]] = []
    errors: list[Any] = []
    seen: set[str] = set()
    for sse in sse_files:
        for event in parse_sse(sse.read_text(encoding="utf-8", errors="replace")):
            data = event.get("data")
            if event.get("event") == "error":
                errors.append(data)
            if not isinstance(data, dict):
                continue
            for message in data.get("messages", []):
                if not isinstance(message, dict):
                    continue
                raw_calls = message.get("tool_calls") or message.get("additional_kwargs", {}).get("tool_calls") or []
                for call in raw_calls if isinstance(raw_calls, list) else []:
                    if not isinstance(call, dict):
                        continue
                    name = call.get("name") or call.get("function", {}).get("name")
                    args = call.get("args") or call.get("function", {}).get("arguments") or {}
                    if isinstance(args, str):
                        try:
                            args = json.loads(args)
                        except json.JSONDecodeError:
                            args = {"_raw": args[:400]}
                    key = json.dumps([name, args], ensure_ascii=False, sort_keys=True)[:800]
                    if key in seen:
                        continue
                    seen.add(key)
                    calls.append({"name": name, "args": args})

    call_blob = json.dumps(calls, ensure_ascii=False)
    skills_from_paths = sorted(set(SKILL_PATH_RE.findall(call_blob)))
    skills_from_names = sorted(set(SKILL_NAME_RE.findall(call_blob)))
    invoked_skills = set(skills_from_paths) | set(skills_from_names)
    # 结构化工具 video_object_analytics 是对象分析的正式入口（文档 §5.3），
    # 调用它等同于走通了 video-object-analytics 能力，不能因为下划线命名而漏计。
    if any(call["name"] == "video_object_analytics" for call in calls):
        invoked_skills.add("video-object-analytics")
    invoked_skills = sorted(invoked_skills)
    tool_names: dict[str, int] = {}
    for call in calls:
        tool_names[str(call["name"])] = tool_names.get(str(call["name"]), 0) + 1

    analytics_ops = sorted({
        str(call["args"].get("operation"))
        for call in calls
        if call["name"] == "video_object_analytics" and isinstance(call["args"], dict) and call["args"].get("operation")
    })

    result_path = case_dir / "validation.json"
    result = json.loads(result_path.read_text(encoding="utf-8")) if result_path.exists() else {}
    final = result.get("final_answer", "")
    if not final:
        final_md = case_dir / "agent_final.md"
        final = final_md.read_text(encoding="utf-8") if final_md.exists() else ""

    return {
        "case": case_dir.name,
        "tool_calls_total": len(calls),
        "tool_names": tool_names,
        "skills_actually_touched": invoked_skills,
        "used_business_orchestration": "city-video-intelligence" in invoked_skills,
        "video_object_analytics_ops": analytics_ops,
        "used_raw_bash_for_video": any(
            call["name"] in {"bash", "bash_tool"}
            and isinstance(call["args"], dict)
            and re.search(r"ffmpeg|ffprobe|yolo|ultralytics", json.dumps(call["args"], ensure_ascii=False), re.I)
            for call in calls
        ),
        "errors": errors[:3],
        "recursion_limit_hit": "GraphRecursionError" in call_blob or any(
            "recursion" in json.dumps(e, ensure_ascii=False).lower() for e in errors
        ),
        "has_final_answer": bool(final),
        "final_answer_chars": len(final),
        "expected_skills": result.get("expected_skills", []),
        "expected_skills_actually_touched": sorted(
            set(result.get("expected_skills", [])) & set(invoked_skills)
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("batch_dir", help="批次目录，例如 outputs/skill-tests/video-surveillance-qwen35b/20260726-xxxx")
    parser.add_argument("--json", action="store_true", help="只输出 JSON")
    args = parser.parse_args()

    batch = Path(args.batch_dir)
    cases = sorted(p for p in batch.iterdir() if p.is_dir())
    report = [analyze_case(case) for case in cases]
    out_path = batch / "tool-call-analysis.json"
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0

    for item in report:
        print(f"== {item['case']}")
        print(f"   工具调用总数: {item['tool_calls_total']}  {item['tool_names']}")
        print(f"   实际触达 skill: {item['skills_actually_touched'] or '无'}")
        print(f"   期望 skill: {item['expected_skills']} -> 实际触达: {item['expected_skills_actually_touched'] or '无'}")
        print(f"   走业务编排: {item['used_business_orchestration']}  "
              f"video_object_analytics: {item['video_object_analytics_ops'] or '未调用'}  "
              f"绕过 skill 直接 bash 视频命令: {item['used_raw_bash_for_video']}")
        print(f"   递归上限: {item['recursion_limit_hit']}  最终回答: {item['has_final_answer']} "
              f"({item['final_answer_chars']} 字符)")
        if item["errors"]:
            print(f"   错误: {json.dumps(item['errors'][0], ensure_ascii=False)[:200]}")
    print(f"\n分析结果: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
