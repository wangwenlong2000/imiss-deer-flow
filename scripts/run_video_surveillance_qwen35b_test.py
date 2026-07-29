#!/usr/bin/env python3
"""Run a small natural-language video-surveillance batch through qwen35B."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


# 与前端真实口径对齐：frontend/src/core/threads/hooks.ts 提交 run 时用的是
# recursion_limit=1000。此前测试脚本硬编码 100，导致多步骤视频任务在测试里
# 撞 GraphRecursionError，却被记成"模型步数预算不足"的产品结论。
DEFAULT_RECURSION_LIMIT = 1000

CASES: list[dict[str, Any]] = [
    {
        "id": "C-001",
        "question": "我上传了一段路口监控，请检查画面质量，判断有没有事故、拥堵或其他异常，并给出时间线和证据。",
        "expected": ["city-video-intelligence", "single-video-event-analysis"],
        "mode": "video",
        "video_file": "Vedio-demo/Trafic.mp4",
    },
    {
        "id": "C-002",
        "question": "请读取这个本地视频的时长、分辨率、帧率和时间信息，整理成后续分析可以使用的标准输入。",
        "expected": ["video-stream-ingestion"],
        "mode": "video",
        "video_file": "Vedio-demo/Trafic.mp4",
    },
    {
        "id": "C-003",
        "question": "从这段长录像中按场景变化抽取关键帧和章节，只准备审查素材，不要判断事件。",
        "expected": ["analyze-video", "frame-sampling"],
        "mode": "video",
        "video_file": "Vedio-demo/Trafic.mp4",
    },
    {
        "id": "C-005",
        "question": "从 01:20:00 到 01:21:30 截取一段视频，并在 01:20:45 导出一张关键帧。",
        "expected": ["ffmpeg-utils", "video-segment-extraction"],
        "mode": "video",
        "video_file": "Vedio-demo/Trafic.mp4",
    },
    {
        "id": "C-011",
        "question": "检测这段视频里的行人、轿车、公交车和卡车，输出每帧的位置框、类别和置信度。",
        "expected": ["video-object-analytics", "object-detection"],
        "mode": "video",
        "video_file": "Vedio-demo/Trafic.mp4",
    },
    {
        "id": "C-012",
        "question": "跟踪这辆白色轿车在视频中的轨迹，告诉我它何时出现、何时离开，以及是否移动或停留。",
        "expected": ["video-object-analytics", "object-tracking"],
        "mode": "video",
        "video_file": "Vedio-demo/Trafic.mp4",
    },
    {
        "id": "C-022",
        "question": "请检查这段视频有没有摔倒、打架、徘徊或翻越围栏，并返回各类行为的时间段。证据不足时请明确标记需要人工复核。",
        "expected": ["city-video-intelligence", "single-video-event-analysis"],
        "mode": "video",
        "video_file": "Vedio-demo/fight.mp4",
    },
    {
        "id": "C-028",
        "question": "找出摄像头 CAM_008 昨天 18 点到 20 点出现公交车的所有录像，并按时间排序。",
        "expected": ["city-video-intelligence", "video-search"],
        "mode": "boundary",
    },
    {
        "id": "C-029",
        "question": "把这个视频目录批量导入视频库，只登记元数据，不做内容检测。",
        "expected": ["batch-video-ingestion"],
        "mode": "boundary",
    },
    {
        "id": "C-034",
        "question": "以事件时间 00:00:02 为中心截取前后各 30 秒，并和证据截图一起生成可下载的证据包。",
        "expected": ["video-segment-extraction", "evidence-package-generation"],
        "mode": "video",
        "video_file": "Vedio-demo/Trafic.mp4",
    },
    {
        "id": "C-038",
        "question": "事件置信度低、摄像头画面有遮挡或事件涉及执法时，哪些结果必须进入人工复核？",
        "expected": ["human-review-routing"],
        "mode": "boundary",
    },
    {
        "id": "C-040",
        "question": "我想确认具体人员身份、读取模糊车牌、生成全市热力图并接入实时 RTSP，但当前数据只有本地视频和检测框。请说明哪些能力无法由现有数据支持，不要编造结果。",
        "expected": ["city-video-intelligence"],
        "mode": "boundary",
        "video_file": "Vedio-demo/Trafic.mp4",
    },
]


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


def post_multipart_file(url: str, file_path: Path, timeout: int) -> tuple[int, str]:
    import mimetypes
    import uuid

    boundary = f"----deerflow-test-{uuid.uuid4().hex}"
    filename = file_path.name
    content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
    content = file_path.read_bytes()
    header = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="files"; filename="{filename}"\r\n'
        f"Content-Type: {content_type}\r\n\r\n"
    ).encode("utf-8")
    body = header + content + f"\r\n--{boundary}--\r\n".encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "Content-Length": str(len(body)),
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")


def create_test_assistant(api: str, run_id: str, timeout: int) -> tuple[str, str]:
    status, body = post_json(
        f"{api.rstrip('/')}/assistants",
        {
            "graph_id": "lead_agent",
            "name": f"qwen35b-video-surveillance-test-{run_id}",
            "config": {"configurable": {"model_name": "qwen3.6-35b-a3b", "thinking_enabled": False}},
            "metadata": {"purpose": "video-surveillance-qwen35b-small-test", "history_preserved": True},
        },
        timeout,
    )
    if status >= 300:
        raise RuntimeError(f"assistant create HTTP {status}: {body[:500]}")
    data = json.loads(body)
    return str(data["assistant_id"]), body


def delete_test_assistant(api: str, assistant_id: str, timeout: int) -> None:
    request = urllib.request.Request(
        f"{api.rstrip('/')}/assistants/{assistant_id}?delete_threads=true",
        headers={"Accept": "application/json"},
        method="DELETE",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout):
            return
    except urllib.error.HTTPError:
        return


def parse_sse(raw: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    event_name: str | None = None
    data_lines: list[str] = []
    for line in raw.splitlines():
        if line.startswith("event:"):
            event_name = line.split(":", 1)[1].strip()
        elif line.startswith("data:"):
            data_lines.append(line.split(":", 1)[1].lstrip())
        elif not line.strip() and data_lines:
            data = "\n".join(data_lines)
            try:
                parsed: Any = json.loads(data)
            except json.JSONDecodeError:
                parsed = data
            events.append({"event": event_name, "data": parsed})
            event_name = None
            data_lines = []
    if data_lines:
        data = "\n".join(data_lines)
        try:
            parsed = json.loads(data)
        except json.JSONDecodeError:
            parsed = data
        events.append({"event": event_name, "data": parsed})
    return events


def content_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(content_text(item) for item in content)
    if isinstance(content, dict):
        return str(content.get("text") or content.get("content") or "")
    return ""


def observe(events: list[dict[str, Any]]) -> dict[str, Any]:
    texts: list[str] = []
    ai_texts: list[str] = []
    tool_calls: list[dict[str, Any]] = []
    models: set[str] = set()
    errors: list[dict[str, Any]] = []
    messages_seen = 0
    for event in events:
        data = event.get("data")
        if event.get("event") == "error":
            errors.append(data if isinstance(data, dict) else {"value": data})
        if not isinstance(data, dict):
            continue
        if event.get("event") == "values":
            for message in data.get("messages", []):
                if not isinstance(message, dict):
                    continue
                messages_seen += 1
                text = content_text(message.get("content"))
                if text:
                    texts.append(text)
                    if message.get("type") in {"ai", "assistant"} or message.get("role") == "assistant":
                        ai_texts.append(text)
                metadata = message.get("response_metadata") or {}
                for key in ("model_name", "model"):
                    value = metadata.get(key)
                    if isinstance(value, str):
                        models.add(value)
                calls = message.get("tool_calls") or message.get("additional_kwargs", {}).get("tool_calls", [])
                if isinstance(calls, list):
                    for call in calls:
                        if isinstance(call, dict):
                            tool_calls.append(call)
    combined = "\n".join(texts)
    # 判定只能基于真实的 tool_calls。消息正文里含有场景过滤器注入的
    # <available_skills> 候选集（一次注入就列出 bundle 内全部 skill 路径），
    # 拿正文做匹配会把"候选"误判成"调用过"，一次调用就假性命中 21 个 skill。
    all_call_text = json.dumps(tool_calls, ensure_ascii=False)
    skills_touched = set(re.findall(r"/mnt/skills/custom/video_surveillance/([a-z0-9-]+)", all_call_text))
    skills_touched.update(re.findall(r"\"skill_name\"\s*:\s*\"([a-z0-9-]+)\"", all_call_text))
    # 结构化工具 video_object_analytics 是单视频对象分析的正式入口，
    # 调用它等同于走通了 video-object-analytics 能力，不能因下划线命名漏计。
    for call in tool_calls:
        name = call.get("name") or (call.get("function") or {}).get("name")
        if name == "video_object_analytics":
            skills_touched.add("video-object-analytics")
    # 正文里出现的 skill 路径只作诊断参考，不参与通过与否的判定。
    mentioned = set(re.findall(r"/mnt/skills/custom/video_surveillance/([a-z0-9-]+)", combined))
    mentioned |= set(re.findall(r"/mnt/skills/(?:custom|public)/(?!video_surveillance/)([^/\s\"'`]+)", combined))
    return {
        "texts": texts,
        "final": ai_texts[-1] if ai_texts else (texts[-1] if texts else ""),
        "tool_calls": tool_calls,
        "models": sorted(models),
        "skills_touched": sorted(skills_touched),
        "skills_mentioned_only": sorted(mentioned - skills_touched),
        "errors": errors,
        "messages_seen": messages_seen,
        "event_count": len(events),
    }


def run_case(
    api: str,
    gateway_api: str,
    assistant_id: str,
    output_root: Path,
    dataset_root: Path,
    case: dict[str, Any],
    timeout: int,
    recursion_limit: int = DEFAULT_RECURSION_LIMIT,
) -> dict[str, Any]:
    case_dir = output_root / case["id"]
    case_dir.mkdir(parents=True, exist_ok=True)
    status, body = post_json(
        f"{api.rstrip('/')}/threads",
        {
            "metadata": {
                "purpose": "video-surveillance-qwen35b-small-test",
                "case_id": case["id"],
                "title": case["question"][:80],
                "video_file": case.get("video_file"),
                "history_preserved": True,
            }
        },
        timeout,
    )
    (case_dir / "thread-create-response.json").write_text(body, encoding="utf-8")
    if status >= 300:
        result = {"id": case["id"], "question": case["question"], "passed": False, "error": f"thread create HTTP {status}: {body[:500]}"}
        (case_dir / "validation.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        return result
    try:
        thread_id = json.loads(body)["thread_id"]
    except (KeyError, json.JSONDecodeError) as exc:
        result = {"id": case["id"], "question": case["question"], "passed": False, "error": f"invalid thread response: {exc}"}
        (case_dir / "validation.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        return result
    video_file = case.get("video_file")
    uploaded_file: dict[str, Any] | None = None
    if video_file:
        host_video = (dataset_root / video_file).resolve()
        if not host_video.is_file():
            result = {
                "id": case["id"],
                "question": case["question"],
                "thread_id": thread_id,
                "video_file": video_file,
                "passed": False,
                "error": f"video file not found: {host_video}",
            }
            (case_dir / "validation.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
            return result
        upload_status, upload_body = post_multipart_file(
            f"{gateway_api.rstrip('/')}/api/threads/{thread_id}/uploads",
            host_video,
            timeout,
        )
        (case_dir / "upload-response.json").write_text(upload_body, encoding="utf-8")
        if upload_status >= 300:
            result = {
                "id": case["id"],
                "question": case["question"],
                "thread_id": thread_id,
                "video_file": video_file,
                "passed": False,
                "error": f"video upload HTTP {upload_status}: {upload_body[:500]}",
            }
            (case_dir / "validation.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
            return result
        upload_data = json.loads(upload_body)
        uploaded = upload_data.get("files") or []
        if not uploaded:
            raise RuntimeError(f"video upload returned no files: {upload_body[:500]}")
        uploaded_info = uploaded[0]
        uploaded_file = {
            "filename": uploaded_info["filename"],
            "size": int(uploaded_info["size"]),
            "path": uploaded_info.get("virtual_path", f"/mnt/user-data/uploads/{uploaded_info['filename']}"),
            "status": "uploaded",
        }
    (case_dir / "thread.json").write_text(
        json.dumps(
            {
                "thread_id": thread_id,
                "assistant_id": assistant_id,
                "case_id": case["id"],
                "video_file": video_file,
                "uploaded_file": uploaded_file,
                "history_preserved": True,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    message: dict[str, Any] = {"role": "user", "content": case["question"]}
    if uploaded_file:
        message["additional_kwargs"] = {"files": [uploaded_file]}
    payload = {
        "assistant_id": assistant_id,
        "input": {"messages": [message]},
        "context": {"thread_id": thread_id},
        "config": {
            "recursion_limit": recursion_limit,
        },
        "stream_mode": ["values"],
    }
    request = urllib.request.Request(
        f"{api.rstrip('/')}/threads/{thread_id}/runs/stream",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "text/event-stream"},
        method="POST",
    )
    error: str | None = None
    raw = ""
    observation: dict[str, Any] = {}
    try:
        # urlopen 的 timeout 只作用于单次 socket 读，不是总时长。Agent 一旦进入
        # 工具调用循环就会持续吐 SSE，每次读都在超时窗口内，整个用例可以跑上几小时
        # 而永远不触发超时——recursion_limit 提到 1000 之后尤其明显。这里按总墙钟
        # 时间硬性截断，并把已收到的部分当作观测结果，至少能看出它循环在什么地方。
        deadline = time.monotonic() + timeout
        chunks: list[bytes] = []
        truncated = False
        with urllib.request.urlopen(request, timeout=timeout) as response:
            while True:
                if time.monotonic() > deadline:
                    truncated = True
                    break
                chunk = response.read1(65536) if hasattr(response, "read1") else response.read(65536)
                if not chunk:
                    break
                chunks.append(chunk)
        raw = b"".join(chunks).decode("utf-8", errors="replace")
        (case_dir / "turn-1.sse").write_text(raw, encoding="utf-8")
        observation = observe(parse_sse(raw))
        if truncated:
            error = f"WallClockTimeout: 超过 {timeout}s 仍未结束，已按总时长截断（通常意味着 Agent 陷入工具调用循环）"
    except Exception as exc:  # Keep one failed case from hiding the rest of the batch.
        error = repr(exc)
    (case_dir / "observation.json").write_text(json.dumps(observation, ensure_ascii=False, indent=2), encoding="utf-8")
    (case_dir / "agent_final.md").write_text(observation.get("final", ""), encoding="utf-8")
    actual = set(observation.get("skills_touched", []))
    # 路由判定采用"允许集合 + 禁止集合"，而不是要求命中某一个固定 skill：
    #   expect_any —— 命中其中任意一个即算路由正确（容纳设计上等价的多条正确链路）
    #   expect_all —— 必须全部命中（严格链路要求，旧 expected 字段等价于它）
    #   forbid     —— 命中任意一个即算路由错误
    expect_any = case.get("expect_any") or []
    expect_all = case.get("expect_all") or case.get("expected") or []
    forbid = case.get("forbid") or []
    missing = [skill for skill in expect_all if skill not in actual]
    unmet_any = bool(expect_any) and not (set(expect_any) & actual)
    violated = sorted(set(forbid) & actual)
    # 有些题目本身就没给必需输入（例如"把事件分成三类"却没有事件数据）。
    # 这种情况下发起追问比硬凑一个 skill 跑出空结果更正确，不应判成路由失败。
    # 只对显式标注 accept_clarification 的用例生效，避免把"该干活时偷懒追问"也放过。
    asked_clarification = any(
        (call.get("name") or (call.get("function") or {}).get("name")) == "ask_clarification"
        for call in observation.get("tool_calls", [])
    )
    clarification_ok = bool(case.get("accept_clarification")) and asked_clarification
    route_ok = clarification_ok or (not missing and not unmet_any and not violated)
    final = observation.get("final", "")
    passed = not error and not observation.get("errors") and bool(final) and route_ok
    result = {
        "id": case["id"],
        "mode": case["mode"],
        "question": case["question"],
        "thread_id": thread_id,
        "video_file": video_file,
        "uploaded_file": uploaded_file,
        "model_expected": "qwen3.6-35b-a3b",
        "assistant_id": assistant_id,
        "models_observed": observation.get("models", []),
        "expect_any": expect_any,
        "expect_all": expect_all,
        "forbid": forbid,
        "actual_skills": sorted(actual),
        "skills_mentioned_only": observation.get("skills_mentioned_only", []),
        "missing_expected_skills": missing,
        "unmet_expect_any": expect_any if unmet_any else [],
        "forbidden_skills_used": violated,
        "route_ok": route_ok,
        "final_answer_ok": bool(final),
        "errors": observation.get("errors", []),
        "error": error,
        "final_answer": final,
        "passed": passed,
    }
    (case_dir / "validation.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", default="http://127.0.0.1:3538/api/langgraph")
    parser.add_argument("--gateway", default="http://127.0.0.1:3538")
    parser.add_argument("--timeout", type=int, default=480)
    parser.add_argument("--output-root", default="outputs/skill-tests/video-surveillance-qwen35b")
    parser.add_argument(
        "--dataset-root",
        default="/home/huangxiao/City_brain/imiss-deer-flow-main/datasets",
        help="Host dataset root used to upload the video attached to each video case.",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Delete the test assistant and its threads after the batch. By default history is preserved.",
    )
    parser.add_argument("--cases", nargs="*", default=["C-001", "C-002", "C-011", "C-038", "C-040"])
    parser.add_argument(
        "--cases-file",
        default=None,
        help="JSON file holding the case list. Replaces the built-in CASES; --cases still filters by id.",
    )
    parser.add_argument(
        "--recursion-limit",
        type=int,
        default=DEFAULT_RECURSION_LIMIT,
        help=f"LangGraph recursion limit per run (default {DEFAULT_RECURSION_LIMIT}, matching the frontend).",
    )
    args = parser.parse_args()
    run_id = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    output_root = Path(args.output_root) / run_id
    output_root.mkdir(parents=True, exist_ok=True)
    if args.cases_file:
        pool = json.loads(Path(args.cases_file).read_text(encoding="utf-8"))
        selected = pool if args.cases == parser.get_default("cases") else [c for c in pool if c["id"] in set(args.cases)]
    else:
        selected = [case for case in CASES if case["id"] in set(args.cases)]
    if not selected:
        raise SystemExit("No matching cases selected")
    (output_root / "cases.json").write_text(json.dumps(selected, ensure_ascii=False, indent=2), encoding="utf-8")
    started = dt.datetime.now().astimezone().isoformat()
    results: list[dict[str, Any]] = []
    assistant_id, assistant_body = create_test_assistant(args.api, run_id, args.timeout)
    (output_root / "assistant-create-response.json").write_text(assistant_body, encoding="utf-8")
    try:
        for index, case in enumerate(selected, 1):
            print(f"[{index}/{len(selected)}] {case['id']}", flush=True)
            result = run_case(
                args.api, args.gateway, assistant_id, output_root, Path(args.dataset_root), case, args.timeout, args.recursion_limit
            )
            results.append(result)
            print(json.dumps({key: result.get(key) for key in ("id", "passed", "models_observed", "actual_skills", "missing_expected_skills", "error")}, ensure_ascii=False), flush=True)
            time.sleep(1)
    finally:
        if args.cleanup:
            delete_test_assistant(args.api, assistant_id, args.timeout)
    summary = {
        "started_at": started,
        "finished_at": dt.datetime.now().astimezone().isoformat(),
        "api": args.api,
        "model": "qwen3.6-35b-a3b",
        "assistant_id": assistant_id,
        "history_preserved": not args.cleanup,
        "video_attachments_uploaded": sum(bool(item.get("uploaded_file")) for item in results),
        "total": len(results),
        "passed": sum(bool(item.get("passed")) for item in results),
        "route_mismatch": sum(bool(item.get("missing_expected_skills")) for item in results),
        "model_mismatch": sum(item.get("models_observed") != ["qwen3.6-35b-a3b"] for item in results),
        "errors": sum(bool(item.get("error") or item.get("errors")) for item in results),
        "results": results,
    }
    (output_root / "manifest.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# qwen35B 视频监控自然问题小规模测试",
        "",
        f"- model: `{summary['model']}`",
        f"- total: `{summary['total']}`",
        f"- passed: `{summary['passed']}`",
        f"- route_mismatch: `{summary['route_mismatch']}`",
        f"- model_mismatch: `{summary['model_mismatch']}`",
        f"- errors: `{summary['errors']}`",
        f"- history_preserved: `{summary['history_preserved']}`",
        f"- video_attachments_uploaded: `{summary['video_attachments_uploaded']}`",
        "",
        "| ID | 通过 | 实际技能 | 缺少预期技能 | 模型 |",
        "| --- | --- | --- | --- | --- |",
    ]
    for item in results:
        lines.append(
            f"| `{item.get('id')}` | `{item.get('passed')}` | `{', '.join(item.get('actual_skills', []))}` | `{', '.join(item.get('missing_expected_skills', []))}` | `{', '.join(item.get('models_observed', []))}` |"
        )
    (output_root / "SUMMARY.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(output_root)
    return 0 if summary["errors"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
