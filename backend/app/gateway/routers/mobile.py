"""Mobile platform adapter API.

This router exposes a DeerFlow-to-mobile adapter endpoint:

POST /api/mobile/chat

Current version:
- Call LangGraph Server through langgraph_sdk.
- Collect stream events from current run.
- Only keep displayable current-turn content.
- Avoid forwarding raw LangGraph token chunks and historical thread state.
"""

from __future__ import annotations

import json
import os
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

import asyncio
from collections.abc import AsyncIterator
from fastapi.responses import StreamingResponse, FileResponse


from pathlib import Path
from urllib.parse import quote





router = APIRouter(tags=["mobile"])


DEFAULT_LANGGRAPH_URL = os.getenv(
    "MOBILE_ADAPTER_LANGGRAPH_URL",
    os.getenv("LANGGRAPH_URL", "http://gaozhuohui-deer-flow-langgraph:3329"),
)
DEFAULT_ASSISTANT_ID = os.getenv("MOBILE_ADAPTER_ASSISTANT_ID", "lead_agent")

PUBLIC_GATEWAY_BASE_URL = os.getenv(
    "MOBILE_ADAPTER_PUBLIC_GATEWAY_BASE_URL",
    "",
).rstrip("/")

DEFAULT_RUN_CONFIG: dict[str, Any] = {
    "recursion_limit": int(os.getenv("MOBILE_ADAPTER_RECURSION_LIMIT", "300"))
}
DEFAULT_RUN_CONTEXT: dict[str, Any] = {
    "thinking_enabled": True,
    "is_plan_mode": False,
    "subagent_enabled": False,
}

CONVERSATION_THREAD_MAP: dict[str, str] = {}

AGENT_NAMES = {
    "intent": "意图识别智能体",
    "planner": "规划智能体",
    "system": "系统",
    "executor": "执行智能体",
    "feedback": "反馈智能体",
    "files": "结果文件",
    "final": "最终回答",
}

DEERFLOW_PUBLIC_BASE_URL = os.getenv("DEERFLOW_PUBLIC_BASE_URL", "http://219.245.186.96:3328").rstrip("/")


class MobileChatRequest(BaseModel):
    query: str = Field(..., description="User query from mobile platform")
    user_id: str | None = Field(default=None, description="Mobile platform user id")
    session_id: str | None = Field(default=None, description="Mobile platform session id")
    thread_id: str | None = Field(default=None, description="Existing DeerFlow LangGraph thread id")
    assistant_id: str | None = Field(default=None, description="LangGraph assistant id")
    stream: bool = Field(default=False, description="Reserved for future SSE streaming")

class DeerFlowMobileRunRequest(BaseModel):
    query: str = Field(..., description="User query from city brain platform")
    conversation_id: str = Field(..., description="Mobile city brain conversation id")
    user_id: str | None = Field(default=None, description="Mobile platform user id")
    stream: bool = Field(default=False, description="Reserved for future streaming")
    context: dict[str, Any] | None = Field(default=None, description="Optional extra context")
    thread_id: str | None = Field(default=None, description="Existing DeerFlow LangGraph thread id")
    assistant_id: str | None = Field(default=None, description="LangGraph assistant id")


def make_segment(
    agent_id: str,
    content: str,
    status: str = "completed",
    source_event: str = "raw_event",
) -> dict[str, Any]:
    return {
        "agent_id": agent_id,
        "agent_name": AGENT_NAMES.get(agent_id, agent_id),
        "status": status,
        "content": content,
        "metadata": {
            "source": "deerflow",
            "source_event": source_event,
        },
    }


def _to_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        return str(value)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _query_matches(value: Any, current_query: str) -> bool:
    text = _extract_text_content(value) or _text(value)
    query = _text(current_query)
    if not text or not query:
        return False

    if text == query:
        return True

    # 短追问不要用包含匹配，避免误命中历史消息
    if len(query) < 12:
        return False

    return query in text


def _extract_text_content(content: Any) -> str:
    if isinstance(content, str):
        return content.strip()

    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                if isinstance(item.get("text"), str):
                    parts.append(item["text"])
                elif isinstance(item.get("content"), str):
                    parts.append(item["content"])
        return "".join(parts).strip()

    if isinstance(content, dict):
        for key in ("text", "content"):
            if isinstance(content.get(key), str):
                return content[key].strip()

    return ""


def _message_type(msg: dict[str, Any]) -> str:
    msg_type = str(msg.get("type") or msg.get("role") or "").lower()
    if "human" in msg_type:
        return "human"
    if "ai" in msg_type or "assistant" in msg_type:
        return "ai"
    if "tool" in msg_type:
        return "tool"
    return msg_type


def _slice_messages_after_current_query(messages: list[Any], current_query: str) -> list[dict[str, Any]]:
    """Keep only messages after the current user query.

    LangGraph values events often contain the whole thread history. This function
    prevents previous-turn intent/tool/final messages from being displayed again.
    """
    start_index = -1

    for i, msg in enumerate(messages):
        if not isinstance(msg, dict):
            continue

        if _message_type(msg) == "human" and _query_matches(msg.get("content"), current_query):
            start_index = i

    if start_index < 0:
        return []

    return [m for m in messages[start_index + 1 :] if isinstance(m, dict)]


def _has_human_message(messages: list[Any]) -> bool:
    return any(
        isinstance(msg, dict) and _message_type(msg) == "human"
        for msg in messages
    )


def _current_turn_or_incremental_messages(
    messages: list[Any],
    current_query: str,
) -> list[dict[str, Any]]:
    """Return safe current-turn messages.

    If messages contain human messages, they are likely a full thread state,
    so we must slice after the current query.

    If messages do not contain human messages, they are likely incremental
    updates from the current run, so they can be used directly.
    """
    current_turn_messages = _slice_messages_after_current_query(messages, current_query)
    if current_turn_messages:
        return current_turn_messages

    if _has_human_message(messages):
        return []

    return [msg for msg in messages if isinstance(msg, dict)]

def _message_has_tool_calls(msg: dict[str, Any]) -> bool:
    tool_calls = msg.get("tool_calls") or []

    additional_kwargs = msg.get("additional_kwargs")
    if not tool_calls and isinstance(additional_kwargs, dict):
        tool_calls = additional_kwargs.get("tool_calls") or []

    return isinstance(tool_calls, list) and len(tool_calls) > 0


def _intent_context_matches_query(intent_context: Any, current_query: str) -> bool:
    if not isinstance(intent_context, dict):
        return False

    candidates = [
        intent_context.get("original_query"),
        intent_context.get("normalized_query"),
        intent_context.get("routing_query"),
    ]

    return any(_query_matches(candidate, current_query) for candidate in candidates)


def _find_current_intent_context(raw_events: list[Any], current_query: str) -> dict[str, Any] | None:
    for event in reversed(raw_events):
        if not isinstance(event, dict):
            continue
        if event.get("event") != "values":
            continue

        data = event.get("data")
        if not isinstance(data, dict):
            continue

        intent_context = data.get("intent_context")
        if _intent_context_matches_query(intent_context, current_query):
            return intent_context

    return None


def _format_scene_tasks(scene_tasks: Any) -> list[str]:
    if not isinstance(scene_tasks, list) or not scene_tasks:
        return []

    lines: list[str] = ["场景任务"]
    for task in scene_tasks:
        if isinstance(task, dict):
            lines.append(f"• {_to_json(task)}")
        else:
            lines.append(f"• {task}")
    return lines


def _build_intent_content(intent_context: dict[str, Any], current_query: str) -> str:
    lines: list[str] = ["意图识别"]

    routing_query = intent_context.get("original_query") or intent_context.get("normalized_query") or current_query
    lines.extend(["路由任务", _text(routing_query)])

    scenes = intent_context.get("scenes") or []
    scene_name = intent_context.get("scene_name")
    scene = intent_context.get("scene")

    if scenes:
        lines.extend(["识别场景", "、".join(str(s) for s in scenes)])
    elif scene_name:
        lines.extend(["识别场景", str(scene_name)])
    elif scene:
        lines.extend(["识别场景", str(scene)])

    lines.extend(_format_scene_tasks(intent_context.get("scene_tasks")))

    if len(lines) == 2:
        lines.extend(["任务内容", f"• {current_query}"])

    return "\n".join(lines)


def _build_fallback_intent_content(current_query: str) -> str:
    return "\n".join(
        [
            "意图识别",
            "当前问题",
            current_query,
            "对话类型",
            "基于当前会话上下文的追问",
        ]
    )


def _split_intent_and_planner_content(intent_content: str) -> tuple[str, str]:
    """把意图识别内容中的“任务切分”部分拆出来，作为规划智能体输出。"""
    if not intent_content.strip():
        return "", ""

    lines = intent_content.splitlines()
    intent_lines: list[str] = []
    planner_lines: list[str] = []

    in_task_split = False

    for line in lines:
        stripped = line.strip()

        # 进入“任务切分”段落
        if stripped.startswith("任务切分"):
            in_task_split = True
            continue

        # 遇到下一个段落，结束“任务切分”
        if in_task_split and (
            stripped.startswith("场景任务")
            or stripped.startswith("识别场景")
            or stripped.startswith("路由任务")
            or stripped.startswith("对话类型")
            or stripped.startswith("任务内容")
        ):
            in_task_split = False
            intent_lines.append(line)
            continue

        if in_task_split:
            if stripped:
                planner_lines.append(line)
            continue

        intent_lines.append(line)

    cleaned_intent = "\n".join(intent_lines).strip()

    if planner_lines:
        planner_content = "任务规划\n" + "\n".join(planner_lines).strip()
    else:
        planner_content = ""

    return cleaned_intent, planner_content

def _extract_skill_name_from_args(args: Any) -> str:
    if not isinstance(args, dict):
        return ""

    direct_keys = [
        "skill_id",
        "skill_name",
        "name",
        "selected_skill",
    ]
    for key in direct_keys:
        value = args.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    for nested_key in ("request", "skill", "input"):
        nested = args.get(nested_key)
        if isinstance(nested, dict):
            nested_name = _extract_skill_name_from_args(nested)
            if nested_name:
                return nested_name

    return ""


def _extract_skill_name_from_path(path: str) -> str:
    parts = [p for p in path.split("/") if p]
    if "skills" in parts:
        idx = parts.index("skills")
        if idx + 1 < len(parts):
            if parts[idx + 1] in {"public", "custom"} and idx + 2 < len(parts):
                return parts[idx + 2]
            return parts[idx + 1]
    return ""

def _filename_from_path(path: str) -> str:
    return path.rstrip("/").split("/")[-1]


def _artifact_url(thread_id: str, virtual_path: str) -> str:
    from urllib.parse import quote

    encoded_path = quote(virtual_path, safe="/")
    path = f"/api/threads/{thread_id}/artifacts{encoded_path}?download=true"
    if PUBLIC_GATEWAY_BASE_URL:
        return f"{PUBLIC_GATEWAY_BASE_URL}{path}"
    return path


def _extract_path_from_args(args: Any) -> str:
    if not isinstance(args, dict):
        return ""

    for key in ("path", "file_path", "filepath", "filename"):
        value = args.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    for nested_key in ("request", "input", "file"):
        nested = args.get(nested_key)
        if isinstance(nested, dict):
            path = _extract_path_from_args(nested)
            if path:
                return path

    return ""


def _extract_command_from_args(args: Any) -> str:
    if not isinstance(args, dict):
        return ""

    for key in ("command", "cmd", "code", "script"):
        value = args.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    for nested_key in ("request", "input", "args"):
        nested = args.get(nested_key)
        if isinstance(nested, dict):
            command = _extract_command_from_args(nested)
            if command:
                return command

    return ""


def _extract_tool_calls_from_message(msg: dict[str, Any]) -> list[dict[str, Any]]:
    tool_calls = msg.get("tool_calls") or []

    additional_kwargs = msg.get("additional_kwargs")
    if not tool_calls and isinstance(additional_kwargs, dict):
        tool_calls = additional_kwargs.get("tool_calls") or []

    return tool_calls if isinstance(tool_calls, list) else []


def _extract_description_from_args(args: Any) -> str:
    if not isinstance(args, dict):
        return ""
    value = args.get("description")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return ""


def _format_tool_call(name: str, args: Any) -> tuple[str, str]:
    source_event = f"tool_call:{name}"
    description = _extract_description_from_args(args)

    if name == "invoke_skill":
        skill_name = _extract_skill_name_from_args(args) or "未知"
        mode = args.get("mode") if isinstance(args, dict) else None

        if mode == "prepare":
            return f"准备调用 Skill：{skill_name}", source_event
        if mode == "wrap_output":
            return f"整理 Skill 输出：{skill_name}", source_event
        return f"调用 Skill：{skill_name}", source_event

    if name in {"read_skill", "read_file"}:
        path = _extract_path_from_args(args)
        title = description or "读取任务相关文件或资料"
        if path:
            return f"{title}\n{path}", source_event
        return title, source_event

    if name in {"bash", "python", "python_repl"}:
        command = _extract_command_from_args(args)
        title = description or "执行代码分析或数据处理"
        if command:
            return f"{title}\n{command}", source_event
        return title, source_event

    if name in {"write_file", "str_replace"}:
        path = _extract_path_from_args(args)
        title = description or "生成或修改结果文件"
        if path:
            return f"{title}\n{path}", source_event
        return title, source_event

    if name in {"web_search", "web_fetch"}:
        return description or "执行网络检索，获取外部信息", source_event

    return description or f"调用工具: {name}", source_event


def _append_unique_segment(
    segments: list[dict[str, Any]],
    seen: set[tuple[str, str]],
    segment: dict[str, Any],
) -> None:
    key = (
        str(segment.get("metadata", {}).get("source_event", "")),
        str(segment.get("content", "")),
    )
    if key in seen:
        return
    seen.add(key)
    segments.append(segment)


def _extract_tool_segments(raw_events: list[Any], current_query: str) -> list[dict[str, Any]]:
    segments: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    def handle_message(msg: dict[str, Any]) -> None:
        tool_calls = msg.get("tool_calls") or []

        additional_kwargs = msg.get("additional_kwargs")
        if not tool_calls and isinstance(additional_kwargs, dict):
            tool_calls = additional_kwargs.get("tool_calls") or []

        if not isinstance(tool_calls, list):
            return

        for tool_call in tool_calls:
            if not isinstance(tool_call, dict):
                continue

            name = str(tool_call.get("name") or tool_call.get("function", {}).get("name") or "")
            if not name:
                continue
            
            if name == "present_files":
                continue

            args = tool_call.get("args")
            if args is None and isinstance(tool_call.get("function"), dict):
                args = tool_call["function"].get("arguments")

            if isinstance(args, str):
                try:
                    args = json.loads(args)
                except Exception:
                    args = {"raw": args}

            content, source_event = _format_tool_call(name, args)

            agent_id = "system" if name == "invoke_skill" else "executor"

            _append_unique_segment(
                segments,
                seen,
                make_segment(
                    agent_id=agent_id,
                    content=content,
                    status="completed",
                    source_event=source_event,
                ),
            )

    for event in raw_events:
        if not isinstance(event, dict):
            continue

        event_name = event.get("event")
        data = event.get("data")

        if event_name == "values" and isinstance(data, dict):
            messages = data.get("raw_messages") or data.get("messages") or []
            if isinstance(messages, list):
                for msg in _current_turn_or_incremental_messages(messages, current_query):
                    handle_message(msg)

        elif event_name == "updates" and isinstance(data, dict):
            for node_value in data.values():
                if not isinstance(node_value, dict):
                    continue

                messages = node_value.get("raw_messages") or node_value.get("messages") or []
                if not isinstance(messages, list):
                    continue

                for msg in _current_turn_or_incremental_messages(messages, current_query):
                    handle_message(msg)

    return segments


def _extract_files_from_tool_calls(
    msg: dict[str, Any],
    thread_id: str,
) -> list[dict[str, str]]:
    files: list[dict[str, str]] = []

    for tool_call in _extract_tool_calls_from_message(msg):
        if not isinstance(tool_call, dict):
            continue

        name = str(tool_call.get("name") or tool_call.get("function", {}).get("name") or "")
        if name != "present_files":
            continue

        args = tool_call.get("args")
        if args is None and isinstance(tool_call.get("function"), dict):
            args = tool_call["function"].get("arguments")

        if isinstance(args, str):
            try:
                args = json.loads(args)
            except Exception:
                args = {"raw": args}

        if not isinstance(args, dict):
            continue

        filepaths = args.get("filepaths") or []
        if not isinstance(filepaths, list):
            continue

        for path in filepaths:
            if not isinstance(path, str) or not path:
                continue

            filename = _filename_from_path(path)
            download_url = _build_mobile_download_url(thread_id, filename)

            files.append(
                {
                    "filename": filename,
                    "path": path,
                    "artifact_url": _artifact_url(thread_id, path),
                    "download_url": download_url or "",
                }
            )

    return files


def _extract_file_segments(
    raw_events: list[Any],
    current_query: str,
    thread_id: str,
) -> list[dict[str, Any]]:
    files_by_path: dict[str, dict[str, str]] = {}

    def handle_message(msg: dict[str, Any]) -> None:
        for item in _extract_files_from_tool_calls(msg, thread_id):
            files_by_path[item["path"]] = item

    for event in raw_events:
        if not isinstance(event, dict):
            continue

        event_name = event.get("event")
        data = event.get("data")

        if event_name == "values" and isinstance(data, dict):
            messages = data.get("raw_messages") or data.get("messages") or []
            if isinstance(messages, list):
                for msg in _current_turn_or_incremental_messages(messages, current_query):
                    handle_message(msg)

        elif event_name == "updates" and isinstance(data, dict):
            for node_value in data.values():
                if not isinstance(node_value, dict):
                    continue

                messages = node_value.get("raw_messages") or node_value.get("messages") or []
                if not isinstance(messages, list):
                    continue

                for msg in _current_turn_or_incremental_messages(messages, current_query):
                    handle_message(msg)

    files = list(files_by_path.values())
    if not files:
        return []

    content = "已生成结果文件：\n" + "\n".join(
        f"• {item['filename']}\n  {item.get('download_url') or item.get('artifact_url') or ''}"
        for item in files
    )

    segment = make_segment(
        agent_id="files",
        content=content,
        status="completed",
        source_event="present_files",
    )
    segment["metadata"]["files"] = files
    return [segment]

def _strip_system_reminder(content: str) -> str:
    return (
        content
        .replace("<system_reminder>", "")
        .replace("</system_reminder>", "")
        .replace(
            "Use the following current-turn intent and routing guidance when deciding whether to create or update todos. Treat it as planning context, not as a user request.",
            "",
        )
        .strip()
    )


def _extract_hidden_step_body(content: str) -> str:
    import re

    if not content:
        return ""

    matches = list(
        re.finditer(
            r"<hidden_step\b([^>]*)>([\s\S]*?)</hidden_step>",
            content,
        )
    )

    if not matches:
        return _strip_system_reminder(content)

    rendered: list[str] = []
    for match in matches:
        attrs = match.group(1) or ""
        body = (match.group(2) or "").strip()

        title = ""
        title_match = re.search(r'title="([^"]+)"', attrs)
        if title_match:
            title = title_match.group(1).strip()

        if title and body:
            rendered.append(f"{title}\n{body}")
        elif body:
            rendered.append(body)

    return "\n\n".join(rendered).strip()


def _walk_dicts(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_dicts(child)


def _extract_current_turn_intent_content(
    raw_events: list[Any],
    current_query: str,
) -> str:
    """Extract frontend-visible hidden intent step from current run events.

    The DeerFlow frontend shows hidden steps from messages whose name is
    todo_routing_guidance. In mobile adapter, these messages may appear under:
    - values.data.messages
    - values.data.raw_messages
    - updates.data.RawTranscriptMiddleware.*.raw_messages
    """
    seen: set[str] = set()

    for event in raw_events:
        if not isinstance(event, dict):
            continue

        for obj in _walk_dicts(event):
            name = str(obj.get("name") or "")
            if name != "todo_routing_guidance":
                continue

            content = _extract_text_content(obj.get("content"))
            if not content or content in seen:
                continue

            # 防止误拿到上一轮：内容里至少要能对应当前 query 的主要文本
            # 这里不用严格相等，因为 hidden_step 里是“改写后的任务：xxx”
            if current_query and current_query not in content:
                continue

            seen.add(content)
            parsed = _extract_hidden_step_body(content)
            if parsed:
                return parsed

    return ""



def _extract_error_segments(raw_events: list[Any]) -> list[dict[str, Any]]:
    segments: list[dict[str, Any]] = []

    for event in raw_events:
        if not isinstance(event, dict):
            continue
        if event.get("event") != "error":
            continue

        data = event.get("data")
        if isinstance(data, dict):
            error_type = data.get("error") or data.get("error_type") or "Error"
            message = data.get("message") or data.get("detail") or _to_json(data)
            content = f"执行过程中出现异常：{error_type}: {message}"
        else:
            content = f"执行过程中出现异常：{data}"

        segments.append(
            make_segment(
                agent_id="system",
                content=content,
                status="failed",
                source_event="error",
            )
        )

    return segments


def _extract_final_answer(raw_events: list[Any], current_query: str) -> str:
    final_answer = ""

    def maybe_update_from_messages(messages: list[Any]) -> None:
        nonlocal final_answer

        current_turn_messages = _current_turn_or_incremental_messages(messages, current_query)
        if not current_turn_messages:
            return

        for msg in current_turn_messages:
            if not isinstance(msg, dict):
                continue
            if _message_type(msg) != "ai":
                continue
            if _message_has_tool_calls(msg):
                continue
            if msg.get("name") in {"todo_routing_guidance", "todo_reminder"}:
                continue

            additional_kwargs = msg.get("additional_kwargs")
            if isinstance(additional_kwargs, dict):
                if additional_kwargs.get("internal") is True:
                    continue
                if additional_kwargs.get("message_type") in {
                    "routed_skill_prompt",
                    "view_image_context",
                }:
                    continue
                if additional_kwargs.get("element") == "task":
                    continue

            content = _extract_text_content(msg.get("content"))
            if content:
                final_answer = content

    for event in raw_events:
        if not isinstance(event, dict):
            continue

        event_name = event.get("event")
        data = event.get("data")

        if event_name == "values" and isinstance(data, dict):
            messages = data.get("raw_messages") or data.get("messages") or []
            if isinstance(messages, list):
                maybe_update_from_messages(messages)

        elif event_name == "updates" and isinstance(data, dict):
            for node_value in data.values():
                if not isinstance(node_value, dict):
                    continue

                messages = node_value.get("raw_messages") or node_value.get("messages") or []
                if isinstance(messages, list):
                    maybe_update_from_messages(messages)

    return final_answer.strip()


def adapt_events_to_mobile(
    session_id: str | None,
    thread_id: str,
    raw_events: list[Any],
    current_query: str,
) -> dict[str, Any]:
    segments: list[dict[str, Any]] = []

    intent_content = _extract_current_turn_intent_content(raw_events, current_query)
    if intent_content:
        intent_content, planner_content = _split_intent_and_planner_content(intent_content)

        if intent_content:
            segments.append(
                make_segment(
                    agent_id="intent",
                    content=intent_content,
                    status="completed",
                    source_event="todo_routing_guidance",
                )
            )

        if planner_content:
            segments.append(
                make_segment(
                    agent_id="planner",
                    content=planner_content,
                    status="completed",
                    source_event="task_planning",
                )
            )

    segments.extend(_extract_tool_segments(raw_events, current_query))
    segments.extend(_extract_file_segments(raw_events, current_query, thread_id))
    segments.extend(_extract_error_segments(raw_events))

    final_answer = _extract_final_answer(raw_events, current_query)
    if final_answer:
        segments.append(
            make_segment(
                agent_id="final",
                content=final_answer,
                status="completed",
                source_event="final_answer",
            )
        )

    if not segments:
        segments.append(
            make_segment(
                agent_id="system",
                content="DeerFlow 已完成调用，但没有提取到可展示的最终回答。",
                status="failed",
                source_event="empty_result",
            )
        )

    return {
        "session_id": session_id or thread_id,
        "thread_id": thread_id,
        "segments": segments,
    }


MOBILE_ALLOWED_ROLES = {
    "意图识别智能体",
    "规划智能体",
    "系统",
    "执行智能体",
    "反馈智能体",
}

SEGMENT_AGENT_TO_MOBILE_ROLE = {
    "intent": "意图识别智能体",
    "planner": "规划智能体",
    "system": "系统",
    "executor": "执行智能体",
    "files": "执行智能体",
    "feedback": "反馈智能体",
    "final": "反馈智能体",
}


def _segment_to_mobile_role(segment: dict[str, Any]) -> str:
    agent_id = str(segment.get("agent_id") or "")
    role = SEGMENT_AGENT_TO_MOBILE_ROLE.get(agent_id)

    if not role:
        agent_name = str(segment.get("agent_name") or "")
        if agent_name in MOBILE_ALLOWED_ROLES:
            role = agent_name

    if role in MOBILE_ALLOWED_ROLES:
        return role

    return "系统"


def _segments_to_ordered_segments(segments: list[Any]) -> list[dict[str, Any]]:
    ordered_segments: list[dict[str, Any]] = []

    for index, segment in enumerate(segments, 1):
        if not isinstance(segment, dict):
            continue

        content = segment.get("content")
        if not isinstance(content, str):
            content = _to_json(content)

        item: dict[str, Any] = {
            "order": index,
            "role": _segment_to_mobile_role(segment),
            "content": content.strip() or "DeerFlow 已完成该步骤，但没有提取到可展示内容。",
            "status": str(segment.get("status") or "completed"),
        }

        metadata = segment.get("metadata")
        if isinstance(metadata, dict) and metadata:
            item["metadata"] = metadata

        ordered_segments.append(item)

    if not ordered_segments:
        ordered_segments.append(
            {
                "order": 1,
                "role": "反馈智能体",
                "content": "执行是否正确：False\n失败原因：DeerFlow 未返回可展示的过程片段。",
                "status": "failed",
            }
        )

    return ordered_segments

def _sse_event(event: str, data: dict[str, Any]) -> str:
    return (
        f"event: {event}\n"
        f"data: {json.dumps(data, ensure_ascii=False, default=str)}\n\n"
    )


def _mobile_run_error_response(
    *,
    conversation_id: str | None,
    thread_id: str | None,
    exc: Exception,
) -> dict[str, Any]:
    message = f"DeerFlow execution failed: {type(exc).__name__}: {exc}"
    return {
        "code": 500,
        "message": message,
        "conversation_id": conversation_id,
        "thread_id": thread_id,
        "ordered_segments": [
            {
                "order": 1,
                "role": "意图识别智能体",
                "content": "已接收用户问题，但 DeerFlow 执行过程中出现异常。",
                "status": "completed",
            },
            {
                "order": 2,
                "role": "反馈智能体",
                "content": f"执行是否正确：False\n失败原因：{message}\n建议：请检查 DeerFlow gateway、LangGraph、Skill、RAG 或依赖服务是否可用。",
                "status": "failed",
            },
        ],
    }


def _build_mobile_download_url(thread_id: str, filename: str) -> str | None:
    if not DEERFLOW_PUBLIC_BASE_URL:
        return None

    safe_filename = Path(filename).name
    return (
        f"{DEERFLOW_PUBLIC_BASE_URL}"
        f"/api/deerflow/mobile/files/{quote(thread_id)}/{quote(safe_filename)}"
    )


async def run_deerflow_and_collect_events(req: MobileChatRequest) -> tuple[str, list[Any]]:
    """Call LangGraph Server and collect current-run events.

    We intentionally do not request messages-tuple here. messages-tuple contains
    token-level chunks and is too verbose for the mobile platform.
    """
    from langgraph_sdk import get_client

    client = get_client(url=DEFAULT_LANGGRAPH_URL)

    conversation_key = req.session_id or req.thread_id or ""
    thread_id = req.thread_id

    if not thread_id and conversation_key:
        thread_id = CONVERSATION_THREAD_MAP.get(conversation_key)

    if not thread_id:
        thread = await client.threads.create()
        thread_id = thread["thread_id"]

    if conversation_key:
        CONVERSATION_THREAD_MAP[conversation_key] = thread_id

    assistant_id = req.assistant_id or DEFAULT_ASSISTANT_ID

    run_context = {
        **DEFAULT_RUN_CONTEXT,
        "thread_id": thread_id,
    }
    if req.user_id:
        run_context["user_id"] = req.user_id
    if req.session_id:
        run_context["session_id"] = req.session_id

    raw_events: list[Any] = []

    async for chunk in client.runs.stream(
        thread_id,
        assistant_id,
        input={"messages": [{"role": "human", "content": req.query}]},
        config=DEFAULT_RUN_CONFIG,
        context=run_context,
        stream_mode=["updates", "values", "messages-tuple"],
    ):
        raw_events.append(
            {
                "event": getattr(chunk, "event", ""),
                "data": getattr(chunk, "data", None),
            }
        )

    return thread_id, raw_events

async def _produce_langgraph_stream(
    chat_req: MobileChatRequest,
    queue: asyncio.Queue[dict[str, Any]],
) -> None:
    from langgraph_sdk import get_client

    try:
        client = get_client(url=DEFAULT_LANGGRAPH_URL)

        conversation_key = chat_req.session_id or chat_req.thread_id or ""
        thread_id = chat_req.thread_id

        if not thread_id and conversation_key:
            thread_id = CONVERSATION_THREAD_MAP.get(conversation_key)

        if not thread_id:
            thread = await client.threads.create()
            thread_id = thread["thread_id"]

        if conversation_key:
            CONVERSATION_THREAD_MAP[conversation_key] = thread_id

        assistant_id = chat_req.assistant_id or DEFAULT_ASSISTANT_ID

        run_context = {
            **DEFAULT_RUN_CONTEXT,
            "thread_id": thread_id,
        }
        if chat_req.user_id:
            run_context["user_id"] = chat_req.user_id
        if chat_req.session_id:
            run_context["session_id"] = chat_req.session_id

        await queue.put(
            {
                "kind": "start",
                "thread_id": thread_id,
            }
        )

        async for chunk in client.runs.stream(
            thread_id,
            assistant_id,
            input={"messages": [{"role": "human", "content": chat_req.query}]},
            config=DEFAULT_RUN_CONFIG,
            context=run_context,
            stream_mode=["updates", "values", "messages-tuple"],
        ):
            await queue.put(
                {
                    "kind": "chunk",
                    "raw_event": {
                        "event": getattr(chunk, "event", ""),
                        "data": getattr(chunk, "data", None),
                    },
                }
            )

        await queue.put({"kind": "done"})

    except Exception as exc:
        await queue.put(
            {
                "kind": "error",
                "error_type": type(exc).__name__,
                "message": str(exc),
            }
        )


async def _stream_deerflow_mobile_run(
    req: DeerFlowMobileRunRequest,
) -> AsyncIterator[str]:
    context = req.context if isinstance(req.context, dict) else {}
    thread_id = req.thread_id or context.get("thread_id")

    chat_req = MobileChatRequest(
        query=req.query,
        user_id=req.user_id,
        session_id=req.conversation_id,
        thread_id=thread_id,
        assistant_id=req.assistant_id,
        stream=True,
    )

    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    producer_task = asyncio.create_task(_produce_langgraph_stream(chat_req, queue))

    raw_events: list[Any] = []
    current_thread_id = thread_id or ""
    emitted_keys: set[tuple[str, str, str]] = set()
    emitted_segments: list[dict[str, Any]] = []
    next_order = 1

    def emit_new_segments(final: bool = False) -> list[dict[str, Any]]:
        nonlocal next_order

        if not current_thread_id:
            return []

        adapted = adapt_events_to_mobile(
            session_id=req.conversation_id,
            thread_id=current_thread_id,
            raw_events=raw_events,
            current_query=req.query,
        )

        ordered = _segments_to_ordered_segments(
            adapted.get("segments") if isinstance(adapted, dict) else []
        )

        new_items: list[dict[str, Any]] = []

        for item in ordered:
            metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
            source_event = str(metadata.get("source_event") or "")
            content = str(item.get("content") or "")
            role = str(item.get("role") or "")

            # 运行中不要把 early empty_result 发出去，避免页面误显示失败
            if source_event == "empty_result" and not final:
                continue

            key = (role, source_event, content)
            if key in emitted_keys:
                continue

            emitted_keys.add(key)

            out = dict(item)
            out["order"] = next_order
            next_order += 1

            emitted_segments.append(out)
            new_items.append(out)

        return new_items

    try:
        yield _sse_event(
            "accepted",
            {
                "code": 0,
                "message": "accepted",
                "conversation_id": req.conversation_id,
                "status": "running",
            },
        )

        while True:
            try:
                item = await asyncio.wait_for(queue.get(), timeout=15)
            except asyncio.TimeoutError:
                # 心跳非常重要：防止 A 侧 requests 读超时
                yield _sse_event(
                    "heartbeat",
                    {
                        "type": "heartbeat",
                        "conversation_id": req.conversation_id,
                        "thread_id": current_thread_id,
                        "status": "running",
                    },
                )
                continue

            kind = item.get("kind")

            if kind == "start":
                current_thread_id = str(item.get("thread_id") or "")
                yield _sse_event(
                    "start",
                    {
                        "code": 0,
                        "message": "started",
                        "conversation_id": req.conversation_id,
                        "thread_id": current_thread_id,
                        "status": "running",
                    },
                )

            elif kind == "chunk":
                raw_event = item.get("raw_event")
                if isinstance(raw_event, dict):
                    raw_events.append(raw_event)

                for segment in emit_new_segments(final=False):
                    yield _sse_event("segment", segment)

            elif kind == "error":
                yield _sse_event(
                    "error",
                    {
                        "code": 500,
                        "message": f"{item.get('error_type')}: {item.get('message')}",
                        "conversation_id": req.conversation_id,
                        "thread_id": current_thread_id,
                        "status": "failed",
                    },
                )
                break

            elif kind == "done":
                for segment in emit_new_segments(final=True):
                    yield _sse_event("segment", segment)

                yield _sse_event(
                    "done",
                    {
                        "code": 0,
                        "message": "success",
                        "conversation_id": req.conversation_id,
                        "thread_id": current_thread_id,
                        "status": "completed",
                        "ordered_segments": emitted_segments,
                    },
                )
                break

    finally:
        if not producer_task.done():
            producer_task.cancel()


@router.post("/api/mobile/chat")
async def mobile_chat(req: MobileChatRequest) -> dict[str, Any]:
    try:
        thread_id, raw_events = await run_deerflow_and_collect_events(req)
        return adapt_events_to_mobile(
            session_id=req.session_id,
            thread_id=thread_id,
            raw_events=raw_events,
            current_query=req.query,
        )
    except Exception as exc:
        return {
            "session_id": req.session_id,
            "thread_id": req.thread_id,
            "segments": [
                make_segment(
                    agent_id="system",
                    content=f"DeerFlow mobile adapter 调用失败：{type(exc).__name__}: {exc}",
                    status="failed",
                    source_event="exception",
                )
            ],
        }


@router.post("/api/deerflow/mobile/run")
async def deerflow_mobile_run(req: DeerFlowMobileRunRequest):

    if req.stream:
        return StreamingResponse(
            _stream_deerflow_mobile_run(req),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )


    context = req.context if isinstance(req.context, dict) else {}
    thread_id = req.thread_id or context.get("thread_id")

    chat_req = MobileChatRequest(
        query=req.query,
        user_id=req.user_id,
        session_id=req.conversation_id,
        thread_id=thread_id,
        assistant_id=req.assistant_id,
        stream=req.stream,
    )

    try:
        deerflow_thread_id, raw_events = await run_deerflow_and_collect_events(chat_req)
        adapted = adapt_events_to_mobile(
            session_id=req.conversation_id,
            thread_id=deerflow_thread_id,
            raw_events=raw_events,
            current_query=req.query,
        )

        ordered_segments = _segments_to_ordered_segments(
            adapted.get("segments") if isinstance(adapted, dict) else []
        )

        return {
            "code": 0,
            "message": "success",
            "conversation_id": req.conversation_id,
            "thread_id": deerflow_thread_id,
            "ordered_segments": ordered_segments,
        }

    except Exception as exc:
        return _mobile_run_error_response(
            conversation_id=req.conversation_id,
            thread_id=thread_id,
            exc=exc,
        )


@router.get("/api/deerflow/mobile/files/{thread_id}/{filename:path}")
async def deerflow_mobile_download_file(thread_id: str, filename: str):
    safe_filename = Path(filename).name

    candidates = [
        Path("backend/.deer-flow/threads") / thread_id / "user-data" / "outputs" / safe_filename,
        Path(".deer-flow/threads") / thread_id / "user-data" / "outputs" / safe_filename,
        Path("/app/backend/.deer-flow/threads") / thread_id / "user-data" / "outputs" / safe_filename,
        Path("/app/.deer-flow/threads") / thread_id / "user-data" / "outputs" / safe_filename,
    ]

    for path in candidates:
        resolved = path.resolve()
        if resolved.is_file():
            return FileResponse(
                path=str(resolved),
                filename=safe_filename,
                media_type="application/octet-stream",
            )

    raise HTTPException(
        status_code=404,
        detail=f"File not found: thread_id={thread_id}, filename={safe_filename}",
    )