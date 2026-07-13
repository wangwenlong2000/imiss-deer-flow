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
    attachments: list[dict[str, Any]] | None = Field(
        default=None,
        description="Uploaded file metadata returned by DeerFlow uploads API",
    )

class DeerFlowMobileRunRequest(BaseModel):
    query: str = Field(..., description="User query from city brain platform")
    conversation_id: str = Field(..., description="Mobile city brain conversation id")
    user_id: str | None = Field(default=None, description="Mobile platform user id")
    stream: bool = Field(default=False, description="Reserved for future streaming")
    context: dict[str, Any] | None = Field(default=None, description="Optional extra context")
    thread_id: str | None = Field(default=None, description="Existing DeerFlow LangGraph thread id")
    assistant_id: str | None = Field(default=None, description="LangGraph assistant id")
    attachments: list[dict[str, Any]] | None = Field(
        default=None,
        description="Uploaded file metadata returned by DeerFlow uploads API",
    )


class DeerFlowMobileEnsureThreadRequest(BaseModel):
    conversation_id: str = Field(..., description="Mobile city brain conversation id")
    source: str | None = Field(default="citybrain", description="Caller source")
    user_id: str | None = Field(default=None, description="Mobile platform user id")
    assistant_id: str | None = Field(default=None, description="LangGraph assistant id")
    metadata: dict[str, Any] | None = Field(default=None, description="Optional extra metadata")


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
    """优先把意图识别内容中的“任务规划”拆出来，作为规划智能体输出。

    兼容旧格式：
    - 新格式：提取“任务规划”
    - 旧格式：如果没有“任务规划”，退回提取“任务切分”
    """
    if not intent_content.strip():
        return "", ""

    lines = intent_content.splitlines()
    intent_lines: list[str] = []
    planner_lines: list[str] = []

    section_starters = (
        "改写后的任务",
        "识别场景",
        "任务切分",
        "任务规划",
        "场景任务",
        "路由任务",
        "对话类型",
        "任务内容",
        "已提取参数",
        "任务提示",
    )

    has_task_planning = any(
        line.strip().startswith("任务规划")
        for line in lines
    )
    target_title = "任务规划" if has_task_planning else "任务切分"

    in_target_section = False

    for line in lines:
        stripped = line.strip()

        if stripped.startswith(target_title):
            in_target_section = True
            continue

        if in_target_section and any(
            stripped.startswith(title)
            for title in section_starters
            if title != target_title
        ):
            in_target_section = False
            intent_lines.append(line)
            continue

        if in_target_section:
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


CLARIFICATION_TOOL_NAMES = {"ask_clarification"}


def _is_clarification_tool_name(name: str) -> bool:
    return str(name or "").strip() in CLARIFICATION_TOOL_NAMES


def _parse_tool_args(args: Any) -> Any:
    if isinstance(args, str):
        try:
            return json.loads(args)
        except Exception:
            return {"raw": args}
    return args



def _extract_pending_action_from_event(raw_event: dict[str, Any]) -> dict[str, Any] | None:
    data = raw_event.get("data") if isinstance(raw_event, dict) else None

    for obj in _walk_dicts(data):
        if not isinstance(obj, dict):
            continue

        pending = obj.get("pending_action")
        if isinstance(pending, dict) and pending.get("type") == "clarification":
            return pending

    return None


def _format_pending_action_question(pending_action: dict[str, Any]) -> str:
    question = str(pending_action.get("question") or "").strip()
    options = pending_action.get("options")

    if question and isinstance(options, list) and options:
        lines: list[str] = []
        for item in options:
            if isinstance(item, dict):
                label = str(item.get("label") or item.get("value") or "").strip()
                index = item.get("index")
                if label:
                    if index:
                        lines.append(f"  {index}. {label}")
                    else:
                        lines.append(f"  - {label}")
            elif str(item).strip():
                lines.append(f"  - {item}")

        if lines:
            return question + "\n\n" + "\n".join(lines)

    return question


def _make_clarification_payload(
    *,
    conversation_id: str | None,
    thread_id: str | None,
    content: str,
    pending_action: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "type": "clarification",
        "status": "need_user_input",
        "role": "反馈智能体",
        "content": content.strip() or "当前任务需要补充信息，请进一步说明你的需求。",
        "thread_id": thread_id,
        "conversation_id": conversation_id,
        "resume_required": True,
    }

    if isinstance(pending_action, dict):
        pending_action_id = pending_action.get("id")
        if isinstance(pending_action_id, str) and pending_action_id.strip():
            payload["pending_action_id"] = pending_action_id.strip()

        payload["pending_action"] = {
            "id": pending_action.get("id"),
            "type": pending_action.get("type"),
            "question": pending_action.get("question"),
            "expected_answer_type": pending_action.get("expected_answer_type"),
            "options": pending_action.get("options") or [],
        }

    return payload


def _is_actual_clarification_tool_message(obj: dict[str, Any]) -> bool:
    """Only match the real ToolMessage generated by ClarificationMiddleware.

    Do not match AI tool_call objects. Some tool_call objects may have
    type='tool_call' and name='ask_clarification', but they are not the final
    user-facing clarification message.
    """
    if not isinstance(obj, dict):
        return False

    raw_type = str(obj.get("type") or obj.get("role") or "").lower()

    # 只认真正的 ToolMessage，不认 tool_call / tool_calls
    if raw_type not in {"tool", "toolmessage", "tool_message"}:
        return False

    if obj.get("name") != "ask_clarification":
        return False

    # AI tool_call 通常会带 args/function；真正 ToolMessage 不应该带这些字段
    if "args" in obj or "function" in obj:
        return False

    content = _extract_text_content(obj.get("content"))
    if not content:
        content = str(obj.get("content") or "").strip()

    return bool(content)

def _iter_message_lists_from_event(raw_event: dict[str, Any]):
    if not isinstance(raw_event, dict):
        return

    data = raw_event.get("data")
    if not isinstance(data, dict):
        return

    # values.data.raw_messages / values.data.messages
    for key in ("raw_messages", "messages"):
        messages = data.get(key)
        if isinstance(messages, list):
            yield messages

    # updates.data.<node>.raw_messages / messages
    if raw_event.get("event") == "updates":
        for node_value in data.values():
            if not isinstance(node_value, dict):
                continue

            for key in ("raw_messages", "messages"):
                messages = node_value.get(key)
                if isinstance(messages, list):
                    yield messages


def _slice_after_latest_human_message(
    messages: list[Any],
    current_query: str,
) -> list[dict[str, Any]]:
    """Only keep messages after the latest human message.

    values events often contain the whole thread history. For clarification,
    we must not scan old ask_clarification messages from previous turns.
    """
    start_index = -1

    # 优先找与当前 query 匹配的 human
    for i, msg in enumerate(messages):
        if not isinstance(msg, dict):
            continue

        if _message_type(msg) == "human" and _query_matches(msg.get("content"), current_query):
            start_index = i

    # 如果匹配不到 current_query，就退回找最后一个 human。
    # 这对“上传文件 + 补充信息”场景更稳，因为 values 里最后一个 human 通常就是当前轮用户消息。
    if start_index < 0:
        for i in range(len(messages) - 1, -1, -1):
            msg = messages[i]
            if isinstance(msg, dict) and _message_type(msg) == "human":
                start_index = i
                break

    if start_index >= 0:
        return [m for m in messages[start_index + 1:] if isinstance(m, dict)]

    # 没有人类消息时，说明可能是当前轮增量 updates，直接保留
    return [m for m in messages if isinstance(m, dict)]


def _iter_current_turn_messages_from_event(
    raw_event: dict[str, Any],
    current_query: str,
):
    for messages in _iter_message_lists_from_event(raw_event) or []:
        for msg in _slice_after_latest_human_message(messages, current_query):
            if isinstance(msg, dict):
                yield msg


def _event_has_current_turn_clarification_tool_call(
    raw_event: dict[str, Any],
    current_query: str,
) -> bool:
    for msg in _iter_current_turn_messages_from_event(raw_event, current_query):
        for tool_call in _extract_tool_calls_from_message(msg):
            if not isinstance(tool_call, dict):
                continue

            name = str(
                tool_call.get("name")
                or tool_call.get("function", {}).get("name")
                or ""
            )

            if _is_clarification_tool_name(name):
                return True

    return False


def _iter_message_lists_from_event(raw_event: dict[str, Any]):
    if not isinstance(raw_event, dict):
        return

    data = raw_event.get("data")
    if not isinstance(data, dict):
        return

    # values.data.raw_messages / values.data.messages
    for key in ("raw_messages", "messages"):
        messages = data.get(key)
        if isinstance(messages, list):
            yield messages

    # updates.data.<node>.raw_messages / messages
    if raw_event.get("event") == "updates":
        for node_value in data.values():
            if not isinstance(node_value, dict):
                continue
            for key in ("raw_messages", "messages"):
                messages = node_value.get(key)
                if isinstance(messages, list):
                    yield messages


def _iter_current_turn_messages_from_event(
    raw_event: dict[str, Any],
    current_query: str,
):
    for messages in _iter_message_lists_from_event(raw_event) or []:
        for msg in _current_turn_or_incremental_messages(messages, current_query):
            if isinstance(msg, dict):
                yield msg


def _event_has_current_turn_clarification_tool_call(
    raw_event: dict[str, Any],
    current_query: str,
) -> bool:
    for msg in _iter_current_turn_messages_from_event(raw_event, current_query):
        for tool_call in _extract_tool_calls_from_message(msg):
            if not isinstance(tool_call, dict):
                continue

            name = str(
                tool_call.get("name")
                or tool_call.get("function", {}).get("name")
                or ""
            )

            if _is_clarification_tool_name(name):
                return True

    return False

def _extract_clarification_from_event(
    raw_event: dict[str, Any],
    *,
    conversation_id: str | None,
    thread_id: str | None,
    current_query: str,
) -> dict[str, Any] | None:
    if not isinstance(raw_event, dict):
        return None

    pending_action = _extract_pending_action_from_event(raw_event)

    # 1. 只从当前用户消息之后的 messages 中找 ask_clarification ToolMessage
    # 不要全量 _walk_dicts(data)，否则会扫到历史 clarification。
    candidates: list[dict[str, Any]] = []

    for msg in _iter_current_turn_messages_from_event(raw_event, current_query):
        if _is_actual_clarification_tool_message(msg):
            candidates.append(msg)

    if candidates:
        # 如果同一事件里出现多个 clarification，取最后一个，更接近当前轮最新状态
        msg = candidates[-1]

        content = _extract_text_content(msg.get("content"))
        if not content:
            content = str(msg.get("content") or "").strip()

        return _make_clarification_payload(
            conversation_id=conversation_id,
            thread_id=thread_id,
            content=content,
            pending_action=pending_action,
        )

    # 2. pending_action 只能作为“当前轮确实调用了 ask_clarification”时的兜底
    # 不能单独看到 pending_action 就返回，否则会把上一轮 pending_action 当成当前轮。
    if (
        isinstance(pending_action, dict)
        and _event_has_current_turn_clarification_tool_call(raw_event, current_query)
    ):
        content = _format_pending_action_question(pending_action)
        if content:
            return _make_clarification_payload(
                conversation_id=conversation_id,
                thread_id=thread_id,
                content=content,
                pending_action=pending_action,
            )

    return None



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

            if _is_clarification_tool_name(name):
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

    for raw_event in raw_events:
        if not isinstance(raw_event, dict):
            continue

        clarification = _extract_clarification_from_event(
            raw_event,
            conversation_id=session_id,
            thread_id=thread_id,
            current_query=current_query,
        )

        if clarification:
            return {
                "session_id": session_id or thread_id,
                "thread_id": thread_id,
                "type": "clarification",
                "status": "need_user_input",
                "clarification": clarification,
                "segments": [
                    make_segment(
                        agent_id="feedback",
                        content=clarification["content"],
                        status="need_user_input",
                        source_event="clarification",
                    )
                ],
            }

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


def _coerce_file_size(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except Exception:
        return None


def _normalize_mobile_attachments(attachments: Any) -> list[dict[str, Any]]:
    """Convert mobile attachments to DeerFlow LangGraph message files.

    DeerFlow UploadsMiddleware expects files under:
    messages[0].additional_kwargs.files[].path

    The path must be the sandbox virtual path, for example:
    /mnt/user-data/uploads/example.xlsx
    """
    if not isinstance(attachments, list):
        return []

    files: list[dict[str, Any]] = []
    for item in attachments:
        if not isinstance(item, dict):
            continue

        filename = str(item.get("filename") or item.get("name") or "").strip()
        raw_path = str(
            item.get("virtual_path")
            or item.get("path")
            or item.get("file_path")
            or ""
        ).strip()

        if not filename and raw_path:
            filename = Path(raw_path).name

        # A side should send virtual_path. This fallback handles accidental
        # backend local paths from the upload response.
        if raw_path and not raw_path.startswith("/mnt/user-data/"):
            marker = "/user-data/"
            if marker in raw_path:
                raw_path = "/mnt" + raw_path[raw_path.index(marker):]

        if not raw_path and filename:
            raw_path = f"/mnt/user-data/uploads/{Path(filename).name}"

        if not filename or not raw_path.startswith("/mnt/user-data/"):
            continue

        file_info: dict[str, Any] = {
            "filename": filename,
            "path": raw_path,
            "status": str(item.get("status") or "uploaded"),
        }

        size = _coerce_file_size(item.get("size"))
        if size is not None:
            file_info["size"] = size

        files.append(file_info)

    return files


def _attachments_from_request(req: Any, context: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    attachments = getattr(req, "attachments", None)
    if isinstance(attachments, list):
        return attachments

    if isinstance(context, dict):
        for key in ("attachments", "files"):
            value = context.get(key)
            if isinstance(value, list):
                return value

    return []


def _build_langgraph_input(query: str, attachments: Any = None) -> dict[str, Any]:
    message: dict[str, Any] = {
        "role": "human",
        "content": query,
    }

    files = _normalize_mobile_attachments(attachments)
    if files:
        message["additional_kwargs"] = {"files": files}

    return {"messages": [message]}


async def ensure_mobile_thread(req: DeerFlowMobileEnsureThreadRequest) -> tuple[str, bool]:
    """Ensure a real LangGraph thread for a mobile conversation.

    Returns:
        (thread_id, created)
    """
    from langgraph_sdk import get_client

    conversation_id = str(req.conversation_id or "").strip()
    if not conversation_id:
        raise ValueError("conversation_id is required")

    existing_thread_id = CONVERSATION_THREAD_MAP.get(conversation_id)
    if existing_thread_id:
        return existing_thread_id, False

    client = get_client(url=DEFAULT_LANGGRAPH_URL)

    metadata: dict[str, Any] = {
        "conversation_id": conversation_id,
        "source": req.source or "citybrain",
    }
    if req.user_id:
        metadata["user_id"] = req.user_id
    if isinstance(req.metadata, dict):
        metadata.update(req.metadata)

    try:
        thread = await client.threads.create(metadata=metadata)
    except TypeError:
        # Compatible with older langgraph_sdk versions that do not accept metadata.
        thread = await client.threads.create()

    thread_id = str(thread.get("thread_id") or "")
    if not thread_id:
        raise RuntimeError(f"LangGraph thread create returned no thread_id: {thread}")

    CONVERSATION_THREAD_MAP[conversation_id] = thread_id
    return thread_id, True


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
        input=_build_langgraph_input(req.query, req.attachments),
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
            input=_build_langgraph_input(chat_req.query, chat_req.attachments),
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
        attachments=_attachments_from_request(req, context),
    )

    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    producer_task = asyncio.create_task(_produce_langgraph_stream(chat_req, queue))

    raw_events: list[Any] = []
    current_thread_id = thread_id or ""
    emitted_keys: set[tuple[str, str, str]] = set()
    emitted_segments: list[dict[str, Any]] = []
    next_order = 1
    clarification_emitted = False
    pending_clarification: dict[str, Any] | None = None

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

                    clarification = _extract_clarification_from_event(
                        raw_event,
                        conversation_id=req.conversation_id,
                        thread_id=current_thread_id,
                        current_query=req.query,
                    )

                    if clarification and not clarification_emitted:
                        clarification_emitted = True
                        pending_clarification = clarification

                        # 不要立刻 break，也不要 cancel producer_task。
                        # 继续消费 LangGraph stream，等 kind == "done" 后再把 clarification 发给移动端。
                        continue

                if not clarification_emitted:
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

                yield "data: [DONE]\n\n"

                break

            elif kind == "done":
                if pending_clarification:
                    yield _sse_event("clarification", pending_clarification)
                    yield "data: [DONE]\n\n"
                    break

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

                yield "data: [DONE]\n\n"

                break

    finally:
        if not producer_task.done():
            producer_task.cancel()


@router.post("/api/deerflow/mobile/threads/ensure")
async def deerflow_mobile_ensure_thread(req: DeerFlowMobileEnsureThreadRequest) -> dict[str, Any]:
    try:
        thread_id, created = await ensure_mobile_thread(req)
        assistant_id = req.assistant_id or DEFAULT_ASSISTANT_ID

        return {
            "code": 0,
            "message": "success",
            "data": {
                "conversation_id": req.conversation_id,
                "thread_id": thread_id,
                "assistant_id": assistant_id,
                "created": created,
            },
        }

    except Exception as exc:
        return {
            "code": 500,
            "message": f"ensure thread failed: {type(exc).__name__}: {exc}",
            "data": {
                "conversation_id": req.conversation_id,
                "thread_id": None,
                "assistant_id": req.assistant_id or DEFAULT_ASSISTANT_ID,
                "created": False,
            },
        }


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
        attachments=_attachments_from_request(req, context),
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