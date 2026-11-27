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

import asyncio
import hashlib
import json
import logging
import os
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any
from urllib.parse import quote
from uuid import UUID, uuid4

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field, PrivateAttr

from app.admission_claims import (
    AdmissionSigningConfigurationError,
    build_admission_metadata,
    ensure_admission_signing_available,
)
from app.gateway.conversation_store import get_or_create_conversation_thread
from app.gateway.mobile_iam import (
    MobileAuthorizationInput,
    authenticate_mobile_request,
    authorize_mobile_request,
    build_trusted_compliance_request,
)
from app.gateway.thread_access import require_thread_access, thread_error_status
from app.internal_auth import internal_graph_headers

logger = logging.getLogger(__name__)
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

DEFAULT_RUN_CONFIG: dict[str, Any] = {"recursion_limit": int(os.getenv("MOBILE_ADAPTER_RECURSION_LIMIT", "300"))}
DEFAULT_RUN_CONTEXT: dict[str, Any] = {
    "thinking_enabled": True,
    "is_plan_mode": False,
    "subagent_enabled": False,
}
MOBILE_STREAM_HEARTBEAT_SECONDS = 15.0
MOBILE_RUN_STATUS_POLL_SECONDS = 1.0

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
    _iam_authenticated: bool = PrivateAttr(default=False)
    _resume_run_id: str | None = PrivateAttr(default=None)

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
    compliance_identity: dict[str, Any] | None = Field(
        default=None,
        description="Compatibility field; trusted mobile identity is supplied by the server-side IAM provider",
    )
    compliance_context: dict[str, Any] | None = Field(
        default=None,
        description="Compliance runtime context forwarded to LangGraph run_context",
    )
    compliance_request: dict[str, Any] | None = Field(
        default=None,
        description="Untrusted operation/resource hints; the server rebuilds the trusted contract after IAM authorization",
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
    compliance_identity: dict[str, Any] | None = Field(
        default=None,
        description="Compatibility field; trusted mobile identity is supplied by the server-side IAM provider",
    )
    compliance_context: dict[str, Any] | None = Field(
        default=None,
        description="Compliance runtime context forwarded to LangGraph run_context",
    )
    compliance_request: dict[str, Any] | None = Field(
        default=None,
        description="Untrusted operation/resource hints; the server rebuilds the trusted contract after IAM authorization",
    )


class DeerFlowMobileEnsureThreadRequest(BaseModel):
    conversation_id: str = Field(..., description="Mobile city brain conversation id")
    source: str | None = Field(default="citybrain", description="Caller source")
    user_id: str | None = Field(default=None, description="Mobile platform user id")
    assistant_id: str | None = Field(default=None, description="LangGraph assistant id")
    metadata: dict[str, Any] | None = Field(default=None, description="Optional extra metadata")


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _mobile_authorization_hints(req: MobileChatRequest) -> tuple[str | None, str | None, str | None, str | None]:
    raw = _mapping(req.compliance_request)
    operation = _mapping(raw.get("operation"))
    resource = _mapping(raw.get("resource"))
    return (
        str(operation["type"]) if operation.get("type") else None,
        str(operation["target_audience"]) if operation.get("target_audience") else None,
        str(resource["resource_id"]) if resource.get("resource_id") else None,
        str(resource["target_org_id"]) if resource.get("target_org_id") else None,
    )


def _mobile_chat_request(req: DeerFlowMobileRunRequest) -> MobileChatRequest:
    """Translate the public mobile request without forwarding arbitrary context."""
    context = _mapping(req.context)
    compliance_context = _mapping(req.compliance_context or context.get("compliance_context"))
    compliance_identity = _mapping(req.compliance_identity or context.get("compliance_identity"))

    if compliance_identity:
        user_context = _mapping(compliance_context.get("user_context"))
        user_context = {**compliance_identity, **user_context}
        compliance_context["user_context"] = user_context
    elif req.user_id and not isinstance(compliance_context.get("user_context"), dict):
        compliance_context["user_context"] = {"user_id": req.user_id}

    compliance_request = _mapping(req.compliance_request or context.get("compliance_request"))
    thread_id = req.thread_id or context.get("thread_id")

    return MobileChatRequest(
        query=req.query,
        user_id=req.user_id,
        session_id=req.conversation_id,
        thread_id=str(thread_id) if thread_id else None,
        assistant_id=req.assistant_id,
        stream=req.stream,
        attachments=_attachments_from_request(req, context),
        compliance_identity=compliance_identity or None,
        compliance_context=compliance_context or None,
        compliance_request=compliance_request or None,
    )


def _build_mobile_run_context(req: MobileChatRequest, thread_id: str) -> dict[str, Any]:
    run_context: dict[str, Any] = {
        **DEFAULT_RUN_CONTEXT,
        "thread_id": thread_id,
    }
    if req.user_id:
        run_context["user_id"] = req.user_id
    if req.session_id:
        run_context["session_id"] = req.session_id
    if req.compliance_context:
        run_context["compliance_context"] = dict(req.compliance_context)
    if req._iam_authenticated:
        from deerflow.agents.memory.scope import build_memory_scope_context

        identity = _mapping(req.compliance_identity)
        run_context["memory_scope"] = build_memory_scope_context(
            org_id=str(identity.get("org_id") or ""),
            user_id=str(identity.get("user_id") or ""),
            thread_id=thread_id,
        )
    return run_context


def _build_mobile_admission_metadata(req: MobileChatRequest, thread_id: str) -> dict[str, Any] | None:
    """Sign scheduler dimensions only after `_prepare_mobile_chat_request` IAM verification."""
    if not req._iam_authenticated:
        return None
    identity = _mapping(req.compliance_identity)
    request_context = _mapping(req.compliance_request)
    operation = _mapping(request_context.get("operation"))
    if not identity:
        return None
    try:
        return build_admission_metadata(
            user_id=str(identity.get("user_id") or ""),
            org_id=str(identity.get("org_id") or ""),
            identity_class=str(identity.get("identity_class") or ""),
            task_type=str(operation.get("type") or ""),
            thread_id=thread_id,
        )
    except (AdmissionSigningConfigurationError, ValueError) as exc:
        logger.error("Unable to sign trusted run admission metadata (%s)", type(exc).__name__)
        raise HTTPException(
            status_code=503,
            detail="Trusted user concurrency admission is unavailable",
        ) from exc


def _mobile_run_metadata(req: MobileChatRequest, thread_id: str) -> dict[str, Any]:
    return {
        **(_build_mobile_admission_metadata(req, thread_id) or {}),
        "mobile_query_sha256": hashlib.sha256(req.query.encode("utf-8")).hexdigest(),
    }


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
    return any(isinstance(msg, dict) and _message_type(msg) == "human" for msg in messages)


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

    has_task_planning = any(line.strip().startswith("任务规划") for line in lines)
    target_title = "任务规划" if has_task_planning else "任务切分"

    in_target_section = False

    for line in lines:
        stripped = line.strip()

        if stripped.startswith(target_title):
            in_target_section = True
            continue

        if in_target_section and any(stripped.startswith(title) for title in section_starters if title != target_title):
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
        return [m for m in messages[start_index + 1 :] if isinstance(m, dict)]

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

            name = str(tool_call.get("name") or tool_call.get("function", {}).get("name") or "")

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

            name = str(tool_call.get("name") or tool_call.get("function", {}).get("name") or "")

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
    if isinstance(pending_action, dict) and _event_has_current_turn_clarification_tool_call(raw_event, current_query):
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

    content = "已生成结果文件：\n" + "\n".join(f"• {item['filename']}\n  {item.get('download_url') or item.get('artifact_url') or ''}" for item in files)

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
        content.replace("<system_reminder>", "")
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


COMPLIANCE_GATES = {"InputGate", "ContextGate", "OutputGate"}
COMPLIANCE_ACTION_PRIORITY = (
    "refuse",
    "manual_review",
    "desensitize",
    "rewrite",
    "aggregate",
    "role_check",
    "warn",
    "allow",
)


def _string_list(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value if str(item)]
    return []


def _final_disposition(actions: list[str]) -> str:
    for action in COMPLIANCE_ACTION_PRIORITY:
        if action in actions:
            return action
    return "allow"


def _normalize_compliance_result(payload: dict[str, Any]) -> dict[str, Any] | None:
    is_retract = payload.get("type") == "compliance_retract"
    gate = "OutputGate" if is_retract else str(payload.get("gate") or "")
    if gate not in COMPLIANCE_GATES:
        return None

    actions = _string_list(payload.get("actions")) or _string_list(payload.get("action"))
    if not actions:
        actions = ["allow"]
    return {
        "gate": gate,
        "scene": str(payload.get("scene") or payload.get("scene_key") or "_unknown"),
        "violation_types": _string_list(payload.get("violation_types")),
        "actions": actions,
        "audit_ref": payload.get("audit_ref"),
        "basis": _string_list(payload.get("basis")),
        "final_disposition": _final_disposition(actions),
    }


def _compliance_result_key(result: dict[str, Any]) -> str:
    return json.dumps(result, ensure_ascii=False, sort_keys=True, default=str)


def _extract_compliance_results(raw_events: list[Any]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    seen: set[str] = set()

    for event in raw_events:
        if not isinstance(event, dict):
            continue
        for payload in _walk_dicts(event.get("data")):
            result = _normalize_compliance_result(payload)
            if result is None:
                continue
            key = _compliance_result_key(result)
            if key in seen:
                continue
            seen.add(key)
            results.append(result)

    return results


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


def _extract_compliance_retract_segments(raw_events: list[Any]) -> list[dict[str, Any]]:
    """Convert OutputGate retractions into mobile feedback segments.

    The replacement is the only user-facing content forwarded from the custom
    event.  The original violating answer is deliberately absent from the
    retraction contract and must never be reconstructed here.
    """
    segments: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    for event in raw_events:
        if not isinstance(event, dict) or event.get("event") != "custom":
            continue

        for payload in _walk_dicts(event.get("data")):
            if payload.get("type") != "compliance_retract":
                continue

            replacement = _extract_text_content(payload.get("replacement"))
            if not replacement:
                replacement = _extract_text_content(payload.get("notice"))
            if not replacement:
                continue

            message_id = _text(payload.get("message_id"))
            key = (message_id, replacement)
            if key in seen:
                continue
            seen.add(key)

            segment = make_segment(
                agent_id="feedback",
                content=replacement,
                status="completed",
                source_event="compliance_retract",
            )
            segment["metadata"].update(
                {
                    "message_id": message_id,
                    "scene": payload.get("scene"),
                    "streaming_mode": payload.get("streaming_mode"),
                    "violation_types": payload.get("violation_types") or [],
                    "action": payload.get("action") or [],
                    "actions": payload.get("actions") or payload.get("action") or [],
                    "notice": payload.get("notice"),
                    "audit_ref": payload.get("audit_ref"),
                    "basis": payload.get("basis") or [],
                }
            )
            segments.append(segment)

    return segments


def _extract_final_answer(raw_events: list[Any], current_query: str, *, completion: dict | None = None) -> str:
    final_answer = ""
    final_status = None
    final_id = None

    def maybe_update_from_messages(messages: list[Any], *, canonical=False) -> None:
        nonlocal final_answer, final_status, final_id

        if canonical:
            final_answer, final_status, final_id = '', None, None

        current_turn_messages = _current_turn_or_incremental_messages(messages, current_query)
        if not current_turn_messages:
            return

        for msg in current_turn_messages:
            if not isinstance(msg, dict):
                continue
            if _message_type(msg) == 'remove' and msg.get('id') == final_id:
                final_answer, final_status, final_id = '', 'needs_verification', None
                continue
            if _message_type(msg) == 'system' and msg.get('name') == 'computation_guard_retry':
                final_status = 'needs_verification'
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
                final_status = (msg.get('response_metadata') or {}).get('execution_status')
                final_id = msg.get('id')
                final_answer = '' if final_status == 'needs_verification' else content

    for event in raw_events:
        if not isinstance(event, dict):
            continue

        event_name = event.get("event")
        data = event.get("data")

        if event_name == "values" and isinstance(data, dict):
            messages = data.get("messages") or data.get("raw_messages") or []
            if isinstance(messages, list):
                maybe_update_from_messages(messages, canonical=bool(data.get('messages')))

        elif event_name == "updates" and isinstance(data, dict):
            for node_value in data.values():
                if not isinstance(node_value, dict):
                    continue

                messages = node_value.get("messages") or node_value.get("raw_messages") or []
                if isinstance(messages, list):
                    maybe_update_from_messages(messages)

    if completion is not None:
        completion['status'] = final_status
    return final_answer.strip()


def adapt_events_to_mobile(
    session_id: str | None,
    thread_id: str,
    raw_events: list[Any],
    current_query: str,
) -> dict[str, Any]:
    errors = _extract_error_segments(raw_events)
    for raw_event in ([] if errors else raw_events):
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
    segments.extend(errors)

    compliance_segments = _extract_compliance_retract_segments(raw_events)
    completion = {}
    if compliance_segments:
        segments.extend(compliance_segments)
    elif not errors:
        final_answer = _extract_final_answer(raw_events, current_query, completion=completion)
        if final_answer:
            failed = completion.get('status') == 'failed'
            segments.append(
                make_segment(
                    agent_id='feedback' if failed else "final",
                    content=final_answer,
                    status='failed' if failed else "completed",
                    source_event='execution_failure' if failed else "final_answer",
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

    result = {
        "session_id": session_id or thread_id,
        "thread_id": thread_id,
        "segments": segments,
        "compliance_results": _extract_compliance_results(raw_events),
    }
    if errors or completion.get('status') == 'failed':
        result['status'] = 'failed'
    elif completion.get('status') == 'needs_verification':
        result['status'] = 'incomplete'
    return result


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
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False, default=str)}\n\n"


def _mobile_compliance_sse_payload(result: dict[str, Any]) -> dict[str, Any]:
    """Keep the canonical fields while supporting OneCity's UI envelope."""
    return {**result, "compliance_result": dict(result)}


def _mobile_run_error_response(
    *,
    conversation_id: str | None,
    thread_id: str | None,
    exc: Exception,
) -> dict[str, Any]:
    status_code, retry_after = _upstream_admission_error(exc)
    if status_code == 429:
        message = "当前用户、任务类别或全局并发名额已满，请稍后重试。"
    else:
        message = f"DeerFlow execution failed: {type(exc).__name__}: {exc}"
    payload = {
        "code": status_code or 500,
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
    if retry_after:
        payload["retry_after"] = retry_after
    return payload


def _upstream_admission_error(exc: Exception) -> tuple[int | None, str | None]:
    """Preserve LangGraph admission responses across the mobile adapter."""
    response = getattr(exc, "response", None)
    status_code = getattr(exc, "status_code", None)
    if not isinstance(status_code, int) and response is not None:
        status_code = getattr(response, "status_code", None)
    if not isinstance(status_code, int) or status_code != 429:
        return None, None

    headers = getattr(response, "headers", None)
    retry_after = headers.get("retry-after") if hasattr(headers, "get") else None
    return 429, str(retry_after).strip() if retry_after else None


def _build_mobile_download_url(thread_id: str, filename: str) -> str | None:
    if not DEERFLOW_PUBLIC_BASE_URL:
        return None

    safe_filename = Path(filename).name
    return f"{DEERFLOW_PUBLIC_BASE_URL}/api/deerflow/mobile/files/{quote(thread_id)}/{quote(safe_filename)}"


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
        raw_path = str(item.get("virtual_path") or item.get("path") or item.get("file_path") or "").strip()

        if not filename and raw_path:
            filename = Path(raw_path).name

        # A side should send virtual_path. This fallback handles accidental
        # backend local paths from the upload response.
        if raw_path and not raw_path.startswith("/mnt/user-data/"):
            marker = "/user-data/"
            if marker in raw_path:
                raw_path = "/mnt" + raw_path[raw_path.index(marker) :]

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


def _build_langgraph_input(
    query: str,
    attachments: Any = None,
    compliance_request: dict[str, Any] | None = None,
) -> dict[str, Any]:
    message: dict[str, Any] = {
        "role": "human",
        "content": query,
    }

    files = _normalize_mobile_attachments(attachments)
    if files:
        message["additional_kwargs"] = {"files": files}

    graph_input: dict[str, Any] = {"messages": [message]}
    if compliance_request:
        graph_input["compliance_request"] = dict(compliance_request)
    return graph_input


async def ensure_mobile_thread(
    req: DeerFlowMobileEnsureThreadRequest,
    *,
    org_id: str | None = None,
    user_id: str | None = None,
) -> tuple[str, bool]:
    """Ensure a persistent conversation binding and recover upstream creates.

    ``org_id`` and ``user_id`` are passed only from the verified IAM principal.
    Caller metadata cannot select ownership or the thread ID. SQLite reserves
    the ID before the network request and releases its transaction immediately.
    """
    from langgraph_sdk import get_client

    conversation_id = str(req.conversation_id or "").strip()
    if not conversation_id:
        raise ValueError("conversation_id is required")
    authenticated = org_id is not None
    if authenticated and (not org_id.strip() or not user_id or not user_id.strip()):
        raise HTTPException(status_code=503, detail="Verified thread ownership is unavailable")
    owner_org_id = org_id if authenticated else ""
    owner_user_id = user_id if authenticated else req.user_id or ""
    try:
        thread_id = await asyncio.to_thread(
            get_or_create_conversation_thread,
            org_id=owner_org_id,
            user_id=owner_user_id,
            conversation_id=conversation_id,
        )
    except Exception as exc:
        logger.error("Mobile conversation store is unavailable (%s)", type(exc).__name__)
        raise HTTPException(status_code=503, detail="Conversation store is unavailable") from exc

    metadata = dict(req.metadata or {})
    for key in ("org_id", "user_id", "owner_user_id", "owner_org_id", "tenant_id", "conversation_id"):
        metadata.pop(key, None)
    metadata.update(
        conversation_id=conversation_id,
        source="trusted_upstream" if authenticated else req.source or "citybrain",
        org_id=owner_org_id,
        user_id=owner_user_id,
    )

    def validate_thread(thread: Any) -> None:
        if not isinstance(thread, dict) or str(thread.get("thread_id") or "") != thread_id:
            raise HTTPException(status_code=503, detail="Thread service returned invalid data")
        if authenticated:
            actual = thread.get("metadata")
            if not isinstance(actual, dict) or any(
                actual.get(key) != value
                for key, value in (("org_id", owner_org_id), ("user_id", owner_user_id), ("conversation_id", conversation_id))
            ):
                raise HTTPException(status_code=404, detail="Thread not found")

    try:
        client = get_client(url=DEFAULT_LANGGRAPH_URL, headers=internal_graph_headers())
        try:
            thread = await client.threads.get(thread_id)
        except Exception as exc:
            if thread_error_status(exc) != 404:
                raise
        else:
            validate_thread(thread)
            return thread_id, False

        try:
            thread = await client.threads.create(thread_id=thread_id, metadata=metadata, if_exists="raise")
        except Exception as exc:
            if thread_error_status(exc) != 409:
                raise
            # Another request created the reserved thread after our lookup.
            thread = await client.threads.get(thread_id)
            validate_thread(thread)
            return thread_id, False
        validate_thread(thread)
        return thread_id, True
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Mobile thread service is unavailable (%s)", type(exc).__name__)
        raise HTTPException(status_code=503, detail="Thread service is unavailable") from exc


async def _resolve_mobile_thread(req: MobileChatRequest, client: Any) -> str:
    """Reuse verified preparation or the development caller's explicit thread."""
    if req.thread_id:
        return req.thread_id
    if req._iam_authenticated:
        identity = _mapping(req.compliance_identity)
        thread_id, _ = await ensure_mobile_thread(
            DeerFlowMobileEnsureThreadRequest(conversation_id=req.session_id or str(uuid4())),
            org_id=str(identity.get("org_id") or ""),
            user_id=str(identity.get("user_id") or ""),
        )
        return thread_id
    if req.session_id:
        thread_id, _ = await ensure_mobile_thread(
            DeerFlowMobileEnsureThreadRequest(conversation_id=req.session_id, user_id=req.user_id)
        )
        return thread_id
    thread = await client.threads.create()
    return str(thread["thread_id"])


async def _prepare_mobile_chat_request(chat_req: MobileChatRequest, request: Request) -> MobileChatRequest:
    """Replace caller identity claims with a server-verified IAM decision."""
    provider, principal = await authenticate_mobile_request(request)
    if provider is None or principal is None:
        # Caller-supplied compliance_request is never an authorization proof.
        # Manual UI context may remain for diagnostics; trusted_upstream
        # requires a provider and otherwise uses the conservative fallback.
        chat_req.compliance_identity = None
        chat_req.compliance_request = None
        chat_req._iam_authenticated = False
        return chat_req

    try:
        ensure_admission_signing_available()
    except AdmissionSigningConfigurationError as exc:
        logger.error("Authenticated mobile concurrency signing is not configured")
        raise HTTPException(
            status_code=503,
            detail="Trusted user concurrency admission is unavailable",
        ) from exc

    operation_type, target_audience, resource_id, target_org_id = _mobile_authorization_hints(chat_req)
    if not operation_type or not target_audience or not resource_id:
        raise HTTPException(
            status_code=422,
            detail="IAM authorization requires operation type, target audience, and resource ID",
        )
    request_id = str(uuid4())
    turn_id = str(uuid4())
    conversation_id = chat_req.session_id or str(uuid4())
    decision = await authorize_mobile_request(
        provider,
        principal,
        MobileAuthorizationInput(
            request_id=request_id,
            turn_id=turn_id,
            conversation_id=conversation_id,
            operation_type=operation_type,
            target_audience=target_audience,
            resource_id=resource_id,
            target_org_id=target_org_id,
        ),
    )

    # Caller-supplied thread IDs never select an authenticated conversation.
    thread_id, _ = await ensure_mobile_thread(
        DeerFlowMobileEnsureThreadRequest(conversation_id=conversation_id, source="trusted_upstream"),
        org_id=principal.org_id,
        user_id=principal.user_id,
    )

    chat_req.session_id = conversation_id
    chat_req.thread_id = thread_id
    chat_req.user_id = principal.user_id
    chat_req.compliance_identity = principal.model_dump(mode="json")
    chat_req.compliance_context = {"user_context": principal.model_dump(mode="json")}
    chat_req.compliance_request = build_trusted_compliance_request(
        principal=principal,
        decision=decision,
        request_id=request_id,
        thread_id=thread_id,
        turn_id=turn_id,
    )
    chat_req._iam_authenticated = True
    return chat_req


async def run_deerflow_and_collect_events(req: MobileChatRequest) -> tuple[str, list[Any]]:
    """Call LangGraph Server and collect current-run events.

    Compliance-safe mode omits token and raw-event streams. Only state snapshots
    are forwarded; the OutputGate rewrites the answer before it enters state.
    """
    from langgraph_sdk import get_client

    client = get_client(url=DEFAULT_LANGGRAPH_URL, headers=internal_graph_headers())
    thread_id = await _resolve_mobile_thread(req, client)

    assistant_id = req.assistant_id or DEFAULT_ASSISTANT_ID

    run_context = _build_mobile_run_context(req, thread_id)

    raw_events: list[Any] = []

    async for chunk in client.runs.stream(
        thread_id,
        assistant_id,
        input=_build_langgraph_input(req.query, req.attachments, req.compliance_request),
        metadata=_mobile_run_metadata(req, thread_id),
        config=DEFAULT_RUN_CONFIG,
        context=run_context,
        # Do not forward token chunks before OutputGate has checked the complete
        # State snapshots contain the OutputGate-checked answer. Do not forward
        # token, raw-event, or arbitrary custom payloads.
        stream_mode=["updates", "values"],
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

    status_task: asyncio.Task | None = None
    admitted_run_id: str | None = chat_req._resume_run_id
    try:
        client = get_client(url=DEFAULT_LANGGRAPH_URL, headers=internal_graph_headers())
        thread_id = await _resolve_mobile_thread(chat_req, client)

        assistant_id = chat_req.assistant_id or DEFAULT_ASSISTANT_ID

        run_context = _build_mobile_run_context(chat_req, thread_id)

        if chat_req._resume_run_id:
            initial_state = await _resume_task_state(client, thread_id, chat_req._resume_run_id, chat_req.query)
        else:
            try:
                initial_state = await client.threads.get_state(thread_id)
            except Exception:
                initial_state = {}
        initial_checkpoint_id = _checkpoint_id(initial_state)

        await queue.put(
            {
                "kind": "start",
                "thread_id": thread_id,
                "initial_checkpoint_id": initial_checkpoint_id,
            }
        )

        def on_run_created(metadata: dict[str, Any]) -> None:
            nonlocal status_task, admitted_run_id
            run_id = metadata.get("run_id")
            if run_id:
                admitted_run_id = str(run_id)
            if run_id and status_task is None:
                status_task = asyncio.create_task(
                    _produce_mobile_run_status(client, thread_id, str(run_id), queue)
                )

        if chat_req._resume_run_id:
            on_run_created({"run_id": chat_req._resume_run_id})
            run = await client.runs.get(thread_id, chat_req._resume_run_id)
            if run.get("status") in {"success", "error", "timeout", "interrupted"}:
                await queue.put({"kind": "chunk", "raw_event": {"event": "values", "data": initial_state.get("values", {})}})
                await queue.put({"kind": "done", "run_status": run.get("status"), "run_id": admitted_run_id})
                return
            source = client.runs.join_stream(
                thread_id, chat_req._resume_run_id,
                stream_mode=["updates", "values"], cancel_on_disconnect=False,
            )
        else:
            source = client.runs.stream(
                thread_id,
                assistant_id,
                input=_build_langgraph_input(chat_req.query, chat_req.attachments, chat_req.compliance_request),
                metadata=_mobile_run_metadata(chat_req, thread_id),
                config=DEFAULT_RUN_CONFIG,
                context=run_context,
                stream_mode=["updates", "values"],
                on_disconnect="continue",
                on_run_created=on_run_created,
            )
        async for chunk in source:
            event_name = getattr(chunk, "event", "")
            event_data = getattr(chunk, "data", None)
            if event_name == "metadata" and isinstance(event_data, dict) and event_data.get("run_id") and status_task is None:
                on_run_created(event_data)
            await queue.put(
                {
                    "kind": "chunk",
                    "raw_event": {
                        "event": getattr(chunk, "event", ""),
                        "data": getattr(chunk, "data", None),
                    },
                }
            )

        run_status = None
        if chat_req._resume_run_id:
            state = await _resume_task_state(client, thread_id, chat_req._resume_run_id, chat_req.query)
            await queue.put({"kind": "chunk", "raw_event": {"event": "values", "data": state.get("values", {})}})
        if admitted_run_id:
            try:
                run_status = (await client.runs.get(thread_id, admitted_run_id)).get("status")
            except Exception:
                pass
        await queue.put({"kind": "done", "run_status": run_status, "run_id": admitted_run_id})

    except Exception as exc:
        status_code, retry_after = _upstream_admission_error(exc)
        message = (
            "当前用户、任务类别或全局并发名额已满，请稍后重试。"
            if status_code == 429
            else "任务服务暂时不可用，请稍后重试。"
        )
        await queue.put(
            {
                "kind": "error",
                "error_type": type(exc).__name__,
                "message": message,
                "status_code": status_code or 500,
                "retry_after": retry_after,
            }
        )
    finally:
        if status_task is not None:
            status_task.cancel()
            await asyncio.gather(status_task, return_exceptions=True)


async def _produce_mobile_run_status(client: Any, thread_id: str, run_id: str, queue: asyncio.Queue) -> None:
    """Report the exact submitted run's queue state without guessing from SSE."""
    previous = None
    while True:
        try:
            run = await client.runs.get(thread_id, run_id)
        except Exception:
            # Polling is supplementary; a temporary lookup failure must not
            # interrupt the actual stream or assert that execution has begun.
            await asyncio.sleep(MOBILE_RUN_STATUS_POLL_SECONDS)
            continue
        status = run.get("status") if isinstance(run, dict) else None
        if status in {"pending", "running"} and status != previous:
            await queue.put({"kind": "status", "thread_id": thread_id, "run_id": run_id, "status": status})
            previous = status
        elif status in {"success", "error", "timeout", "interrupted"}:
            return
        await asyncio.sleep(MOBILE_RUN_STATUS_POLL_SECONDS)


def _checkpoint_id(state: Any) -> str:
    if not isinstance(state, dict):
        return ""
    direct = state.get("checkpoint_id")
    if direct:
        return str(direct)
    checkpoint = state.get("checkpoint")
    if isinstance(checkpoint, dict) and checkpoint.get("checkpoint_id"):
        return str(checkpoint["checkpoint_id"])
    return ""


def _terminal_state_event(
    state: Any,
    *,
    current_query: str,
    initial_checkpoint_id: str,
) -> dict[str, Any] | None:
    """Convert a newly completed current-turn state into a synthetic values event.

    Some LangGraph SDK streams remain open after the run has committed its final
    checkpoint.  The checkpoint and current-query guards prevent an old thread
    state from being mistaken for completion of the active mobile request.
    """
    if not isinstance(state, dict):
        return None
    next_steps = state.get("next")
    if not isinstance(next_steps, (list, tuple)) or next_steps:
        return None

    checkpoint_id = _checkpoint_id(state)
    if not checkpoint_id or checkpoint_id == initial_checkpoint_id:
        return None

    values = state.get("values")
    if not isinstance(values, dict):
        return None
    messages = values.get("messages")
    if not isinstance(messages, list):
        return None
    current_turn = _slice_messages_after_current_query(messages, current_query)
    if not any(_message_type(message) == "ai" for message in current_turn):
        return None

    return {"event": "values", "data": values}


async def _load_terminal_state_event(
    thread_id: str,
    *,
    run_id: str | None = None,
    current_query: str,
    initial_checkpoint_id: str,
) -> dict[str, Any] | None:
    from langgraph_sdk import get_client

    if not thread_id or not run_id:
        return None
    try:
        client = get_client(url=DEFAULT_LANGGRAPH_URL, headers=internal_graph_headers())
        run = await client.runs.get(thread_id, run_id)
        if run.get("status") != "success":
            return None
        state = await client.threads.get_state(thread_id)
    except Exception:
        return None
    return _terminal_state_event(
        state,
        current_query=current_query,
        initial_checkpoint_id=initial_checkpoint_id,
    )


async def _stream_deerflow_mobile_run(
    req: DeerFlowMobileRunRequest,
    prepared_chat_req: MobileChatRequest | None = None,
) -> AsyncIterator[str]:
    chat_req = prepared_chat_req or _mobile_chat_request(req)
    thread_id = chat_req.thread_id

    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    producer_task = asyncio.create_task(_produce_langgraph_stream(chat_req, queue))

    raw_events: list[Any] = []
    current_thread_id = thread_id or ""
    current_run_status = "submitted"
    current_run_id = chat_req._resume_run_id
    initial_checkpoint_id = ""
    emitted_keys: set[tuple[str, str, str]] = set()
    emitted_segments: list[dict[str, Any]] = []
    emitted_compliance_keys: set[str] = set()
    compliance_results: list[dict[str, Any]] = []
    next_order = 1
    clarification_emitted = False
    pending_clarification: dict[str, Any] | None = None

    def emit_new_compliance_results() -> list[dict[str, Any]]:
        new_items: list[dict[str, Any]] = []
        for item in _extract_compliance_results(raw_events):
            key = _compliance_result_key(item)
            if key in emitted_compliance_keys:
                continue
            emitted_compliance_keys.add(key)
            compliance_results.append(item)
            new_items.append(item)
        return new_items

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

        ordered = _segments_to_ordered_segments(adapted.get("segments") if isinstance(adapted, dict) else [])

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
                "status": "submitted",
            },
        )

        loop = asyncio.get_running_loop()
        next_terminal_check = loop.time() + MOBILE_STREAM_HEARTBEAT_SECONDS

        while True:
            check_timeout = max(0.001, next_terminal_check - loop.time())
            try:
                item = await asyncio.wait_for(queue.get(), timeout=check_timeout)
            except TimeoutError:
                next_terminal_check = loop.time() + MOBILE_STREAM_HEARTBEAT_SECONDS
                terminal_event = await _load_terminal_state_event(
                    current_thread_id,
                    run_id=current_run_id,
                    current_query=req.query,
                    initial_checkpoint_id=initial_checkpoint_id,
                )
                if terminal_event is None:
                    # 心跳非常重要：防止 A 侧 requests 读超时
                    yield _sse_event(
                        "heartbeat",
                        {
                            "type": "heartbeat",
                            "conversation_id": req.conversation_id,
                            "thread_id": current_thread_id,
                            "status": current_run_status,
                        },
                    )
                    continue

                raw_events.append(terminal_event)
                for compliance_result in emit_new_compliance_results():
                    yield _sse_event(
                        "compliance",
                        _mobile_compliance_sse_payload(compliance_result),
                    )
                if not clarification_emitted:
                    for segment in emit_new_segments(final=False):
                        yield _sse_event("segment", segment)
                item = {"kind": "done"}

            kind = item.get("kind")

            if kind == "start":
                current_thread_id = str(item.get("thread_id") or "")
                initial_checkpoint_id = str(item.get("initial_checkpoint_id") or "")
                yield _sse_event(
                    "start",
                    {
                        "code": 0,
                        "message": "preparing",
                        "conversation_id": req.conversation_id,
                        "thread_id": current_thread_id,
                        "status": "submitted",
                    },
                )

            elif kind == "status":
                current_run_status = str(item["status"])
                current_run_id = str(item["run_id"])
                yield _sse_event(
                    "status",
                    {
                        "code": 0,
                        "conversation_id": req.conversation_id,
                        "thread_id": current_thread_id,
                        "run_id": item["run_id"],
                        "status": current_run_status,
                    },
                )

            elif kind == "chunk":
                raw_event = item.get("raw_event")
                if isinstance(raw_event, dict):
                    raw_events.append(raw_event)

                    for compliance_result in emit_new_compliance_results():
                        yield _sse_event(
                            "compliance",
                            _mobile_compliance_sse_payload(compliance_result),
                        )

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
                error_payload = {
                    "code": item.get("status_code") or 500,
                    "message": (
                        item.get("message")
                        if item.get("status_code") == 429
                        else f"{item.get('error_type')}: {item.get('message')}"
                    ),
                    "conversation_id": req.conversation_id,
                    "thread_id": current_thread_id,
                    "status": "failed",
                }
                if item.get("retry_after"):
                    error_payload["retry_after"] = item["retry_after"]
                yield _sse_event("error", error_payload)

                yield "data: [DONE]\n\n"

                break

            elif kind == "done":
                # The run can finish before supplementary status polling ever
                # observes pending/running. Preserve its admitted identity in
                # the terminal frame so clients can verify or resume that run.
                current_run_id = item.get("run_id") or current_run_id
                adapted = adapt_events_to_mobile(
                    session_id=req.conversation_id, thread_id=current_thread_id,
                    raw_events=raw_events, current_query=req.query,
                )
                terminal_status = adapted.get("status") or "completed"
                if item.get("run_status") in {"error", "timeout"}:
                    terminal_status = "failed"

                if pending_clarification and terminal_status not in {"failed", "incomplete", "cancelled"}:
                    yield _sse_event("clarification", pending_clarification)
                    # A clarification is a successful, terminal turn from the
                    # transport's perspective.  It must still have the same
                    # structured terminal event as normal completion; a bare
                    # [DONE] leaves clients unable to distinguish completion
                    # from a truncated stream.
                    yield _sse_event(
                        "done",
                        {
                            "code": 0,
                            "message": "clarification_required",
                            "conversation_id": req.conversation_id,
                            "thread_id": current_thread_id,
                            "run_id": current_run_id,
                            "status": "awaiting_clarification",
                        },
                    )
                    yield "data: [DONE]\n\n"
                    break

                for segment in emit_new_segments(final=True):
                    yield _sse_event("segment", segment)

                if item.get("run_status") == "interrupted":
                    terminal_status = "cancelled"
                successful = terminal_status == "completed"

                yield _sse_event(
                    "done",
                    {
                        "code": 0 if successful else 409 if terminal_status == "cancelled" else 500,
                        "message": "success" if successful else terminal_status,
                        "conversation_id": req.conversation_id,
                        "thread_id": current_thread_id,
                        "run_id": current_run_id,
                        "status": terminal_status,
                        "ordered_segments": emitted_segments,
                        "compliance_results": compliance_results,
                    },
                )

                yield "data: [DONE]\n\n"

                break

    finally:
        if not producer_task.done():
            producer_task.cancel()
        await asyncio.gather(producer_task, return_exceptions=True)


async def _mobile_owned_run(thread_id: str, run_id: str, request: Request):
    """Recheck current IAM grants before using the private native client."""
    from langgraph_sdk import get_client

    await require_thread_access(request, thread_id, operation_type="analysis")
    client = get_client(url=DEFAULT_LANGGRAPH_URL, headers=internal_graph_headers())
    try:
        run = await client.runs.get(thread_id, run_id)
    except Exception as exc:
        status = 404 if thread_error_status(exc) == 404 else 503
        raise HTTPException(status, "Task not found" if status == 404 else "Task service is unavailable") from exc
    return client, run


async def _resume_task_state(client: Any, thread_id: str, run_id: str, query: str) -> dict[str, Any]:
    """Bind replay to the admitted query and reject a newer turn during reads."""
    run = await client.runs.get(thread_id, run_id)
    query_hash = _mapping(run.get("metadata")).get("mobile_query_sha256")
    if query_hash is not None and query_hash != hashlib.sha256(query.encode("utf-8")).hexdigest():
        raise HTTPException(409, "The task query does not match the admitted request")
    for_state = await client.runs.list(thread_id, limit=1)
    if not for_state or for_state[0].get("run_id") != run_id:
        raise HTTPException(409, "Only the current conversation task can be resumed")
    state = await client.threads.get_state(thread_id)
    confirmed = await client.runs.list(thread_id, limit=1)
    if not confirmed or confirmed[0].get("run_id") != run_id:
        raise HTTPException(409, "The conversation advanced while restoring the task")
    if query_hash is None or run.get("status") in {"success", "error", "timeout", "interrupted"}:
        messages = _mapping(state.get("values")).get("messages") or []
        last_query = next((
            message for message in reversed(messages)
            if isinstance(message, dict) and _message_type(message) == "human"
            and message.get("name") not in {"routing_skill_guidance", "todo_routing_guidance", "todo_reminder"}
            and not _mapping(message.get("additional_kwargs")).get("internal")
        ), None)
        if last_query is None or _extract_text_content(last_query.get("content")) != query.strip():
            raise HTTPException(409, "The current task query could not be verified")
    return state


def _mobile_run_status(thread_id: str, run_id: str, run: dict) -> dict[str, Any]:
    return {"code": 0, "data": {"thread_id": thread_id, "run_id": run_id, "status": run.get("status")}}


@router.get("/api/deerflow/mobile/runs/{thread_id}/{run_id}")
async def mobile_run_status(thread_id: UUID, run_id: UUID, request: Request):
    _, run = await _mobile_owned_run(str(thread_id), str(run_id), request)
    return _mobile_run_status(str(thread_id), str(run_id), run)


@router.post("/api/deerflow/mobile/runs/{thread_id}/{run_id}/cancel")
async def mobile_cancel_run(thread_id: UUID, run_id: UUID, request: Request):
    thread, task = str(thread_id), str(run_id)
    client, run = await _mobile_owned_run(thread, task, request)
    try:
        if run.get("status") in {"pending", "running"}:
            await client.runs.cancel(thread, task, wait=True, action="interrupt")
        run = await client.runs.get(thread, task)
    except Exception as exc:
        raise HTTPException(503, "Task cancellation could not be confirmed") from exc
    if run.get("status") not in {"success", "error", "timeout", "interrupted"}:
        raise HTTPException(503, "Task cancellation could not be confirmed")
    return _mobile_run_status(thread, task, run)


@router.get("/api/deerflow/mobile/runs/{thread_id}/{run_id}/stream")
async def mobile_resume_run(thread_id: UUID, run_id: UUID, request: Request, conversation_id: str, query: str):
    thread, task = str(thread_id), str(run_id)
    client, _ = await _mobile_owned_run(thread, task, request)
    try:
        metadata = (await client.threads.get(thread)).get("metadata", {})
        await _resume_task_state(client, thread, task, query)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(503, "Task service is unavailable") from exc
    if metadata.get("conversation_id") != conversation_id:
        raise HTTPException(409, "Only the current conversation task can be resumed")
    prepared = MobileChatRequest(query=query, session_id=conversation_id, thread_id=thread)
    prepared._resume_run_id = task
    req = DeerFlowMobileRunRequest(query=query, conversation_id=conversation_id, thread_id=thread, stream=True)
    return StreamingResponse(
        _stream_deerflow_mobile_run(req, prepared), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/api/deerflow/mobile/threads/ensure")
async def deerflow_mobile_ensure_thread(req: DeerFlowMobileEnsureThreadRequest, request: Request) -> dict[str, Any]:
    try:
        provider, principal = await authenticate_mobile_request(request)
        if principal is not None:
            if provider is None:
                raise HTTPException(status_code=503, detail="Mobile IAM provider is unavailable")
            req.user_id = principal.user_id
            await authorize_mobile_request(
                provider,
                principal,
                MobileAuthorizationInput(
                    request_id=str(uuid4()),
                    turn_id=str(uuid4()),
                    conversation_id=req.conversation_id,
                    operation_type="analysis",
                    target_audience="self",
                    resource_id=req.conversation_id,
                ),
            )
        thread_id, created = await ensure_mobile_thread(
            req,
            org_id=principal.org_id if principal is not None else None,
            user_id=principal.user_id if principal is not None else None,
        )
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
        if isinstance(exc, HTTPException):
            raise
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
async def mobile_chat(req: MobileChatRequest, request: Request) -> dict[str, Any]:
    chat_req = await _prepare_mobile_chat_request(req, request)
    try:
        thread_id, raw_events = await run_deerflow_and_collect_events(chat_req)
        return adapt_events_to_mobile(
            session_id=chat_req.session_id,
            thread_id=thread_id,
            raw_events=raw_events,
            current_query=chat_req.query,
        )
    except Exception as exc:
        status_code, retry_after = _upstream_admission_error(exc)
        message = (
            "当前用户、任务类别或全局并发名额已满，请稍后重试。"
            if status_code == 429
            else f"DeerFlow mobile adapter 调用失败：{type(exc).__name__}: {exc}"
        )
        result = {
            "code": status_code or 500,
            "message": message,
            "session_id": req.session_id,
            "thread_id": req.thread_id,
            "segments": [
                make_segment(
                    agent_id="system",
                    content=message,
                    status="failed",
                    source_event="exception",
                )
            ],
        }
        if retry_after:
            result["retry_after"] = retry_after
        return result


@router.post("/api/deerflow/mobile/run")
async def deerflow_mobile_run(req: DeerFlowMobileRunRequest, request: Request):
    chat_req = await _prepare_mobile_chat_request(_mobile_chat_request(req), request)
    if req.stream:
        return StreamingResponse(
            _stream_deerflow_mobile_run(req, chat_req),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    thread_id = chat_req.thread_id

    try:
        deerflow_thread_id, raw_events = await run_deerflow_and_collect_events(chat_req)
        adapted = adapt_events_to_mobile(
            session_id=chat_req.session_id,
            thread_id=deerflow_thread_id,
            raw_events=raw_events,
            current_query=req.query,
        )

        ordered_segments = _segments_to_ordered_segments(adapted.get("segments") if isinstance(adapted, dict) else [])
        terminal_status = adapted.get("status") or "completed"
        successful = terminal_status == "completed"

        return {
            "code": 0 if successful else 500,
            "message": "success" if successful else terminal_status,
            "status": terminal_status,
            "conversation_id": chat_req.session_id,
            "thread_id": deerflow_thread_id,
            "ordered_segments": ordered_segments,
            "compliance_results": adapted.get("compliance_results", []),
        }

    except Exception as exc:
        return _mobile_run_error_response(
            conversation_id=chat_req.session_id,
            thread_id=thread_id,
            exc=exc,
        )


@router.get("/api/deerflow/mobile/files/{thread_id}/{filename:path}")
async def deerflow_mobile_download_file(thread_id: str, filename: str, request: Request):
    await require_thread_access(request, thread_id, operation_type="analysis")

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
