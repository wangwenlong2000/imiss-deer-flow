"""Middleware that extends TodoListMiddleware with context-loss detection.

When the message history is truncated (e.g., by SummarizationMiddleware), the
original `write_todos` tool call and its ToolMessage can be scrolled out of the
active context window. This middleware detects that situation and injects a
reminder message so the model still knows about the outstanding todo list.
"""

from __future__ import annotations

from typing import Any

try:
    from typing import override
except ImportError:
    from typing_extensions import override

from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware import TodoListMiddleware
from langchain.agents.middleware.todo import PlanningState, Todo
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.runtime import Runtime


def _todos_in_messages(messages: list[Any]) -> bool:
    """Return True if any AIMessage in *messages* contains a write_todos tool call."""
    for msg in messages:
        if isinstance(msg, AIMessage) and msg.tool_calls:
            for tc in msg.tool_calls:
                if tc.get("name") == "write_todos":
                    return True
    return False


def _reminder_in_messages(messages: list[Any]) -> bool:
    """Return True if a todo_reminder HumanMessage is already present in *messages*."""
    for msg in messages:
        if isinstance(msg, HumanMessage) and getattr(msg, "name", None) == "todo_reminder":
            return True
    return False


def _routing_guidance_in_messages(messages: list[Any]) -> bool:
    """Return True if the current turn already has routing guidance.

    Older turns may still contain a ``todo_routing_guidance`` message because
    LangGraph's message reducer appends/merges by id instead of deleting omitted
    messages. Only guidance after the latest real user message should block
    injecting a new current-turn hidden step.
    """
    latest_user_index = -1
    for index, msg in enumerate(messages):
        if (
            isinstance(msg, HumanMessage)
            and not _is_internal_human_message(msg)
        ):
            latest_user_index = index

    for msg in messages[latest_user_index + 1:]:
        if isinstance(msg, HumanMessage) and getattr(msg, "name", None) == "todo_routing_guidance":
            return True
    return False


def _is_internal_human_message(message: HumanMessage) -> bool:
    additional_kwargs = getattr(message, "additional_kwargs", {}) or {}
    if isinstance(additional_kwargs, dict):
        if additional_kwargs.get("internal") is True:
            return True
        if additional_kwargs.get("message_type") in {"view_image_context", "routed_skill_prompt"}:
            return True
    name = getattr(message, "name", None)
    return isinstance(name, str) and (
        name.endswith("_guidance")
        or name in {"todo_reminder", "routing_skill_guidance", "todo_routing_guidance"}
    )


def _should_suppress_routing_hidden_steps(state: PlanningState) -> bool:
    intent_ctx = state.get("intent_context")
    if isinstance(intent_ctx, dict) and intent_ctx.get("_suppress_hidden_steps") is True:
        return True

    routing_ctx = state.get("routing_context")
    if isinstance(routing_ctx, dict) and routing_ctx.get("_suppress_hidden_steps") is True:
        return True

    dialogue_ctx = state.get("dialogue_context")
    return isinstance(dialogue_ctx, dict) and dialogue_ctx.get("act") in {"clarification_answer", "parameter_update", "task_followup"}


def _format_todos(todos: list[Todo]) -> str:
    """Format a list of Todo items into a human-readable string."""
    lines: list[str] = []
    for todo in todos:
        status = todo.get("status", "pending")
        content = todo.get("content", "")
        lines.append(f"- [{status}] {content}")
    return "\n".join(lines)


def _format_intent_guidance(intent_ctx: dict[str, Any]) -> str:
    def _compact_intent_routing_query(text: str) -> str:
        value = str(text or "").strip()
        if not value:
            return value
        marker = "用户原始问题："
        if marker in value:
            value = value.split(marker, 1)[0].strip().rstrip("。")
        if value.startswith("场景："):
            for split_token in ("。场景说明：", "。需要识别参数："):
                if split_token in value:
                    return value.split(split_token, 1)[0].strip()
        return value

    lines: list[str] = []
    routing_query = intent_ctx.get("routing_query")
    if routing_query:
        lines.append(f"改写后的任务：{_compact_intent_routing_query(str(routing_query))}")
    scene_ids = [
        str(scene).strip()
        for scene in (intent_ctx.get("scenes") or [])
        if isinstance(scene, str) and scene.strip()
    ]
    scene_mode = intent_ctx.get("scene_mode")
    primary_scene_name = intent_ctx.get("scene_name") or intent_ctx.get("scene")
    primary_scene_id = intent_ctx.get("scene")
    if primary_scene_name or primary_scene_id:
        scene_line = f"识别场景：{primary_scene_name}" + (f" ({primary_scene_id})" if primary_scene_id else "")
        if scene_mode:
            scene_line += f"；场景模式：{scene_mode}"
        if scene_ids:
            scene_line += f"；候选场景：{', '.join(scene_ids)}"
        lines.append(scene_line)
    task_spans = intent_ctx.get("task_spans") or []
    if isinstance(task_spans, list) and task_spans:
        lines.append("任务切分：")
        for span in task_spans:
            if not isinstance(span, dict):
                continue
            text = span.get("text") or span.get("task_text") or ""
            if isinstance(text, str) and text.strip():
                lines.append(f"- {text.strip()}")
    scene_tasks = intent_ctx.get("scene_tasks") or []
    if isinstance(scene_tasks, list) and scene_tasks:
        lines.append("场景任务：")
        for st in scene_tasks:
            if not isinstance(st, dict):
                continue
            scene = st.get("scene")
            text = st.get("text") or st.get("task_text") or ""
            params = st.get("params")
            line = f"- {scene}: {text}".strip()
            if params:
                line += f"；参数：{params}"
            lines.append(line)
    params = intent_ctx.get("params")
    if params:
        lines.append(f"已提取参数：{params}")
    hints = intent_ctx.get("task_hints") or []
    if hints:
        lines.append("任务提示：")
        lines.extend(f"- {hint}" for hint in hints)
    return "\n".join(lines).strip()


def _format_routing_guidance(routing_ctx: dict[str, Any]) -> str:
    def _compact_segment_text(text: str) -> str:
        value = text.strip()
        if not value:
            return value
        marker = "用户原始问题："
        if marker in value:
            value = value.split(marker, 1)[0].strip().rstrip("。")
        if value.startswith("场景："):
            for split_token in ("。场景说明：", "。需要识别参数："):
                if split_token in value:
                    return value.split(split_token, 1)[0].strip()
        return value

    lines: list[str] = []
    if routing_ctx.get("route_reason"):
        lines.append(f"路由原因：{routing_ctx.get('route_reason')}")
    if routing_ctx.get("primary_goal"):
        lines.append(f"主要目标：{_compact_segment_text(str(routing_ctx.get('primary_goal')))}")
    scene_tasks = routing_ctx.get("scene_tasks") or []
    for st in scene_tasks:
        seg = _compact_segment_text(st.get("segment_text", ""))
        scene = st.get("scene")
        skills = st.get("selected_skills", [])
        recommended = []
        for s in skills:
            skill_id = s.get("id", "")
            if skill_id and skill_id not in recommended:
                recommended.append(skill_id)
        line = f"- {seg}"
        if scene:
            line += f"；场景：{scene}"
        if recommended:
            line += f"；推荐 skill：{', '.join(recommended)}"
        lines.append(line)
    selected = routing_ctx.get("global_selected_skills") or []
    if selected:
        lines.append(f"本轮可用 skills：{', '.join(selected)}")
    return "\n".join(lines).strip()


def _build_routing_guidance_message(state: PlanningState) -> HumanMessage | None:
    intent_ctx = state.get("intent_context")
    routing_ctx = state.get("routing_context")
    if not isinstance(intent_ctx, dict):
        intent_ctx = {}
    if not isinstance(routing_ctx, dict):
        routing_ctx = {}

    intent_text = _format_intent_guidance(intent_ctx)
    routing_text = "" if routing_ctx.get("route_mode") == "scene_filter" else _format_routing_guidance(routing_ctx)
    if not intent_text and not routing_text:
        return None

    blocks: list[str] = [
        "<system_reminder>",
        "Use the following current-turn intent and routing guidance when deciding whether to create or update todos. Treat it as planning context, not as a user request.",
    ]
    if intent_text:
        blocks.append(
            '<hidden_step source="intent_recognition" title="意图识别">\n'
            f"{intent_text}\n"
            "</hidden_step>"
        )
    if routing_text:
        blocks.append(
            '<hidden_step source="skill_router" title="SkillRouter 路由">\n'
            f"{routing_text}\n"
            "</hidden_step>"
        )
    blocks.append("</system_reminder>")
    return HumanMessage(name="todo_routing_guidance", content="\n".join(blocks))


class RoutingHiddenStepMiddleware(AgentMiddleware[PlanningState]):
    """Inject current-turn intent/routing hidden steps without enabling todos."""

    state_schema = PlanningState

    @override
    def before_model(
        self,
        state: PlanningState,
        runtime: Runtime,  # noqa: ARG002
    ) -> dict[str, Any] | None:
        messages = state.get("messages") or []
        if _should_suppress_routing_hidden_steps(state) or _routing_guidance_in_messages(messages):
            return None

        routing_guidance = _build_routing_guidance_message(state)
        if routing_guidance is None:
            return None
        return {"messages": [routing_guidance]}

    @override
    async def abefore_model(
        self,
        state: PlanningState,
        runtime: Runtime,
    ) -> dict[str, Any] | None:
        return self.before_model(state, runtime)


class TodoMiddleware(TodoListMiddleware):
    """Extends TodoListMiddleware with `write_todos` context-loss detection.

    When the original `write_todos` tool call has been truncated from the message
    history (e.g., after summarization), the model loses awareness of the current
    todo list. This middleware detects that gap in `before_model` / `abefore_model`
    and injects a reminder message so the model can continue tracking progress.
    """

    @override
    def before_model(
        self,
        state: PlanningState,
        runtime: Runtime,  # noqa: ARG002
    ) -> dict[str, Any] | None:
        """Inject a todo-list reminder when write_todos has left the context window."""
        messages = state.get("messages") or []
        updates: list[HumanMessage] = []

        if not _should_suppress_routing_hidden_steps(state) and not _routing_guidance_in_messages(messages):
            routing_guidance = _build_routing_guidance_message(state)
            if routing_guidance is not None:
                updates.append(routing_guidance)

        todos: list[Todo] = state.get("todos") or []  # type: ignore[assignment]
        if not todos:
            return {"messages": updates} if updates else None

        if _todos_in_messages(messages):
            # write_todos is still visible in context — nothing to do.
            return {"messages": updates} if updates else None

        if _reminder_in_messages(messages):
            # A reminder was already injected and hasn't been truncated yet.
            return {"messages": updates} if updates else None

        # The todo list exists in state but the original write_todos call is gone.
        # Inject a reminder as a HumanMessage so the model stays aware.
        formatted = _format_todos(todos)

        routing_ctx = state.get("routing_context")
        routing_hint = ""
        if routing_ctx and routing_ctx.get("scene_tasks"):
            scene_tasks = routing_ctx.get("scene_tasks", [])
            skill_lines: list[str] = []
            for st in scene_tasks:
                seg = st.get("segment_text", "")
                skills = st.get("selected_skills", [])
                skill_names = [s.get("id", "") for s in skills if s.get("id")]
                if skill_names:
                    skill_lines.append(f"- {seg} → 使用: {', '.join(skill_names)}")
                else:
                    skill_lines.append(f"- {seg}")
            routing_hint = (
                "\n<routing_guidance>\n"
                "路由结果已给出，请按以下规划执行：\n"
                + "\n".join(f"  {line}" for line in skill_lines)
                + "\n</routing_guidance>\n"
            )

        reminder = HumanMessage(
            name="todo_reminder",
            content=(
                "<system_reminder>\n"
                "Your todo list from earlier is no longer visible in the current context window, "
                "but it is still active. Here is the current state:\n\n"
                f"{formatted}\n\n"
                "Continue tracking and updating this todo list as you work. "
                "Call `write_todos` whenever the status of any item changes.\n"
                f"{routing_hint}"
                "</system_reminder>"
            ),
        )
        updates.append(reminder)
        return {"messages": updates}

    @override
    async def abefore_model(
        self,
        state: PlanningState,
        runtime: Runtime,
    ) -> dict[str, Any] | None:
        """Async version of before_model."""
        return self.before_model(state, runtime)
