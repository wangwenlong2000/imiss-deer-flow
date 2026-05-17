"""Middleware that extends TodoListMiddleware with context-loss detection.

When the message history is truncated (e.g., by SummarizationMiddleware), the
original `write_todos` tool call and its ToolMessage can be scrolled out of the
active context window. This middleware detects that situation and injects a
reminder message so the model still knows about the outstanding todo list.
"""

from __future__ import annotations

from typing import Any, override

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
    """Return True if current model context already has the routing guidance."""
    for msg in messages:
        if isinstance(msg, HumanMessage) and getattr(msg, "name", None) == "todo_routing_guidance":
            return True
    return False


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
    if intent_ctx.get("scene_name") or intent_ctx.get("scene"):
        scene = intent_ctx.get("scene_name") or intent_ctx.get("scene")
        scene_id = intent_ctx.get("scene")
        lines.append(f"识别场景：{scene}" + (f" ({scene_id})" if scene_id else ""))
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
        skill_names = [s.get("id", "") for s in skills if s.get("id")]
        line = f"- {seg}"
        if scene:
            line += f"；场景：{scene}"
        if skill_names:
            line += f"；使用 skill：{', '.join(skill_names)}"
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
    routing_text = _format_routing_guidance(routing_ctx)
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

        if not _routing_guidance_in_messages(messages):
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
