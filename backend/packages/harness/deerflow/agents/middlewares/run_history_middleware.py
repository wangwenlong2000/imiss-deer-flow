"""Middleware for persisting final run history into local JSONL logs."""

from __future__ import annotations

from typing import Any, override

from langchain.agents import AgentState
from langchain.agents.middleware import AgentMiddleware
from langgraph.runtime import Runtime

from deerflow.monitoring import to_jsonable, write_run_event_log


class RunHistoryMiddlewareState(AgentState):
    """Compatible with the ThreadState schema."""

    pass


def _extract_final_ai_text(messages: list[Any]) -> str:
    """Extract the latest assistant-visible response text from message history."""
    for message in reversed(messages):
        message_type = getattr(message, "type", None)
        if message_type == "human":
            break
        if message_type != "ai":
            continue

        content = getattr(message, "content", "")
        if isinstance(content, str) and content:
            return content
        if isinstance(content, list):
            parts: list[str] = []
            for block in content:
                if isinstance(block, str):
                    parts.append(block)
                elif isinstance(block, dict):
                    text = block.get("text")
                    if isinstance(text, str):
                        parts.append(text)
            text = "".join(parts)
            if text:
                return text
    return ""


class RunHistoryMiddleware(AgentMiddleware[RunHistoryMiddlewareState]):
    """Persist final lead-agent execution state for local monitoring."""

    state_schema = RunHistoryMiddlewareState

    @override
    def after_agent(self, state: RunHistoryMiddlewareState, runtime: Runtime) -> dict | None:
        thread_id = runtime.context.get("thread_id")
        if not isinstance(thread_id, str) or not thread_id:
            return None

        messages = state.get("messages", [])
        payload = {
            "context": to_jsonable(runtime.context),
            "message_count": len(messages),
            "response_text": _extract_final_ai_text(messages),
            "messages": to_jsonable(messages),
            "artifacts": to_jsonable(state.get("artifacts", [])),
            "title": state.get("title"),
            "todos": to_jsonable(state.get("todos")),
            "uploaded_files": to_jsonable(state.get("uploaded_files")),
            "thread_data": to_jsonable(state.get("thread_data")),
        }
        write_run_event_log(
            thread_id,
            "agent.run.final",
            payload,
            source="lead_agent.middleware",
        )
        return None