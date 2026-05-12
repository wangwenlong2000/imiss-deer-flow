"""Middleware for preserving UI-visible chat history across summarization."""

from __future__ import annotations

from typing import Annotated, Any, override

from langchain.agents import AgentState
from langchain.agents.middleware import AgentMiddleware
from langgraph.runtime import Runtime

from deerflow.agents.thread_state import merge_display_messages


class DisplayMessagesMiddlewareState(AgentState):
    """Compatible with the ThreadState schema."""

    display_messages: Annotated[list[Any], merge_display_messages]


class DisplayMessagesMiddleware(AgentMiddleware[DisplayMessagesMiddlewareState]):
    """Append current messages to the UI transcript without changing model context."""

    state_schema = DisplayMessagesMiddlewareState

    @override
    def after_agent(
        self,
        state: DisplayMessagesMiddlewareState,
        runtime: Runtime,
    ) -> dict | None:
        messages = state.get("messages", [])
        if not messages:
            return None

        return {"display_messages": list(messages)}
