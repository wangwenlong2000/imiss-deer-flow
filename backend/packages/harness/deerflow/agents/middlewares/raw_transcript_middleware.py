"""Middleware that preserves the full raw transcript alongside compressed context."""

from typing import Annotated, NotRequired, override

from langchain.agents import AgentState
from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import BaseMessage
from langgraph.runtime import Runtime

from deerflow.agents.thread_state import is_compaction_summary_message, merge_raw_messages


class RawTranscriptMiddlewareState(AgentState):
    """Compatible with the `ThreadState` schema."""

    raw_messages: NotRequired[Annotated[list[BaseMessage], merge_raw_messages]]


class RawTranscriptMiddleware(AgentMiddleware[RawTranscriptMiddlewareState]):
    """Persist the uncompressed transcript for UI and audit consumers."""

    state_schema = RawTranscriptMiddlewareState

    @override
    def before_model(self, state: RawTranscriptMiddlewareState, runtime: Runtime) -> dict | None:
        messages = _visible_messages(state.get("messages", []))
        if not messages:
            return None

        return {"raw_messages": messages}

    @override
    async def abefore_model(self, state: RawTranscriptMiddlewareState, runtime: Runtime) -> dict | None:
        messages = _visible_messages(state.get("messages", []))
        if not messages:
            return None

        return {"raw_messages": messages}

    @override
    def after_model(self, state: RawTranscriptMiddlewareState, runtime: Runtime) -> dict | None:
        messages = _visible_messages(state.get("messages", []))
        if not messages:
            return None

        return {"raw_messages": messages}

    @override
    async def aafter_model(self, state: RawTranscriptMiddlewareState, runtime: Runtime) -> dict | None:
        messages = _visible_messages(state.get("messages", []))
        if not messages:
            return None

        return {"raw_messages": messages}


def _visible_messages(messages: list[BaseMessage]) -> list[BaseMessage]:
    return [message for message in messages if not is_compaction_summary_message(message)]
