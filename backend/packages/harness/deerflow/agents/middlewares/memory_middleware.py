"""Middleware for memory mechanism."""

import re
from collections.abc import Awaitable, Callable
from typing import Any, override

from langchain.agents import AgentState
from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import ModelCallResult, ModelRequest, ModelResponse
from langchain_core.messages import SystemMessage
from langgraph.runtime import Runtime

from deerflow.agents.memory.queue import get_memory_queue
from deerflow.agents.memory.scope import memory_scope_from_context
from deerflow.compliance.runtime import runtime_context
from deerflow.config.memory_config import get_memory_config


class MemoryMiddlewareState(AgentState):
    """Compatible with the `ThreadState` schema."""

    pass


def _filter_messages_for_memory(messages: list[Any]) -> list[Any]:
    """Filter messages to keep only user inputs and final assistant responses.

    This filters out:
    - Tool messages (intermediate tool call results)
    - AI messages with tool_calls (intermediate steps, not final responses)
    - The <uploaded_files> block injected by UploadsMiddleware into human messages
      (file paths are session-scoped and must not persist in long-term memory).
      The user's actual question is preserved; only turns whose content is entirely
      the upload block (nothing remains after stripping) are dropped along with
      their paired assistant response.

    Only keeps:
    - Human messages (with the ephemeral upload block removed)
    - AI messages without tool_calls (final assistant responses), unless the
      paired human turn was upload-only and had no real user text.

    Args:
        messages: List of all conversation messages.

    Returns:
        Filtered list containing only user inputs and final assistant responses.
    """
    _UPLOAD_BLOCK_RE = re.compile(r"<uploaded_files>[\s\S]*?</uploaded_files>\n*", re.IGNORECASE)

    filtered = []
    skip_next_ai = False
    for msg in messages:
        msg_type = getattr(msg, "type", None)

        if msg_type == "human":
            content = getattr(msg, "content", "")
            if isinstance(content, list):
                content = " ".join(p.get("text", "") for p in content if isinstance(p, dict))
            content_str = str(content)
            if "<uploaded_files>" in content_str:
                # Strip the ephemeral upload block; keep the user's real question.
                stripped = _UPLOAD_BLOCK_RE.sub("", content_str).strip()
                if not stripped:
                    # Nothing left — the entire turn was upload bookkeeping;
                    # skip it and the paired assistant response.
                    skip_next_ai = True
                    continue
                # Rebuild the message with cleaned content so the user's question
                # is still available for memory summarisation.
                from copy import copy

                clean_msg = copy(msg)
                clean_msg.content = stripped
                filtered.append(clean_msg)
                skip_next_ai = False
            else:
                filtered.append(msg)
                skip_next_ai = False
        elif msg_type == "ai":
            tool_calls = getattr(msg, "tool_calls", None)
            if not tool_calls:
                if skip_next_ai:
                    skip_next_ai = False
                    continue
                filtered.append(msg)
        # Skip tool messages and AI messages with tool_calls

    return filtered


class MemoryMiddleware(AgentMiddleware[MemoryMiddlewareState]):
    """Middleware that queues conversation for memory update after agent execution.

    This middleware:
    1. After each agent execution, queues the conversation for memory update
    2. Only includes user inputs and final assistant responses (ignores tool calls)
    3. The queue uses debouncing to batch multiple updates together
    4. Memory is updated asynchronously via LLM summarization
    """

    state_schema = MemoryMiddlewareState

    def __init__(self, agent_name: str | None = None):
        """Initialize the MemoryMiddleware.

        Args:
            agent_name: If provided, memory is stored per-agent. If None, uses global memory.
        """
        super().__init__()
        self._agent_name = agent_name

    def _scoped_model_request(self, request: ModelRequest) -> ModelRequest:
        """Resolve memory at model-call time, never in a shared graph instance."""
        from deerflow.agents.lead_agent.prompt import _get_memory_context

        try:
            scope = memory_scope_from_context(runtime_context(request))
            if scope is None:
                # Legacy development prompts already contain global memory.
                return request
            memory_context = _get_memory_context(self._agent_name, scope=scope)
        except PermissionError:
            memory_context = ""

        # A factory-created prompt must never retain global memory in an IAM
        # run, including one missing its signed scope. Preserve non-text blocks.
        system_message = request.system_message
        content = system_message.content if system_message is not None else ""
        memory_block = re.compile(r"<memory>[\s\S]*?</memory>\s*")
        if isinstance(content, str):
            updated_content = memory_block.sub("", content)
            if memory_context:
                updated_content += "\n" + memory_context
        else:
            updated_content = []
            for block in content:
                if isinstance(block, str):
                    updated_content.append(memory_block.sub("", block))
                elif isinstance(block, dict) and isinstance(block.get("text"), str):
                    updated_content.append({**block, "text": memory_block.sub("", block["text"])})
                else:
                    updated_content.append(block)
            if memory_context:
                updated_content.append({"type": "text", "text": memory_context})

        message = system_message.model_copy(update={"content": updated_content}) if system_message is not None else SystemMessage(content=updated_content)
        return request.override(system_message=message)

    @override
    def wrap_model_call(self, request: ModelRequest, handler: Callable[[ModelRequest], ModelResponse]) -> ModelCallResult:
        return handler(self._scoped_model_request(request))

    @override
    async def awrap_model_call(self, request: ModelRequest, handler: Callable[[ModelRequest], Awaitable[ModelResponse]]) -> ModelCallResult:
        return await handler(self._scoped_model_request(request))

    @override
    def after_agent(self, state: MemoryMiddlewareState, runtime: Runtime) -> dict | None:
        """Queue conversation for memory update after agent completes.

        Args:
            state: The current agent state.
            runtime: The runtime context.

        Returns:
            None (no state changes needed from this middleware).
        """
        config = get_memory_config()
        if not config.enabled:
            return None

        try:
            scope = memory_scope_from_context(runtime_context(runtime))
        except PermissionError:
            return None

        # Get thread ID from runtime context
        thread_id = runtime.context.get("thread_id")
        if not thread_id:
            print("MemoryMiddleware: No thread_id in context, skipping memory update")
            return None

        # Get messages from state
        messages = state.get("messages", [])
        if not messages:
            print("MemoryMiddleware: No messages in state, skipping memory update")
            return None

        # Filter to only keep user inputs and final assistant responses
        filtered_messages = _filter_messages_for_memory(messages)

        # Only queue if there's meaningful conversation
        # At minimum need one user message and one assistant response
        user_messages = [m for m in filtered_messages if getattr(m, "type", None) == "human"]
        assistant_messages = [m for m in filtered_messages if getattr(m, "type", None) == "ai"]

        if not user_messages or not assistant_messages:
            return None

        # Queue the filtered conversation for memory update
        queue = get_memory_queue()
        queue.add(thread_id=thread_id, messages=filtered_messages, agent_name=self._agent_name, scope=scope)

        return None
