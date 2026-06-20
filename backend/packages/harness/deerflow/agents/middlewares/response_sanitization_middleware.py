"""Sanitize model-visible assistant text before it is streamed or persisted."""

from __future__ import annotations

import re
from typing import Any, override

from langchain.agents import AgentState
from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import AIMessage
from langgraph.runtime import Runtime


_THINK_BLOCK_RE = re.compile(r"<think\b[^>]*>.*?</think\s*>", re.IGNORECASE | re.DOTALL)
_THINK_OPEN_RE = re.compile(r"<think\b[^>]*>", re.IGNORECASE)
_THINK_CLOSE_RE = re.compile(r"</think\s*>", re.IGNORECASE)


def _sanitize_text(text: str) -> str:
    if "<think" not in text.lower() and "</think" not in text.lower():
        return text
    cleaned = _THINK_BLOCK_RE.sub("", text)
    closing_matches = list(_THINK_CLOSE_RE.finditer(cleaned))
    if closing_matches:
        cleaned = cleaned[closing_matches[-1].end() :]
    cleaned = _THINK_OPEN_RE.sub("", cleaned)
    cleaned = _THINK_CLOSE_RE.sub("", cleaned)
    return cleaned.strip()


def _sanitize_content(content: Any) -> Any:
    if isinstance(content, str):
        return _sanitize_text(content)
    if isinstance(content, list):
        changed = False
        sanitized = []
        for block in content:
            if isinstance(block, str):
                new_block = _sanitize_text(block)
                changed = changed or new_block != block
                sanitized.append(new_block)
            elif isinstance(block, dict) and isinstance(block.get("text"), str):
                new_text = _sanitize_text(block["text"])
                changed = changed or new_text != block["text"]
                sanitized.append({**block, "text": new_text})
            else:
                sanitized.append(block)
        return sanitized if changed else content
    return content


class ResponseSanitizationMiddleware(AgentMiddleware[AgentState]):
    """Remove internal thinking tags from assistant messages."""

    def _apply(self, state: AgentState) -> dict | None:
        messages = state.get("messages", [])
        if not messages:
            return None

        last_msg = messages[-1]
        if not isinstance(last_msg, AIMessage):
            return None

        sanitized_content = _sanitize_content(last_msg.content)
        if sanitized_content == last_msg.content:
            return None

        return {"messages": [last_msg.model_copy(update={"content": sanitized_content})]}

    @override
    def after_model(self, state: AgentState, runtime: Runtime) -> dict | None:
        return self._apply(state)

    @override
    async def aafter_model(self, state: AgentState, runtime: Runtime) -> dict | None:
        return self._apply(state)
