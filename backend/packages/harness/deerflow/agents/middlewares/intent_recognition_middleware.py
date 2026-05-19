"""Current-turn intent recognition middleware.

The middleware is intentionally lightweight: it writes a structured
``intent_context`` into state and does not alter messages.  SkillRouter and
Todo middleware can consume the same normalized routing query and scene hints
without losing the stable base system prompt.
"""

from __future__ import annotations

import logging
from typing import Any

try:
    from typing import override
except ImportError:
    from typing_extensions import override

from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import HumanMessage
from langgraph.runtime import Runtime

from deerflow.agents.thread_state import ThreadState
from deerflow.models import create_chat_model
from deerflow.routing.intent import (
    aclassify_routing_intent_with_llm,
    classify_routing_intent_with_llm,
    load_scene_templates,
)

logger = logging.getLogger(__name__)


class IntentRecognitionMiddleware(AgentMiddleware[ThreadState]):
    """Extract current-turn intent metadata for downstream routing."""

    state_schema = ThreadState

    def __init__(self, *, model_name: str | None = None) -> None:
        super().__init__()
        self.model_name = model_name

    @override
    def before_agent(self, state: ThreadState, runtime: Runtime) -> dict[str, Any] | None:
        prepared = self._prepare_input(state, runtime)
        if prepared is None:
            return None
        query, frontend_ids, uploaded_files, previous_intent = prepared

        llm = create_chat_model(name=self.model_name, thinking_enabled=False)
        intent = classify_routing_intent_with_llm(
            query,
            llm=llm,
            scene_templates=load_scene_templates(),
            uploaded_files=uploaded_files,
            available_skill_ids=frontend_ids if isinstance(frontend_ids, list) else None,
            previous_intent=previous_intent,
        )
        self._log_intent(intent, query)
        return {"intent_context": intent.model_dump()}

    @override
    async def abefore_agent(self, state: ThreadState, runtime: Runtime) -> dict[str, Any] | None:
        prepared = self._prepare_input(state, runtime)
        if prepared is None:
            return None
        query, frontend_ids, uploaded_files, previous_intent = prepared

        llm = create_chat_model(name=self.model_name, thinking_enabled=False)
        intent = await aclassify_routing_intent_with_llm(
            query,
            llm=llm,
            scene_templates=load_scene_templates(),
            uploaded_files=uploaded_files,
            available_skill_ids=frontend_ids if isinstance(frontend_ids, list) else None,
            previous_intent=previous_intent,
        )
        self._log_intent(intent, query)
        return {"intent_context": intent.model_dump()}

    def _prepare_input(
        self,
        state: ThreadState,
        runtime: Runtime,
    ) -> tuple[str, list[str] | None, list[dict], dict[str, Any] | None] | None:
        messages = state.get("messages") or []
        last_user_msg = None
        for msg in reversed(messages):
            if isinstance(msg, HumanMessage):
                last_user_msg = msg
                break

        if last_user_msg is None:
            return None

        query = self._extract_text(last_user_msg)
        if not query or not query.strip():
            return None

        frontend_ids = state.get("frontend_enabled_skill_ids")
        if frontend_ids is None:
            runtime_context = getattr(runtime, "context", None)
            if isinstance(runtime_context, dict):
                frontend_ids = runtime_context.get("frontend_enabled_skill_ids")

        uploaded_files = state.get("uploaded_files") or []
        previous_intent = state.get("intent_context")
        if not isinstance(previous_intent, dict):
            previous_intent = None
        return query, frontend_ids if isinstance(frontend_ids, list) else None, uploaded_files, previous_intent

    @staticmethod
    def _log_intent(intent: Any, query: str) -> None:
        logger.info(
            "IntentRecognition: intent=%s scene=%s confidence=%.2f query=%r routing_query=%r",
            intent.intent,
            intent.scene,
            intent.confidence,
            query[:80],
            intent.routing_query[:120],
        )

    @staticmethod
    def _extract_text(message: HumanMessage) -> str:
        content = message.content
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts: list[str] = []
            for block in content:
                if isinstance(block, dict) and block.get("type") == "text":
                    parts.append(block.get("text", ""))
                else:
                    text = getattr(block, "text", None)
                    if isinstance(text, str):
                        parts.append(text)
            return "\n".join(parts)
        return str(content) if content else ""
