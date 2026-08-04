"""Current-turn intent recognition middleware.

The middleware is intentionally lightweight: it writes a structured
``intent_context`` into state and does not alter messages.  SkillRouter and
Todo middleware can consume the same normalized routing query and scene hints
without losing the stable base system prompt.
"""

from __future__ import annotations

import logging
import re
from typing import Any, override

from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import HumanMessage
from langgraph.runtime import Runtime

from deerflow.agents.thread_state import ThreadState
from deerflow.compliance.intent import classify_compliance_intent
from deerflow.config.compliance_config import get_compliance_config
from deerflow.models import create_chat_model
from deerflow.routing.dialogue_act import classify_dialogue_act
from deerflow.routing.intent import (
    RoutingIntentResult,
    aclassify_routing_intent_with_llm,
    classify_routing_intent_with_llm,
    load_scene_templates,
)

logger = logging.getLogger(__name__)
_UPLOAD_BLOCK_RE = re.compile(r"<uploaded_files>[\s\S]*?</uploaded_files>\s*", re.IGNORECASE)


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
        query, frontend_ids, uploaded_files, previous_intent, pending_action, source_message_key = prepared

        if self._same_source(previous_intent, source_message_key):
            logger.debug("IntentRecognition: reuse existing intent_context for source=%s", source_message_key)
            return None

        compliance_intent = self._compliance_intent(query)

        try:
            llm = create_chat_model(name=self.model_name, thinking_enabled=False)
        except Exception:
            # Keep the original dialogue and deterministic/LLM routing flow;
            # the compliance classifier is additive and never a replacement.
            logger.debug("IntentRecognition: routing model unavailable; using deterministic classifier", exc_info=True)
            llm = None
        previous_routing = state.get("routing_context")
        if not isinstance(previous_routing, dict):
            previous_routing = None
        dialogue = classify_dialogue_act(
            query,
            pending_action=pending_action,
            previous_intent_context=previous_intent,
            previous_routing_context=previous_routing,
            llm=llm,
        )
        if dialogue.act in {"clarification_answer", "parameter_update", "task_followup"}:
            intent = self._resume_intent_from_dialogue(query, dialogue.resume_intent_context)
            self._log_intent(intent, query)
            return {
                "dialogue_context": dialogue.__dict__,
                "intent_context": self._dump_intent_context(intent, source_message_key, suppress_hidden_steps=True, compliance_intent=compliance_intent),
                "pending_action": None,
            }
        if dialogue.act == "chitchat":
            intent = RoutingIntentResult(
                intent="chitchat",
                original_query=query,
                normalized_query=query.strip(),
                routing_query=query.strip(),
                confidence=dialogue.confidence,
                reason=dialogue.reason,
            )
            self._log_intent(intent, query)
            return {"dialogue_context": dialogue.__dict__, "intent_context": self._dump_intent_context(intent, source_message_key, compliance_intent=compliance_intent)}

        intent = classify_routing_intent_with_llm(
            query,
            llm=llm,
            scene_templates=load_scene_templates(),
            uploaded_files=uploaded_files,
            available_skill_ids=frontend_ids if isinstance(frontend_ids, list) else None,
            previous_intent=previous_intent,
        )
        self._log_intent(intent, query)
        return {"dialogue_context": dialogue.__dict__, "intent_context": self._dump_intent_context(intent, source_message_key, compliance_intent=compliance_intent)}

    @override
    async def abefore_agent(self, state: ThreadState, runtime: Runtime) -> dict[str, Any] | None:
        prepared = self._prepare_input(state, runtime)
        if prepared is None:
            return None
        query, frontend_ids, uploaded_files, previous_intent, pending_action, source_message_key = prepared

        if self._same_source(previous_intent, source_message_key):
            logger.debug("IntentRecognition: reuse existing intent_context for source=%s", source_message_key)
            return None

        compliance_intent = self._compliance_intent(query)

        try:
            llm = create_chat_model(name=self.model_name, thinking_enabled=False)
        except Exception:
            logger.debug("IntentRecognition: routing model unavailable; using deterministic classifier", exc_info=True)
            llm = None
        previous_routing = state.get("routing_context")
        if not isinstance(previous_routing, dict):
            previous_routing = None
        dialogue = classify_dialogue_act(
            query,
            pending_action=pending_action,
            previous_intent_context=previous_intent,
            previous_routing_context=previous_routing,
            llm=llm,
        )
        if dialogue.act in {"clarification_answer", "parameter_update", "task_followup"}:
            intent = self._resume_intent_from_dialogue(query, dialogue.resume_intent_context)
            self._log_intent(intent, query)
            return {
                "dialogue_context": dialogue.__dict__,
                "intent_context": self._dump_intent_context(intent, source_message_key, suppress_hidden_steps=True, compliance_intent=compliance_intent),
                "pending_action": None,
            }
        if dialogue.act == "chitchat":
            intent = RoutingIntentResult(
                intent="chitchat",
                original_query=query,
                normalized_query=query.strip(),
                routing_query=query.strip(),
                confidence=dialogue.confidence,
                reason=dialogue.reason,
            )
            self._log_intent(intent, query)
            return {"dialogue_context": dialogue.__dict__, "intent_context": self._dump_intent_context(intent, source_message_key, compliance_intent=compliance_intent)}

        intent = await aclassify_routing_intent_with_llm(
            query,
            llm=llm,
            scene_templates=load_scene_templates(),
            uploaded_files=uploaded_files,
            available_skill_ids=frontend_ids if isinstance(frontend_ids, list) else None,
            previous_intent=previous_intent,
        )
        self._log_intent(intent, query)
        return {"dialogue_context": dialogue.__dict__, "intent_context": self._dump_intent_context(intent, source_message_key, compliance_intent=compliance_intent)}

    def _prepare_input(
        self,
        state: ThreadState,
        runtime: Runtime,
    ) -> tuple[str, list[str] | None, list[dict], dict[str, Any] | None, dict[str, Any] | None, str] | None:
        messages = state.get("messages") or []
        last_user_msg = None
        for msg in reversed(messages):
            if isinstance(msg, HumanMessage) and not self._is_internal_human_message(msg):
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
        pending_action = state.get("pending_action")
        if not isinstance(pending_action, dict):
            pending_action = None
        return query, frontend_ids if isinstance(frontend_ids, list) else None, uploaded_files, previous_intent, pending_action, self._source_message_key(last_user_msg, query)

    @staticmethod
    def _log_intent(intent: Any, query: str) -> None:
        logger.info(
            "IntentRecognition: intent=%s scene=%s mode=%s tasks=%d confidence=%.2f query=%r routing_query=%r",
            intent.intent,
            intent.scene,
            getattr(intent, "scene_mode", None),
            len(getattr(intent, "scene_tasks", []) or []),
            intent.confidence,
            query[:80],
            intent.routing_query[:120],
        )

    @staticmethod
    def _extract_text(message: HumanMessage) -> str:
        content = message.content
        if isinstance(content, str):
            return _UPLOAD_BLOCK_RE.sub("", content).strip()
        if isinstance(content, list):
            parts: list[str] = []
            for block in content:
                if isinstance(block, dict) and block.get("type") == "text":
                    parts.append(block.get("text", ""))
                else:
                    text = getattr(block, "text", None)
                    if isinstance(text, str):
                        parts.append(text)
            return _UPLOAD_BLOCK_RE.sub("", "\n".join(parts)).strip()
        return _UPLOAD_BLOCK_RE.sub("", str(content)).strip() if content else ""

    @staticmethod
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

    @staticmethod
    def _source_message_key(message: HumanMessage, query: str) -> str:
        message_id = getattr(message, "id", None)
        if message_id:
            return f"id:{message_id}"
        return f"text:{query.strip()}"

    @staticmethod
    def _same_source(previous_context: dict[str, Any] | None, source_message_key: str) -> bool:
        return isinstance(previous_context, dict) and previous_context.get("_source_message_key") == source_message_key

    @staticmethod
    def _dump_intent_context(
        intent: RoutingIntentResult,
        source_message_key: str,
        *,
        suppress_hidden_steps: bool = False,
        compliance_intent: dict[str, object] | None = None,
    ) -> dict[str, Any]:
        data = intent.model_dump()
        if compliance_intent is not None:
            data["compliance_intent"] = compliance_intent
        data["_source_message_key"] = source_message_key
        if suppress_hidden_steps:
            data["_suppress_hidden_steps"] = True
        return data

    @staticmethod
    def _compliance_intent(query: str) -> dict[str, object] | None:
        if not get_compliance_config().enabled:
            return None
        return classify_compliance_intent(query)

    @staticmethod
    def _resume_intent_from_dialogue(query: str, previous_intent: dict[str, Any] | None) -> RoutingIntentResult:
        if isinstance(previous_intent, dict):
            try:
                intent = RoutingIntentResult.model_validate(previous_intent)
                intent.original_query = query
                intent.normalized_query = query.strip()
                intent.reason = "dialogue_act_clarification_answer"
                intent.confidence = max(intent.confidence, 0.8)
                return intent
            except Exception:
                pass
        return RoutingIntentResult(
            intent="chitchat",
            original_query=query,
            normalized_query=query.strip(),
            routing_query=query.strip(),
            confidence=1.0,
            reason="dialogue_act_clarification_answer_without_resume_context",
        )
