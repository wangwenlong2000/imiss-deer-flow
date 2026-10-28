"""Current-turn intent recognition middleware.

The middleware is intentionally lightweight: it writes a structured
``intent_context`` into state and does not alter messages.  SkillRouter and
Todo middleware can consume the same normalized routing query and scene hints
without losing the stable base system prompt.
"""

from __future__ import annotations

import asyncio
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
from deerflow.routing.turn_requirements import TurnRequirements, requirements_from_context

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
            logger.debug("IntentRecognition: routing model unavailable; business recognition will fail explicitly", exc_info=True)
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
        resolution_query = self._clarification_resolution_query(query, dialogue)
        if dialogue.act in {"clarification_answer", "parameter_update", "task_followup"} and resolution_query is None and (dialogue.act != "task_followup" or llm is not None):
            intent = self._resume_intent_from_dialogue(query, dialogue.resume_intent_context, dialogue=dialogue)
            self._log_intent(intent, query)
            return {
                "dialogue_context": dialogue.__dict__,
                "intent_context": self._dump_intent_context(intent, source_message_key, suppress_hidden_steps=True, compliance_intent=compliance_intent, confirmed_task_query=(dialogue.resume_intent_context or {}).get("_confirmed_task_query")),
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
            resolution_query or query,
            llm=llm,
            scene_templates=load_scene_templates(),
            uploaded_files=uploaded_files,
            available_skill_ids=frontend_ids if isinstance(frontend_ids, list) else None,
            previous_intent=dialogue.resume_intent_context if resolution_query else previous_intent,
        )
        if resolution_query:
            intent.original_query = query
            intent.normalized_query = query.strip()
        self._log_intent(intent, query)
        context = self._dump_intent_context(intent, source_message_key, compliance_intent=compliance_intent)
        if resolution_query:
            context["_confirmed_task_query"] = resolution_query
        return {"dialogue_context": dialogue.__dict__, "intent_context": context, **({"pending_action": None} if resolution_query else {})}

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
            logger.debug("IntentRecognition: routing model unavailable; business recognition will fail explicitly", exc_info=True)
            llm = None
        previous_routing = state.get("routing_context")
        if not isinstance(previous_routing, dict):
            previous_routing = None
        dialogue = await asyncio.to_thread(
            classify_dialogue_act,
            query,
            pending_action=pending_action,
            previous_intent_context=previous_intent,
            previous_routing_context=previous_routing,
            llm=llm,
        )
        resolution_query = self._clarification_resolution_query(query, dialogue)
        if dialogue.act in {"clarification_answer", "parameter_update", "task_followup"} and resolution_query is None and (dialogue.act != "task_followup" or llm is not None):
            intent = self._resume_intent_from_dialogue(query, dialogue.resume_intent_context, dialogue=dialogue)
            self._log_intent(intent, query)
            return {
                "dialogue_context": dialogue.__dict__,
                "intent_context": self._dump_intent_context(intent, source_message_key, suppress_hidden_steps=True, compliance_intent=compliance_intent, confirmed_task_query=(dialogue.resume_intent_context or {}).get("_confirmed_task_query")),
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
            resolution_query or query,
            llm=llm,
            scene_templates=load_scene_templates(),
            uploaded_files=uploaded_files,
            available_skill_ids=frontend_ids if isinstance(frontend_ids, list) else None,
            previous_intent=dialogue.resume_intent_context if resolution_query else previous_intent,
        )
        if resolution_query:
            intent.original_query = query
            intent.normalized_query = query.strip()
        self._log_intent(intent, query)
        context = self._dump_intent_context(intent, source_message_key, compliance_intent=compliance_intent)
        if resolution_query:
            context["_confirmed_task_query"] = resolution_query
        return {"dialogue_context": dialogue.__dict__, "intent_context": context, **({"pending_action": None} if resolution_query else {})}

    @staticmethod
    def _clarification_resolution_query(query: str, dialogue: Any) -> str | None:
        """Resolve a formerly unknown task using an actual clarification reply.

        Known-scene parameter/choice replies retain the existing no-recognition
        fast path. A selected option label is grounded in the user's selection,
        never in a previous model-generated routing description.
        """
        if dialogue.act not in {"clarification_answer", "parameter_update", "task_followup"} or dialogue.parsed_answer is False:
            return None
        previous = dialogue.resume_intent_context or {}
        scenes = previous.get("scenes") or [previous.get("scene")]
        if any(scene in load_scene_templates() for scene in scenes if isinstance(scene, str)):
            return None
        original = previous.get("_confirmed_task_query") or previous.get("original_query") or ""
        answer = dialogue.parsed_answer
        selected = (answer.get("label") or answer.get("value")) if isinstance(answer, dict) else None
        parts = [str(original)[:2000], f"用户澄清回答：{query}"]
        if isinstance(selected, str) and selected != query:
            parts.append(f"用户选择的选项：{selected[:512]}")
        return "\n".join(part for part in parts if part)

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
        # A file can be revisited after an unrelated task. The immediately
        # preceding query alone cannot restore that file's actual user goal.
        history = []
        for message in messages:
            if message is last_user_msg:
                break
            if isinstance(message, HumanMessage) and not self._is_internal_human_message(message):
                text = self._extract_text(message)
                if text:
                    history.append(text[:700])
        if history:
            previous_intent = {**(previous_intent or {}), "_user_task_history": history[-6:]}
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
        confirmed_task_query: str | None = None,
    ) -> dict[str, Any]:
        data = intent.model_dump()
        if compliance_intent is not None:
            data["compliance_intent"] = compliance_intent
        data["_source_message_key"] = source_message_key
        if suppress_hidden_steps:
            data["_suppress_hidden_steps"] = True
        if confirmed_task_query:
            data["_confirmed_task_query"] = confirmed_task_query
        return data

    @staticmethod
    def _compliance_intent(query: str) -> dict[str, object] | None:
        if not get_compliance_config().enabled:
            return None
        return classify_compliance_intent(query)

    @staticmethod
    def _resume_intent_from_dialogue(query: str, previous_intent: dict[str, Any] | None, *, dialogue=None) -> RoutingIntentResult:
        if isinstance(previous_intent, dict):
            try:
                intent = RoutingIntentResult.model_validate(previous_intent)
                intent.original_query = query
                intent.normalized_query = query.strip()
                intent.routing_query = query.strip()
                intent.reason = "dialogue_act_clarification_answer"
                metadata = getattr(dialogue, 'metadata', {}) or {}
                requirements = metadata.get('turn_requirements')
                if isinstance(requirements, dict) or getattr(dialogue, 'act', None) == 'task_followup':
                    current = TurnRequirements.model_validate(requirements or {}).model_dump()
                    intent = intent.model_copy(update=current)
                updates = metadata.get('parameter_updates') or {}
                if isinstance(updates, dict):
                    updates = {k: v for k, v in updates.items() if k in intent.params}
                    intent.params = {**intent.params, **updates}
                intent.planning_steps = []
                for task in intent.scene_tasks + intent.sub_tasks:
                    task.text = query
                    task.params = {**task.params, **{k: v for k, v in updates.items() if k in task.params}} if isinstance(updates, dict) else task.params
                return intent.model_copy(update=requirements_from_context(intent.model_dump()).model_dump())
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
