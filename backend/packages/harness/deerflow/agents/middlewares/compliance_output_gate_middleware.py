"""OutputGate — sanitize complete answers before they enter graph state.

The first-party web, mobile and channel stream relays request state snapshots,
not token or raw-event streams. ``wrap_model_call`` checks and rewrites the
complete answer before LangGraph can persist or expose it. A retract event is
still emitted for integrations that explicitly subscribe to that protocol.

``after_model`` is a backstop for messages changed after the model node. It
skips an exact answer already checked by ``wrap_model_call`` and rechecks any
later mutation before returning it to graph state.

``scan_increment`` remains a bounded helper for a future transport that can
inspect chunks before publishing them. First-party transports do not rely on
post-display retraction for safety.
"""

from __future__ import annotations

import dataclasses
import hashlib
import logging
import uuid
from collections.abc import Awaitable, Callable
from typing import Any, override

from langchain.agents import AgentState
from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import AIMessage
from langgraph.errors import GraphBubbleUp
from langgraph.runtime import Runtime

from deerflow.agents.thread_state import ThreadState
from deerflow.compliance.actions import REFUSAL_TEXT, apply_actions, has_terminal_refusal
from deerflow.compliance.normalizers.llm_output import LlmOutputNormalizer, extract_text
from deerflow.compliance.runtime import (
    FAIL_CLOSED_NOTICE,
    INCOMPLETE_SCAN_NOTICE,
    compliance_context,
    decision_types,
    decision_check,
    failure_decision,
    gate_config,
    gate_enabled,
    get_engine,
    runtime_context,
    user_context,
    user_notice,
)
from deerflow.compliance.scene import scene_origin_from_state
from deerflow.compliance.types import ComplianceDecision

logger = logging.getLogger(__name__)

GATE = "OutputGate"

#: Custom stream event type the frontend listens for.
RETRACT_EVENT = "compliance_retract"


def _digest(text: str) -> str:
    """Identify text already cleared, so the backstop can skip it."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class ComplianceOutputGateMiddleware(AgentMiddleware[ThreadState]):
    """Check the model's answer, retract and rewrite it when it violates."""

    state_schema = ThreadState

    def __init__(self, *, engine: Any = None, model_name: str | None = None) -> None:
        super().__init__()
        self._engine = engine
        self._model_name = model_name
        self._normalizer = LlmOutputNormalizer()
        #: message_id -> characters already scanned, for the incremental pass.
        self._scan_offsets: dict[str, int] = {}
        #: message_id -> digest of text already cleared, so the after_model
        #: backstop does not re-scan what wrap_model_call already handled.
        self._checked: dict[str, str] = {}

    # ── hooks ───────────────────────────────────────────────────────────────

    @override
    def wrap_model_call(self, request: Any, handler: Callable[[Any], Any]) -> Any:
        """Sanitize the response before it can enter graph state.

        This is the authoritative rewrite. ``after_model`` is only a backstop.
        """
        return self._sanitize_response(handler(request), request, state=getattr(request, "state", None))

    @override
    async def awrap_model_call(self, request: Any, handler: Callable[[Any], Awaitable[Any]]) -> Any:
        return self._sanitize_response(await handler(request), request, state=getattr(request, "state", None))

    @override
    def after_model(self, state: AgentState, runtime: Runtime) -> dict[str, Any] | None:
        return self._process(state, runtime)

    @override
    async def aafter_model(self, state: AgentState, runtime: Runtime) -> dict[str, Any] | None:
        return self._process(state, runtime)

    # ── authoritative sanitization ──────────────────────────────────────────

    def _sanitize_response(self, response: Any, request: Any = None, *, state: Any = None) -> Any:
        """Replace the model's answer inside the response, before it reaches state.

        ``_build_commands`` in langchain's factory does ``{"messages": response.result}``,
        so whatever is returned here is the *only* version any downstream component
        — RawTranscript, Title, the SSE frames, the checkpoint — will ever observe.
        """
        result = getattr(response, "result", None)
        if not isinstance(result, list) or not result:
            return response

        new_result = list(result)
        changed = False
        for index, message in enumerate(result):
            if not isinstance(message, AIMessage):
                continue

            if getattr(message, "tool_calls", None):
                # Tool-call messages can include an unreviewed natural-language
                # preamble. They are intermediate protocol state, so keep the
                # tool calls and non-text blocks but do not expose that text in
                # snapshots.
                if gate_enabled(GATE):
                    safe_content = self._without_tool_call_text(message.content)
                    if safe_content != message.content:
                        new_result[index] = message.model_copy(update={"content": safe_content})
                        changed = True
                continue

            check_state = dict(state or {})
            check_state["messages"] = list(new_result[: index + 1])
            update = self._process(check_state, request)
            if update is not None:
                new_result[index] = update["messages"][0]
                changed = True

        if not changed:
            return response
        # dataclasses.replace keeps structured_response and any sibling messages.
        return dataclasses.replace(response, result=new_result)

    @staticmethod
    def _without_tool_call_text(content: Any) -> Any:
        if isinstance(content, str):
            return ""
        if isinstance(content, list):
            return [
                block
                for block in content
                if not (
                    isinstance(block, str)
                    or (
                        isinstance(block, dict)
                        and block.get("type") == "text"
                        and isinstance(block.get("text"), str)
                    )
                )
            ]
        return content

    # ── core ────────────────────────────────────────────────────────────────

    def _process(self, state: AgentState, runtime_or_request: Any = None) -> dict[str, Any] | None:
        if not gate_enabled(GATE):
            return None

        message = self._last_ai_message(state)
        if message is None:
            return None

        text = extract_text(message.content)
        if not text.strip():
            return None

        # A message with tool calls is an intermediate step, not an answer to the
        # user. Rewriting it would break the tool-call/response pairing.
        if getattr(message, "tool_calls", None):
            return None

        # Backstop short-circuit: wrap_model_call already cleared this exact text.
        # Without it every answer would be scanned twice and audited twice.
        message_id = str(message.id or "")
        if message_id and self._checked.get(message_id) == _digest(text):
            return None

        request_id = f"out-{uuid.uuid4().hex[:12]}"
        try:
            decision = self._check(text, message, request_id, runtime_or_request, state=state)
        except GraphBubbleUp:
            raise
        except Exception as exc:
            logger.exception("compliance: OutputGate detection failed")
            decision = failure_decision(GATE, request_id, exc)
            if not decision.actions:
                return None
            return self._retract(message, FAIL_CLOSED_NOTICE, decision, state)

        if decision.diagnostics.truncated:
            return self._retract(message, INCOMPLETE_SCAN_NOTICE, decision, state)

        # `allow` is an explicit policy result for self_use.  Do not turn it
        # into a warning/retraction merely because a detector found an entity
        # that the current scene permits.
        if not decision.actions or set(decision.actions) <= {"allow"}:
            self._scan_offsets.pop(message_id, None)
            self._remember(message, text)
            return {"messages": [self._rewritten(message, text, decision, state, retracted=False)]}

        replacement = (
            REFUSAL_TEXT
            if has_terminal_refusal(decision.actions, decision.per_violation_actions)
            else apply_actions(text, decision.actions, decision.hits)
        )
        if replacement is None:
            # warn / manual_review only: nothing to rewrite, but the user should
            # still be told the answer was flagged.
            return self._notice_only(message, decision, state)

        return self._retract(message, replacement, decision, state)

    def _check(
        self,
        text: str,
        message: AIMessage,
        request_id: str,
        runtime_or_request: Any = None,
        *,
        state: Any = None,
    ) -> ComplianceDecision:
        config = gate_config(GATE)
        units = self._normalizer.to_units(message, gate=GATE, message_id=str(message.id or ""))
        if not units:
            return ComplianceDecision(request_id=request_id, gate=GATE, scene_key="_unknown")

        engine = self._engine or get_engine()
        manual_context = compliance_context(runtime_or_request)
        scene_origin, _ = scene_origin_from_state(state, manual_context=manual_context)
        return engine.check(
            units,
            gate=GATE,
            request_id=request_id,
            thread_id=self._thread_id(runtime_or_request),
            user=user_context(runtime_or_request),
            model_name=self._model_name,
            budget_ms=config.budget_ms,
            max_units=config.max_units,
            audit_clean=True,
            origin={
                "message_id": str(message.id or ""),
                "chars": len(text),
                "compliance_context": compliance_context(runtime_or_request),
                "streaming_mode": "buffered_snapshot",
                "transient_exposure_possible": False,
                **scene_origin,
            },
        )

    @staticmethod
    def _thread_id(runtime_or_request: Any) -> str | None:
        try:
            context = runtime_context(runtime_or_request)
            if context.get("thread_id"):
                return str(context["thread_id"])

            # ModelRequest exposes the LangGraph Runtime as ``request.runtime``;
            # keep the direct fallbacks for plain Runtime and test doubles.
            runtime = getattr(runtime_or_request, "runtime", runtime_or_request)
            return getattr(runtime, "thread_id", None) or getattr(runtime_or_request, "thread_id", None)
        except Exception:
            return None

    # ── incremental scanning (layer 1) ──────────────────────────────────────

    def scan_increment(self, message_id: str, accumulated_text: str) -> ComplianceDecision | None:
        """Scan a partially generated answer; return a decision when it violates.

        Called by the streaming layer as tokens accumulate. Returns ``None`` until
        at least ``interval_chars`` new characters have arrived since the last
        scan, so cost stays proportional to output length rather than to token
        count.

        Shrinking ``interval_chars`` narrows the leak window and costs more
        detector calls; that is the whole trade and it is deliberately a dial.
        """
        if not gate_enabled(GATE):
            return None
        config = gate_config(GATE)
        scan_config = getattr(config, "incremental_scan", None)
        if scan_config is None or not scan_config.enabled:
            return None

        seen = self._scan_offsets.get(message_id, 0)
        if len(accumulated_text) - seen < scan_config.interval_chars:
            return None
        self._scan_offsets[message_id] = len(accumulated_text)

        request_id = f"out-inc-{uuid.uuid4().hex[:8]}"
        try:
            units = self._normalizer.to_units(accumulated_text, gate=GATE, message_id=message_id)
            if not units:
                return None
            engine = self._engine or get_engine()
            decision = engine.check(
                units,
                gate=GATE,
                request_id=request_id,
                model_name=self._model_name,
                budget_ms=config.budget_ms,
                max_units=config.max_units,
                origin={"message_id": message_id, "incremental": True, "chars": len(accumulated_text)},
            )
        except Exception as exc:
            logger.exception("compliance: OutputGate incremental scan failed")
            decision = failure_decision(GATE, request_id, exc)

        return decision if decision.actions else None

    def reset_scan_state(self, message_id: str) -> None:
        self._scan_offsets.pop(message_id, None)
        self._checked.pop(message_id, None)

    # ── retraction (layer 1 event + layer 2 rewrite) ────────────────────────

    def _retract(
        self,
        message: AIMessage,
        replacement: str,
        decision: ComplianceDecision,
        state: AgentState,
    ) -> dict[str, Any]:
        self.emit_retract_event(message, replacement, decision)
        self._remember(message, replacement)
        return {"messages": [self._rewritten(message, replacement, decision, state, retracted=True)]}

    def _remember(self, message: AIMessage, replacement: str) -> None:
        """Record the sanitized text so the backstop treats it as already cleared."""
        message_id = str(message.id or "")
        if message_id:
            self._checked[message_id] = _digest(replacement)

    def _notice_only(
        self,
        message: AIMessage,
        decision: ComplianceDecision,
        state: AgentState,
    ) -> dict[str, Any] | None:
        """warn / manual_review: keep the answer, append the compliance notice."""
        text = extract_text(message.content)
        annotated = f"{text}\n\n{user_notice(decision)}"
        self.emit_retract_event(message, annotated, decision)
        self._remember(message, annotated)
        return {"messages": [self._rewritten(message, annotated, decision, state, retracted=True)]}

    def emit_retract_event(self, message: AIMessage, replacement: str, decision: ComplianceDecision) -> bool:
        """Send the ``compliance_retract`` custom stream event.

        Uses LangGraph's ``get_stream_writer()``, which rides the existing
        ``custom`` stream mode — no protocol change needed.

        Outside a streaming run there is no writer, which is normal (a plain
        ``runs.wait()`` caller simply receives the already-rewritten message).
        Returns whether the event actually went out.
        """
        try:
            from langgraph.config import get_stream_writer

            writer = get_stream_writer()
        except Exception:
            logger.debug("compliance: no stream writer available; skipping retract event")
            return False
        if writer is None:
            return False

        try:
            writer(build_retract_event(message, replacement, decision))
        except Exception:
            # The rewritten message is the authoritative outcome; a failed screen
            # update must not take the whole turn down.
            logger.exception("compliance: failed to emit retract event")
            return False
        return True

    @staticmethod
    def _rewritten(
        message: AIMessage,
        replacement: str,
        decision: ComplianceDecision,
        state: AgentState,
        *,
        retracted: bool,
    ) -> AIMessage:
        """Rebuild the message so persisted state holds only the safe version.

        The id is preserved so LangGraph's message reducer replaces the original
        rather than appending — that is what keeps the violation out of the
        checkpoint.
        """
        metadata = dict(getattr(message, "response_metadata", None) or {})
        input_check = (state or {}).get("compliance_input_result")
        checks = [input_check] if isinstance(input_check, dict) else []
        checks.append(decision_check(decision))
        metadata["compliance"] = {
            "evaluated": True,
            "gate": GATE,
            "scene": decision.scene_key,
            "streaming_mode": "buffered_snapshot",
            "transient_exposure_possible": False,
            "actions": list(decision.actions),
            "violation_types": sorted({hit.violation_type for hit in decision.hits}),
            **decision_types(decision),
            "audit_ref": decision.audit_ref,
            # `basis` and `notice` also ride the transient retract event, but that
            # event is gone after a page reload. Guide requirement 5 ("on the basis
            # of which rule") has to survive a refresh, so persist them here too.
            "basis": list(decision.basis),
            "notice": user_notice(decision),
            "retracted": retracted,
            "checks": checks,
        }
        return AIMessage(
            content=replacement,
            id=message.id,
            name=getattr(message, "name", None),
            response_metadata=metadata,
            additional_kwargs=dict(getattr(message, "additional_kwargs", None) or {}),
        )

    @staticmethod
    def _last_ai_message(state: AgentState) -> AIMessage | None:
        messages = (state or {}).get("messages") or []
        for message in reversed(messages):
            if isinstance(message, AIMessage):
                return message
        return None


def build_retract_event(message: AIMessage, replacement: str, decision: ComplianceDecision) -> dict[str, Any]:
    """Build the ``compliance_retract`` payload.

    Kept as a free function so the frontend contract can be asserted in tests
    without instantiating the middleware.
    """
    return {
        "type": RETRACT_EVENT,
        "message_id": str(message.id or ""),
        "scene": decision.scene_key,
        "streaming_mode": "compatible_retract",
        "violation_types": sorted({hit.violation_type for hit in decision.hits}),
        "action": list(decision.actions),
        **decision_types(decision),
        "replacement": replacement,
        "notice": user_notice(decision),
        "audit_ref": decision.audit_ref,
        "basis": list(decision.basis),
    }


__all__ = ["RETRACT_EVENT", "ComplianceOutputGateMiddleware", "REFUSAL_TEXT", "build_retract_event"]
