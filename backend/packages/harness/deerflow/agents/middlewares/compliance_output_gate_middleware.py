"""OutputGate — streaming with retract-on-hit. Never buffered.

The answer keeps streaming token by token, exactly as before. When a violation
lands, it is *retracted* rather than having been withheld.

Two layers, and both are required
---------------------------------

::

    LLM streams tokens ──► frontend renders as they arrive
            │
            ├─ layer 1: incremental scan (configurable)
            │    every N characters, run the gate; retract as early as possible
            │
            └─ layer 2: after_model on the complete message (always on)
                  │
                  hit ──┬──► a `compliance_retract` custom stream event
                        │      the frontend clears the message and shows the
                        │      replacement immediately
                        │
                        └──► a rewritten AIMessage returned from after_model
                               so the checkpoint / values snapshot never holds
                               the original

They solve different problems. Layer 1+2's event gets it **off the screen the
user is looking at**. The rewritten message keeps it **out of the persisted
state**, so reloading the page, browsing history or exporting a report does not
resurrect it. Layer 1 alone means a refresh brings the violation back; layer 2
alone leaves it sitting on screen.

Where the rewrite happens, and why not in ``after_model``
---------------------------------------------------------
The authoritative rewrite is in **``wrap_model_call``**, not ``after_model``.

``after_model`` looked like the natural place and was wrong. It runs in
**reverse** middleware order, so this gate — mounted first — ran *last*, and
every other ``after_model`` middleware saw the violating text first. Two of them
kept it:

* ``RawTranscriptMiddleware`` snapshots into ``raw_messages``, whose reducer
  dedups by id **keeping the first** — so the original was locked in permanently
  and the frontend, which reads ``raw_messages`` first, showed it again after a
  page reload.
* ``TitleMiddleware`` feeds the first assistant answer into a prompt and sends it
  to an **external model**, then persists the returned title.

``wrap_model_call`` composes first-in-list = **outermost**, and langchain's
``_build_commands`` does ``{"messages": response.result}``. Sanitizing there means
the original never enters graph state at all — no reducer, no transcript, no
title, no checkpoint, no SSE frame can observe it. The guarantee becomes
structural instead of order-dependent, and it is the same
"compliance gates go first" rule that ``wrap_tool_call`` already follows.

``after_model`` is kept as a **backstop**, gated on a digest of the text already
cleared. On the normal path it costs nothing. It earns its place when something
mutates the message after the model node — ``LoopDetectionMiddleware`` appends a
hard-stop notice to the content, turning a tool-calling message (which this gate
skips by design) into a user-facing answer.

``test_compliance_gates.py`` and ``test_compliance_output_gate_isolation.py``
assert both properties rather than trusting this comment.

Honest limitation
-----------------
Retraction is after-the-fact, not prevention. Between a violating token being
emitted and the retraction arriving there is a **visible window** — roughly the
scan interval with incremental scanning on, or the rest of the generation
without it. Anything a user screenshots or simply reads inside that window
cannot be recalled. That is inherent to not buffering; ``interval_chars`` is the
dial that trades window size against CPU.
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

from deerflow.compliance.actions import REFUSAL_TEXT, apply_actions
from deerflow.compliance.normalizers.llm_output import LlmOutputNormalizer, extract_text
from deerflow.compliance.runtime import (
    FAIL_CLOSED_NOTICE,
    compliance_context,
    failure_decision,
    gate_config,
    gate_enabled,
    get_engine,
    user_context,
    user_notice,
)
from deerflow.compliance.types import ComplianceDecision

logger = logging.getLogger(__name__)

GATE = "OutputGate"

#: Custom stream event type the frontend listens for.
RETRACT_EVENT = "compliance_retract"


def _digest(text: str) -> str:
    """Identify text already cleared, so the backstop can skip it."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class ComplianceOutputGateMiddleware(AgentMiddleware[AgentState]):
    """Check the model's answer, retract and rewrite it when it violates."""

    state_schema = AgentState

    def __init__(self, *, engine: Any = None) -> None:
        super().__init__()
        self._engine = engine
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
        return self._sanitize_response(handler(request), request)

    @override
    async def awrap_model_call(self, request: Any, handler: Callable[[Any], Awaitable[Any]]) -> Any:
        return self._sanitize_response(await handler(request), request)

    @override
    def after_model(self, state: AgentState, runtime: Runtime) -> dict[str, Any] | None:
        return self._process(state, runtime)

    @override
    async def aafter_model(self, state: AgentState, runtime: Runtime) -> dict[str, Any] | None:
        return self._process(state, runtime)

    # ── authoritative sanitization ──────────────────────────────────────────

    def _sanitize_response(self, response: Any, request: Any = None) -> Any:
        """Replace the model's answer inside the response, before it reaches state.

        ``_build_commands`` in langchain's factory does ``{"messages": response.result}``,
        so whatever is returned here is the *only* version any downstream component
        — RawTranscript, Title, the SSE frames, the checkpoint — will ever observe.
        """
        result = getattr(response, "result", None)
        if not isinstance(result, list) or not result:
            return response

        index = next((i for i in range(len(result) - 1, -1, -1) if isinstance(result[i], AIMessage)), None)
        if index is None:
            return response

        update = self._process({"messages": list(result)}, request)
        if update is None:
            return response

        new_result = list(result)
        new_result[index] = update["messages"][0]
        # dataclasses.replace keeps structured_response and any sibling messages.
        return dataclasses.replace(response, result=new_result)

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
            decision = self._check(text, message, request_id, runtime_or_request)
        except GraphBubbleUp:
            raise
        except Exception as exc:
            logger.exception("compliance: OutputGate detection failed")
            decision = failure_decision(GATE, request_id, exc)
            if not decision.actions:
                return None
            return self._retract(message, FAIL_CLOSED_NOTICE, decision)

        if not decision.actions:
            self._scan_offsets.pop(message_id, None)
            if message_id:
                self._checked[message_id] = _digest(text)
            return None

        replacement = apply_actions(text, decision.actions, decision.hits)
        if replacement is None:
            # warn / manual_review only: nothing to rewrite, but the user should
            # still be told the answer was flagged.
            return self._notice_only(message, decision)

        return self._retract(message, replacement, decision)

    def _check(self, text: str, message: AIMessage, request_id: str, runtime_or_request: Any = None) -> ComplianceDecision:
        config = gate_config(GATE)
        units = self._normalizer.to_units(message, gate=GATE, message_id=str(message.id or ""))
        if not units:
            return ComplianceDecision(request_id=request_id, gate=GATE, scene_key="_unknown")

        engine = self._engine or get_engine()
        return engine.check(
            units,
            gate=GATE,
            request_id=request_id,
            user=user_context(runtime_or_request),
            budget_ms=config.budget_ms,
            max_units=config.max_units,
            origin={
                "message_id": str(message.id or ""),
                "chars": len(text),
                "compliance_context": compliance_context(runtime_or_request),
            },
        )

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

    def _retract(self, message: AIMessage, replacement: str, decision: ComplianceDecision) -> dict[str, Any]:
        self.emit_retract_event(message, replacement, decision)
        self._remember(message, replacement)
        return {"messages": [self._rewritten(message, replacement, decision)]}

    def _remember(self, message: AIMessage, replacement: str) -> None:
        """Record the sanitized text so the backstop treats it as already cleared."""
        message_id = str(message.id or "")
        if message_id:
            self._checked[message_id] = _digest(replacement)

    def _notice_only(self, message: AIMessage, decision: ComplianceDecision) -> dict[str, Any] | None:
        """warn / manual_review: keep the answer, append the compliance notice."""
        text = extract_text(message.content)
        annotated = f"{text}\n\n{user_notice(decision)}"
        self.emit_retract_event(message, annotated, decision)
        self._remember(message, annotated)
        return {"messages": [self._rewritten(message, annotated, decision)]}

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
    def _rewritten(message: AIMessage, replacement: str, decision: ComplianceDecision) -> AIMessage:
        """Rebuild the message so persisted state holds only the safe version.

        The id is preserved so LangGraph's message reducer replaces the original
        rather than appending — that is what keeps the violation out of the
        checkpoint.
        """
        metadata = dict(getattr(message, "response_metadata", None) or {})
        metadata["compliance"] = {
            "gate": GATE,
            "actions": list(decision.actions),
            "violation_types": sorted({hit.violation_type for hit in decision.hits}),
            "audit_ref": decision.audit_ref,
            # `basis` and `notice` also ride the transient retract event, but that
            # event is gone after a page reload. Guide requirement 5 ("on the basis
            # of which rule") has to survive a refresh, so persist them here too.
            "basis": list(decision.basis),
            "notice": user_notice(decision),
            "retracted": True,
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
        "violation_types": sorted({hit.violation_type for hit in decision.hits}),
        "action": list(decision.actions),
        "replacement": replacement,
        "notice": user_notice(decision),
        "audit_ref": decision.audit_ref,
        "basis": list(decision.basis),
    }


__all__ = ["RETRACT_EVENT", "ComplianceOutputGateMiddleware", "REFUSAL_TEXT", "build_retract_event"]
