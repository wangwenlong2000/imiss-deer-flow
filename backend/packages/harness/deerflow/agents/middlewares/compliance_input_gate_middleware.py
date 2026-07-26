"""InputGate — check the user query as it enters the system.

Phase 1 ships the plumbing, not the detectors. The three model detectors this
work owns (types 8/9/10) do not declare ``InputGate``, and types 1-7 belong to
other owners. So with the shipped configuration this middleware runs, finds
nothing, and returns ``None`` — which is the expected outcome, not a bug.

What it does provide is the mounting point and the joint judgement, so a detector
added later needs no engine change.

The joint judgement (guide §9.7)
--------------------------------
A keyword hit is **not** a violation here. Only "sensitive entity + high-risk
intent + scene" together makes one. "Please mask the phone numbers in this
table" and "extract all these phone numbers for public release" contain
identical entities and mean opposite things.

That is why this middleware must run **after** ``IntentRecognitionMiddleware``:
it needs the recognized intent to make the call. ``IntentGuard`` inside the
engine does the actual combining; here we just pass the intent along.

Uploaded files are scanned separately, from the app side — see
``scan_upload_paths`` and its caller in ``app/gateway/routers/uploads.py``.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

try:
    from typing import override
except ImportError:  # pragma: no cover - Python < 3.12
    from typing_extensions import override

from langchain.agents import AgentState
from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.errors import GraphBubbleUp
from langgraph.runtime import Runtime

from deerflow.compliance.contract import IntentInfo, UserContext
from deerflow.compliance.normalizers.user_input import UploadedFileNormalizer, UserInputNormalizer
from deerflow.compliance.runtime import (
    FAIL_CLOSED_NOTICE,
    failure_decision,
    gate_config,
    gate_enabled,
    get_engine,
    user_notice,
)
from deerflow.compliance.types import ComplianceDecision

logger = logging.getLogger(__name__)

GATE = "InputGate"


def _intent_from_state(state: AgentState) -> "IntentInfo | None":
    """Read whatever ``IntentRecognitionMiddleware`` left in state.

    Tolerant by design: the intent schema belongs to the routing subsystem and
    may evolve. Returning ``None`` when nothing is recognizable is safe — the
    engine treats a missing intent as "cannot prove benign" and keeps the hits.
    """
    raw = (state or {}).get("intent_context") or (state or {}).get("routing_context") or {}
    if not isinstance(raw, dict):
        return None

    intent = raw.get("intent") or raw.get("intent_name") or raw.get("dialogue_act")
    scenes = raw.get("scenes") or raw.get("scene")
    scene_hint = scenes[0] if isinstance(scenes, list) and scenes else (scenes if isinstance(scenes, str) else None)
    if intent is None and scene_hint is None:
        return None

    return IntentInfo(
        intent=str(intent) if intent else None,
        scene_hint=str(scene_hint) if scene_hint else None,
        high_risk=bool(raw.get("high_risk") or raw.get("is_high_risk")),
        confidence=float(raw.get("confidence") or 0.0),
    )


def _user_from_state(state: AgentState) -> "UserContext | None":
    """Best-effort user context. Phase 1 rarely has one (plan risk 12)."""
    raw = (state or {}).get("user_context") or {}
    if not isinstance(raw, dict) or not raw:
        return None
    roles = raw.get("roles")
    return UserContext(
        user_id=raw.get("user_id"),
        roles=tuple(roles) if isinstance(roles, list) else (),
        org_id=raw.get("org_id"),
    )


class ComplianceInputGateMiddleware(AgentMiddleware[AgentState]):
    """Check the latest user query before the agent acts on it."""

    state_schema = AgentState

    def __init__(self, *, engine: Any = None) -> None:
        super().__init__()
        self._engine = engine
        self._normalizer = UserInputNormalizer()

    @override
    def before_agent(self, state: AgentState, runtime: Runtime) -> "dict[str, Any] | None":
        return self._process(state, runtime)

    @override
    async def abefore_agent(self, state: AgentState, runtime: Runtime) -> "dict[str, Any] | None":
        return self._process(state, runtime)

    def _process(self, state: AgentState, runtime: Runtime) -> "dict[str, Any] | None":
        if not gate_enabled(GATE):
            return None

        query = self._last_user_text(state)
        if not query:
            return None

        thread_id = self._thread_id(runtime)
        request_id = f"in-{uuid.uuid4().hex[:12]}"

        try:
            decision = self._check(query, thread_id, state, request_id)
        except GraphBubbleUp:
            raise
        except Exception as exc:
            logger.exception("compliance: InputGate detection failed")
            decision = failure_decision(GATE, request_id, exc)
            if not decision.actions:
                return None
            return self._block(FAIL_CLOSED_NOTICE)

        if "refuse" in decision.actions:
            # Guide action 10: terminate the request and return a standard notice.
            return self._block(user_notice(decision))
        return None

    def _check(self, query: str, thread_id: str | None, state: AgentState, request_id: str) -> ComplianceDecision:
        config = gate_config(GATE)
        units = self._normalizer.to_units(query, gate=GATE, thread_id=thread_id)
        if not units:
            return ComplianceDecision(request_id=request_id, gate=GATE, scene_key="_unknown")

        engine = self._engine or get_engine()
        return engine.check(
            units,
            gate=GATE,
            request_id=request_id,
            thread_id=thread_id,
            user=_user_from_state(state),
            intent=_intent_from_state(state),
            budget_ms=config.budget_ms,
            max_units=config.max_units,
            origin={"kind": "user_query"},
        )

    @staticmethod
    def _block(notice: str) -> dict[str, Any]:
        """Refuse the request by answering directly instead of running the agent."""
        return {"messages": [AIMessage(content=notice)], "jump_to": "end"}

    @staticmethod
    def _thread_id(runtime: Runtime) -> str | None:
        try:
            return (getattr(runtime, "context", None) or {}).get("thread_id") or getattr(runtime, "thread_id", None)
        except Exception:
            return None

    @staticmethod
    def _last_user_text(state: AgentState) -> str | None:
        for message in reversed((state or {}).get("messages") or []):
            if not isinstance(message, HumanMessage):
                continue
            content = message.content
            if isinstance(content, str) and content.strip():
                return content.strip()
            if isinstance(content, list):
                parts = [b["text"] for b in content if isinstance(b, dict) and isinstance(b.get("text"), str)]
                if parts:
                    return "\n".join(parts).strip()
        return None


def scan_upload_paths(paths: "list[str]", *, thread_id: str | None = None, engine: Any = None) -> ComplianceDecision:
    """Scan uploaded files at the input gate.

    Called from ``app/gateway/routers/uploads.py`` **after** the files land and
    any conversion runs, but **before** success is reported — so a violating
    upload is never acknowledged as accepted.

    App calls harness; the dependency direction stays legal
    (``test_harness_boundary.py``).
    """
    request_id = f"in-upload-{uuid.uuid4().hex[:12]}"
    config = gate_config(GATE)
    units = UploadedFileNormalizer().to_units(paths, gate=GATE, thread_id=thread_id)
    if not units:
        return ComplianceDecision(request_id=request_id, gate=GATE, scene_key="_unknown")

    resolved = engine or get_engine()
    return resolved.check(
        units,
        gate=GATE,
        request_id=request_id,
        thread_id=thread_id,
        budget_ms=config.budget_ms,
        max_units=config.max_units,
        origin={"kind": "uploaded_files", "count": len(paths)},
    )


__all__ = ["ComplianceInputGateMiddleware", "scan_upload_paths"]
