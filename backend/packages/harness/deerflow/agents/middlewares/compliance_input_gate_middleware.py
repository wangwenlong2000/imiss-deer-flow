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
from typing import Any, override

from langchain.agents import AgentState
from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.errors import GraphBubbleUp
from langgraph.runtime import Runtime

from deerflow.agents.thread_state import ThreadState
from deerflow.compliance.actions import REFUSAL_TEXT
from deerflow.compliance.contract import UNKNOWN_SCENE_KEY, IntentInfo, UserContext
from deerflow.compliance.normalizers.user_input import UploadedFileNormalizer, UserInputNormalizer
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
from deerflow.compliance.scene import COMPLIANCE_REQUEST_KEY, SCENE_CONTEXT_KEY, scene_origin_from_state
from deerflow.compliance.types import ComplianceDecision

logger = logging.getLogger(__name__)

GATE = "InputGate"


def _intent_from_state(state: AgentState) -> IntentInfo | None:
    """Read whatever ``IntentRecognitionMiddleware`` left in state.

    Tolerant by design: the intent schema belongs to the routing subsystem and
    may evolve. Returning ``None`` when nothing is recognizable is safe — the
    engine treats a missing intent as "cannot prove benign" and keeps the hits.
    """
    raw = (state or {}).get("intent_context") or (state or {}).get("routing_context") or {}
    if not isinstance(raw, dict):
        return None

    compliance = raw.get("compliance_intent")
    if not isinstance(compliance, dict):
        compliance = raw
    intent = raw.get("intent") or raw.get("intent_name") or raw.get("dialogue_act")
    scenes = raw.get("scenes") or raw.get("scene")
    scene_hint = scenes[0] if isinstance(scenes, list) and scenes else (scenes if isinstance(scenes, str) else None)
    if intent is None and scene_hint is None:
        return None

    return IntentInfo(
        intent=str(intent) if intent else None,
        scene_hint=str(scene_hint) if scene_hint else None,
        high_risk=bool(compliance.get("is_high_risk") or compliance.get("high_risk")),
        confidence=float(compliance.get("confidence") or raw.get("confidence") or 0.0),
        intent_type=str(compliance.get("intent_type") or "unknown"),
        requested_operation=str(compliance.get("requested_operation") or "unknown"),
        risk_level=str(compliance.get("risk_level") or "unknown"),
        reason_codes=tuple(str(code) for code in (compliance.get("reason_codes") or ())),
        reason=str(compliance.get("reason") or ""),
        source=str(compliance.get("source") or "legacy"),
    )


def _user_from_state(state: AgentState) -> UserContext | None:
    """Best-effort user context. Phase 1 rarely has one (plan risk 12)."""
    contract = (state or {}).get(COMPLIANCE_REQUEST_KEY) or {}
    actor = contract.get("actor") if isinstance(contract, dict) else {}
    raw = actor if isinstance(actor, dict) and actor else ((state or {}).get("user_context") or {})
    if not isinstance(raw, dict) or not raw:
        return None
    roles = raw.get("roles")
    return UserContext(
        user_id=raw.get("user_id"),
        roles=tuple(roles) if isinstance(roles, list) else (),
        org_id=raw.get("org_id"),
    )


class ComplianceInputGateMiddleware(AgentMiddleware[ThreadState]):
    """Check the latest user query before the agent acts on it."""

    state_schema = ThreadState

    def __init__(self, *, engine: Any = None) -> None:
        super().__init__()
        self._engine = engine
        self._normalizer = UserInputNormalizer()

    @override
    def before_agent(self, state: AgentState, runtime: Runtime) -> dict[str, Any] | None:
        return self._process(state, runtime)

    @override
    async def abefore_agent(self, state: AgentState, runtime: Runtime) -> dict[str, Any] | None:
        return self._process(state, runtime)

    # LangChain only installs conditional edges for ``jump_to`` when the hook
    # explicitly declares its legal destinations.  Without this metadata the
    # ``jump_to: end`` included in ``_block`` is persisted as ordinary state
    # but the graph follows its default edge to the model anyway.  That made a
    # correctly refused public-release request continue to execute.
    before_agent.__can_jump_to__ = ["end"]
    abefore_agent.__can_jump_to__ = ["end"]

    def _process(self, state: AgentState, runtime: Runtime) -> dict[str, Any] | None:
        if not gate_enabled(GATE):
            return None

        query = self._last_user_text(state)
        if not query:
            return None

        thread_id = self._thread_id(runtime)
        request_id = f"in-{uuid.uuid4().hex[:12]}"
        manual_context = compliance_context(runtime)
        scene_origin, scene_resolution = scene_origin_from_state(
            state,
            force=True,
            manual_context=manual_context,
        )
        has_contract = bool((state or {}).get(COMPLIANCE_REQUEST_KEY))
        has_scene_context = has_contract or scene_resolution.scene != UNKNOWN_SCENE_KEY

        try:
            decision = self._check(query, thread_id, state, request_id, runtime, scene_origin=scene_origin)
        except GraphBubbleUp:
            raise
        except Exception as exc:
            logger.exception("compliance: InputGate detection failed")
            decision = failure_decision(GATE, request_id, exc)
            if not decision.actions:
                return {SCENE_CONTEXT_KEY: scene_resolution.to_dict()} if has_scene_context else None
            return self._block(
                decision,
                scene_resolution.to_dict() if has_scene_context else None,
                content=FAIL_CLOSED_NOTICE,
            )

        if "refuse" in decision.actions:
            # Guide action 10: terminate the request before the model runs.  The
            # refusal is also stamped with the same durable metadata contract as
            # OutputGate retractions, so the frontend renders its structured,
            # destructive compliance card instead of treating this as ordinary
            # assistant Markdown.
            return self._block(decision, scene_resolution.to_dict() if has_scene_context else None)
        return {SCENE_CONTEXT_KEY: scene_resolution.to_dict()} if has_scene_context else None

    def _check(
        self,
        query: str,
        thread_id: str | None,
        state: AgentState,
        request_id: str,
        runtime: Runtime | None = None,
        *,
        scene_origin: dict[str, Any] | None = None,
    ) -> ComplianceDecision:
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
            user=user_context(runtime) or _user_from_state(state),
            intent=_intent_from_state(state),
            budget_ms=config.budget_ms,
            max_units=config.max_units,
            origin={
                "kind": "user_query",
                "compliance_context": compliance_context(runtime),
                **(scene_origin or {}),
            },
        )

    @staticmethod
    def _block(
        decision: ComplianceDecision,
        scene_context: dict[str, Any] | None = None,
        *,
        content: str = REFUSAL_TEXT,
    ) -> dict[str, Any]:
        """Refuse before the model runs, with a frontend-readable disposition."""
        notice = user_notice(decision)
        metadata = {
            "gate": GATE,
            "scene": decision.scene_key,
            "streaming_mode": "pre_model_refusal",
            "transient_exposure_possible": False,
            "actions": list(decision.actions),
            "violation_types": sorted({hit.violation_type for hit in decision.hits}),
            "audit_ref": decision.audit_ref,
            "basis": list(decision.basis),
            "notice": notice,
            "retracted": True,
        }
        message = AIMessage(
            content=content,
            response_metadata={"compliance": metadata},
        )
        update: dict[str, Any] = {"messages": [message], "jump_to": "end"}
        if scene_context is not None:
            update[SCENE_CONTEXT_KEY] = scene_context
        return update

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


def scan_upload_paths(paths: list[str], *, thread_id: str | None = None, engine: Any = None) -> ComplianceDecision:
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
