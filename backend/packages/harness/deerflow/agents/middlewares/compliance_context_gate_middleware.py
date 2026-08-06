"""ContextGate — inspect retrieval results before they enter the LLM context.

Wraps every tool call. When the result is a ``SkillResult``, its evidence is
normalized into detection units, checked, and rewritten in place if the
disposition matrix says so. Anything that is not a ``SkillResult`` passes through
completely untouched.

Three things this middleware exists to get right
------------------------------------------------

**1. It must sit at the very front of the middleware list.**
``wrap_tool_call`` composes first-in-list = outermost, and
``ToolErrorHandlingMiddleware`` (appended last, therefore innermost) turns any
exception into an error ``ToolMessage``. If the compliance gate were inside it,
a detection failure would be silently converted into "tool failed, carry on" —
a compliance gate that fails open without saying so. ``test_compliance_gates.py``
asserts the position.

**2. Failure semantics are explicit.** ``fail_mode: closed`` (the default) means a
detection error blocks the evidence. ``open`` lets it through and says so loudly
in the diagnostics.

**3. Truncation is never silent.** With more evidence than the budget allows, the
highest-scoring items are kept and the tool result carries an explicit warning
naming how many were skipped. Checking 32 of 400 items and reporting nothing
would read as a clean pass.
"""

from __future__ import annotations

import copy
import json
import logging
import uuid
from collections.abc import Awaitable, Callable
from typing import Any, override

from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import ToolMessage
from langgraph.errors import GraphBubbleUp
from langgraph.prebuilt.tool_node import ToolCallRequest

from deerflow.agents.thread_state import ThreadState
from deerflow.compliance.actions import REFUSAL_TEXT, mask_text
from deerflow.compliance.normalizers.skill_result import (
    SkillResultNormalizer,
    evidence_scores,
    try_parse_skill_result,
)
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
from deerflow.compliance.scene import scene_origin_from_state
from deerflow.compliance.types import ComplianceDecision

logger = logging.getLogger(__name__)

GATE = "ContextGate"


class ComplianceContextGateMiddleware(AgentMiddleware[ThreadState]):
    """Check tool results for compliance violations before the model sees them."""

    state_schema = ThreadState

    def __init__(self, *, engine: Any = None) -> None:
        super().__init__()
        self._engine = engine  # injected in tests; resolved lazily in production
        self._normalizer = SkillResultNormalizer()

    # ── hooks ───────────────────────────────────────────────────────────────

    @override
    def wrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], ToolMessage | Any],
    ) -> ToolMessage | Any:
        message = handler(request)
        return self._inspect(message, request)

    @override
    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Any]],
    ) -> ToolMessage | Any:
        message = await handler(request)
        return self._inspect(message, request)

    # ── core ────────────────────────────────────────────────────────────────

    def _inspect(self, message: Any, request: ToolCallRequest) -> Any:
        if not gate_enabled(GATE):
            return message
        if not isinstance(message, ToolMessage):
            # Command and friends are control flow, not retrieved content.
            return message

        payload = try_parse_skill_result(message.content)
        if payload is None:
            return message

        tool_name = str(request.tool_call.get("name") or "unknown_tool")
        request_id = f"ctx-{uuid.uuid4().hex[:12]}"

        try:
            decision = self._check(payload, request_id, tool_name, request)
        except GraphBubbleUp:
            raise  # never swallow LangGraph control-flow signals
        except Exception as exc:
            logger.exception("compliance: ContextGate detection failed for tool %s", tool_name)
            decision = failure_decision(GATE, request_id, exc)
            if not decision.actions:
                return self._annotate_only(message, payload, decision)
            return self._rewrite(message, payload, decision, fail_closed=True)

        if not decision.actions:
            return message

        return self._rewrite(message, payload, decision, fail_closed=False)

    def _check(
        self,
        payload: dict[str, Any],
        request_id: str,
        tool_name: str,
        request: Any = None,
        *,
        state: Any = None,
    ) -> ComplianceDecision:
        config = gate_config(GATE)
        units = self._normalizer.to_units(payload, gate=GATE)
        if not units:
            return ComplianceDecision(request_id=request_id, gate=GATE, scene_key="_unknown")

        units = self._prioritize(units, payload, config.max_units)
        engine = self._engine or get_engine()
        manual_context = compliance_context(request)
        scene_origin, _ = scene_origin_from_state(
            state if state is not None else getattr(request, "state", None),
            manual_context=manual_context,
        )
        return engine.check(
            units,
            gate=GATE,
            request_id=request_id,
            user=user_context(request),
            budget_ms=config.budget_ms,
            max_units=config.max_units,
            origin={
                "tool_name": tool_name,
                "skill_name": payload.get("skill_name"),
                "compliance_context": compliance_context(request),
                **scene_origin,
            },
        )

    @staticmethod
    def _prioritize(units: tuple, payload: dict[str, Any], max_units: int) -> tuple:
        """Order units so the budget is spent on the most relevant evidence.

        Plan §6.2: truncate by evidence ``score`` descending rather than by
        arbitrary arrival order. Units without a score keep their original
        relative position behind the scored ones.
        """
        if len(units) <= max_units:
            return units
        scores = evidence_scores(payload)
        if not scores:
            return units

        def rank(index_and_unit: tuple[int, Any]) -> tuple[float, int]:
            index, unit = index_and_unit
            evidence_id = unit.unit_id.rsplit(":", 1)[-1]
            return (-scores.get(evidence_id, float("-inf")), index)

        ordered = sorted(enumerate(units), key=rank)
        return tuple(unit for _, unit in ordered)

    # ── rewriting ───────────────────────────────────────────────────────────

    def _annotate_only(self, message: ToolMessage, payload: dict[str, Any], decision: ComplianceDecision) -> ToolMessage:
        """Record diagnostics without changing the evidence (``fail_mode: open``)."""
        mutated = copy.deepcopy(payload)
        self._append_warnings(mutated, decision)
        return self._replace_content(message, mutated)

    def _rewrite(self, message: ToolMessage, payload: dict[str, Any], decision: ComplianceDecision, *, fail_closed: bool) -> ToolMessage:
        """Apply the disposition to the ``SkillResult`` and rebuild the message."""
        mutated = copy.deepcopy(payload)
        result = mutated.get("result")
        if isinstance(result, dict):
            if fail_closed or "refuse" in decision.actions:
                self._blank_result(result, FAIL_CLOSED_NOTICE if fail_closed else REFUSAL_TEXT)
            elif any(action in decision.actions for action in ("desensitize", "rewrite", "aggregate")):
                self._mask_result(result, decision)

        self._append_warnings(mutated, decision)
        return self._replace_content(message, mutated)

    @staticmethod
    def _blank_result(result: dict[str, Any], notice: str) -> None:
        """Drop the payload entirely, keeping the envelope shape intact.

        The model still receives a well-formed ``SkillResult``, so it can react
        sensibly instead of hitting a parse error it cannot interpret.
        """
        result["display_text"] = notice
        result["evidence"] = []
        result["findings"] = []
        summary = result.get("summary")
        if isinstance(summary, dict):
            summary["overview"] = notice

    @staticmethod
    def _mask_result(result: dict[str, Any], decision: ComplianceDecision) -> None:
        """Mask the reported risk locations, leaving the rest readable.

        Surgical beats wholesale: the model keeps the parts of the evidence it
        legitimately needs, and only the risky spans are starred out.
        """
        hits = list(decision.hits)
        located = {location.locator for hit in hits for location in hit.risk_locations}

        display = result.get("display_text")
        if isinstance(display, str) and display:
            masked, _ = mask_text(display, hits)
            result["display_text"] = masked

        evidence_list = result.get("evidence")
        if not isinstance(evidence_list, list):
            return
        for evidence in evidence_list:
            if not isinstance(evidence, dict):
                continue
            for key in ("data", "description"):
                value = evidence.get(key)
                if isinstance(value, str) and value:
                    masked, _ = mask_text(value, hits)
                    evidence[key] = masked
            metadata = evidence.get("metadata")
            if isinstance(metadata, dict):
                _mask_mapping(metadata, located, prefix="metadata")
            data = evidence.get("data")
            if isinstance(data, dict):
                _mask_mapping(data, located, prefix="data")

    @staticmethod
    def _append_warnings(payload: dict[str, Any], decision: ComplianceDecision) -> None:
        """Surface the determination in ``diagnostics.warnings``.

        The model reads this. It is how a downstream answer can say "some
        evidence was withheld for compliance reasons" instead of quietly
        reasoning over a hole.
        """
        diagnostics = payload.setdefault("diagnostics", {})
        if not isinstance(diagnostics, dict):
            diagnostics = {}
            payload["diagnostics"] = diagnostics
        warnings = diagnostics.setdefault("warnings", [])
        if not isinstance(warnings, list):
            warnings = []
            diagnostics["warnings"] = warnings

        if decision.actions or decision.hits:
            warnings.append(user_notice(decision))
        warnings.extend(decision.diagnostics.warnings)

        diagnostics["compliance"] = {
            "gate": GATE,
            "scene": decision.scene_key,
            "actions": list(decision.actions),
            "violation_types": sorted({hit.violation_type for hit in decision.hits}),
            "audit_ref": decision.audit_ref,
            "units_checked": decision.diagnostics.units_checked,
            "units_total": decision.diagnostics.units_total,
            "truncated": decision.diagnostics.truncated,
        }

    @staticmethod
    def _replace_content(message: ToolMessage, payload: dict[str, Any]) -> ToolMessage:
        return ToolMessage(
            content=json.dumps(payload, ensure_ascii=False, indent=2),
            tool_call_id=message.tool_call_id,
            name=message.name,
            status=message.status,
            id=message.id,
        )


def _mask_mapping(mapping: dict[str, Any], located: set[str], *, prefix: str) -> None:
    """Star out mapping values whose dotted path was reported as risky."""
    for key, value in list(mapping.items()):
        path = f"{prefix}.{key}"
        if path in located and isinstance(value, (str, int, float)):
            mapping[key] = "*" * len(str(value))
        elif isinstance(value, dict):
            _mask_mapping(value, located, prefix=path)


__all__ = ["ComplianceContextGateMiddleware"]
