"""ContextGate — inspect tool results before they enter the LLM context.

Wraps every tool call. ``SkillResult`` evidence is normalized and rewritten in
place if the disposition matrix says so. Plain text tool results are also
checked; image blocks are ignored.

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
from deerflow.compliance.actions import (
    AGGREGATE_NOTICE,
    REFUSAL_TEXT,
    REWRITE_NOTICE,
    has_terminal_refusal,
    mask_text,
)
from deerflow.compliance.normalizers.skill_result import (
    SkillResultNormalizer,
    evidence_scores,
    try_parse_skill_result,
)
from deerflow.compliance.normalizers.base import MAX_FIELD_CHARS, stringify
from deerflow.compliance.normalizers.llm_output import LlmOutputNormalizer
from deerflow.compliance.runtime import (
    FAIL_CLOSED_NOTICE,
    INCOMPLETE_SCAN_NOTICE,
    compliance_context,
    decision_types,
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

    def __init__(self, *, engine: Any = None, model_name: str | None = None) -> None:
        super().__init__()
        self._engine = engine  # injected in tests; resolved lazily in production
        self._model_name = model_name
        self._normalizer = SkillResultNormalizer()
        self._text_normalizer = LlmOutputNormalizer()

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

        tool_name = str(request.tool_call.get("name") or "unknown_tool")
        request_id = f"ctx-{uuid.uuid4().hex[:12]}"

        payload = try_parse_skill_result(message.content)
        if payload is None:
            text = self._plain_tool_text(message.content)
            if not text.strip():
                return message
            try:
                decision = self._check_text(text, request_id, tool_name, request)
            except GraphBubbleUp:
                raise
            except Exception as exc:
                logger.exception("compliance: ContextGate detection failed for tool %s", tool_name)
                decision = failure_decision(GATE, request_id, exc)
                if not decision.actions:
                    return message
                return self._replace_text(message, FAIL_CLOSED_NOTICE)

            if decision.diagnostics.truncated:
                return self._replace_text(message, INCOMPLETE_SCAN_NOTICE)
            if not decision.actions or set(decision.actions) <= {"allow"}:
                return message
            if has_terminal_refusal(decision.actions, decision.per_violation_actions):
                return self._replace_text(message, REFUSAL_TEXT)

            replacement = apply_actions(text, decision.actions, decision.hits)
            if replacement is not None:
                return self._replace_text(message, replacement)
            if decision.actions or decision.hits:
                return self._replace_text(message, f"{text}\n\n{user_notice(decision)}")
            return message

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

        if decision.diagnostics.truncated:
            return self._rewrite(
                message,
                payload,
                decision,
                fail_closed=True,
                failure_notice=INCOMPLETE_SCAN_NOTICE,
            )

        # `allow` is a real matrix disposition, not a warning-only result.  A
        # self-use hit must remain byte-for-byte unchanged at this gate.
        if not decision.actions or set(decision.actions) <= {"allow"}:
            # A non-applicable hit has no action by design, but the engine may
            # have emitted a routing warning. Preserve that warning in the
            # structured envelope instead of silently passing a misrouted
            # detector result through the RAG boundary.
            if decision.hits and decision.diagnostics.warnings:
                return self._annotate_only(message, payload, decision)
            return message

        return self._rewrite(message, payload, decision, fail_closed=False)

    def _check_text(self, text: str, request_id: str, tool_name: str, request: Any) -> ComplianceDecision:
        config = gate_config(GATE)
        message_id = str(getattr(request, "id", None) or request_id)
        units = self._text_normalizer.to_units(text, gate=GATE, message_id=message_id)
        if not units:
            return ComplianceDecision(request_id=request_id, gate=GATE, scene_key="_unknown")

        engine = self._engine or get_engine()
        manual_context = compliance_context(request)
        scene_origin, _ = scene_origin_from_state(
            getattr(request, "state", None),
            manual_context=manual_context,
        )
        return engine.check(
            units,
            gate=GATE,
            request_id=request_id,
            user=user_context(request),
            model_name=self._model_name,
            budget_ms=config.budget_ms,
            max_units=config.max_units,
            origin={
                "tool_name": tool_name,
                "compliance_context": manual_context,
                **scene_origin,
            },
        )

    @staticmethod
    def _plain_tool_text(content: Any) -> str:
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            non_image = [
                block
                for block in content
                if not (
                    isinstance(block, dict)
                    and (
                        str(block.get("type") or "").lower() in {"image", "image_url"}
                        or "image_url" in block
                        or str(block.get("mime_type") or "").lower().startswith("image/")
                    )
                )
            ]
            # Preserve a sentinel character past the normalization bound so
            # the engine can mark this result incomplete and fail closed.
            return stringify(non_image, limit=MAX_FIELD_CHARS + 1)
        return stringify(content, limit=MAX_FIELD_CHARS + 1)

    @staticmethod
    def _replace_text(message: ToolMessage, content: str) -> ToolMessage:
        return ToolMessage(
            content=content,
            tool_call_id=message.tool_call_id,
            name=message.name,
            status=message.status,
            id=message.id,
        )

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
            model_name=self._model_name,
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

    def _rewrite(
        self,
        message: ToolMessage,
        payload: dict[str, Any],
        decision: ComplianceDecision,
        *,
        fail_closed: bool,
        failure_notice: str = FAIL_CLOSED_NOTICE,
    ) -> ToolMessage:
        """Apply the disposition to the ``SkillResult`` and rebuild the message."""
        mutated = copy.deepcopy(payload)
        result = mutated.get("result")
        if isinstance(result, dict):
            transform_actions = set(decision.actions) & {"aggregate", "desensitize", "rewrite"}
            if fail_closed or has_terminal_refusal(decision.actions, decision.per_violation_actions):
                self._blank_result(result, failure_notice if fail_closed else REFUSAL_TEXT)
            elif transform_actions:
                changed = self._mask_result(result, decision)
                if not changed:
                    # Matrix cells that offer a transformation with refusal as
                    # a fallback must never leak the original simply because a
                    # detector returned a field location the structured result
                    # could not apply.
                    self._blank_result(result, REFUSAL_TEXT)
                else:
                    if "aggregate" in transform_actions:
                        result["display_text"] = f"{AGGREGATE_NOTICE}\n{result.get('display_text', '')}".strip()
                    if "rewrite" in transform_actions:
                        result["display_text"] = f"{REWRITE_NOTICE}\n{result.get('display_text', '')}".strip()
            elif "refuse" in decision.actions:
                self._blank_result(result, REFUSAL_TEXT)

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
        result["artifacts"] = []
        result["summary"] = {"overview": notice}
        # Skill implementations may add extension fields.  Refusal must clear
        # those too instead of assuming only the canonical four keys exist.
        for key in list(result):
            if key not in {"display_text", "evidence", "findings", "artifacts", "summary"}:
                result[key] = None

    @staticmethod
    def _mask_result(result: dict[str, Any], decision: ComplianceDecision) -> bool:
        """Mask the reported risk locations, leaving the rest readable.

        Surgical beats wholesale: the model keeps the parts of the evidence it
        legitimately needs, and only the risky spans are starred out.
        """
        hits = list(decision.hits)
        located = {location.locator for hit in hits for location in hit.risk_locations}
        changed = False

        # Literal locations and character spans can appear in display text,
        # findings, artifact URIs, or extension fields.  Walk all values so a
        # sensitive string is not hidden merely because it was duplicated
        # outside evidence[].
        literal_texts = sorted(
            {
                location.text
                for hit in hits
                for location in hit.risk_locations
                if location.text
            },
            key=len,
            reverse=True,
        )

        def mask_literals(value: Any) -> Any:
            nonlocal changed
            if isinstance(value, str):
                masked = value
                for literal in literal_texts:
                    if literal in masked:
                        masked = masked.replace(literal, "*" * len(literal))
                        changed = True
                return masked
            if isinstance(value, dict):
                for key, item in list(value.items()):
                    value[key] = mask_literals(item)
                return value
            if isinstance(value, list):
                for index, item in enumerate(value):
                    value[index] = mask_literals(item)
                return value
            return value

        mask_literals(result)

        # Character offsets are meaningful only in the text-bearing fields from
        # which the normalizer built a TextItem.  Do not apply an output span
        # such as `0:12` to unrelated titles/URIs at the same array level.
        def mask_text_field(container: dict[str, Any], key: str) -> None:
            nonlocal changed
            value = container.get(key)
            if isinstance(value, str):
                masked, did_change = mask_text(value, hits)
                container[key] = masked
                changed = changed or did_change

        mask_text_field(result, "display_text")

        evidence_list = result.get("evidence")
        if isinstance(evidence_list, list):
            for evidence in evidence_list:
                if not isinstance(evidence, dict):
                    continue
                for key in ("title", "description", "data", "text", "message"):
                    mask_text_field(evidence, key)
                changed = _mask_mapping(evidence.get("metadata"), located, prefix="metadata") or changed
                changed = _mask_mapping(evidence.get("data"), located, prefix="data") or changed

        findings = result.get("findings")
        if isinstance(findings, list):
            for finding in findings:
                if isinstance(finding, dict):
                    for key in ("title", "description", "data", "text", "message"):
                        mask_text_field(finding, key)
                    changed = _mask_mapping(finding, located, prefix="findings") or changed

        artifacts = result.get("artifacts")
        if isinstance(artifacts, list):
            for artifact in artifacts:
                if isinstance(artifact, dict):
                    changed = _mask_mapping(artifact, located, prefix="artifacts") or changed
        return changed

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
            **decision_types(decision),
            "audit_ref": decision.audit_ref,
            "basis": list(decision.basis),
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


def _mask_mapping(mapping: Any, located: set[str], *, prefix: str) -> bool:
    """Star out mapping values whose dotted path was reported as risky."""
    if not isinstance(mapping, (dict, list)):
        return False
    changed = False
    if isinstance(mapping, list):
        for index, value in enumerate(mapping):
            changed = _mask_mapping(value, located, prefix=f"{prefix}[{index}]") or changed
        return changed
    for key, value in list(mapping.items()):
        path = f"{prefix}.{key}"
        if path in located and isinstance(value, (str, int, float)):
            mapping[key] = "*" * len(str(value))
            changed = True
        elif isinstance(value, dict):
            changed = _mask_mapping(value, located, prefix=path) or changed
        elif isinstance(value, list):
            changed = _mask_mapping(value, located, prefix=path) or changed
    return changed


__all__ = ["ComplianceContextGateMiddleware"]
