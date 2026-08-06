"""Translate the delivered normalized-row API to DeerFlow's detector contract."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from deerflow.compliance.contract import (
    DetectContext,
    DetectionHit,
    DetectionUnit,
    RiskLocation,
)
from deerflow.compliance.detectors.delivered_rules.configured_llm import (
    ConfiguredLLMAdapter,
)

MAX_LOCATION_TEXT = 256
MAX_LOCATIONS = 32
MAX_EVIDENCE_RULES = 32


def unit_to_delivery_sample(unit: DetectionUnit) -> tuple[dict[str, Any], str]:
    """Build the normalized shape consumed by the delivered detectors.

    Evaluation-only fields such as ``is_positive`` and expected actions are
    deliberately absent. Text items retain their order; field items retain
    their dotted paths so findings can be mapped back to the frozen unit.
    """

    content_text = "\n".join(
        item.text
        for item in unit.text_items
        if isinstance(item.text, str) and item.text.strip()
    )
    features = {item.path: item.value for item in unit.field_items}
    return (
        {
            "sample_id": unit.unit_id,
            "data_id": unit.unit_id,
            "data_type": unit.data_type or "",
            "gate": unit.gate,
            "trigger_gate": unit.gate,
            "content_text": content_text,
            "features": features,
        },
        content_text,
    )


def _bounded_text(value: Any) -> str | None:
    text = str(value or "")
    if not text or len(text) > MAX_LOCATION_TEXT:
        return None
    return text


def _location_from_delivery(
    risk: Mapping[str, Any],
    content_text: str,
) -> RiskLocation | None:
    field_path = str(risk.get("field_path") or "content_text")
    raw_text = str(risk.get("text") or "")
    entity_type = str(risk.get("risk_type") or "delivered_rule")

    if field_path != "content_text":
        return RiskLocation(
            kind="field_path",
            locator=field_path,
            text=_bounded_text(raw_text),
            entity_type=entity_type,
        )

    start = risk.get("start")
    end = risk.get("end")
    if isinstance(start, int) and isinstance(end, int) and 0 <= start < end <= len(content_text):
        # Some delivery rules report offsets for an unquoted assignment branch
        # as -1. Accept offsets only after validating the covered text.
        covered = content_text[start:end]
        if not raw_text or covered == raw_text:
            return RiskLocation(
                kind="char_span",
                locator=f"{start}:{end}",
                text=_bounded_text(covered),
                entity_type=entity_type,
            )

    if raw_text:
        located_at = content_text.find(raw_text)
        if located_at >= 0:
            return RiskLocation(
                kind="char_span",
                locator=f"{located_at}:{located_at + len(raw_text)}",
                text=_bounded_text(raw_text),
                entity_type=entity_type,
            )

    # A delivery rule sometimes marks the whole content value (for example an
    # internal domain + API combination). Preserve the actionable span without
    # copying an arbitrarily large sensitive payload into the hit object.
    if raw_text == content_text and content_text:
        return RiskLocation(
            kind="char_span",
            locator=f"0:{len(content_text)}",
            text=_bounded_text(content_text),
            entity_type=entity_type,
        )
    return None


def _locations(result: Mapping[str, Any], content_text: str) -> tuple[RiskLocation, ...]:
    converted: list[RiskLocation] = []
    seen: set[tuple[str, str, str | None, str | None]] = set()
    risks = result.get("risk_locations") or ()
    if not isinstance(risks, (list, tuple)):
        return ()

    for risk in risks:
        if not isinstance(risk, Mapping):
            continue
        location = _location_from_delivery(risk, content_text)
        if location is None:
            continue
        key = (location.kind, location.locator, location.text, location.entity_type)
        if key in seen:
            continue
        seen.add(key)
        converted.append(location)
        if len(converted) >= MAX_LOCATIONS:
            break
    return tuple(converted)


def _severity(violation_type: str, confidence: float) -> str:
    if violation_type == "hardcoded_cred":
        return "critical" if confidence >= 0.97 else "high"
    if confidence >= 0.90:
        return "high"
    if confidence >= 0.75:
        return "medium"
    return "low"


class DeliveredRulesDetector:
    """Reusable contract wrapper around one delivered rule implementation."""

    detector_id = ""
    violation_type = ""
    delivery_detector_class: type[Any]

    def __init__(self) -> None:
        self._params: dict[str, Any] = {}
        self._delivery_detector: Any | None = None

    def setup(self, params: Mapping[str, Any]) -> None:
        self._params = dict(params or {})
        use_llm = bool(self._params.get("use_llm", False))
        llm_adapter = None
        if use_llm:
            llm_adapter = ConfiguredLLMAdapter(
                max_content_chars=int(
                    self._params.get("llm_max_content_chars", 2500)
                ),
                max_output_tokens=int(
                    self._params.get("llm_max_output_tokens", 512)
                ),
                timeout_seconds=float(
                    self._params.get("llm_timeout_seconds", 7.0)
                ),
            )
        self._delivery_detector = self.delivery_detector_class(
            use_llm=use_llm,
            llm_adapter=llm_adapter,
        )

    def detect(
        self,
        unit: DetectionUnit,
        ctx: DetectContext,
    ) -> tuple[DetectionHit, ...]:
        if self._delivery_detector is None:
            self.setup(self._params)

        sample, content_text = unit_to_delivery_sample(unit)
        # Do not swallow delivery failures here. ComplianceEngine isolates each
        # detector and records its exception in diagnostics; converting an
        # implementation error into an empty result would look like a clean pass.
        llm_adapter = getattr(self._delivery_detector, "llm_adapter", None)
        if isinstance(llm_adapter, ConfiguredLLMAdapter):
            with llm_adapter.use_model(ctx.model_name):
                result = self._delivery_detector.detect(sample)
        else:
            result = self._delivery_detector.detect(sample)

        if not isinstance(result, Mapping) or not bool(result.get("is_hit")):
            return ()

        try:
            confidence = max(0.0, min(1.0, float(result.get("confidence") or 0.0)))
        except (TypeError, ValueError):
            confidence = 0.0

        raw_rules_value = result.get("matched_rules") or ()
        raw_rules = raw_rules_value if isinstance(raw_rules_value, (list, tuple)) else ()
        rules = tuple(
            str(rule)[:96]
            for rule in raw_rules
            if isinstance(rule, (str, int, float))
        )[:MAX_EVIDENCE_RULES]
        reason = str(result.get("reason") or "delivered_rule_match")[:240]

        return (
            DetectionHit(
                detector_id=self.detector_id,
                violation_type=self.violation_type,
                confidence=confidence,
                severity=_severity(self.violation_type, confidence),
                risk_locations=_locations(result, content_text),
                reason_code=rules[0] if rules else "delivered_rule_match",
                evidence={
                    "source": "compliance_detectors",
                    "matched_rules": rules,
                    "reason": reason,
                },
                basis=("项目交付合规规则",),
            ),
        )


__all__ = ["DeliveredRulesDetector", "unit_to_delivery_sample"]
