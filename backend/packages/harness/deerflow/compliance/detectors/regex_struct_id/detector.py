from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from deerflow.compliance.contract import (
    DetectContext,
    DetectionHit,
    DetectionUnit,
    RiskLocation,
)

from . import legacy_core


class StructIdDetector:
    """Type-1 lightweight in-process detector.

    The validated legacy rules remain in ``legacy_core.py``.  This class only
    adapts DeerFlow's DetectionUnit/DetectionHit contract to that rule engine.
    """

    detector_id = "regex_struct_id"

    def __init__(self) -> None:
        self._params: dict[str, Any] = {}
        self._presidio = legacy_core.OptionalPresidioAnalyzer("never")

    def setup(self, params: Mapping[str, Any]) -> None:
        self._params = dict(params or {})
        # The in-process plugin must remain light.  Presidio/spaCy will be
        # integrated separately through subprocess_cli or HTTP.
        self._presidio = legacy_core.OptionalPresidioAnalyzer("never")

    @staticmethod
    def _to_legacy_sample(unit: DetectionUnit) -> tuple[dict[str, Any], str]:
        content_text = "\n".join(
            item.text for item in unit.text_items
            if isinstance(item.text, str) and item.text.strip()
        )
        features = {
            item.path: item.value
            for item in unit.field_items
        }
        sample = {
            "sample_id": unit.unit_id,
            "data_id": unit.unit_id,
            "data_type": unit.data_type or "unknown",
            "gate": unit.gate,
            "trigger_gate": unit.gate,
            "content_text": content_text,
            "features": features,
        }
        return sample, content_text

    @staticmethod
    def _risk_location(risk: Mapping[str, Any], content_text: str) -> RiskLocation:
        field = str(risk.get("field") or "content_text")
        text = str(risk.get("text") or "")
        entity_type = str(risk.get("entity_type") or "structured_identifier")

        if field == "content_text":
            start = content_text.find(text) if text else -1
            if start >= 0:
                return RiskLocation(
                    kind="char_span",
                    locator=f"{start}:{start + len(text)}",
                    text=text or None,
                    entity_type=entity_type,
                )

        return RiskLocation(
            kind="field_path",
            locator=field,
            text=text or None,
            entity_type=entity_type,
        )

    def detect(
        self,
        unit: DetectionUnit,
        ctx: DetectContext,
    ) -> list[DetectionHit]:
        del ctx
        try:
            sample, content_text = self._to_legacy_sample(unit)
            result = legacy_core.detect_struct_id(sample, self._presidio)
        except Exception:
            # Ordinary malformed input must not bring down the compliance engine.
            return []

        if not bool(result.get("is_positive_pred")):
            return []

        raw_risks = result.get("risk_locations_pred") or []
        locations = tuple(
            self._risk_location(risk, content_text)
            for risk in raw_risks
            if isinstance(risk, Mapping)
        )

        confidence = max(0.0, min(1.0, float(result.get("confidence") or 0.0)))
        methods = sorted({
            str(risk.get("method"))
            for risk in raw_risks
            if isinstance(risk, Mapping) and risk.get("method")
        })
        reason_codes = sorted({
            str(risk.get("reason_code"))
            for risk in raw_risks
            if isinstance(risk, Mapping) and risk.get("reason_code")
        })

        return [
            DetectionHit(
                detector_id=self.detector_id,
                violation_type="struct_id",
                confidence=confidence,
                severity="high" if confidence >= 0.90 else "medium",
                risk_locations=locations,
                reason_code="struct_id_rule_match",
                evidence={
                    "legacy_detector_version": result.get("detector_version"),
                    "methods": methods,
                    "reason_codes": reason_codes,
                    "data_type": unit.data_type,
                },
                basis=("项目规则",),
            )
        ]
