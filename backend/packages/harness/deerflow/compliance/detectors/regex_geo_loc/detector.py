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


class GeoLocDetector:
    """Type-2 lightweight in-process detector.

    The validated geo-location rules remain in ``legacy_core.py``.  Presidio
    LOCATION is auxiliary only and is disabled in this light online plugin.
    """

    detector_id = "regex_geo_loc"

    def __init__(self) -> None:
        self._params: dict[str, Any] = {}
        self._presidio = legacy_core.OptionalPresidio("never")
        self._exclude_data_types: set[str] = set()

    def setup(self, params: Mapping[str, Any]) -> None:
        self._params = dict(params or {})
        self._presidio = legacy_core.OptionalPresidio("never")
        raw_excluded = self._params.get("exclude_data_types") or ()
        self._exclude_data_types = {
            str(value) for value in raw_excluded
        }

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
            "data_type": unit.data_type or "",
            "gate": unit.gate,
            "trigger_gate": unit.gate,
            "content_text": content_text,
            "features": features,
        }
        return sample, content_text

    @staticmethod
    def _risk_location(risk: Mapping[str, Any], content_text: str) -> RiskLocation:
        field_path = str(risk.get("field_path") or "content_text")
        text = str(risk.get("text") or "")
        entity_type = str(risk.get("risk_type") or "precise_geo_location")

        if field_path == "content_text":
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
            locator=field_path,
            text=text or None,
            entity_type=entity_type,
        )

    @classmethod
    def _risk_locations(
        cls,
        risks: list[Mapping[str, Any]],
        content_text: str,
    ) -> tuple[RiskLocation, ...]:
        """Prefer structured evidence when text and fields report one value.

        UserInputNormalizer keeps raw text for fallback detection, so the same
        coordinate can appear once as a text span and once as a field. Distinct
        field paths remain separate because they represent separate sources.
        """
        locations = [cls._risk_location(risk, content_text) for risk in risks if isinstance(risk, Mapping)]
        field_values = {
            (location.entity_type, location.text)
            for location in locations
            if location.kind == "field_path" and location.text
        }
        seen: set[tuple[str, str, str | None, str | None]] = set()
        kept: list[RiskLocation] = []
        for location in locations:
            if location.kind == "char_span" and (location.entity_type, location.text) in field_values:
                continue
            key = (location.kind, location.locator, location.text, location.entity_type)
            if key in seen:
                continue
            seen.add(key)
            kept.append(location)
        return tuple(kept)

    @staticmethod
    def _is_explicit_synthetic_remote_coordinate(unit: DetectionUnit) -> bool:
        """Exclude coordinates explicitly marked synthetic in remote metadata."""
        data_type = str(unit.data_type or "").casefold().replace("-", "_")
        if data_type not in {"remote_sensing", "satellite_remote_sensing"}:
            return False
        return any(
            "synthetic" in item.path.casefold()
            and str(item.value).casefold() in {"true", "1", "yes"}
            for item in unit.field_items
        )

    def detect(
        self,
        unit: DetectionUnit,
        ctx: DetectContext,
    ) -> list[DetectionHit]:
        del ctx
        if self._is_explicit_synthetic_remote_coordinate(unit):
            return []
        try:
            sample, content_text = self._to_legacy_sample(unit)
            result = legacy_core.detect_geo_loc(
                sample,
                self._presidio,
                self._exclude_data_types,
            )
        except Exception:
            return []

        if not bool(result.get("is_positive_pred")):
            return []

        raw_risks = result.get("risk_locations_pred") or []
        locations = self._risk_locations(raw_risks, content_text)

        confidence = max(0.0, min(1.0, float(result.get("confidence") or 0.0)))
        methods = sorted({
            str(risk.get("method"))
            for risk in raw_risks
            if isinstance(risk, Mapping) and risk.get("method")
        })

        return [
            DetectionHit(
                detector_id=self.detector_id,
                violation_type="geo_loc",
                confidence=confidence,
                severity="high" if confidence >= 0.90 else "medium",
                risk_locations=locations,
                reason_code=str(result.get("reason_code") or "geo_loc_rule_match"),
                evidence={
                    "legacy_detector_version": legacy_core.DETECTOR_VERSION,
                    "methods": methods,
                    "data_type": unit.data_type,
                },
                basis=("项目规则",),
            )
        ]
