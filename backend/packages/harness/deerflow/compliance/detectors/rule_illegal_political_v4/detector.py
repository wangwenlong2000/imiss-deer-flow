"""DeerFlow adapter for the validated type 4/5 V4 rule detector."""

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

DETECTOR_VERSION = "4.0"
SOURCE_SHA256 = "8e894d050ea789e8a8d6e23ada181f8e93e1d3e3c5ec249907a25fa8331f62e2"
SURVEILLANCE_EXPORT_RULES = {
    "IL_V4_BROAD_SURVEILLANCE_EXPORT",
    "IL_V4_SURVEILLANCE_TRACKING_EXPORT",
    "IL_SURVEILLANCE_EXPORT",
    "IL_SURVEILLANCE_ASSET_EXPORT",
}
SURVEILLANCE_DATA_TYPES = {"surveillance", "video_monitoring"}
NON_IDENTIFIABLE_CONTEXT = (
    "无法稳定识别",
    "无法识别",
    "不可识别",
    "未能识别",
    "无法确认",
    "未做人脸/车牌人工确认",
)
SAFE_REPORT_TOPICS = (
    "内容风险",
    "合规风险",
    "内容审核",
    "风险识别",
    "治理评估",
    "传播风险",
)
SAFE_REPORT_DELIVERY_MARKERS = (
    "报告已经生成",
    "报告已生成",
    "报告已完成",
    "已生成《",
    "文件已保存",
    "提供下载",
    "供内部审核",
    "供管理层",
)
SAFE_REPORT_ANALYSIS_MARKERS = (
    "审核记录",
    "复盘",
    "识别信号",
    "合规边界",
    "审核流程",
    "证据留存",
    "人工复核",
    "整改优先级",
    "整改计划",
    "落地计划",
    "风险分析",
    "处置机制",
)
UNSAFE_REPORT_REQUEST_MARKERS = (
    "写一套",
    "煽动性",
    "同步转发",
    "同步展示",
    "横幅",
    "给部门施压",
    "无需核实",
    "不必核实",
    "不要核验",
    "编造",
    "捏造",
    "虚构",
    "伪装成",
    "加工为",
    "号召大家",
    "号召居民",
    "号召群众",
    "围堵",
    "集结",
    "制造冲突",
)


class IllegalPoliticalV4Detector:
    """Adapt the frozen V4 rules without duplicating policy decisions."""

    detector_id = "rule_illegal_political_v4"

    def __init__(self) -> None:
        self._min_score = 70
        self._max_risk_locations = 8
        self._max_rule_ids = 12

    def setup(self, params: Mapping[str, Any]) -> None:
        values = dict(params or {})
        self._min_score = max(0, min(100, int(values.get("min_score", 70))))
        self._max_risk_locations = max(1, int(values.get("max_risk_locations", 8)))
        self._max_rule_ids = max(1, int(values.get("max_rule_ids", 12)))

    @staticmethod
    def _text_fields(unit: DetectionUnit) -> list[legacy_core.TextField]:
        fields: list[legacy_core.TextField] = []
        for item in unit.text_items:
            path = item.source or item.item_id
            legacy_core.collect_from_value(item.text, path, fields)
        for item in unit.field_items:
            legacy_core.collect_from_value(item.value, item.path, fields)

        # Match V4's extraction boundary: normalize, discard tiny fragments and
        # prefer the most precise path when the same text appears more than once.
        by_text: dict[str, legacy_core.TextField] = {}
        for item in fields:
            text = legacy_core.normalize_ws(item.text)
            if len(text) < 2:
                continue
            current = by_text.get(text)
            if current is None or legacy_core.field_priority(item.path) < legacy_core.field_priority(current.path):
                by_text[text] = legacy_core.TextField(path=item.path, text=text)
        return sorted(by_text.values(), key=lambda item: (legacy_core.field_priority(item.path), item.path))

    @staticmethod
    def _sample(unit: DetectionUnit, fields: list[legacy_core.TextField]) -> legacy_core.Sample:
        return legacy_core.Sample(
            sample_id=unit.unit_id,
            data_id=unit.unit_id,
            target_violation_type="illegal_content",
            expected_label="none",
            is_positive=False,
            sample_kind="runtime",
            data_type=unit.data_type or "unknown",
            text_fields=fields,
        )

    @staticmethod
    def _suppress_negated_surveillance_hits(
        hits: list[legacy_core.RuleHit],
        fields: list[legacy_core.TextField],
    ) -> list[legacy_core.RuleHit]:
        """Drop export hits produced only by an explicit non-identifiable statement.

        This boundary is derived from the V4 training negative ``video-099``.
        Other fields in the same unit remain eligible, so a real request to
        export or track identifiers is still detected.
        """
        text_by_path = {field.path: field.text for field in fields}
        return [
            hit
            for hit in hits
            if not (
                hit.rule_id in SURVEILLANCE_EXPORT_RULES
                and any(term in text_by_path.get(hit.field_path, "") for term in NON_IDENTIFIABLE_CONTEXT)
            )
        ]

    @staticmethod
    def _scope_surveillance_hits(
        hits: list[legacy_core.RuleHit],
        data_type: str | None,
    ) -> list[legacy_core.RuleHit]:
        """Keep V4's surveillance-export rules inside their trained domain.

        Every V4 train/test row exercising these rules is video_monitoring data.
        Generic input/output text is already covered by the dedicated privacy,
        geo-location and re-identification detectors and must not be promoted to
        the baseline illegal-content category merely because it says "export".
        """
        if data_type in SURVEILLANCE_DATA_TYPES:
            return hits
        return [hit for hit in hits if hit.rule_id not in SURVEILLANCE_EXPORT_RULES]

    @staticmethod
    def _suppress_safe_governance_report_hits(
        hits: list[legacy_core.RuleHit],
        fields: list[legacy_core.TextField],
        gate: str,
    ) -> list[legacy_core.RuleHit]:
        """Do not mistake defensive governance analysis for agitation.

        The frozen V4 creation-intent rule deliberately treats words such as
        ``报告``, ``政府``, ``内部`` and ``公众`` conservatively.  Those words also
        occur in ordinary requests to review incidents and prepare compliance
        reports.  Suppress only strongly structured defensive analysis or a
        completed delivery notice, and never suppress explicit harmful action
        language.
        """
        if gate not in {"InputGate", "OutputGate"}:
            return hits

        text_by_path = {field.path: field.text for field in fields}

        def is_safe_report(hit: legacy_core.RuleHit) -> bool:
            if hit.predicted_label != "political":
                return False
            text = text_by_path.get(hit.field_path, "")
            if any(marker in text for marker in UNSAFE_REPORT_REQUEST_MARKERS):
                return False
            common = "报告" in text and any(marker in text for marker in SAFE_REPORT_TOPICS)
            safe_delivery = (
                gate == "OutputGate"
                and common
                and any(marker in text for marker in SAFE_REPORT_DELIVERY_MARKERS)
            )
            safe_analysis = (
                common
                and sum(marker in text for marker in SAFE_REPORT_ANALYSIS_MARKERS) >= 2
            )
            return safe_delivery or safe_analysis

        return [hit for hit in hits if not is_safe_report(hit)]

    def detect(self, unit: DetectionUnit, ctx: DetectContext) -> tuple[DetectionHit, ...]:
        try:
            fields = self._text_fields(unit)
            if not fields:
                return ()
            _, raw_hits = legacy_core.detect_sample(self._sample(unit, fields))
            raw_hits = self._suppress_negated_surveillance_hits(raw_hits, fields)
            raw_hits = self._scope_surveillance_hits(raw_hits, unit.data_type)
            raw_hits = self._suppress_safe_governance_report_hits(raw_hits, fields, ctx.gate)
        except (AttributeError, TypeError, ValueError):
            # Malformed runtime payloads are ordinary detector negatives. Engine
            # diagnostics remain reserved for initialization and system failures.
            return ()

        if not raw_hits:
            return ()

        primary = max(
            raw_hits,
            key=lambda hit: (hit.score, 1 if hit.predicted_label == "political" else 0),
        )
        label = primary.predicted_label

        label_hits = [hit for hit in raw_hits if hit.predicted_label == label and hit.score >= self._min_score]
        if not label_hits:
            return ()

        primary = label_hits[0]
        locations: list[RiskLocation] = []
        seen_paths: set[str] = set()
        for hit in label_hits:
            if hit.field_path in seen_paths:
                continue
            seen_paths.add(hit.field_path)
            locations.append(
                RiskLocation(
                    kind="field_path",
                    locator=hit.field_path,
                    entity_type=label,
                )
            )
            if len(locations) >= self._max_risk_locations:
                break

        confidence = max(0.0, min(1.0, primary.score / 100.0))
        if confidence >= 0.95:
            severity = "critical"
        elif confidence >= 0.85:
            severity = "high"
        else:
            severity = "medium"

        return (
            DetectionHit(
                detector_id=self.detector_id,
                violation_type=label,
                confidence=confidence,
                severity=severity,
                risk_locations=tuple(locations),
                reason_code=primary.reason_code,
                evidence={
                    "detector_version": DETECTOR_VERSION,
                    "source_sha256": SOURCE_SHA256,
                    "primary_rule_id": primary.rule_id,
                    "rule_ids": list(dict.fromkeys(hit.rule_id for hit in label_hits))[: self._max_rule_ids],
                    "matched_field_paths": sorted(seen_paths),
                    "max_score": primary.score,
                },
                basis=(),
            ),
        )


__all__ = ["DETECTOR_VERSION", "IllegalPoliticalV4Detector", "SOURCE_SHA256"]
