from __future__ import annotations

from deerflow.compliance.detectors.delivered_rules.adapter import DeliveredRulesDetector
from deerflow.compliance.detectors.delivered_rules.vendor.text_id import (
    TextIdDetector as DeliveryTextIdDetector,
)


class NerTextIdDetector(DeliveredRulesDetector):
    detector_id = "ner_text_id"
    violation_type = "text_id"
    delivery_detector_class = DeliveryTextIdDetector


__all__ = ["NerTextIdDetector"]

