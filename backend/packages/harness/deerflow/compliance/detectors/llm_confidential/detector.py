from __future__ import annotations

from deerflow.compliance.detectors.delivered_rules.adapter import DeliveredRulesDetector
from deerflow.compliance.detectors.delivered_rules.vendor.confidential import (
    ConfidentialDetector as DeliveryConfidentialDetector,
)


class LlmConfidentialDetector(DeliveredRulesDetector):
    """Rule-first confidential detector; external LLM review stays isolated."""

    detector_id = "llm_confidential"
    violation_type = "confidential"
    delivery_detector_class = DeliveryConfidentialDetector


__all__ = ["LlmConfidentialDetector"]

