from __future__ import annotations

from deerflow.compliance.detectors.delivered_rules.adapter import DeliveredRulesDetector
from deerflow.compliance.detectors.delivered_rules.vendor.hardcoded_cred import (
    HardcodedCredDetector as DeliveryHardcodedCredDetector,
)


class EntropyHardcodedCredDetector(DeliveredRulesDetector):
    detector_id = "entropy_hardcoded_cred"
    violation_type = "hardcoded_cred"
    delivery_detector_class = DeliveryHardcodedCredDetector


__all__ = ["EntropyHardcodedCredDetector"]

