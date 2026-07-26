"""Compliance violation detection: one engine, three gates, ten violation types.

Layout
------
``contract.py``    the detector boundary — the only module detector authors import
``types.py``       engine-internal request/decision types
``engine.py``      orchestration (normalize -> route -> detect -> intent -> policy -> audit)
``registry.py``    manifest discovery; the only place concrete detectors are loaded
``router.py``      candidate selection by gate x data_type x cost
``policy.py``      violation x scene -> actions, read from YAML
``scene.py``       SceneResolver seam (phase 1 returns None; matrix falls back to `_unknown`)
``intent.py``      "sensitive entity + high-risk intent" joint judgement at the input gate
``actions.py``     disposition executors (mask / rewrite / aggregate / refuse)
``audit.py``       JSONL determination trail with compliance basis
``adapters/``      inprocess | subprocess_cli | http_service
``normalizers/``   source payload -> DetectionUnit
``detectors/``     self-contained detector packages, each with its own manifest

Adding a detector requires no change to any file above ``detectors/``.
See ``docs/compliance-detector-integration-guide.md``.
"""

from deerflow.compliance.contract import (
    CONTRACT_VERSION,
    DetectContext,
    DetectionHit,
    DetectionUnit,
    Detector,
    FieldItem,
    RiskLocation,
    TextItem,
)
from deerflow.compliance.types import ComplianceDecision, DetectionRequest, Diagnostics

__all__ = [
    "CONTRACT_VERSION",
    "ComplianceDecision",
    "DetectContext",
    "DetectionHit",
    "DetectionRequest",
    "DetectionUnit",
    "Detector",
    "Diagnostics",
    "FieldItem",
    "RiskLocation",
    "TextItem",
]
