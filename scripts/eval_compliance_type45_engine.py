#!/usr/bin/env python3
"""Evaluate the frozen type 4/5 V4 rows through the real compliance engine."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
HARNESS = REPO_ROOT / "backend/packages/harness"
if str(HARNESS) not in sys.path:
    sys.path.insert(0, str(HARNESS))

from deerflow.compliance.contract import DetectionUnit, TextItem  # noqa: E402
from deerflow.compliance.detectors.rule_illegal_political_v4 import legacy_core  # noqa: E402
from deerflow.compliance.engine import build_engine  # noqa: E402
from deerflow.config.compliance_config import ComplianceConfig  # noqa: E402

DATA_FILE = REPO_ROOT / "datasets/compliance/type45_frozen_test/4_5_test_20pct.jsonl"
DETECTOR_ID = "rule_illegal_political_v4"


def _unit(sample: legacy_core.Sample, gate: str) -> DetectionUnit:
    return DetectionUnit(
        unit_id=sample.sample_id,
        gate=gate,
        data_type=sample.data_type,
        text_items=tuple(
            TextItem(item_id=f"t-{index}", text=item.text, source=item.path)
            for index, item in enumerate(sample.text_fields, 1)
        ),
        raw={"kind": "type45_frozen_regression"},
    )


def _metrics(counts: Counter[str]) -> dict[str, float | int]:
    total = counts["total"]
    correct = counts["correct"]
    return {
        "samples": total,
        "correct": correct,
        "accuracy": correct / total if total else 0.0,
    }


def evaluate() -> dict[str, Any]:
    samples, _ = legacy_core.load_samples(DATA_FILE)
    config = ComplianceConfig(
        enabled=True,
        detectors_config_path="config/compliance/detectors.yaml",
        policy_matrix_path="config/compliance/policy_matrix.yaml",
        audit={"enabled": False},
    )
    engine = build_engine(config)
    for registration in engine.registry.all():
        registration.enabled = registration.detector_id == DETECTOR_ID

    total_counts: Counter[str] = Counter()
    per_target: dict[str, Counter[str]] = defaultdict(Counter)
    mismatches: list[dict[str, Any]] = []
    legacy_mismatches: list[dict[str, Any]] = []
    gold_positive_refuse = 0
    gold_positive_total = 0
    detected_positive_refuse = 0
    detected_positive_total = 0
    rows = [json.loads(line) for line in DATA_FILE.read_text(encoding="utf-8").splitlines() if line.strip()]
    rows_by_id = {
        str(row.get("sample_id") or row.get("data_id")): row
        for row in rows
    }

    try:
        for sample in samples:
            row = rows_by_id.get(sample.sample_id, {})
            gate = row.get("trigger_gate") or row.get("gate") or "OutputGate"
            if gate not in {"InputGate", "ContextGate", "OutputGate"}:
                gate = "OutputGate"

            unit = _unit(sample, gate)
            decision = engine.check(
                (unit,),
                gate=unit.gate,
                request_id=f"type45-{sample.sample_id}",
                budget_ms=2000,
                origin={"kind": "type45_frozen_regression"},
            )
            own_hits = [hit for hit in decision.hits if hit.detector_id == DETECTOR_ID]
            predicted = own_hits[0].violation_type if own_hits else "none"
            legacy_predicted, _ = legacy_core.detect_sample(sample)
            gold = sample.expected_label
            positive = gold != "none"
            total_counts["total"] += 1
            per_target[sample.target_violation_type]["total"] += 1
            if predicted == gold:
                total_counts["correct"] += 1
                per_target[sample.target_violation_type]["correct"] += 1

            if positive:
                gold_positive_total += 1
                gold_positive_refuse += int("refuse" in decision.actions)
            if predicted != "none":
                detected_positive_total += 1
                detected_positive_refuse += int("refuse" in decision.actions)
            if predicted != gold:
                mismatches.append({"sample_id": sample.sample_id, "gold": gold, "predicted": predicted})
            if predicted != legacy_predicted:
                legacy_mismatches.append(
                    {"sample_id": sample.sample_id, "legacy": legacy_predicted, "engine": predicted}
                )
    finally:
        engine.registry.close()

    return {
        "detector_id": DETECTOR_ID,
        "dataset": str(DATA_FILE),
        "overall": _metrics(total_counts),
        "per_target": {name: _metrics(counts) for name, counts in sorted(per_target.items())},
        "gold_positive_refuse_coverage": (
            gold_positive_refuse / gold_positive_total if gold_positive_total else 0.0
        ),
        "detected_positive_refuse_rate": (
            detected_positive_refuse / detected_positive_total if detected_positive_total else 0.0
        ),
        "legacy_engine_100pct_consistent": not legacy_mismatches,
        "legacy_mismatches": legacy_mismatches,
        "mismatches": mismatches,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true", help="Retained for command symmetry; output is always JSON.")
    parser.parse_args()
    print(json.dumps(evaluate(), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
