#!/usr/bin/env python3
"""Evaluate type 1/2 frozen rows through the real compliance engine.

This is an integration regression, not a rule-tuning utility.  It preserves the
frozen JSONL inputs, builds DetectionUnit objects, routes them through Registry,
Router, ComplianceEngine and the in-process adapters, then compares the boolean
prediction with the original rule-only entry point.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
HARNESS = REPO_ROOT / "backend/packages/harness"
if str(HARNESS) not in sys.path:
    sys.path.insert(0, str(HARNESS))

from deerflow.compliance.contract import DetectionUnit, FieldItem, TextItem  # noqa: E402
from deerflow.compliance.detectors.regex_geo_loc import legacy_core as geo_core  # noqa: E402
from deerflow.compliance.detectors.regex_struct_id import legacy_core as struct_core  # noqa: E402
from deerflow.compliance.engine import build_engine  # noqa: E402
from deerflow.config.compliance_config import ComplianceConfig  # noqa: E402

DATA_DIR = REPO_ROOT / "datasets/compliance/type12_frozen_test"
FILES = {
    "struct_id": DATA_DIR / "struct_id_test_44.jsonl",
    "geo_loc": DATA_DIR / "geo_loc_test_40.jsonl",
}


def _load(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _unit(row: dict[str, Any], violation_type: str) -> DetectionUnit:
    """Build the canonical adapter boundary without calling detector.detect."""
    texts: list[str] = []
    fields: dict[str, Any] = {}
    if violation_type == "struct_id":
        content_text, features = struct_core.extract_detection_input(row)
        if content_text:
            texts.append(content_text)
        fields.update(features)
    else:
        items, text_blobs = geo_core.collect_items(row)
        texts.extend(text for _, text in text_blobs if text)
        fields.update({path: value for path, value in items})

    gate = row.get("trigger_gate")
    if gate not in {"InputGate", "ContextGate", "OutputGate"}:
        gate = "OutputGate"
    return DetectionUnit(
        unit_id=str(row.get("sample_id") or row.get("data_id")),
        gate=gate,
        data_type=str(row.get("data_type") or ""),
        text_items=tuple(TextItem(item_id=f"t-{i}", text=text, source="frozen") for i, text in enumerate(texts, 1)),
        field_items=tuple(
            FieldItem(item_id=f"f-{i}", path=path, value=value, source="frozen")
            for i, (path, value) in enumerate(fields.items(), 1)
        ),
        raw={"kind": "type12_frozen_regression"},
    )


def _legacy_prediction(row: dict[str, Any], violation_type: str) -> bool:
    if violation_type == "struct_id":
        result = struct_core.detect_struct_id(row, struct_core.OptionalPresidioAnalyzer("never"))
    else:
        result = geo_core.detect_geo_loc(row, geo_core.OptionalPresidio("never"), set())
    return bool(result.get("is_positive_pred"))


def evaluate() -> dict[str, Any]:
    config = ComplianceConfig(
        enabled=True,
        detectors_config_path="config/compliance/detectors.yaml",
        policy_matrix_path="config/compliance/policy_matrix.yaml",
        audit={"enabled": False},
    )
    engine = build_engine(config)
    # This regression is deliberately scoped to types 1/2.  The independently
    # shipped type 8/9/10 model assets may be absent in a lightweight dev
    # container and must not add unrelated detector errors to every row.
    model_registration = engine.registry.get("model_tfidf_knn")
    if model_registration is not None:
        model_registration.enabled = False
    report: dict[str, Any] = {}
    try:
        for violation_type, path in FILES.items():
            counts = {"TP": 0, "FP": 0, "TN": 0, "FN": 0}
            fp_ids: list[str] = []
            fn_ids: list[str] = []
            mismatches: list[dict[str, Any]] = []
            rows = _load(path)
            for row in rows:
                unit = _unit(row, violation_type)
                decision = engine.check(
                    (unit,),
                    gate=unit.gate,
                    request_id=f"frozen-{unit.unit_id}",
                    budget_ms=2000,
                    origin={"kind": "type12_frozen_regression"},
                )
                predicted = any(hit.violation_type == violation_type for hit in decision.hits)
                legacy = _legacy_prediction(row, violation_type)
                gold = bool(row.get("is_positive"))
                sample_id = str(row.get("sample_id") or row.get("data_id"))
                bucket = ("T" if predicted == gold else "F") + ("P" if predicted else "N")
                counts[bucket] += 1
                if bucket == "FP":
                    fp_ids.append(sample_id)
                elif bucket == "FN":
                    fn_ids.append(sample_id)
                if predicted != legacy:
                    mismatches.append(
                        {
                            "sample_id": sample_id,
                            "engine": predicted,
                            "legacy": legacy,
                            "likely_boundary": "content_text/features/field_items/data_type/adapter",
                        }
                    )

            tp, fp, tn, fn = (counts[key] for key in ("TP", "FP", "TN", "FN"))
            precision = tp / (tp + fp) if tp + fp else 0.0
            recall = tp / (tp + fn) if tp + fn else 0.0
            report[violation_type] = {
                "samples": len(rows),
                **counts,
                "precision": precision,
                "recall": recall,
                "f1": 2 * precision * recall / (precision + recall) if precision + recall else 0.0,
                "accuracy": (tp + tn) / len(rows),
                "fp_sample_ids": fp_ids,
                "fn_sample_ids": fn_ids,
                "legacy_engine_100pct_consistent": not mismatches,
                "mismatches": mismatches,
            }
    finally:
        engine.registry.close()
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    args = parser.parse_args()
    report = evaluate()
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
