#!/usr/bin/env python3
"""Predict with the trainable compliance classifier."""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from violation_detection.model_detectors.io_utils import load_jsonl, write_jsonl
from violation_detection.model_detectors.tfidf_knn import TfidfKNNModel


def prediction_record(row: dict, model: TfidfKNNModel, min_active_confidence: float, min_similarity: float) -> dict:
    label, confidence, probabilities = model.predict(
        row,
        min_active_confidence=min_active_confidence,
        min_similarity=min_similarity,
    )
    return {
        "data_id": row.get("data_id") or row.get("sample_id"),
        "sample_id": row.get("sample_id"),
        "data_type": row.get("data_type"),
        "gate": row.get("gate") or row.get("trigger_gate"),
        "predicted_violation_type": label,
        "confidence": round(confidence, 4),
        "reason_code": "ml_tfidf_knn_classifier",
        "action_suggestion": ["manual_review"] if label != "none" else ["allow"],
        "probabilities": {key: round(value, 4) for key, value in sorted(probabilities.items())},
        "nearest_neighbors": model.nearest_neighbors(row, limit=model.k),
        "evidence_features": model.explain(row, label),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--model", type=Path, default=Path(__file__).resolve().parent / "models" / "ml_detector.json")
    parser.add_argument("--output", type=Path, default=PROJECT_ROOT / "reports" / "ml_predictions.jsonl")
    parser.add_argument("--min-active-confidence", type=float, default=0.0)
    parser.add_argument("--min-similarity", type=float, default=0.0)
    args = parser.parse_args(argv)

    rows = load_jsonl(args.input)
    model = TfidfKNNModel.load(args.model)
    predictions = [prediction_record(row, model, args.min_active_confidence, args.min_similarity) for row in rows]
    write_jsonl(args.output, predictions)

    counts = Counter(row["predicted_violation_type"] for row in predictions)
    print(
        json.dumps(
            {
                "input": str(args.input),
                "model": str(args.model),
                "output": str(args.output),
                "rows": len(rows),
                "predicted_counts": dict(sorted(counts.items())),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
