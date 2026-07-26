#!/usr/bin/env python3
"""Evaluate the trainable classifier on a labeled JSONL split."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from violation_detection.model_detectors.feature_extractor import map_label
from violation_detection.model_detectors.io_utils import load_jsonl, write_jsonl
from violation_detection.model_detectors.metrics import classification_report
from violation_detection.model_detectors.tfidf_knn import TfidfKNNModel


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=PROJECT_ROOT / "normalized" / "splits" / "test.jsonl")
    parser.add_argument("--model", type=Path, default=Path(__file__).resolve().parent / "models" / "ml_detector.json")
    parser.add_argument("--output-summary", type=Path, default=PROJECT_ROOT / "reports" / "ml_eval_summary.json")
    parser.add_argument("--output-mismatches", type=Path, default=PROJECT_ROOT / "reports" / "ml_eval_mismatches.jsonl")
    parser.add_argument("--min-accuracy", type=float, default=0.9)
    parser.add_argument("--min-active-confidence", type=float, default=0.0)
    parser.add_argument("--min-similarity", type=float, default=0.0)
    args = parser.parse_args(argv)

    rows = load_jsonl(args.input)
    model = TfidfKNNModel.load(args.model)
    predictions = [
        model.predict(
            row,
            min_active_confidence=args.min_active_confidence,
            min_similarity=args.min_similarity,
        )[0]
        for row in rows
    ]
    summary = classification_report(rows, predictions, model.labels)
    summary.update(
        {
            "input": str(args.input),
            "model": str(args.model),
            "min_active_confidence": args.min_active_confidence,
            "min_similarity": args.min_similarity,
            "target_accuracy": args.min_accuracy,
            "static_business_lexicon": False,
        }
    )

    mismatches = []
    for row, pred in zip(rows, predictions):
        gold = map_label(row)
        if gold == pred:
            continue
        label, confidence, probabilities = model.predict(
            row,
            min_active_confidence=args.min_active_confidence,
            min_similarity=args.min_similarity,
        )
        mismatches.append(
            {
                "data_id": row.get("data_id"),
                "sample_id": row.get("sample_id"),
                "data_type": row.get("data_type"),
                "gate": row.get("gate") or row.get("trigger_gate"),
                "gold": gold,
                "predicted": label,
                "confidence": round(confidence, 4),
                "probabilities": {key: round(value, 4) for key, value in sorted(probabilities.items())},
                "nearest_neighbors": model.nearest_neighbors(row, limit=model.k),
                "evidence_features": model.explain(row, label),
                "content_text_preview": (row.get("content_text") or "")[:800],
            }
        )

    args.output_summary.parent.mkdir(parents=True, exist_ok=True)
    with args.output_summary.open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    write_jsonl(args.output_mismatches, mismatches)

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"mismatches={len(mismatches)}")
    return 0 if float(summary["accuracy"]) >= args.min_accuracy else 1


if __name__ == "__main__":
    raise SystemExit(main())
