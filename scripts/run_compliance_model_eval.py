#!/usr/bin/env python3
"""Offline model evaluation, reproducing the delivery package's own numbers.

Baseline that must hold (0624 split, 56-row test set):

    accuracy = 0.9821, macro_f1 = 0.9859, mismatches = 1/56

Metric definitions come from the vendored ``metrics.py``, so the figures here are
directly comparable to the delivery package's rather than a parallel
reimplementation that might differ in edge cases.

Two extra things this adds over the vendor script:

* ``--rebuild-rows`` recomputes each row from ``features`` via ``row_builder``,
  which is what production actually does. Comparing the two runs shows whether
  the online path degrades the model.
* The mismatch report includes nearest-neighbour similarity, so the
  ``min_similarity`` threshold can be tuned against evidence instead of taste.

⚠️ ``--min-similarity`` here is NOT the detector's ``min_similarity``
-------------------------------------------------------------------
This flag is the *vendor model's* parameter: below the threshold
``predict_proba`` forces the label to ``none``, which converts true positives
into false negatives (measured: 0.25 drops accuracy 0.9821 -> 0.9107, mismatches
1 -> 5).

``ModelTfidfKnnDetector`` deliberately does **not** pass it to ``predict()``. It
still reports the hit and instead downgrades severity to ``low``, so the finding
lands in the manual-review queue rather than vanishing. Same number, opposite
effect on recall — keep the two straight when tuning.

Exit code 0 when accuracy meets ``--min-accuracy``; 1 otherwise.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
HARNESS = REPO_ROOT / "backend/packages/harness"
if str(HARNESS) not in sys.path:
    sys.path.insert(0, str(HARNESS))

from deerflow.compliance.detectors.model_tfidf_knn.row_builder import build_row  # noqa: E402
from deerflow.compliance.detectors.model_tfidf_knn.vendor.model_detectors.feature_extractor import map_label  # noqa: E402
from deerflow.compliance.detectors.model_tfidf_knn.vendor.model_detectors.io_utils import load_jsonl  # noqa: E402
from deerflow.compliance.detectors.model_tfidf_knn.vendor.model_detectors.metrics import classification_report  # noqa: E402
from deerflow.compliance.detectors.model_tfidf_knn.vendor.model_detectors.tfidf_knn import TfidfKNNModel  # noqa: E402

DEFAULT_INPUT = REPO_ROOT / "datasets/compliance/normalized/0624_supported_split/test.jsonl"
DEFAULT_MODEL = REPO_ROOT / "models/compliance/ml_detector_0624_fresh.json"

#: Published baseline from the plan (§1.3 / §9.2).
BASELINE = {"accuracy": 0.9821, "macro_f1": 0.9859, "mismatches": 1}


def evaluate(rows: list[dict], model: TfidfKNNModel, *, rebuild_rows: bool, min_active_confidence: float, min_similarity: float) -> tuple[list[str], list[dict], float]:
    """Predict every row; return labels, mismatch records and per-row latency."""
    predictions: list[str] = []
    mismatches: list[dict] = []

    began = time.perf_counter()
    for row in rows:
        candidate = row
        if rebuild_rows:
            candidate = build_row(
                features=row.get("features") or {},
                data_type=row.get("data_type"),
                gate=row.get("gate") or row.get("trigger_gate"),
            )
        label, confidence, probabilities = model.predict(
            candidate,
            min_active_confidence=min_active_confidence,
            min_similarity=min_similarity,
        )
        predictions.append(label)

        gold = map_label(row)
        if gold != label:
            neighbors = model.nearest_neighbors(candidate, limit=3)
            mismatches.append(
                {
                    "data_id": row.get("data_id"),
                    "sample_id": row.get("sample_id"),
                    "data_type": row.get("data_type"),
                    "gate": row.get("gate") or row.get("trigger_gate"),
                    "gold": gold,
                    "predicted": label,
                    "confidence": round(float(confidence), 4),
                    "top_similarity": round(float(neighbors[0]["similarity"]), 6) if neighbors else 0.0,
                    "probabilities": {k: round(float(v), 4) for k, v in sorted(probabilities.items())},
                    "content_text_preview": (row.get("content_text") or "")[:400],
                }
            )
    elapsed_ms = (time.perf_counter() - began) * 1000.0
    per_row_ms = elapsed_ms / max(len(rows), 1)
    return predictions, mismatches, per_row_ms


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--model", type=Path, default=DEFAULT_MODEL)
    parser.add_argument("--min-accuracy", type=float, default=0.98)
    parser.add_argument("--min-active-confidence", type=float, default=0.0)
    parser.add_argument(
        "--min-similarity",
        type=float,
        default=0.0,
        help="Vendor model threshold: below it the label is forced to `none`. NOT the detector's severity downgrade — see the module docstring.",
    )
    parser.add_argument("--rebuild-rows", action="store_true", help="Rebuild each row from `features` (the production path) before predicting.")
    parser.add_argument("--output-summary", type=Path, default=None)
    parser.add_argument("--output-mismatches", type=Path, default=None)
    args = parser.parse_args(argv)

    if not args.input.is_file():
        print(f"FAIL: input not found: {args.input}", file=sys.stderr)
        print("      Run `make compliance-assets` to unpack the delivery package.", file=sys.stderr)
        return 1
    if not args.model.is_file():
        print(f"FAIL: model not found: {args.model}", file=sys.stderr)
        print("      Model weights are gitignored; run `make compliance-assets`.", file=sys.stderr)
        return 1

    rows = load_jsonl(args.input)
    load_began = time.perf_counter()
    model = TfidfKNNModel.load(args.model)
    load_ms = (time.perf_counter() - load_began) * 1000.0

    predictions, mismatches, per_row_ms = evaluate(
        rows,
        model,
        rebuild_rows=args.rebuild_rows,
        min_active_confidence=args.min_active_confidence,
        min_similarity=args.min_similarity,
    )

    summary = classification_report(rows, predictions, model.labels)
    summary.update(
        {
            "input": str(args.input),
            "model": str(args.model),
            "rows": len(rows),
            "rebuild_rows": args.rebuild_rows,
            "min_active_confidence": args.min_active_confidence,
            "min_similarity": args.min_similarity,
            "target_accuracy": args.min_accuracy,
            "mismatches": len(mismatches),
            "model_load_ms": round(load_ms, 2),
            "per_row_ms": round(per_row_ms, 3),
            "baseline": BASELINE,
        }
    )

    accuracy = float(summary["accuracy"])
    macro_f1 = float(summary["macro_f1"])

    print(f"input          : {args.input}")
    print(f"model          : {args.model}")
    print(f"rows           : {len(rows)}")
    print(f"rebuild_rows   : {args.rebuild_rows} ({'production path' if args.rebuild_rows else 'original stored rows'})")
    print(f"model load     : {load_ms:.1f} ms")
    print(f"per-row latency: {per_row_ms:.2f} ms")
    print()
    print(f"accuracy       : {accuracy:.4f}   (baseline {BASELINE['accuracy']})")
    print(f"macro_f1       : {macro_f1:.4f}   (baseline {BASELINE['macro_f1']})")
    print(f"mismatches     : {len(mismatches)}/{len(rows)}   (baseline {BASELINE['mismatches']})")
    print()
    print("per class:")
    for label, stats in summary["per_class"].items():
        print(f"  {label:16s} support={stats['support']:3d}  P={stats['precision']:.4f}  R={stats['recall']:.4f}  F1={stats['f1']:.4f}")

    if mismatches:
        print()
        print("mismatches:")
        for item in mismatches:
            print(f"  {item['data_id']}")
            print(f"    gold={item['gold']} predicted={item['predicted']} confidence={item['confidence']} top_similarity={item['top_similarity']}")
            print(f"    text: {item['content_text_preview'][:160]}")

    if args.output_summary:
        args.output_summary.parent.mkdir(parents=True, exist_ok=True)
        args.output_summary.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    if args.output_mismatches:
        args.output_mismatches.parent.mkdir(parents=True, exist_ok=True)
        with args.output_mismatches.open("w", encoding="utf-8") as f:
            for item in mismatches:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")

    print()
    if accuracy >= args.min_accuracy:
        print(f"PASS: accuracy {accuracy:.4f} >= {args.min_accuracy}")
        return 0
    print(f"FAIL: accuracy {accuracy:.4f} < {args.min_accuracy}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
