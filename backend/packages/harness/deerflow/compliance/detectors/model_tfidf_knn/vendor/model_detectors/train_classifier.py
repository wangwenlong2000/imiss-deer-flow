#!/usr/bin/env python3
"""Train a no-static-lexicon classifier for the delivered detectors."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from violation_detection.model_detectors.feature_extractor import ALL_LABELS
from violation_detection.model_detectors.io_utils import load_jsonl
from violation_detection.model_detectors.metrics import classification_report
from violation_detection.model_detectors.tfidf_knn import TfidfKNNModel


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--train", type=Path, default=PROJECT_ROOT / "normalized" / "splits" / "train.jsonl")
    parser.add_argument("--model", type=Path, default=Path(__file__).resolve().parent / "models" / "ml_detector.json")
    parser.add_argument("--min-df", type=int, default=1)
    parser.add_argument("--max-features", type=int, default=50000)
    parser.add_argument("--k", type=int, default=1)
    args = parser.parse_args(argv)

    rows = load_jsonl(args.train)
    model = TfidfKNNModel(
        labels=list(ALL_LABELS),
        min_df=args.min_df,
        max_features=args.max_features,
        k=args.k,
    ).fit(rows)
    model.save(args.model)

    predictions = [model.predict(row)[0] for row in rows]
    summary = classification_report(rows, predictions, model.labels)
    summary.update(
        {
            "train_input": str(args.train),
            "model_output": str(args.model),
            "model_type": "pure_python_tfidf_knn",
            "vocabulary_size": len(model.vocabulary),
            "min_df": args.min_df,
            "max_features": args.max_features,
            "k": args.k,
            "static_business_lexicon": False,
        }
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
