#!/usr/bin/env python3
"""Create deterministic 8:2 train/test splits for trainable detectors."""
from __future__ import annotations

import argparse
import json
import random
import sys
from collections import Counter, defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from violation_detection.model_detectors.feature_extractor import map_label
from violation_detection.model_detectors.io_utils import load_jsonl, write_jsonl


def split_rows(rows: list[dict], *, test_ratio: float, seed: int) -> tuple[list[dict], list[dict]]:
    randomizer = random.Random(seed)
    by_label: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_label[map_label(row)].append(row)

    train: list[dict] = []
    test: list[dict] = []
    for label, label_rows in sorted(by_label.items()):
        shuffled = list(label_rows)
        randomizer.shuffle(shuffled)
        test_count = max(1, round(len(shuffled) * test_ratio)) if len(shuffled) > 1 else 0
        test.extend(shuffled[:test_count])
        train.extend(shuffled[test_count:])

    randomizer.shuffle(train)
    randomizer.shuffle(test)
    return train, test


def summarize(rows: list[dict]) -> dict:
    return {
        "rows": len(rows),
        "labels": dict(sorted(Counter(map_label(row) for row in rows).items())),
        "data_types": dict(sorted(Counter(str(row.get("data_type") or "") for row in rows).items())),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=PROJECT_ROOT / "normalized" / "all_normalized_800.jsonl")
    parser.add_argument("--output-dir", type=Path, default=PROJECT_ROOT / "normalized" / "splits")
    parser.add_argument("--test-ratio", type=float, default=0.2)
    parser.add_argument("--seed", type=int, default=20260618)
    args = parser.parse_args(argv)

    rows = load_jsonl(args.input)
    train, test = split_rows(rows, test_ratio=args.test_ratio, seed=args.seed)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_jsonl(args.output_dir / "train.jsonl", train)
    write_jsonl(args.output_dir / "test.jsonl", test)

    summary = {
        "input": str(args.input),
        "seed": args.seed,
        "test_ratio": args.test_ratio,
        "label_mapping": "final_violation_type in {video_meta_leak,re_identify,domain}; others -> none",
        "train": summarize(train),
        "test": summarize(test),
    }
    with (args.output_dir / "split_summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

