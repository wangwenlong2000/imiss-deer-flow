#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

COMMON = Path(__file__).resolve().parents[2] / "citybench-skills-pack-new" / "custom" / "_trajectory_common"
sys.path.insert(0, str(COMMON))

from trajectory_tasks import mine_trajectory_patterns  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Mine frequent stay-point trajectory sequences.")
    parser.add_argument("--input", required=True, help="Input staypoints.jsonl")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--geohash-precision", type=int, default=5)
    parser.add_argument("--max-length", type=int, default=4)
    parser.add_argument("--min-support", type=float, default=0.05)
    parser.add_argument("--top-k", type=int, default=50)
    args = parser.parse_args()
    summary = mine_trajectory_patterns(
        Path(args.input).expanduser().resolve(),
        Path(args.output_dir).expanduser().resolve(),
        precision=args.geohash_precision,
        min_support=args.min_support,
        max_length=args.max_length,
        top_k=args.top_k,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
