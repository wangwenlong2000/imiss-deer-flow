#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

COMMON = Path(__file__).resolve().parents[2] / "citybench-skills-pack-new" / "custom" / "_trajectory_common"
sys.path.insert(0, str(COMMON))

from trajectory_tasks import detect_trajectory_cooccurrence  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Detect trajectory co-occurrence events.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--geohash-precision", type=int, default=5)
    parser.add_argument("--min-overlap-min", type=float, default=15.0)
    parser.add_argument("--target-users")
    parser.add_argument("--max-pairs", type=int, default=50000)
    args = parser.parse_args()
    summary = detect_trajectory_cooccurrence(
        Path(args.input).expanduser().resolve(),
        Path(args.output_dir).expanduser().resolve(),
        precision=args.geohash_precision,
        min_overlap_min=args.min_overlap_min,
        target_users=[u.strip() for u in args.target_users.split(",") if u.strip()] if args.target_users else None,
        max_pairs=args.max_pairs,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
