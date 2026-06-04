#!/usr/bin/env python3
"""多目标时空伴行检测 — 用户两两在同 geohash 网格 + 时间窗重叠的伴行检测。"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

COMMON = Path(__file__).resolve().parents[2] / "_trajectory_common_v2"
sys.path.insert(0, str(COMMON))

from trajectory_tasks import detect_trajectory_cooccurrence  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="多目标时空伴行检测")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--geohash-precision", type=int, default=6)
    parser.add_argument("--min-overlap-min", type=float, default=15.0, help="最小时间重叠（分钟）")
    parser.add_argument("--target-users", default=None, help="只查这些用户（逗号分隔），空则全两两")
    parser.add_argument("--max-pairs", type=int, default=10000, help="最大对数限制")
    args = parser.parse_args()
    summary = detect_trajectory_cooccurrence(
        Path(args.input).expanduser().resolve(),
        Path(args.output_dir).expanduser().resolve(),
        precision=args.geohash_precision,
        min_overlap_min=args.min_overlap_min,
        target_users=[u.strip() for u in args.target_users.split(',')] if args.target_users else None,
        max_pairs=args.max_pairs
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
