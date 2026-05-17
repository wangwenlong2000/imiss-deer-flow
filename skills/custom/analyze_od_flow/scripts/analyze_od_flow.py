#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

COMMON = Path(__file__).resolve().parents[2] / "citybench-skills-pack-new" / "custom" / "_trajectory_common"
sys.path.insert(0, str(COMMON))

from trajectory_tasks import analyze_od_flow  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Aggregate stay points into an OD flow matrix.")
    parser.add_argument("--input", required=True, help="Input staypoints.jsonl")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--geohash-precision", type=int, default=5)
    parser.add_argument("--top-k", type=int, default=20)
    parser.add_argument("--hour-start", type=int, default=None)
    parser.add_argument("--hour-end", type=int, default=None)
    parser.add_argument("--max-od-hours", type=float, default=12.0)
    args = parser.parse_args()
    summary = analyze_od_flow(
        Path(args.input).expanduser().resolve(),
        Path(args.output_dir).expanduser().resolve(),
        precision=args.geohash_precision,
        top_k=args.top_k,
        hour_start=args.hour_start,
        hour_end=args.hour_end,
        max_od_hours=args.max_od_hours,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
