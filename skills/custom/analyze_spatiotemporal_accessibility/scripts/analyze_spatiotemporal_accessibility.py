#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

COMMON = Path(__file__).resolve().parents[2] / "citybench-skills-pack-new" / "custom" / "_trajectory_common"
sys.path.insert(0, str(COMMON))

from trajectory_tasks import analyze_spatiotemporal_accessibility  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze empirical trajectory accessibility.")
    parser.add_argument("--od-matrix", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--origin-geohash", required=True)
    parser.add_argument("--budgets-min", default="15,30,60")
    parser.add_argument("--min-flow", type=int, default=1)
    args = parser.parse_args()
    summary = analyze_spatiotemporal_accessibility(
        Path(args.od_matrix).expanduser().resolve(),
        Path(args.output_dir).expanduser().resolve(),
        origin_geohash=args.origin_geohash,
        budgets_min=[float(x.strip()) for x in args.budgets_min.split(",") if x.strip()],
        min_flow=args.min_flow,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
