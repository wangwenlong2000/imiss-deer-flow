#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

COMMON = Path(__file__).resolve().parents[2] / "_trajectory_common"
sys.path.insert(0, str(COMMON))

from trajectory_tasks import profile_urban_region  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Profile urban region functions from trajectories and optional POI evidence.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--poi-input")
    parser.add_argument("--geohash-col")
    parser.add_argument("--lat-col")
    parser.add_argument("--lon-col")
    parser.add_argument("--time-col")
    parser.add_argument("--user-col")
    parser.add_argument("--poi-lat-col")
    parser.add_argument("--poi-lon-col")
    parser.add_argument("--poi-category-col")
    parser.add_argument("--geohash-precision", type=int, default=6)
    parser.add_argument("--top-k", type=int, default=20)
    args = parser.parse_args()
    summary = profile_urban_region(
        Path(args.input).expanduser().resolve(),
        Path(args.output_dir).expanduser().resolve(),
        precision=args.geohash_precision,
        poi_input=Path(args.poi_input).expanduser().resolve() if args.poi_input else None,
        poi_lat_col=args.poi_lat_col,
        poi_lon_col=args.poi_lon_col,
        poi_category_col=args.poi_category_col,
        geohash_col=args.geohash_col,
        lat_col=args.lat_col,
        lon_col=args.lon_col,
        time_col=args.time_col,
        user_col=args.user_col,
        top_k=args.top_k,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
