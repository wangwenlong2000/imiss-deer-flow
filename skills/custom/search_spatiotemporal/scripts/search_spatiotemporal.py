#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

COMMON = Path(__file__).resolve().parents[2] / "_trajectory_common"
sys.path.insert(0, str(COMMON))

from trajectory_tasks import search_records  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Search spatiotemporal trajectory records by time, space, city, and text.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--query")
    parser.add_argument("--city")
    parser.add_argument("--time-start")
    parser.add_argument("--time-end")
    parser.add_argument("--hour-start", type=int)
    parser.add_argument("--hour-end", type=int)
    parser.add_argument("--bbox", help="min_lon,min_lat,max_lon,max_lat")
    parser.add_argument("--geohash")
    parser.add_argument("--geohash-col")
    parser.add_argument("--time-col")
    parser.add_argument("--lat-col")
    parser.add_argument("--lon-col")
    parser.add_argument("--top-k", type=int, default=50)
    args = parser.parse_args()
    summary = search_records(
        Path(args.input).expanduser().resolve(),
        Path(args.output_dir).expanduser().resolve(),
        query=args.query,
        city=args.city,
        geohash=args.geohash,
        geohash_col=args.geohash_col,
        time_col=args.time_col,
        time_start=args.time_start,
        time_end=args.time_end,
        hour_start=args.hour_start,
        hour_end=args.hour_end,
        bbox=args.bbox,
        lat_col=args.lat_col,
        lon_col=args.lon_col,
        top_k=args.top_k,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
