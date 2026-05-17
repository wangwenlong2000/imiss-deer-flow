#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

COMMON = Path(__file__).resolve().parents[2] / "_trajectory_common"
sys.path.insert(0, str(COMMON))

from trajectory_tasks import forecast_region_flow  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Forecast future regional flow from raw trajectory points or region heat series.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--query")
    parser.add_argument("--city")
    parser.add_argument("--time-start")
    parser.add_argument("--time-end")
    parser.add_argument("--hour-start", type=int)
    parser.add_argument("--hour-end", type=int)
    parser.add_argument("--bbox")
    parser.add_argument("--geohash")
    parser.add_argument("--group-col")
    parser.add_argument("--metric-col")
    parser.add_argument("--time-col")
    parser.add_argument("--geohash-col")
    parser.add_argument("--lat-col")
    parser.add_argument("--lon-col")
    parser.add_argument("--user-col")
    parser.add_argument("--geohash-precision", type=int, default=6)
    parser.add_argument("--time-bucket", choices=["hour", "day", "month"], default="day")
    parser.add_argument("--forecast-steps", type=int, default=3)
    parser.add_argument("--history-window", type=int, default=7)
    parser.add_argument("--top-k", type=int, default=20)
    args = parser.parse_args()
    summary = forecast_region_flow(
        Path(args.input).expanduser().resolve(),
        Path(args.output_dir).expanduser().resolve(),
        precision=args.geohash_precision,
        bucket=args.time_bucket,
        forecast_steps=args.forecast_steps,
        history_window=args.history_window,
        top_k=args.top_k,
        group_col=args.group_col,
        metric_col=args.metric_col,
        time_col=args.time_col,
        geohash_col=args.geohash_col,
        lat_col=args.lat_col,
        lon_col=args.lon_col,
        user_col=args.user_col,
        query=args.query,
        city=args.city,
        geohash=args.geohash,
        time_start=args.time_start,
        time_end=args.time_end,
        hour_start=args.hour_start,
        hour_end=args.hour_end,
        bbox=args.bbox,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
