#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from trajectory_preprocess_lib import clean_points, load_points, segment_trips, write_jsonl


def main() -> int:
    parser = argparse.ArgumentParser(description="Segment trajectory data into trips.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--user-col")
    parser.add_argument("--trajectory-col")
    parser.add_argument("--time-col")
    parser.add_argument("--lat-col")
    parser.add_argument("--lon-col")
    parser.add_argument("--max-speed-kmh", type=float, default=200.0)
    parser.add_argument("--trip-gap-minutes", type=float, default=60.0)
    parser.add_argument("--min-trip-points", type=int, default=2)
    parser.add_argument("--geohash-precision", type=int, default=6)
    args = parser.parse_args()

    input_path = Path(args.input).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    points, stats, columns = load_points(
        input_path,
        args.user_col,
        args.trajectory_col,
        args.time_col,
        args.lat_col,
        args.lon_col,
        args.geohash_precision,
    )
    cleaned = clean_points(points, stats, args.max_speed_kmh, args.geohash_precision)
    trips = segment_trips(cleaned, args.trip_gap_minutes, args.min_trip_points, args.geohash_precision)
    write_jsonl(output_dir / "trips.jsonl", trips)
    payload = {
        "ok": True,
        "input": str(input_path),
        "output": str(output_dir / "trips.jsonl"),
        "detected_columns": columns,
        "cleaned_points": len(cleaned),
        "trips": len(trips),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

