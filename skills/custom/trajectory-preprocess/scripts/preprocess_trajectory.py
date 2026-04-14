#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from trajectory_preprocess_lib import (
    build_summary,
    clean_points,
    extract_staypoints,
    load_points,
    point_to_record,
    segment_trips,
    write_jsonl,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Clean and standardize raw trajectory data.")
    parser.add_argument("--input", required=True, help="Input CSV/TSV/JSON/JSONL trajectory file")
    parser.add_argument("--output-dir", required=True, help="Directory for cleaned outputs")
    parser.add_argument("--user-col", help="User id column")
    parser.add_argument("--trajectory-col", help="Trajectory/trip id column")
    parser.add_argument("--time-col", help="Timestamp column")
    parser.add_argument("--lat-col", help="Latitude column")
    parser.add_argument("--lon-col", help="Longitude column")
    parser.add_argument("--max-speed-kmh", type=float, default=200.0)
    parser.add_argument("--stay-radius-m", type=float, default=200.0)
    parser.add_argument("--stay-min-minutes", type=float, default=20.0)
    parser.add_argument("--trip-gap-minutes", type=float, default=60.0)
    parser.add_argument("--min-trip-points", type=int, default=2)
    parser.add_argument("--geohash-precision", type=int, default=6)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    input_path = Path(args.input).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    points, stats, columns = load_points(
        input_path,
        user_col=args.user_col,
        trajectory_col=args.trajectory_col,
        time_col=args.time_col,
        lat_col=args.lat_col,
        lon_col=args.lon_col,
        geohash_precision=args.geohash_precision,
    )
    cleaned = clean_points(points, stats, args.max_speed_kmh, args.geohash_precision)
    staypoints = extract_staypoints(
        cleaned,
        stay_radius_m=args.stay_radius_m,
        stay_min_minutes=args.stay_min_minutes,
        geohash_precision=args.geohash_precision,
    )
    trips = segment_trips(
        cleaned,
        trip_gap_minutes=args.trip_gap_minutes,
        min_trip_points=args.min_trip_points,
        geohash_precision=args.geohash_precision,
    )

    write_jsonl(output_dir / "cleaned_points.jsonl", (point_to_record(p) for p in cleaned))
    write_jsonl(output_dir / "staypoints.jsonl", staypoints)
    write_jsonl(output_dir / "trips.jsonl", trips)

    parameters = {
        "max_speed_kmh": args.max_speed_kmh,
        "stay_radius_m": args.stay_radius_m,
        "stay_min_minutes": args.stay_min_minutes,
        "trip_gap_minutes": args.trip_gap_minutes,
        "min_trip_points": args.min_trip_points,
        "geohash_precision": args.geohash_precision,
    }
    summary = build_summary(stats, input_path, output_dir, columns, parameters, staypoints, trips)
    (output_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary["counts"]["cleaned_points"] > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

