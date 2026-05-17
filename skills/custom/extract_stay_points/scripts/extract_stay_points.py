#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PREPROCESS_SCRIPTS = Path(__file__).resolve().parents[2] / "trajectory-preprocess" / "scripts"
sys.path.insert(0, str(PREPROCESS_SCRIPTS))

from trajectory_preprocess_lib import clean_points, extract_staypoints, load_points, write_jsonl  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract stay points from trajectory data.")
    parser.add_argument("--input", required=True, help="Input CSV/TSV/JSON/JSONL trajectory file")
    parser.add_argument("--output-dir", required=True, help="Directory for staypoint outputs")
    parser.add_argument("--user-col")
    parser.add_argument("--trajectory-col")
    parser.add_argument("--time-col")
    parser.add_argument("--lat-col")
    parser.add_argument("--lon-col")
    parser.add_argument("--max-speed-kmh", type=float, default=200.0)
    parser.add_argument("--stay-radius-m", type=float, default=200.0)
    parser.add_argument("--stay-min-minutes", type=float, default=20.0)
    parser.add_argument("--geohash-precision", type=int, default=6)
    args = parser.parse_args()

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
    staypoints = extract_staypoints(cleaned, args.stay_radius_m, args.stay_min_minutes, args.geohash_precision)
    staypoints_path = output_dir / "staypoints.jsonl"
    write_jsonl(staypoints_path, staypoints)
    summary = {
        "ok": True,
        "skill": "extract_stay_points",
        "input": str(input_path),
        "output_dir": str(output_dir),
        "detected_columns": columns,
        "parameters": {
            "max_speed_kmh": args.max_speed_kmh,
            "stay_radius_m": args.stay_radius_m,
            "stay_min_minutes": args.stay_min_minutes,
            "geohash_precision": args.geohash_precision,
        },
        "counts": {
            "raw_points": stats.raw_points,
            "cleaned_points": len(cleaned),
            "staypoints": len(staypoints),
        },
        "outputs": {
            "staypoints": str(staypoints_path),
            "summary": str(output_dir / "summary.json"),
        },
        "warnings": sorted(set(stats.warnings)),
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
