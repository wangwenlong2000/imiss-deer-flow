#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

COMMON = Path(__file__).resolve().parents[2] / "citybench-skills-pack-new" / "custom" / "_trajectory_common"
sys.path.insert(0, str(COMMON))

from trajectory_tasks import analyze_event_impact  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze event or weather impact on regional mobility.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--event-start", required=True)
    parser.add_argument("--event-end", required=True)
    parser.add_argument("--event-name", default="external_event")
    parser.add_argument("--treatment-geohash", required=True, help="Comma-separated treatment geohashes")
    parser.add_argument("--control-geohash", help="Comma-separated control geohashes")
    parser.add_argument("--metric", default="checkin_count")
    args = parser.parse_args()
    summary = analyze_event_impact(
        Path(args.input).expanduser().resolve(),
        Path(args.output_dir).expanduser().resolve(),
        event_start=args.event_start,
        event_end=args.event_end,
        treatment_geohash=[g.strip() for g in args.treatment_geohash.split(",") if g.strip()],
        control_geohash=[g.strip() for g in args.control_geohash.split(",") if g.strip()] if args.control_geohash else None,
        event_name=args.event_name,
        metric=args.metric,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
