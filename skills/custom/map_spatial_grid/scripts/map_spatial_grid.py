#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

COMMON = Path(__file__).resolve().parents[2] / "_trajectory_common"
sys.path.insert(0, str(COMMON))

from trajectory_tasks import build_grid_records  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Map trajectory records to spatial grids.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--lat-col")
    parser.add_argument("--lon-col")
    parser.add_argument("--geohash-precision", type=int, default=6)
    args = parser.parse_args()
    summary = build_grid_records(
        Path(args.input).expanduser().resolve(),
        Path(args.output_dir).expanduser().resolve(),
        args.geohash_precision,
        args.lat_col,
        args.lon_col,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
