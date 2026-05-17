#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

COMMON = Path(__file__).resolve().parents[2] / "_trajectory_common"
sys.path.insert(0, str(COMMON))

from trajectory_tasks import route_anomaly  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Detect route-level trip anomalies.")
    parser.add_argument("--input", required=True, help="Input trips.jsonl")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--group-col", default="user_id")
    parser.add_argument("--threshold", type=float, default=3.5)
    parser.add_argument("--top-k", type=int, default=20)
    args = parser.parse_args()
    summary = route_anomaly(
        Path(args.input).expanduser().resolve(),
        Path(args.output_dir).expanduser().resolve(),
        args.group_col,
        args.threshold,
        args.top_k,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
