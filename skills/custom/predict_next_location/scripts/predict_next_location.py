#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

COMMON = Path(__file__).resolve().parents[2] / "citybench-skills-pack-new" / "custom" / "_trajectory_common"
sys.path.insert(0, str(COMMON))

from trajectory_tasks import predict_next_location  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Predict next location with a Markov chain.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--current-geohash")
    parser.add_argument("--current-time")
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--geohash-precision", type=int, default=5)
    parser.add_argument("--train-only", action="store_true")
    args = parser.parse_args()
    summary = predict_next_location(
        Path(args.input).expanduser().resolve(),
        Path(args.output_dir).expanduser().resolve(),
        current_geohash=args.current_geohash,
        current_time=args.current_time,
        top_k=args.top_k,
        precision=args.geohash_precision,
        train_only=args.train_only,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
