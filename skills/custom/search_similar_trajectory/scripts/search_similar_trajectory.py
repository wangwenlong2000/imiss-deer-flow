#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

COMMON = Path(__file__).resolve().parents[2] / "_trajectory_common"
sys.path.insert(0, str(COMMON))

from trajectory_tasks import similarity_search  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Search trips similar to a target trajectory.")
    parser.add_argument("--input", required=True, help="Input trips.jsonl")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--target-trip-id")
    parser.add_argument("--top-k", type=int, default=10)
    args = parser.parse_args()
    summary = similarity_search(
        Path(args.input).expanduser().resolve(),
        Path(args.output_dir).expanduser().resolve(),
        args.target_trip_id,
        args.top_k,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
