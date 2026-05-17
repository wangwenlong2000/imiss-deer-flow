#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

COMMON = Path(__file__).resolve().parents[2] / "citybench-skills-pack-new" / "custom" / "_trajectory_common"
sys.path.insert(0, str(COMMON))

from trajectory_tasks import classify_trajectory_state  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Classify user trajectory states.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--geohash-precision", type=int, default=5)
    args = parser.parse_args()
    summary = classify_trajectory_state(
        Path(args.input).expanduser().resolve(),
        Path(args.output_dir).expanduser().resolve(),
        precision=args.geohash_precision,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
