#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

COMMON = Path(__file__).resolve().parents[2] / "_trajectory_common"
sys.path.insert(0, str(COMMON))

from trajectory_tasks import fuse_spatial_evidence  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Fuse multiple spatial evidence files into a unified regional view.")
    parser.add_argument("--input", action="append", required=True, help="Repeat this argument to pass multiple JSONL/CSV inputs.")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--top-k", type=int, default=20)
    args = parser.parse_args()
    summary = fuse_spatial_evidence(
        [Path(item).expanduser().resolve() for item in args.input],
        Path(args.output_dir).expanduser().resolve(),
        args.top_k,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
