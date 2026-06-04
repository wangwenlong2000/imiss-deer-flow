#!/usr/bin/env python3
"""时空分布熵 / 活力指数 — 香农熵（来源/去向/时段三维）+ 综合活力指数。"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

COMMON = Path(__file__).resolve().parents[2] / "_trajectory_common_v2"
sys.path.insert(0, str(COMMON))

from trajectory_tasks import measure_spatiotemporal_entropy  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="时空分布熵 / 活力指数")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--od-matrix", default=None, help="od_matrix.jsonl")
    parser.add_argument("--evidence", default=None, help="evidence.jsonl")
    args = parser.parse_args()
    summary = measure_spatiotemporal_entropy(
        Path(args.output_dir).expanduser().resolve(),
        od_matrix=Path(args.od_matrix).expanduser().resolve() if args.od_matrix else None,
        evidence=Path(args.evidence).expanduser().resolve() if args.evidence else None
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
