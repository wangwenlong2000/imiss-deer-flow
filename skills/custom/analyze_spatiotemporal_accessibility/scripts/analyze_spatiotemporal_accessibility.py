#!/usr/bin/env python3
"""经验等时圈空间可达性 — 基于历史 OD 耗时矩阵跑 Dijkstra，多 budget 切等时圈。"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

COMMON = Path(__file__).resolve().parents[2] / "_trajectory_common_v2"
sys.path.insert(0, str(COMMON))

from trajectory_tasks import analyze_spatiotemporal_accessibility  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="经验等时圈空间可达性")
    parser.add_argument("--od-matrix", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--origin-geohash", default=None, help="起点 geohash（必填）")
    parser.add_argument("--budgets-min", default="15,30,60", help="时间预算列表（分钟），逗号分隔")
    parser.add_argument("--min-flow", type=int, default=2, help="OD 边的最小流量阈值")
    args = parser.parse_args()
    summary = analyze_spatiotemporal_accessibility(
        Path(args.od_matrix).expanduser().resolve(),
        Path(args.output_dir).expanduser().resolve(),
        origin_geohash=args.origin_geohash,
        budgets_min=[float(x) for x in args.budgets_min.split(',')],
        min_flow=args.min_flow
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
