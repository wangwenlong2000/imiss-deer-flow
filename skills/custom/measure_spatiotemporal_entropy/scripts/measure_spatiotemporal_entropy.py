#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

COMMON = Path(__file__).resolve().parents[2] / "citybench-skills-pack-new" / "custom" / "_trajectory_common"
sys.path.insert(0, str(COMMON))

from trajectory_tasks import measure_spatiotemporal_entropy  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Measure spatiotemporal entropy and vitality.")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--od-matrix")
    parser.add_argument("--evidence")
    args = parser.parse_args()
    summary = measure_spatiotemporal_entropy(
        Path(args.output_dir).expanduser().resolve(),
        od_matrix=Path(args.od_matrix).expanduser().resolve() if args.od_matrix else None,
        evidence=Path(args.evidence).expanduser().resolve() if args.evidence else None,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
