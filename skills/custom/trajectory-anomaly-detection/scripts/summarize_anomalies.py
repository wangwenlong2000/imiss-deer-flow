#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Print a compact table of top trajectory anomalies.")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--limit", type=int, default=10)
    args = parser.parse_args()
    anomalies_path = Path(args.output_dir).expanduser().resolve() / "anomalies.jsonl"

    rows = []
    if anomalies_path.exists():
        with anomalies_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    rows.append(json.loads(line))

    for item in rows[: args.limit]:
        flags = item.get("flags", [])
        first = flags[0] if flags else {}
        print(
            f"[{item.get('rank', '?')}] score={item.get('anomaly_score')} "
            f"group={item.get('group')} time={item.get('timestamp')} "
            f"metric={first.get('metric')} method={first.get('method')} "
            f"direction={first.get('direction')} value={first.get('value')}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

