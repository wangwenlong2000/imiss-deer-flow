#!/usr/bin/env python3
"""Create a small demo trajectory CSV for DeerFlow Skill smoke tests."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


ROWS = [
    ("u1", "2024-01-01T08:00:00", 31.23040, 121.47370),
    ("u1", "2024-01-01T08:10:00", 31.23100, 121.47420),
    ("u1", "2024-01-01T08:20:00", 31.23150, 121.47480),
    ("u1", "2024-01-01T10:00:00", 31.23200, 121.47500),
    ("u1", "2024-01-01T10:12:00", 31.23270, 121.47580),
    ("u1", "2024-01-01T10:24:00", 31.23350, 121.47650),
    ("u1", "2024-01-01T13:00:00", 31.23400, 121.47700),
    ("u1", "2024-01-01T13:15:00", 31.23500, 121.47800),
    ("u1", "2024-01-01T13:30:00", 31.23600, 121.47900),
    ("u1", "2024-01-01T18:00:00", 31.23040, 121.47370),
    ("u1", "2024-01-01T18:40:00", 31.35000, 121.62000),
    ("u1", "2024-01-01T19:20:00", 31.51000, 121.80000),
    ("u2", "2024-01-01T08:00:00", 39.90420, 116.40740),
    ("u2", "2024-01-01T08:10:00", 39.90500, 116.40800),
    ("u2", "2024-01-01T08:20:00", 39.90600, 116.40900),
    ("u2", "2024-01-01T10:00:00", 39.90700, 116.41000),
    ("u2", "2024-01-01T10:15:00", 39.90800, 116.41100),
    ("u2", "2024-01-01T10:30:00", 39.90900, 116.41200),
    ("u2", "2024-01-01T13:00:00", 39.91000, 116.41300),
    ("u2", "2024-01-01T13:14:00", 39.91100, 116.41400),
    ("u2", "2024-01-01T13:28:00", 39.91200, 116.41500),
    ("u2", "2024-01-01T18:00:00", 39.90420, 116.40740),
    ("u2", "2024-01-01T18:40:00", 40.05000, 116.58000),
    ("u2", "2024-01-01T19:20:00", 40.20000, 116.76000),
]


def main() -> int:
    parser = argparse.ArgumentParser(description="Create a deterministic demo trajectory CSV.")
    parser.add_argument("--output", required=True, help="Output CSV path.")
    args = parser.parse_args()

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    with output.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["user_id", "timestamp", "lat", "lon"])
        writer.writerows(ROWS)

    print(f"Wrote demo trajectory sample with {len(ROWS)} rows to {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
