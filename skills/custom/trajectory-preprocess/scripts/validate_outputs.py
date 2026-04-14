#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


REQUIRED = {
    "cleaned_points.jsonl": {"user_id", "timestamp", "lat", "lon", "geohash"},
    "staypoints.jsonl": {"user_id", "start_time", "end_time", "duration_minutes", "centroid_lat", "centroid_lon"},
    "trips.jsonl": {"user_id", "start_time", "end_time", "point_count", "distance_km"},
}


def read_first_jsonl(path: Path) -> dict[str, Any] | None:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                return json.loads(line)
    return None


def count_jsonl(path: Path) -> int:
    count = 0
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                count += 1
    return count


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate trajectory preprocessing outputs.")
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()

    errors: list[str] = []
    warnings: list[str] = []
    counts: dict[str, int] = {}

    summary_path = output_dir / "summary.json"
    if not summary_path.exists():
        errors.append("summary.json is missing")
    else:
        try:
            json.loads(summary_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            errors.append(f"summary.json is invalid JSON: {exc}")

    for filename, required_fields in REQUIRED.items():
        path = output_dir / filename
        if not path.exists():
            errors.append(f"{filename} is missing")
            continue
        counts[filename] = count_jsonl(path)
        first = read_first_jsonl(path)
        if first is None:
            warnings.append(f"{filename} is empty")
            continue
        missing = sorted(required_fields - set(first.keys()))
        if missing:
            errors.append(f"{filename} missing fields: {', '.join(missing)}")

    payload = {
        "ok": not errors,
        "output_dir": str(output_dir),
        "counts": counts,
        "errors": errors,
        "warnings": warnings,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())

