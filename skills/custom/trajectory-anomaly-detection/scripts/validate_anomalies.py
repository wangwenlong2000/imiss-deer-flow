#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


def count_jsonl(path: Path) -> int:
    count = 0
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                json.loads(line)
                count += 1
    return count


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate trajectory anomaly detection outputs.")
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()

    errors: list[str] = []
    warnings: list[str] = []
    counts: dict[str, int] = {}

    for filename in ["summary.json", "anomalies.jsonl", "scored_records.jsonl"]:
        path = output_dir / filename
        if not path.exists():
            errors.append(f"{filename} is missing")
            continue
        try:
            if filename.endswith(".json"):
                json.loads(path.read_text(encoding="utf-8"))
            else:
                counts[filename] = count_jsonl(path)
        except Exception as exc:
            errors.append(f"{filename} is invalid: {exc}")

    if counts.get("scored_records.jsonl", 0) == 0:
        warnings.append("scored_records.jsonl is empty")

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

