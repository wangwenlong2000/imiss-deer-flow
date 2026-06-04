#!/usr/bin/env python3
"""轨迹脱敏 — 用户ID哈希化 + geohash 粗化 + 时间分桶。"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

COMMON = Path(__file__).resolve().parents[2] / "_trajectory_common_v2"
sys.path.insert(0, str(COMMON))

from trajectory_tasks import (  # noqa: E402
    read_records,
    normalize_record,
    hash_uid,
    write_jsonl,
    write_summary,
)


def _bucket_ts(ts, mode: str):
    if ts is None:
        return None
    if mode == "day":
        return ts.strftime("%Y-%m-%d")
    return ts.strftime("%Y-%m-%dT%H:00:00")


def anonymize(input_path: Path, output_dir: Path, coarse_precision: int,
              time_bucket: str, salt: str, drop_latlon: bool) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    rows = read_records(input_path)
    out = []
    for rec in rows:
        n = normalize_record(rec)
        coarse_gh = (n.get("geohash") or "")[:coarse_precision]
        rec_out = {
            "user_id": hash_uid(n.get("user_id"), salt=salt) if n.get("user_id") is not None else None,
            "geohash": coarse_gh,
            "ts": _bucket_ts(n.get("ts"), time_bucket),
            "city": n.get("city"),
        }
        if not drop_latlon:
            rec_out["lat"] = n.get("lat")
            rec_out["lon"] = n.get("lon")
        out.append(rec_out)

    write_jsonl(output_dir / "anonymized.jsonl", out)

    cell_counts = Counter(r["geohash"] for r in out if r["geohash"])
    k_min = min(cell_counts.values()) if cell_counts else 0
    summary = {
        "skill": "anonymize_trajectory_data",
        "n_input": len(rows),
        "n_output": len(out),
        "n_unique_pseudonyms": len({r["user_id"] for r in out if r["user_id"]}),
        "coarse_precision": coarse_precision,
        "time_bucket": time_bucket,
        "drop_latlon": drop_latlon,
        "n_coarse_cells": len(cell_counts),
        "min_cell_k": k_min,
        "outputs": {
            "anonymized": str(output_dir / "anonymized.jsonl"),
            "summary": str(output_dir / "summary.json"),
        },
    }
    write_summary(output_dir, summary)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="轨迹数据脱敏")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--coarse-precision", type=int, default=5)
    parser.add_argument("--time-bucket", choices=["hour", "day"], default="hour")
    parser.add_argument("--salt", default="spatiotemporal_v1")
    parser.add_argument("--keep-latlon", action="store_true",
                        help="保留精确经纬度（默认丢弃）")
    args = parser.parse_args()
    summary = anonymize(
        Path(args.input).expanduser().resolve(),
        Path(args.output_dir).expanduser().resolve(),
        coarse_precision=args.coarse_precision,
        time_bucket=args.time_bucket,
        salt=args.salt,
        drop_latlon=not args.keep_latlon,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
