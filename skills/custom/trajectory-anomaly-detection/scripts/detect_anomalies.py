#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from anomaly_lib import (
    add_flag,
    detect_group_col,
    detect_metrics,
    detect_time_col,
    esd_flags,
    flatten,
    iqr_flags,
    isolation_forest_flags,
    lof_flags,
    mad_flags,
    parse_float,
    parse_time,
    read_records,
    rolling_z_flags,
    write_jsonl,
    zscore_flags,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Detect anomalies in trajectory or mobility records.")
    parser.add_argument("--input", required=True, help="Input CSV/TSV/JSON/JSONL file")
    parser.add_argument("--output-dir", required=True, help="Directory for anomaly outputs")
    parser.add_argument("--group-col", help="Grouping field, e.g. user_id or meta.geo_scope.geohash")
    parser.add_argument("--time-col", help="Timestamp field used for rolling detection")
    parser.add_argument("--metric", action="append", help="Metric field to score. Can be repeated.")
    parser.add_argument(
        "--method",
        choices=["ensemble", "zscore", "mad", "iqr", "esd", "rolling-z", "lof", "isolation-forest"],
        default="ensemble",
    )
    parser.add_argument("--top-k", type=int, default=20)
    parser.add_argument("--min-group-size", type=int, default=3)
    parser.add_argument("--z-threshold", type=float, default=3.0)
    parser.add_argument("--mad-threshold", type=float, default=3.5)
    parser.add_argument("--iqr-multiplier", type=float, default=1.5)
    parser.add_argument("--esd-max-fraction", type=float, default=0.1)
    parser.add_argument("--rolling-window", type=int, default=5)
    parser.add_argument("--lof-k", type=int, default=5)
    parser.add_argument("--lof-threshold", type=float, default=1.6)
    parser.add_argument("--contamination", type=float, default=0.1)
    return parser


def selected_methods(method: str) -> list[str]:
    if method == "ensemble":
        return ["zscore", "mad", "iqr", "esd", "rolling-z", "lof"]
    return [method]


def main() -> int:
    args = build_parser().parse_args()
    input_path = Path(args.input).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_records = read_records(input_path)
    flat_records = [flatten(record) for record in raw_records]
    time_col = detect_time_col(flat_records, args.time_col)
    group_col = detect_group_col(flat_records, args.group_col)
    metrics = detect_metrics(flat_records, args.metric)
    methods = selected_methods(args.method)
    warnings: list[str] = []

    if not raw_records:
        warnings.append("Input has no records.")
    if not metrics:
        warnings.append("No numeric metrics were detected. Use --metric to specify fields.")
    if group_col is None:
        warnings.append("No group column detected; all records were scored together.")

    groups: dict[str, list[int]] = defaultdict(list)
    timestamps: dict[int, Any] = {}
    for idx, record in enumerate(flat_records):
        group = str(record.get(group_col, "_all")) if group_col else "_all"
        groups[group].append(idx)
        timestamps[idx] = parse_time(record.get(time_col)) if time_col else None

    flags_by_record: dict[int, list[dict[str, Any]]] = defaultdict(list)

    for group, indexes in groups.items():
        if len(indexes) < args.min_group_size:
            warnings.append(f"Group {group} has only {len(indexes)} records; some methods were skipped.")
        for metric in metrics:
            metric_items = [
                (idx, parse_float(flat_records[idx].get(metric)))
                for idx in indexes
            ]
            metric_items = [(idx, value) for idx, value in metric_items if value is not None]
            if len(metric_items) < args.min_group_size:
                continue
            local_indexes = [idx for idx, _ in metric_items]
            values = [float(value) for _, value in metric_items]

            if "zscore" in methods:
                for local_idx, score in zscore_flags(values, args.z_threshold).items():
                    record_idx = local_indexes[local_idx]
                    add_flag(flags_by_record, record_idx, metric, "zscore", score, values[local_idx])
            if "mad" in methods:
                for local_idx, score in mad_flags(values, args.mad_threshold).items():
                    record_idx = local_indexes[local_idx]
                    add_flag(flags_by_record, record_idx, metric, "mad", score, values[local_idx])
            if "iqr" in methods:
                for local_idx, score in iqr_flags(values, args.iqr_multiplier).items():
                    record_idx = local_indexes[local_idx]
                    signed = score if values[local_idx] >= sorted(values)[len(values) // 2] else -score
                    add_flag(flags_by_record, record_idx, metric, "iqr", signed, values[local_idx])
            if "esd" in methods:
                for local_idx, score in esd_flags(values, args.z_threshold, args.esd_max_fraction).items():
                    record_idx = local_indexes[local_idx]
                    add_flag(flags_by_record, record_idx, metric, "esd", score, values[local_idx])
            if "rolling-z" in methods:
                rolling_items = [
                    (idx, timestamps[idx], float(value))
                    for idx, value in metric_items
                ]
                for record_idx, score in rolling_z_flags(rolling_items, args.z_threshold, args.rolling_window).items():
                    add_flag(
                        flags_by_record,
                        record_idx,
                        metric,
                        "rolling-z",
                        score,
                        float(flat_records[record_idx][metric]),
                    )

    vector_records: list[tuple[int, list[float]]] = []
    for idx, record in enumerate(flat_records):
        vector = []
        for metric in metrics:
            value = parse_float(record.get(metric))
            if value is None:
                break
            vector.append(value)
        else:
            if vector:
                vector_records.append((idx, vector))

    if "lof" in methods and len(metrics) >= 1:
        for record_idx, score in lof_flags(vector_records, args.lof_threshold, args.lof_k).items():
            flags_by_record[record_idx].append({
                "metric": ",".join(metrics),
                "method": "lof",
                "score": round(float(score), 6),
                "signed_score": round(float(score), 6),
                "direction": "local_outlier",
                "value": None,
            })

    if "isolation-forest" in methods:
        isolation_flags, warning = isolation_forest_flags(vector_records, args.contamination)
        if warning:
            warnings.append(warning)
        for record_idx, score in isolation_flags.items():
            flags_by_record[record_idx].append({
                "metric": ",".join(metrics),
                "method": "isolation-forest",
                "score": round(float(score), 6),
                "signed_score": round(float(score), 6),
                "direction": "isolated",
                "value": None,
            })

    scored_records: list[dict[str, Any]] = []
    anomalies: list[dict[str, Any]] = []
    for idx, record in enumerate(raw_records):
        flags = flags_by_record.get(idx, [])
        score = sum(flag.get("score", 0.0) for flag in flags)
        group = str(flat_records[idx].get(group_col, "_all")) if group_col else "_all"
        timestamp_value = flat_records[idx].get(time_col) if time_col else None
        scored = {
            "record_index": idx,
            "group": group,
            "timestamp": timestamp_value,
            "anomaly_score": round(float(score), 6),
            "is_anomaly": bool(flags),
            "flags": flags,
            "record": record,
        }
        scored_records.append(scored)
        if flags:
            anomalies.append(scored)

    anomalies.sort(key=lambda item: item["anomaly_score"], reverse=True)
    for rank, item in enumerate(anomalies, start=1):
        item["rank"] = rank
    top_anomalies = anomalies[: args.top_k]

    write_jsonl(output_dir / "scored_records.jsonl", scored_records)
    write_jsonl(output_dir / "anomalies.jsonl", top_anomalies)

    summary = {
        "ok": True,
        "input": str(input_path),
        "output_dir": str(output_dir),
        "detected": {
            "group_col": group_col,
            "time_col": time_col,
            "metrics": metrics,
        },
        "parameters": {
            "method": args.method,
            "methods_used": methods,
            "top_k": args.top_k,
            "min_group_size": args.min_group_size,
            "z_threshold": args.z_threshold,
            "mad_threshold": args.mad_threshold,
            "iqr_multiplier": args.iqr_multiplier,
            "rolling_window": args.rolling_window,
            "lof_k": args.lof_k,
            "lof_threshold": args.lof_threshold,
            "contamination": args.contamination,
        },
        "counts": {
            "records": len(raw_records),
            "scored_records": len(scored_records),
            "groups": len(groups),
            "anomalies": len(anomalies),
            "exported_anomalies": len(top_anomalies),
        },
        "outputs": {
            "anomalies": str(output_dir / "anomalies.jsonl"),
            "scored_records": str(output_dir / "scored_records.jsonl"),
            "summary": str(output_dir / "summary.json"),
        },
        "top_anomalies": [
            {
                "rank": item["rank"],
                "record_index": item["record_index"],
                "group": item["group"],
                "timestamp": item["timestamp"],
                "anomaly_score": item["anomaly_score"],
                "flags": item["flags"][:5],
            }
            for item in top_anomalies[:10]
        ],
        "warnings": sorted(set(warnings)),
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

