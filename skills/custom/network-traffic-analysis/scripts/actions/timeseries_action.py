"""
Timeseries Action

Aggregates flows into time buckets and computes temporal anomaly scores
using EWMA residuals and RCF structural outlier detection.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb  # type: ignore

from analysis.anomaly_models import score_timeseries_rcf
from utils.formatter import render_rows_section, export_rows
from utils.math import _safe_float_local
from utils.sql import analysis_time_bucket_expr


def execute_timeseries(
    con: duckdb.DuckDBPyConnection,
    where_clause: str,
    files: list[str],
    *,
    interval: str = "hour",
    limit: int = 20,
    output_file: str | None = None,
    **kwargs,
) -> dict:
    """Execute timeseries analysis and return structured results."""
    from utils.math import _mean_std, _zscore

    # Determine sort key based on time kind
    # For relative time: extract numeric seconds for proper numeric sorting
    # For absolute time: bucket is already a timestamp string that sorts correctly
    bucket_expr = analysis_time_bucket_expr(interval)
    sql = f"""
        SELECT {bucket_expr} AS bucket,
               COUNT(*) AS records,
               SUM(COALESCE(bytes, 0)) AS total_bytes,
               SUM(COALESCE(packets, 0)) AS total_packets,
               MIN(analysis_time_relative_s) FILTER (WHERE analysis_time_kind = 'relative') AS sort_numeric,
               MIN(analysis_time_ts) FILTER (WHERE analysis_time_kind = 'absolute') AS sort_timestamp
        FROM flows
        {where_clause}
        GROUP BY 1
        ORDER BY sort_numeric ASC NULLS LAST, sort_timestamp ASC NULLS LAST, 1 ASC
    """
    result = con.execute(sql)
    columns = [item[0] for item in result.description]
    rows = result.fetchall()

    # Handle output_file export
    if output_file:
        export_columns = columns[:4]  # bucket, records, total_bytes, total_packets
        export_rows_data = [(r[0], r[1], r[2], r[3]) for r in rows]
        export_rows(export_columns, export_rows_data, output_file)
        return {
            "interval": interval,
            "bucket_rows": [],
            "scored_rows": [],
            "message": f"Timeseries data exported to {output_file}",
            "output_file": output_file,
            "total_buckets": len(rows),
        }

    bucket_rows = [
        {
            "bucket": row[0],
            "records": _safe_float_local(row[1]),
            "total_bytes": _safe_float_local(row[2]),
            "total_packets": _safe_float_local(row[3]),
        }
        for row in rows
    ]

    if len(bucket_rows) < 2:
        return {
            "bucket_rows": bucket_rows,
            "scored_rows": [],
            "interval": interval,
            "message": "Not enough buckets to compute temporal anomaly scores.",
            "limit": limit,
        }

    alpha = 0.3
    ewma_bytes_values: list[float] = []
    ewma_records_values: list[float] = []
    prev_bytes = bucket_rows[0]["total_bytes"]
    prev_records = bucket_rows[0]["records"]
    for row in bucket_rows:
        prev_bytes = alpha * row["total_bytes"] + (1 - alpha) * prev_bytes
        prev_records = alpha * row["records"] + (1 - alpha) * prev_records
        ewma_bytes_values.append(prev_bytes)
        ewma_records_values.append(prev_records)

    byte_residuals = [row["total_bytes"] - ewma_bytes_values[idx] for idx, row in enumerate(bucket_rows)]
    record_residuals = [row["records"] - ewma_records_values[idx] for idx, row in enumerate(bucket_rows)]
    packet_values = [row["total_packets"] for row in bucket_rows]
    packet_mean, packet_std = _mean_std(packet_values)
    byte_mean, byte_std = _mean_std(byte_residuals)
    record_mean, record_std = _mean_std(record_residuals)
    rcf_scores = score_timeseries_rcf(
        bucket_rows,
        numeric_fields=["records", "total_bytes", "total_packets"],
    )

    scored_rows: list[dict[str, Any]] = []
    for idx, row in enumerate(bucket_rows):
        byte_z = _zscore(byte_residuals[idx], byte_mean, byte_std)
        record_z = _zscore(record_residuals[idx], record_mean, record_std)
        packet_z = _zscore(row["total_packets"], packet_mean, packet_std)
        residual_score = max(abs(byte_z), abs(record_z), abs(packet_z))
        rcf_score = rcf_scores[idx]
        anomaly_score = round(0.55 * min(residual_score / 3.0, 1.0) + 0.45 * rcf_score, 4)
        severity = "critical" if anomaly_score >= 0.85 else "high" if anomaly_score >= 0.65 else "medium" if anomaly_score >= 0.45 else "low"
        if rcf_score >= 0.75 and residual_score < 1.2:
            reason = "rcf_structural_outlier"
        elif abs(byte_z) >= abs(record_z) and abs(byte_z) >= abs(packet_z):
            reason = "byte_volume_residual_spike"
        elif abs(record_z) >= abs(packet_z):
            reason = "record_count_residual_spike"
        else:
            reason = "packet_count_outlier"
        scored_rows.append({
            "bucket": row["bucket"],
            "records": int(row["records"]),
            "total_bytes": int(row["total_bytes"]),
            "total_packets": int(row["total_packets"]),
            "ewma_bytes": round(ewma_bytes_values[idx], 2),
            "byte_residual": round(byte_residuals[idx], 2),
            "byte_z": round(byte_z, 3),
            "record_z": round(record_z, 3),
            "packet_z": round(packet_z, 3),
            "rcf_score": round(rcf_score, 4),
            "anomaly_score": anomaly_score,
            "severity": severity,
            "likely_reason": reason,
        })

    # Keep full scored_rows for statistics, but sort for top-N display
    scored_rows.sort(key=lambda item: item["anomaly_score"], reverse=True)

    return {
        "interval": interval,
        "bucket_rows": bucket_rows,
        "scored_rows": scored_rows,
        "message": "",
        "limit": limit,
    }


def format_results(results: dict) -> str:
    """Format timeseries results as text."""
    sections = []

    # Handle export case
    output_file = results.get("output_file")
    if output_file:
        sections.append(f"Timeseries data exported to {output_file}")
        msg = results.get("message", "")
        if msg:
            sections.append(msg)
        return "\n\n".join(sections)

    # Baseline timeseries
    bucket_rows = results.get("bucket_rows", [])
    limit = results.get("limit", 20)
    if bucket_rows:
        columns = ["bucket", "records", "total_bytes", "total_packets"]
        rows_data = [[r["bucket"], r["records"], r["total_bytes"], r["total_packets"]] for r in bucket_rows]
        sections.append(render_rows_section("Timeseries baseline", columns, rows_data))

    # Anomaly review — limit output to top N
    scored_rows = results.get("scored_rows", [])
    if scored_rows:
        columns = [
            "bucket", "records", "total_bytes", "total_packets",
            "ewma_bytes", "byte_residual", "byte_z", "record_z",
            "packet_z", "rcf_score", "anomaly_score", "severity", "likely_reason",
        ]
        rows_data = [
            [r["bucket"], r["records"], r["total_bytes"], r["total_packets"],
             r["ewma_bytes"], r["byte_residual"], r["byte_z"], r["record_z"],
             r["packet_z"], r["rcf_score"], r["anomaly_score"], r["severity"], r["likely_reason"]]
            for r in scored_rows[:limit]
        ]
        sections.append(render_rows_section("Timeseries anomaly review (EWMA + residual z-score)", columns, rows_data))
    else:
        msg = results.get("message", "Not enough buckets to compute temporal anomaly scores.")
        sections.append(f"Timeseries anomaly review\n{msg}")

    return "\n\n".join(sections)


def build_skill_result_parts(
    results: dict,
    raw_output: str,
) -> dict[str, Any]:
    """Build structured SkillResult for timeseries action."""
    interval = results.get("interval", "hour")
    bucket_rows = results.get("bucket_rows", [])
    scored_rows = results.get("scored_rows", [])
    message = results.get("message", "")
    limit = results.get("limit", 20)

    findings: list[dict[str, Any]] = []
    evidence: list[dict[str, Any]] = []

    # Export case
    output_file = results.get("output_file")
    if output_file:
        artifacts = [{
            "artifact_id": "a-timeseries-export",
            "type": "csv_export",
            "title": f"Timeseries data exported to {Path(output_file).name}",
            "content": output_file,
        }]
        return {
            "summary": {
                "title": f"Timeseries Analysis ({interval})",
                "overview": f"Timeseries data exported to {output_file}. {results.get('total_buckets', 0)} buckets written.",
                "severity": "info",
                "confidence": 1.0,
                "key_metrics": [{"name": "total_buckets", "value": results.get("total_buckets", 0)}],
            },
            "findings": [],
            "evidence": [],
            "artifacts": artifacts,
            "diagnostics": {"warnings": [], "data_quality": {}},
        }

    # Baseline timeseries evidence — include all buckets for complete baseline
    if bucket_rows:
        evidence.append({
            "evidence_id": "e-timeseries-baseline",
            "type": "table",
            "title": f"Timeseries Baseline ({interval})",
            "columns": ["bucket", "records", "total_bytes", "total_packets"],
            "rows": [[r["bucket"], r["records"], r["total_bytes"], r["total_packets"]] for r in bucket_rows],
        })

    # Anomaly review evidence — limit to top N for readability
    if scored_rows:
        evidence.append({
            "evidence_id": "e-timeseries-anomaly",
            "type": "table",
            "title": f"Timeseries Anomaly Review ({interval})",
            "columns": ["bucket", "records", "total_bytes", "anomaly_score", "severity", "likely_reason"],
            "rows": [[r["bucket"], r["records"], r["total_bytes"], r["anomaly_score"], r["severity"], r["likely_reason"]] for r in scored_rows[:limit]],
        })

    # High severity findings
    high_score_rows = [r for r in scored_rows if r["anomaly_score"] >= 0.45]
    if high_score_rows:
        for row in high_score_rows[:5]:  # Top 5
            findings.append({
                "finding_id": f"f-ts-{row['bucket']}",
                "type": "temporal_anomaly",
                "severity": row["severity"],
                "confidence": min(row["anomaly_score"], 1.0),
                "title": f"Anomaly at {row['bucket']}: {row['likely_reason']} (score={row['anomaly_score']:.2f})",
                "description": f"Time bucket {row['bucket']} shows anomalous behavior with anomaly score {row['anomaly_score']:.2f}. Likely reason: {row['likely_reason']}. Records: {row['records']:,}, Bytes: {row['total_bytes']:,}, Packets: {row['total_packets']:,}.",
                "entities": [{"type": "time_bucket", "value": str(row["bucket"])}],
                "evidence_refs": ["e-timeseries-anomaly"],
            })

    total_buckets = len(bucket_rows)
    anomalous_buckets = len(high_score_rows)

    return {
        "summary": {
            "title": f"Timeseries Analysis ({interval})",
            "overview": f"{total_buckets} time buckets analyzed. {anomalous_buckets} buckets flagged with anomaly score >= 0.45.",
            "severity": "high" if any(f["severity"] in ("critical", "high") for f in findings) else "info",
            "confidence": 0.8,
            "key_metrics": [
                {"name": "interval", "value": interval},
                {"name": "total_buckets", "value": total_buckets},
                {"name": "anomalous_buckets", "value": anomalous_buckets},
            ],
        },
        "findings": findings,
        "evidence": evidence,
        "artifacts": [],
        "diagnostics": {
            "warnings": [{"code": "insufficient_buckets", "message": message, "severity": "info"}] if message else [],
            "data_quality": {},
        },
    }
