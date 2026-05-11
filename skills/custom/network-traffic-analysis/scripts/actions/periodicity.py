from __future__ import annotations

from typing import Any

import duckdb  # type: ignore

from analysis.anomaly_models import score_timeseries_rcf
from analysis.feature_engineering import rows_from_query
from utils.formatter import render_rows_section
from core.schema_mapping import available_canonical_fields, ensure_required
from utils.math import _dominant_periodicity, _lag_autocorrelation, _safe_float_local, _safe_text
from utils.formatter import _rows_to_tuples
from utils.sql import analysis_time_bucket_expr


def _where_to_and(where_clause: str) -> str:
    stripped = where_clause.strip()
    if not stripped:
        return ""
    if stripped.upper().startswith("WHERE"):
        body = stripped[len("WHERE"):].strip()
        return f" AND {body}" if body else ""
    if stripped.upper().startswith("AND"):
        return f" {stripped}"
    return f" {stripped}"


def execute_periodicity_review(
    con: duckdb.DuckDBPyConnection,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    interval: str,
    limit: int,
) -> dict[str, Any]:
    """Execute periodicity-review analysis and return structured data dict."""
    # Type guard for SQL interpolation
    if not isinstance(limit, int) or limit < 1:
        raise ValueError(f"limit must be a positive integer, got {limit!r}")

    bucket_expr = analysis_time_bucket_expr(interval)
    scoped_connector = "AND" if where_clause else "WHERE"

    # Overview buckets
    overview_sql = f"""
        SELECT {bucket_expr} AS bucket,
               COUNT(*) AS records,
               SUM(COALESCE(bytes, 0)) AS total_bytes,
               SUM(COALESCE(packets, 0)) AS total_packets
        FROM flows
        {where_clause}
        GROUP BY 1
        ORDER BY 1
    """
    _, overview_rows = rows_from_query(con, overview_sql)

    # Dataset-level periodicity
    bucket_series = [
        {
            "records": _safe_float_local(row.get("records")),
            "total_bytes": _safe_float_local(row.get("total_bytes")),
            "total_packets": _safe_float_local(row.get("total_packets")),
        }
        for row in overview_rows
    ]
    bucket_rcf_scores = score_timeseries_rcf(
        bucket_series, numeric_fields=["records", "total_bytes", "total_packets"]
    )

    bytes_values = [row["total_bytes"] for row in bucket_series]
    records_values = [row["records"] for row in bucket_series]
    packets_values = [row["total_packets"] for row in bucket_series]
    byte_period, byte_corr = _dominant_periodicity(bytes_values)
    record_period, record_corr = _dominant_periodicity(records_values)
    packet_period, packet_corr = _dominant_periodicity(packets_values)

    # Source-level candidates
    source_sql = f"""
        WITH bucketed AS (
            SELECT src_ip,
                   {bucket_expr} AS bucket,
                   COUNT(*) AS records,
                   SUM(COALESCE(bytes, 0)) AS total_bytes,
                   SUM(COALESCE(packets, 0)) AS total_packets
            FROM flows
            {where_clause}
            {scoped_connector} src_ip IS NOT NULL
            GROUP BY 1, 2
        ),
        source_summary AS (
            SELECT src_ip,
                   COUNT(*) AS bucket_count,
                   SUM(records) AS total_records,
                   SUM(total_bytes) AS total_bytes,
                   SUM(total_packets) AS total_packets
            FROM bucketed
            GROUP BY 1
            HAVING COUNT(*) >= 4
        )
        SELECT src_ip, bucket, records, total_bytes, total_packets
        FROM bucketed
        WHERE src_ip IN (
            SELECT src_ip
            FROM source_summary
            ORDER BY total_records DESC, total_bytes DESC
            LIMIT {max(limit * 3, 12)}
        )
        ORDER BY src_ip, bucket
    """
    _, source_rows = rows_from_query(con, source_sql)
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in source_rows:
        grouped.setdefault(_safe_text(row.get("src_ip")), []).append(row)

    ranked_rows: list[tuple[Any, ...]] = []
    for src_ip, ip_rows in grouped.items():
        series_rows = [
            {
                "records": _safe_float_local(row.get("records")),
                "total_bytes": _safe_float_local(row.get("total_bytes")),
                "total_packets": _safe_float_local(row.get("total_packets")),
            }
            for row in ip_rows
        ]
        if len(series_rows) < 4:
            continue
        rcf_scores = score_timeseries_rcf(
            series_rows, numeric_fields=["records", "total_bytes", "total_packets"]
        )
        bytes_period, bytes_corr = _dominant_periodicity(
            [row["total_bytes"] for row in series_rows]
        )
        records_period, records_corr = _dominant_periodicity(
            [row["records"] for row in series_rows]
        )
        periodicity_score = round(
            0.55 * max(bytes_corr, records_corr) + 0.45 * max(rcf_scores), 4
        )
        severity = (
            "high"
            if periodicity_score >= 0.75
            else "medium" if periodicity_score >= 0.5 else "low"
        )
        if max(bytes_corr, records_corr) >= 0.7:
            reason = "strong_periodic_beacon_pattern"
        elif max(rcf_scores) >= 0.7:
            reason = "structural_time_series_outlier"
        else:
            reason = "weak_periodicity_signal"
        ranked_rows.append(
            (
                src_ip,
                len(series_rows),
                int(sum(row["records"] for row in series_rows)),
                int(sum(row["total_bytes"] for row in series_rows)),
                bytes_period,
                round(bytes_corr, 4),
                records_period,
                round(records_corr, 4),
                round(max(rcf_scores), 4),
                periodicity_score,
                severity,
                reason,
            )
        )

    ranked_rows.sort(key=lambda item: item[9], reverse=True)

    return {
        "interval": interval,
        "overview_rows": overview_rows,
        "bucket_count": len(overview_rows),
        "byte_period": byte_period,
        "byte_corr": byte_corr,
        "record_period": record_period,
        "record_corr": record_corr,
        "packet_period": packet_period,
        "packet_corr": packet_corr,
        "bucket_rcf_scores": bucket_rcf_scores,
        "max_bucket_rcf_score": round(max(bucket_rcf_scores), 4) if bucket_rcf_scores else 0.0,
        "ranked_rows": ranked_rows,
        "_limit": limit,
    }


def format_periodicity_review(data: dict[str, Any], raw_output: str | None = None) -> str:
    """Produce the text report for backward-compatible output."""
    sections = [f"Periodicity interval: {data['interval']}"]

    # Baseline buckets
    sections.append(
        render_rows_section(
            "Periodicity baseline buckets",
            ["bucket", "records", "total_bytes", "total_packets"],
            _rows_to_tuples(
                ["bucket", "records", "total_bytes", "total_packets"],
                data["overview_rows"][: data["_limit"]],
            ),
        )
    )

    if data["bucket_count"] < 4:
        sections.append(
            "Periodicity review\nNot enough time buckets to estimate periodic structure. Capture a longer timespan or use a finer interval."
        )
        return "\n\n".join(sections)

    # Dataset periodicity summary
    def _fmt_period(period: int | None) -> str:
        return str(period) if period is not None else "N/A(insufficient_data)"

    sections.append(
        "\n".join(
            [
                "Dataset periodicity summary",
                f"bytes_period_lag={_fmt_period(data['byte_period'])}, bytes_autocorr={round(data['byte_corr'], 4)}, records_period_lag={_fmt_period(data['record_period'])}, records_autocorr={round(data['record_corr'], 4)}, packets_period_lag={_fmt_period(data['packet_period'])}, packets_autocorr={round(data['packet_corr'], 4)}",
                f"max_rcf_score={data['max_bucket_rcf_score']}",
            ]
        )
    )

    # Ranked candidates
    sections.append(
        render_rows_section(
            "Top periodicity candidates (autocorrelation + RCF)",
            [
                "src_ip",
                "bucket_count",
                "total_records",
                "total_bytes",
                "bytes_period_lag",
                "bytes_autocorr",
                "records_period_lag",
                "records_autocorr",
                "max_rcf_score",
                "periodicity_score",
                "severity",
                "likely_reason",
            ],
            data["ranked_rows"][: data["_limit"]],
        )
    )

    return "\n\n".join(sections)


def build_skill_result_parts(data: dict[str, Any], raw_output: str) -> dict[str, Any]:
    """Build structured SkillResult for periodicity-review action."""
    evidence: list[dict[str, Any]] = []
    findings: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    metrics: list[dict[str, Any]] = []

    limit = data["_limit"]
    ranked_rows = data["ranked_rows"]
    bucket_count = data["bucket_count"]

    # Metrics
    high_periodicity = sum(1 for r in ranked_rows if r[9] >= 0.65)
    max_score = max((r[9] for r in ranked_rows), default=0.0)

    metrics.append({"name": "time_bucket_count", "value": bucket_count})
    metrics.append({"name": "candidate_pairs", "value": len(ranked_rows)})
    metrics.append({"name": "high_periodicity_pairs", "value": high_periodicity})
    metrics.append({"name": "max_periodicity_score", "value": max_score})

    evidence.append(
        {
            "evidence_id": "e-periodicity-metrics",
            "type": "metric",
            "title": "Periodicity Review Metrics",
            "metrics": metrics,
        }
    )

    # Periodicity candidates table
    if ranked_rows:
        candidate_columns = [
            "src_ip", "bucket_count", "total_records", "total_bytes",
            "bytes_period_lag", "bytes_autocorr", "records_period_lag",
            "records_autocorr", "max_rcf_score", "periodicity_score",
            "severity", "likely_reason",
        ]
        candidate_rows = [list(r) for r in ranked_rows[:limit]]
        evidence.append(
            {
                "evidence_id": "e-periodicity-candidates",
                "type": "table",
                "title": "Periodicity Candidates",
                "columns": candidate_columns,
                "rows": candidate_rows,
            }
        )

    # Overview buckets table (optional interval details)
    overview_rows = data["overview_rows"]
    if overview_rows:
        overview_columns = ["bucket", "records", "total_bytes", "total_packets"]
        overview_table_rows = [
            [r.get(c) for c in overview_columns] for r in overview_rows[:limit]
        ]
        evidence.append(
            {
                "evidence_id": "e-periodicity-intervals",
                "type": "table",
                "title": "Periodicity Baseline Buckets",
                "columns": overview_columns,
                "rows": overview_table_rows,
            }
        )

    # Findings
    for row in ranked_rows[:limit]:
        score = row[9]
        if score >= 0.65:
            findings.append(
                {
                    "finding_id": f"f-periodic-beacon-{row[0]}",
                    "type": "periodic_beacon_candidate",
                    "severity": row[10],
                    "confidence": score,
                    "title": f"Periodic beacon candidate: {row[0]}",
                    "description": f"Source {row[0]} shows periodicity score {score} across {row[1]} time buckets ({row[2]} records, {row[3]} bytes). Likely pattern: {row[11]}.",
                    "entities": [{"type": "src_ip", "value": row[0]}],
                    "evidence_refs": [
                        "e-periodicity-candidates",
                        "e-periodicity-metrics",
                    ],
                }
            )

    # Highly regular interval finding
    for row in ranked_rows[:limit]:
        records_corr = row[7]
        records_period = row[6]
        bucket_count_src = row[1]
        if records_corr >= 0.8 and bucket_count_src >= 6:
            findings.append(
                {
                    "finding_id": f"f-regular-interval-{row[0]}",
                    "type": "highly_regular_interval",
                    "severity": "high" if records_corr >= 0.9 else "medium",
                    "confidence": records_corr,
                    "title": f"Highly regular interval: {row[0]}",
                    "description": f"Source {row[0]} has highly regular communication with interval CV equivalent (autocorr={records_corr}, period lag={records_period}). This is consistent with beaconing behavior.",
                    "entities": [{"type": "src_ip", "value": row[0]}],
                    "evidence_refs": [
                        "e-periodicity-candidates",
                        "e-periodicity-metrics",
                    ],
                }
            )

    # Warnings
    if bucket_count < 4:
        warnings.append(
            {
                "code": "insufficient_time_buckets",
                "message": f"Only {bucket_count} time buckets available. Periodicity analysis requires at least 4 buckets for meaningful autocorrelation.",
                "severity": "warning",
            }
        )

    # Raw report
    evidence.append(
        {
            "evidence_id": "e-raw-report",
            "type": "text",
            "title": "Raw Periodicity Review Output",
            "content": raw_output,
        }
    )

    # Fix evidence_refs
    existing_ids = {e["evidence_id"] for e in evidence}
    for finding in findings:
        finding["evidence_refs"] = [
            ref for ref in finding["evidence_refs"] if ref in existing_ids
        ]

    # Compute severity
    severity_order = {"critical": 4, "high": 3, "medium": 2, "low": 1, "info": 0}
    highest = "info"
    for f in findings:
        if severity_order.get(f.get("severity", "info"), 0) > severity_order.get(highest, 0):
            highest = f["severity"]

    overview_text = f"Periodicity review (interval: {data['interval']}, {bucket_count} buckets)."
    if findings:
        overview_text += f" {len(findings)} finding(s) identified."

    return {
        "summary": {
            "title": "Periodicity Review",
            "overview": overview_text,
            "severity": highest,
            "confidence": None,
            "key_metrics": metrics,
        },
        "findings": findings,
        "evidence": evidence,
        "artifacts": [],
        "diagnostics": {
            "warnings": warnings,
            "data_quality": {},
        },
    }


def periodicity_review_action(
    con: duckdb.DuckDBPyConnection,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    interval: str,
    limit: int,
) -> str:
    try:
        data = execute_periodicity_review(con, mappings, where_clause, interval, limit)
        return format_periodicity_review(data)
    except Exception as exc:
        return (
            f"Periodicity review failed: {exc}\n"
            "Hint: Run --action inspect to verify schema, or --action timeseries --interval minute for basic time profile."
        )
