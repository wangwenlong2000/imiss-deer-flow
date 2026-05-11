from __future__ import annotations

from typing import Any

import duckdb  # type: ignore

from utils.formatter import render_rows_section, render_section
from utils.math import _shannon_entropy
from utils.sql import analysis_time_bucket_expr, quote_identifier


def protocol_drift_section(
    con: duckdb.DuckDBPyConnection,
    where_clause: str,
    *,
    field_name: str,
    title: str,
    limit: int,
) -> str:
    from utils.math import _mean_std, _safe_text, _zscore
    from analysis.feature_engineering import rows_from_query
    from utils.math import _safe_float_local

    bucket_expr = analysis_time_bucket_expr("hour")
    sql = f"""
        WITH scoped AS (
            SELECT {bucket_expr} AS bucket,
                   COALESCE(CAST({quote_identifier(field_name)} AS VARCHAR), 'UNKNOWN') AS protocol_value,
                   COUNT(*) AS records
            FROM flows
            {where_clause}
            GROUP BY 1, 2
        ),
        bucket_totals AS (
            SELECT bucket, SUM(records) AS total_records
            FROM scoped
            GROUP BY 1
        )
        SELECT s.bucket,
               s.protocol_value,
               s.records,
               bt.total_records,
               ROUND(s.records * 100.0 / NULLIF(bt.total_records, 0), 2) AS share_pct
        FROM scoped s
        JOIN bucket_totals bt ON s.bucket = bt.bucket
        ORDER BY s.bucket, s.records DESC, s.protocol_value ASC
    """
    _, rows = rows_from_query(con, sql)
    if len(rows) < 2:
        return f"{title}\nNot enough protocol/time buckets to estimate drift."

    series: dict[str, list[float]] = {}
    for row in rows:
        series.setdefault(_safe_text(row.get("protocol_value")), []).append(_safe_float_local(row.get("share_pct")))

    selected = sorted(series.items(), key=lambda item: max(item[1]) if item[1] else 0.0, reverse=True)[: max(limit, 3)]
    selected_names = {name for name, _ in selected}

    scored_rows: list[tuple[Any, ...]] = []
    for row in rows:
        protocol_value = _safe_text(row.get("protocol_value"))
        if protocol_value not in selected_names:
            continue
        values = series.get(protocol_value, [])
        mean_value, std_value = _mean_std(values)
        share_pct = _safe_float_local(row.get("share_pct"))
        z_value = _zscore(share_pct, mean_value, std_value)
        drift_score = round(abs(z_value), 4)
        severity = "high" if drift_score >= 2.5 else "medium" if drift_score >= 1.5 else "low"
        scored_rows.append(
            (
                row.get("bucket"),
                protocol_value,
                int(_safe_float_local(row.get("records"))),
                round(share_pct, 2),
                round(mean_value, 2),
                round(z_value, 3),
                drift_score,
                severity,
            )
        )
    scored_rows.sort(key=lambda item: item[6], reverse=True)
    return render_rows_section(
        title,
        ["bucket", field_name, "records", "share_pct", "avg_share_pct", "share_z", "drift_score", "severity"],
        scored_rows[:limit],
    )


def overview_report_action(
    con: duckdb.DuckDBPyConnection,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    view: str,
) -> str:
    from core.schema_mapping import available_canonical_fields

    available = available_canonical_fields(mappings)
    sections = [f"Analysis view: {view}"]

    sections.append(
        render_section(
            con,
            "Overview",
            f"""
            WITH base AS (SELECT * FROM flows {where_clause})
            SELECT
                COUNT(*) AS records,
                MIN(analysis_time_ts) AS min_time,
                MAX(analysis_time_ts) AS max_time,
                MIN(analysis_time_relative_s) FILTER (WHERE analysis_time_kind = 'relative') AS min_relative_time_s,
                MAX(analysis_time_relative_s) FILTER (WHERE analysis_time_kind = 'relative') AS max_relative_time_s,
                COUNT(DISTINCT src_ip) AS unique_src_ip,
                COUNT(DISTINCT dst_ip) AS unique_dst_ip,
                SUM(COALESCE(bytes, 0)) AS total_bytes,
                SUM(COALESCE(packets, 0)) AS total_packets
            FROM base
            """,
        )
    )

    sections.append(
        render_section(
            con,
            "Top protocol mix",
            f"""
            SELECT COALESCE(protocol, 'UNKNOWN') AS protocol,
                   COUNT(*) AS records,
                   SUM(COALESCE(bytes, 0)) AS total_bytes
            FROM flows
            {where_clause}
            GROUP BY 1
            ORDER BY records DESC, total_bytes DESC, protocol ASC
            LIMIT 10
            """,
        )
    )

    if "app_protocol" in available:
        sections.append(
            render_section(
                con,
                "Top application protocol mix",
                f"""
                SELECT COALESCE(app_protocol, 'UNKNOWN') AS app_protocol,
                       COUNT(*) AS records,
                       SUM(COALESCE(bytes, 0)) AS total_bytes
                FROM flows
                {where_clause}
                GROUP BY 1
                ORDER BY records DESC, total_bytes DESC, app_protocol ASC
                LIMIT 10
                """,
            )
        )

    if "src_ip" in available:
        sections.append(
            render_section(
                con,
                "Top source IPs by bytes",
                f"""
                SELECT src_ip, COUNT(*) AS records, SUM(COALESCE(bytes, 0)) AS total_bytes
                FROM flows
                {where_clause}
                WHERE src_ip IS NOT NULL
                GROUP BY 1
                ORDER BY total_bytes DESC, records DESC, src_ip ASC
                LIMIT 10
                """,
            )
        )

    if "dst_ip" in available:
        sections.append(
            render_section(
                con,
                "Top destination IPs by bytes",
                f"""
                SELECT dst_ip, COUNT(*) AS records, SUM(COALESCE(bytes, 0)) AS total_bytes
                FROM flows
                {where_clause}
                WHERE dst_ip IS NOT NULL
                GROUP BY 1
                ORDER BY total_bytes DESC, records DESC, dst_ip ASC
                LIMIT 10
                """,
            )
        )

    if "dst_port" in available:
        sections.append(
            render_section(
                con,
                "Top destination ports",
                f"""
                SELECT dst_port, COUNT(*) AS records, SUM(COALESCE(bytes, 0)) AS total_bytes
                FROM flows
                {where_clause}
                WHERE dst_port IS NOT NULL
                GROUP BY 1
                ORDER BY records DESC, total_bytes DESC, CAST(dst_port AS VARCHAR) ASC
                LIMIT 10
                """,
            )
        )

    return "\n\n".join(sections)
