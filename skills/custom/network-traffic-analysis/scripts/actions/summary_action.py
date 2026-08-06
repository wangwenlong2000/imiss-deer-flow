"""
Summary Action

Provides a high-level overview of the dataset with key metrics,
protocol distribution, and notable observations.
"""

from __future__ import annotations

from typing import Any

from actions.advanced_action_common import fetch_rows, scoped_where
from utils.sql import quote_identifier


def execute_summary(
    con: Any,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    files: list[str],
    **kwargs,
) -> dict:
    """Execute summary queries and return structured results."""
    limit = kwargs.get("limit", 10)

    summary_sql = f"""
        WITH base AS (SELECT * FROM flows {scoped_where(where_clause, "1=1")})
        SELECT
            COUNT(*) AS records,
            MIN(analysis_time_ts) AS min_time,
            MAX(analysis_time_ts) AS max_time,
            MIN(analysis_time_relative_s) FILTER (WHERE analysis_time_kind = 'relative') AS min_relative_time_s,
            MAX(analysis_time_relative_s) FILTER (WHERE analysis_time_kind = 'relative') AS max_relative_time_s,
            COUNT(DISTINCT src_ip) AS unique_src_ip,
            COUNT(DISTINCT dst_ip) AS unique_dst_ip,
            SUM(COALESCE(bytes, 0)) AS total_bytes,
            SUM(COALESCE(packets, 0)) AS total_packets,
            AVG(COALESCE(flow_duration, 0)) AS avg_flow_duration,
            COUNT(DISTINCT protocol) AS unique_protocols
        FROM base
    """
    summary_rows = fetch_rows(con, summary_sql)
    summary_data = summary_rows[0] if summary_rows else {}

    protocol_sql = f"""
        SELECT COALESCE(protocol, 'UNKNOWN') AS protocol, COUNT(*) AS records, SUM(COALESCE(bytes, 0)) AS total_bytes
        FROM flows
        {scoped_where(where_clause, "1=1")}
        GROUP BY 1
        ORDER BY total_bytes DESC, records DESC, protocol ASC
        LIMIT {limit}
    """
    protocol_rows = fetch_rows(con, protocol_sql)

    return {
        "summary_data": summary_data,
        "protocol_rows": protocol_rows,
    }


def format_results(results: dict) -> str:
    """Format summary results as text."""
    output = []
    output.append("# Dataset Summary\n")

    sd = results.get("summary_data", {})
    if sd:
        output.append("## Key Metrics\n")
        output.append(f"- **Records**: {sd.get('records', 0):,}")
        if sd.get("unique_src_ip"):
            output.append(f"- **Unique Source IPs**: {sd['unique_src_ip']:,}")
        if sd.get("unique_dst_ip"):
            output.append(f"- **Unique Destination IPs**: {sd['unique_dst_ip']:,}")
        if sd.get("unique_protocols"):
            output.append(f"- **Unique Protocols**: {sd['unique_protocols']}")
        if sd.get("total_bytes"):
            output.append(f"- **Total Bytes**: {sd['total_bytes']:,.0f}")
        if sd.get("total_packets"):
            output.append(f"- **Total Packets**: {sd['total_packets']:,.0f}")
        if sd.get("avg_flow_duration"):
            output.append(f"- **Avg Flow Duration**: {sd['avg_flow_duration']:.2f}s")
        if sd.get("min_time") and sd.get("max_time"):
            output.append(f"- **Time Range**: {sd['min_time']} → {sd['max_time']}")

    protocol_rows = results.get("protocol_rows", [])
    if protocol_rows:
        output.append("\n## Top Protocol Mix\n")
        from actions.advanced_action_common import format_dict_rows
        output.append(format_dict_rows(protocol_rows))

    return "\n".join(output)


def build_skill_result_parts(
    con: Any,
    results: dict,
    raw_output: str,
) -> dict[str, Any]:
    """Build structured SkillResult for summary action."""
    sd = results.get("summary_data", {})
    protocol_rows = results.get("protocol_rows", [])

    findings: list[dict[str, Any]] = []
    evidence: list[dict[str, Any]] = []

    # Key metrics as metric evidence
    metrics = []
    if sd.get("records") is not None:
        metrics.append({"name": "total_records", "value": int(sd["records"])})
    if sd.get("unique_src_ip") is not None:
        metrics.append({"name": "unique_src_ips", "value": int(sd["unique_src_ip"])})
    if sd.get("unique_dst_ip") is not None:
        metrics.append({"name": "unique_dst_ips", "value": int(sd["unique_dst_ip"])})
    if sd.get("unique_protocols") is not None:
        metrics.append({"name": "unique_protocols", "value": int(sd["unique_protocols"])})
    if sd.get("total_bytes") is not None:
        metrics.append({"name": "total_bytes", "value": int(sd["total_bytes"])})
    if sd.get("total_packets") is not None:
        metrics.append({"name": "total_packets", "value": int(sd["total_packets"])})
    if sd.get("avg_flow_duration") is not None:
        metrics.append({"name": "avg_flow_duration_seconds", "value": round(float(sd["avg_flow_duration"]), 2)})

    if metrics:
        evidence.append({
            "evidence_id": "e-summary-metrics",
            "type": "metric",
            "title": "Dataset Key Metrics",
            "metrics": metrics,
        })

    # Protocol distribution as table evidence
    if protocol_rows:
        evidence.append({
            "evidence_id": "e-protocol-distribution",
            "type": "table",
            "title": "Protocol Distribution",
            "columns": ["protocol", "records", "total_bytes"],
            "rows": [[r.get("protocol", ""), r.get("records", 0), r.get("total_bytes", 0)] for r in protocol_rows],
        })

    # Notable observations as findings
    unique_protocols = sd.get("unique_protocols", 0)
    if unique_protocols and unique_protocols > 10:
        findings.append({
            "finding_id": "f-high-protocol-diversity",
            "type": "observation",
            "severity": "info",
            "confidence": 1.0,
            "title": f"High protocol diversity: {unique_protocols} distinct protocols",
            "description": f"Dataset contains {unique_protocols} different protocol values. Consider filtering by protocol for focused analysis.",
            "entities": [{"type": "dataset", "value": "protocol_diversity"}],
            "evidence_refs": ["e-protocol-distribution"],
        })

    unique_src = sd.get("unique_src_ip", 0)
    unique_dst = sd.get("unique_dst_ip", 0)
    if unique_src and unique_dst:
        ratio = unique_dst / max(unique_src, 1)
        if ratio > 10:
            findings.append({
                "finding_id": "f-fan-out-ratio",
                "type": "observation",
                "severity": "info",
                "confidence": 1.0,
                "title": f"High fan-out ratio: {ratio:.0f}x destinations per source",
                "description": f"{unique_src:,} source IPs communicating with {unique_dst:,} destination IPs. This may indicate scanning, CDN traffic, or distributed services.",
                "entities": [
                    {"type": "src_ip_count", "value": unique_src},
                    {"type": "dst_ip_count", "value": unique_dst},
                ],
                "evidence_refs": ["e-summary-metrics"],
            })

    return {
        "summary": {
            "title": "Dataset Summary",
            "overview": f"Summary of {sd.get('records', 0):,} records across {unique_protocols} protocol(s), {unique_src:,} source IP(s), {unique_dst:,} destination IP(s).",
            "severity": "info",
            "confidence": 1.0,
            "key_metrics": metrics[:5],  # Top 5 for summary
        },
        "findings": findings,
        "evidence": evidence,
        "artifacts": [],
        "diagnostics": {
            "warnings": [],
            "data_quality": {},
        },
    }
