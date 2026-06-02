from __future__ import annotations

from typing import Any

from analysis.anomaly_models import score_generic_candidates
from analysis.feature_engineering import rows_from_query
from core.schema_mapping import available_canonical_fields, ensure_required
from utils.formatter import render_rows_section
from utils.math import _safe_float_local


# Scoring policy constants
HIGH_RISK_THRESHOLD = 0.65
BYTE_ASYMMETRY_THRESHOLD = 0.75
LARGE_FLOW_BYTES = 1_048_576
LARGE_FLOW_RATIO_THRESHOLD = 0.2
LONG_FLOW_RATIO_THRESHOLD = 0.2
BYTE_RATIO_THRESHOLD = 3.0
BYTE_RATIO_SCORE = 0.18
BYTE_ASYMMETRY_SCORE = 0.18
LARGE_FLOW_SCORE = 0.15
LONG_FLOW_SCORE = 0.12
TOTAL_BYTES_THRESHOLD = 5_000_000
UNIQUE_DST_SCORE = 0.1
CROSS_COUNTRY_SCORE = 0.08
NEGATIVE_ACTION_THRESHOLD = 0.35


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def execute_data_exfiltration_review(
    con: Any,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    limit: int,
) -> dict[str, Any]:
    """Execute data exfiltration review and return structured data dict."""
    available = available_canonical_fields(mappings)
    ensure_required(mappings, ["src_ip", "dst_ip", "bytes", "packets"])

    # Detect directional field availability explicitly
    has_src_to_dst_byte_ratio = "src_to_dst_byte_ratio" in available
    has_byte_asymmetry = "byte_asymmetry" in available
    has_src_to_dst_packet_ratio = "src_to_dst_packet_ratio" in available
    has_packet_asymmetry = "packet_asymmetry" in available

    outbound_ratio_expr = (
        "AVG(COALESCE(src_to_dst_byte_ratio, 0)) AS avg_src_to_dst_byte_ratio,"
        if has_src_to_dst_byte_ratio
        else "CAST(NULL AS DOUBLE) AS avg_src_to_dst_byte_ratio,"
    )
    packet_ratio_expr = (
        "AVG(COALESCE(src_to_dst_packet_ratio, 0)) AS avg_src_to_dst_packet_ratio,"
        if has_src_to_dst_packet_ratio
        else "CAST(NULL AS DOUBLE) AS avg_src_to_dst_packet_ratio,"
    )
    byte_asymmetry_expr = (
        "AVG(COALESCE(byte_asymmetry, 0)) AS avg_byte_asymmetry,"
        if has_byte_asymmetry
        else "CAST(NULL AS DOUBLE) AS avg_byte_asymmetry,"
    )
    packet_asymmetry_expr = (
        "AVG(COALESCE(packet_asymmetry, 0)) AS avg_packet_asymmetry,"
        if has_packet_asymmetry
        else "CAST(NULL AS DOUBLE) AS avg_packet_asymmetry,"
    )
    action_negative_expr = (
        "CASE WHEN LOWER(COALESCE(action, '')) IN ('deny', 'drop', 'block', 'reset', 'reject') THEN 1 ELSE 0 END"
        if "action" in available
        else "0"
    )
    duration_expr = "COALESCE(duration_ms, flow_duration, 0)" if "duration_ms" in available else "COALESCE(flow_duration, 0)" if "flow_duration" in available else "0"
    dst_country_cardinality = "COUNT(DISTINCT dst_country) AS unique_dst_country," if "dst_country" in available else "CAST(NULL AS DOUBLE) AS unique_dst_country,"

    directional_fields = [f for f, v in [
        ("src_to_dst_byte_ratio", has_src_to_dst_byte_ratio),
        ("byte_asymmetry", has_byte_asymmetry),
        ("src_to_dst_packet_ratio", has_src_to_dst_packet_ratio),
        ("packet_asymmetry", has_packet_asymmetry),
    ] if v]
    missing_directional = [f for f, v in [
        ("src_to_dst_byte_ratio", has_src_to_dst_byte_ratio),
        ("byte_asymmetry", has_byte_asymmetry),
        ("src_to_dst_packet_ratio", has_src_to_dst_packet_ratio),
        ("packet_asymmetry", has_packet_asymmetry),
    ] if not v]

    sql = f"""
        SELECT
            src_ip,
            COUNT(*) AS flow_count,
            SUM(COALESCE(bytes, 0)) AS total_bytes,
            AVG(COALESCE(bytes, 0)) AS avg_bytes,
            MAX(COALESCE(bytes, 0)) AS max_bytes,
            AVG(COALESCE(packets, 0)) AS avg_packets,
            COUNT(DISTINCT dst_ip) AS unique_dst_ip,
            COUNT(DISTINCT dst_port) AS unique_dst_port,
            {dst_country_cardinality}
            ROUND(AVG({duration_expr}), 2) AS avg_duration,
            MAX({duration_expr}) AS max_duration,
            {outbound_ratio_expr}
            {packet_ratio_expr}
            {byte_asymmetry_expr}
            {packet_asymmetry_expr}
            ROUND(SUM(CASE WHEN COALESCE(bytes, 0) >= {LARGE_FLOW_BYTES} THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0), 4) AS large_flow_ratio,
            ROUND(SUM(CASE WHEN {duration_expr} >= 600 THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0), 4) AS long_flow_ratio,
            ROUND(SUM({action_negative_expr}) * 1.0 / NULLIF(COUNT(*), 0), 4) AS negative_action_ratio
        FROM flows
        {where_clause}
        {"AND" if where_clause else "WHERE"} src_ip IS NOT NULL
        GROUP BY 1
        HAVING COUNT(*) >= 2
        ORDER BY total_bytes DESC, avg_bytes DESC, unique_dst_ip DESC, src_ip ASC
        LIMIT {max(limit * 50, 1000)}
    """
    _, rows = rows_from_query(con, sql)

    result: dict[str, Any] = {
        "candidates": [],
        "scored": [],
        "available_fields": list(available),
        "directional_fields_available": directional_fields,
        "missing_directional_fields": missing_directional,
        "_limit": limit,
    }

    if not rows:
        return result

    candidates = [
        {
            "src_ip": row.get("src_ip"),
            "flow_count": int(_safe_float(row.get("flow_count"))),
            "total_bytes": round(_safe_float(row.get("total_bytes")), 2),
            "avg_bytes": round(_safe_float(row.get("avg_bytes")), 2),
            "max_bytes": round(_safe_float(row.get("max_bytes")), 2),
            "avg_packets": round(_safe_float(row.get("avg_packets")), 2),
            "unique_dst_ip": int(_safe_float(row.get("unique_dst_ip"))),
            "unique_dst_port": int(_safe_float(row.get("unique_dst_port"))),
            "unique_dst_country": int(_safe_float(row.get("unique_dst_country"))),
            "avg_duration": round(_safe_float(row.get("avg_duration")), 2),
            "max_duration": round(_safe_float(row.get("max_duration")), 2),
            "avg_src_to_dst_byte_ratio": round(_safe_float(row.get("avg_src_to_dst_byte_ratio")), 4),
            "avg_src_to_dst_packet_ratio": round(_safe_float(row.get("avg_src_to_dst_packet_ratio")), 4),
            "avg_byte_asymmetry": round(_safe_float(row.get("avg_byte_asymmetry")), 4),
            "avg_packet_asymmetry": round(_safe_float(row.get("avg_packet_asymmetry")), 4),
            "large_flow_ratio": round(_safe_float(row.get("large_flow_ratio")), 4),
            "long_flow_ratio": round(_safe_float(row.get("long_flow_ratio")), 4),
            "negative_action_ratio": round(_safe_float(row.get("negative_action_ratio")), 4),
        }
        for row in rows
    ]
    result["candidates"] = candidates

    # Only apply directional scoring when fields are actually available
    def exfil_rule_score(row: dict[str, Any]) -> float:
        score = 0.0
        if has_src_to_dst_byte_ratio and _safe_float(row.get("avg_src_to_dst_byte_ratio")) >= BYTE_RATIO_THRESHOLD:
            score += BYTE_RATIO_SCORE
        if has_byte_asymmetry and _safe_float(row.get("avg_byte_asymmetry")) >= BYTE_ASYMMETRY_THRESHOLD:
            score += BYTE_ASYMMETRY_SCORE
        if _safe_float(row.get("large_flow_ratio")) >= LARGE_FLOW_RATIO_THRESHOLD:
            score += LARGE_FLOW_SCORE
        if _safe_float(row.get("long_flow_ratio")) >= LONG_FLOW_RATIO_THRESHOLD:
            score += LONG_FLOW_SCORE
        if _safe_float(row.get("total_bytes")) >= TOTAL_BYTES_THRESHOLD:
            score += UNIQUE_DST_SCORE  # reuse same score weight as unique_dst
        if _safe_float(row.get("unique_dst_ip")) >= 5:
            score += UNIQUE_DST_SCORE
        if _safe_float(row.get("unique_dst_country")) >= 2:
            score += CROSS_COUNTRY_SCORE
        if _safe_float(row.get("negative_action_ratio")) >= NEGATIVE_ACTION_THRESHOLD:
            score += 0.07
        return min(1.0, score)

    def exfil_reason(row: dict[str, Any], final_score: float, rule_score: float) -> str:
        reasons: list[str] = []
        if has_src_to_dst_byte_ratio and _safe_float(row.get("avg_src_to_dst_byte_ratio")) >= BYTE_RATIO_THRESHOLD:
            reasons.append("outbound_byte_ratio")
        if has_byte_asymmetry and _safe_float(row.get("avg_byte_asymmetry")) >= BYTE_ASYMMETRY_THRESHOLD:
            reasons.append("outbound_byte_asymmetry")
        if _safe_float(row.get("large_flow_ratio")) >= LARGE_FLOW_RATIO_THRESHOLD:
            reasons.append("large_transfer_pattern")
        if _safe_float(row.get("long_flow_ratio")) >= LONG_FLOW_RATIO_THRESHOLD:
            reasons.append("long_lived_transfer_pattern")
        if _safe_float(row.get("unique_dst_ip")) >= 5:
            reasons.append("multi_destination_distribution")
        if _safe_float(row.get("unique_dst_country")) >= 2:
            reasons.append("cross_country_egress_pattern")
        if _safe_float(row.get("negative_action_ratio")) >= NEGATIVE_ACTION_THRESHOLD:
            reasons.append("partially_blocked_egress_activity")
        if not reasons and final_score >= HIGH_RISK_THRESHOLD:
            reasons.append("model_ranked_exfiltration_candidate")
        if not reasons and rule_score >= 0.35:
            reasons.append("rule_ranked_exfiltration_candidate")
        return ",".join(reasons) if reasons else "mixed_low_signal_egress_activity"

    scored = score_generic_candidates(
        candidates,
        numeric_fields=[
            "flow_count",
            "total_bytes",
            "avg_bytes",
            "max_bytes",
            "avg_packets",
            "unique_dst_ip",
            "unique_dst_port",
            "unique_dst_country",
            "avg_duration",
            "max_duration",
            "avg_src_to_dst_byte_ratio",
            "avg_src_to_dst_packet_ratio",
            "avg_byte_asymmetry",
            "avg_packet_asymmetry",
            "large_flow_ratio",
            "long_flow_ratio",
            "negative_action_ratio",
        ],
        categorical_fields=[],
        rule_score_fn=exfil_rule_score,
        reason_fn=exfil_reason,
        output_field="exfiltration_risk_score",
        contamination=0.12,
        engine="hybrid",
    )
    result["scored"] = scored
    return result


def format_data_exfiltration_review(data: dict[str, Any]) -> str:
    """Produce the text report for backward-compatible output."""
    sections: list[str] = []
    scored = data["scored"]
    limit = data.get("_limit", 10)

    if not scored and not data["candidates"]:
        return "Data exfiltration review\nNo flow-level exfiltration candidates were found in the selected scope."

    sections.append(
        render_rows_section(
            "Data exfiltration hotspots",
            [
                "src_ip",
                "exfiltration_risk_score",
                "severity",
                "total_bytes",
                "avg_src_to_dst_byte_ratio",
                "avg_byte_asymmetry",
                "large_flow_ratio",
                "long_flow_ratio",
                "unique_dst_ip",
                "likely_reason",
            ],
            [
                (
                    row.get("src_ip"),
                    row.get("exfiltration_risk_score"),
                    row.get("severity"),
                    row.get("total_bytes"),
                    row.get("avg_src_to_dst_byte_ratio"),
                    row.get("avg_byte_asymmetry"),
                    row.get("large_flow_ratio"),
                    row.get("long_flow_ratio"),
                    row.get("unique_dst_ip"),
                    row.get("likely_reason"),
                )
                for row in scored[:limit]
            ],
        )
    )

    sections.append(
        "This review emphasizes directional byte asymmetry, large-transfer concentration, long-lived flows, and destination spread. It is strongest on datasets that include directional byte and packet features from preprocessing."
    )

    return "\n\n".join(section for section in sections if section)


def build_skill_result_parts(data: dict[str, Any], raw_output: str) -> dict[str, Any]:
    """Build structured SkillResult for data-exfiltration-review action."""
    evidence: list[dict[str, Any]] = []
    findings: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    metrics: list[dict[str, Any]] = []
    scored = data["scored"]
    limit = data.get("_limit", 10)

    has_directional = bool(data.get("directional_fields_available", []))

    # Metrics — name depends on whether directional fields exist
    high_risk = sum(1 for r in scored if float(r.get("exfiltration_risk_score", 0.0)) >= HIGH_RISK_THRESHOLD)
    max_score = max((float(r.get("exfiltration_risk_score", 0.0)) for r in scored), default=0.0)
    total_bytes = sum(int(r.get("total_bytes", 0)) for r in scored)
    max_total_bytes = max((int(r.get("total_bytes", 0)) for r in scored), default=0)

    metrics.append({"name": "candidate_sources", "value": len(scored)})
    metrics.append({"name": "high_risk_sources", "value": high_risk})
    metrics.append({"name": "max_exfiltration_risk_score", "value": round(max_score, 4)})
    if has_directional:
        metrics.append({"name": "total_egress_bytes", "value": total_bytes})
        metrics.append({"name": "max_egress_bytes_single_source", "value": max_total_bytes})
    else:
        metrics.append({"name": "total_candidate_bytes", "value": total_bytes})
        metrics.append({"name": "max_candidate_bytes_single_source", "value": max_total_bytes})

    # Exfiltration metrics evidence
    evidence.append({
        "evidence_id": "e-exfiltration-metrics",
        "type": "metric",
        "title": "Data Exfiltration Review Metrics",
        "metrics": metrics,
    })

    # Hotspots table — rename total_bytes to avoid confusion
    if scored:
        hotspot_columns = [
            "src_ip", "exfiltration_risk_score", "severity",
            "total_candidate_bytes", "avg_src_to_dst_byte_ratio", "avg_byte_asymmetry",
            "large_flow_ratio", "long_flow_ratio", "unique_dst_ip", "likely_reason",
        ]
        hotspot_rows = [
            [
                r.get("src_ip"), r.get("exfiltration_risk_score"), r.get("severity"),
                r.get("total_bytes"), r.get("avg_src_to_dst_byte_ratio"),
                r.get("avg_byte_asymmetry"), r.get("large_flow_ratio"),
                r.get("long_flow_ratio"), r.get("unique_dst_ip"), r.get("likely_reason"),
            ]
            for r in scored[:limit]
        ]
        evidence.append({
            "evidence_id": "e-exfiltration-hotspots",
            "type": "table",
            "title": "Data Exfiltration Hotspots",
            "columns": hotspot_columns,
            "rows": hotspot_rows,
        })

    # Findings for high-risk sources
    for row in scored[:limit]:
        score = float(row.get("exfiltration_risk_score", 0.0))
        if score >= HIGH_RISK_THRESHOLD:
            severity = str(row.get("severity") or "medium").lower()
            findings.append({
                "finding_id": f"f-exfiltration-{row.get('src_ip', 'unknown')}",
                "type": "exfiltration_candidate",
                "severity": severity,
                "confidence": score,
                "title": f"Data exfiltration candidate: {row.get('src_ip')}",
                "description": row.get("likely_reason") or "Source flagged as data exfiltration candidate.",
                "entities": [{"type": "src_ip", "value": row.get("src_ip")}],
                "evidence_refs": ["e-exfiltration-hotspots", "e-exfiltration-metrics"],
            })

    # Byte asymmetry findings — only when directional fields exist
    if scored and has_directional:
        max_asym = max((float(r.get("avg_byte_asymmetry", 0)) for r in scored), default=0.0)
        max_ratio = max((float(r.get("avg_src_to_dst_byte_ratio", 0)) for r in scored), default=0.0)
        if max_asym >= BYTE_ASYMMETRY_THRESHOLD:
            top_src = max(scored, key=lambda r: float(r.get("avg_byte_asymmetry", 0)))
            findings.append({
                "finding_id": "f-exfil-byte-asymmetry",
                "type": "outbound_byte_asymmetry",
                "severity": "high",
                "confidence": 0.7,
                "title": f"High outbound byte asymmetry: {round(max_asym, 2)}",
                "description": (
                    f"Source {top_src.get('src_ip')} has avg_byte_asymmetry={round(max_asym, 2)} "
                    f"(based on byte_asymmetry). "
                    f"Disproportionate outbound-to-inbound byte ratio is an exfiltration risk indicator "
                    f"when directionality is reliable."
                ),
                "entities": [{"type": "src_ip", "value": top_src.get("src_ip")}],
                "evidence_refs": ["e-exfiltration-hotspots", "e-exfiltration-metrics"],
            })
        if max_ratio >= BYTE_RATIO_THRESHOLD:
            top_src = max(scored, key=lambda r: float(r.get("avg_src_to_dst_byte_ratio", 0)))
            findings.append({
                "finding_id": "f-exfil-byte-ratio",
                "type": "outbound_byte_ratio",
                "severity": "medium",
                "confidence": 0.65,
                "title": f"High outbound-to-inbound byte ratio: {round(max_ratio, 1)}:1",
                "description": (
                    f"Source {top_src.get('src_ip')} has avg_src_to_dst_byte_ratio={round(max_ratio, 1)} "
                    f"(based on src_to_dst_byte_ratio). "
                    f"The source is sending significantly more data than receiving, "
                    f"an exfiltration risk indicator when directionality is reliable."
                ),
                "entities": [{"type": "src_ip", "value": top_src.get("src_ip")}],
                "evidence_refs": ["e-exfiltration-hotspots", "e-exfiltration-metrics"],
            })

    # Large transfer finding
    if scored:
        max_large = max((float(r.get("large_flow_ratio", 0)) for r in scored), default=0.0)
        if max_large >= LARGE_FLOW_RATIO_THRESHOLD:
            top_src = max(scored, key=lambda r: float(r.get("large_flow_ratio", 0)))
            findings.append({
                "finding_id": "f-exfil-large-transfers",
                "type": "large_transfer_pattern",
                "severity": "medium",
                "confidence": 0.6,
                "title": f"Large transfer concentration: {round(max_large * 100, 1)}%",
                "description": (
                    f"Source {top_src.get('src_ip')} has large_flow_ratio={round(max_large, 2)}. "
                    f"A significant fraction of flows exceed 1MB, suggesting bulk data transfer."
                ),
                "entities": [{"type": "src_ip", "value": top_src.get("src_ip")}],
                "evidence_refs": ["e-exfiltration-hotspots", "e-exfiltration-metrics"],
            })

    # Warnings for missing directional fields — from result, not candidate dict
    missing_directional = data.get("missing_directional_fields", [])
    if missing_directional:
        warnings.append({
            "code": "missing_directional_fields",
            "message": (
                f"Directional byte fields are unavailable: {', '.join(missing_directional)}. "
                f"Outbound byte asymmetry and byte ratio indicators are not computed. "
                f"Consider re-running preprocessing with directional byte features."
            ),
            "severity": "warning",
        })

    # Scoring policy diagnostics
    scoring_policy = {
        "high_risk_threshold": HIGH_RISK_THRESHOLD,
        "byte_asymmetry_threshold": BYTE_ASYMMETRY_THRESHOLD,
        "large_flow_bytes": LARGE_FLOW_BYTES,
        "large_flow_ratio_threshold": LARGE_FLOW_RATIO_THRESHOLD,
        "byte_ratio_threshold": BYTE_RATIO_THRESHOLD,
        "engine": "hybrid",
    }

    # Raw report
    evidence.append({
        "evidence_id": "e-raw-report",
        "type": "text",
        "title": "Raw Data Exfiltration Review Output",
        "content": raw_output,
    })

    # Fix evidence_refs
    existing_ids = {e["evidence_id"] for e in evidence}
    for finding in findings:
        finding["evidence_refs"] = [ref for ref in finding["evidence_refs"] if ref in existing_ids]

    # Compute overall severity
    severity_order = {"critical": 4, "high": 3, "medium": 2, "low": 1, "info": 0}
    highest = "info"
    for f in findings:
        if severity_order.get(f.get("severity", "info"), 0) > severity_order.get(highest, 0):
            highest = f["severity"]

    overview_text = "Data exfiltration review."
    if findings:
        overview_text += f" {len(findings)} finding(s) identified."

    return {
        "summary": {
            "title": "Data Exfiltration Review",
            "overview": overview_text,
            "severity": highest,
            "confidence": round(max_score, 4) if max_score > 0 else None,
            "key_metrics": metrics,
        },
        "findings": findings,
        "evidence": evidence,
        "artifacts": [],
        "diagnostics": {
            "warnings": warnings,
            "data_quality": {
                "candidate_sources": len(scored),
                "high_risk_sources": high_risk,
                "directional_fields_available": data.get("directional_fields_available", []),
                "missing_directional_fields": missing_directional,
            },
            "scoring_policy": scoring_policy,
        },
    }


# Backward-compatible entry point
def data_exfiltration_review_action(
    con: Any,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    limit: int,
) -> str:
    """Legacy entry point — delegates to execute + format for backward compatibility."""
    data = execute_data_exfiltration_review(con, mappings, where_clause, limit)
    return format_data_exfiltration_review(data)
