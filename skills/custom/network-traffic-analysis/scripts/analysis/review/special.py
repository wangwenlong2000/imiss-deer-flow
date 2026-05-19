from __future__ import annotations

from typing import Any

from analysis.anomaly_models import score_generic_candidates
from analysis.feature_engineering import rows_from_query
from core.schema_mapping import available_canonical_fields, ensure_required
from utils.formatter import render_rows_section
from utils.math import _safe_float_local
from utils.zeek import _private_ip_predicate

def data_exfiltration_review_action(
    con: Any,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    limit: int,
) -> str:
    available = available_canonical_fields(mappings)
    ensure_required(mappings, ["src_ip", "dst_ip", "bytes", "packets"])

    duration_expr = "COALESCE(duration_ms, flow_duration, 0)" if "duration_ms" in available else "COALESCE(flow_duration, 0)" if "flow_duration" in available else "0"
    outbound_ratio_expr = (
        "AVG(COALESCE(src_to_dst_byte_ratio, 0)) AS avg_src_to_dst_byte_ratio,"
        if "src_to_dst_byte_ratio" in available
        else "CAST(NULL AS DOUBLE) AS avg_src_to_dst_byte_ratio,"
    )
    packet_ratio_expr = (
        "AVG(COALESCE(src_to_dst_packet_ratio, 0)) AS avg_src_to_dst_packet_ratio,"
        if "src_to_dst_packet_ratio" in available
        else "CAST(NULL AS DOUBLE) AS avg_src_to_dst_packet_ratio,"
    )
    byte_asymmetry_expr = (
        "AVG(COALESCE(byte_asymmetry, 0)) AS avg_byte_asymmetry,"
        if "byte_asymmetry" in available
        else "CAST(NULL AS DOUBLE) AS avg_byte_asymmetry,"
    )
    packet_asymmetry_expr = (
        "AVG(COALESCE(packet_asymmetry, 0)) AS avg_packet_asymmetry,"
        if "packet_asymmetry" in available
        else "CAST(NULL AS DOUBLE) AS avg_packet_asymmetry,"
    )
    action_negative_expr = (
        "CASE WHEN LOWER(COALESCE(action, '')) IN ('deny', 'drop', 'block', 'reset', 'reject') THEN 1 ELSE 0 END"
        if "action" in available
        else "0"
    )
    dst_country_cardinality = "COUNT(DISTINCT dst_country) AS unique_dst_country," if "dst_country" in available else "CAST(NULL AS DOUBLE) AS unique_dst_country,"
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
            ROUND(SUM(CASE WHEN COALESCE(bytes, 0) >= 1048576 THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0), 4) AS large_flow_ratio,
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
    if not rows:
        return "Data exfiltration review\nNo flow-level exfiltration candidates were found in the selected scope."

    candidates = [
        {
            "src_ip": row.get("src_ip"),
            "flow_count": int(_safe_float_local(row.get("flow_count"))),
            "total_bytes": round(_safe_float_local(row.get("total_bytes")), 2),
            "avg_bytes": round(_safe_float_local(row.get("avg_bytes")), 2),
            "max_bytes": round(_safe_float_local(row.get("max_bytes")), 2),
            "avg_packets": round(_safe_float_local(row.get("avg_packets")), 2),
            "unique_dst_ip": int(_safe_float_local(row.get("unique_dst_ip"))),
            "unique_dst_port": int(_safe_float_local(row.get("unique_dst_port"))),
            "unique_dst_country": int(_safe_float_local(row.get("unique_dst_country"))),
            "avg_duration": round(_safe_float_local(row.get("avg_duration")), 2),
            "max_duration": round(_safe_float_local(row.get("max_duration")), 2),
            "avg_src_to_dst_byte_ratio": round(_safe_float_local(row.get("avg_src_to_dst_byte_ratio")), 4),
            "avg_src_to_dst_packet_ratio": round(_safe_float_local(row.get("avg_src_to_dst_packet_ratio")), 4),
            "avg_byte_asymmetry": round(_safe_float_local(row.get("avg_byte_asymmetry")), 4),
            "avg_packet_asymmetry": round(_safe_float_local(row.get("avg_packet_asymmetry")), 4),
            "large_flow_ratio": round(_safe_float_local(row.get("large_flow_ratio")), 4),
            "long_flow_ratio": round(_safe_float_local(row.get("long_flow_ratio")), 4),
            "negative_action_ratio": round(_safe_float_local(row.get("negative_action_ratio")), 4),
        }
        for row in rows
    ]

    def exfil_rule_score(row: dict[str, Any]) -> float:
        score = 0.0
        if _safe_float_local(row.get("avg_src_to_dst_byte_ratio")) >= 3.0:
            score += 0.18
        if _safe_float_local(row.get("avg_byte_asymmetry")) >= 0.75:
            score += 0.18
        if _safe_float_local(row.get("large_flow_ratio")) >= 0.2:
            score += 0.15
        if _safe_float_local(row.get("long_flow_ratio")) >= 0.2:
            score += 0.12
        if _safe_float_local(row.get("total_bytes")) >= 5_000_000:
            score += 0.12
        if _safe_float_local(row.get("unique_dst_ip")) >= 5:
            score += 0.1
        if _safe_float_local(row.get("unique_dst_country")) >= 2:
            score += 0.08
        if _safe_float_local(row.get("negative_action_ratio")) >= 0.35:
            score += 0.07
        return min(1.0, score)

    def exfil_reason(row: dict[str, Any], final_score: float, rule_score: float) -> str:
        reasons: list[str] = []
        if _safe_float_local(row.get("avg_src_to_dst_byte_ratio")) >= 3.0 or _safe_float_local(row.get("avg_byte_asymmetry")) >= 0.75:
            reasons.append("outbound_byte_asymmetry")
        if _safe_float_local(row.get("large_flow_ratio")) >= 0.2:
            reasons.append("large_transfer_pattern")
        if _safe_float_local(row.get("long_flow_ratio")) >= 0.2:
            reasons.append("long_lived_transfer_pattern")
        if _safe_float_local(row.get("unique_dst_ip")) >= 5:
            reasons.append("multi_destination_distribution")
        if _safe_float_local(row.get("unique_dst_country")) >= 2:
            reasons.append("cross_country_egress_pattern")
        if _safe_float_local(row.get("negative_action_ratio")) >= 0.35:
            reasons.append("partially_blocked_egress_activity")
        if not reasons and final_score >= 0.65:
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

    return "\n\n".join(
        [
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
            ),
            "This review emphasizes directional byte asymmetry, large-transfer concentration, long-lived flows, and destination spread. It is strongest on datasets that include directional byte and packet features from preprocessing.",
        ]
    )


def lateral_movement_review_action(
    con: Any,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    limit: int,
) -> str:
    available = available_canonical_fields(mappings)
    ensure_required(mappings, ["src_ip", "dst_ip", "dst_port", "bytes", "packets"])

    private_src_predicate = _private_ip_predicate("src_ip")
    private_dst_predicate = _private_ip_predicate("dst_ip")
    internal_scope_predicate = f"{private_src_predicate} AND {private_dst_predicate}"
    duration_expr = "COALESCE(duration_ms, flow_duration, 0)" if "duration_ms" in available else "COALESCE(flow_duration, 0)" if "flow_duration" in available else "0"
    short_like_expr = f"CASE WHEN COALESCE(bytes, 0) <= 400 AND COALESCE(packets, 0) <= 3 AND {duration_expr} <= 30 THEN 1 ELSE 0 END"
    action_negative_expr = (
        "CASE WHEN LOWER(COALESCE(action, '')) IN ('deny', 'drop', 'block', 'reset', 'reject') THEN 1 ELSE 0 END"
        if "action" in available
        else "0"
    )
    risky_state_expr = (
        "CASE WHEN LOWER(COALESCE(session_state, '')) IN ('syn_sent', 'syn_only', 'reset', 'rst', 'failed', 'reject', 'timeout') THEN 1 ELSE 0 END"
        if "session_state" in available
        else "0"
    )
    service_expr = "LOWER(COALESCE(service, ''))" if "service" in available else "''"
    credential_port_expr = "CASE WHEN COALESCE(dst_port, 0) IN (22, 88, 135, 139, 389, 445, 464, 636, 1433, 1521, 3306, 3389, 5432, 5985, 5986) THEN 1 ELSE 0 END"
    admin_service_expr = f"CASE WHEN {service_expr} IN ('ssh', 'rdp', 'smb', 'dce_rpc', 'kerberos', 'ldap', 'ldaps', 'winrm', 'mssql', 'mysql', 'postgresql', 'rpc') THEN 1 ELSE 0 END"

    sql = f"""
        SELECT
            src_ip,
            COUNT(*) AS flow_count,
            COUNT(DISTINCT dst_ip) AS unique_internal_dst_ip,
            COUNT(DISTINCT dst_port) AS unique_internal_dst_port,
            SUM(CASE WHEN {credential_port_expr} = 1 THEN 1 ELSE 0 END) AS credential_port_flows,
            COUNT(DISTINCT CASE WHEN {credential_port_expr} = 1 THEN dst_port ELSE NULL END) AS credential_port_count,
            ROUND(SUM(CASE WHEN {credential_port_expr} = 1 THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0), 4) AS credential_port_ratio,
            ROUND(SUM(CASE WHEN {admin_service_expr} = 1 THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0), 4) AS admin_service_ratio,
            ROUND(SUM({action_negative_expr}) * 1.0 / NULLIF(COUNT(*), 0), 4) AS negative_action_ratio,
            ROUND(SUM({risky_state_expr}) * 1.0 / NULLIF(COUNT(*), 0), 4) AS risky_state_ratio,
            ROUND(SUM({short_like_expr}) * 1.0 / NULLIF(COUNT(*), 0), 4) AS short_like_ratio,
            ROUND(AVG(COALESCE(bytes, 0)), 2) AS avg_bytes,
            ROUND(MAX(COALESCE(bytes, 0)), 2) AS max_bytes,
            ROUND(AVG(COALESCE(packets, 0)), 2) AS avg_packets,
            ROUND(AVG({duration_expr}), 2) AS avg_duration,
            MAX({duration_expr}) AS max_duration
        FROM flows
        {where_clause}
        {"AND" if where_clause else "WHERE"} src_ip IS NOT NULL
          AND dst_ip IS NOT NULL
          AND {internal_scope_predicate}
        GROUP BY 1
        HAVING COUNT(*) >= 2
        ORDER BY unique_internal_dst_ip DESC, credential_port_flows DESC, risky_state_ratio DESC, src_ip ASC
        LIMIT {max(limit * 50, 1000)}
    """
    _, rows = rows_from_query(con, sql)
    if not rows:
        return (
            "Lateral movement review\n"
            "No internal east-west communication candidates were found in the selected scope. "
            "This review needs private-address source and destination activity to rank lateral movement hotspots."
        )

    candidates = [
        {
            "src_ip": row.get("src_ip"),
            "flow_count": int(_safe_float_local(row.get("flow_count"))),
            "unique_internal_dst_ip": int(_safe_float_local(row.get("unique_internal_dst_ip"))),
            "unique_internal_dst_port": int(_safe_float_local(row.get("unique_internal_dst_port"))),
            "credential_port_flows": int(_safe_float_local(row.get("credential_port_flows"))),
            "credential_port_count": int(_safe_float_local(row.get("credential_port_count"))),
            "credential_port_ratio": round(_safe_float_local(row.get("credential_port_ratio")), 4),
            "admin_service_ratio": round(_safe_float_local(row.get("admin_service_ratio")), 4),
            "negative_action_ratio": round(_safe_float_local(row.get("negative_action_ratio")), 4),
            "risky_state_ratio": round(_safe_float_local(row.get("risky_state_ratio")), 4),
            "short_like_ratio": round(_safe_float_local(row.get("short_like_ratio")), 4),
            "avg_bytes": round(_safe_float_local(row.get("avg_bytes")), 2),
            "max_bytes": round(_safe_float_local(row.get("max_bytes")), 2),
            "avg_packets": round(_safe_float_local(row.get("avg_packets")), 2),
            "avg_duration": round(_safe_float_local(row.get("avg_duration")), 2),
            "max_duration": round(_safe_float_local(row.get("max_duration")), 2),
        }
        for row in rows
    ]

    def lateral_rule_score(row: dict[str, Any]) -> float:
        score = 0.0
        if _safe_float_local(row.get("unique_internal_dst_ip")) >= 6:
            score += 0.22
        elif _safe_float_local(row.get("unique_internal_dst_ip")) >= 3:
            score += 0.12
        if _safe_float_local(row.get("credential_port_ratio")) >= 0.35 or _safe_float_local(row.get("credential_port_count")) >= 2:
            score += 0.2
        if _safe_float_local(row.get("admin_service_ratio")) >= 0.2:
            score += 0.12
        if _safe_float_local(row.get("risky_state_ratio")) >= 0.25:
            score += 0.12
        if _safe_float_local(row.get("negative_action_ratio")) >= 0.25:
            score += 0.1
        if _safe_float_local(row.get("short_like_ratio")) >= 0.35 and _safe_float_local(row.get("flow_count")) >= 8:
            score += 0.1
        if _safe_float_local(row.get("unique_internal_dst_port")) >= 6:
            score += 0.08
        if _safe_float_local(row.get("avg_duration")) <= 60 and _safe_float_local(row.get("unique_internal_dst_ip")) >= 4:
            score += 0.06
        return min(1.0, score)

    def lateral_reason(row: dict[str, Any], final_score: float, rule_score: float) -> str:
        reasons: list[str] = []
        if _safe_float_local(row.get("unique_internal_dst_ip")) >= 4:
            reasons.append("internal_multi_host_fanout")
        if _safe_float_local(row.get("credential_port_ratio")) >= 0.35 or _safe_float_local(row.get("credential_port_count")) >= 2:
            reasons.append("credential_service_spread")
        if _safe_float_local(row.get("admin_service_ratio")) >= 0.2:
            reasons.append("admin_protocol_concentration")
        if _safe_float_local(row.get("risky_state_ratio")) >= 0.25:
            reasons.append("risky_internal_session_states")
        if _safe_float_local(row.get("negative_action_ratio")) >= 0.25:
            reasons.append("blocked_internal_access_pattern")
        if _safe_float_local(row.get("short_like_ratio")) >= 0.35:
            reasons.append("short_internal_probe_pattern")
        if not reasons and final_score >= 0.65:
            reasons.append("model_ranked_lateral_candidate")
        if not reasons and rule_score >= 0.35:
            reasons.append("rule_ranked_lateral_candidate")
        return ",".join(reasons) if reasons else "mixed_low_signal_internal_activity"

    scored = score_generic_candidates(
        candidates,
        numeric_fields=[
            "flow_count",
            "unique_internal_dst_ip",
            "unique_internal_dst_port",
            "credential_port_flows",
            "credential_port_count",
            "credential_port_ratio",
            "admin_service_ratio",
            "negative_action_ratio",
            "risky_state_ratio",
            "short_like_ratio",
            "avg_bytes",
            "max_bytes",
            "avg_packets",
            "avg_duration",
            "max_duration",
        ],
        categorical_fields=[],
        rule_score_fn=lateral_rule_score,
        reason_fn=lateral_reason,
        output_field="lateral_movement_risk_score",
        contamination=0.12,
        engine="hybrid",
    )

    return "\n\n".join(
        [
            render_rows_section(
                "Lateral movement hotspots",
                [
                    "src_ip",
                    "lateral_movement_risk_score",
                    "severity",
                    "unique_internal_dst_ip",
                    "credential_port_ratio",
                    "admin_service_ratio",
                    "risky_state_ratio",
                    "short_like_ratio",
                    "likely_reason",
                ],
                [
                    (
                        row.get("src_ip"),
                        row.get("lateral_movement_risk_score"),
                        row.get("severity"),
                        row.get("unique_internal_dst_ip"),
                        row.get("credential_port_ratio"),
                        row.get("admin_service_ratio"),
                        row.get("risky_state_ratio"),
                        row.get("short_like_ratio"),
                        row.get("likely_reason"),
                    )
                    for row in scored[:limit]
                ],
            ),
            "This review emphasizes private-address east-west spread, credential-service coverage, risky internal session states, and short internal probe patterns. It is strongest on datasets with meaningful internal IP visibility and session/action fields.",
        ]
    )
