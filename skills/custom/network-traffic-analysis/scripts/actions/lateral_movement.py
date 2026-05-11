from __future__ import annotations

from typing import Any

from analysis.anomaly_models import score_generic_candidates
from analysis.feature_engineering import rows_from_query
from core.schema_mapping import available_canonical_fields, ensure_required
from utils.formatter import render_rows_section
from utils.math import _safe_float_local
from utils.zeek import _private_ip_predicate


# Scoring thresholds and constants
LATERAL_HIGH_FANOUT_THRESHOLD = 6
LATERAL_MED_FANOUT_THRESHOLD = 3
LATERAL_HIGH_FANOUT_SCORE = 0.22
LATERAL_MED_FANOUT_SCORE = 0.12

LATERAL_CREDENTIAL_PORT_RATIO_THRESHOLD = 0.35
LATERAL_CREDENTIAL_PORT_COUNT_THRESHOLD = 2
LATERAL_CREDENTIAL_PORT_SCORE = 0.2

LATERAL_ADMIN_SERVICE_RATIO_THRESHOLD = 0.2
LATERAL_ADMIN_SERVICE_SCORE = 0.12

LATERAL_RISKY_STATE_RATIO_THRESHOLD = 0.25
LATERAL_RISKY_STATE_SCORE = 0.12

LATERAL_NEGATIVE_ACTION_RATIO_THRESHOLD = 0.25
LATERAL_NEGATIVE_ACTION_SCORE = 0.1

LATERAL_SHORT_BYTES_THRESHOLD = 400
LATERAL_SHORT_PACKETS_THRESHOLD = 3
LATERAL_SHORT_DURATION_SECONDS = 30
LATERAL_SHORT_RATIO_THRESHOLD = 0.35
LATERAL_SHORT_MIN_FLOWS = 8
LATERAL_SHORT_SCORE = 0.1

LATERAL_UNIQUE_PORT_THRESHOLD = 6
LATERAL_UNIQUE_PORT_SCORE = 0.08

LATERAL_SHORT_DURATION_THRESHOLD = 60
LATERAL_SHORT_DURATION_MIN_HOSTS = 4
LATERAL_SHORT_DURATION_SCORE = 0.06

LATERAL_HIGH_FANOUT_FOR_PROBE = 4

LATERAL_HIGH_RISK_THRESHOLD = 0.65
LATERAL_RULE_SCORE_THRESHOLD = 0.35
LATERAL_MIN_FLOWS_FOR_CANDIDATE = 2
LATERAL_MAX_RESULTS_MULTIPLIER = 50
LATERAL_MAX_RESULTS_CAP = 1000

LATERAL_CREDENTIAL_PORTS = (22, 88, 135, 139, 389, 445, 464, 636, 1433, 1521, 3306, 3389, 5432, 5985, 5986)
LATERAL_ADMIN_SERVICES = ("ssh", "rdp", "smb", "dce_rpc", "kerberos", "ldap", "ldaps", "winrm", "mssql", "mysql", "postgresql", "rpc")
LATERAL_NEGATIVE_ACTIONS = ("deny", "drop", "block", "reset", "reject")
LATERAL_RISKY_STATES = ("syn_sent", "syn_only", "reset", "rst", "failed", "reject", "timeout")

LATERAL_SCORING_CONTAMINATION = 0.12

LATERAL_FANOUT_FINDING_CONFIDENCE = 0.7
LATERAL_CREDENTIAL_FINDING_CONFIDENCE = 0.65
LATERAL_SHORT_PROBE_FINDING_CONFIDENCE = 0.6


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def execute_lateral_movement_review(
    con: Any,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    limit: int,
) -> dict[str, Any]:
    """Execute lateral movement review and return structured data dict."""
    available = available_canonical_fields(mappings)
    ensure_required(mappings, ["src_ip", "dst_ip", "dst_port", "bytes", "packets"])

    # Track missing optional fields that affect scoring signals
    has_action = "action" in available
    has_session_state = "session_state" in available
    has_service = "service" in available
    has_duration = "duration_ms" in available or "flow_duration" in available

    missing_fields = []
    if not has_action:
        missing_fields.append("action")
    if not has_session_state:
        missing_fields.append("session_state")
    if not has_service:
        missing_fields.append("service")
    if not has_duration:
        missing_fields.append("duration")

    private_src_predicate = _private_ip_predicate("src_ip")
    private_dst_predicate = _private_ip_predicate("dst_ip")
    internal_scope_predicate = f"{private_src_predicate} AND {private_dst_predicate}"

    # Duration expression: no default 0 so NULLs aren't treated as 0s
    if has_duration:
        duration_expr = "COALESCE(duration_ms, flow_duration)"
        short_signal_name = "short_like_ratio"
        short_expr = f"CASE WHEN COALESCE(bytes, 0) <= {LATERAL_SHORT_BYTES_THRESHOLD} AND COALESCE(packets, 0) <= {LATERAL_SHORT_PACKETS_THRESHOLD} AND {duration_expr} IS NOT NULL AND {duration_expr} <= {LATERAL_SHORT_DURATION_SECONDS} THEN 1 ELSE 0 END"
    else:
        duration_expr = "NULL"
        short_signal_name = "short_low_payload_ratio"
        short_expr = f"CASE WHEN COALESCE(bytes, 0) <= {LATERAL_SHORT_BYTES_THRESHOLD} AND COALESCE(packets, 0) <= {LATERAL_SHORT_PACKETS_THRESHOLD} THEN 1 ELSE 0 END"

    # Service-based expressions (gated on field availability)
    if has_service:
        service_expr = "LOWER(COALESCE(service, ''))"
        admin_service_expr = f"CASE WHEN {service_expr} IN {LATERAL_ADMIN_SERVICES} THEN 1 ELSE 0 END"
    else:
        service_expr = None
        admin_service_expr = "0"

    admin_service_ratio_col = "admin_service_ratio"
    credential_port_expr = f"CASE WHEN COALESCE(dst_port, 0) IN {LATERAL_CREDENTIAL_PORTS} THEN 1 ELSE 0 END"
    action_negative_expr = (
        f"CASE WHEN LOWER(COALESCE(action, '')) IN {LATERAL_NEGATIVE_ACTIONS} THEN 1 ELSE 0 END"
        if has_action
        else "0"
    )
    risky_state_expr = (
        f"CASE WHEN LOWER(COALESCE(session_state, '')) IN {LATERAL_RISKY_STATES} THEN 1 ELSE 0 END"
        if has_session_state
        else "0"
    )

    # Duration expression for SELECT
    if has_duration:
        avg_duration_col = "ROUND(AVG(COALESCE(duration_ms, flow_duration)), 2) AS avg_duration"
        max_duration_col = "MAX(COALESCE(duration_ms, flow_duration)) AS max_duration"
    else:
        avg_duration_col = "NULL AS avg_duration"
        max_duration_col = "NULL AS max_duration"

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
            ROUND(SUM({short_expr}) * 1.0 / NULLIF(COUNT(*), 0), 4) AS {short_signal_name},
            ROUND(AVG(COALESCE(bytes, 0)), 2) AS avg_bytes,
            MAX(COALESCE(bytes, 0)) AS max_bytes,
            ROUND(AVG(COALESCE(packets, 0)), 2) AS avg_packets,
            {avg_duration_col},
            {max_duration_col}
        FROM flows
        {where_clause}
        {"AND" if where_clause else "WHERE"} src_ip IS NOT NULL
          AND dst_ip IS NOT NULL
          AND {internal_scope_predicate}
        GROUP BY 1
        HAVING COUNT(*) >= {LATERAL_MIN_FLOWS_FOR_CANDIDATE}
        ORDER BY unique_internal_dst_ip DESC, credential_port_flows DESC, risky_state_ratio DESC, src_ip ASC
        LIMIT {max(limit * LATERAL_MAX_RESULTS_MULTIPLIER, LATERAL_MAX_RESULTS_CAP)}
    """
    _, rows = rows_from_query(con, sql)

    result: dict[str, Any] = {
        "candidates": [],
        "scored": [],
        "available_fields": list(available),
        "missing_fields": missing_fields,
        "signal_coverage": {
            "action": has_action,
            "session_state": has_session_state,
            "service": has_service,
            "duration": has_duration,
        },
        "short_signal_name": short_signal_name,
        "_limit": limit,
    }

    if not rows:
        return result

    candidates = [
        {
            "src_ip": row.get("src_ip"),
            "flow_count": int(_safe_float(row.get("flow_count"))),
            "unique_internal_dst_ip": int(_safe_float(row.get("unique_internal_dst_ip"))),
            "unique_internal_dst_port": int(_safe_float(row.get("unique_internal_dst_port"))),
            "credential_port_flows": int(_safe_float(row.get("credential_port_flows"))),
            "credential_port_count": int(_safe_float(row.get("credential_port_count"))),
            "credential_port_ratio": round(_safe_float(row.get("credential_port_ratio")), 4),
            "admin_service_ratio": round(_safe_float(row.get("admin_service_ratio")), 4),
            "negative_action_ratio": round(_safe_float(row.get("negative_action_ratio")), 4),
            "risky_state_ratio": round(_safe_float(row.get("risky_state_ratio")), 4),
            short_signal_name: round(_safe_float(row.get(short_signal_name)), 4),
            "avg_bytes": round(_safe_float(row.get("avg_bytes")), 2),
            "max_bytes": round(_safe_float(row.get("max_bytes")), 2),
            "avg_packets": round(_safe_float(row.get("avg_packets")), 2),
            "avg_duration": round(_safe_float(row.get("avg_duration")), 2),
            "max_duration": round(_safe_float(row.get("max_duration")), 2),
        }
        for row in rows
    ]
    result["candidates"] = candidates

    def lateral_rule_score(row: dict[str, Any]) -> float:
        score = 0.0
        if _safe_float(row.get("unique_internal_dst_ip")) >= LATERAL_HIGH_FANOUT_THRESHOLD:
            score += LATERAL_HIGH_FANOUT_SCORE
        elif _safe_float(row.get("unique_internal_dst_ip")) >= LATERAL_MED_FANOUT_THRESHOLD:
            score += LATERAL_MED_FANOUT_SCORE
        if _safe_float(row.get("credential_port_ratio")) >= LATERAL_CREDENTIAL_PORT_RATIO_THRESHOLD or _safe_float(row.get("credential_port_count")) >= LATERAL_CREDENTIAL_PORT_COUNT_THRESHOLD:
            score += LATERAL_CREDENTIAL_PORT_SCORE
        if has_service and _safe_float(row.get("admin_service_ratio")) >= LATERAL_ADMIN_SERVICE_RATIO_THRESHOLD:
            score += LATERAL_ADMIN_SERVICE_SCORE
        if has_session_state and _safe_float(row.get("risky_state_ratio")) >= LATERAL_RISKY_STATE_RATIO_THRESHOLD:
            score += LATERAL_RISKY_STATE_SCORE
        if has_action and _safe_float(row.get("negative_action_ratio")) >= LATERAL_NEGATIVE_ACTION_RATIO_THRESHOLD:
            score += LATERAL_NEGATIVE_ACTION_SCORE
        if has_duration and _safe_float(row.get(short_signal_name)) >= LATERAL_SHORT_RATIO_THRESHOLD and _safe_float(row.get("flow_count")) >= LATERAL_SHORT_MIN_FLOWS:
            score += LATERAL_SHORT_SCORE
        if _safe_float(row.get("unique_internal_dst_port")) >= LATERAL_UNIQUE_PORT_THRESHOLD:
            score += LATERAL_UNIQUE_PORT_SCORE
        if has_duration and _safe_float(row.get("avg_duration")) != 0 and _safe_float(row.get("avg_duration")) <= LATERAL_SHORT_DURATION_THRESHOLD and _safe_float(row.get("unique_internal_dst_ip")) >= LATERAL_SHORT_DURATION_MIN_HOSTS:
            score += LATERAL_SHORT_DURATION_SCORE
        return min(1.0, score)

    def lateral_reason(row: dict[str, Any], final_score: float, rule_score: float) -> str:
        reasons: list[str] = []
        if _safe_float(row.get("unique_internal_dst_ip")) >= LATERAL_HIGH_FANOUT_FOR_PROBE:
            reasons.append("internal_multi_host_fanout")
        if _safe_float(row.get("credential_port_ratio")) >= LATERAL_CREDENTIAL_PORT_RATIO_THRESHOLD or _safe_float(row.get("credential_port_count")) >= LATERAL_CREDENTIAL_PORT_COUNT_THRESHOLD:
            reasons.append("credential_service_spread")
        if has_service and _safe_float(row.get("admin_service_ratio")) >= LATERAL_ADMIN_SERVICE_RATIO_THRESHOLD:
            reasons.append("admin_protocol_concentration")
        if has_session_state and _safe_float(row.get("risky_state_ratio")) >= LATERAL_RISKY_STATE_RATIO_THRESHOLD:
            reasons.append("risky_internal_session_states")
        if has_action and _safe_float(row.get("negative_action_ratio")) >= LATERAL_NEGATIVE_ACTION_RATIO_THRESHOLD:
            reasons.append("blocked_internal_access_pattern")
        if has_duration and _safe_float(row.get(short_signal_name)) >= LATERAL_SHORT_RATIO_THRESHOLD:
            reasons.append("short_internal_probe_pattern")
        if not reasons and final_score >= LATERAL_HIGH_RISK_THRESHOLD:
            reasons.append("model_ranked_lateral_candidate")
        if not reasons and rule_score >= LATERAL_RULE_SCORE_THRESHOLD:
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
            short_signal_name,
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
        contamination=LATERAL_SCORING_CONTAMINATION,
        engine="hybrid",
    )
    result["scored"] = scored
    return result


def format_lateral_movement_review(data: dict[str, Any]) -> str:
    """Produce the text report for backward-compatible output."""
    sections: list[str] = []
    scored = data["scored"]
    limit = data.get("_limit", 10)
    signal_coverage = data.get("signal_coverage", {})

    if not scored and not data["candidates"]:
        return (
            "Lateral movement review\n"
            "No internal east-west communication candidates were found in the selected scope. "
            "This review needs private-address source and destination activity to rank lateral movement hotspots."
        )

    coverage_lines = []
    for field in ("action", "session_state", "service", "duration"):
        status = "available" if signal_coverage.get(field) else "missing"
        coverage_lines.append(f"  {field}: {status}")
    sections.append("Signal coverage:\n" + "\n".join(coverage_lines))

    short_signal_name = data.get("short_signal_name", "short_like_ratio")
    sections.append(
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
                short_signal_name,
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
                    row.get(short_signal_name),
                    row.get("likely_reason"),
                )
                for row in scored[:limit]
            ],
        )
    )

    sections.append(
        "This review emphasizes private-address east-west spread, credential-service coverage, risky internal session states, and short internal probe patterns. It is strongest on datasets with meaningful internal IP visibility and session/action fields."
    )

    return "\n\n".join(section for section in sections if section)


def build_skill_result_parts(data: dict[str, Any], raw_output: str) -> dict[str, Any]:
    """Build structured SkillResult for lateral-movement-review action."""
    evidence: list[dict[str, Any]] = []
    findings: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    metrics: list[dict[str, Any]] = []
    scored = data["scored"]
    limit = data.get("_limit", 10)
    signal_coverage = data.get("signal_coverage", {})
    has_duration = signal_coverage.get("duration", False)
    short_signal_name = data.get("short_signal_name", "short_like_ratio")

    # Metrics
    high_risk = sum(1 for r in scored if float(r.get("lateral_movement_risk_score", 0.0)) >= LATERAL_HIGH_RISK_THRESHOLD)
    max_score = max((float(r.get("lateral_movement_risk_score", 0.0)) for r in scored), default=0.0)
    max_dst_ip = max((int(r.get("unique_internal_dst_ip", 0)) for r in scored), default=0)
    max_dst_port = max((int(r.get("unique_internal_dst_port", 0)) for r in scored), default=0)
    max_cred_ports = max((int(r.get("credential_port_count", 0)) for r in scored), default=0)

    metrics.append({"name": "candidate_sources", "value": len(scored)})
    metrics.append({"name": "high_risk_sources", "value": high_risk})
    metrics.append({"name": "max_lateral_movement_risk_score", "value": round(max_score, 4)})
    metrics.append({"name": "max_internal_dst_ip", "value": max_dst_ip})
    metrics.append({"name": "max_internal_dst_port", "value": max_dst_port})
    metrics.append({"name": "max_credential_port_count", "value": max_cred_ports})

    # Lateral movement metrics evidence
    evidence.append({
        "evidence_id": "e-lateral-movement-metrics",
        "type": "metric",
        "title": "Lateral Movement Review Metrics",
        "metrics": metrics,
    })

    # Hotspots table
    if scored:
        hotspot_columns = [
            "src_ip", "lateral_movement_risk_score", "severity",
            "unique_internal_dst_ip", "credential_port_ratio",
            "admin_service_ratio", "risky_state_ratio", short_signal_name,
            "likely_reason",
        ]
        hotspot_rows = [
            [
                r.get("src_ip"), r.get("lateral_movement_risk_score"), r.get("severity"),
                r.get("unique_internal_dst_ip"), r.get("credential_port_ratio"),
                r.get("admin_service_ratio"), r.get("risky_state_ratio"),
                r.get(short_signal_name), r.get("likely_reason"),
            ]
            for r in scored[:limit]
        ]
        evidence.append({
            "evidence_id": "e-lateral-movement-hotspots",
            "type": "table",
            "title": "Lateral Movement Hotspots",
            "columns": hotspot_columns,
            "rows": hotspot_rows,
        })

    # Findings for high-risk sources
    for row in scored[:limit]:
        score = float(row.get("lateral_movement_risk_score", 0.0))
        if score >= LATERAL_HIGH_RISK_THRESHOLD:
            severity = str(row.get("severity") or "medium").lower()
            findings.append({
                "finding_id": f"f-lateral-movement-{row.get('src_ip', 'unknown')}",
                "type": "lateral_movement_candidate",
                "severity": severity,
                "confidence": score,
                "title": f"Lateral movement candidate: {row.get('src_ip')}",
                "description": row.get("likely_reason") or "Source flagged as lateral movement candidate.",
                "entities": [{"type": "src_ip", "value": row.get("src_ip")}],
                "evidence_refs": ["e-lateral-movement-hotspots", "e-lateral-movement-metrics"],
            })

    # Internal fanout finding
    if max_dst_ip >= LATERAL_HIGH_FANOUT_THRESHOLD:
        top_src = max(scored, key=lambda r: int(r.get("unique_internal_dst_ip", 0)))
        findings.append({
            "finding_id": "f-lateral-fanout",
            "type": "internal_multi_host_fanout",
            "severity": "high",
            "confidence": LATERAL_FANOUT_FINDING_CONFIDENCE,
            "title": f"Internal host fanout: {max_dst_ip} unique internal destinations",
            "description": (
                f"Source {top_src.get('src_ip')} communicated with {max_dst_ip} unique internal IPs. "
                f"This fanout pattern is consistent with lateral movement or internal discovery."
            ),
            "entities": [{"type": "src_ip", "value": top_src.get("src_ip")}],
            "evidence_refs": ["e-lateral-movement-hotspots", "e-lateral-movement-metrics"],
        })

    # Credential service spread finding
    if max_cred_ports >= LATERAL_CREDENTIAL_PORT_COUNT_THRESHOLD:
        top_src = max(scored, key=lambda r: int(r.get("credential_port_count", 0)))
        findings.append({
            "finding_id": "f-lateral-cred-spread",
            "type": "credential_service_spread",
            "severity": "medium",
            "confidence": LATERAL_CREDENTIAL_FINDING_CONFIDENCE,
            "title": f"Credential port spread: {max_cred_ports} distinct credential ports",
            "description": (
                f"Source {top_src.get('src_ip')} accessed {max_cred_ports} distinct credential-related ports "
                f"(SSH, RDP, SMB, Kerberos, LDAP, WinRM, etc.). This may indicate credential harvesting or privilege escalation."
            ),
            "entities": [{"type": "src_ip", "value": top_src.get("src_ip")}],
            "evidence_refs": ["e-lateral-movement-hotspots", "e-lateral-movement-metrics"],
        })

    # Short internal probe finding — only when duration is available
    if scored and has_duration:
        max_short = max((float(r.get(short_signal_name, 0)) for r in scored), default=0.0)
        if max_short >= LATERAL_SHORT_RATIO_THRESHOLD:
            top_src = max(scored, key=lambda r: float(r.get(short_signal_name, 0)))
            findings.append({
                "finding_id": "f-lateral-short-probe",
                "type": "short_internal_probe_pattern",
                "severity": "medium",
                "confidence": LATERAL_SHORT_PROBE_FINDING_CONFIDENCE,
                "title": f"Short internal probe pattern: {round(max_short * 100, 1)}%",
                "description": (
                    f"Source {top_src.get('src_ip')} has {short_signal_name}={round(max_short, 2)}. "
                    f"A high fraction of short, low-payload flows to internal hosts suggests probing or enumeration."
                ),
                "entities": [{"type": "src_ip", "value": top_src.get("src_ip")}],
                "evidence_refs": ["e-lateral-movement-hotspots", "e-lateral-movement-metrics"],
            })

    # Raw report
    evidence.append({
        "evidence_id": "e-raw-report",
        "type": "text",
        "title": "Raw Lateral Movement Review Output",
        "content": raw_output,
    })

    # Missing field warnings
    available = set(data.get("available_fields", []))
    if "action" not in available:
        warnings.append({
            "code": "missing_action_field",
            "message": "No action field available; negative_action_ratio is zero-filled. Internal access denial patterns cannot be reliably assessed.",
            "severity": "warning",
        })
    if "session_state" not in available:
        warnings.append({
            "code": "missing_session_state_field",
            "message": "No session_state field available; risky_state_ratio is zero-filled. Failed internal session analysis is limited.",
            "severity": "warning",
        })
    if "service" not in available:
        warnings.append({
            "code": "missing_service_field",
            "message": "No service field available; admin_service_ratio is zero-filled. Admin protocol concentration signals are unavailable.",
            "severity": "warning",
        })
    if not has_duration:
        warnings.append({
            "code": "missing_duration_field",
            "message": "No duration field available (duration_ms or flow_duration); short_like_ratio cannot be computed. Short low-payload-only signal used instead without duration gating.",
            "severity": "warning",
        })

    # Fix evidence_refs
    existing_ids = {e["evidence_id"] for e in evidence}
    for finding in findings:
        finding["evidence_refs"] = [ref for ref in finding["evidence_refs"] if ref in existing_ids]

    # Compute overall severity — use max(candidate severities) when candidates exist
    severity_order = {"critical": 4, "high": 3, "medium": 2, "low": 1, "info": 0}
    highest = "info"
    for f in findings:
        if severity_order.get(f.get("severity", "info"), 0) > severity_order.get(highest, 0):
            highest = f["severity"]
    # Also consider candidate severities
    for r in scored:
        sev = str(r.get("severity") or "info").lower()
        if severity_order.get(sev, 0) > severity_order.get(highest, 0):
            highest = sev

    overview_text = "Lateral movement review."
    if findings:
        overview_text += f" {len(findings)} finding(s) identified."
    elif scored:
        overview_text += f" {len(scored)} candidate(s) ranked, no high-confidence findings."

    return {
        "summary": {
            "title": "Lateral Movement Review",
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
                "signal_coverage": signal_coverage,
                "missing_fields": data.get("missing_fields", []),
            },
        },
    }


# Backward-compatible entry point
def lateral_movement_review_action(
    con: Any,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    limit: int,
) -> str:
    """Legacy entry point — delegates to execute + format for backward compatibility."""
    data = execute_lateral_movement_review(con, mappings, where_clause, limit)
    return format_lateral_movement_review(data)
