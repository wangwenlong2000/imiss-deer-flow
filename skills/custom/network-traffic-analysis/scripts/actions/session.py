from __future__ import annotations

from typing import Any

import duckdb  # type: ignore

from analysis.anomaly_models import score_session_candidates
from analysis.feature_engineering import rows_from_query, session_candidate_sql
from utils.formatter import render_rows_section, render_section
from core.schema_mapping import available_canonical_fields, ensure_required


def _where_to_and(where_clause: str) -> str:
    """Convert "" or "WHERE ..." into an AND ... suffix."""
    stripped = where_clause.strip()
    if not stripped:
        return ""
    if stripped.upper().startswith("WHERE"):
        body = stripped[len("WHERE"):].strip()
        return f" AND {body}" if body else ""
    if stripped.upper().startswith("AND"):
        return f" {stripped}"
    return f" {stripped}"


def execute_session_review(
    con: duckdb.DuckDBPyConnection,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    view: str,
    limit: int,
) -> dict[str, Any]:
    """Execute session review and return structured data."""
    available = available_canonical_fields(mappings)
    and_clause = _where_to_and(where_clause)

    result: dict[str, Any] = {
        "view": view,
        "risk_sources": [],
        "session_state_rows": [],
        "connection_outcome_rows": [],
        "failure_heavy_rows": [],
        "short_low_byte_rows": [],
        "handshake_summary": [],
        "tcp_flag_rows": [],
        "handshake_failure_rows": [],
        "small_packet_rows": [],
        "metrics": {},
        "coverage": {},
    }

    if view == "packet":
        if "tcp_flags" in available:
            row = con.execute(
                f"""
                WITH tcp_packets AS (
                    SELECT *
                    FROM flows
                    WHERE 1=1 {and_clause}
                      AND protocol = 'TCP'
                      AND tcp_flags IS NOT NULL
                      AND tcp_flags != ''
                )
                SELECT
                    COALESCE(COUNT(*), 0) AS tcp_packets,
                    COALESCE(SUM(CASE WHEN COALESCE(tcp_flags, '') LIKE '%SA%' THEN 1 ELSE 0 END), 0) AS syn_ack_packets,
                    COALESCE(SUM(CASE WHEN COALESCE(tcp_flags, '') = 'S' THEN 1 ELSE 0 END), 0) AS syn_only_packets,
                    COALESCE(ROUND(SUM(CASE WHEN COALESCE(tcp_flags, '') = 'S' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2), 0.0) AS syn_only_pct,
                    COALESCE(SUM(CASE WHEN COALESCE(tcp_flags, '') LIKE '%R%' THEN 1 ELSE 0 END), 0) AS rst_packets,
                    COALESCE(ROUND(SUM(CASE WHEN COALESCE(tcp_flags, '') LIKE '%R%' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2), 0.0) AS rst_pct
                FROM tcp_packets
                """
            ).fetchone()
            if row:
                result["handshake_summary"] = [
                    {"tcp_packets": row[0], "syn_ack_packets": row[1], "syn_only_packets": row[2],
                     "syn_only_pct": row[3], "rst_packets": row[4], "rst_pct": row[5]}
                ]
            result["coverage"]["has_tcp_flags"] = True

        if "tcp_flags" in available:
            rows = con.execute(
                f"""
                SELECT COALESCE(tcp_flags, 'UNKNOWN') AS tcp_flags,
                       COUNT(*) AS packets,
                       COUNT(DISTINCT src_ip) AS unique_src_ip,
                       COUNT(DISTINCT dst_ip) AS unique_dst_ip
                FROM flows
                WHERE 1=1 {and_clause}
                GROUP BY 1
                ORDER BY packets DESC, tcp_flags ASC
                LIMIT {limit}
                """
            ).fetchall()
            result["tcp_flag_rows"] = [list(r) for r in rows]

        if {"src_ip", "dst_ip", "tcp_flags"}.issubset(available):
            rows = con.execute(
                f"""
                SELECT src_ip,
                       COUNT(*) AS packets,
                       SUM(CASE WHEN COALESCE(tcp_flags, '') = 'S' THEN 1 ELSE 0 END) AS syn_only_packets,
                       ROUND(SUM(CASE WHEN COALESCE(tcp_flags, '') = 'S' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS syn_only_pct,
                       SUM(CASE WHEN COALESCE(tcp_flags, '') LIKE '%R%' THEN 1 ELSE 0 END) AS rst_packets,
                       ROUND(SUM(CASE WHEN COALESCE(tcp_flags, '') LIKE '%R%' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS rst_pct,
                       COUNT(DISTINCT dst_ip) AS unique_dst_ip
                FROM flows
                WHERE 1=1 {and_clause} AND src_ip IS NOT NULL
                GROUP BY 1
                HAVING syn_only_packets > 0 OR rst_packets > 0
                ORDER BY syn_only_packets DESC, rst_packets DESC, packets DESC
                LIMIT {limit}
                """
            ).fetchall()
            result["handshake_failure_rows"] = [list(r) for r in rows]

        if "frame_len" in available or "payload_bytes" in available:
            length_expr = "COALESCE(payload_bytes, frame_len, bytes, 0)"
            rows = con.execute(
                f"""
                SELECT src_ip,
                       COUNT(*) AS packets,
                       SUM(CASE WHEN {length_expr} <= 128 THEN 1 ELSE 0 END) AS small_packets,
                       ROUND(SUM(CASE WHEN {length_expr} <= 128 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS small_packet_pct
                FROM flows
                WHERE 1=1 {and_clause} AND src_ip IS NOT NULL
                GROUP BY 1
                HAVING COUNT(*) >= 20
                ORDER BY small_packet_pct DESC, packets DESC, src_ip ASC
                LIMIT {limit}
                """
            ).fetchall()
            result["small_packet_rows"] = [list(r) for r in rows]
    else:
        _, session_rows = rows_from_query(
            con,
            session_candidate_sql(and_clause, limit=max(limit * 20, 500)),
        )
        scored_sessions = score_session_candidates(session_rows)
        result["risk_sources"] = [dict(row) for row in scored_sessions[:limit]]

        high_risk = sum(1 for row in scored_sessions if float(row.get("session_risk_score", 0.0)) >= 0.65)
        result["metrics"]["candidate_sources"] = len(scored_sessions)
        result["metrics"]["high_risk_sources"] = high_risk

        if "session_state" in available:
            rows = con.execute(
                f"""
                SELECT COALESCE(session_state, 'UNKNOWN') AS session_state,
                       COUNT(*) AS records,
                       SUM(COALESCE(bytes, 0)) AS total_bytes
                FROM flows
                WHERE 1=1 {and_clause}
                GROUP BY 1
                ORDER BY records DESC, total_bytes DESC, session_state ASC
                LIMIT {limit}
                """
            ).fetchall()
            result["session_state_rows"] = [list(r) for r in rows]
            result["coverage"]["has_session_state"] = True

        if "action" in available:
            rows = con.execute(
                f"""
                SELECT COALESCE(action, 'UNKNOWN') AS action,
                       COUNT(*) AS records,
                       SUM(COALESCE(bytes, 0)) AS total_bytes
                FROM flows
                WHERE 1=1 {and_clause}
                GROUP BY 1
                ORDER BY records DESC, total_bytes DESC, action ASC
                LIMIT {limit}
                """
            ).fetchall()
            result["connection_outcome_rows"] = [list(r) for r in rows]
            result["coverage"]["has_action"] = True

        if "src_ip" in available and "action" in available:
            rows = con.execute(
                f"""
                SELECT src_ip,
                       COUNT(*) AS flows,
                       SUM(CASE WHEN LOWER(COALESCE(action, '')) IN ('deny', 'drop', 'block', 'reset', 'reject') THEN 1 ELSE 0 END) AS negative_outcomes,
                       ROUND(SUM(CASE WHEN LOWER(COALESCE(action, '')) IN ('deny', 'drop', 'block', 'reset', 'reject') THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS negative_pct,
                       SUM(COALESCE(bytes, 0)) AS total_bytes
                FROM flows
                WHERE 1=1 {and_clause} AND src_ip IS NOT NULL
                GROUP BY 1
                HAVING COUNT(*) >= 5
                ORDER BY negative_pct DESC, negative_outcomes DESC, flows DESC
                LIMIT {limit}
                """
            ).fetchall()
            result["failure_heavy_rows"] = [list(r) for r in rows]

        if {"src_ip", "bytes", "flow_duration"}.issubset(available):
            rows = con.execute(
                f"""
                SELECT src_ip,
                       COUNT(*) AS flows,
                       SUM(CASE WHEN COALESCE(bytes, 0) <= 128 AND COALESCE(flow_duration, 0) <= 1000 THEN 1 ELSE 0 END) AS short_low_byte_flows,
                       ROUND(SUM(CASE WHEN COALESCE(bytes, 0) <= 128 AND COALESCE(flow_duration, 0) <= 1000 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS short_low_byte_pct
                FROM flows
                WHERE 1=1 {and_clause} AND src_ip IS NOT NULL
                GROUP BY 1
                HAVING COUNT(*) >= 5
                ORDER BY short_low_byte_pct DESC, short_low_byte_flows DESC, flows DESC
                LIMIT {limit}
                """
            ).fetchall()
            result["short_low_byte_rows"] = [list(r) for r in rows]

    return result


def format_session_review(data: dict[str, Any]) -> str:
    """Format structured session review data as human-readable text."""
    sections = [f"Analysis view: {data['view']}"]

    if data["view"] == "packet":
        if data["handshake_summary"]:
            s = data["handshake_summary"][0]
            sections.append(
                "\n".join([
                    "Packet handshake and reset summary",
                    f"tcp_packets={s['tcp_packets']}, syn_ack={s['syn_ack_packets']}, "
                    f"syn_only={s['syn_only_packets']} ({s['syn_only_pct']}%), "
                    f"rst={s['rst_packets']} ({s['rst_pct']}%)",
                ])
            )
        if data["tcp_flag_rows"]:
            sections.append(
                render_rows_section("TCP flag quality review", ["tcp_flags", "packets", "unique_src_ip", "unique_dst_ip"], data["tcp_flag_rows"])
            )
        if data["handshake_failure_rows"]:
            sections.append(
                render_rows_section("Potential handshake-failure sources", ["src_ip", "packets", "syn_only_packets", "syn_only_pct", "rst_packets", "rst_pct", "unique_dst_ip"], data["handshake_failure_rows"])
            )
        if data["small_packet_rows"]:
            sections.append(
                render_rows_section("Small-packet concentration", ["src_ip", "packets", "small_packets", "small_packet_pct"], data["small_packet_rows"])
            )
    else:
        if data["risk_sources"]:
            risk_columns = ["src_ip", "flows", "negative_pct", "risky_state_pct", "short_low_byte_pct", "session_risk_score", "severity", "likely_reason"]
            risk_rows = [
                (
                    row.get("src_ip"), row.get("flows"), row.get("negative_pct"),
                    row.get("risky_state_pct"), row.get("short_low_byte_pct"),
                    row.get("session_risk_score"), row.get("severity"),
                    row.get("likely_reason"),
                )
                for row in data["risk_sources"]
            ]
            sections.append(render_rows_section("Top session-risk sources (hybrid scoring)", risk_columns, risk_rows))

        if data["session_state_rows"]:
            sections.append(render_rows_section("Session state distribution", ["session_state", "records", "total_bytes"], data["session_state_rows"]))

        if data["connection_outcome_rows"]:
            sections.append(render_rows_section("Connection outcome distribution", ["action", "records", "total_bytes"], data["connection_outcome_rows"]))

        m = data["metrics"]
        sections.append(
            "\n".join([
                "Session-risk summary",
                f"candidate_sources={m.get('candidate_sources', 0)}, high_risk_sources={m.get('high_risk_sources', 0)}",
                "Hybrid session scoring combines failure outcomes, risky session-state composition, short low-byte concentration, and source-level outlier scoring.",
            ])
        )

        if data["failure_heavy_rows"]:
            sections.append(render_rows_section("Potential failure-heavy sources", ["src_ip", "flows", "negative_outcomes", "negative_pct", "total_bytes"], data["failure_heavy_rows"]))

        if data["short_low_byte_rows"]:
            sections.append(render_rows_section("Short and low-byte connection review", ["src_ip", "flows", "short_low_byte_flows", "short_low_byte_pct"], data["short_low_byte_rows"]))

    return "\n\n".join(sections)


def session_review_action(
    con: duckdb.DuckDBPyConnection,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    view: str,
    limit: int,
) -> str:
    """Legacy entry point — delegates to execute + format for backward compatibility."""
    data = execute_session_review(con, mappings, where_clause, view, limit)
    return format_session_review(data)


def build_skill_result_parts(data: dict[str, Any], raw_output: str) -> dict[str, Any]:
    """Build structured SkillResult for session-review action."""
    findings: list[dict[str, Any]] = []
    evidence: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    metrics: list[dict[str, Any]] = []

    view = data["view"]

    if view == "packet":
        # Handshake summary as metric evidence
        if data["handshake_summary"]:
            s = data["handshake_summary"][0]
            metrics.extend([
                {"name": "tcp_packets", "value": s["tcp_packets"]},
                {"name": "syn_ack_packets", "value": s["syn_ack_packets"]},
                {"name": "syn_only_packets", "value": s["syn_only_packets"]},
                {"name": "syn_only_pct", "value": s["syn_only_pct"]},
                {"name": "rst_packets", "value": s["rst_packets"]},
                {"name": "rst_pct", "value": s["rst_pct"]},
            ])
            syn_pct = s.get("syn_only_pct", 0) or 0
            rst_pct = s.get("rst_pct", 0) or 0
            if syn_pct >= 30:
                findings.append({
                    "finding_id": "f-syn-only-heavy",
                    "type": "syn_only_heavy",
                    "severity": "high",
                    "confidence": 0.7,
                    "title": f"SYN-only packets at {syn_pct}%",
                    "description": (
                        f"SYN-only packets account for {syn_pct}% of TCP packets. "
                        f"This can indicate port scanning, SYN floods, or misconfigured clients."
                    ),
                    "evidence_refs": ["e-handshake-posture", "e-packet-metrics"],
                })
            if rst_pct >= 30:
                findings.append({
                    "finding_id": "f-rst-heavy",
                    "type": "rst_heavy",
                    "severity": "medium",
                    "confidence": 0.7,
                    "title": f"RST packets at {rst_pct}%",
                    "description": (
                        f"RST packets account for {rst_pct}% of TCP packets. "
                        f"High RST rates can indicate connection rejection, firewall blocks, or aggressive scanning."
                    ),
                    "evidence_refs": ["e-handshake-posture", "e-packet-metrics"],
                })

        if data["tcp_flag_rows"]:
            evidence.append({
                "evidence_id": "e-tcp-flags-distribution",
                "type": "table",
                "title": "TCP Flags Distribution",
                "columns": ["tcp_flags", "packets", "unique_src_ip", "unique_dst_ip"],
                "rows": data["tcp_flag_rows"],
            })

        if data["handshake_summary"]:
            evidence.append({
                "evidence_id": "e-handshake-posture",
                "type": "metric",
                "title": "TCP Handshake Posture",
                "metrics": metrics,
            })

        if data["handshake_failure_rows"]:
            evidence.append({
                "evidence_id": "e-handshake-anomaly-sources",
                "type": "table",
                "title": "Potential Handshake Failure Sources",
                "columns": ["src_ip", "packets", "syn_only_packets", "syn_only_pct", "rst_packets", "rst_pct", "unique_dst_ip"],
                "rows": data["handshake_failure_rows"],
            })

        if data["small_packet_rows"]:
            evidence.append({
                "evidence_id": "e-small-packet-concentration",
                "type": "table",
                "title": "Small-Packet Concentration",
                "columns": ["src_ip", "packets", "small_packets", "small_packet_pct"],
                "rows": data["small_packet_rows"],
            })
    else:
        # Flow view — session risk sources
        if data["risk_sources"]:
            evidence.append({
                "evidence_id": "e-session-risk-sources",
                "type": "table",
                "title": "Top Session Risk Sources",
                "columns": ["src_ip", "flows", "negative_pct", "risky_state_pct", "short_low_byte_pct", "session_risk_score", "severity", "likely_reason"],
                "rows": [
                    (
                        r.get("src_ip"), r.get("flows"), r.get("negative_pct"),
                        r.get("risky_state_pct"), r.get("short_low_byte_pct"),
                        r.get("session_risk_score"), r.get("severity"),
                        r.get("likely_reason"),
                    )
                    for r in data["risk_sources"]
                ],
            })

        high_risk = data["metrics"].get("high_risk_sources", 0)
        if high_risk > 0:
            high_risk_sources = [
                r for r in data["risk_sources"]
                if float(r.get("session_risk_score", 0)) >= 0.65
            ]
            top_src = high_risk_sources[0].get("src_ip") if high_risk_sources else "unknown"
            top_score = high_risk_sources[0].get("session_risk_score", 0) if high_risk_sources else 0
            findings.append({
                "finding_id": "f-session-risk-source",
                "type": "session_risk_source",
                "severity": "high",
                "confidence": 0.7,
                "title": f"{high_risk} high-risk session source(s) detected",
                "description": (
                    f"{high_risk} out of {data['metrics'].get('candidate_sources', 0)} candidate sources "
                    f"have session_risk_score >= 0.65. Top source: {top_src} (score: {top_score})."
                ),
                "entities": [{"type": "src_ip", "value": top_src}],
                "evidence_refs": ["e-session-risk-sources", "e-session-risk-metrics"],
            })

        if data["session_state_rows"]:
            evidence.append({
                "evidence_id": "e-session-state-distribution",
                "type": "table",
                "title": "Session State Distribution",
                "columns": ["session_state", "records", "total_bytes"],
                "rows": data["session_state_rows"],
            })

        if data["connection_outcome_rows"]:
            evidence.append({
                "evidence_id": "e-connection-outcome-distribution",
                "type": "table",
                "title": "Connection Outcome Distribution",
                "columns": ["action", "records", "total_bytes"],
                "rows": data["connection_outcome_rows"],
            })

        # Compute negative outcome percentage from connection outcomes
        neg_outcomes = sum(
            row[1] for row in data["connection_outcome_rows"]
            if str(row[0]).lower() in ("deny", "drop", "block", "reset", "reject")
        )
        total_outcomes = sum(row[1] for row in data["connection_outcome_rows"])
        neg_pct = round(neg_outcomes / max(total_outcomes, 1) * 100, 1) if total_outcomes else 0
        if neg_pct > 0:
            metrics.append({"name": "negative_outcome_pct", "value": neg_pct})

        if data["metrics"].get("candidate_sources") is not None:
            metrics.append({"name": "candidate_sources", "value": data["metrics"]["candidate_sources"]})
        if data["metrics"].get("high_risk_sources") is not None:
            metrics.append({"name": "high_risk_sources", "value": data["metrics"]["high_risk_sources"]})

        if metrics:
            evidence.append({
                "evidence_id": "e-session-risk-metrics",
                "type": "metric",
                "title": "Session Risk Metrics",
                "metrics": metrics,
            })

        if data["failure_heavy_rows"]:
            evidence.append({
                "evidence_id": "e-failure-heavy-sources",
                "type": "table",
                "title": "Potential Failure-Heavy Sources",
                "columns": ["src_ip", "flows", "negative_outcomes", "negative_pct", "total_bytes"],
                "rows": data["failure_heavy_rows"],
            })
            if neg_pct >= 50:
                findings.append({
                    "finding_id": "f-failure-heavy-source",
                    "type": "failure_heavy_source",
                    "severity": "medium",
                    "confidence": 0.7,
                    "title": f"High negative outcome rate: {neg_pct}%",
                    "description": (
                        f"{neg_pct}% of connection outcomes are negative (deny/drop/block/reset/reject). "
                        f"This may indicate misconfigured services, firewall blocks, or probing behavior."
                    ),
                    "evidence_refs": ["e-connection-outcome-distribution", "e-failure-heavy-sources"],
                })

        if data["short_low_byte_rows"]:
            evidence.append({
                "evidence_id": "e-short-low-byte-sources",
                "type": "table",
                "title": "Short and Low-Byte Connection Sources",
                "columns": ["src_ip", "flows", "short_low_byte_flows", "short_low_byte_pct"],
                "rows": data["short_low_byte_rows"],
            })

    evidence.append({
        "evidence_id": "e-raw-report",
        "type": "text",
        "title": "Raw Session Review Output",
        "content": raw_output,
    })

    cov = data.get("coverage", {})
    if view != "packet":
        if cov.get("has_session_state") is False:
            warnings.append({
                "code": "no_session_state",
                "message": "No session_state field available; session quality analysis is limited.",
                "severity": "info",
            })
        if cov.get("has_action") is False:
            warnings.append({
                "code": "no_action_field",
                "message": "No action field available; connection outcome analysis is unavailable.",
                "severity": "warning",
            })

    overview_text = f"Session review (view={view})."
    if view == "packet":
        overview_text += " TCP handshake and flag analysis."
    else:
        overview_text += f" {data['metrics'].get('candidate_sources', 0)} candidates, {data['metrics'].get('high_risk_sources', 0)} high-risk."

    return {
        "summary": {
            "title": "Session Review",
            "overview": overview_text,
            "severity": "high" if any(f.get("severity") == "high" for f in findings) else ("medium" if findings else "info"),
            "confidence": 0.75,
            "key_metrics": metrics[:3] if metrics else [],
        },
        "findings": findings,
        "evidence": evidence,
        "artifacts": [],
        "diagnostics": {
            "warnings": warnings,
            "data_quality": {"view": view, "coverage": cov},
        },
    }
