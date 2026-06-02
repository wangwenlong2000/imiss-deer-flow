from __future__ import annotations

from typing import Any

import duckdb  # type: ignore

from analysis.feature_engineering import icmp_probe_candidate_sql, rows_from_query
from utils.formatter import render_rows_section, render_section
from core.schema_mapping import available_canonical_fields, ensure_required
from utils.sql import quote_identifier


def execute_packet_review(
    con: duckdb.DuckDBPyConnection,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    limit: int,
) -> dict[str, Any]:
    """Execute packet review and return structured data."""
    available = available_canonical_fields(mappings)

    result: dict[str, Any] = {
        "handshake_summary": [],
        "packet_protocol_rows": [],
        "tcp_flag_rows": [],
        "handshake_anomaly_rows": [],
        "packet_size_rows": [],
        "icmp_rows": [],
        "top_talker_rows": [],
        "metrics": {},
        "coverage": {},
    }

    if "tcp_flags" in available:
        row = con.execute(
            f"""
            WITH tcp_packets AS (
                SELECT *
                FROM flows
                {where_clause}
                {"AND" if where_clause else "WHERE"} protocol = 'TCP'
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
                {
                    "tcp_packets": row[0] or 0,
                    "syn_ack_packets": row[1] or 0,
                    "syn_only_packets": row[2] or 0,
                    "syn_only_pct": row[3] or 0.0,
                    "rst_packets": row[4] or 0,
                    "rst_pct": row[5] or 0.0,
                }
            ]
        result["coverage"]["has_tcp_flags"] = True

    if "protocol" in available:
        rows = con.execute(
            f"""
            SELECT COALESCE(protocol, 'UNKNOWN') AS protocol,
                   COUNT(*) AS packets,
                   SUM(COALESCE(bytes, 0)) AS total_bytes
            FROM flows
            {where_clause}
            GROUP BY 1
            ORDER BY packets DESC, total_bytes DESC, protocol ASC
            LIMIT {limit}
            """
        ).fetchall()
        result["packet_protocol_rows"] = [list(r) for r in rows]
        result["coverage"]["has_protocol"] = True

    if "tcp_flags" in available:
        rows = con.execute(
            f"""
            SELECT COALESCE(tcp_flags, 'UNKNOWN') AS tcp_flags,
                   COUNT(*) AS packets,
                   COUNT(DISTINCT src_ip) AS unique_src_ip,
                   COUNT(DISTINCT dst_ip) AS unique_dst_ip
            FROM flows
            {where_clause}
            GROUP BY 1
            ORDER BY packets DESC, tcp_flags ASC
            LIMIT {limit}
            """
        ).fetchall()
        result["tcp_flag_rows"] = [list(r) for r in rows]

        if {"src_ip", "dst_ip", "dst_port"}.issubset(available):
            rows = con.execute(
                f"""
                SELECT src_ip,
                       COUNT(*) AS packets,
                       SUM(CASE WHEN COALESCE(tcp_flags, '') = 'S' THEN 1 ELSE 0 END) AS syn_only_packets,
                       SUM(CASE WHEN COALESCE(tcp_flags, '') LIKE '%R%' THEN 1 ELSE 0 END) AS rst_packets,
                       COUNT(DISTINCT dst_ip) AS unique_dst_ip,
                       COUNT(DISTINCT dst_port) AS unique_dst_port
                FROM flows
                {where_clause}
                WHERE src_ip IS NOT NULL
                GROUP BY 1
                HAVING syn_only_packets > 0 OR rst_packets > 0
                ORDER BY syn_only_packets DESC, rst_packets DESC, unique_dst_ip DESC, unique_dst_port DESC
                LIMIT {limit}
                """
            ).fetchall()
            result["handshake_anomaly_rows"] = [list(r) for r in rows]

    if "frame_len" in available or "payload_bytes" in available:
        size_expr = "COALESCE(payload_bytes, frame_len, bytes, 0)"
        rows = con.execute(
            f"""
            SELECT
                CASE
                    WHEN {size_expr} < 64 THEN '<64'
                    WHEN {size_expr} < 128 THEN '64-127'
                    WHEN {size_expr} < 512 THEN '128-511'
                    WHEN {size_expr} < 1500 THEN '512-1499'
                    ELSE '1500+'
                END AS size_band,
                COUNT(*) AS packets,
                SUM(COALESCE(bytes, 0)) AS total_bytes
            FROM flows
            {where_clause}
            GROUP BY 1
            ORDER BY packets DESC, size_band ASC
            """
        ).fetchall()
        result["packet_size_rows"] = [list(r) for r in rows]
        result["coverage"]["has_packet_size"] = True

    if "icmp_type" in available:
        rows = con.execute(
            f"""
            SELECT COALESCE(CAST(icmp_type AS VARCHAR), 'UNKNOWN') AS icmp_type,
                   COALESCE(CAST(icmp_code AS VARCHAR), 'UNKNOWN') AS icmp_code,
                   COUNT(*) AS packets
            FROM flows
            {where_clause}
            WHERE icmp_type IS NOT NULL
            GROUP BY 1, 2
            ORDER BY packets DESC, icmp_type ASC, icmp_code ASC
            LIMIT {limit}
            """
        ).fetchall()
        result["icmp_rows"] = [list(r) for r in rows]
        result["coverage"]["has_icmp"] = True

    if {"src_ip", "dst_ip"}.issubset(available):
        rows = con.execute(
            f"""
            SELECT src_ip,
                   COUNT(*) AS packets,
                   COUNT(DISTINCT dst_ip) AS unique_dst_ip,
                   SUM(COALESCE(bytes, 0)) AS total_bytes
            FROM flows
            {where_clause}
            WHERE src_ip IS NOT NULL
            GROUP BY 1
            ORDER BY packets DESC, total_bytes DESC, src_ip ASC
            LIMIT {limit}
            """
        ).fetchall()
        result["top_talker_rows"] = [list(r) for r in rows]
        result["coverage"]["has_top_talkers"] = True

    return result


def format_packet_review(data: dict[str, Any]) -> str:
    """Format structured packet review data as human-readable text."""
    sections = ["Analysis view: packet"]

    if data["handshake_summary"]:
        s = data["handshake_summary"][0]
        sections.append(
            "\n".join([
                "Handshake and reset posture",
                f"tcp_packets={s['tcp_packets']}, syn_ack={s['syn_ack_packets']}, "
                f"syn_only={s['syn_only_packets']} ({s['syn_only_pct']}%), "
                f"rst={s['rst_packets']} ({s['rst_pct']}%)",
            ])
        )

    if data["packet_protocol_rows"]:
        sections.append(render_rows_section("Packet protocol mix", ["protocol", "packets", "total_bytes"], data["packet_protocol_rows"]))

    if data["tcp_flag_rows"]:
        sections.append(render_rows_section("TCP flags distribution", ["tcp_flags", "packets", "unique_src_ip", "unique_dst_ip"], data["tcp_flag_rows"]))

    if data["handshake_anomaly_rows"]:
        sections.append(render_rows_section("Handshake-anomaly sample sources", ["src_ip", "packets", "syn_only_packets", "rst_packets", "unique_dst_ip", "unique_dst_port"], data["handshake_anomaly_rows"]))

    if data["packet_size_rows"]:
        sections.append(render_rows_section("Packet size profile", ["size_band", "packets", "total_bytes"], data["packet_size_rows"]))

    if data["icmp_rows"]:
        sections.append(render_rows_section("ICMP activity review", ["icmp_type", "icmp_code", "packets"], data["icmp_rows"]))

    if data["top_talker_rows"]:
        sections.append(render_rows_section("Top packet talkers", ["src_ip", "packets", "unique_dst_ip", "total_bytes"], data["top_talker_rows"]))

    return "\n\n".join(sections)


def packet_review_action(
    con: duckdb.DuckDBPyConnection,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    limit: int,
) -> str:
    """Legacy entry point — delegates to execute + format for backward compatibility."""
    data = execute_packet_review(con, mappings, where_clause, limit)
    return format_packet_review(data)


def build_skill_result_parts(data: dict[str, Any], raw_output: str) -> dict[str, Any]:
    """Build structured SkillResult for packet-review action."""
    findings: list[dict[str, Any]] = []
    evidence: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    metrics: list[dict[str, Any]] = []

    if data["handshake_summary"]:
        s = data["handshake_summary"][0]
        tcp_total = s["tcp_packets"]
        metrics.extend([
            {"name": "tcp_packets", "value": tcp_total},
            {"name": "syn_ack_packets", "value": s["syn_ack_packets"]},
            {"name": "syn_only_packets", "value": s["syn_only_packets"]},
            {"name": "syn_only_pct", "value": s["syn_only_pct"]},
            {"name": "rst_packets", "value": s["rst_packets"]},
            {"name": "rst_pct", "value": s["rst_pct"]},
        ])

        evidence.append({
            "evidence_id": "e-handshake-posture",
            "type": "metric",
            "title": "TCP Handshake Posture",
            "metrics": metrics,
        })

        if tcp_total == 0:
            warnings.append({
                "code": "no_tcp_handshake_evidence",
                "message": "No TCP packets with populated tcp_flags were available for handshake posture analysis.",
                "severity": "info",
            })

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
                "evidence_refs": ["e-handshake-posture", "e-tcp-flags-distribution"],
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
                "evidence_refs": ["e-handshake-posture", "e-tcp-flags-distribution"],
            })

    if data["packet_protocol_rows"]:
        evidence.append({
            "evidence_id": "e-packet-protocol-mix",
            "type": "table",
            "title": "Packet Protocol Mix",
            "columns": ["protocol", "packets", "total_bytes"],
            "rows": data["packet_protocol_rows"],
        })

    if data["tcp_flag_rows"]:
        evidence.append({
            "evidence_id": "e-tcp-flags-distribution",
            "type": "table",
            "title": "TCP Flags Distribution",
            "columns": ["tcp_flags", "packets", "unique_src_ip", "unique_dst_ip"],
            "rows": data["tcp_flag_rows"],
        })

    if data["handshake_anomaly_rows"]:
        evidence.append({
            "evidence_id": "e-handshake-anomaly-sources",
            "type": "table",
            "title": "Handshake Anomaly Sources",
            "columns": ["src_ip", "packets", "syn_only_packets", "rst_packets", "unique_dst_ip", "unique_dst_port"],
            "rows": data["handshake_anomaly_rows"],
        })

    if data["packet_size_rows"]:
        evidence.append({
            "evidence_id": "e-packet-size-profile",
            "type": "table",
            "title": "Packet Size Profile",
            "columns": ["size_band", "packets", "total_bytes"],
            "rows": data["packet_size_rows"],
        })

    if data["icmp_rows"]:
        evidence.append({
            "evidence_id": "e-icmp-activity",
            "type": "table",
            "title": "ICMP Activity Review",
            "columns": ["icmp_type", "icmp_code", "packets"],
            "rows": data["icmp_rows"],
        })

    if data["top_talker_rows"]:
        evidence.append({
            "evidence_id": "e-top-packet-talkers",
            "type": "table",
            "title": "Top Packet Talkers",
            "columns": ["src_ip", "packets", "unique_dst_ip", "total_bytes"],
            "rows": data["top_talker_rows"],
        })

    evidence.append({
        "evidence_id": "e-raw-report",
        "type": "text",
        "title": "Raw Packet Review Output",
        "content": raw_output,
    })

    overview_text = "Packet review."
    if data["handshake_summary"]:
        s = data["handshake_summary"][0]
        overview_text += f" {s['tcp_packets']} TCP packets."

    return {
        "summary": {
            "title": "Packet Review",
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
            "data_quality": {"coverage": data.get("coverage", {})},
        },
    }
