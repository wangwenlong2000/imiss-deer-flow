from __future__ import annotations

from typing import Any

import duckdb  # type: ignore

from analysis.anomaly_models import score_scan_candidates
from analysis.feature_engineering import rows_from_query, scan_candidate_sql
from utils.formatter import render_rows_section, render_section
from core.schema_mapping import available_canonical_fields, ensure_required


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


def execute_scan_review(
    con: duckdb.DuckDBPyConnection,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    view: str,
    limit: int,
) -> dict[str, Any]:
    """Execute scan-review analysis and return structured data dict."""
    available = available_canonical_fields(mappings)

    if view == "packet":
        ensure_required(mappings, ["src_ip", "dst_ip", "dst_port", "tcp_flags"])
        and_clause = _where_to_and(where_clause)
        packet_cols, packet_rows = rows_from_query(
            con,
            f"""
            SELECT src_ip,
                   COUNT(*) AS packets,
                   COUNT(DISTINCT dst_ip) AS unique_dst_ip,
                   COUNT(DISTINCT dst_port) AS unique_dst_port,
                   SUM(CASE WHEN COALESCE(tcp_flags, '') = 'S' THEN 1 ELSE 0 END) AS syn_only_packets,
                   SUM(COALESCE(bytes, 0)) AS total_bytes
            FROM flows
            WHERE 1=1 {and_clause} AND src_ip IS NOT NULL
            GROUP BY 1
            HAVING COUNT(DISTINCT dst_ip) >= 5
                OR COUNT(DISTINCT dst_port) >= 10
                OR SUM(CASE WHEN COALESCE(tcp_flags, '') = 'S' THEN 1 ELSE 0 END) >= 10
            ORDER BY syn_only_packets DESC, unique_dst_ip DESC, unique_dst_port DESC, packets DESC
            LIMIT {limit}
            """,
        )

        port_rows: list[dict[str, Any]] = []
        if "dst_port" in available:
            port_cols, port_rows = rows_from_query(
                con,
                f"""
                SELECT dst_port,
                       COUNT(*) AS packets,
                       COUNT(DISTINCT src_ip) AS unique_src_ip,
                       SUM(CASE WHEN COALESCE(tcp_flags, '') = 'S' THEN 1 ELSE 0 END) AS syn_only_packets
                FROM flows
                WHERE 1=1 {and_clause} AND dst_port IS NOT NULL
                GROUP BY 1
                ORDER BY packets DESC, unique_src_ip DESC, CAST(dst_port AS VARCHAR) ASC
                LIMIT {limit}
                """,
            )

        return {
            "view": "packet",
            "packet_sources": packet_rows,
            "packet_columns": packet_cols,
            "port_targets": port_rows,
            "scored_candidates": [],
            "flow_sources": [],
            "flow_columns": [],
            "rare_ports": [],
            "rare_port_columns": [],
            "_limit": limit,
            "_has_tcp_flags": "tcp_flags" in available,
        }

    # Flow view
    ensure_required(mappings, ["src_ip", "dst_ip", "dst_port"])
    and_clause = _where_to_and(where_clause)
    _, candidate_rows = rows_from_query(
        con,
        scan_candidate_sql(and_clause, packet_view=False, limit=max(limit * 20, 500)),
    )
    scored_candidates = score_scan_candidates(candidate_rows, packet_view=False)

    flow_cols, flow_rows = rows_from_query(
        con,
        f"""
        SELECT src_ip,
               COUNT(*) AS flows,
               COUNT(DISTINCT dst_ip) AS unique_dst_ip,
               COUNT(DISTINCT dst_port) AS unique_dst_port,
               SUM(COALESCE(bytes, 0)) AS total_bytes
        FROM flows
        WHERE 1=1 {and_clause} AND src_ip IS NOT NULL
        GROUP BY 1
        HAVING COUNT(DISTINCT dst_ip) >= 5 OR COUNT(DISTINCT dst_port) >= 10
        ORDER BY unique_dst_ip DESC, unique_dst_port DESC, flows DESC, total_bytes DESC
        LIMIT {limit}
        """,
    )

    rare_cols, rare_rows = rows_from_query(
        con,
        f"""
        SELECT dst_port, COUNT(*) AS records, SUM(COALESCE(bytes, 0)) AS total_bytes
        FROM flows
        WHERE 1=1 {and_clause} AND dst_port IS NOT NULL
        GROUP BY 1
        HAVING COUNT(*) <= 3
        ORDER BY records ASC, total_bytes DESC, CAST(dst_port AS VARCHAR) ASC
        LIMIT {limit}
        """,
    )

    return {
        "view": "flow",
        "scored_candidates": scored_candidates,
        "flow_sources": flow_rows,
        "flow_columns": flow_cols,
        "rare_ports": rare_rows,
        "rare_port_columns": rare_cols,
        "packet_sources": [],
        "packet_columns": [],
        "port_targets": [],
        "_limit": limit,
    }


def format_scan_review(data: dict[str, Any]) -> str:
    """Produce the text report for backward-compatible output."""
    sections: list[str] = []
    view = data["view"]
    sections.append(f"Analysis view: {view}")

    if view == "packet":
        packet_rows = data["packet_sources"]
        packet_cols = data["packet_columns"]
        if packet_rows:
            sections.append("Packet-level scan review\n" + _render_dict_rows(packet_cols, packet_rows))

        port_targets = data["port_targets"]
        if port_targets:
            port_cols = list(port_targets[0].keys()) if port_targets else []
            sections.append("Most targeted destination ports\n" + _render_dict_rows(port_cols, port_targets))
    else:
        scored = data["scored_candidates"]
        sections.append(
            render_rows_section(
                "Top scan-risk sources (hybrid scoring)",
                [
                    "src_ip",
                    "flows",
                    "unique_dst_ip",
                    "unique_dst_port",
                    "syn_only_pct",
                    "rst_pct",
                    "scan_risk_score",
                    "severity",
                    "likely_reason",
                ],
                [
                    (
                        row.get("src_ip"),
                        row.get("flows"),
                        row.get("unique_dst_ip"),
                        row.get("unique_dst_port"),
                        row.get("syn_only_pct"),
                        row.get("rst_pct"),
                        row.get("scan_risk_score"),
                        row.get("severity"),
                        row.get("likely_reason"),
                    )
                    for row in scored[:data.get("_limit", 10)]
                ],
            )
        )

        flow_rows = data["flow_sources"]
        flow_cols = data["flow_columns"]
        if flow_rows:
            sections.append("Flow-level scan review\n" + _render_dict_rows(flow_cols, flow_rows))

        high_risk = sum(1 for row in scored if float(row.get("scan_risk_score", 0.0)) >= 0.65)
        sections.append(
            "\n".join(
                [
                    "Scan-risk summary",
                    f"candidate_sources={len(scored)}, high_risk_sources={high_risk}",
                    "Hybrid scan scoring combines broad target coverage, broad port spread, TCP reset / SYN pressure, and source-level outlier scoring.",
                ]
            )
        )

        rare_rows = data["rare_ports"]
        rare_cols = data["rare_port_columns"]
        if rare_rows:
            sections.append("Rare destination port screening\n" + _render_dict_rows(rare_cols, rare_rows))

    return "\n\n".join(sections)


def _render_dict_rows(columns: list[str], rows: list[dict[str, Any]]) -> str:
    """Render dict rows as the same pipe-delimited table format."""
    from utils.formatter import format_rows
    tuples = [tuple(row.get(col) for col in columns) for row in rows]
    return format_rows(columns, tuples)


def build_skill_result_parts(data: dict[str, Any], raw_output: str) -> dict[str, Any]:
    """Build structured SkillResult for scan-review action."""
    evidence: list[dict[str, Any]] = []
    findings: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    metrics: list[dict[str, Any]] = []
    view = data["view"]
    limit = data.get("_limit", 10)

    if view == "packet":
        packet_rows = data["packet_sources"]
        if packet_rows:
            pkt_columns = ["src_ip", "packets", "unique_dst_ip", "unique_dst_port", "syn_only_packets", "total_bytes"]
            pkt_table_rows = [[r.get(c) for c in pkt_columns] for r in packet_rows]
            evidence.append({
                "evidence_id": "e-packet-scan-sources",
                "type": "table",
                "title": "Packet-Level Scan Sources",
                "columns": pkt_columns,
                "rows": pkt_table_rows,
            })

        port_targets = data["port_targets"]
        if port_targets:
            port_columns = ["dst_port", "packets", "unique_src_ip", "syn_only_packets"]
            port_table_rows = [[r.get(c) for c in port_columns] for r in port_targets]
            evidence.append({
                "evidence_id": "e-tcp-flag-scan-sources",
                "type": "table",
                "title": "Most Targeted Destination Ports",
                "columns": port_columns,
                "rows": port_table_rows,
            })

        if not data.get("_has_tcp_flags", True):
            warnings.append({
                "code": "scan_packet_fields_missing",
                "message": "Packet scan evidence is limited because tcp_flags or icmp fields are unavailable.",
                "severity": "info",
            })

    else:
        scored = data["scored_candidates"]

        # Scan risk sources table
        if scored:
            risk_columns = ["src_ip", "flows", "unique_dst_ip", "unique_dst_port", "total_bytes", "scan_risk_score", "severity", "likely_reason"]
            risk_table_rows = [
                [r.get("src_ip"), r.get("flows"), r.get("unique_dst_ip"), r.get("unique_dst_port"), r.get("total_bytes", 0), r.get("scan_risk_score"), r.get("severity"), r.get("likely_reason")]
                for r in scored[:limit]
            ]
            evidence.append({
                "evidence_id": "e-scan-risk-sources",
                "type": "table",
                "title": "Scan-Risk Sources (Hybrid Scoring)",
                "columns": risk_columns,
                "rows": risk_table_rows,
            })

            # Destination spread
            dst_spread_rows = sorted(scored, key=lambda r: (-int(r.get("unique_dst_ip", 0)), -int(r.get("flows", 0))))[:limit]
            dst_columns = ["src_ip", "unique_dst_ip", "flows", "total_bytes"]
            dst_table_rows = [[r.get("src_ip"), r.get("unique_dst_ip"), r.get("flows"), r.get("total_bytes", 0)] for r in dst_spread_rows]
            evidence.append({
                "evidence_id": "e-destination-spread-sources",
                "type": "table",
                "title": "Destination Spread by Source",
                "columns": dst_columns,
                "rows": dst_table_rows,
            })

            # Port spread
            port_spread_rows = sorted(scored, key=lambda r: (-int(r.get("unique_dst_port", 0)), -int(r.get("flows", 0))))[:limit]
            port_columns = ["src_ip", "unique_dst_port", "flows", "total_bytes"]
            port_table_rows = [[r.get("src_ip"), r.get("unique_dst_port"), r.get("flows"), r.get("total_bytes", 0)] for r in port_spread_rows]
            evidence.append({
                "evidence_id": "e-port-spread-sources",
                "type": "table",
                "title": "Port Spread by Source",
                "columns": port_columns,
                "rows": port_table_rows,
            })

            # Findings from scored candidates
            for row in scored[:limit]:
                score = float(row.get("scan_risk_score", 0.0))
                if score >= 0.65:
                    severity = str(row.get("severity") or "medium").lower()
                    findings.append({
                        "finding_id": f"f-scan-risk-{row.get('src_ip', 'unknown')}",
                        "type": "scan_risk_source",
                        "severity": severity,
                        "confidence": score,
                        "title": f"Scan-risk source: {row.get('src_ip')}",
                        "description": row.get("likely_reason") or "Source flagged by scan-risk analysis.",
                        "entities": [{"type": "src_ip", "value": row.get("src_ip")}],
                        "evidence_refs": ["e-scan-risk-sources", "e-scan-metrics"],
                    })

            # Broad destination spread finding
            max_dst = max((int(r.get("unique_dst_ip", 0)) for r in scored), default=0)
            if max_dst >= 20:
                findings.append({
                    "finding_id": "f-scan-broad-dst",
                    "type": "broad_destination_spread",
                    "severity": "medium",
                    "confidence": 0.6,
                    "title": f"Broad destination spread detected: {max_dst} unique destinations",
                    "description": f"At least one source communicated with {max_dst} unique destination IPs. This may indicate scanning, discovery, or lateral movement.",
                    "entities": [{"type": "metric", "value": f"max_unique_dst_ip={max_dst}"}],
                    "evidence_refs": ["e-destination-spread-sources", "e-scan-metrics"],
                })

            # Broad port spread finding
            max_ports = max((int(r.get("unique_dst_port", 0)) for r in scored), default=0)
            if max_ports >= 20:
                findings.append({
                    "finding_id": "f-scan-broad-port",
                    "type": "broad_port_spread",
                    "severity": "medium",
                    "confidence": 0.6,
                    "title": f"Broad port spread detected: {max_ports} unique ports",
                    "description": f"At least one source communicated with {max_ports} unique destination ports. This may indicate port scanning or service enumeration.",
                    "entities": [{"type": "metric", "value": f"max_unique_dst_port={max_ports}"}],
                    "evidence_refs": ["e-port-spread-sources", "e-scan-metrics"],
                })

        # Scan metrics
        high_risk = sum(1 for r in scored if float(r.get("scan_risk_score", 0.0)) >= 0.65)
        max_dst = max((int(r.get("unique_dst_ip", 0)) for r in scored), default=0)
        max_ports = max((int(r.get("unique_dst_port", 0)) for r in scored), default=0)
        metrics.append({"name": "candidate_sources", "value": len(scored)})
        metrics.append({"name": "high_risk_sources", "value": high_risk})
        metrics.append({"name": "max_unique_dst_ip", "value": max_dst})
        metrics.append({"name": "max_unique_dst_port", "value": max_ports})

        evidence.append({
            "evidence_id": "e-scan-metrics",
            "type": "metric",
            "title": "Scan Review Metrics",
            "metrics": metrics,
        })

        # Flow sources table
        flow_rows = data["flow_sources"]
        flow_cols = data["flow_columns"]
        if flow_rows:
            flow_table_rows = [[r.get(c) for c in flow_cols] for r in flow_rows[:limit]]
            evidence.append({
                "evidence_id": "e-flow-scan-sources",
                "type": "table",
                "title": "Flow-Level Scan Sources",
                "columns": flow_cols,
                "rows": flow_table_rows,
            })

        # Rare ports table
        rare_rows = data["rare_ports"]
        rare_cols = data["rare_port_columns"]
        if rare_rows:
            rare_table_rows = [[r.get(c) for c in rare_cols] for r in rare_rows[:limit]]
            evidence.append({
                "evidence_id": "e-rare-port-screening",
                "type": "table",
                "title": "Rare Destination Port Screening",
                "columns": rare_cols,
                "rows": rare_table_rows,
            })

    # Raw report
    evidence.append({
        "evidence_id": "e-raw-report",
        "type": "text",
        "title": "Raw Scan Review Output",
        "content": raw_output,
    })

    # Fix evidence_refs
    existing_ids = {e["evidence_id"] for e in evidence}
    for finding in findings:
        finding["evidence_refs"] = [ref for ref in finding["evidence_refs"] if ref in existing_ids]

    # Compute severity
    severity_order = {"critical": 4, "high": 3, "medium": 2, "low": 1, "info": 0}
    highest = "info"
    for f in findings:
        if severity_order.get(f.get("severity", "info"), 0) > severity_order.get(highest, 0):
            highest = f["severity"]

    overview_text = f"Scan review (view: {view})."
    if findings:
        overview_text += f" {len(findings)} finding(s) identified."

    return {
        "summary": {
            "title": "Scan Review",
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
            "data_quality": {
                "view": view,
            },
        },
    }


def scan_review_action(
    con: duckdb.DuckDBPyConnection,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    view: str,
    limit: int,
) -> str:
    data = execute_scan_review(con, mappings, where_clause, view, limit)
    return format_scan_review(data)
