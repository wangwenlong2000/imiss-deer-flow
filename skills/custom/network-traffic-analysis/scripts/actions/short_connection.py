from __future__ import annotations

from typing import Any

import duckdb  # type: ignore

from analysis.anomaly_models import score_short_connection_candidates
from analysis.feature_engineering import rows_from_query, short_connection_candidate_sql, source_microflow_summary_sql
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


def execute_short_connection_review(
    con: duckdb.DuckDBPyConnection,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    limit: int,
) -> dict[str, Any]:
    """Execute short-connection-review analysis and return structured data dict."""
    available = available_canonical_fields(mappings)
    duration_expr = "COALESCE(duration_ms, flow_duration, 0)" if "duration_ms" in available else "COALESCE(flow_duration, 0)"
    bytes_expr = "COALESCE(bytes, 0)"
    packets_expr = "COALESCE(packets, 0)"

    ensure_required(mappings, ["src_ip", "dst_ip", "dst_port", "protocol", "bytes", "packets"])
    if "duration_ms" not in available and "flow_duration" not in available:
        raise ValueError("short-connection-review requires duration_ms or flow_duration in the resolved flow view.")

    and_clause = _where_to_and(where_clause)

    # Quality metrics
    quality_sql = f"""
        WITH scoped AS (
            SELECT *
            FROM flows
            WHERE 1=1 {and_clause}
        )
        SELECT
            COUNT(*) AS total_flows,
            SUM(CASE WHEN {duration_expr} = 0 THEN 1 ELSE 0 END) AS zero_duration_flows,
            SUM(CASE WHEN {duration_expr} <= 1 THEN 1 ELSE 0 END) AS tiny_duration_flows,
            SUM(CASE WHEN {packets_expr} = 1 THEN 1 ELSE 0 END) AS single_packet_flows,
            SUM(CASE WHEN {packets_expr} <= 2 THEN 1 ELSE 0 END) AS two_packet_or_less_flows,
            SUM(CASE WHEN {bytes_expr} <= 300 THEN 1 ELSE 0 END) AS tiny_byte_flows
        FROM scoped
    """
    (
        total_flows,
        zero_duration_flows,
        tiny_duration_flows,
        single_packet_flows,
        two_packet_or_less_flows,
        tiny_byte_flows,
    ) = con.execute(quality_sql).fetchone()
    total_flows = int(total_flows or 0)
    zero_duration_flows = int(zero_duration_flows or 0)
    tiny_duration_flows = int(tiny_duration_flows or 0)
    single_packet_flows = int(single_packet_flows or 0)
    two_packet_or_less_flows = int(two_packet_or_less_flows or 0)
    tiny_byte_flows = int(tiny_byte_flows or 0)

    # Dominant pattern
    dominant_pattern_sql = f"""
        WITH scoped AS (
            SELECT *
            FROM flows
            WHERE 1=1 {and_clause}
        )
        SELECT
            COALESCE(app_protocol, '') AS app_protocol,
            COALESCE(service, '') AS service,
            COALESCE(CAST(dst_port AS VARCHAR), '') AS dst_port,
            COALESCE(session_state, '') AS session_state,
            {packets_expr} AS packets,
            COUNT(*) AS flows,
            ROUND(AVG({bytes_expr}), 2) AS avg_bytes,
            ROUND(AVG({duration_expr}), 2) AS avg_duration_ms
        FROM scoped
        WHERE {duration_expr} <= 10
        GROUP BY 1, 2, 3, 4, 5
        ORDER BY flows DESC, avg_bytes DESC, dst_port ASC
        LIMIT 1
    """
    dominant_pattern = con.execute(dominant_pattern_sql).fetchone()

    # Wide summary
    wide_summary_sql = f"""
        WITH scoped AS (
            SELECT *
            FROM flows
            WHERE 1=1 {and_clause}
        )
        SELECT
            COUNT(*) AS total_flows,
            SUM(CASE WHEN {duration_expr} <= 10 THEN 1 ELSE 0 END) AS short_flows_10ms,
            ROUND(SUM(CASE WHEN {duration_expr} <= 10 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS short_flows_10ms_pct,
            SUM(CASE WHEN {duration_expr} <= 10 AND {bytes_expr} <= 300 AND {packets_expr} <= 2 THEN 1 ELSE 0 END) AS compact_short_flows,
            ROUND(SUM(CASE WHEN {duration_expr} <= 10 AND {bytes_expr} <= 300 AND {packets_expr} <= 2 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS compact_short_flows_pct
        FROM scoped
    """
    (
        total_flows_summary,
        short_flows_10ms,
        short_flows_10ms_pct,
        compact_short_flows,
        compact_short_flows_pct,
    ) = con.execute(wide_summary_sql).fetchone()
    total_flows_summary = int(total_flows_summary or 0)
    short_flows_10ms = int(short_flows_10ms or 0)
    short_flows_10ms_pct = float(short_flows_10ms_pct or 0.0)
    compact_short_flows = int(compact_short_flows or 0)
    compact_short_flows_pct = float(compact_short_flows_pct or 0.0)

    # Scored candidates
    _, candidate_rows = rows_from_query(
        con,
        short_connection_candidate_sql(and_clause, available, candidate_limit=max(limit * 50, 500)),
    )
    scored_rows = score_short_connection_candidates(candidate_rows)

    # Source aggregation
    source_scores: dict[str, dict[str, Any]] = {}
    for row in scored_rows:
        src_ip = str(row.get("src_ip") or "UNKNOWN")
        entry = source_scores.setdefault(
            src_ip,
            {
                "src_ip": src_ip,
                "candidate_flows": 0,
                "max_anomaly_score": 0.0,
                "avg_anomaly_score": 0.0,
                "high_risk_flows": 0,
                "compact_short_flows": 0,
                "unique_dst_ip_hint": row.get("src_unique_dst_ip", 0),
                "unique_dst_port_hint": row.get("src_unique_dst_port", 0),
            },
        )
        entry["candidate_flows"] += 1
        score = float(row.get("anomaly_score", 0.0))
        entry["avg_anomaly_score"] += score
        entry["max_anomaly_score"] = max(entry["max_anomaly_score"], score)
        if score >= 0.65:
            entry["high_risk_flows"] += 1
        if float(row.get("duration_ms", 0.0)) <= 10 and float(row.get("bytes", 0.0)) <= 300 and float(row.get("packets", 0.0)) <= 2:
            entry["compact_short_flows"] += 1

    ranked_sources = []
    for entry in source_scores.values():
        flow_count = max(int(entry["candidate_flows"]), 1)
        entry["avg_anomaly_score"] = round(float(entry["avg_anomaly_score"]) / flow_count, 4)
        ranked_sources.append(entry)
    ranked_sources.sort(
        key=lambda item: (
            float(item["max_anomaly_score"]),
            float(item["avg_anomaly_score"]),
            int(item["high_risk_flows"]),
            int(item["candidate_flows"]),
        ),
        reverse=True,
    )

    # Additional SQL sections
    src_columns, src_rows = rows_from_query(
        con,
        source_microflow_summary_sql(and_clause, available, limit=limit),
    )

    port_columns, port_rows = rows_from_query(
        con,
        f"""
        SELECT dst_port,
               COUNT(*) AS short_flows_10ms,
               SUM(CASE WHEN {duration_expr} <= 10 AND {bytes_expr} <= 300 AND {packets_expr} <= 2 THEN 1 ELSE 0 END) AS compact_short_flows,
               SUM({bytes_expr}) AS total_bytes
        FROM flows
        WHERE 1=1 {and_clause} AND dst_port IS NOT NULL
          AND {duration_expr} <= 10
        GROUP BY 1
        ORDER BY short_flows_10ms DESC, compact_short_flows DESC, total_bytes DESC, CAST(dst_port AS VARCHAR) ASC
        LIMIT {limit}
        """,
    )

    mix_columns, mix_rows = rows_from_query(
        con,
        f"""
        SELECT COALESCE(protocol, 'UNKNOWN') AS protocol,
               COALESCE(app_protocol, 'UNKNOWN') AS app_protocol,
               COALESCE(session_state, 'UNKNOWN') AS session_state,
               COUNT(*) AS short_flows_10ms,
               SUM(CASE WHEN {duration_expr} <= 10 AND {bytes_expr} <= 300 AND {packets_expr} <= 2 THEN 1 ELSE 0 END) AS compact_short_flows
        FROM flows
        WHERE 1=1 {and_clause}
          AND {duration_expr} <= 10
        GROUP BY 1, 2, 3
        ORDER BY short_flows_10ms DESC, compact_short_flows DESC, protocol ASC, app_protocol ASC, session_state ASC
        LIMIT {limit}
        """,
    )

    sample_columns, sample_rows = rows_from_query(
        con,
        f"""
        SELECT src_ip,
               dst_ip,
               dst_port,
               protocol,
               COALESCE(app_protocol, 'UNKNOWN') AS app_protocol,
               {bytes_expr} AS bytes,
               {packets_expr} AS packets,
               {duration_expr} AS duration_ms,
               COALESCE(session_state, 'UNKNOWN') AS session_state
        FROM flows
        WHERE 1=1 {and_clause}
          AND {duration_expr} <= 10
          AND {bytes_expr} <= 300
          AND {packets_expr} <= 2
        ORDER BY duration_ms ASC, bytes ASC, packets ASC
        LIMIT {limit}
        """,
    )

    # Risk judgement
    high_risk_candidate_count = sum(1 for row in scored_rows if float(row.get("anomaly_score", 0.0)) >= 0.65)
    critical_candidate_count = sum(1 for row in scored_rows if float(row.get("anomaly_score", 0.0)) >= 0.85)

    risk_lines: list[str] = []
    if dominant_pattern:
        app_protocol = (dominant_pattern[0] or "").upper()
        service = (dominant_pattern[1] or "").lower()
        dst_port = str(dominant_pattern[2] or "")
        session_state = (dominant_pattern[3] or "").upper()
        packets = int(dominant_pattern[4] or 0)
        tls_like = app_protocol in {"TLS", "SSL", "SSLV3"} or service in {"https", "tls"} or dst_port == "443"
        if tls_like and session_state == "ACK" and packets <= 2 and short_flows_10ms_pct >= 90.0:
            risk_lines.append(
                "The dominant short-flow pattern looks like TLS/443 micro-transactions or handshake fragments rather than scan-like or failed-connection behavior."
            )
            risk_lines.append(
                "High short-flow share is expected for this capture shape and should not be labeled anomalous without additional evidence such as failed states, broad port spread, or strong source outliers."
            )
        elif compact_short_flows_pct >= 70.0:
            risk_lines.append(
                "Compact short flows dominate this dataset. Review state/port mix carefully before treating them as suspicious."
            )
            if high_risk_candidate_count > 0:
                risk_lines.append(
                    "The new hybrid scorer still found a concentrated subset of high-risk flows, so use the anomaly-ranked rows above before dismissing the capture as benign."
                )
        else:
            risk_lines.append(
                "Short flows are present, but they are not dominant enough to justify a direct anomaly conclusion from duration/size statistics alone."
            )
    else:
        risk_lines.append("Unable to identify a dominant short-flow pattern.")

    return {
        "total_flows": total_flows,
        "zero_duration_flows": zero_duration_flows,
        "tiny_duration_flows": tiny_duration_flows,
        "single_packet_flows": single_packet_flows,
        "two_packet_or_less_flows": two_packet_or_less_flows,
        "tiny_byte_flows": tiny_byte_flows,
        "dominant_pattern": dominant_pattern,
        "total_flows_summary": total_flows_summary,
        "short_flows_10ms": short_flows_10ms,
        "short_flows_10ms_pct": short_flows_10ms_pct,
        "compact_short_flows": compact_short_flows,
        "compact_short_flows_pct": compact_short_flows_pct,
        "scored_rows": scored_rows,
        "ranked_sources": ranked_sources,
        "src_columns": src_columns,
        "src_rows": src_rows,
        "port_columns": port_columns,
        "port_rows": port_rows,
        "mix_columns": mix_columns,
        "mix_rows": mix_rows,
        "sample_columns": sample_columns,
        "sample_rows": sample_rows,
        "high_risk_candidate_count": high_risk_candidate_count,
        "critical_candidate_count": critical_candidate_count,
        "risk_lines": risk_lines,
        "_limit": limit,
    }


def format_short_connection_review(data: dict[str, Any]) -> str:
    """Produce the text report for backward-compatible output."""
    sections: list[str] = ["Analysis view: flow"]

    total_flows = data["total_flows"]
    zero_duration_flows = data["zero_duration_flows"]
    tiny_duration_flows = data["tiny_duration_flows"]
    single_packet_flows = data["single_packet_flows"]
    two_packet_or_less_flows = data["two_packet_or_less_flows"]
    tiny_byte_flows = data["tiny_byte_flows"]

    if total_flows > 0:
        zero_duration_pct = round(zero_duration_flows * 100.0 / total_flows, 2)
        tiny_duration_pct = round(tiny_duration_flows * 100.0 / total_flows, 2)
        single_packet_pct = round(single_packet_flows * 100.0 / total_flows, 2)
        two_packet_or_less_pct = round(two_packet_or_less_flows * 100.0 / total_flows, 2)
        tiny_byte_pct = round(tiny_byte_flows * 100.0 / total_flows, 2)
        quality_lines = [
            "Duration quality and short-flow measurement context",
            f"zero_duration_pct={zero_duration_pct}%, duration_le_1ms_pct={tiny_duration_pct}%, single_packet_pct={single_packet_pct}%, packets_le_2_pct={two_packet_or_less_pct}%, bytes_le_300_pct={tiny_byte_pct}%",
        ]
        if tiny_duration_pct >= 95.0 and two_packet_or_less_pct >= 95.0:
            quality_lines.extend(
                [
                    "This dataset is dominated by sub-10ms microflows, so any duration-only short-connection metric will saturate easily.",
                    "Treat short-flow percentages here as traffic-shape description, not direct evidence of abnormal behavior.",
                ]
            )
        elif zero_duration_pct >= 95.0 and single_packet_pct >= 95.0:
            quality_lines.extend(
                [
                    "This dataset behaves like a degenerate flow export: almost every record is a single-packet flow with zero recorded duration.",
                    "Use protocol-review, overview-report, and packet/session evidence before drawing anomaly conclusions.",
                ]
            )
        sections.append("\n".join(quality_lines))

    dominant_pattern = data["dominant_pattern"]
    if dominant_pattern:
        app_protocol, service, dst_port, session_state, packets, flows, avg_bytes, avg_duration_ms = dominant_pattern
        sections.append(
            "\n".join(
                [
                    "Dominant short-flow pattern",
                    f"app_protocol={app_protocol or 'UNKNOWN'}, service={service or 'UNKNOWN'}, dst_port={dst_port or 'UNKNOWN'}, session_state={session_state or 'UNKNOWN'}, packets={packets}, flows={flows}, avg_bytes={avg_bytes}, avg_duration_ms={avg_duration_ms}",
                    "This pattern is more useful than a single short-flow percentage when judging whether the traffic looks like normal TLS/P2P micro-transactions or suspicious failed connections.",
                ]
            )
        )

    total_flows_summary = data["total_flows_summary"]
    short_flows_10ms = data["short_flows_10ms"]
    short_flows_10ms_pct = data["short_flows_10ms_pct"]
    compact_short_flows = data["compact_short_flows"]
    compact_short_flows_pct = data["compact_short_flows_pct"]

    sections.append(
        "\n".join(
            [
                "Short-flow summary",
                f"total_flows={total_flows_summary}, duration_le_10ms_flows={short_flows_10ms} ({short_flows_10ms_pct}%), compact_short_flows={compact_short_flows} ({compact_short_flows_pct}%)",
                "duration<=10ms is used here as a descriptive microflow window; compact_short_flows additionally require bytes<=300 and packets<=2.",
            ]
        )
    )

    scored_rows = data["scored_rows"]
    limit = data["_limit"]

    top_scored_rows = [
        (
            row.get("src_ip"),
            row.get("dst_ip"),
            row.get("dst_port"),
            row.get("protocol"),
            row.get("app_protocol"),
            row.get("bytes"),
            row.get("packets"),
            row.get("duration_ms"),
            row.get("session_state"),
            row.get("anomaly_score"),
            row.get("severity"),
            row.get("likely_reason"),
        )
        for row in scored_rows[:limit]
    ]
    sections.append(
        render_rows_section(
            "Top anomalous short-flow candidates (hybrid scoring)",
            [
                "src_ip",
                "dst_ip",
                "dst_port",
                "protocol",
                "app_protocol",
                "bytes",
                "packets",
                "duration_ms",
                "session_state",
                "anomaly_score",
                "severity",
                "likely_reason",
            ],
            top_scored_rows,
        )
    )

    ranked_sources = data["ranked_sources"]
    sections.append(
        render_rows_section(
            "Highest-risk microflow sources",
            [
                "src_ip",
                "candidate_flows",
                "high_risk_flows",
                "compact_short_flows",
                "avg_anomaly_score",
                "max_anomaly_score",
                "unique_dst_ip_hint",
                "unique_dst_port_hint",
            ],
            [
                (
                    item["src_ip"],
                    item["candidate_flows"],
                    item["high_risk_flows"],
                    item["compact_short_flows"],
                    item["avg_anomaly_score"],
                    round(float(item["max_anomaly_score"]), 4),
                    item["unique_dst_ip_hint"],
                    item["unique_dst_port_hint"],
                )
                for item in ranked_sources[:limit]
            ],
        )
    )

    if data["src_rows"]:
        sections.append(render_rows_section(
            "Source-level compact short-flow baseline",
            data["src_columns"],
            [tuple(r.get(c) for c in data["src_columns"]) for r in data["src_rows"]],
        ))

    if data["port_rows"]:
        sections.append(render_rows_section(
            "Top microflow destination ports",
            data["port_columns"],
            [tuple(r.get(c) for c in data["port_columns"]) for r in data["port_rows"]],
        ))

    if data["mix_rows"]:
        sections.append(render_rows_section(
            "Microflow state and protocol mix",
            data["mix_columns"],
            [tuple(r.get(c) for c in data["mix_columns"]) for r in data["mix_rows"]],
        ))

    if data["sample_rows"]:
        sections.append(render_rows_section(
            "Representative compact microflow samples",
            data["sample_columns"],
            [tuple(r.get(c) for c in data["sample_columns"]) for r in data["sample_rows"]],
        ))

    if total_flows_summary > 0:
        risk_lines = ["Risk judgement"]
        high_risk_candidate_count = data["high_risk_candidate_count"]
        critical_candidate_count = data["critical_candidate_count"]
        risk_lines.append(
            f"hybrid_model_candidates={len(scored_rows)}, high_risk_candidates={high_risk_candidate_count}, critical_candidates={critical_candidate_count}"
        )
        risk_lines.extend(data["risk_lines"])
        sections.append("\n".join(risk_lines))

    return "\n\n".join(sections)


def build_skill_result_parts(data: dict[str, Any], raw_output: str) -> dict[str, Any]:
    """Build structured SkillResult for short-connection-review action."""
    evidence: list[dict[str, Any]] = []
    findings: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    metrics: list[dict[str, Any]] = []

    limit = data["_limit"]
    scored_rows = data["scored_rows"]
    ranked_sources = data["ranked_sources"]

    # Summary metrics
    total_flows_summary = data["total_flows_summary"]
    short_flows_10ms = data["short_flows_10ms"]
    short_flows_10ms_pct = data["short_flows_10ms_pct"]
    compact_short_flows = data["compact_short_flows"]
    compact_short_flows_pct = data["compact_short_flows_pct"]
    high_risk_candidate_count = data["high_risk_candidate_count"]

    metrics.append({"name": "total_flows", "value": total_flows_summary})
    metrics.append({"name": "duration_le_10ms_flows", "value": short_flows_10ms})
    metrics.append({"name": "duration_le_10ms_pct", "value": short_flows_10ms_pct})
    metrics.append({"name": "compact_short_flows", "value": compact_short_flows})
    metrics.append({"name": "compact_short_flows_pct", "value": compact_short_flows_pct})
    metrics.append({"name": "hybrid_model_candidates", "value": len(scored_rows)})
    metrics.append({"name": "high_risk_candidates", "value": high_risk_candidate_count})

    evidence.append({
        "evidence_id": "e-short-connection-summary",
        "type": "metric",
        "title": "Short-Connection Summary Metrics",
        "metrics": metrics,
    })

    # Short connection sources table (scored candidates)
    if scored_rows:
        src_columns = ["src_ip", "dst_ip", "dst_port", "protocol", "app_protocol", "bytes", "packets", "duration_ms", "session_state", "anomaly_score", "severity", "likely_reason"]
        src_table_rows = [[r.get(c) for c in src_columns] for r in scored_rows[:limit]]
        evidence.append({
            "evidence_id": "e-short-connection-sources",
            "type": "table",
            "title": "Anomalous Short-Flow Candidates",
            "columns": src_columns,
            "rows": src_table_rows,
        })

    # Short low-byte sources (ranked sources with high compact short flow ratio)
    low_byte_sources = [
        s for s in ranked_sources
        if s["compact_short_flows"] > 0
    ]
    if low_byte_sources:
        low_byte_columns = ["src_ip", "candidate_flows", "compact_short_flows", "high_risk_flows", "avg_anomaly_score", "max_anomaly_score", "unique_dst_ip_hint", "unique_dst_port_hint"]
        low_byte_rows = [[s.get(c) for c in low_byte_columns] for s in low_byte_sources[:limit]]
        evidence.append({
            "evidence_id": "e-short-low-byte-sources",
            "type": "table",
            "title": "Highest-Risk Microflow Sources",
            "columns": low_byte_columns,
            "rows": low_byte_rows,
        })

    # Protocol short-flow mix
    mix_columns = data["mix_columns"]
    mix_rows = data["mix_rows"]
    if mix_rows:
        mix_table_rows = [[r.get(c) for c in mix_columns] for r in mix_rows[:limit]]
        evidence.append({
            "evidence_id": "e-protocol-short-flow-mix",
            "type": "table",
            "title": "Microflow State and Protocol Mix",
            "columns": mix_columns,
            "rows": mix_table_rows,
        })

    # Findings
    # 1. Short connection concentration
    if compact_short_flows_pct >= 70.0:
        severity = "high" if compact_short_flows_pct >= 90.0 else "medium"
        findings.append({
            "finding_id": "f-short-concentration",
            "type": "short_connection_concentration",
            "severity": severity,
            "confidence": min(compact_short_flows_pct / 100.0, 1.0),
            "title": f"Short-flow concentration: {compact_short_flows_pct}% compact short flows",
            "description": f"{compact_short_flows} of {total_flows_summary} flows are compact short flows (duration<=10ms, bytes<=300, packets<=2). This level of concentration suggests a dominant microflow pattern such as TLS handshakes, P2P keepalives, or automated polling.",
            "entities": [{"type": "metric", "value": f"compact_short_flows_pct={compact_short_flows_pct}"}],
            "evidence_refs": ["e-short-connection-summary"],
        })

    # 2. High-risk short-connection sources
    high_risk_sources = [s for s in ranked_sources if s["max_anomaly_score"] >= 0.65][:limit]
    for src in high_risk_sources:
        findings.append({
            "finding_id": f"f-short-low-byte-{src['src_ip']}",
            "type": "short_low_byte_source",
            "severity": str(src.get("max_anomaly_score", 0) >= 0.85 and "high" or "medium"),
            "confidence": round(float(src["max_anomaly_score"]), 2),
            "title": f"High-risk microflow source: {src['src_ip']}",
            "description": f"Source {src['src_ip']} has {src['candidate_flows']} candidate flows, {src['compact_short_flows']} compact short flows, max anomaly score {src['max_anomaly_score']}.",
            "entities": [{"type": "src_ip", "value": src["src_ip"]}],
            "evidence_refs": ["e-short-low-byte-sources", "e-short-connection-sources"],
        })

    # Data quality warnings
    total_flows = data["total_flows"]
    if total_flows > 0:
        tiny_duration_pct = round(data["tiny_duration_flows"] * 100.0 / total_flows, 2)
        two_packet_or_less_pct = round(data["two_packet_or_less_flows"] * 100.0 / total_flows, 2)
        if tiny_duration_pct >= 95.0 and two_packet_or_less_pct >= 95.0:
            warnings.append({
                "code": "short_connection_microflow_saturation",
                "message": f"Dataset is dominated by sub-10ms microflows ({tiny_duration_pct}% duration<=1ms, {two_packet_or_less_pct}% packets<=2). Short-flow percentages describe traffic shape, not anomaly evidence.",
                "severity": "info",
            })
        zero_duration_pct = round(data["zero_duration_flows"] * 100.0 / total_flows, 2)
        single_packet_pct = round(data["single_packet_flows"] * 100.0 / total_flows, 2)
        if zero_duration_pct >= 95.0 and single_packet_pct >= 95.0:
            warnings.append({
                "code": "short_connection_degenerate_flows",
                "message": f"Dataset behaves like a degenerate flow export ({zero_duration_pct}% zero duration, {single_packet_pct}% single-packet). Use protocol-review or packet evidence before drawing anomaly conclusions.",
                "severity": "info",
            })

    # Raw report
    evidence.append({
        "evidence_id": "e-raw-report",
        "type": "text",
        "title": "Raw Short-Connection Review Output",
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

    overview_text = f"Short-connection review ({total_flows_summary} flows, {compact_short_flows_pct}% compact short)."
    if findings:
        overview_text += f" {len(findings)} finding(s) identified."

    return {
        "summary": {
            "title": "Short-Connection Review",
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


def short_connection_review_action(
    con: duckdb.DuckDBPyConnection,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    limit: int,
) -> str:
    data = execute_short_connection_review(con, mappings, where_clause, limit)
    return format_short_connection_review(data)
