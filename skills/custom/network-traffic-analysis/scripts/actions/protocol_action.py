from __future__ import annotations

from typing import Any

import duckdb  # type: ignore

from utils.formatter import render_rows_section, render_section
from core.schema_mapping import available_canonical_fields, ensure_required


def _shannon_entropy_safe(values: list[float]) -> float:
    from utils.math import _shannon_entropy, _safe_float_local
    counts = [_safe_float_local(v) for v in values]
    return _shannon_entropy(counts)


def execute_protocol_review(
    con: duckdb.DuckDBPyConnection,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    view: str,
    limit: int,
) -> dict[str, Any]:
    """Execute protocol review and return structured data."""
    from actions.overview import protocol_drift_section
    from utils.math import _shannon_entropy, _safe_float_local

    available = available_canonical_fields(mappings)

    result: dict[str, Any] = {
        "view": view,
        "metrics": {},
        "coverage": {},
        "protocol_rows": [],
        "app_protocol_rows": [],
        "dns_rows": [],
        "tls_sni_rows": [],
        "http_host_rows": [],
        "packet_protocol_rows": [],
        "tcp_flags_rows": [],
        "icmp_rows": [],
        "packet_size_rows": [],
    }

    if view == "packet":
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

        if "tcp_flags" in available:
            rows = con.execute(
                f"""
                SELECT COALESCE(tcp_flags, 'UNKNOWN') AS tcp_flags,
                       COUNT(*) AS packets
                FROM flows
                {where_clause}
                GROUP BY 1
                ORDER BY packets DESC, tcp_flags ASC
                LIMIT {limit}
                """
            ).fetchall()
            result["tcp_flags_rows"] = [list(r) for r in rows]

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

        if "payload_bytes" in available or "frame_len" in available:
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
                    COUNT(*) AS packets
                FROM flows
                {where_clause}
                GROUP BY 1
                ORDER BY packets DESC, size_band ASC
                """
            ).fetchall()
            result["packet_size_rows"] = [list(r) for r in rows]
    else:
        if "protocol" in available:
            rows = con.execute(
                f"""
                SELECT COALESCE(protocol, 'UNKNOWN') AS protocol,
                       COUNT(*) AS records,
                       SUM(COALESCE(bytes, 0)) AS total_bytes
                FROM flows
                {where_clause}
                GROUP BY 1
                ORDER BY records DESC, total_bytes DESC, protocol ASC
                LIMIT {limit}
                """
            ).fetchall()
            result["protocol_rows"] = [list(r) for r in rows]

            if rows:
                counts = [_safe_float_local(row[1]) for row in rows]
                entropy = _shannon_entropy(counts)
                total_records = sum(counts)
                dominant_share = round((counts[0] / total_records) * 100.0, 2) if total_records else 0.0
                result["metrics"]["protocol_entropy"] = round(entropy, 4)
                result["metrics"]["dominant_protocol"] = rows[0][0]
                result["metrics"]["dominant_protocol_share_pct"] = dominant_share
                result["metrics"]["protocol_total_records"] = total_records
            result["coverage"]["has_protocol"] = True

        if "app_protocol" in available:
            rows = con.execute(
                f"""
                SELECT COALESCE(app_protocol, 'UNKNOWN') AS app_protocol,
                       COUNT(*) AS records,
                       SUM(COALESCE(bytes, 0)) AS total_bytes
                FROM flows
                {where_clause}
                GROUP BY 1
                ORDER BY records DESC, total_bytes DESC, app_protocol ASC
                LIMIT {limit}
                """
            ).fetchall()
            result["app_protocol_rows"] = [list(r) for r in rows]

            if rows:
                counts = [_safe_float_local(row[1]) for row in rows]
                entropy = _shannon_entropy(counts)
                total_records = sum(counts)
                dominant_share = round((counts[0] / total_records) * 100.0, 2) if total_records else 0.0
                result["metrics"]["app_protocol_entropy"] = round(entropy, 4)
                result["metrics"]["dominant_app_protocol"] = rows[0][0]
                result["metrics"]["dominant_app_protocol_share_pct"] = dominant_share
            result["coverage"]["has_app_protocol"] = True

        if "dns_query" in available:
            rows = con.execute(
                f"""
                SELECT dns_query,
                       COUNT(*) AS records,
                       SUM(COALESCE(bytes, 0)) AS total_bytes
                FROM flows
                {where_clause}
                WHERE dns_query IS NOT NULL AND dns_query != ''
                GROUP BY 1
                ORDER BY records DESC, total_bytes DESC, dns_query ASC
                LIMIT {limit}
                """
            ).fetchall()
            result["dns_rows"] = [list(r) for r in rows]
            result["coverage"]["has_dns_query"] = len(rows) > 0

        if "tls_sni" in available:
            rows = con.execute(
                f"""
                SELECT tls_sni,
                       COUNT(*) AS records,
                       SUM(COALESCE(bytes, 0)) AS total_bytes
                FROM flows
                {where_clause}
                WHERE tls_sni IS NOT NULL AND tls_sni != ''
                GROUP BY 1
                ORDER BY records DESC, total_bytes DESC, tls_sni ASC
                LIMIT {limit}
                """
            ).fetchall()
            result["tls_sni_rows"] = [list(r) for r in rows]
            result["coverage"]["has_tls_sni"] = len(rows) > 0

        if "http_host" in available:
            rows = con.execute(
                f"""
                SELECT http_host,
                       COUNT(*) AS records,
                       SUM(COALESCE(bytes, 0)) AS total_bytes
                FROM flows
                {where_clause}
                WHERE http_host IS NOT NULL AND http_host != ''
                GROUP BY 1
                ORDER BY records DESC, total_bytes DESC, http_host ASC
                LIMIT {limit}
                """
            ).fetchall()
            result["http_host_rows"] = [list(r) for r in rows]
            result["coverage"]["has_http_host"] = len(rows) > 0

    return result


def format_protocol_review(
    data: dict[str, Any],
    *,
    con: duckdb.DuckDBPyConnection | None = None,
    where_clause: str = "",
    limit: int = 20,
) -> str:
    """Format structured protocol review data as human-readable text."""
    from actions.overview import protocol_drift_section

    sections = [f"Analysis view: {data['view']}"]

    if data["view"] == "packet":
        if data["packet_protocol_rows"]:
            sections.append(
                render_rows_section(
                    "Packet protocol mix",
                    ["protocol", "packets", "total_bytes"],
                    data["packet_protocol_rows"],
                )
            )

        if data["tcp_flags_rows"]:
            sections.append(
                render_rows_section(
                    "TCP flags distribution",
                    ["tcp_flags", "packets"],
                    data["tcp_flags_rows"],
                )
            )

        if data["icmp_rows"]:
            sections.append(
                render_rows_section(
                    "ICMP type and code distribution",
                    ["icmp_type", "icmp_code", "packets"],
                    data["icmp_rows"],
                )
            )

        if data["packet_size_rows"]:
            sections.append(
                render_rows_section(
                    "Packet size bands",
                    ["size_band", "packets"],
                    data["packet_size_rows"],
                )
            )
    else:
        if data["protocol_rows"]:
            m = data["metrics"]
            sections.append(
                "\n".join(
                    [
                        "Protocol distribution summary",
                        f"protocol_entropy={m.get('protocol_entropy', 'N/A')}, dominant_protocol={m.get('dominant_protocol', 'N/A')}, dominant_share_pct={m.get('dominant_protocol_share_pct', 'N/A')}",
                        "Lower entropy and high dominant share usually indicate concentrated traffic families; abrupt share drift across buckets can indicate operational or malicious change.",
                    ]
                )
            )
            sections.append(
                render_rows_section("Flow protocol mix", ["protocol", "records", "total_bytes"], data["protocol_rows"])
            )
            if con is not None:
                sections.append(protocol_drift_section(con, where_clause, field_name="protocol", title="Protocol share drift review", limit=limit))

        if data["app_protocol_rows"]:
            m = data["metrics"]
            sections.append(
                "\n".join(
                    [
                        "Application protocol distribution summary",
                        f"app_protocol_entropy={m.get('app_protocol_entropy', 'N/A')}, dominant_app_protocol={m.get('dominant_app_protocol', 'N/A')}, dominant_share_pct={m.get('dominant_app_protocol_share_pct', 'N/A')}",
                        "App-protocol entropy is a stronger signal than raw counts when comparing concentrated bot/C2-like behavior with broad benign traffic.",
                    ]
                )
            )
            sections.append(
                render_rows_section("Application protocol mix", ["app_protocol", "records", "total_bytes"], data["app_protocol_rows"])
            )
            if con is not None:
                sections.append(protocol_drift_section(con, where_clause, field_name="app_protocol", title="Application protocol share drift review", limit=limit))

        if data["dns_rows"]:
            sections.append(
                render_rows_section("Top DNS queries", ["dns_query", "records", "total_bytes"], data["dns_rows"])
            )

        if data["tls_sni_rows"]:
            sections.append(
                render_rows_section("Top TLS SNI values", ["tls_sni", "records", "total_bytes"], data["tls_sni_rows"])
            )

        if data["http_host_rows"]:
            sections.append(
                render_rows_section("Top HTTP host values", ["http_host", "records", "total_bytes"], data["http_host_rows"])
            )

    return "\n\n".join(sections)


def protocol_review_action(
    con: duckdb.DuckDBPyConnection,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    view: str,
    limit: int,
) -> str:
    """Legacy entry point — delegates to execute + format for backward compatibility."""
    data = execute_protocol_review(con, mappings, where_clause, view, limit)
    return format_protocol_review(data, con=con, where_clause=where_clause, limit=limit)


def build_skill_result_parts(data: dict[str, Any], raw_output: str) -> dict[str, Any]:
    """Build structured SkillResult for protocol-review action."""
    findings: list[dict[str, Any]] = []
    evidence: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    metrics: list[dict[str, Any]] = []

    view = data["view"]

    # Table evidence for protocol mix
    if data["protocol_rows"]:
        evidence.append({
            "evidence_id": "e-protocol-mix",
            "type": "table",
            "title": "Protocol Distribution",
            "columns": ["protocol", "records", "total_bytes"],
            "rows": data["protocol_rows"],
        })

    if data["app_protocol_rows"]:
        evidence.append({
            "evidence_id": "e-app-protocol-mix",
            "type": "table",
            "title": "Application Protocol Distribution",
            "columns": ["app_protocol", "records", "total_bytes"],
            "rows": data["app_protocol_rows"],
        })

    if data["dns_rows"]:
        evidence.append({
            "evidence_id": "e-dns-queries",
            "type": "table",
            "title": "Top DNS Queries",
            "columns": ["dns_query", "records", "total_bytes"],
            "rows": data["dns_rows"],
        })

    if data["tls_sni_rows"]:
        evidence.append({
            "evidence_id": "e-tls-sni",
            "type": "table",
            "title": "Top TLS SNI Values",
            "columns": ["tls_sni", "records", "total_bytes"],
            "rows": data["tls_sni_rows"],
        })

    if data["http_host_rows"]:
        evidence.append({
            "evidence_id": "e-http-hosts",
            "type": "table",
            "title": "Top HTTP Hosts",
            "columns": ["http_host", "records", "total_bytes"],
            "rows": data["http_host_rows"],
        })

    # Packet-view table evidence
    if view == "packet":
        if data["packet_protocol_rows"]:
            evidence.append({
                "evidence_id": "e-packet-protocol-mix",
                "type": "table",
                "title": "Packet Protocol Mix",
                "columns": ["protocol", "packets", "total_bytes"],
                "rows": data["packet_protocol_rows"],
            })
        if data["tcp_flags_rows"]:
            evidence.append({
                "evidence_id": "e-tcp-flags-distribution",
                "type": "table",
                "title": "TCP Flags Distribution",
                "columns": ["tcp_flags", "packets"],
                "rows": data["tcp_flags_rows"],
            })
        if data["icmp_rows"]:
            evidence.append({
                "evidence_id": "e-icmp-activity",
                "type": "table",
                "title": "ICMP Type/Code Distribution",
                "columns": ["icmp_type", "icmp_code", "packets"],
                "rows": data["icmp_rows"],
            })
        if data["packet_size_rows"]:
            evidence.append({
                "evidence_id": "e-packet-size-profile",
                "type": "table",
                "title": "Packet Size Bands",
                "columns": ["size_band", "packets"],
                "rows": data["packet_size_rows"],
            })

    # Metric evidence
    m = data["metrics"]
    if m.get("protocol_entropy") is not None:
        metrics.append({"name": "protocol_entropy", "value": m["protocol_entropy"]})
    if m.get("dominant_protocol_share_pct") is not None:
        metrics.append({"name": "dominant_protocol_share_pct", "value": m["dominant_protocol_share_pct"]})
    if m.get("app_protocol_entropy") is not None:
        metrics.append({"name": "app_protocol_entropy", "value": m["app_protocol_entropy"]})
    if m.get("dominant_app_protocol_share_pct") is not None:
        metrics.append({"name": "dominant_app_protocol_share_pct", "value": m["dominant_app_protocol_share_pct"]})

    total_records = m.get("protocol_total_records", 0)
    if total_records:
        metrics.append({"name": "protocol_total_records", "value": total_records})

    if metrics:
        evidence.append({
            "evidence_id": "e-protocol-metrics",
            "type": "metric",
            "title": "Protocol Metrics",
            "metrics": metrics,
        })

    # Supplemental raw text
    evidence.append({
        "evidence_id": "e-raw-report",
        "type": "text",
        "title": "Raw Protocol Review Output",
        "content": raw_output,
    })

    # Findings: concentration
    share_pct = m.get("dominant_protocol_share_pct")
    dominant = m.get("dominant_protocol")
    if share_pct is not None and share_pct >= 80:
        findings.append({
            "finding_id": "f-protocol-concentration",
            "type": "protocol_concentration",
            "severity": "medium",
            "confidence": 0.8,
            "title": f"Dominant protocol {dominant} at {share_pct}%",
            "description": (
                f"{dominant} accounts for {share_pct}% of records. "
                f"High protocol concentration may indicate a single application family "
                f"or automated behavior."
            ),
            "evidence_refs": ["e-protocol-mix", "e-protocol-metrics"],
        })

    app_share_pct = m.get("dominant_app_protocol_share_pct")
    app_dominant = m.get("dominant_app_protocol")
    if app_share_pct is not None and app_share_pct >= 80:
        findings.append({
            "finding_id": "f-app-protocol-concentration",
            "type": "application_protocol_concentration",
            "severity": "medium",
            "confidence": 0.8,
            "title": f"Dominant app protocol {app_dominant} at {app_share_pct}%",
            "description": (
                f"{app_dominant} accounts for {app_share_pct}% of records. "
                f"High application protocol concentration can indicate homogenous "
                f"client behavior or a single service dependency."
            ),
            "evidence_refs": ["e-app-protocol-mix", "e-protocol-metrics"],
        })

    # Coverage warnings (not findings — trust/coverage info)
    cov = data.get("coverage", {})
    if cov.get("has_protocol") and not cov.get("has_dns_query"):
        warnings.append({
            "code": "no_dns_evidence",
            "message": "No DNS query data available for protocol review.",
            "severity": "info",
        })
    if cov.get("has_protocol") and not cov.get("has_tls_sni"):
        warnings.append({
            "code": "no_tls_evidence",
            "message": "No TLS SNI data available for protocol review.",
            "severity": "info",
        })
    if cov.get("has_protocol") and not cov.get("has_http_host"):
        warnings.append({
            "code": "no_http_evidence",
            "message": "No HTTP host data available for protocol review.",
            "severity": "info",
        })

    overview_text = f"Protocol review (view={view})."
    if dominant:
        overview_text += f" Dominant protocol: {dominant} ({share_pct}%)."
    elif view == "packet":
        overview_text += f" Packet-level protocol analysis."

    return {
        "summary": {
            "title": "Protocol Review",
            "overview": overview_text,
            "severity": "medium" if findings else "info",
            "confidence": 0.85,
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
