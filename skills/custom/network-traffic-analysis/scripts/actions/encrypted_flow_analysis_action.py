"""
Encrypted Flow Analysis Action

Action handler for encrypted traffic analysis including JA3 fingerprinting,
TLS behavior analysis, and encrypted application classification.
"""

from __future__ import annotations

from typing import Any

from actions.advanced_action_common import append_file_errors, fetch_rows, format_dict_rows, present_fields, scoped_where
from analysis.encrypted_traffic import EncryptedTrafficAnalyzer


def _severity_from_score(score: float) -> str:
    if score >= 0.75:
        return "high"
    if score >= 0.5:
        return "medium"
    if score > 0:
        return "low"
    return "info"


def _confidence_from_row(row: dict[str, Any], default: float = 0.5) -> float:
    for field in ("confidence", "risk_score"):
        value = row.get(field)
        if value in (None, ""):
            continue
        try:
            return max(0.0, min(1.0, float(value)))
        except (TypeError, ValueError):
            continue
    return default


def execute_encrypted_flow_analysis(
    con: Any,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    files: list[str],
    **kwargs,
) -> dict:
    limit = kwargs.get("limit", 20)

    results = {
        "action": "encrypted-flow-analysis",
        "files_analyzed": [],
        "summary": {
            "total_flows_analyzed": 0,
            "ja3_fingerprints_generated": 0,
            "risk_indicators_found": 0,
            "encrypted_applications_classified": 0,
            "tunnel_indicators_found": 0,
        },
        "findings": [],
    }

    analyzer = EncryptedTrafficAnalyzer()
    available = present_fields(mappings)
    file_result = {
        "file": files[0] if files else "selected scope",
        "flow_count": 0,
        "ja3_fingerprints": [],
        "risk_findings": [],
        "application_classifications": [],
        "tunnel_indicators": [],
    }

    try:
        if "dst_port" not in available:
            file_result["error"] = "Encrypted flow analysis requires dst_port in the canonical flow view."
            results["files_analyzed"].append(file_result)
            return results

        duration_expr = "COALESCE(duration_ms, flow_duration, 0)"
        encrypted_predicate = "(" + " OR ".join(
            [
                "tls_sni IS NOT NULL",
                "COALESCE(dst_port, 0) IN (443, 465, 587, 636, 853, 993, 995, 5222, 5223, 8443)",
            ]
        ) + ")"
        def expr(field: str, default: str = "''") -> str:
            return field if field in available else default

        sql = f"""
            SELECT
                src_ip,
                dst_ip,
                dst_port,
                protocol,
                COALESCE(bytes, 0) AS bytes,
                COALESCE(packets, 0) AS packets,
                {duration_expr} AS duration_ms,
                COALESCE(tls_sni, '') AS tls_sni,
                COALESCE({expr('tls_version')}, '') AS tls_version,
                COALESCE({expr('tls_ciphers')}, '') AS tls_ciphers,
                COALESCE({expr('tls_extensions')}, '') AS tls_extensions,
                COALESCE({expr('tls_supported_groups')}, '') AS tls_supported_groups,
                COALESCE({expr('tls_point_formats')}, '') AS tls_point_formats,
                COALESCE({expr('tls_server_cipher')}, '') AS tls_server_cipher,
                COALESCE({expr('tls_server_extensions')}, '') AS tls_server_extensions,
                COALESCE({expr('ja3_string')}, '') AS ja3_string,
                COALESCE({expr('ja3_hash')}, '') AS ja3_hash,
                COALESCE({expr('ja3s_string')}, '') AS ja3s_string,
                COALESCE({expr('ja3s_hash')}, '') AS ja3s_hash,
                COALESCE({expr('tls_metadata_source')}, '') AS tls_metadata_source,
                COALESCE(analysis_time_display, '') AS timestamp
            FROM flows
            {scoped_where(where_clause, encrypted_predicate)}
            ORDER BY COALESCE(bytes, 0) DESC, COALESCE(packets, 0) DESC
            LIMIT {limit}
        """
        flows = fetch_rows(con, sql)
        file_result["flow_count"] = len(flows)
        results["summary"]["total_flows_analyzed"] += len(flows)

        for flow in flows:
            analysis = analyzer.analyze_encrypted_flow(flow)
            ja3_match: dict[str, Any] = {}
            if analysis.ja3_fingerprint and analysis.ja3_fingerprint.ja3_hash:
                ja3_match = analyzer.classify_by_ja3(analysis.ja3_fingerprint.ja3_hash)
            if analysis.ja3_fingerprint:
                file_result["ja3_fingerprints"].append(
                    {
                        "src_ip": flow.get("src_ip", ""),
                        "dst_ip": flow.get("dst_ip", ""),
                        "dst_port": flow.get("dst_port", 0),
                        "ja3_hash": analysis.ja3_fingerprint.ja3_hash,
                        "ja3s_hash": analysis.ja3_fingerprint.ja3s_hash,
                        "tls_version": analysis.ja3_fingerprint.tls_version,
                        "metadata_source": analysis.metadata_source,
                        "matched_application": ja3_match.get("application") or ja3_match.get("label", ""),
                        "match_category": ja3_match.get("category", ""),
                        "match_risk_level": ja3_match.get("risk_level", ""),
                        "match_source": ja3_match.get("source", ""),
                    }
                )
                results["summary"]["ja3_fingerprints_generated"] += 1

            if analysis.risk_indicators:
                file_result["risk_findings"].append(
                    {
                        "src_ip": flow.get("src_ip", ""),
                        "dst_ip": flow.get("dst_ip", ""),
                        "dst_port": flow.get("dst_port", 0),
                        "ja3_hash": analysis.ja3_fingerprint.ja3_hash if analysis.ja3_fingerprint else "",
                        "risk_score": round(analysis.risk_score, 4),
                        "risk_indicators": ", ".join(analysis.risk_indicators),
                        "behavior_tags": ", ".join(analysis.behavior_tags),
                    }
                )
                results["summary"]["risk_indicators_found"] += len(analysis.risk_indicators)

            file_result["application_classifications"].append(
                {
                    "src_ip": flow.get("src_ip", ""),
                    "dst_ip": flow.get("dst_ip", ""),
                    "dst_port": flow.get("dst_port", 0),
                    "application": analysis.application_guess,
                    "confidence": round(analysis.confidence, 4),
                    "method": analysis.classification_method,
                    "evidence_level": analysis.evidence_level,
                    "metadata_source": analysis.metadata_source,
                    "category": ja3_match.get("category", ""),
                    "risk_level": ja3_match.get("risk_level", ""),
                    "source": ja3_match.get("source", ""),
                    "source_url": ja3_match.get("source_url", ""),
                    "description": ja3_match.get("description", ""),
                }
                )
            results["summary"]["encrypted_applications_classified"] += 1

            tunnel_analysis = analyzer.detect_encrypted_tunnel(flow)
            if tunnel_analysis.get("tunnel_indicators"):
                file_result["tunnel_indicators"].append(
                    {
                        "src_ip": flow.get("src_ip", ""),
                        "dst_ip": flow.get("dst_ip", ""),
                        "dst_port": flow.get("dst_port", 0),
                        "tunnel_types": ", ".join(tunnel_analysis.get("possible_tunnel_types", [])),
                        "indicators": ", ".join(tunnel_analysis.get("tunnel_indicators", [])),
                    }
                )
                results["summary"]["tunnel_indicators_found"] += len(tunnel_analysis.get("tunnel_indicators", []))

        if not flows:
            file_result["note"] = "No encrypted-flow candidates matched the selected scope."
        elif "ja3_hash" not in available and "tls_version" not in available and "tls_ciphers" not in available:
            file_result["note"] = "Results were produced without TLS handshake metadata; classifications are weak port/flow inference only."
        results["files_analyzed"].append(file_result)
    except Exception as e:
        file_result["error"] = str(e)
        results["files_analyzed"].append(file_result)

    return results


def build_skill_result_parts(results: dict, raw_output: str) -> dict[str, Any]:
    summary = results.get("summary", {})
    files = results.get("files_analyzed", [])
    risk_rows: list[dict[str, Any]] = []
    ja3_rows: list[dict[str, Any]] = []
    app_rows: list[dict[str, Any]] = []
    tunnel_rows: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    for file_result in files:
        risk_rows.extend(file_result.get("risk_findings") or [])
        ja3_rows.extend(file_result.get("ja3_fingerprints") or [])
        app_rows.extend(file_result.get("application_classifications") or [])
        tunnel_rows.extend(file_result.get("tunnel_indicators") or [])
        if file_result.get("note"):
            warnings.append(
                {
                    "code": "ENCRYPTED_ANALYSIS_NOTE",
                    "message": file_result["note"],
                    "severity": "info",
                }
            )

    max_risk_score = max((_confidence_from_row(row, 0.0) for row in risk_rows), default=0.0)
    ja3_matches = [row for row in app_rows if row.get("method") == "ja3_match"]
    overview = (
        f"Analyzed {summary.get('total_flows_analyzed', 0)} encrypted-flow candidates; "
        f"found {len(risk_rows)} risky flows, {len(ja3_matches)} JA3 intelligence matches, "
        f"and {len(tunnel_rows)} tunnel-indicator rows."
    )

    result_summary = {
        "title": "Encrypted Flow Analysis",
        "overview": overview,
        "severity": _severity_from_score(max_risk_score),
        "confidence": round(max_risk_score, 4) if max_risk_score else None,
        "key_metrics": [
            {"name": "total_flows_analyzed", "value": summary.get("total_flows_analyzed", 0)},
            {"name": "ja3_fingerprints_generated", "value": summary.get("ja3_fingerprints_generated", 0)},
            {"name": "risk_indicators_found", "value": summary.get("risk_indicators_found", 0)},
            {"name": "encrypted_applications_classified", "value": summary.get("encrypted_applications_classified", 0)},
            {"name": "tunnel_indicators_found", "value": summary.get("tunnel_indicators_found", 0)},
        ],
    }

    evidence: list[dict[str, Any]] = [
        {
            "evidence_id": "e-summary-metrics",
            "type": "metric",
            "title": "Encrypted Flow Summary Metrics",
            "content": result_summary["key_metrics"],
        }
    ]
    if ja3_rows:
        evidence.append(
            {
                "evidence_id": "e-ja3-fingerprints",
                "type": "table",
                "title": "JA3 Fingerprints",
                "content": ja3_rows,
            }
        )
    if risk_rows:
        evidence.append(
            {
                "evidence_id": "e-risk-findings",
                "type": "table",
                "title": "Encrypted Flow Risk Findings",
                "content": risk_rows,
            }
        )
    if app_rows:
        evidence.append(
            {
                "evidence_id": "e-application-classifications",
                "type": "table",
                "title": "Encrypted Application Classifications",
                "content": app_rows,
            }
        )
    if tunnel_rows:
        evidence.append(
            {
                "evidence_id": "e-tunnel-indicators",
                "type": "table",
                "title": "Encrypted Tunnel Indicators",
                "content": tunnel_rows,
            }
        )
    if raw_output:
        evidence.append(
            {
                "evidence_id": "e-raw-report",
                "type": "text",
                "title": "Raw Action Output",
                "content": raw_output,
            }
        )

    findings: list[dict[str, Any]] = []
    seen_ja3_findings: set[tuple[Any, ...]] = set()
    ja3_finding_index = 0
    for row in ja3_matches:
        finding_key = (
            row.get("src_ip"),
            row.get("dst_ip"),
            row.get("dst_port"),
            row.get("application"),
            row.get("source"),
        )
        if finding_key in seen_ja3_findings:
            continue
        seen_ja3_findings.add(finding_key)
        ja3_finding_index += 1
        risk_level = str(row.get("risk_level") or "").lower()
        severity = risk_level if risk_level in {"low", "medium", "high", "critical"} else "medium"
        findings.append(
            {
                "finding_id": f"f-ja3-{ja3_finding_index:03d}",
                "type": "ja3_match",
                "severity": severity,
                "confidence": _confidence_from_row(row, 0.8),
                "title": f"JA3 matched {row.get('application') or 'known fingerprint'}",
                "description": row.get("description") or "The encrypted flow matched a known JA3 fingerprint record.",
                "entities": {
                    "src_ip": row.get("src_ip"),
                    "dst_ip": row.get("dst_ip"),
                    "dst_port": row.get("dst_port"),
                    "application": row.get("application"),
                    "category": row.get("category"),
                    "source": row.get("source"),
                    "source_url": row.get("source_url"),
                },
                "evidence_refs": ["e-ja3-fingerprints", "e-application-classifications"],
            }
        )

    seen_risk_findings: set[tuple[Any, ...]] = set()
    risk_finding_index = 0
    for row in risk_rows:
        finding_key = (
            row.get("src_ip"),
            row.get("dst_ip"),
            row.get("dst_port"),
            row.get("ja3_hash"),
            row.get("risk_indicators"),
        )
        if finding_key in seen_risk_findings:
            continue
        seen_risk_findings.add(finding_key)
        risk_finding_index += 1
        findings.append(
            {
                "finding_id": f"f-risk-{risk_finding_index:03d}",
                "type": "encrypted_flow_risk",
                "severity": _severity_from_score(_confidence_from_row(row, 0.0)),
                "confidence": _confidence_from_row(row, 0.5),
                "title": "Encrypted flow risk indicators detected",
                "description": row.get("risk_indicators") or "Encrypted flow risk indicators were detected.",
                "entities": {
                    "src_ip": row.get("src_ip"),
                    "dst_ip": row.get("dst_ip"),
                    "dst_port": row.get("dst_port"),
                    "ja3_hash": row.get("ja3_hash"),
                    "behavior_tags": row.get("behavior_tags"),
                },
                "evidence_refs": ["e-risk-findings"],
            }
        )

    seen_tunnel_findings: set[tuple[Any, ...]] = set()
    tunnel_finding_index = 0
    for row in tunnel_rows:
        finding_key = (
            row.get("src_ip"),
            row.get("dst_ip"),
            row.get("dst_port"),
            row.get("tunnel_types"),
            row.get("indicators"),
        )
        if finding_key in seen_tunnel_findings:
            continue
        seen_tunnel_findings.add(finding_key)
        tunnel_finding_index += 1
        findings.append(
            {
                "finding_id": f"f-tunnel-{tunnel_finding_index:03d}",
                "type": "encrypted_tunnel_indicator",
                "severity": "medium",
                "confidence": 0.6,
                "title": "Possible encrypted tunnel behavior",
                "description": row.get("indicators") or "Tunnel-like encrypted traffic behavior was detected.",
                "entities": {
                    "src_ip": row.get("src_ip"),
                    "dst_ip": row.get("dst_ip"),
                    "dst_port": row.get("dst_port"),
                    "tunnel_types": row.get("tunnel_types"),
                },
                "evidence_refs": ["e-tunnel-indicators"],
            }
        )

    return {
        "summary": result_summary,
        "findings": findings,
        "evidence": evidence,
        "diagnostics": {
            "warnings": warnings,
            "data_quality": {
                "flows_analyzed": summary.get("total_flows_analyzed", 0),
                "ja3_fingerprints": summary.get("ja3_fingerprints_generated", 0),
                "risk_rows": len(risk_rows),
                "application_rows": len(app_rows),
                "tunnel_rows": len(tunnel_rows),
            },
        },
    }


def format_results(results: dict) -> str:
    output = []
    output.append("# Encrypted Flow Analysis Results\n")

    summary = results["summary"]
    output.append("## Summary\n")
    output.append(f"- **Total Flows Analyzed**: {summary['total_flows_analyzed']}")
    output.append(f"- **JA3 Fingerprints Generated**: {summary['ja3_fingerprints_generated']}")
    output.append(f"- **Risk Indicators Found**: {summary['risk_indicators_found']}")
    output.append(f"- **Encrypted Applications Classified**: {summary['encrypted_applications_classified']}")
    output.append(f"- **Tunnel Indicators Found**: {summary['tunnel_indicators_found']}\n")

    for file_result in results["files_analyzed"]:
        if file_result.get("ja3_fingerprints"):
            output.append(f"\n## File: {file_result['file']}\n")
            output.append("### JA3 Fingerprints\n")
            output.append(format_dict_rows(file_result["ja3_fingerprints"]))
        if file_result.get("risk_findings"):
            output.append("\n### Risk Findings\n")
            output.append(format_dict_rows(file_result["risk_findings"]))
        if file_result.get("application_classifications"):
            output.append("\n### Application Classifications\n")
            output.append(format_dict_rows(file_result["application_classifications"][:10]))
        if file_result.get("tunnel_indicators"):
            output.append("\n### Tunnel Indicators\n")
            output.append(format_dict_rows(file_result["tunnel_indicators"]))
        if file_result.get("note"):
            output.append(f"\n{file_result['note']}\n")

    append_file_errors(output, results)
    return "\n".join(output)
