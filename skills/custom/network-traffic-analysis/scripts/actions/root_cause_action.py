"""
Root Cause Analysis Action

Action handler for root cause analysis with heuristic feature contribution
ranking and automated explanation generation.
"""

from __future__ import annotations

from typing import Any

from actions.advanced_action_common import append_file_errors, fetch_rows, format_dict_rows, present_fields, scoped_where
from analysis.root_cause import RootCauseAnalyzer


def execute_root_cause_analysis(
    con: Any,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    files: list[str],
    **kwargs,
) -> dict:
    limit = kwargs.get("limit", 20)
    # Optional: link to behavior-analysis findings
    behavior_context = kwargs.get("behavior_context")

    results = {
        "action": "root-cause-analysis",
        "files_analyzed": [],
        "summary": {
            "total_anomalies_analyzed": 0,
            "root_causes_identified": 0,
            "high_severity_findings": 0,
            "recommendations_generated": 0,
            "behavior_shifts_referenced": 0,
        },
        "rca_results": [],
    }

    analyzer = RootCauseAnalyzer()
    available = present_fields(mappings)
    file_result = {
        "file": files[0] if files else "selected scope",
        "anomaly_count": 0,
        "rca_results": [],
    }

    try:
        if not {"src_ip", "dst_ip", "bytes", "packets"}.issubset(available):
            file_result["error"] = "Root cause analysis requires src_ip, dst_ip, bytes, and packets in the canonical flow view."
            results["files_analyzed"].append(file_result)
            return results

        duration_expr = "COALESCE(duration_ms, flow_duration, 0)"
        byte_asymmetry_expr = "COALESCE(byte_asymmetry, 0)" if "byte_asymmetry" in available else "0"
        packet_asymmetry_expr = "COALESCE(packet_asymmetry, 0)" if "packet_asymmetry" in available else "0"
        dns_entropy_expr = "COALESCE(dns_query_entropy, 0)" if "dns_query_entropy" in available else "0"
        ratio_expr = "COALESCE(src_to_dst_byte_ratio, 0)" if "src_to_dst_byte_ratio" in available else "0"

        sql = f"""
            SELECT
                src_ip,
                dst_ip,
                COALESCE(dst_port, 0) AS dst_port,
                COALESCE(protocol, 'UNKNOWN') AS protocol,
                COALESCE(bytes, 0) AS bytes,
                COALESCE(packets, 0) AS packets,
                {duration_expr} AS duration_ms,
                {byte_asymmetry_expr} AS byte_asymmetry,
                {packet_asymmetry_expr} AS packet_asymmetry,
                {dns_entropy_expr} AS dns_query_entropy,
                {ratio_expr} AS src_to_dst_byte_ratio,
                COALESCE(ttl_avg, 0) AS ttl_avg,
                TRY_CAST(timestamp AS VARCHAR) AS timestamp,
                COALESCE(app_protocol, '') AS app_protocol,
                COALESCE(service, '') AS service,
                COALESCE(dns_query, '') AS dns_query,
                COALESCE(tls_sni, '') AS tls_sni,
                (
                    CASE WHEN COALESCE(bytes, 0) >= 1000000 THEN 0.2 ELSE 0 END +
                    CASE WHEN COALESCE(packets, 0) >= 1000 THEN 0.15 ELSE 0 END +
                    CASE WHEN {duration_expr} >= 600 THEN 0.15 ELSE 0 END +
                    CASE WHEN {byte_asymmetry_expr} >= 0.7 THEN 0.2 ELSE 0 END +
                    CASE WHEN {packet_asymmetry_expr} >= 0.7 THEN 0.1 ELSE 0 END +
                    CASE WHEN {dns_entropy_expr} >= 3.6 THEN 0.1 ELSE 0 END +
                    CASE WHEN {ratio_expr} >= 3 THEN 0.1 ELSE 0 END
                ) AS anomaly_score
            FROM flows
            {scoped_where(where_clause, "src_ip IS NOT NULL AND dst_ip IS NOT NULL")}
            ORDER BY anomaly_score DESC, COALESCE(bytes, 0) DESC
            LIMIT {limit}
        """
        anomalies = [row for row in fetch_rows(con, sql) if float(row.get("anomaly_score") or 0) > 0]
        file_result["anomaly_count"] = len(anomalies)
        results["summary"]["total_anomalies_analyzed"] += len(anomalies)

        # Build behavior reference map
        behavior_map: dict[str, dict] = {}
        if behavior_context:
            for bp in behavior_context.get("entity_profiles", []):
                eid = bp.get("entity_id")
                if eid:
                    behavior_map[eid] = bp

        # Group anomalies by entity and aggregate
        entity_anomalies: dict[str, list[dict]] = {}
        for anomaly in anomalies:
            entity_id = str(anomaly.get("src_ip", ""))
            entity_anomalies.setdefault(entity_id, []).append(anomaly)

        # Sort entities by behavior deviation score (higher first) when available
        sorted_entity_ids = sorted(
            entity_anomalies.keys(),
            key=lambda eid: -(behavior_map.get(eid, {}).get("deviation_score") or 0),
        )

        for entity_id in sorted_entity_ids:
            entity_flow_list = entity_anomalies[entity_id]
            # Use the highest-scoring anomaly as the representative for RCA
            top_anomaly = entity_flow_list[0]  # Already sorted by anomaly_score DESC

            # Enrich with behavior context if available
            behavior_ref = behavior_map.get(entity_id)
            context = {
                "known_threat_ips": [],
                "investigation_timestamp": top_anomaly.get("timestamp", ""),
                "behavior_deviation_score": behavior_ref.get("deviation_score") if behavior_ref else None,
                "behavior_tags": behavior_ref.get("behavior_tags") if behavior_ref else None,
            }
            if behavior_ref:
                results["summary"]["behavior_shifts_referenced"] += 1

            rca_result = analyzer.analyze_root_cause(top_anomaly, context)
            rca_dict = {
                "entity_id": rca_result.entity_id,
                "conclusion_grade": rca_result.conclusion_grade,
                "anomaly_score": round(float(rca_result.anomaly_score or 0), 4),
                "confidence": round(rca_result.confidence, 4),
                # Converged output
                "primary_cause": rca_result.primary_cause,
                "supporting_causes": rca_result.supporting_causes,
                "uncertain_notes": rca_result.uncertain_notes,
                # Metadata
                "fields_used": rca_result.fields_used,
                "fields_missing": rca_result.fields_missing,
                "anomalous_flow_count": len(entity_flow_list),
                "top_dst_ips": sorted(set(str(a.get("dst_ip", "")) for a in entity_flow_list if a.get("dst_ip")))[:5],
                "top_ports": sorted(set(int(a.get("dst_port", 0)) for a in entity_flow_list if a.get("dst_port")))[:10],
                "recommendations": "; ".join(rca_result.recommended_actions[:3]),
            }
            if behavior_ref:
                rca_dict["behavior_context"] = {
                    "deviation_score": behavior_ref.get("deviation_score"),
                    "behavior_tags": behavior_ref.get("behavior_tags"),
                    "comparison_summary": f"Baseline: {behavior_ref.get('baseline_flows')} flows vs Current: {behavior_ref.get('current_flows')} flows",
                }
            file_result["rca_results"].append(rca_dict)
            results["rca_results"].append(rca_dict)
            results["summary"]["root_causes_identified"] += len(rca_result.root_causes)
            results["summary"]["recommendations_generated"] += len(rca_result.recommended_actions)
            if any(root_cause.get("severity") in ["high", "critical"] for root_cause in rca_result.root_causes):
                results["summary"]["high_severity_findings"] += 1

        if not anomalies:
            file_result["note"] = "No anomaly candidates crossed the RCA heuristic threshold in the selected scope."
        results["files_analyzed"].append(file_result)
    except Exception as e:
        file_result["error"] = str(e)
        results["files_analyzed"].append(file_result)

    return results


def format_results(results: dict) -> str:
    output = []
    output.append("# Root Cause Analysis Results\n")

    summary = results["summary"]
    output.append("## Summary\n")
    output.append(f"- **Total Anomalies Analyzed**: {summary['total_anomalies_analyzed']}")
    output.append(f"- **Root Causes Identified**: {summary['root_causes_identified']}")
    output.append(f"- **High Severity Findings**: {summary['high_severity_findings']}")
    output.append(f"- **Recommendations Generated**: {summary['recommendations_generated']}\n")

    for file_result in results["files_analyzed"]:
        if file_result.get("rca_results"):
            output.append(f"\n## File: {file_result['file']}\n")

            # Grade summary
            grades = {}
            for r in file_result["rca_results"]:
                g = r.get("conclusion_grade", "unknown")
                grades[g] = grades.get(g, 0) + 1
            grade_parts = []
            for g in ["confirmed", "likely", "possible", "insufficient_evidence"]:
                if g in grades:
                    grade_parts.append(f"**{g}**: {grades[g]}")
            if grade_parts:
                output.append("### Conclusion Grades\n")
                output.append(" | ".join(grade_parts))
                output.append("")

            # Primary causes table
            entities_with_primary = [r for r in file_result["rca_results"] if r.get("primary_cause")]
            if entities_with_primary:
                output.append("### Primary Causes\n")
                primary_rows = []
                for r in entities_with_primary:
                    pc = r["primary_cause"]
                    primary_rows.append({
                        "entity": r["entity_id"],
                        "grade": r.get("conclusion_grade", ""),
                        "primary_cause": pc.get("feature", ""),
                        "value": pc.get("current_value", ""),
                        "outside_normal": "Yes" if pc.get("outside_normal_range") else "No",
                        "flow_count": r.get("anomalous_flow_count", ""),
                        "description": pc.get("description", ""),
                    })
                output.append(format_dict_rows(primary_rows))

            # Supporting causes
            entities_with_supporting = [r for r in file_result["rca_results"] if r.get("supporting_causes")]
            if entities_with_supporting:
                output.append("\n### Supporting Causes\n")
                supp_rows = []
                for r in entities_with_supporting:
                    for s in r["supporting_causes"]:
                        supp_rows.append({
                            "entity": r["entity_id"],
                            "feature": s["feature"],
                            "value": s.get("current_value", ""),
                            "description": s.get("description", ""),
                        })
                output.append(format_dict_rows(supp_rows))

            # Uncertain notes (consolidated, not repeated)
            uncertain_entries = []
            for r in file_result["rca_results"]:
                for note in r.get("uncertain_notes", []):
                    uncertain_entries.append(f"- **{r['entity_id']}**: {note}")
            if uncertain_entries:
                output.append("\n### Uncertain / Insufficient Data\n")
                output.append("\n".join(uncertain_entries))
                output.append("")

            # One-line correlation disclaimer (only once)
            has_uncertain = any(r.get("uncertain_notes") for r in file_result["rca_results"])
            if has_uncertain:
                output.append("_Attribution is heuristic; treat as correlation evidence, not causal proof._\n")

        if file_result.get("note"):
            output.append(f"\n{file_result['note']}\n")

    append_file_errors(output, results)
    return "\n".join(output)


def build_skill_result_parts(results: dict, raw_output: str) -> dict[str, Any]:
    summary = results.get("summary", {})
    rca_rows = results.get("rca_results", [])
    errors = [
        {"file": item.get("file", "selected scope"), "error": item["error"]}
        for item in results.get("files_analyzed", [])
        if item.get("error")
    ]
    warnings = [
        item["note"]
        for item in results.get("files_analyzed", [])
        if item.get("note")
    ]
    warnings.append(
        "Root-cause output is heuristic feature contribution ranking, not SHAP or model-causal attribution."
    )

    findings: list[dict[str, Any]] = []
    for index, row in enumerate(rca_rows, 1):
        anomaly_score = float(row.get("anomaly_score") or 0)
        confidence = float(row.get("confidence") or 0)
        grade = row.get("conclusion_grade", "unknown")
        primary = row.get("primary_cause") or {}

        severity = (
            "high" if grade == "confirmed" else
            "medium" if grade == "likely" else
            "low" if grade == "possible" else "info"
        )
        findings.append(
            {
                "finding_id": f"f-rca-{index:03d}",
                "type": "root_cause",
                "severity": severity,
                "confidence": round(confidence, 4),
                "conclusion_grade": grade,
                "title": f"Primary cause: {primary.get('feature', 'unknown')}",
                "description": primary.get("description", "Root cause analysis completed."),
                "entities": [
                    {"type": "entity", "value": row.get("entity_id", "")},
                    {"type": "primary_cause", "value": primary.get("feature", "")},
                ],
                "evidence_refs": ["e-rca-results"],
                "recommended_actions": [
                    action.strip()
                    for action in str(row.get("recommendations", "")).split(";")
                    if action.strip()
                ][:3],
            }
        )

    evidence: list[dict[str, Any]] = []
    if rca_rows:
        evidence.append(
            {
                "evidence_id": "e-rca-results",
                "type": "table",
                "title": "Heuristic Root-Cause Results",
                "columns": list(rca_rows[0].keys()),
                "rows": rca_rows,
            }
        )
    evidence.append(
        {
            "evidence_id": "e-raw-report",
            "type": "text",
            "title": "Raw Root Cause Analysis Report",
            "content": raw_output,
        }
    )

    return {
        "summary": {
            "title": "Root Cause Analysis",
            "overview": (
                f"Analyzed {summary.get('total_anomalies_analyzed', 0)} anomaly candidates and "
                f"identified {summary.get('root_causes_identified', 0)} heuristic root-cause contributors."
            ),
            "severity": "high" if summary.get("high_severity_findings", 0) else "info",
            "confidence": 0.68,
            "key_metrics": [
                {"name": "anomalies_analyzed", "value": summary.get("total_anomalies_analyzed", 0)},
                {"name": "root_causes_identified", "value": summary.get("root_causes_identified", 0)},
                {"name": "high_severity_findings", "value": summary.get("high_severity_findings", 0)},
                {"name": "recommendations_generated", "value": summary.get("recommendations_generated", 0)},
            ],
        },
        "findings": findings,
        "evidence": evidence,
        "diagnostics": {
            "warnings": warnings,
            "data_quality": {
                "files_with_errors": len(errors),
                "rca_rows_returned": len(rca_rows),
            },
            "behavior_shifts_referenced": summary.get("behavior_shifts_referenced", 0),
            "errors": errors,
        },
    }
