"""
Behavior Analysis Action

Action handler for behavior analysis including baseline building,
shift detection, and pattern mining.
"""

from __future__ import annotations

from typing import Any

from actions.advanced_action_common import append_file_errors, fetch_rows, format_dict_rows, present_fields, scoped_where
from analysis.behavior_analysis import BehaviorAnalyzer


def execute_behavior_analysis(
    con: Any,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    files: list[str],
    **kwargs,
) -> dict:
    limit = kwargs.get("limit", 20)
    # Explicit time window parameters
    baseline_start = kwargs.get("baseline_start")
    baseline_end = kwargs.get("baseline_end")
    current_start = kwargs.get("current_start")
    current_end = kwargs.get("current_end")
    has_explicit_windows = all([baseline_start, baseline_end, current_start, current_end])

    results = {
        "action": "behavior-analysis",
        "files_analyzed": [],
        "summary": {
            "total_entities_analyzed": 0,
            "behavior_shifts_detected": 0,
            "high_deviation_entities": 0,
            "anomalies_found": 0,
            "windowing_method": "explicit_time_windows" if has_explicit_windows else "heuristic_split",
        },
        "entity_profiles": [],
    }

    analyzer = BehaviorAnalyzer()
    available = present_fields(mappings)
    file_result = {
        "file": files[0] if files else "selected scope",
        "entity_count": 0,
        "behavior_profiles": [],
    }

    try:
        if "src_ip" not in available:
            file_result["error"] = "Behavior analysis requires src_ip in the canonical flow view."
            results["files_analyzed"].append(file_result)
            return results

        entity_sql = f"""
            SELECT src_ip
            FROM flows
            {scoped_where(where_clause, "src_ip IS NOT NULL")}
            GROUP BY 1
            ORDER BY COUNT(*) DESC, src_ip ASC
            LIMIT {limit}
        """
        entity_rows = fetch_rows(con, entity_sql)
        entity_ids = [str(row["src_ip"]) for row in entity_rows if row.get("src_ip")]
        file_result["entity_count"] = len(entity_ids)
        results["summary"]["total_entities_analyzed"] += len(entity_ids)

        for entity_id in entity_ids:
            escaped_entity_id = entity_id.replace("'", "''")
            flow_sql = f"""
                SELECT
                    CAST(timestamp AS VARCHAR) AS timestamp,
                    CAST(start_relative_time_s AS VARCHAR) AS start_relative_time_s,
                    CAST(end_relative_time_s AS VARCHAR) AS end_relative_time_s,
                    COALESCE(
                        CAST(timestamp AS VARCHAR),
                        CAST(start_relative_time_s AS VARCHAR),
                        CAST(end_relative_time_s AS VARCHAR),
                        ''
                    ) AS analysis_time_ts,
                    COALESCE(bytes, 0) AS bytes,
                    COALESCE(packets, 0) AS packets,
                    COALESCE(dst_ip, '') AS dst_ip,
                    COALESCE(dst_port, 0) AS dst_port,
                    COALESCE(protocol, 'UNKNOWN') AS protocol,
                    COALESCE(app_protocol, '') AS app_protocol,
                    COALESCE(service, '') AS service,
                    COALESCE(dns_query, '') AS dns_query,
                    COALESCE(tls_sni, '') AS tls_sni,
                    COALESCE(flow_duration, 0) AS flow_duration,
                    COALESCE(src_bytes, 0) AS src_bytes,
                    COALESCE(dst_bytes, 0) AS dst_bytes
                FROM flows
                {scoped_where(where_clause, f"src_ip = '{escaped_entity_id}'")}
                ORDER BY timestamp NULLS LAST, dst_ip ASC
            """
            flows = fetch_rows(con, flow_sql)

            profile = analyzer.analyze_behavior(
                entity_id,
                flows,
                baseline_start=baseline_start,
                baseline_end=baseline_end,
                current_start=current_start,
                current_end=current_end,
            )
            profile_dict = {
                "entity_id": profile.entity_id,
                "entity_type": profile.entity_type,
                "deviation_score": round(profile.deviation_score, 4),
                "behavior_tags": ", ".join(profile.behavior_tags) if profile.behavior_tags else "normal",
                "anomaly_count": len(profile.anomalies),
                "baseline_flows": profile.baseline.get("total_flows", 0),
                "current_flows": profile.current_behavior.get("total_flows", 0),
                "baseline_method": profile.baseline.get("windowing", {}).get("method", "unknown"),
                "time_ordered": profile.baseline.get("windowing", {}).get("time_ordered", False),
            }
            # Include detailed comparison if available
            if profile.comparison_details:
                profile_dict["comparison_details"] = profile.comparison_details
            # Include data quality assessment
            if profile.data_quality:
                profile_dict["data_quality"] = profile.data_quality

            file_result["behavior_profiles"].append(profile_dict)
            if profile.baseline.get("warnings"):
                file_result.setdefault("warnings", []).extend(profile.baseline["warnings"])
            results["entity_profiles"].append(profile_dict)
            if profile.deviation_score > 0.5:
                results["summary"]["behavior_shifts_detected"] += 1
            if profile.deviation_score > 0.7:
                results["summary"]["high_deviation_entities"] += 1
            results["summary"]["anomalies_found"] += len(profile.anomalies)

        results["files_analyzed"].append(file_result)
    except Exception as e:
        file_result["error"] = str(e)
        results["files_analyzed"].append(file_result)

    return results


def format_results(results: dict) -> str:
    output = []
    output.append("# Behavior Analysis Results\n")

    summary = results["summary"]
    output.append("## Summary\n")
    output.append(f"- **Total Entities Analyzed**: {summary['total_entities_analyzed']}")
    output.append(f"- **Behavior Shifts Detected**: {summary['behavior_shifts_detected']}")
    output.append(f"- **High Deviation Entities**: {summary['high_deviation_entities']}")
    output.append(f"- **Total Anomalies Found**: {summary['anomalies_found']}")
    output.append(f"- **Windowing Method**: {summary.get('windowing_method', 'heuristic_split')}\n")

    for file_result in results["files_analyzed"]:
        if file_result.get("behavior_profiles"):
            output.append(f"\n## File: {file_result['file']}\n")

            # Layer 1: High-confidence findings
            high_conf = [p for p in file_result["behavior_profiles"]
                         if p.get("deviation_score", 0) > 0.5 and p.get("data_quality", {}).get("sufficient_data", True)]
            if high_conf:
                output.append("### Confirmed Behavior Shifts\n")
                output.append(format_dict_rows(high_conf))

            # Layer 2: Low-confidence hints
            low_conf = [p for p in file_result["behavior_profiles"]
                        if 0.3 < p.get("deviation_score", 0) <= 0.5]
            if low_conf:
                output.append("\n### Low-Confidence Behavior Hints\n")
                output.append("_These results have limited evidence. Treat as investigation hints, not conclusions._\n")
                output.append(format_dict_rows(low_conf))

            # Layer 3: Indeterminate due to missing data
            indeterminate = [p for p in file_result["behavior_profiles"]
                             if not p.get("data_quality", {}).get("sufficient_data", True)]
            if indeterminate:
                output.append("\n### Indeterminate (Insufficient Data)\n")
                output.append("_These entities could not be analyzed due to missing fields or insufficient data._\n")
                for p in indeterminate:
                    issues = p.get("data_quality", {}).get("issues", [])
                    output.append(f"- **{p.get('entity_id', 'unknown')}**: {', '.join(issues) if issues else 'insufficient data'}")

    append_file_errors(output, results)
    return "\n".join(output)


def build_skill_result_parts(results: dict, raw_output: str) -> dict[str, Any]:
    summary = results.get("summary", {})
    profiles = results.get("entity_profiles", [])
    errors = [
        {"file": item.get("file", "selected scope"), "error": item["error"]}
        for item in results.get("files_analyzed", [])
        if item.get("error")
    ]
    file_warnings = []
    for item in results.get("files_analyzed", []):
        file_warnings.extend(item.get("warnings") or [])

    findings: list[dict[str, Any]] = []
    for index, profile in enumerate(profiles, 1):
        deviation = float(profile.get("deviation_score") or 0)
        if deviation <= 0.5:
            continue
        severity = "high" if deviation >= 0.8 else "medium" if deviation >= 0.65 else "low"
        findings.append(
            {
                "finding_id": f"f-behavior-{index:03d}",
                "type": "behavior_shift",
                "severity": severity,
                "confidence": min(0.95, round(max(0.5, deviation), 4)),
                "title": f"Behavior shift for {profile.get('entity_id', 'unknown')}",
                "description": (
                    f"Entity behavior deviated from its constructed baseline with score {deviation:.4f}. "
                    "Baseline/current segmentation uses explicit time windows when provided, otherwise a heuristic split."
                ),
                "entities": [
                    {"type": "src_ip", "value": profile.get("entity_id", "")},
                    {"type": "behavior_tags", "value": profile.get("behavior_tags", "")},
                ],
                "evidence_refs": ["e-behavior-profiles"],
                "recommended_actions": [
                    "Review the entity's top destinations and service mix in session-review.",
                    "Compare against a known-good historical window before treating this as confirmed compromise.",
                ],
            }
        )

    warnings = list(dict.fromkeys(file_warnings))
    if profiles:
        if summary.get("windowing_method") == "explicit_time_windows":
            warnings.append(
                "Behavior baselines are built from explicit time windows; review window definitions if the shift looks unexpected."
            )
        else:
            warnings.append(
                "Behavior baselines are built from the selected scope and should be interpreted as heuristic profile shifts."
            )

    evidence: list[dict[str, Any]] = []
    if profiles:
        evidence.append(
            {
                "evidence_id": "e-behavior-profiles",
                "type": "table",
                "title": "Entity Behavior Profiles",
                "columns": list(profiles[0].keys()),
                "rows": profiles,
            }
        )
    evidence.append(
        {
            "evidence_id": "e-raw-report",
            "type": "text",
            "title": "Raw Behavior Analysis Report",
            "content": raw_output,
        }
    )

    max_deviation = max((float(row.get("deviation_score") or 0) for row in profiles), default=0.0)
    return {
        "summary": {
            "title": "Behavior Analysis",
            "overview": (
                f"Analyzed {summary.get('total_entities_analyzed', 0)} entities and detected "
                f"{summary.get('behavior_shifts_detected', 0)} behavior shifts."
            ),
            "severity": "high" if max_deviation >= 0.8 else "medium" if max_deviation >= 0.65 else "info",
            "confidence": 0.7,
            "key_metrics": [
                {"name": "entities_analyzed", "value": summary.get("total_entities_analyzed", 0)},
                {"name": "behavior_shifts_detected", "value": summary.get("behavior_shifts_detected", 0)},
                {"name": "high_deviation_entities", "value": summary.get("high_deviation_entities", 0)},
                {"name": "anomalies_found", "value": summary.get("anomalies_found", 0)},
            ],
        },
        "findings": findings,
        "evidence": evidence,
        "diagnostics": {
            "warnings": warnings,
            "data_quality": {
                "files_with_errors": len(errors),
                "profiles_returned": len(profiles),
            },
            "errors": errors,
        },
    }
