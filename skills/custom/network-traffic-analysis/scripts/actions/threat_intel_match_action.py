"""
Threat Intelligence Matching Action

Action handler for threat intelligence IOC matching and alert enrichment.
"""

from __future__ import annotations

from typing import Any

from actions.advanced_action_common import append_file_errors, fetch_rows, format_dict_rows
from analysis.threat_intel import ThreatIntelMatcher


SEVERITY_ORDER = {
    "critical": 4,
    "high": 3,
    "medium": 2,
    "low": 1,
    "info": 0,
}


def _highest_severity(rows: list[dict[str, Any]]) -> str:
    severity = "info"
    for row in rows:
        current = str(row.get("severity") or "info").lower()
        if SEVERITY_ORDER.get(current, 0) > SEVERITY_ORDER.get(severity, 0):
            severity = current
    return severity


def _safe_confidence(value: Any, default: float = 0.5) -> float:
    try:
        return max(0.0, min(1.0, float(value)))
    except (TypeError, ValueError):
        return default


def execute_threat_intel_match(
    con: Any,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    files: list[str],
    **kwargs,
) -> dict:
    limit = kwargs.get("limit", 50)

    results = {
        "action": "threat-intel-match",
        "matcher_status": {},
        "files_analyzed": [],
        "summary": {
            "total_entities_checked": 0,
            "threat_matches_found": 0,
            "critical_threats": 0,
            "high_threats": 0,
            "mitre_techniques_mapped": 0,
            "campaigns_identified": 0,
        },
        "matches": [],
    }

    matcher = ThreatIntelMatcher()
    results["matcher_status"] = matcher.get_status()
    file_result = {
        "file": files[0] if files else "selected scope",
        "entity_count": 0,
        "threat_matches": [],
        "notes": [],
    }

    try:
        query = f"""
            SELECT DISTINCT
                dst_ip,
                dns_query,
                tls_sni,
                http_host
            FROM flows
            {where_clause}
            LIMIT {limit}
        """
        entities = fetch_rows(con, query)
        file_result["entity_count"] = len(entities)
        results["summary"]["total_entities_checked"] += len(entities)

        for entity in entities:
            entities_to_check = []
            seen_entities = set()
            if entity.get("dst_ip"):
                item = (entity["dst_ip"], "ip")
                if item not in seen_entities:
                    entities_to_check.append({"entity": entity["dst_ip"], "type": "ip"})
                    seen_entities.add(item)
            if entity.get("dns_query"):
                item = (entity["dns_query"], "domain")
                if item not in seen_entities:
                    entities_to_check.append({"entity": entity["dns_query"], "type": "domain"})
                    seen_entities.add(item)
            if entity.get("tls_sni"):
                item = (entity["tls_sni"], "domain")
                if item not in seen_entities:
                    entities_to_check.append({"entity": entity["tls_sni"], "type": "domain"})
                    seen_entities.add(item)
            if entity.get("http_host"):
                item = (entity["http_host"], "domain")
                if item not in seen_entities:
                    entities_to_check.append({"entity": entity["http_host"], "type": "domain"})
                    seen_entities.add(item)

            for match in matcher.match_iocs(entities_to_check):
                if not match.matched:
                    continue
                match_dict = {
                    "entity": match.entity_id,
                    "type": match.entity_type,
                    "threat_type": match.threat_type,
                    "severity": match.severity,
                    "reputation_score": match.reputation_score,
                    "source": match.source,
                    "source_url": match.source_url,
                    "confidence": match.confidence,
                    "coverage_mode": match.coverage_mode,
                    "first_seen": match.first_seen or "",
                    "last_seen": match.last_seen or "",
                    "cache_age_hours": match.cache_age_hours if match.cache_age_hours is not None else "",
                    "mitre_techniques": ", ".join(match.mitre_techniques),
                    "campaigns": ", ".join(match.campaigns),
                }
                file_result["threat_matches"].append(match_dict)
                results["matches"].append(match_dict)
                results["summary"]["threat_matches_found"] += 1
                if match.severity == "critical":
                    results["summary"]["critical_threats"] += 1
                elif match.severity == "high":
                    results["summary"]["high_threats"] += 1
                results["summary"]["mitre_techniques_mapped"] += len(match.mitre_techniques)
                results["summary"]["campaigns_identified"] += len(match.campaigns)

        if results["matcher_status"].get("coverage_warning"):
            file_result["notes"].append(results["matcher_status"]["coverage_warning"])

        results["files_analyzed"].append(file_result)
    except Exception as e:
        file_result["error"] = str(e)
        results["files_analyzed"].append(file_result)

    return results


def build_skill_result_parts(results: dict, raw_output: str) -> dict[str, Any]:
    summary = results.get("summary", {})
    matches = results.get("matches") or []
    matcher_status = results.get("matcher_status") or {}
    files = results.get("files_analyzed") or []

    warnings: list[dict[str, Any]] = []
    if matcher_status.get("coverage_warning"):
        warnings.append(
            {
                "code": "THREAT_INTEL_COVERAGE_WARNING",
                "message": matcher_status["coverage_warning"],
                "severity": "warning",
            }
        )
    for error in matcher_status.get("refresh_errors") or []:
        warnings.append(
            {
                "code": "THREAT_INTEL_REFRESH_ERROR",
                "message": str(error),
                "severity": "warning",
            }
        )
    for file_result in files:
        for note in file_result.get("notes") or []:
            warnings.append(
                {
                    "code": "THREAT_INTEL_NOTE",
                    "message": note,
                    "severity": "info",
                }
            )

    highest_severity = _highest_severity(matches)
    overview = (
        f"Checked {summary.get('total_entities_checked', 0)} network entities; "
        f"found {summary.get('threat_matches_found', 0)} threat-intel matches "
        f"from {matcher_status.get('coverage_mode', 'unknown')} coverage."
    )
    result_summary = {
        "title": "Threat Intelligence Match",
        "overview": overview,
        "severity": highest_severity,
        "confidence": max((_safe_confidence(row.get("confidence"), 0.0) for row in matches), default=None),
        "key_metrics": [
            {"name": "total_entities_checked", "value": summary.get("total_entities_checked", 0)},
            {"name": "threat_matches_found", "value": summary.get("threat_matches_found", 0)},
            {"name": "critical_threats", "value": summary.get("critical_threats", 0)},
            {"name": "high_threats", "value": summary.get("high_threats", 0)},
            {"name": "mitre_techniques_mapped", "value": summary.get("mitre_techniques_mapped", 0)},
            {"name": "campaigns_identified", "value": summary.get("campaigns_identified", 0)},
        ],
    }

    evidence: list[dict[str, Any]] = [
        {
            "evidence_id": "e-threat-intel-metrics",
            "type": "metric",
            "title": "Threat Intelligence Summary Metrics",
            "content": result_summary["key_metrics"],
        },
        {
            "evidence_id": "e-threat-intel-coverage",
            "type": "metric",
            "title": "Threat Intelligence Coverage Status",
            "content": {
                "coverage_mode": matcher_status.get("coverage_mode", "unknown"),
                "loaded_feed_count": matcher_status.get("loaded_feed_count", 0),
                "total_loaded_indicators": matcher_status.get("total_loaded_indicators", 0),
                "feed_directory": matcher_status.get("feed_directory", ""),
            },
        },
    ]
    if matches:
        evidence.append(
            {
                "evidence_id": "e-threat-intel-matches",
                "type": "table",
                "title": "Threat Intelligence Matches",
                "content": matches,
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
    seen: set[tuple[Any, ...]] = set()
    finding_index = 0
    for row in matches:
        finding_key = (
            row.get("entity"),
            row.get("type"),
            row.get("threat_type"),
            row.get("source"),
        )
        if finding_key in seen:
            continue
        seen.add(finding_key)
        finding_index += 1
        mitre = [item.strip() for item in str(row.get("mitre_techniques") or "").split(",") if item.strip()]
        campaigns = [item.strip() for item in str(row.get("campaigns") or "").split(",") if item.strip()]
        findings.append(
            {
                "finding_id": f"f-threat-intel-{finding_index:03d}",
                "type": "threat_intel_match",
                "severity": str(row.get("severity") or "info").lower(),
                "confidence": _safe_confidence(row.get("confidence"), 0.5),
                "title": f"Threat intelligence match: {row.get('entity')}",
                "description": f"{row.get('type', 'entity')} matched {row.get('threat_type', 'threat indicator')} from {row.get('source', 'unknown source')}.",
                "entities": {
                    "entity": row.get("entity"),
                    "entity_type": row.get("type"),
                    "threat_type": row.get("threat_type"),
                    "reputation_score": row.get("reputation_score"),
                    "source": row.get("source"),
                    "source_url": row.get("source_url"),
                    "coverage_mode": row.get("coverage_mode"),
                    "first_seen": row.get("first_seen"),
                    "last_seen": row.get("last_seen"),
                    "mitre_techniques": mitre,
                    "campaigns": campaigns,
                },
                "evidence_refs": ["e-threat-intel-matches"],
            }
        )

    return {
        "summary": result_summary,
        "findings": findings,
        "evidence": evidence,
        "diagnostics": {
            "warnings": warnings,
            "data_quality": {
                "entities_checked": summary.get("total_entities_checked", 0),
                "matches": len(matches),
                "deduplicated_findings": len(findings),
                "coverage_mode": matcher_status.get("coverage_mode", "unknown"),
                "loaded_feed_count": matcher_status.get("loaded_feed_count", 0),
                "loaded_indicators": matcher_status.get("total_loaded_indicators", 0),
            },
            "threat_intel": matcher_status,
        },
    }


def format_results(results: dict) -> str:
    output = []
    output.append("# Threat Intelligence Matching Results\n")

    summary = results["summary"]
    output.append("## Summary\n")
    output.append(f"- **Total Entities Checked**: {summary['total_entities_checked']}")
    output.append(f"- **Threat Matches Found**: {summary['threat_matches_found']}")
    output.append(f"- **Critical Threats**: {summary['critical_threats']}")
    output.append(f"- **High Threats**: {summary['high_threats']}")
    output.append(f"- **MITRE Techniques Mapped**: {summary['mitre_techniques_mapped']}")
    output.append(f"- **Campaigns Identified**: {summary['campaigns_identified']}\n")

    matcher_status = results.get("matcher_status", {})
    if matcher_status:
        output.append("## Coverage Status\n")
        output.append(f"- **Coverage Mode**: {matcher_status.get('coverage_mode', 'unknown')}")
        output.append(f"- **Loaded Feed Count**: {matcher_status.get('loaded_feed_count', 0)}")
        output.append(f"- **Loaded Indicators**: {matcher_status.get('total_loaded_indicators', 0)}")
        if matcher_status.get("feed_directory"):
            output.append(f"- **Feed Directory**: {matcher_status['feed_directory']}")
        if matcher_status.get("refresh_errors"):
            output.append(f"- **Refresh Errors**: {'; '.join(matcher_status['refresh_errors'])}")
        if matcher_status.get("coverage_warning"):
            output.append(f"- **Warning**: {matcher_status['coverage_warning']}")
        output.append("")

    if results["matches"]:
        output.append("## Threat Matches\n")
        output.append(
            format_dict_rows(
                results["matches"],
                [
                    "entity",
                    "type",
                    "threat_type",
                    "severity",
                    "source",
                    "coverage_mode",
                    "confidence",
                    "reputation_score",
                    "mitre_techniques",
                ],
            )
        )
    else:
        output.append("## Threat Matches\n")
        output.append("No threat matches were found in the selected scope.\n")

    for file_result in results["files_analyzed"]:
        if file_result.get("notes"):
            output.append(f"## Notes: {file_result['file']}\n")
            for note in file_result["notes"]:
                output.append(f"- {note}")

    append_file_errors(output, results)
    return "\n".join(output)
