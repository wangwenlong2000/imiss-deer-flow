from __future__ import annotations

import math
import statistics
from collections import defaultdict
from typing import Any

from analysis.anomaly_models import score_generic_candidates
from analysis.feature_engineering import rows_from_query
from core.schema_mapping import available_canonical_fields, ensure_required, quote_identifier
from utils.formatter import render_rows_section
from utils.math import _safe_float_local, _text_entropy_local, _coerce_event_seconds, _safe_ratio_local
from utils.path import _metadata_candidates_for_file, repo_root, to_repo_relative_display
from utils.zeek import _discover_zeek_logs, _load_zeek_json_rows, _zeek_value, _zeek_semantic_candidates, _signature_source_candidates


# Scoring thresholds and constants
DNS_QUERY_LENGTH_THRESHOLD = 35
DNS_QUERY_ENTROPY_THRESHOLD = 3.6
DNS_LONG_QUERY_RATIO_THRESHOLD = 0.3
DNS_HIGH_ENTROPY_RATIO_THRESHOLD = 0.3
DNS_NXDOMAIN_RATIO_THRESHOLD = 0.3
DNS_NXDOMAIN_MIN_QUERIES = 5
DNS_REGULARITY_INTERVAL_CV_THRESHOLD = 0.4
DNS_REGULARITY_MIN_RECORDS = 5
DNS_MIN_RECORDS = 2

DNS_QUERY_LENGTH_SCORE = 0.2
DNS_QUERY_ENTROPY_SCORE = 0.2
DNS_LONG_QUERY_RATIO_SCORE = 0.15
DNS_HIGH_ENTROPY_RATIO_SCORE = 0.15
DNS_NXDOMAIN_SCORE = 0.15
DNS_REGULARITY_SCORE = 0.15

DNS_UNIQUE_QUERY_CARDINALITY_THRESHOLD = 10
HIGH_RISK_THRESHOLD = 0.65
RULE_SCORE_THRESHOLD = 0.35

DNS_MAX_RESULTS_MULTIPLIER = 50
DNS_MAX_RESULTS_CAP = 1000
DNS_SCORING_CONTAMINATION = 0.15


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


def execute_dns_tunnel_review(
    con: Any,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    files: list[str],
    limit: int,
) -> dict[str, Any]:
    """Execute DNS tunnel review and return structured data dict."""
    available = available_canonical_fields(mappings)
    ensure_required(mappings, ["src_ip"])

    has_flow_dns = any(field in available for field in {"dns_query", "dns_query_length", "dns_label_count", "dns_query_entropy"})
    _, grouped_logs = _discover_zeek_logs(files)
    dns_log_rows = [row for path in grouped_logs.get("dns.log", []) for row in _load_zeek_json_rows(path)]

    result: dict[str, Any] = {
        "has_flow_dns": has_flow_dns,
        "has_zeek_dns": bool(dns_log_rows),
        "flow_candidate_map": {},
        "zeek_candidate_map": {},
        "scored": [],
        "warnings": [],
        "_limit": limit,
    }

    if not has_flow_dns and not dns_log_rows:
        result["warnings"].append({
            "code": "no_dns_evidence",
            "message": "No DNS lexical fields or Zeek dns.log artifacts were found for the selected scope.",
            "severity": "warning",
        })
        return result

    # Flow DNS candidates
    if has_flow_dns:
        timestamp_available = "analysis_time_relative_s" in available or "analysis_time_ts" in available
        event_time_expr = "COALESCE(analysis_time_relative_s, EXTRACT(EPOCH FROM analysis_time_ts))" if timestamp_available else "NULL"
        lexical_predicates: list[str] = []
        if "dns_query" in available:
            lexical_predicates.append("(dns_query IS NOT NULL AND dns_query != '')")
        if "dns_query_length" in available:
            lexical_predicates.append("COALESCE(dns_query_length, 0) > 0")
        if "dns_query_entropy" in available:
            lexical_predicates.append("COALESCE(dns_query_entropy, 0) > 0")
        flow_scope_predicate = " OR ".join(lexical_predicates) if lexical_predicates else "FALSE"
        flow_sql = f"""
            WITH dns_base AS (
                SELECT
                    src_ip,
                    COALESCE(dns_query, '') AS dns_query,
                    COALESCE(dns_query_length, LENGTH(COALESCE(dns_query, ''))) AS dns_query_length,
                    COALESCE(dns_label_count, CASE WHEN COALESCE(dns_query, '') = '' THEN 0 ELSE LENGTH(COALESCE(dns_query, '')) - LENGTH(REPLACE(COALESCE(dns_query, ''), '.', '')) + 1 END) AS dns_label_count,
                    COALESCE(dns_query_entropy, 0) AS dns_query_entropy,
                    {event_time_expr} AS event_time_s
                FROM flows
                {where_clause}
                {"AND" if where_clause else "WHERE"} src_ip IS NOT NULL
                  AND ({flow_scope_predicate})
            ),
            dns_intervals AS (
                SELECT
                    src_ip,
                    event_time_s - LAG(event_time_s) OVER (PARTITION BY src_ip ORDER BY event_time_s) AS delta_s
                FROM dns_base
                WHERE event_time_s IS NOT NULL
            )
            SELECT
                b.src_ip,
                COUNT(*) AS dns_records,
                COUNT(DISTINCT NULLIF(b.dns_query, '')) AS unique_queries,
                ROUND(AVG(NULLIF(b.dns_query_length, 0)), 2) AS avg_query_length,
                MAX(b.dns_query_length) AS max_query_length,
                ROUND(AVG(NULLIF(b.dns_label_count, 0)), 2) AS avg_label_count,
                ROUND(AVG(NULLIF(b.dns_query_entropy, 0)), 4) AS avg_query_entropy,
                ROUND(SUM(CASE WHEN b.dns_query_length >= {DNS_QUERY_LENGTH_THRESHOLD} THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0), 4) AS long_query_ratio,
                ROUND(SUM(CASE WHEN b.dns_query_entropy >= {DNS_QUERY_ENTROPY_THRESHOLD} THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0), 4) AS high_entropy_ratio,
                ROUND(STDDEV_SAMP(i.delta_s), 4) AS interval_std,
                ROUND(AVG(i.delta_s), 4) AS interval_mean,
                ROUND(COALESCE(STDDEV_SAMP(i.delta_s) / NULLIF(AVG(i.delta_s), 0), 0), 4) AS interval_cv
            FROM dns_base b
            LEFT JOIN dns_intervals i
              ON b.src_ip = i.src_ip
            GROUP BY 1
            HAVING COUNT(*) >= {DNS_MIN_RECORDS}
            ORDER BY dns_records DESC, unique_queries DESC, avg_query_entropy DESC, avg_query_length DESC, src_ip ASC
            LIMIT {max(limit * DNS_MAX_RESULTS_MULTIPLIER, DNS_MAX_RESULTS_CAP)}
        """
        _, flow_rows = rows_from_query(con, flow_sql)
        result["flow_candidate_map"] = {str(row.get("src_ip")): row for row in flow_rows}

    # Zeek DNS candidates
    if dns_log_rows:
        zeek_stats: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "src_ip": "UNKNOWN",
                "dns_queries": 0.0,
                "nxdomain_count": 0.0,
                "_queries": set(),
                "_query_lengths": [],
                "_label_counts": [],
                "_entropies": [],
                "_timestamps": [],
            }
        )
        for row in dns_log_rows:
            src_ip = _zeek_value(row, "id.orig_h", default="UNKNOWN")
            stats = zeek_stats[src_ip]
            stats["src_ip"] = src_ip
            stats["dns_queries"] += 1.0
            query = _zeek_value(row, "query", default="")
            if query:
                normalized = str(query).strip().lower().rstrip(".")
                stats["_queries"].add(normalized)
                stats["_query_lengths"].append(float(len(normalized)))
                stats["_label_counts"].append(float(normalized.count(".") + 1))
                stats["_entropies"].append(_text_entropy_local(normalized))
            rcode = _zeek_value(row, "rcode_name", default="UNKNOWN").upper()
            if rcode == "NXDOMAIN":
                stats["nxdomain_count"] += 1.0
            ts_value = _coerce_event_seconds(row.get("ts"))
            if ts_value is not None:
                stats["_timestamps"].append(ts_value)

        zeek_rows: list[dict[str, Any]] = []
        for stats in zeek_stats.values():
            timestamps = sorted(stats["_timestamps"])
            deltas = [timestamps[idx] - timestamps[idx - 1] for idx in range(1, len(timestamps)) if timestamps[idx] >= timestamps[idx - 1]]
            interval_mean = statistics.fmean(deltas) if deltas else 0.0
            interval_std = statistics.pstdev(deltas) if len(deltas) >= 2 else 0.0
            zeek_rows.append(
                {
                    "src_ip": stats["src_ip"],
                    "dns_queries": int(stats["dns_queries"]),
                    "unique_queries": len(stats["_queries"]),
                    "avg_query_length": round(statistics.fmean(stats["_query_lengths"]), 2) if stats["_query_lengths"] else 0.0,
                    "avg_label_count": round(statistics.fmean(stats["_label_counts"]), 2) if stats["_label_counts"] else 0.0,
                    "avg_query_entropy": round(statistics.fmean(stats["_entropies"]), 4) if stats["_entropies"] else 0.0,
                    "long_query_ratio": round(_safe_ratio_local(sum(1 for item in stats["_query_lengths"] if item >= DNS_QUERY_LENGTH_THRESHOLD), len(stats["_query_lengths"])), 4),
                    "high_entropy_ratio": round(_safe_ratio_local(sum(1 for item in stats["_entropies"] if item >= DNS_QUERY_ENTROPY_THRESHOLD), len(stats["_entropies"])), 4),
                    "nxdomain_ratio": round(_safe_ratio_local(stats["nxdomain_count"], stats["dns_queries"]), 4),
                    "interval_mean": round(interval_mean, 4),
                    "interval_std": round(interval_std, 4),
                    "interval_cv": round(_safe_ratio_local(interval_std, interval_mean), 4) if interval_mean > 0 else 0.0,
                }
            )
        result["zeek_candidate_map"] = {str(row.get("src_ip")): row for row in zeek_rows}

    # Merge and score
    flow_candidate_map = result["flow_candidate_map"]
    zeek_candidate_map = result["zeek_candidate_map"]
    merged: list[dict[str, Any]] = []
    for src_ip in sorted(set(flow_candidate_map) | set(zeek_candidate_map)):
        flow_row = flow_candidate_map.get(src_ip, {})
        zeek_row = zeek_candidate_map.get(src_ip, {})
        merged.append(
            {
                "src_ip": src_ip,
                "flow_dns_records": int(_safe_float_local(flow_row.get("dns_records"))),
                "flow_unique_queries": int(_safe_float_local(flow_row.get("unique_queries"))),
                "flow_avg_query_length": round(_safe_float_local(flow_row.get("avg_query_length")), 2),
                "flow_max_query_length": round(_safe_float_local(flow_row.get("max_query_length")), 2),
                "flow_avg_label_count": round(_safe_float_local(flow_row.get("avg_label_count")), 2),
                "flow_avg_query_entropy": round(_safe_float_local(flow_row.get("avg_query_entropy")), 4),
                "flow_long_query_ratio": round(_safe_float_local(flow_row.get("long_query_ratio")), 4),
                "flow_high_entropy_ratio": round(_safe_float_local(flow_row.get("high_entropy_ratio")), 4),
                "flow_interval_cv": round(_safe_float_local(flow_row.get("interval_cv")), 4),
                "zeek_dns_queries": int(_safe_float_local(zeek_row.get("dns_queries"))),
                "zeek_unique_queries": int(_safe_float_local(zeek_row.get("unique_queries"))),
                "zeek_avg_query_length": round(_safe_float_local(zeek_row.get("avg_query_length")), 2),
                "zeek_avg_label_count": round(_safe_float_local(zeek_row.get("avg_label_count")), 2),
                "zeek_avg_query_entropy": round(_safe_float_local(zeek_row.get("avg_query_entropy")), 4),
                "zeek_long_query_ratio": round(_safe_float_local(zeek_row.get("long_query_ratio")), 4),
                "zeek_high_entropy_ratio": round(_safe_float_local(zeek_row.get("high_entropy_ratio")), 4),
                "zeek_nxdomain_ratio": round(_safe_float_local(zeek_row.get("nxdomain_ratio")), 4),
                "zeek_interval_cv": round(_safe_float_local(zeek_row.get("interval_cv")), 4),
            }
        )

    def dns_tunnel_rule_score(row: dict[str, Any]) -> float:
        score = 0.0
        if _safe_float_local(row.get("flow_avg_query_length")) >= DNS_QUERY_LENGTH_THRESHOLD or _safe_float_local(row.get("zeek_avg_query_length")) >= DNS_QUERY_LENGTH_THRESHOLD:
            score += DNS_QUERY_LENGTH_SCORE
        if _safe_float_local(row.get("flow_avg_query_entropy")) >= DNS_QUERY_ENTROPY_THRESHOLD or _safe_float_local(row.get("zeek_avg_query_entropy")) >= DNS_QUERY_ENTROPY_THRESHOLD:
            score += DNS_QUERY_ENTROPY_SCORE
        if _safe_float_local(row.get("flow_long_query_ratio")) >= DNS_LONG_QUERY_RATIO_THRESHOLD or _safe_float_local(row.get("zeek_long_query_ratio")) >= DNS_LONG_QUERY_RATIO_THRESHOLD:
            score += DNS_LONG_QUERY_RATIO_SCORE
        if _safe_float_local(row.get("flow_high_entropy_ratio")) >= DNS_HIGH_ENTROPY_RATIO_THRESHOLD or _safe_float_local(row.get("zeek_high_entropy_ratio")) >= DNS_HIGH_ENTROPY_RATIO_THRESHOLD:
            score += DNS_HIGH_ENTROPY_RATIO_SCORE
        if _safe_float_local(row.get("zeek_nxdomain_ratio")) >= DNS_NXDOMAIN_RATIO_THRESHOLD and _safe_float_local(row.get("zeek_dns_queries")) >= DNS_NXDOMAIN_MIN_QUERIES:
            score += DNS_NXDOMAIN_SCORE
        regularity_candidates = [item for item in [_safe_float_local(row.get("flow_interval_cv")), _safe_float_local(row.get("zeek_interval_cv"))] if item > 0]
        regularity = min(regularity_candidates) if regularity_candidates else None
        if regularity is not None and regularity <= DNS_REGULARITY_INTERVAL_CV_THRESHOLD and (_safe_float_local(row.get("flow_dns_records")) >= DNS_REGULARITY_MIN_RECORDS or _safe_float_local(row.get("zeek_dns_queries")) >= DNS_REGULARITY_MIN_RECORDS):
            score += DNS_REGULARITY_SCORE
        return min(1.0, score)

    def dns_tunnel_reason(row: dict[str, Any], final_score: float, rule_score: float) -> str:
        reasons: list[str] = []
        if _safe_float_local(row.get("flow_avg_query_entropy")) >= DNS_QUERY_ENTROPY_THRESHOLD or _safe_float_local(row.get("zeek_avg_query_entropy")) >= DNS_QUERY_ENTROPY_THRESHOLD:
            reasons.append("high_entropy_dns_queries")
        if _safe_float_local(row.get("flow_avg_query_length")) >= DNS_QUERY_LENGTH_THRESHOLD or _safe_float_local(row.get("zeek_avg_query_length")) >= DNS_QUERY_LENGTH_THRESHOLD:
            reasons.append("long_dns_queries")
        if _safe_float_local(row.get("zeek_nxdomain_ratio")) >= DNS_NXDOMAIN_RATIO_THRESHOLD:
            reasons.append("nxdomain_heavy_dns_pattern")
        regularity_candidates = [item for item in [_safe_float_local(row.get("flow_interval_cv")), _safe_float_local(row.get("zeek_interval_cv"))] if item > 0]
        regularity = min(regularity_candidates) if regularity_candidates else None
        if regularity is not None and regularity <= DNS_REGULARITY_INTERVAL_CV_THRESHOLD:
            reasons.append("regular_dns_intervals")
        if _safe_float_local(row.get("flow_unique_queries")) >= DNS_UNIQUE_QUERY_CARDINALITY_THRESHOLD or _safe_float_local(row.get("zeek_unique_queries")) >= DNS_UNIQUE_QUERY_CARDINALITY_THRESHOLD:
            reasons.append("high_dns_query_cardinality")
        if not reasons and final_score >= HIGH_RISK_THRESHOLD:
            reasons.append("model_ranked_dns_tunnel_candidate")
        if not reasons and rule_score >= RULE_SCORE_THRESHOLD:
            reasons.append("rule_ranked_dns_tunnel_candidate")
        return ",".join(reasons) if reasons else "mixed_low_signal_dns_activity"

    scored = score_generic_candidates(
        merged,
        numeric_fields=[
            "flow_dns_records", "flow_unique_queries", "flow_avg_query_length",
            "flow_max_query_length", "flow_avg_label_count", "flow_avg_query_entropy",
            "flow_long_query_ratio", "flow_high_entropy_ratio", "flow_interval_cv",
            "zeek_dns_queries", "zeek_unique_queries", "zeek_avg_query_length",
            "zeek_avg_label_count", "zeek_avg_query_entropy", "zeek_long_query_ratio",
            "zeek_high_entropy_ratio", "zeek_nxdomain_ratio", "zeek_interval_cv",
        ],
        categorical_fields=[],
        rule_score_fn=dns_tunnel_rule_score,
        reason_fn=dns_tunnel_reason,
        output_field="dns_tunnel_risk_score",
        contamination=DNS_SCORING_CONTAMINATION,
        engine="hybrid",
    )
    result["scored"] = scored

    return result


def format_dns_tunnel_review(data: dict[str, Any]) -> str:
    """Produce the text report for backward-compatible output."""
    sections: list[str] = []
    scored = data["scored"]
    limit = data.get("_limit", 10)

    if not data["has_flow_dns"] and not data["has_zeek_dns"]:
        return (
            "DNS tunnel review\n"
            "No DNS lexical fields or Zeek dns.log artifacts were found for the selected scope. "
            "Re-run prepare_pcap.py with DNS-aware preprocessing or choose a dataset containing DNS evidence."
        )

    sections.append(
        render_rows_section(
            "DNS tunnel hotspots",
            ["src_ip", "dns_tunnel_risk_score", "severity", "flow_unique_queries", "flow_avg_query_entropy", "flow_long_query_ratio", "zeek_nxdomain_ratio", "zeek_interval_cv", "likely_reason"],
            [
                (
                    row.get("src_ip"),
                    row.get("dns_tunnel_risk_score"),
                    row.get("severity"),
                    row.get("flow_unique_queries"),
                    row.get("flow_avg_query_entropy"),
                    row.get("flow_long_query_ratio"),
                    row.get("zeek_nxdomain_ratio"),
                    row.get("zeek_interval_cv"),
                    row.get("likely_reason"),
                )
                for row in scored[:limit]
            ],
        )
    )

    if not data["has_zeek_dns"]:
        sections.append("Zeek dns.log was not available for this scope, so DNS tunnel scoring currently relies on flow-level lexical and timing features only.")
    if not data["has_flow_dns"]:
        sections.append("Flow-level DNS lexical fields were not available, so DNS tunnel scoring is currently relying on Zeek DNS semantics only.")

    return "\n\n".join(section for section in sections if section)


def build_skill_result_parts(data: dict[str, Any], raw_output: str) -> dict[str, Any]:
    """Build structured SkillResult for dns-tunnel-review action."""
    evidence: list[dict[str, Any]] = []
    findings: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    metrics: list[dict[str, Any]] = []
    scored = data["scored"]
    limit = data.get("_limit", 10)

    # Metrics
    high_risk = sum(1 for r in scored if float(r.get("dns_tunnel_risk_score", 0.0)) >= HIGH_RISK_THRESHOLD)
    max_score = max((float(r.get("dns_tunnel_risk_score", 0.0)) for r in scored), default=0.0)
    candidate_sources = len(scored)
    flow_sources = len(data.get("flow_candidate_map", {}))
    zeek_sources = len(data.get("zeek_candidate_map", {}))

    metrics.append({"name": "candidate_sources", "value": candidate_sources})
    metrics.append({"name": "flow_dns_sources", "value": flow_sources})
    metrics.append({"name": "zeek_dns_sources", "value": zeek_sources})
    metrics.append({"name": "high_risk_sources", "value": high_risk})
    metrics.append({"name": "max_dns_tunnel_risk_score", "value": round(max_score, 4)})

    # DNS tunnel metrics evidence
    evidence.append({
        "evidence_id": "e-dns-tunnel-metrics",
        "type": "metric",
        "title": "DNS Tunnel Review Metrics",
        "metrics": metrics,
    })

    # Hotspots table
    if scored:
        hotspot_columns = [
            "src_ip", "dns_tunnel_risk_score", "severity",
            "flow_unique_queries", "flow_avg_query_entropy", "flow_long_query_ratio",
            "zeek_nxdomain_ratio", "zeek_interval_cv", "likely_reason",
        ]
        hotspot_rows = [
            [
                r.get("src_ip"), r.get("dns_tunnel_risk_score"), r.get("severity"),
                r.get("flow_unique_queries"), r.get("flow_avg_query_entropy"),
                r.get("flow_long_query_ratio"), r.get("zeek_nxdomain_ratio"),
                r.get("zeek_interval_cv"), r.get("likely_reason"),
            ]
            for r in scored[:limit]
        ]
        evidence.append({
            "evidence_id": "e-dns-tunnel-hotspots",
            "type": "table",
            "title": "DNS Tunnel Hotspots",
            "columns": hotspot_columns,
            "rows": hotspot_rows,
        })

    # Optional: flow DNS lexical candidates table
    flow_map = data.get("flow_candidate_map", {})
    if flow_map:
        flow_cols = ["src_ip", "dns_records", "unique_queries", "avg_query_length", "max_query_length", "avg_query_entropy", "long_query_ratio", "high_entropy_ratio", "interval_cv"]
        flow_rows = [
            [
                v.get("src_ip"), v.get("dns_records"), v.get("unique_queries"),
                v.get("avg_query_length"), v.get("max_query_length"), v.get("avg_query_entropy"),
                v.get("long_query_ratio"), v.get("high_entropy_ratio"), v.get("interval_cv"),
            ]
            for v in list(flow_map.values())[:limit]
        ]
        evidence.append({
            "evidence_id": "e-flow-dns-lexical-candidates",
            "type": "table",
            "title": "Flow DNS Lexical Candidates",
            "columns": flow_cols,
            "rows": flow_rows,
        })

    # Optional: Zeek DNS semantic candidates table
    zeek_map = data.get("zeek_candidate_map", {})
    if zeek_map:
        zeek_cols = ["src_ip", "dns_queries", "unique_queries", "avg_query_length", "avg_query_entropy", "long_query_ratio", "high_entropy_ratio", "nxdomain_ratio", "interval_cv"]
        zeek_rows = [
            [
                v.get("src_ip"), v.get("dns_queries"), v.get("unique_queries"),
                v.get("avg_query_length"), v.get("avg_query_entropy"),
                v.get("long_query_ratio"), v.get("high_entropy_ratio"),
                v.get("nxdomain_ratio"), v.get("interval_cv"),
            ]
            for v in list(zeek_map.values())[:limit]
        ]
        evidence.append({
            "evidence_id": "e-zeek-dns-semantic-candidates",
            "type": "table",
            "title": "Zeek DNS Semantic Candidates",
            "columns": zeek_cols,
            "rows": zeek_rows,
        })

    # Findings for high-risk sources
    for row in scored[:limit]:
        score = float(row.get("dns_tunnel_risk_score", 0.0))
        if score >= HIGH_RISK_THRESHOLD:
            severity = str(row.get("severity") or "medium").lower()
            findings.append({
                "finding_id": f"f-dns-tunnel-{row.get('src_ip', 'unknown')}",
                "type": "dns_tunnel_candidate",
                "severity": severity,
                "confidence": score,
                "title": f"DNS tunnel candidate: {row.get('src_ip')}",
                "description": row.get("likely_reason") or "Source flagged as DNS tunnel candidate.",
                "entities": [{"type": "src_ip", "value": row.get("src_ip")}],
                "evidence_refs": ["e-dns-tunnel-hotspots", "e-dns-tunnel-metrics"],
            })

    # Warnings for missing data
    if not data["has_zeek_dns"]:
        warnings.append({
            "code": "no_zeek_dns_log",
            "message": "Zeek dns.log was not available for this scope. DNS tunnel scoring relies on flow-level lexical and timing features only.",
            "severity": "info",
        })
    if not data["has_flow_dns"]:
        warnings.append({
            "code": "no_flow_dns_fields",
            "message": "Flow-level DNS lexical fields were not available. DNS tunnel scoring relies on Zeek DNS semantics only.",
            "severity": "info",
        })
    if not data["has_zeek_dns"] and not data["has_flow_dns"]:
        warnings.append({
            "code": "no_dns_evidence",
            "message": "Neither Zeek DNS log nor flow DNS fields are available. No DNS tunnel analysis possible.",
            "severity": "warning",
        })

    # Raw report
    evidence.append({
        "evidence_id": "e-raw-report",
        "type": "text",
        "title": "Raw DNS Tunnel Review Output",
        "content": raw_output,
    })

    # Fix evidence_refs
    existing_ids = {e["evidence_id"] for e in evidence}
    for finding in findings:
        finding["evidence_refs"] = [ref for ref in finding["evidence_refs"] if ref in existing_ids]

    # Compute overall severity
    severity_order = {"critical": 4, "high": 3, "medium": 2, "low": 1, "info": 0}
    highest = "info"
    for f in findings:
        if severity_order.get(f.get("severity", "info"), 0) > severity_order.get(highest, 0):
            highest = f["severity"]

    overview_text = "DNS tunnel review."
    if findings:
        overview_text += f" {len(findings)} finding(s) identified."

    return {
        "summary": {
            "title": "DNS Tunnel Review",
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
                "has_flow_dns": data["has_flow_dns"],
                "has_zeek_dns": data["has_zeek_dns"],
            },
        },
    }


# Backward-compatible entry point
def dns_tunnel_review_action(
    con: Any,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    files: list[str],
    limit: int,
) -> str:
    """Legacy entry point — delegates to execute + format for backward compatibility."""
    data = execute_dns_tunnel_review(con, mappings, where_clause, files, limit)
    return format_dns_tunnel_review(data)
