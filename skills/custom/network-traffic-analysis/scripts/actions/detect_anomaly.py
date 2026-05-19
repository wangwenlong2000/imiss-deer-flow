from __future__ import annotations

from typing import Any

import duckdb  # type: ignore

from analysis.anomaly_models import score_generic_candidates, score_scan_candidates
from analysis.feature_engineering import (
    failure_rate_candidate_sql,
    handshake_failure_candidate_sql,
    icmp_probe_candidate_sql,
    rare_port_candidate_sql,
    rows_from_query,
    rst_heavy_candidate_sql,
    scan_candidate_sql,
    small_packet_burst_candidate_sql,
    volume_spike_candidate_sql,
)
from utils.formatter import _rows_to_tuples, render_rows_section, render_section
from core.schema_mapping import ensure_required
from utils.sql import analysis_time_bucket_expr, legacy_detect_anomaly_sql


def where_to_and_clause(where_clause: str) -> str:
    """Convert ``""`` or ``"WHERE ..."`` into an ``AND ...`` suffix.

    Returns ``""`` when there are no user filters, otherwise ``" AND <conditions>"``.
    """
    stripped = where_clause.strip()
    if not stripped:
        return ""
    if stripped.upper().startswith("WHERE"):
        body = stripped[len("WHERE"):].strip()
        return f" AND {body}" if body else ""
    if stripped.upper().startswith("AND"):
        return f" {stripped}"
    raise ValueError(f"Unsupported SQL clause shape: {where_clause!r}")


# ---------------------------------------------------------------------------
# Hybrid / ML scoring path
# ---------------------------------------------------------------------------

def _run_anomaly_detection(
    con: duckdb.DuckDBPyConnection,
    mappings: dict[str, dict[str, str]],
    and_clause: str,
    *,
    rule: str,
    engine: str,
    limit: int,
) -> list[dict[str, Any]]:
    """Run anomaly detection with ML scoring and return ranked candidates."""
    from analysis.anomaly_models import score_timeseries_rcf

    if rule == "scan-source":
        _, rows = rows_from_query(con, scan_candidate_sql(and_clause, packet_view=False, limit=max(limit * 4, 200)))
        return score_scan_candidates(rows, packet_view=False, engine=engine)[:limit]

    if rule == "rare-port":
        _, rows = rows_from_query(con, rare_port_candidate_sql(and_clause, limit=max(limit * 4, 200)))
        return score_generic_candidates(
            rows,
            numeric_fields=["records", "unique_src_ip", "unique_dst_ip", "total_bytes", "unique_protocols", "unique_app_protocols"],
            categorical_fields=["dst_port"],
            rule_score_fn=lambda row: (
                0.55 if float(row.get("records") or 0) <= 3 else 0.15
            ) + (
                0.20 if float(row.get("unique_src_ip") or 0) >= 3 else 0.0
            ) + (
                0.15 if float(row.get("unique_dst_ip") or 0) >= 3 else 0.0
            ) + (
                0.10 if float(row.get("unique_protocols") or 0) >= 2 else 0.0
            ),
            reason_fn=lambda row, final, rule_score: "rare_low_frequency_port" if float(row.get("records") or 0) <= 3 else "port_behavior_outlier",
            output_field="rare_port_risk_score",
            contamination=0.12,
            engine=engine,
        )[:limit]

    if rule == "failure-rate":
        _, rows = rows_from_query(con, failure_rate_candidate_sql(and_clause, limit=max(limit * 4, 200)))
        return score_generic_candidates(
            rows,
            numeric_fields=["flows", "negative_outcomes", "negative_pct", "unique_actions", "unique_dst_ip", "unique_dst_port", "total_bytes", "avg_bytes"],
            categorical_fields=["src_ip"],
            rule_score_fn=lambda row: (
                0.55 if float(row.get("negative_pct") or 0) >= 40 else 0.30 if float(row.get("negative_pct") or 0) >= 20 else 0.0
            ) + (
                0.20 if float(row.get("negative_outcomes") or 0) >= 5 else 0.0
            ) + (
                0.15 if float(row.get("flows") or 0) >= 20 else 0.0
            ) + (
                0.10 if float(row.get("unique_actions") or 0) >= 2 else 0.0
            ),
            reason_fn=lambda row, final, rule_score: "failure_heavy_source" if float(row.get("negative_pct") or 0) >= 20 else "failure_pattern_outlier",
            output_field="failure_risk_score",
            contamination=0.15,
            engine=engine,
        )[:limit]

    if rule == "volume-spike":
        _, rows = rows_from_query(
            con,
            volume_spike_candidate_sql(and_clause, bucket_expr=analysis_time_bucket_expr("hour"), limit=max(limit * 4, 200)),
        )
        rcf_scores = score_timeseries_rcf(rows, numeric_fields=["records", "total_bytes", "total_packets"])
        for index, row in enumerate(rows):
            row["_rcf_score"] = rcf_scores[index]
        ranked = score_generic_candidates(
            rows,
            numeric_fields=["records", "total_bytes", "total_packets", "unique_src_ip", "unique_dst_ip"],
            categorical_fields=["bucket"],
            rule_score_fn=lambda row: min(
                1.0,
                (0.55 if float(row.get("total_bytes") or 0) > 0 else 0.0)
                + (0.20 if float(row.get("records") or 0) >= 100 else 0.0)
                + (0.15 if float(row.get("unique_src_ip") or 0) >= 10 else 0.0)
                + (0.10 if float(row.get("unique_dst_ip") or 0) >= 10 else 0.0),
            ),
            reason_fn=lambda row, final, rule_score: "rcf_detected_bucket_outlier" if float(row.get("_rcf_score") or 0) >= 0.7 else ("traffic_volume_spike" if float(row.get("total_bytes") or 0) > 0 else "bucket_behavior_outlier"),
            output_field="volume_spike_score",
            contamination=0.1,
            engine="iforest" if engine == "iforest" else "lof" if engine == "lof" else "hybrid",
        )
        for row in ranked:
            rcf_score = float(row.get("_rcf_score", 0.0))
            row["rcf_score"] = round(rcf_score, 4)
            base_score = float(row.get("volume_spike_score", 0.0))
            if engine == "rcf":
                row["volume_spike_score"] = rcf_score
            elif engine == "hybrid":
                row["volume_spike_score"] = round(0.5 * base_score + 0.5 * rcf_score, 4)
            row["severity"] = "critical" if float(row.get("volume_spike_score", 0.0)) >= 0.85 else "high" if float(row.get("volume_spike_score", 0.0)) >= 0.65 else "medium" if float(row.get("volume_spike_score", 0.0)) >= 0.45 else "low"
        ranked = sorted(ranked, key=lambda item: float(item.get("volume_spike_score", 0.0)), reverse=True)[:limit]
        return ranked

    if rule == "syn-scan":
        ensure_required(mappings, ["src_ip", "dst_ip", "dst_port", "tcp_flags"])
        _, rows = rows_from_query(con, scan_candidate_sql(and_clause, packet_view=True, limit=max(limit * 4, 200)))
        return score_scan_candidates(rows, packet_view=True, engine=engine)[:limit]

    if rule == "rst-heavy":
        ensure_required(mappings, ["src_ip", "tcp_flags"])
        _, rows = rows_from_query(con, rst_heavy_candidate_sql(and_clause, limit=max(limit * 4, 200)))
        return score_generic_candidates(
            rows,
            numeric_fields=["packets", "rst_packets", "rst_pct", "unique_dst_ip", "unique_dst_port", "total_bytes"],
            categorical_fields=["src_ip"],
            rule_score_fn=lambda row: (
                0.55 if float(row.get("rst_pct") or 0) >= 30 else 0.30 if float(row.get("rst_pct") or 0) >= 15 else 0.0
            ) + (
                0.20 if float(row.get("rst_packets") or 0) >= 10 else 0.0
            ) + (
                0.15 if float(row.get("unique_dst_ip") or 0) >= 5 else 0.0
            ) + (
                0.10 if float(row.get("unique_dst_port") or 0) >= 10 else 0.0
            ),
            reason_fn=lambda row, final, rule_score: "rst_dominant_source" if float(row.get("rst_pct") or 0) >= 15 else "reset_behavior_outlier",
            output_field="rst_heavy_score",
            contamination=0.14,
            engine=engine,
        )[:limit]

    if rule == "handshake-failure":
        ensure_required(mappings, ["src_ip", "dst_ip", "tcp_flags"])
        _, rows = rows_from_query(con, handshake_failure_candidate_sql(and_clause, limit=max(limit * 4, 200)))
        return score_generic_candidates(
            rows,
            numeric_fields=["packets", "syn_only_packets", "syn_ack_packets", "rst_packets", "unique_dst_ip", "unique_dst_port"],
            categorical_fields=["src_ip"],
            rule_score_fn=lambda row: min(
                1.0,
                (0.45 if float(row.get("syn_only_packets") or 0) > float(row.get("syn_ack_packets") or 0) else 0.0)
                + (0.20 if float(row.get("rst_packets") or 0) > 0 else 0.0)
                + (0.20 if float(row.get("unique_dst_ip") or 0) >= 5 else 0.0)
                + (0.15 if float(row.get("unique_dst_port") or 0) >= 10 else 0.0),
            ),
            reason_fn=lambda row, final, rule_score: "tcp_handshake_failure_pattern" if float(row.get("syn_only_packets") or 0) > float(row.get("syn_ack_packets") or 0) else "handshake_behavior_outlier",
            output_field="handshake_failure_score",
            contamination=0.14,
            engine=engine,
        )[:limit]

    if rule == "icmp-probe":
        ensure_required(mappings, ["src_ip", "dst_ip", "icmp_type"])
        _, rows = rows_from_query(con, icmp_probe_candidate_sql(and_clause, limit=max(limit * 4, 200)))
        return score_generic_candidates(
            rows,
            numeric_fields=["packets", "unique_dst_ip", "unique_icmp_type", "unique_icmp_code", "total_bytes"],
            categorical_fields=["src_ip"],
            rule_score_fn=lambda row: (
                0.45 if float(row.get("unique_dst_ip") or 0) >= 5 else 0.20 if float(row.get("unique_dst_ip") or 0) >= 3 else 0.0
            ) + (
                0.25 if float(row.get("packets") or 0) >= 10 else 0.0
            ) + (
                0.15 if float(row.get("unique_icmp_type") or 0) >= 2 else 0.0
            ) + (
                0.15 if float(row.get("unique_icmp_code") or 0) >= 2 else 0.0
            ),
            reason_fn=lambda row, final, rule_score: "icmp_probe_pattern" if float(row.get("unique_dst_ip") or 0) >= 3 else "icmp_behavior_outlier",
            output_field="icmp_probe_score",
            contamination=0.12,
            engine=engine,
        )[:limit]

    if rule == "small-packet-burst":
        _, rows = rows_from_query(con, small_packet_burst_candidate_sql(and_clause, limit=max(limit * 4, 200)))
        return score_generic_candidates(
            rows,
            numeric_fields=["packets", "small_packets", "small_packet_pct", "unique_dst_ip", "unique_dst_port", "total_bytes", "avg_payload_bytes"],
            categorical_fields=["src_ip"],
            rule_score_fn=lambda row: (
                0.45 if float(row.get("small_packet_pct") or 0) >= 60 else 0.25 if float(row.get("small_packet_pct") or 0) >= 40 else 0.0
            ) + (
                0.20 if float(row.get("small_packets") or 0) >= 20 else 0.0
            ) + (
                0.15 if float(row.get("packets") or 0) >= 30 else 0.0
            ) + (
                0.10 if float(row.get("unique_dst_ip") or 0) >= 5 else 0.0
            ) + (
                0.10 if float(row.get("unique_dst_port") or 0) >= 10 else 0.0
            ),
            reason_fn=lambda row, final, rule_score: "small_packet_burst_pattern" if float(row.get("small_packet_pct") or 0) >= 40 else "packet_size_outlier",
            output_field="small_packet_burst_score",
            contamination=0.14,
            engine=engine,
        )[:limit]

    return []


# ---------------------------------------------------------------------------
# Pure rule (legacy SQL) path
# ---------------------------------------------------------------------------

def _run_legacy_rule_detection(
    con: duckdb.DuckDBPyConnection,
    rule: str,
    and_clause: str,
    *,
    limit: int,
) -> tuple[list[str], list[dict[str, Any]]]:
    """Execute legacy SQL rule and return (columns, rows). Raises on SQL error."""
    sql = legacy_detect_anomaly_sql(rule, and_clause)
    return rows_from_query(con, sql)


# ---------------------------------------------------------------------------
# Column / score / entity metadata
# ---------------------------------------------------------------------------

def _rule_columns(rule: str) -> list[str]:
    column_map = {
        "scan-source": [
            "src_ip", "flows", "unique_dst_ip", "unique_dst_port",
            "rule_score", "iforest_score", "lof_score", "rcf_score",
            "scan_risk_score", "severity", "likely_reason",
        ],
        "syn-scan": [
            "src_ip", "packets", "unique_dst_ip", "unique_dst_port",
            "syn_only_packets", "syn_only_pct", "rule_score", "iforest_score",
            "lof_score", "rcf_score", "scan_risk_score", "severity", "likely_reason",
        ],
        "rare-port": [
            "dst_port", "records", "unique_src_ip", "unique_dst_ip",
            "rule_score", "iforest_score", "lof_score", "rcf_score",
            "rare_port_risk_score", "severity", "likely_reason",
        ],
        "failure-rate": [
            "src_ip", "flows", "negative_outcomes", "negative_pct",
            "rule_score", "iforest_score", "lof_score", "rcf_score",
            "failure_risk_score", "severity", "likely_reason",
        ],
        "volume-spike": [
            "bucket", "records", "total_bytes", "total_packets",
            "rule_score", "iforest_score", "lof_score", "rcf_score",
            "volume_spike_score", "severity", "likely_reason",
        ],
        "rst-heavy": [
            "src_ip", "packets", "rst_packets", "rst_pct",
            "rule_score", "iforest_score", "lof_score", "rcf_score",
            "rst_heavy_score", "severity", "likely_reason",
        ],
        "handshake-failure": [
            "src_ip", "packets", "syn_only_packets", "syn_ack_packets", "rst_packets",
            "rule_score", "iforest_score", "lof_score", "rcf_score",
            "handshake_failure_score", "severity", "likely_reason",
        ],
        "icmp-probe": [
            "src_ip", "packets", "unique_dst_ip", "unique_icmp_type", "unique_icmp_code",
            "rule_score", "iforest_score", "lof_score", "rcf_score",
            "icmp_probe_score", "severity", "likely_reason",
        ],
        "small-packet-burst": [
            "src_ip", "packets", "small_packets", "small_packet_pct",
            "rule_score", "iforest_score", "lof_score", "rcf_score",
            "small_packet_burst_score", "severity", "likely_reason",
        ],
    }
    return column_map.get(rule, [])


def _risk_score_field(rule: str) -> str:
    field_map = {
        "scan-source": "scan_risk_score",
        "syn-scan": "scan_risk_score",
        "rare-port": "rare_port_risk_score",
        "failure-rate": "failure_risk_score",
        "volume-spike": "volume_spike_score",
        "rst-heavy": "rst_heavy_score",
        "handshake-failure": "handshake_failure_score",
        "icmp-probe": "icmp_probe_score",
        "small-packet-burst": "small_packet_burst_score",
    }
    return field_map.get(rule, "risk_score")


def _entity_type_for_rule(rule: str) -> str:
    if rule in {"rst-heavy", "handshake-failure", "icmp-probe", "small-packet-burst"}:
        return "src_ip"
    if rule == "rare-port":
        return "dst_port"
    if rule == "volume-spike":
        return "bucket"
    return "src_ip"


# ---------------------------------------------------------------------------
# SkillResult builders
# ---------------------------------------------------------------------------

def _build_evidence_from_ranked(rule: str, engine: str, ranked: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build evidence list from ML-scored ranked candidates."""
    evidence: list[dict[str, Any]] = []
    score_field = _risk_score_field(rule)
    columns = _rule_columns(rule)

    if ranked:
        evidence.append({
            "evidence_id": "e-anomaly-table",
            "type": "table",
            "title": f"Anomaly Results: {rule}",
            "columns": columns,
            "rows": [[row.get(col, "") for col in columns] for row in ranked],
        })

    max_score = 0.0
    high_severity_count = 0
    for row in ranked:
        score = float(row.get(score_field, 0.0))
        if score > max_score:
            max_score = score
        if row.get("severity", "low") in {"critical", "high"}:
            high_severity_count += 1

    evidence.append({
        "evidence_id": "e-anomaly-metrics",
        "type": "metric",
        "title": "Anomaly Detection Metrics",
        "metrics": [
            {"name": "rule", "value": rule},
            {"name": "engine", "value": engine},
            {"name": "candidates_returned", "value": len(ranked)},
            {"name": "max_risk_score", "value": round(max_score, 4)},
            {"name": "high_severity_count", "value": high_severity_count},
        ],
    })
    return evidence


def _build_evidence_from_legacy(rule: str, columns: list[str], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build evidence list from legacy SQL results."""
    evidence: list[dict[str, Any]] = []
    if rows:
        evidence.append({
            "evidence_id": "e-anomaly-table",
            "type": "table",
            "title": f"Anomaly Results: {rule} (rule engine)",
            "columns": columns,
            "rows": [[row.get(col, "") for col in columns] for row in rows],
        })
    evidence.append({
        "evidence_id": "e-anomaly-metrics",
        "type": "metric",
        "title": "Anomaly Detection Metrics",
        "metrics": [
            {"name": "rule", "value": rule},
            {"name": "engine", "value": "rule"},
            {"name": "candidates_returned", "value": len(rows)},
            {"name": "max_risk_score", "value": 0.0},
            {"name": "high_severity_count", "value": 0},
        ],
    })
    return evidence


def _build_findings(rule: str, ranked: list[dict[str, Any]], score_field: str) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    entity_type = _entity_type_for_rule(rule)

    max_score = max((float(r.get(score_field, 0.0)) for r in ranked), default=0.0)
    high_severity_count = sum(1 for r in ranked if r.get("severity", "low") in {"critical", "high"})

    top_row = ranked[0] if ranked else None
    if top_row and max_score >= 0.65:
        entity_value = top_row.get(entity_type, "unknown")
        findings.append({
            "finding_id": "f-anomaly-top-risk",
            "type": "anomaly",
            "severity": top_row.get("severity", "high"),
            "confidence": round(max_score, 2),
            "title": f"High-risk {rule} detected: {entity_value}",
            "description": (
                f"The top candidate for rule '{rule}' ({entity_type}={entity_value}) "
                f"has a risk score of {max_score:.2f} ({top_row.get('likely_reason', 'unknown')})."
            ),
            "entities": [{"type": entity_type, "value": str(entity_value)}],
            "evidence_refs": ["e-anomaly-table", "e-anomaly-metrics"],
            "recommended_actions": [
                f"Investigate {entity_value} for {rule.replace('-', ' ')} behavior.",
                "Cross-reference with session-review or packet-review for deeper context.",
            ],
        })

    if high_severity_count > 0:
        findings.append({
            "finding_id": "f-anomaly-severity-summary",
            "type": "severity_summary",
            "severity": "high" if high_severity_count > 1 else "medium",
            "confidence": 0.8,
            "title": f"{high_severity_count} candidate(s) with high or critical severity",
            "description": f"{high_severity_count} out of {len(ranked)} candidates for rule '{rule}' are rated as high or critical severity.",
            "entities": [],
            "evidence_refs": ["e-anomaly-metrics"],
            "recommended_actions": ["Prioritize high-severity candidates for immediate investigation."],
        })

    return findings


def _overall_severity(ranked: list[dict], score_field: str) -> str:
    max_score = max((float(r.get(score_field, 0.0)) for r in ranked), default=0.0)
    high_severity_count = sum(1 for r in ranked if r.get("severity", "low") in {"critical", "high"})
    if max_score >= 0.85 or high_severity_count >= 3:
        return "critical"
    if max_score >= 0.65 or high_severity_count >= 1:
        return "high"
    if max_score >= 0.45:
        return "medium"
    if ranked:
        return "low"
    return "info"


def build_skill_result_parts(
    rule: str,
    engine: str,
    ranked: list[dict[str, Any]],
    raw_output: str,
) -> dict[str, Any]:
    """Build structured SkillResult for detect-anomaly with ML scoring."""
    score_field = _risk_score_field(rule)
    evidence = _build_evidence_from_ranked(rule, engine, ranked)

    return {
        "summary": {
            "title": f"Anomaly Detection: {rule}",
            "overview": (
                f"Detected {len(ranked)} anomaly candidate(s) for rule '{rule}' "
                f"using engine '{engine}'. Max risk score: {max((float(r.get(score_field, 0.0)) for r in ranked), default=0.0):.2f}."
            ),
            "severity": _overall_severity(ranked, score_field),
            "confidence": round(max((float(r.get(score_field, 0.0)) for r in ranked), default=0.0), 2) if ranked else None,
            "key_metrics": [m for m in evidence[-1]["metrics"][:4]],
        },
        "findings": _build_findings(rule, ranked, score_field),
        "evidence": evidence,
        "artifacts": [],
        "diagnostics": {
            "warnings": [],
            "data_quality": {},
        },
    }


def build_legacy_rule_skill_result_parts(
    rule: str,
    columns: list[str],
    rows: list[dict[str, Any]],
    raw_output: str,
) -> dict[str, Any]:
    """Build structured SkillResult for detect-anomaly in rule (legacy SQL) mode."""
    return {
        "summary": {
            "title": f"Anomaly Detection: {rule}",
            "overview": f"Rule-based detection for '{rule}' returned {len(rows)} row(s).",
            "severity": "info",
            "confidence": None,
            "key_metrics": [
                {"name": "rule", "value": rule},
                {"name": "engine", "value": "rule"},
                {"name": "candidates_returned", "value": len(rows)},
            ],
        },
        "findings": [],
        "evidence": _build_evidence_from_legacy(rule, columns, rows),
        "artifacts": [],
        "diagnostics": {
            "warnings": [],
            "data_quality": {},
        },
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def detect_anomaly_action(
    con: duckdb.DuckDBPyConnection,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    *,
    rule: str,
    engine: str,
    limit: int,
    output_file: str | None = None,
) -> dict[str, Any]:
    """Execute anomaly detection and return both text and structured results."""
    and_clause = where_to_and_clause(where_clause)

    if engine == "rule":
        # Pure rule mode: execute legacy SQL directly. SQL errors propagate.
        legacy_and = where_to_and_clause(where_clause)
        columns, legacy_rows = _run_legacy_rule_detection(con, rule, legacy_and, limit=limit)
        ranked = legacy_rows[:limit]
        col_names = _rule_columns(rule) or columns
        text_output = f"Anomaly engine: {engine}\n\n{render_rows_section(f'Top {rule} anomalies', columns, _rows_to_tuples(columns, ranked))}"
        skill_result = build_legacy_rule_skill_result_parts(rule, columns, ranked, text_output)
        _export_columns = columns
    else:
        ranked = _run_anomaly_detection(con, mappings, and_clause, rule=rule, engine=engine, limit=limit)

        columns = _rule_columns(rule)
        text_output = f"Anomaly engine: {engine}\n\n{render_rows_section(f'Top {rule} anomalies', columns, _rows_to_tuples(columns, ranked))}"
        skill_result = build_skill_result_parts(rule, engine, ranked, text_output)
        _export_columns = columns

    # Unified output-file export for both rule and hybrid engines
    if output_file and ranked:
        import csv
        import os
        os.makedirs(os.path.dirname(output_file) if os.path.dirname(output_file) else ".", exist_ok=True)
        with open(output_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=_export_columns)
            writer.writeheader()
            for row in ranked:
                writer.writerow({col: row.get(col, "") for col in _export_columns})
        skill_result["artifacts"].append({
            "artifact_id": "a-anomaly-export",
            "type": "csv",
            "title": "Anomaly Detection Results",
            "uri": output_file,
        })

    return {"text": text_output, "skill_result": skill_result}
