"""Structured SkillResult adapter for signature-review action data."""

from __future__ import annotations

from typing import Any


SEVERITY_ORDER = {
    "critical": 4,
    "high": 3,
    "medium": 2,
    "low": 1,
    "info": 0,
}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _dict_rows_to_table(rows: list[dict[str, str]]):
    """Convert list of dict rows to (columns, rows) format."""
    if not rows:
        return [], []
    columns = list(rows[0].keys())
    table_rows = [[row.get(col, "") for col in columns] for row in rows]
    return columns, table_rows


def render_text(data: dict[str, Any], limit: int = 10) -> str:
    """Render structured signature review data as text for non-JSON output."""
    from utils.formatter import render_rows_section

    if "error" in data:
        return f"Signature review\n{data['error']}"

    sections = []
    sections.append(render_rows_section(
        "Signature review scope",
        ["candidate_fields", "scanned_rows", "hit_count"],
        [(", ".join(data.get("candidate_fields", [])), data.get("scanned_rows", 0), data.get("hit_count", 0))],
    ))

    indicator_rows = data.get("indicator_rows", [])
    value_rows = data.get("value_rows", [])
    hotspot_rows = data.get("hotspot_rows", [])

    if indicator_rows:
        sections.append(render_rows_section(
            "Signature indicator summary",
            ["rule_id", "severity", "category", "hits", "src_ips", "distinct_values", "description"],
            [
                (r.get("rule_id"), r.get("severity"), r.get("category"), r.get("hits"), r.get("src_ips"), r.get("distinct_values"), r.get("description"))
                for r in indicator_rows
            ],
        ))

    if value_rows:
        sections.append(render_rows_section(
            "Signature top matched values",
            ["rule_id", "field", "matched_value", "hits", "src_ips"],
            [(r.get("rule_id"), r.get("field"), r.get("matched_value"), r.get("hits"), r.get("src_ips")) for r in value_rows],
        ))

    if hotspot_rows:
        sections.append(render_rows_section(
            "Signature source hotspots (hybrid scoring)",
            ["src_ip", "signature_risk_score", "severity", "total_hits", "critical_hits", "high_hits", "unique_rules", "likely_reason"],
            [
                (r.get("src_ip"), r.get("signature_risk_score"), r.get("severity"), r.get("total_hits"), r.get("critical_hits"), r.get("high_hits"), r.get("unique_rules"), r.get("likely_reason"))
                for r in hotspot_rows
            ],
        ))

    if not indicator_rows and not hotspot_rows:
        sections.append("No built-in signature indicators matched the selected semantic fields.")

    return "\n\n".join(s for s in sections if s)


def build_skill_result_parts(data: dict[str, Any]) -> dict[str, Any]:
    """Build structured SkillResult for signature-review action from structured data."""
    if "error" in data:
        scanned_rows = data.get("scanned_rows", 0)
        hit_count = data.get("hit_count", 0)
        warnings = [{"code": "no_signature_capable_fields", "message": data["error"], "severity": "info"}]
        return {
            "summary": {
                "title": "Signature Review",
                "overview": data["error"],
                "severity": "info",
                "confidence": None,
                "key_metrics": [
                    {"name": "scanned_rows", "value": scanned_rows},
                    {"name": "hit_count", "value": hit_count},
                ],
            },
            "findings": [],
            "evidence": [],
            "artifacts": [],
            "diagnostics": {"warnings": warnings, "data_quality": {"scanned_rows": scanned_rows, "hit_count": hit_count}},
        }

    scanned_rows = data.get("scanned_rows", 0)
    hit_count = data.get("hit_count", 0)
    indicator_rows = data.get("indicator_rows", [])
    value_rows = data.get("value_rows", [])
    hotspot_rows = data.get("hotspot_rows", [])

    evidence: list[dict[str, Any]] = []
    findings: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    metrics: list[dict[str, Any]] = []

    metrics.append({"name": "scanned_rows", "value": scanned_rows})
    metrics.append({"name": "hit_count", "value": hit_count})
    metrics.append({"name": "candidate_fields", "value": len(data.get("candidate_fields", []))})

    evidence.append({
        "evidence_id": "e-signature-metrics",
        "type": "metric",
        "title": "Signature Review Metrics",
        "metrics": metrics,
    })

    if indicator_rows:
        columns, table_rows = _dict_rows_to_table(indicator_rows)
        evidence.append({
            "evidence_id": "e-signature-summary",
            "type": "table",
            "title": "Signature Indicator Summary",
            "columns": columns,
            "rows": table_rows,
        })

    if value_rows:
        columns, table_rows = _dict_rows_to_table(value_rows)
        evidence.append({
            "evidence_id": "e-signature-matched-values",
            "type": "table",
            "title": "Signature Top Matched Values",
            "columns": columns,
            "rows": table_rows,
        })

    if hotspot_rows:
        columns, table_rows = _dict_rows_to_table(hotspot_rows)
        evidence.append({
            "evidence_id": "e-signature-source-hotspots",
            "type": "table",
            "title": "Signature Source Hotspots",
            "columns": columns,
            "rows": table_rows,
        })

    for index, row in enumerate(hotspot_rows, start=1):
        score = _safe_float(row.get("signature_risk_score"))
        severity = str(row.get("severity") or "info").lower()
        if score >= 0.35:
            findings.append({
                "finding_id": f"f-signature-source-{index:03d}",
                "type": "signature_match",
                "severity": severity,
                "confidence": score,
                "title": f"Signature match: {row.get('src_ip')}",
                "description": row.get("likely_reason") or "Source host flagged by signature analysis.",
                "entities": [{"type": "src_ip", "value": row.get("src_ip")}],
                "evidence_refs": ["e-signature-source-hotspots", "e-signature-summary"],
            })

    for row in indicator_rows:
        sev = str(row.get("severity", "info")).lower()
        hits = _safe_int(row.get("hits"))
        if sev in ("critical", "high") and hits > 0:
            findings.append({
                "finding_id": f"f-signature-{sev}-{row.get('rule_id', 'UNKNOWN')}",
                "type": "signature_rule_match",
                "severity": sev,
                "confidence": 0.8 if sev == "critical" else 0.7,
                "title": f"{sev.capitalize()} signature rule: {row.get('rule_id')}",
                "description": (
                    f"{hits} hit(s) from {row.get('rule_id')} (category: {row.get('category')}). "
                    f"{row.get('description', '')}"
                ),
                "entities": [{"type": "rule_id", "value": row.get("rule_id")}],
                "evidence_refs": ["e-signature-summary"],
            })

    if hit_count == 0:
        warnings.append({
            "code": "no_signature_hits",
            "message": f"No signature indicators matched across {scanned_rows} scanned rows. The signature library may be incomplete or the dataset may not contain known malicious indicators.",
            "severity": "info",
        })

    raw_text = render_text(data)
    evidence.append({
        "evidence_id": "e-raw-report",
        "type": "text",
        "title": "Raw Signature Review Output",
        "content": raw_text,
    })

    existing_ids = {e["evidence_id"] for e in evidence}
    for finding in findings:
        finding["evidence_refs"] = [ref for ref in finding["evidence_refs"] if ref in existing_ids]

    highest_severity = "info"
    for f in findings:
        if SEVERITY_ORDER.get(f.get("severity", "info"), 0) > SEVERITY_ORDER.get(highest_severity, 0):
            highest_severity = f["severity"]

    overview_text = f"Signature review of {scanned_rows} rows; {hit_count} hits."
    if hotspot_rows:
        overview_text += f" {len(hotspot_rows)} source hotspot(s) identified."

    return {
        "summary": {
            "title": "Signature Review",
            "overview": overview_text,
            "severity": highest_severity,
            "confidence": _safe_float(hotspot_rows[0].get("signature_risk_score")) if hotspot_rows else None,
            "key_metrics": metrics,
        },
        "findings": findings,
        "evidence": evidence,
        "artifacts": [],
        "diagnostics": {
            "warnings": warnings,
            "data_quality": {
                "scanned_rows": scanned_rows,
                "hit_count": hit_count,
            },
        },
    }
