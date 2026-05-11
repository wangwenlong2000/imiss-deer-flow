"""Structured SkillResult adapter for risk-fusion-review action data."""

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


def _dict_rows_to_table(rows: list[dict]):
    """Convert list of dict rows to (columns, rows) format."""
    if not rows:
        return [], []
    columns = list(rows[0].keys())
    table_rows = [[row.get(col, "") for col in columns] for row in rows]
    return columns, table_rows


def render_text(data: dict[str, Any], limit: int = 10) -> str:
    """Render structured risk fusion review data as text for non-JSON output."""
    from utils.formatter import render_rows_section

    sections = []
    sections.append(render_rows_section(
        "Risk fusion coverage",
        ["flow_sources", "zeek_sources", "signature_sources", "signature_hits", "zeek_artifact_sets"],
        [(
            data.get("flow_sources", 0),
            data.get("zeek_sources", 0),
            data.get("signature_sources", 0),
            data.get("signature_hits", 0),
            data.get("zeek_artifact_sets", 0),
        )],
    ))

    risk_rows = data.get("risk_rows", [])
    evidence_mix_rows = data.get("evidence_mix_rows", [])

    if risk_rows:
        sections.append(render_rows_section(
            "Final fused risk view (flow + zeek + signature)",
            [
                "src_ip", "final_risk_score", "severity", "flow_count",
                "unique_dst_ip", "short_like_ratio", "avg_byte_asymmetry",
                "avg_dns_query_entropy", "zeek_risk_score",
                "signature_risk_score", "signature_critical_hits", "likely_reason",
            ],
            [
                (
                    r.get("src_ip"), r.get("final_risk_score"), r.get("severity"),
                    r.get("flow_count"), r.get("unique_dst_ip"),
                    r.get("short_like_ratio"), r.get("avg_byte_asymmetry"),
                    r.get("avg_dns_query_entropy"), r.get("zeek_risk_score"),
                    r.get("signature_risk_score"), r.get("signature_critical_hits"),
                    r.get("likely_reason"),
                )
                for r in risk_rows
            ],
        ))

    if evidence_mix_rows:
        sections.append(render_rows_section(
            "Fused evidence mix",
            ["src_ip", "flow_failure_pattern", "zeek_event_count", "zeek_weird_events", "signature_total_hits", "signature_category"],
            [
                (
                    r.get("src_ip"), r.get("flow_failure_pattern"),
                    r.get("zeek_event_count"), r.get("zeek_weird_events"),
                    r.get("signature_total_hits"), r.get("signature_category"),
                )
                for r in evidence_mix_rows
            ],
        ))

    for note in data.get("notes", []):
        sections.append(note)

    return "\n\n".join(s for s in sections if s)


def build_skill_result_parts(data: dict[str, Any]) -> dict[str, Any]:
    """Build structured SkillResult for risk-fusion-review action from structured data."""
    risk_rows = data.get("risk_rows", [])
    evidence_mix_rows = data.get("evidence_mix_rows", [])
    notes = data.get("notes", [])

    top_score = max((_safe_float(r.get("final_risk_score")) for r in risk_rows), default=0.0)
    highest_severity = "info"
    for r in risk_rows:
        sev = str(r.get("severity") or "info").lower()
        if SEVERITY_ORDER.get(sev, 0) > SEVERITY_ORDER.get(highest_severity, 0):
            highest_severity = sev

    overview = (
        f"Ranked {len(risk_rows)} source hosts with fused flow, Zeek, and signature evidence; "
        f"top risk score is {top_score:.4f}."
    )

    key_metrics = [
        {"name": "ranked_sources", "value": len(risk_rows)},
        {"name": "flow_sources", "value": data.get("flow_sources", 0)},
        {"name": "zeek_sources", "value": data.get("zeek_sources", 0)},
        {"name": "signature_sources", "value": data.get("signature_sources", 0)},
        {"name": "signature_hits", "value": data.get("signature_hits", 0)},
        {"name": "zeek_artifact_sets", "value": data.get("zeek_artifact_sets", 0)},
    ]

    evidence: list[dict[str, Any]] = [
        {
            "evidence_id": "e-risk-fusion-coverage",
            "type": "metric",
            "title": "Risk Fusion Coverage",
            "metrics": key_metrics[1:],
        }
    ]

    if risk_rows:
        columns, table_rows = _dict_rows_to_table(risk_rows)
        evidence.append({
            "evidence_id": "e-final-fused-risk",
            "type": "table",
            "title": "Final Fused Risk View",
            "columns": columns,
            "rows": table_rows,
        })

    if evidence_mix_rows:
        columns, table_rows = _dict_rows_to_table(evidence_mix_rows)
        evidence.append({
            "evidence_id": "e-fused-evidence-mix",
            "type": "table",
            "title": "Fused Evidence Mix",
            "columns": columns,
            "rows": table_rows,
        })

    findings: list[dict[str, Any]] = []
    for index, row in enumerate(risk_rows, start=1):
        score = _safe_float(row.get("final_risk_score"))
        findings.append({
            "finding_id": f"f-risk-fusion-{index:03d}",
            "type": "fused_source_risk",
            "severity": str(row.get("severity") or "info").lower(),
            "confidence": score,
            "title": f"Fused risk source: {row.get('src_ip')}",
            "description": row.get("likely_reason") or "Source host ranked by fused flow, Zeek, and signature evidence.",
            "entities": {
                "src_ip": row.get("src_ip"),
                "final_risk_score": score,
                "flow_count": _safe_int(row.get("flow_count")),
                "unique_dst_ip": _safe_int(row.get("unique_dst_ip")),
                "zeek_risk_score": _safe_float(row.get("zeek_risk_score")),
                "signature_risk_score": _safe_float(row.get("signature_risk_score")),
                "signature_critical_hits": _safe_int(row.get("signature_critical_hits")),
                "likely_reason": row.get("likely_reason"),
            },
            "evidence_refs": ["e-final-fused-risk", "e-fused-evidence-mix"],
        })

    warnings = [
        {"code": "RISK_FUSION_NOTE", "message": note, "severity": "info"}
        for note in notes
    ]

    raw_text = render_text(data)
    evidence.append({
        "evidence_id": "e-raw-report",
        "type": "text",
        "title": "Raw Action Output",
        "content": raw_text,
    })

    return {
        "summary": {
            "title": "Risk Fusion Review",
            "overview": overview,
            "severity": highest_severity,
            "confidence": round(top_score, 4) if risk_rows else None,
            "key_metrics": key_metrics,
        },
        "findings": findings,
        "evidence": evidence,
        "artifacts": [],
        "diagnostics": {
            "warnings": warnings,
            "data_quality": {
                "ranked_sources": len(risk_rows),
                "flow_sources": data.get("flow_sources", 0),
                "zeek_sources": data.get("zeek_sources", 0),
                "signature_sources": data.get("signature_sources", 0),
                "signature_hits": data.get("signature_hits", 0),
                "zeek_artifact_sets": data.get("zeek_artifact_sets", 0),
            },
        },
    }
