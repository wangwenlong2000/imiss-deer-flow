"""
TopN Action

Identifies the top N dimension values by a metric (bytes, packets, flows, destinations, ports).
"""

from __future__ import annotations

from typing import Any

from actions.advanced_action_common import fetch_rows, present_fields, scoped_where
from utils.sql import quote_identifier

# Metrics where top-N coverage/dominance findings make sense (additive)
_ADDITIVE_METRICS = {"bytes", "packets", "flows", "records"}

# Metrics where individual buckets contribute to a total (even if not strictly additive)
_SUPPORTED_METRICS = _ADDITIVE_METRICS | {"destinations", "ports"}


def execute_topn(
    con: Any,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    files: list[str],
    *,
    dimension: str,
    metric: str = "bytes",
    limit: int = 10,
    **kwargs,
) -> dict:
    """Execute top-N query and return structured results.

    Raises:
        ValueError: If the metric is unsupported or the dimension is not available.
    """
    if metric not in _SUPPORTED_METRICS:
        raise ValueError(
            f"Unsupported metric: '{metric}'. "
            f"Allowed metrics are: {', '.join(sorted(_SUPPORTED_METRICS))}."
        )

    available = present_fields(mappings)
    if dimension not in available:
        raise ValueError(
            f"Dimension '{dimension}' is not available in the current dataset. "
            f"Available fields: {', '.join(sorted(available))}."
        )

    metric_expr = "SUM(COALESCE(bytes, 0))"
    if metric == "packets":
        metric_expr = "SUM(COALESCE(packets, 0))"
    elif metric in {"flows", "records"}:
        metric_expr = "COUNT(*)"
    elif metric == "destinations":
        metric_expr = "COUNT(DISTINCT dst_ip)"
    elif metric == "ports":
        metric_expr = "COUNT(DISTINCT dst_port)"

    total_sql = f"""
        SELECT {metric_expr} AS total_metric
        FROM flows
        {scoped_where(where_clause, "1=1")}
    """
    total_rows = fetch_rows(con, total_sql)
    total_metric = total_rows[0]["total_metric"] if total_rows else 0

    topn_sql = f"""
        SELECT {quote_identifier(dimension)} AS dimension_value, {metric_expr} AS metric_value
        FROM flows
        {scoped_where(where_clause, "1=1")}
        GROUP BY 1
        ORDER BY metric_value DESC NULLS LAST, CAST(dimension_value AS VARCHAR) ASC
        LIMIT {limit}
    """
    topn_rows = fetch_rows(con, topn_sql)

    return {
        "dimension": dimension,
        "metric": metric,
        "total_metric": total_metric,
        "topn_rows": topn_rows,
        "is_additive_metric": metric in _ADDITIVE_METRICS,
    }


def format_results(results: dict) -> str:
    """Format topn results as text."""
    output = []
    dimension = results.get("dimension", "unknown")
    metric = results.get("metric", "bytes")
    total = results.get("total_metric", 0)

    output.append(f"# Top {len(results['topn_rows'])} {dimension} by {metric}\n")
    output.append(f"**Total {metric}**: {total:,.0f}\n")

    topn_rows = results.get("topn_rows", [])
    if topn_rows:
        from actions.advanced_action_common import format_dict_rows
        output.append(format_dict_rows(topn_rows))

        # Show coverage percentage (only for additive metrics)
        if results.get("is_additive_metric") and total and total > 0:
            top_sum = sum(r.get("metric_value", 0) for r in topn_rows)
            coverage = top_sum / total * 100
            output.append(f"\n**Coverage**: Top {len(topn_rows)} account for {coverage:.1f}% of total {metric}")

    return "\n".join(output)


def build_skill_result_parts(
    results: dict,
    raw_output: str,
) -> dict[str, Any]:
    """Build structured SkillResult for topn action."""
    dimension = results.get("dimension", "unknown")
    metric = results.get("metric", "bytes")
    total = results.get("total_metric", 0)
    topn_rows = results.get("topn_rows", [])
    is_additive = results.get("is_additive_metric", False)

    findings: list[dict[str, Any]] = []
    evidence: list[dict[str, Any]] = []

    # Top-N table evidence
    if topn_rows:
        evidence.append({
            "evidence_id": "e-topn-table",
            "type": "table",
            "title": f"Top {len(topn_rows)} {dimension} by {metric}",
            "columns": ["dimension_value", "metric_value"],
            "rows": [[r.get("dimension_value", ""), r.get("metric_value", 0)] for r in topn_rows],
        })

    # Coverage and dominance findings (only for additive metrics)
    if is_additive and total and total > 0:
        top_sum = sum(r.get("metric_value", 0) for r in topn_rows)
        coverage = top_sum / total * 100
        if coverage < 50 and len(topn_rows) >= 5:
            findings.append({
                "finding_id": "f-topn-low-coverage",
                "type": "observation",
                "severity": "info",
                "confidence": 1.0,
                "title": f"Low coverage: top {len(topn_rows)} account for {coverage:.1f}% of {metric}",
                "description": f"Top {len(topn_rows)} {dimension} values by {metric} only account for {coverage:.1f}% of the total ({total:,.0f}). Consider increasing --limit or filtering for focused analysis.",
                "entities": [{"type": "dimension", "value": dimension}],
                "evidence_refs": ["e-topn-table"],
            })

        # Dominant value finding
        if topn_rows:
            top_val = topn_rows[0].get("metric_value", 0)
            dominance = top_val / total * 100
            if dominance > 50:
                findings.append({
                    "finding_id": "f-topn-dominant",
                    "type": "observation",
                    "severity": "info",
                    "confidence": 1.0,
                    "title": f"Dominant {dimension}: {topn_rows[0]['dimension_value']} accounts for {dominance:.0f}%",
                    "description": f"A single {dimension} value ({topn_rows[0]['dimension_value']}) accounts for {dominance:.0f}% of total {metric} ({total:,.0f}).",
                    "entities": [{"type": "dimension_value", "value": str(topn_rows[0]["dimension_value"])}],
                    "evidence_refs": ["e-topn-table"],
                })

    return {
        "summary": {
            "title": f"TopN: {dimension} by {metric}",
            "overview": f"Top {len(topn_rows)} {dimension} values by {metric}. Total {metric}: {total:,.0f}.",
            "severity": "info",
            "confidence": 1.0,
            "key_metrics": [
                {"name": "dimension", "value": dimension},
                {"name": "metric", "value": metric},
                {"name": "total_metric", "value": int(total) if isinstance(total, (int, float)) else total},
                {"name": "top_n_count", "value": len(topn_rows)},
            ],
        },
        "findings": findings,
        "evidence": evidence,
        "artifacts": [],
        "diagnostics": {
            "warnings": [],
            "data_quality": {},
        },
    }
