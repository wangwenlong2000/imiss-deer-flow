"""
Aggregate Action

Groups flows by one or more dimensions and computes aggregated metrics
(count, sum:bytes, sum:packets, avg:flow_duration, etc.).
"""

from __future__ import annotations

from typing import Any

from actions.advanced_action_common import fetch_rows, present_fields, scoped_where
from core.schema_mapping import metric_sql
from utils.sql import quote_identifier

_SUPPORTED_AGGREGATIONS = {"sum", "avg", "max", "min", "count_distinct"}


def execute_aggregate(
    con: Any,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    files: list[str],
    *,
    group_by: str,
    metrics: str | None = None,
    limit: int = 20,
    **kwargs,
) -> dict:
    """Execute aggregate query and return structured results.

    Raises:
        ValueError: If group_by is empty or metrics are invalid.
    """
    if not group_by:
        raise ValueError("--group-by is required for aggregate")

    groups = [item.strip() for item in group_by.split(",") if item.strip()]
    if not groups:
        raise ValueError("--group-by is required for aggregate")

    available = present_fields(mappings)
    invalid_groups = [group for group in groups if group not in available]
    if invalid_groups:
        raise ValueError(
            f"Group field(s) not available in the current dataset: {', '.join(invalid_groups)}. "
            f"Available fields: {', '.join(sorted(available))}."
        )

    default_metrics = mappings.get("default_metrics", ["count", "sum:bytes", "sum:packets", "avg:flow_duration"])
    metric_items = [item.strip() for item in (metrics or ",".join(default_metrics)).split(",") if item.strip()]

    for metric in metric_items:
        if metric == "count":
            continue
        aggregation, separator, field = metric.partition(":")
        if not separator or not field:
            raise ValueError(
                f"Invalid metric specification: {metric}. "
                "Use count or <aggregation>:<field>."
            )
        if aggregation not in _SUPPORTED_AGGREGATIONS:
            raise ValueError(
                f"Unsupported metric aggregation: {aggregation}. "
                f"Supported aggregations: {', '.join(sorted(_SUPPORTED_AGGREGATIONS))}."
            )
        if field not in available:
            raise ValueError(
                f"Metric field '{field}' is not available in the current dataset. "
                f"Available fields: {', '.join(sorted(available))}."
            )

    order_index = len(groups) + 1
    sql = f"""
        SELECT {', '.join(quote_identifier(group) for group in groups)},
               {', '.join(metric_sql(metric) for metric in metric_items)}
        FROM flows
        {scoped_where(where_clause, "1=1")}
        GROUP BY {', '.join(quote_identifier(group) for group in groups)}
        ORDER BY {order_index} DESC NULLS LAST, {', '.join(quote_identifier(group) for group in groups)}
        LIMIT {limit}
    """
    rows = fetch_rows(con, sql)

    return {
        "groups": groups,
        "metrics": metric_items,
        "rows": rows,
        "total_groups": len(rows),
    }


def format_results(results: dict) -> str:
    """Format aggregate results as text."""
    output = []
    groups = results.get("groups", [])
    metrics = results.get("metrics", [])

    output.append(f"# Aggregate by {', '.join(groups)}\n")
    output.append(f"**Metrics**: {', '.join(metrics)}")
    output.append(f"**Groups**: {results['total_groups']}\n")

    rows = results.get("rows", [])
    if rows:
        from actions.advanced_action_common import format_dict_rows
        output.append(format_dict_rows(rows))

    return "\n".join(output)


def build_skill_result_parts(
    results: dict,
    raw_output: str,
) -> dict[str, Any]:
    """Build structured SkillResult for aggregate action."""
    groups = results.get("groups", [])
    metrics = results.get("metrics", [])
    rows = results.get("rows", [])
    total_groups = results.get("total_groups", 0)

    findings: list[dict[str, Any]] = []
    evidence: list[dict[str, Any]] = []

    # Aggregate table evidence
    if rows:
        columns = list(rows[0].keys())
        evidence.append({
            "evidence_id": "e-aggregate-table",
            "type": "table",
            "title": f"Aggregate by {', '.join(groups)}",
            "columns": columns,
            "rows": [[r.get(col, "") for col in columns] for r in rows],
        })

    # Top group finding
    if rows and metrics:
        first_metric = metrics[0]
        metric_key = first_metric.replace(":", "_")
        # Find the row with highest value for the first metric
        top_row = max(rows, key=lambda r: float(r.get(metric_key, 0) or 0))
        top_value = top_row.get(metric_key, 0)
        group_values = ", ".join(f"{g}={top_row.get(g, '')}" for g in groups)

        findings.append({
            "finding_id": "f-aggregate-top-group",
            "type": "observation",
            "severity": "info",
            "confidence": 1.0,
            "title": f"Top group by {first_metric}: {group_values}",
            "description": f"The group ({group_values}) has the highest {first_metric} value ({top_value}). Total groups: {total_groups}.",
            "entities": [{"type": "group", "value": group_values}],
            "evidence_refs": ["e-aggregate-table"],
        })

    return {
        "summary": {
            "title": f"Aggregate by {', '.join(groups)}",
            "overview": f"Aggregated flows by {', '.join(groups)} into {total_groups} groups with metrics: {', '.join(metrics)}.",
            "severity": "info",
            "confidence": 1.0,
            "key_metrics": [
                {"name": "groups", "value": ", ".join(groups)},
                {"name": "metrics", "value": ", ".join(metrics)},
                {"name": "total_groups", "value": total_groups},
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
