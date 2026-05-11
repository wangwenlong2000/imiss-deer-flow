"""
Distribution Action

Shows the distribution of records and bytes across unique values of a dimension field.
"""

from __future__ import annotations

from typing import Any

from actions.advanced_action_common import fetch_rows, present_fields, scoped_where
from utils.sql import quote_identifier


def execute_distribution(
    con: Any,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    files: list[str],
    *,
    dimension: str,
    limit: int = 20,
    **kwargs,
) -> dict:
    """Execute distribution query and return structured results.

    Raises:
        ValueError: If the dimension is not available in the dataset.
    """
    available = present_fields(mappings)
    if dimension not in available:
        raise ValueError(
            f"Dimension '{dimension}' is not available in the current dataset. "
            f"Available fields: {', '.join(sorted(available))}."
        )

    total_sql = f"""
        SELECT COUNT(*) AS total_records, SUM(COALESCE(bytes, 0)) AS total_bytes
        FROM flows
        {scoped_where(where_clause, "1=1")}
    """
    total_rows = fetch_rows(con, total_sql)
    total_data = total_rows[0] if total_rows else {"total_records": 0, "total_bytes": 0}

    dist_sql = f"""
        SELECT COALESCE(CAST({quote_identifier(dimension)} AS VARCHAR), 'NULL') AS bucket,
               COUNT(*) AS records,
               SUM(COALESCE(bytes, 0)) AS total_bytes
        FROM flows
        {scoped_where(where_clause, "1=1")}
        GROUP BY 1
        ORDER BY records DESC, total_bytes DESC, bucket ASC
        LIMIT {limit}
    """
    distribution_rows = fetch_rows(con, dist_sql)

    return {
        "dimension": dimension,
        "total_records": total_data.get("total_records", 0),
        "total_bytes": total_data.get("total_bytes", 0),
        "distribution_rows": distribution_rows,
    }


def format_results(results: dict) -> str:
    """Format distribution results as text."""
    output = []
    dimension = results.get("dimension", "unknown")

    output.append(f"# Distribution of {dimension}\n")
    output.append(f"**Total Records**: {results['total_records']:,}")
    output.append(f"**Total Bytes**: {results['total_bytes']:,.0f}\n")

    distribution_rows = results.get("distribution_rows", [])
    if distribution_rows:
        from actions.advanced_action_common import format_dict_rows
        output.append(format_dict_rows(distribution_rows))

        # Coverage percentage
        total = results.get("total_records", 0)
        if total and total > 0:
            top_sum = sum(r.get("records", 0) for r in distribution_rows)
            coverage = top_sum / total * 100
            output.append(f"\n**Coverage**: Top {len(distribution_rows)} buckets account for {coverage:.1f}% of records")

    return "\n".join(output)


def build_skill_result_parts(
    results: dict,
    raw_output: str,
) -> dict[str, Any]:
    """Build structured SkillResult for distribution action."""
    dimension = results.get("dimension", "unknown")
    total_records = results.get("total_records", 0)
    total_bytes = results.get("total_bytes", 0)
    distribution_rows = results.get("distribution_rows", [])

    findings: list[dict[str, Any]] = []
    evidence: list[dict[str, Any]] = []

    # Distribution table evidence
    if distribution_rows:
        evidence.append({
            "evidence_id": "e-distribution-table",
            "type": "table",
            "title": f"Distribution of {dimension}",
            "columns": ["bucket", "records", "total_bytes"],
            "rows": [[r.get("bucket", ""), r.get("records", 0), r.get("total_bytes", 0)] for r in distribution_rows],
        })

    # Findings
    if distribution_rows and total_records and total_records > 0:
        # Concentration finding
        top_sum = sum(r.get("records", 0) for r in distribution_rows)
        coverage = top_sum / total_records * 100
        if coverage > 90 and len(distribution_rows) <= 5:
            findings.append({
                "finding_id": "f-dist-high-concentration",
                "type": "observation",
                "severity": "info",
                "confidence": 1.0,
                "title": f"High concentration: top {len(distribution_rows)} {dimension} values account for {coverage:.1f}% of records",
                "description": f"The distribution of {dimension} is heavily concentrated. Top {len(distribution_rows)} buckets account for {coverage:.1f}% of all {total_records:,} records.",
                "entities": [{"type": "dimension", "value": dimension}],
                "evidence_refs": ["e-distribution-table"],
            })

        # Dominant bucket finding — show precise percentage (e.g., 99.9% not 100%)
        top_bucket = distribution_rows[0]
        top_records = top_bucket.get("records", 0)
        dominance = top_records / max(total_records, 1) * 100
        if dominance > 50:
            pct_str = f"{dominance:.1f}" if dominance < 99.95 else f"{dominance:.1f}"
            findings.append({
                "finding_id": "f-dist-dominant-bucket",
                "type": "observation",
                "severity": "info",
                "confidence": 1.0,
                "title": f"Dominant bucket: '{top_bucket['bucket']}' accounts for {pct_str}% of records",
                "description": f"A single {dimension} value ('{top_bucket['bucket']}') accounts for {pct_str}% of all {total_records:,} records ({top_records:,} / {total_records:,}).",
                "entities": [{"type": "bucket", "value": str(top_bucket["bucket"])}],
                "evidence_refs": ["e-distribution-table"],
            })

    return {
        "summary": {
            "title": f"Distribution: {dimension}",
            "overview": f"Distribution of {dimension}: {len(distribution_rows)} buckets, {total_records:,} total records, {total_bytes:,.0f} total bytes.",
            "severity": "info",
            "confidence": 1.0,
            "key_metrics": [
                {"name": "dimension", "value": dimension},
                {"name": "total_records", "value": int(total_records)},
                {"name": "total_bytes", "value": int(total_bytes)},
                {"name": "bucket_count", "value": len(distribution_rows)},
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
