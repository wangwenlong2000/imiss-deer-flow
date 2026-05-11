from __future__ import annotations

import json
from typing import Any

import duckdb  # type: ignore

from utils.formatter import format_rows
from utils.sql import quote_identifier


def inspect_action(con: duckdb.DuckDBPyConnection, table_info: dict[str, dict[str, Any]], mappings: dict[str, dict[str, str]]) -> str:
    parts: list[str] = []
    for table_name, meta in table_info.items():
        columns = con.execute(f"DESCRIBE {quote_identifier(table_name)}").fetchall()
        row_count = con.execute(f"SELECT COUNT(*) FROM {quote_identifier(table_name)}").fetchone()[0]
        parts.append(f"\n{'=' * 72}")
        parts.append(f"Table: {table_name}")
        parts.append(f"Source file: {meta['file']}")
        parts.append(f"Rows: {row_count}")
        if meta.get("ingestion_mode"):
            parts.append(f"Ingestion mode: {meta['ingestion_mode']}")
        if meta.get("ingestion_warning"):
            parts.append(f"Ingestion warning: {meta['ingestion_warning']}")
        elif str(meta.get("file", "")).lower().endswith(".csv"):
            parts.append("Ingestion mode: lenient (legacy cache)")
            parts.append("Ingestion warning: CSV cache may have been loaded with ignore_errors=true and null_padding=true; rebuild the cache for exact ingestion metadata.")
        if meta.get("mapping_profile"):
            parts.append(f"Detected mapping profile: {meta['mapping_profile']}")
        parts.append(f"Detected canonical fields: {json.dumps(mappings.get(table_name, {}), ensure_ascii=False)}")
        parts.append(f"{'-' * 72}")
        parts.append(f"{'Name':<28} {'Type':<18} {'Nullable'}")
        for col_name, col_type, nullable, *_ in columns:
            parts.append(f"{col_name:<28} {col_type:<18} {nullable}")
        sample = con.execute(f"SELECT * FROM {quote_identifier(table_name)} LIMIT 5").fetchall()
        if sample:
            parts.append("\nSample rows:")
            parts.append(format_rows([row[0] for row in columns], sample))
    summary = con.execute(
        "SELECT COUNT(*) AS records, "
        "MIN(analysis_time_ts) AS min_time, MAX(analysis_time_ts) AS max_time, "
        "MIN(analysis_time_relative_s) FILTER (WHERE analysis_time_kind = 'relative') AS min_relative_time_s, "
        "MAX(analysis_time_relative_s) FILTER (WHERE analysis_time_kind = 'relative') AS max_relative_time_s, "
        "COUNT(DISTINCT src_ip) AS unique_src_ip, COUNT(DISTINCT dst_ip) AS unique_dst_ip FROM flows"
    ).fetchone()
    parts.append(f"\n{'=' * 72}")
    parts.append("Unified flows view")
    parts.append(f"Records: {summary[0]}")
    if summary[1] is not None or summary[2] is not None:
        parts.append(f"Absolute time range: {summary[1]} -> {summary[2]}")
    if summary[3] is not None or summary[4] is not None:
        parts.append(f"Relative time range (s): {summary[3]} -> {summary[4]}")
    parts.append(f"Unique src_ip: {summary[5]}")
    parts.append(f"Unique dst_ip: {summary[6]}")
    return "\n".join(parts)


def build_skill_result_parts(
    con: Any,
    table_info: dict[str, dict[str, Any]],
    mappings: dict[str, dict[str, str]],
    raw_output: str,
) -> dict[str, Any]:
    """Build structured SkillResult for inspect action.

    Provides:
    - summary: table overview, row counts, ingestion mode
    - findings: data-quality observations AND notable schema features
    - evidence: column schemas (columns+rows format), flow metrics
    - diagnostics: ingestion metadata, mapping profile, structured warnings
    """
    findings: list[dict[str, Any]] = []
    evidence: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    for table_name, meta in table_info.items():
        columns = con.execute(f"DESCRIBE {quote_identifier(table_name)}").fetchall()
        row_count = con.execute(f"SELECT COUNT(*) FROM {quote_identifier(table_name)}").fetchone()[0]

        # Column schema evidence — use columns + rows (2D arrays) format
        schema_rows = [[c[0], c[1], c[2]] for c in columns]
        evidence.append({
            "evidence_id": f"e-{table_name}-schema",
            "type": "table",
            "title": f"Schema: {table_name}",
            "columns": ["name", "type", "nullable"],
            "rows": schema_rows,
        })

        # Notable schema observations as findings
        column_names = {c[0].lower() for c in columns}
        notable_fields = []

        # Time-related fields
        time_fields = [f for f in column_names if "time" in f or "timestamp" in f]
        if time_fields:
            notable_fields.append(f"time fields: {', '.join(sorted(time_fields))}")

        # IP fields
        ip_fields = [f for f in column_names if "ip" in f or "src" in f or "dst" in f or "src_" in f or "dst_" in f]
        if ip_fields:
            notable_fields.append(f"IP/network fields: {len(ip_fields)} fields detected")

        # Security-relevant fields
        security_fields = [f for f in column_names if any(k in f for k in ("threat", "malware", "c2", "dga", "botnet", "phish"))]
        if security_fields:
            notable_fields.append(f"security fields: {', '.join(sorted(security_fields))}")

        # Fingerprint fields
        fp_fields = [f for f in column_names if any(k in f for k in ("ja3", "hassh", "fingerprint", "mac_", "user_agent", "dhcp"))]
        if fp_fields:
            notable_fields.append(f"fingerprint fields: {len(fp_fields)} fields detected")

        if notable_fields:
            findings.append({
                "finding_id": f"f-{table_name}-schema-features",
                "type": "schema_observation",
                "severity": "info",
                "confidence": 1.0,
                "title": f"Notable schema features in {table_name}",
                "description": " | ".join(notable_fields),
                "entities": [{"type": "table", "value": table_name}],
                "evidence_refs": [f"e-{table_name}-schema"],
            })

        # Ingestion quality findings
        ingestion_mode = meta.get("ingestion_mode", "unknown")
        physical_rows = meta.get("physical_data_rows")
        dropped = meta.get("approx_dropped_rows")
        null_key = meta.get("approx_null_key_rows")

        if physical_rows is not None and dropped is not None and dropped > 0:
            findings.append({
                "finding_id": f"f-{table_name}-row-quality",
                "type": "data_quality",
                "severity": "medium" if dropped / max(physical_rows, 1) > 0.05 else "low",
                "confidence": 0.9,
                "title": f"Possible row loss in {table_name}: ~{dropped} rows dropped during ingestion",
                "description": (
                    f"Loaded {row_count} rows but file has {physical_rows} physical lines. "
                    f"~{dropped} rows may have been dropped during ingestion (lenient mode). "
                    f"Mode: {ingestion_mode}."
                ),
                "entities": [{"type": "table", "value": table_name}],
                "evidence_refs": [f"e-{table_name}-schema"],
            })

        if null_key is not None and null_key > 0:
            null_key_pct = round(null_key / max(row_count, 1) * 100, 1)
            findings.append({
                "finding_id": f"f-{table_name}-null-key-rows",
                "type": "data_quality",
                "severity": "medium" if null_key / max(row_count, 1) > 0.05 else "low",
                "confidence": 0.7,
                "title": f"{null_key} rows with null src_ip/dst_ip in {table_name} ({null_key_pct}%)",
                "description": (
                    f"{null_key} out of {row_count} rows have NULL src_ip or dst_ip. "
                    f"This may indicate null-padded CSV rows from lenient ingestion, "
                    f"but can also include legitimate captures lacking IP fields (e.g., ARP, DHCP)."
                ),
                "entities": [{"type": "table", "value": table_name}],
                "evidence_refs": [f"e-{table_name}-schema"],
            })

        if ingestion_mode == "lenient" and (null_key is None or null_key == 0) and (dropped is None or dropped == 0):
            findings.append({
                "finding_id": f"f-{table_name}-lenient-mode",
                "type": "data_quality",
                "severity": "low",
                "confidence": 0.9,
                "title": f"Table {table_name} ingested in lenient mode",
                "description": (
                    f"No dropped or null-key rows detected. "
                    f"rows_loaded={row_count}, physical_data_rows={physical_rows}."
                ),
                "entities": [{"type": "table", "value": table_name}],
                "evidence_refs": [f"e-{table_name}-schema"],
            })

        # Structured warning for mapping profile only; lenient ingestion is
        # already surfaced by the CLI ingestion pipeline.
        profile = meta.get("mapping_profile")
        if profile:
            warnings.append({
                "code": "mapping_profile_detected",
                "message": f"Table {table_name} matched mapping profile: {profile}",
                "severity": "info",
            })

    # Flow-level summary evidence — use metrics format
    flow_summary = con.execute(
        "SELECT COUNT(*) AS records, "
        "COUNT(DISTINCT src_ip) AS unique_src_ip, "
        "COUNT(DISTINCT dst_ip) AS unique_dst_ip, "
        "COUNT(DISTINCT protocol) AS unique_protocols "
        "FROM flows"
    ).fetchone()
    if flow_summary:
        evidence.append({
            "evidence_id": "e-flow-overview",
            "type": "metric",
            "title": "Flows View Overview",
            "metrics": [
                {"name": "total_records", "value": flow_summary[0]},
                {"name": "unique_src_ips", "value": flow_summary[1]},
                {"name": "unique_dst_ips", "value": flow_summary[2]},
                {"name": "unique_protocols", "value": flow_summary[3]},
            ],
        })

    return {
        "summary": {
            "title": "Schema Inspection",
            "overview": f"Inspected {len(table_info)} table(s). {flow_summary[0] if flow_summary else 0} records in unified flows view.",
            "severity": "medium" if any(f.get("severity") == "medium" for f in findings) else "info",
            "confidence": 0.95,
            "key_metrics": [
                {"name": "tables_inspected", "value": len(table_info)},
                {"name": "total_flow_records", "value": flow_summary[0] if flow_summary else 0},
                {"name": "unique_src_ips", "value": flow_summary[1] if flow_summary else 0},
            ],
        },
        "findings": findings,
        "evidence": evidence,
        "artifacts": [],
        "diagnostics": {
            "warnings": warnings,
            "data_quality": {
                "input_files": len(table_info),
                "tables": [
                    {
                        "table": tn,
                        "file": m.get("file", ""),
                        "ingestion_mode": m.get("ingestion_mode", "unknown"),
                        "rows_loaded": m.get("rows_loaded"),
                        "physical_data_rows": m.get("physical_data_rows"),
                        "approx_dropped_rows": m.get("approx_dropped_rows"),
                        "approx_null_key_rows": m.get("approx_null_key_rows"),
                        "mapping_profile": m.get("mapping_profile"),
                    }
                    for tn, m in table_info.items()
                ],
            },
        },
    }
