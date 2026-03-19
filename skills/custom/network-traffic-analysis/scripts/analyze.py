#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import logging
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Any

from file_resolution import get_default_search_roots, normalize_name, resolve_reference

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

try:
    import duckdb
except ImportError:
    os.system(f"{sys.executable} -m pip install duckdb openpyxl pyyaml -q")
    import duckdb

try:
    import yaml
except ImportError:
    os.system(f"{sys.executable} -m pip install pyyaml -q")
    import yaml

try:
    import pytz  # noqa: F401
except ImportError:
    os.system(f"{sys.executable} -m pip install pytz -q")

CANONICAL_COLUMNS = [
    "timestamp",
    "src_ip",
    "dst_ip",
    "src_port",
    "dst_port",
    "protocol",
    "app_protocol",
    "service",
    "bytes",
    "packets",
    "flow_duration",
    "duration_ms",
    "session_state",
    "rule_name",
    "tcp_flags",
    "dns_query",
    "tls_sni",
    "http_host",
    "direction",
    "action",
    "asset_id",
    "user_id",
    "device_id",
    "sensor_id",
    "dataset_label",
    "traffic_family",
]
NUMERIC_COLUMNS = {"src_port", "dst_port", "bytes", "packets", "flow_duration", "duration_ms"}
CACHE_DIR = Path(tempfile.gettempdir()) / ".network-traffic-analysis-cache"
SUPPORTED_PATTERNS = ("*.csv", "*.parquet", "*.json", "*.jsonl", "*.xlsx", "*.xls")


def quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sanitize_table_name(name: str) -> str:
    name = re.sub(r"[^\w]", "_", name)
    if name and name[0].isdigit():
        name = f"t_{name}"
    if name.lower() == "flows":
        name = "flows_source"
    return name


def ensure_cache_dir() -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def load_mapping(path: str | None) -> dict[str, Any]:
    if path is None:
        repo_root = Path(__file__).resolve().parents[4]
        path = str(repo_root / "datasets" / "network-traffic" / "schema" / "field_mapping.yaml")
    mapping_path = Path(path)
    if not mapping_path.exists():
        raise FileNotFoundError(f"Field mapping file not found: {mapping_path}")
    with open(mapping_path, encoding="utf-8") as f:
        payload = yaml.safe_load(f) or {}
    payload.setdefault("canonical_fields", {})
    payload.setdefault("default_metrics", ["count", "sum:bytes", "sum:packets", "avg:flow_duration"])
    return payload


def discover_files(values: list[str]) -> list[str]:
    files: list[str] = []
    for value in values:
        path = Path(value)
        if path.is_dir():
            for pattern in SUPPORTED_PATTERNS:
                files.extend(str(p) for p in sorted(path.rglob(pattern)))
        elif path.exists():
            files.append(str(path))
        else:
            files.extend(resolve_file_reference(value))
    deduped: list[str] = []
    seen: set[str] = set()
    for item in files:
        norm = str(Path(item))
        if norm not in seen:
            deduped.append(norm)
            seen.add(norm)
    return deduped


def resolve_file_reference(reference: str) -> list[str]:
    result = resolve_reference(reference)
    if result.status == "resolved":
        return result.matches
    if result.status == "ambiguous":
        sample = "\n".join(f"  - {path}" for path in result.matches[:10])
        raise ValueError(
            f"File reference '{reference}' matched multiple datasets. "
            f"Use a more specific path.\nCandidates:\n{sample}"
        )
    raise ValueError(result.message)


def compute_cache_key(files: list[str], mapping: dict[str, Any]) -> str:
    hasher = hashlib.sha256()
    for file_path in sorted(files):
        hasher.update(file_path.encode("utf-8"))
        try:
            with open(file_path, "rb") as f:
                while chunk := f.read(8192):
                    hasher.update(chunk)
        except OSError:
            pass
    hasher.update(json.dumps(mapping, sort_keys=True).encode("utf-8"))
    return hasher.hexdigest()


def save_json(path: Path, payload: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def load_json(path: Path) -> Any | None:
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_sources(con: duckdb.DuckDBPyConnection, files: list[str]) -> dict[str, dict[str, str]]:
    table_info: dict[str, dict[str, str]] = {}
    for file_path in files:
        path = Path(file_path)
        if not path.exists():
            logger.warning(f"File not found: {file_path}")
            continue
        table_name = sanitize_table_name(path.stem)
        base_name = table_name
        counter = 1
        while table_name in table_info:
            table_name = f"{base_name}_{counter}"
            counter += 1

        ext = path.suffix.lower()
        if ext == ".csv":
            sql = (
                f"CREATE TABLE {quote_identifier(table_name)} AS "
                f"SELECT * FROM read_csv_auto("
                f"{quote_literal(str(path))}, "
                f"delim=',', header=true, SAMPLE_SIZE=-1, ignore_errors=true, null_padding=true)"
            )
        elif ext == ".parquet":
            sql = f"CREATE TABLE {quote_identifier(table_name)} AS SELECT * FROM read_parquet({quote_literal(str(path))})"
        elif ext in {".json", ".jsonl"}:
            sql = f"CREATE TABLE {quote_identifier(table_name)} AS SELECT * FROM read_json_auto({quote_literal(str(path))}, format='auto')"
        elif ext in {".xlsx", ".xls"}:
            con.execute("INSTALL spatial; LOAD spatial;")
            sql = (
                f"CREATE TABLE {quote_identifier(table_name)} AS "
                f"SELECT * FROM st_read({quote_literal(str(path))}, open_options = ['HEADERS=FORCE', 'FIELD_TYPES=AUTO'])"
            )
        else:
            logger.warning(f"Unsupported file format: {ext} ({file_path})")
            continue

        try:
            con.execute(sql)
            table_info[table_name] = {"file": str(path)}
        except Exception as exc:
            logger.warning(f"Failed to load {file_path}: {exc}")
    return table_info


def get_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> list[str]:
    return [row[0] for row in con.execute(f"DESCRIBE {quote_identifier(table_name)}").fetchall()]


def detect_mapping(columns: list[str], mapping: dict[str, Any]) -> dict[str, str]:
    aliases = mapping.get("canonical_fields", {})
    normalized = {normalize_name(col): col for col in columns}
    resolved: dict[str, str] = {}
    for canonical in CANONICAL_COLUMNS:
        for alias in aliases.get(canonical, []):
            hit = normalized.get(normalize_name(alias))
            if hit:
                resolved[canonical] = hit
                break
    return resolved


def timestamp_expr(column_sql: str) -> str:
    return (
        f"COALESCE(try_cast({column_sql} AS TIMESTAMP), "
        f"to_timestamp(try_cast({column_sql} AS DOUBLE)), "
        f"try_strptime(CAST({column_sql} AS VARCHAR), '%Y-%m-%d %H:%M:%S'), "
        f"try_strptime(CAST({column_sql} AS VARCHAR), '%Y-%m-%dT%H:%M:%S'), "
        f"try_strptime(CAST({column_sql} AS VARCHAR), '%Y-%m-%dT%H:%M:%S.%f'))"
    )


def build_flows_view(
    con: duckdb.DuckDBPyConnection,
    table_info: dict[str, dict[str, str]],
    mapping: dict[str, Any],
) -> dict[str, dict[str, str]]:
    resolved_all: dict[str, dict[str, str]] = {}
    union_selects: list[str] = []
    for table_name, meta in table_info.items():
        resolved = detect_mapping(get_columns(con, table_name), mapping)
        resolved_all[table_name] = resolved
        fields: list[str] = []
        for canonical in CANONICAL_COLUMNS:
            source = resolved.get(canonical)
            if source:
                column = quote_identifier(source)
                if canonical == "timestamp":
                    expr = f"{timestamp_expr(column)} AS {quote_identifier(canonical)}"
                elif canonical in NUMERIC_COLUMNS:
                    expr = f"try_cast({column} AS DOUBLE) AS {quote_identifier(canonical)}"
                else:
                    expr = f"CAST({column} AS VARCHAR) AS {quote_identifier(canonical)}"
            else:
                if canonical == "timestamp":
                    expr = f"CAST(NULL AS TIMESTAMP) AS {quote_identifier(canonical)}"
                elif canonical in NUMERIC_COLUMNS:
                    expr = f"CAST(NULL AS DOUBLE) AS {quote_identifier(canonical)}"
                else:
                    expr = f"CAST(NULL AS VARCHAR) AS {quote_identifier(canonical)}"
            fields.append(expr)
        fields.append(f"{quote_literal(table_name)} AS source_table")
        fields.append(f"{quote_literal(meta['file'])} AS source_file")
        union_selects.append(f"SELECT {', '.join(fields)} FROM {quote_identifier(table_name)}")
    if union_selects:
        con.execute("DROP VIEW IF EXISTS flows")
        con.execute("DROP TABLE IF EXISTS flows")
        con.execute("CREATE OR REPLACE VIEW flows AS " + " UNION ALL ".join(union_selects))
    return resolved_all


def add_ip_udf(con: duckdb.DuckDBPyConnection) -> None:
    def ip_in_cidr(ip_value: Any, cidr_value: Any) -> bool:
        if ip_value in (None, "") or cidr_value in (None, ""):
            return False
        try:
            return ipaddress.ip_address(str(ip_value)) in ipaddress.ip_network(str(cidr_value), strict=False)
        except ValueError:
            return False

    con.create_function("ip_in_cidr", ip_in_cidr, ["VARCHAR", "VARCHAR"], "BOOLEAN")


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return quote_literal(str(value))


def build_where_clause(filters_json: str | None, start_time: str | None, end_time: str | None) -> str:
    clauses: list[str] = []
    if start_time:
        clauses.append(f"timestamp >= {quote_literal(start_time)}::TIMESTAMP")
    if end_time:
        clauses.append(f"timestamp <= {quote_literal(end_time)}::TIMESTAMP")
    if filters_json:
        payload = json.loads(filters_json)
        if isinstance(payload, dict):
            payload = [payload]
        for item in payload:
            field = quote_identifier(item["field"])
            op = item.get("op", "eq")
            value = item.get("value")
            if op == "eq":
                clauses.append(f"{field} = {sql_literal(value)}")
            elif op == "neq":
                clauses.append(f"{field} <> {sql_literal(value)}")
            elif op == "gt":
                clauses.append(f"{field} > {sql_literal(value)}")
            elif op == "gte":
                clauses.append(f"{field} >= {sql_literal(value)}")
            elif op == "lt":
                clauses.append(f"{field} < {sql_literal(value)}")
            elif op == "lte":
                clauses.append(f"{field} <= {sql_literal(value)}")
            elif op == "in":
                if not isinstance(value, list) or not value:
                    raise ValueError("Filter op 'in' requires a non-empty array")
                clauses.append(f"{field} IN ({', '.join(sql_literal(v) for v in value)})")
            elif op == "contains":
                clauses.append(f"CAST({field} AS VARCHAR) ILIKE {quote_literal('%' + str(value) + '%')}")
            elif op == "startswith":
                clauses.append(f"CAST({field} AS VARCHAR) ILIKE {quote_literal(str(value) + '%')}")
            elif op == "endswith":
                clauses.append(f"CAST({field} AS VARCHAR) ILIKE {quote_literal('%' + str(value))}")
            elif op == "in_cidr":
                clauses.append(f"ip_in_cidr(CAST({field} AS VARCHAR), {quote_literal(str(value))})")
            else:
                raise ValueError(f"Unsupported filter op: {op}")
    return "WHERE " + " AND ".join(clauses) if clauses else ""


def format_rows(columns: list[str], rows: list[tuple[Any, ...]]) -> str:
    if not rows:
        return "Query returned 0 rows."
    widths = [len(col) for col in columns]
    for row in rows:
        for i, value in enumerate(row):
            widths[i] = min(max(widths[i], len(str(value))), 48)
    header = " | ".join(columns[i].ljust(widths[i]) for i in range(len(columns)))
    sep = "-+-".join("-" * widths[i] for i in range(len(columns)))
    body = []
    for row in rows:
        body.append(" | ".join(str(row[i])[:48].ljust(widths[i]) for i in range(len(columns))))
    return "\n".join([header, sep] + body + [f"\n({len(rows)} rows)"])


def export_rows(columns: list[str], rows: list[tuple[Any, ...]], output_file: str) -> str:
    path = Path(output_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    ext = path.suffix.lower()
    if ext == ".csv":
        import csv

        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(rows)
    elif ext == ".json":
        with open(path, "w", encoding="utf-8") as f:
            json.dump([{columns[i]: row[i] for i in range(len(columns))} for row in rows], f, indent=2, ensure_ascii=False, default=str)
    elif ext == ".md":
        with open(path, "w", encoding="utf-8") as f:
            f.write("| " + " | ".join(columns) + " |\n")
            f.write("| " + " | ".join("---" for _ in columns) + " |\n")
            for row in rows:
                f.write("| " + " | ".join(str(value).replace("|", "\\|") for value in row) + " |\n")
    else:
        raise ValueError(f"Unsupported output format: {ext}. Use .csv, .json, or .md")
    return f"Results exported to {path} ({len(rows)} rows)"


def execute_render(con: duckdb.DuckDBPyConnection, sql: str, output_file: str | None = None) -> str:
    result = con.execute(sql)
    columns = [item[0] for item in result.description]
    rows = result.fetchall()
    if output_file:
        return export_rows(columns, rows, output_file)
    return format_rows(columns, rows)


def ensure_required(mappings: dict[str, dict[str, str]], columns: list[str]) -> None:
    available = {key for mapping in mappings.values() for key in mapping.keys()}
    missing = [column for column in columns if column not in available]
    if missing:
        raise ValueError(
            "Missing required canonical field(s): "
            + ", ".join(missing)
            + ". Update datasets/network-traffic/schema/field_mapping.yaml or use compatible files."
        )


def metric_sql(metric: str) -> str:
    if metric == "count":
        return "COUNT(*) AS count"
    agg, _, field = metric.partition(":")
    if not field:
        raise ValueError(f"Invalid metric specification: {metric}")
    column = quote_identifier(field)
    alias = quote_identifier(f"{agg}_{field}")
    if agg == "sum":
        return f"SUM(COALESCE({column}, 0)) AS {alias}"
    if agg == "avg":
        return f"AVG(COALESCE({column}, 0)) AS {alias}"
    if agg == "max":
        return f"MAX({column}) AS {alias}"
    if agg == "min":
        return f"MIN({column}) AS {alias}"
    if agg == "count_distinct":
        return f"COUNT(DISTINCT {column}) AS {alias}"
    raise ValueError(f"Unsupported metric aggregation: {agg}")


def inspect_action(con: duckdb.DuckDBPyConnection, table_info: dict[str, dict[str, str]], mappings: dict[str, dict[str, str]]) -> str:
    parts: list[str] = []
    for table_name, meta in table_info.items():
        columns = con.execute(f"DESCRIBE {quote_identifier(table_name)}").fetchall()
        row_count = con.execute(f"SELECT COUNT(*) FROM {quote_identifier(table_name)}").fetchone()[0]
        parts.append(f"\n{'=' * 72}")
        parts.append(f"Table: {table_name}")
        parts.append(f"Source file: {meta['file']}")
        parts.append(f"Rows: {row_count}")
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
        "SELECT COUNT(*) AS records, MIN(timestamp) AS min_time, MAX(timestamp) AS max_time, "
        "COUNT(DISTINCT src_ip) AS unique_src_ip, COUNT(DISTINCT dst_ip) AS unique_dst_ip FROM flows"
    ).fetchone()
    parts.append(f"\n{'=' * 72}")
    parts.append("Unified flows view")
    parts.append(f"Records: {summary[0]}")
    parts.append(f"Time range: {summary[1]} -> {summary[2]}")
    parts.append(f"Unique src_ip: {summary[3]}")
    parts.append(f"Unique dst_ip: {summary[4]}")
    return "\n".join(parts)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze tabular network traffic logs")
    parser.add_argument("--files", nargs="+", required=True, help="File paths or directories")
    parser.add_argument(
        "--action",
        required=True,
        choices=["inspect", "summary", "query", "topn", "timeseries", "distribution", "filter", "aggregate", "detect-anomaly", "export"],
    )
    parser.add_argument("--field-mapping", default=None, help="Path to field mapping YAML")
    parser.add_argument("--filters", default=None, help="JSON array or object of filters")
    parser.add_argument("--group-by", default=None, help="Comma-separated group-by fields")
    parser.add_argument("--metrics", default=None, help="Comma-separated metrics like count,sum:bytes")
    parser.add_argument("--time-column", default="timestamp", help="Reserved for future custom time-column support")
    parser.add_argument("--start-time", default=None, help="Inclusive time filter")
    parser.add_argument("--end-time", default=None, help="Inclusive time filter")
    parser.add_argument("--output-file", default=None, help="Export destination")
    parser.add_argument("--format", default="table", help="Reserved for future output format options")
    parser.add_argument("--sql", default=None, help="Custom SQL for query or export")
    parser.add_argument("--dimension", default="src_ip", help="Dimension for topn or distribution")
    parser.add_argument("--metric", default="bytes", help="Metric for topn")
    parser.add_argument("--limit", type=int, default=50, help="Row limit")
    parser.add_argument("--interval", choices=["minute", "hour", "day"], default="hour", help="Timeseries bucket size")
    parser.add_argument("--rule", default="scan-source", help="Anomaly rule")
    return parser


def main() -> int:
    ensure_cache_dir()
    parser = build_parser()
    args = parser.parse_args()

    files = discover_files(args.files)
    if not files:
        parser.error("No supported files found from --files")
    mapping = load_mapping(args.field_mapping)
    cache_key = compute_cache_key(files, mapping)
    db_path = CACHE_DIR / f"{cache_key}.duckdb"
    tables_path = CACHE_DIR / f"{cache_key}.tables.json"
    mappings_path = CACHE_DIR / f"{cache_key}.mappings.json"

    if db_path.exists() and tables_path.exists() and mappings_path.exists():
        con = duckdb.connect(str(db_path), read_only=False)
        table_info = load_json(tables_path) or {}
        mappings = load_json(mappings_path) or {}
        logger.info(f"Cache hit: {db_path}")
    else:
        con = duckdb.connect(str(db_path))
        table_info = load_sources(con, files)
        if not table_info:
            logger.error("No tables were loaded. Check file paths and formats.")
            return 1
        mappings = build_flows_view(con, table_info, mapping)
        save_json(tables_path, table_info)
        save_json(mappings_path, mappings)
        logger.info(f"Loaded {len(table_info)} source table(s)")

    add_ip_udf(con)
    mappings = build_flows_view(con, table_info, mapping)
    where_clause = build_where_clause(args.filters, args.start_time, args.end_time)

    try:
        if args.action == "inspect":
            output = inspect_action(con, table_info, mappings)
        elif args.action == "summary":
            output = execute_render(
                con,
                f"""
                WITH base AS (SELECT * FROM flows {where_clause})
                SELECT
                    COUNT(*) AS records,
                    MIN(timestamp) AS min_time,
                    MAX(timestamp) AS max_time,
                    COUNT(DISTINCT src_ip) AS unique_src_ip,
                    COUNT(DISTINCT dst_ip) AS unique_dst_ip,
                    SUM(COALESCE(bytes, 0)) AS total_bytes,
                    SUM(COALESCE(packets, 0)) AS total_packets,
                    AVG(COALESCE(flow_duration, 0)) AS avg_flow_duration
                FROM base
                """,
            )
            output += "\n\nTop protocol mix\n" + execute_render(
                con,
                f"""
                SELECT COALESCE(protocol, 'UNKNOWN') AS protocol, COUNT(*) AS records, SUM(COALESCE(bytes, 0)) AS total_bytes
                FROM flows
                {where_clause}
                GROUP BY 1
                ORDER BY total_bytes DESC, records DESC, protocol ASC
                LIMIT 10
                """,
            )
        elif args.action == "query":
            if not args.sql:
                parser.error("--sql is required for query")
            output = execute_render(con, args.sql, args.output_file)
        elif args.action == "topn":
            ensure_required(mappings, [args.dimension])
            metric_expr = "SUM(COALESCE(bytes, 0))"
            if args.metric == "packets":
                metric_expr = "SUM(COALESCE(packets, 0))"
            elif args.metric == "flows":
                metric_expr = "COUNT(*)"
            elif args.metric == "destinations":
                metric_expr = "COUNT(DISTINCT dst_ip)"
            elif args.metric == "ports":
                metric_expr = "COUNT(DISTINCT dst_port)"
            output = execute_render(
                con,
                f"""
                SELECT {quote_identifier(args.dimension)} AS dimension_value, {metric_expr} AS metric_value
                FROM flows
                {where_clause}
                GROUP BY 1
                ORDER BY metric_value DESC NULLS LAST, CAST(dimension_value AS VARCHAR) ASC
                LIMIT {args.limit}
                """,
                args.output_file,
            )
        elif args.action == "timeseries":
            ensure_required(mappings, ["timestamp"])
            output = execute_render(
                con,
                f"""
                SELECT DATE_TRUNC('{args.interval}', timestamp) AS bucket,
                       COUNT(*) AS records,
                       SUM(COALESCE(bytes, 0)) AS total_bytes,
                       SUM(COALESCE(packets, 0)) AS total_packets
                FROM flows
                {where_clause}
                GROUP BY 1
                ORDER BY 1
                """,
                args.output_file,
            )
        elif args.action == "distribution":
            ensure_required(mappings, [args.dimension])
            output = execute_render(
                con,
                f"""
                SELECT COALESCE(CAST({quote_identifier(args.dimension)} AS VARCHAR), 'NULL') AS bucket,
                       COUNT(*) AS records,
                       SUM(COALESCE(bytes, 0)) AS total_bytes
                FROM flows
                {where_clause}
                GROUP BY 1
                ORDER BY records DESC, total_bytes DESC, bucket ASC
                LIMIT {args.limit}
                """,
                args.output_file,
            )
        elif args.action == "filter":
            output = execute_render(
                con,
                f"SELECT * FROM flows {where_clause} ORDER BY timestamp NULLS LAST LIMIT {args.limit}",
                args.output_file,
            )
        elif args.action == "aggregate":
            groups = [item.strip() for item in (args.group_by or "").split(",") if item.strip()]
            if not groups:
                parser.error("--group-by is required for aggregate")
            metric_items = [item.strip() for item in (args.metrics or ",".join(mapping["default_metrics"])).split(",") if item.strip()]
            order_index = len(groups) + 1
            output = execute_render(
                con,
                f"""
                SELECT {', '.join(quote_identifier(group) for group in groups)},
                       {', '.join(metric_sql(metric) for metric in metric_items)}
                FROM flows
                {where_clause}
                GROUP BY {', '.join(quote_identifier(group) for group in groups)}
                ORDER BY {order_index} DESC NULLS LAST, {', '.join(quote_identifier(group) for group in groups)}
                """,
                args.output_file,
            )
        elif args.action == "detect-anomaly":
            if args.rule == "volume-spike":
                sql = f"""
                    WITH buckets AS (
                        SELECT DATE_TRUNC('hour', timestamp) AS bucket, SUM(COALESCE(bytes, 0)) AS total_bytes
                        FROM flows
                        {where_clause}
                        GROUP BY 1
                    )
                    SELECT bucket, total_bytes, AVG(total_bytes) OVER () AS avg_bytes,
                           CASE WHEN total_bytes > AVG(total_bytes) OVER () * 2 THEN 'spike' ELSE 'normal' END AS status
                    FROM buckets
                    ORDER BY total_bytes DESC
                """
            elif args.rule == "rare-port":
                sql = f"""
                    SELECT dst_port, COUNT(*) AS records
                    FROM flows
                    {where_clause}
                    GROUP BY 1
                    HAVING COUNT(*) <= 3
                    ORDER BY records ASC, dst_port ASC
                """
            elif args.rule == "failure-rate":
                sql = f"""
                    SELECT action, COUNT(*) AS records,
                           ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
                    FROM flows
                    {where_clause}
                    WHERE action IS NOT NULL
                    GROUP BY 1
                    ORDER BY pct DESC, records DESC
                """
            else:
                sql = f"""
                    SELECT src_ip, COUNT(*) AS flows, COUNT(DISTINCT dst_ip) AS unique_dst_ip,
                           COUNT(DISTINCT dst_port) AS unique_dst_port
                    FROM flows
                    {where_clause}
                    GROUP BY 1
                    HAVING COUNT(DISTINCT dst_ip) >= 5 OR COUNT(DISTINCT dst_port) >= 10
                    ORDER BY unique_dst_ip DESC, unique_dst_port DESC, flows DESC
                """
            output = execute_render(con, sql, args.output_file)
        else:
            if not args.output_file:
                parser.error("--output-file is required for export")
            sql = args.sql or f"SELECT * FROM flows {where_clause} ORDER BY timestamp NULLS LAST LIMIT {args.limit}"
            output = execute_render(con, sql, args.output_file)
        print(output)
        return 0
    except Exception as exc:
        logger.error(f"Error: {exc}")
        return 1
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
