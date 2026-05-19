from __future__ import annotations

import ipaddress
import json
import logging
import os
from pathlib import Path
from typing import Any, Literal

from constants import CANONICAL_COLUMNS, NUMERIC_COLUMNS, FLOW_PREFERRED_FIELDS, PACKET_PREFERRED_FIELDS
from utils.io import ensure_yaml
from utils.path import repo_root, dataset_root, skill_root, normalize_name, to_repo_relative_display
from utils.sql import quote_identifier, quote_literal, sanitize_table_name

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

logger = logging.getLogger(__name__)


def default_field_mapping_candidates() -> list[Path]:
    """Return ordered list of candidate paths for field_mapping.yaml.

    Priority:
    1. --field-mapping (explicit CLI arg, handled by caller)
    2. $NETWORK_TRAFFIC_FIELD_MAPPING env var
    3. dataset_root()/schema/field_mapping.yaml
    4. skill_root()/config/field_mapping.yaml
    5. repo_root()/datasets/network-traffic/schema/field_mapping.yaml
    """
    candidates: list[Path] = []

    env_path = os.environ.get("NETWORK_TRAFFIC_FIELD_MAPPING")
    if env_path:
        candidates.append(Path(env_path))

    candidates.extend([
        dataset_root() / "schema" / "field_mapping.yaml",
        skill_root() / "config" / "field_mapping.yaml",
        repo_root() / "datasets" / "network-traffic" / "schema" / "field_mapping.yaml",
    ])

    deduped: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key not in seen:
            deduped.append(candidate)
            seen.add(key)
    return deduped


def _parse_yaml_mapping(path: Path, yaml_module: Any) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        payload = yaml_module.safe_load(f) or {}
    payload.setdefault("canonical_fields", {})
    payload.setdefault("profiles", {})
    payload.setdefault("default_metrics", ["count", "sum:bytes", "sum:packets", "avg:flow_duration"])
    payload.setdefault("_metadata", {})
    payload["_metadata"]["field_mapping_path"] = str(path)
    return payload


def load_mapping(path: str | None = None) -> dict[str, Any]:
    yaml_module = ensure_yaml()

    if path is not None:
        mapping_path = Path(path)
        if not mapping_path.exists():
            raise FileNotFoundError(f"Field mapping file not found at explicit path: {mapping_path}")
        return _parse_yaml_mapping(mapping_path, yaml_module)

    for candidate in default_field_mapping_candidates():
        if candidate.exists():
            return _parse_yaml_mapping(candidate, yaml_module)

    searched = "\n".join(f"- {p}" for p in default_field_mapping_candidates())
    raise FileNotFoundError(
        f"Field mapping file not found. Searched:\n{searched}\n"
        "Mount /mnt/datasets/network-traffic/schema/field_mapping.yaml, "
        "install the skill with config/field_mapping.yaml, or pass --field-mapping."
    )


IngestionMode = Literal["strict", "lenient"]


def _count_physical_rows(path: Path) -> int | None:
    try:
        with path.open("rb") as handle:
            line_count = sum(1 for _ in handle)
    except OSError:
        return None
    return max(0, line_count - 1)


def _count_null_key_rows(con: "duckdb.DuckDBPyConnection", table_name: str) -> int | None:
    """Count rows with NULL src_ip or dst_ip in a source table.

    This is a proxy signal: it captures both null-padded rows from lenient CSV
    ingestion AND legitimate captures that lack IP fields (e.g., ARP, DHCP).
    Returns None when the table lacks src_ip and dst_ip columns.
    """
    try:
        cols = {row[0] for row in con.execute(f"DESCRIBE {quote_identifier(table_name)}").fetchall()}
    except Exception:
        return None
    has_src = "src_ip" in cols
    has_dst = "dst_ip" in cols
    if not has_src or not has_dst:
        return None
    try:
        result = con.execute(
            f"SELECT COUNT(*) FROM {quote_identifier(table_name)} "
            "WHERE src_ip IS NULL OR dst_ip IS NULL"
        ).fetchone()
        return int(result[0]) if result else 0
    except Exception:
        return None


def _csv_ingestion_metadata(
    path: Path,
    loaded_rows: int,
    mode: IngestionMode,
    *,
    con: "duckdb.DuckDBPyConnection" | None = None,
    table_name: str | None = None,
) -> dict[str, Any]:
    physical_rows = _count_physical_rows(path)
    metadata: dict[str, Any] = {
        "ingestion_mode": mode,
        "rows_loaded": loaded_rows,
    }
    if physical_rows is not None:
        metadata["physical_data_rows"] = physical_rows

    dropped = max(0, (physical_rows or 0) - loaded_rows) if physical_rows is not None else None
    metadata["approx_dropped_rows"] = dropped

    null_key: int | None = None
    if mode == "lenient" and con is not None and table_name is not None:
        null_key = _count_null_key_rows(con, table_name)
    if null_key is not None:
        metadata["approx_null_key_rows"] = null_key

    if mode == "lenient":
        metadata["ingestion_warning"] = (
            "CSV loaded with ignore_errors=true and null_padding=true; malformed rows may be skipped "
            "or padded by DuckDB. Compare physical_data_rows with rows_loaded for drop estimate; "
            "approx_null_key_rows counts rows with null src_ip/dst_ip (may include legitimate captures "
            "lacking IP fields, not just null-padded rows)."
        )
    return metadata


def load_sources(
    con: "duckdb.DuckDBPyConnection",
    files: list[str],
    *,
    ingestion_mode: IngestionMode = "lenient",
    quiet: bool = False,
) -> dict[str, dict[str, Any]]:
    table_info: dict[str, dict[str, Any]] = {}
    for file_path in files:
        path = Path(file_path)
        if not path.exists():
            if not quiet:
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
            if ingestion_mode == "strict":
                csv_options = "delim=',', header=true, SAMPLE_SIZE=-1"
            else:
                csv_options = "delim=',', header=true, SAMPLE_SIZE=-1, ignore_errors=true, null_padding=true"
            sql = (
                f"CREATE TABLE {quote_identifier(table_name)} AS "
                f"SELECT * FROM read_csv_auto("
                f"{quote_literal(str(path))}, "
                f"{csv_options})"
            )
            ingestion_metadata: dict[str, Any] = {"ingestion_mode": ingestion_mode}
        elif ext == ".parquet":
            sql = f"CREATE TABLE {quote_identifier(table_name)} AS SELECT * FROM read_parquet({quote_literal(str(path))})"
            ingestion_metadata = {"ingestion_mode": "strict"}
        elif ext in {".json", ".jsonl"}:
            sql = f"CREATE TABLE {quote_identifier(table_name)} AS SELECT * FROM read_json_auto({quote_literal(str(path))}, format='auto')"
            ingestion_metadata = {"ingestion_mode": "strict"}
        elif ext in {".xlsx", ".xls"}:
            con.execute("INSTALL spatial; LOAD spatial;")
            sql = (
                f"CREATE TABLE {quote_identifier(table_name)} AS "
                f"SELECT * FROM st_read({quote_literal(str(path))}, open_options = ['HEADERS=FORCE', 'FIELD_TYPES=AUTO'])"
            )
            ingestion_metadata = {"ingestion_mode": "strict"}
        else:
            if not quiet:
                logger.warning(f"Unsupported file format: {ext} ({file_path})")
            continue

        try:
            con.execute(sql)
            loaded_rows = int(con.execute(f"SELECT COUNT(*) FROM {quote_identifier(table_name)}").fetchone()[0])
            if ext == ".csv":
                ingestion_metadata = _csv_ingestion_metadata(
                    path, loaded_rows, ingestion_mode, con=con, table_name=table_name,
                )
            else:
                ingestion_metadata["rows_loaded"] = loaded_rows
            table_info[table_name] = {"file": to_repo_relative_display(path), **ingestion_metadata}
        except Exception as exc:
            if not quiet:
                logger.warning(f"Failed to load {file_path}: {exc}")
    return table_info


def get_columns(con: "duckdb.DuckDBPyConnection", table_name: str) -> list[str]:
    return [row[0] for row in con.execute(f"DESCRIBE {quote_identifier(table_name)}").fetchall()]


def _dedupe_aliases(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = normalize_name(value)
        if normalized and normalized not in seen:
            deduped.append(value)
            seen.add(normalized)
    return deduped


def merged_aliases(mapping: dict[str, Any], profile_name: str | None = None) -> dict[str, list[str]]:
    aliases: dict[str, list[str]] = {}
    base_aliases = mapping.get("canonical_fields", {})
    for canonical in CANONICAL_COLUMNS:
        aliases[canonical] = list(base_aliases.get(canonical, []))

    if profile_name:
        profiles = mapping.get("profiles", {})
        profile = profiles.get(profile_name, {})
        for canonical, extra_aliases in profile.get("canonical_fields", {}).items():
            aliases.setdefault(canonical, [])
            aliases[canonical].extend(extra_aliases or [])

    for canonical in aliases:
        aliases[canonical] = _dedupe_aliases(aliases[canonical])
    return aliases


def select_mapping_profile(columns: list[str], mapping: dict[str, Any]) -> str | None:
    normalized = {normalize_name(col): col for col in columns}
    best_profile: str | None = None
    best_score = 0

    for profile_name, profile in (mapping.get("profiles", {}) or {}).items():
        score = 0
        for aliases in (profile.get("canonical_fields", {}) or {}).values():
            for alias in aliases or []:
                if normalize_name(alias) in normalized:
                    score += 1
                    break
        if score > best_score:
            best_profile = profile_name
            best_score = score

    return best_profile if best_score > 0 else None


def detect_mapping(columns: list[str], mapping: dict[str, Any]) -> tuple[dict[str, str], str | None]:
    profile_name = select_mapping_profile(columns, mapping)
    aliases = merged_aliases(mapping, profile_name)
    normalized = {normalize_name(col): col for col in columns}
    resolved: dict[str, str] = {}
    for canonical in CANONICAL_COLUMNS:
        exact = normalized.get(normalize_name(canonical))
        if exact:
            resolved[canonical] = exact
            continue
        for alias in aliases.get(canonical, []):
            hit = normalized.get(normalize_name(alias))
            if hit:
                resolved[canonical] = hit
                break
    return resolved, profile_name


def timestamp_expr(column_sql: str) -> str:
    return (
        f"COALESCE(try_cast({column_sql} AS TIMESTAMP), "
        f"to_timestamp(try_cast({column_sql} AS DOUBLE)), "
        f"try_strptime(CAST({column_sql} AS VARCHAR), '%Y-%m-%d %H:%M:%S'), "
        f"try_strptime(CAST({column_sql} AS VARCHAR), '%Y-%m-%dT%H:%M:%S'), "
        f"try_strptime(CAST({column_sql} AS VARCHAR), '%Y-%m-%dT%H:%M:%S.%f'))"
    )


def numeric_expr(column_sql: str) -> str:
    return f"try_cast({column_sql} AS DOUBLE)"


def booleanish_expr(column_sql: str) -> str:
    return (
        "CASE "
        f"WHEN lower(trim(CAST({column_sql} AS VARCHAR))) IN ('true', '1', 'yes', 'y') THEN TRUE "
        f"WHEN lower(trim(CAST({column_sql} AS VARCHAR))) IN ('false', '0', 'no', 'n') THEN FALSE "
        "ELSE NULL END"
    )


def relative_interval_seconds(interval: str) -> int:
    return {"minute": 60, "hour": 3600, "day": 86400}[interval]


def analysis_time_bucket_expr(interval: str) -> str:
    seconds = relative_interval_seconds(interval)
    return (
        "CASE "
        f"WHEN analysis_time_kind = 'absolute' AND analysis_time_ts IS NOT NULL THEN CAST(DATE_TRUNC('{interval}', analysis_time_ts) AS VARCHAR) "
        f"WHEN analysis_time_kind = 'relative' AND analysis_time_relative_s IS NOT NULL THEN CONCAT('t+', CAST(CAST(FLOOR(analysis_time_relative_s / {seconds}) * {seconds} AS BIGINT) AS VARCHAR), 's') "
        "ELSE 'unknown' END"
    )


def build_flows_view(
    con: "duckdb.DuckDBPyConnection",
    table_info: dict[str, dict[str, Any]],
    mapping: dict[str, Any],
) -> dict[str, dict[str, str]]:
    resolved_all: dict[str, dict[str, str]] = {}
    union_selects: list[str] = []
    for table_name, meta in table_info.items():
        resolved, profile_name = detect_mapping(get_columns(con, table_name), mapping)
        resolved_all[table_name] = resolved
        if profile_name:
            meta["mapping_profile"] = profile_name
        fields: list[str] = []
        timestamp_source = resolved.get("timestamp")
        absolute_time_expr = timestamp_expr(quote_identifier(timestamp_source)) if timestamp_source else "CAST(NULL AS TIMESTAMP)"
        relative_time_candidates: list[str] = []
        if resolved.get("start_relative_time_s"):
            relative_time_candidates.append(numeric_expr(quote_identifier(resolved["start_relative_time_s"])))
        if resolved.get("relative_time_s"):
            relative_time_candidates.append(numeric_expr(quote_identifier(resolved["relative_time_s"])))
        relative_time_candidates.append("CAST(NULL AS DOUBLE)")
        relative_time_expr_sql = "COALESCE(" + ", ".join(relative_time_candidates) + ")"
        if resolved.get("time_is_relative"):
            relative_flag_expr = booleanish_expr(quote_identifier(resolved["time_is_relative"]))
        else:
            relative_flag_expr = "CAST(NULL AS BOOLEAN)"
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
        fields.append(f"{absolute_time_expr} AS analysis_time_ts")
        fields.append(f"{relative_time_expr_sql} AS analysis_time_relative_s")
        fields.append(
            "CASE "
            f"WHEN COALESCE({relative_flag_expr}, FALSE) THEN 'relative' "
            f"WHEN {absolute_time_expr} IS NOT NULL THEN 'absolute' "
            f"WHEN {relative_time_expr_sql} IS NOT NULL THEN 'relative' "
            "ELSE 'unknown' END AS analysis_time_kind"
        )
        fields.append(
            "CASE "
            f"WHEN {absolute_time_expr} IS NOT NULL THEN CAST({absolute_time_expr} AS VARCHAR) "
            f"WHEN COALESCE({relative_flag_expr}, FALSE) OR {relative_time_expr_sql} IS NOT NULL THEN CONCAT('t+', CAST({relative_time_expr_sql} AS VARCHAR), 's') "
            "ELSE NULL END AS analysis_time_display"
        )
        fields.append(f"{quote_literal(table_name)} AS source_table")
        fields.append(f"{quote_literal(meta['file'])} AS source_file")
        union_selects.append(f"SELECT {', '.join(fields)} FROM {quote_identifier(table_name)}")
    if union_selects:
        # Use a temp view so cached databases can stay read-only while the
        # current session still gets a unified canonical `flows` relation.
        con.execute("CREATE OR REPLACE TEMP VIEW flows AS " + " UNION ALL ".join(union_selects))
    return resolved_all


def add_ip_udf(con: "duckdb.DuckDBPyConnection") -> None:
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
        clauses.append(f"analysis_time_ts >= {quote_literal(start_time)}::TIMESTAMP")
    if end_time:
        clauses.append(f"analysis_time_ts <= {quote_literal(end_time)}::TIMESTAMP")
    if filters_json:
        try:
            payload = json.loads(filters_json)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Invalid JSON in --filters: {exc}\n"
                "Hint: Use a JSON array of objects like [{\"field\": \"src_ip\", \"value\": \"10.0.0.1\"}]"
            ) from exc
        if isinstance(payload, dict):
            payload = [payload]
        if not isinstance(payload, list):
            raise ValueError(
                f"--filters must be a JSON array or object, got {type(payload).__name__}.\n"
                "Hint: Use [{\"field\": \"src_ip\", \"value\": \"10.0.0.1\"}] or {{\"field\": \"src_ip\", \"value\": \"10.0.0.1\"}}"
            )
        for item in payload:
            if "field" not in item:
                raise ValueError(
                    f"Filter item missing required 'field' key: {item}\n"
                    "Hint: Each filter must have a 'field' key, e.g. {{\"field\": \"src_ip\", \"value\": \"10.0.0.1\"}}"
                )
            field = quote_identifier(item["field"])
            op = str(item.get("op", "eq")).strip().lower()
            value = item.get("value")
            op_aliases = {
                "=": "eq",
                "==": "eq",
                "eq": "eq",
                "!=": "neq",
                "<>": "neq",
                "neq": "neq",
                ">": "gt",
                "gt": "gt",
                ">=": "gte",
                "gte": "gte",
                "<": "lt",
                "lt": "lt",
                "<=": "lte",
                "lte": "lte",
            }
            op = op_aliases.get(op, op)
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
                if value is None:
                    clauses.append(f"CAST({field} AS VARCHAR) IS NULL")
                else:
                    clauses.append(f"CAST({field} AS VARCHAR) ILIKE {quote_literal('%' + str(value) + '%')}")
            elif op == "startswith":
                if value is None:
                    clauses.append(f"CAST({field} AS VARCHAR) IS NULL")
                else:
                    clauses.append(f"CAST({field} AS VARCHAR) ILIKE {quote_literal(str(value) + '%')}")
            elif op == "endswith":
                if value is None:
                    clauses.append(f"CAST({field} AS VARCHAR) IS NULL")
                else:
                    clauses.append(f"CAST({field} AS VARCHAR) ILIKE {quote_literal('%' + str(value))}")
            elif op == "in_cidr":
                if value is None:
                    raise ValueError("Filter op 'in_cidr' requires a non-null CIDR value")
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


def render_rows_section(title: str, columns: list[str], rows: list[tuple[Any, ...]]) -> str:
    return title + "\n" + format_rows(columns, rows)


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


def execute_render(con: "duckdb.DuckDBPyConnection", sql: str, output_file: str | None = None) -> str:
    try:
        import duckdb as _dd  # noqa: F811
    except ImportError:
        return "DuckDB is not available. Install it before running analysis."

    try:
        result = con.execute(sql)
    except _dd.ParserException as exc:
        return (
            f"SQL Binding Error: {exc}\n"
            "Hint: Column or table name may not exist. Run --action inspect to see available fields."
        )
    except _dd.CatalogException as exc:
        return (
            f"Catalog Error: {exc}\n"
            "Hint: Referenced table or view does not exist. Available: flows."
        )
    except _dd.ConversionException as exc:
        return (
            f"Type Conversion Error: {exc}\n"
            "Hint: Check that column types match the expected SQL operations."
        )
    except _dd.IOException as exc:
        return (
            f"I/O Error: {exc}\n"
            "Hint: Check file paths or permissions for external file operations."
        )
    except Exception as exc:
        return f"SQL Execution Error: {exc}\nHint: Use --action inspect to review schema, or try a simpler query first."

    # Handle non-SELECT statements where result.description is None
    if result.description is None:
        return "Statement executed successfully (no result set)."

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


def available_canonical_fields(mappings: dict[str, dict[str, str]]) -> set[str]:
    return {key for mapping in mappings.values() for key in mapping.keys()}


def infer_analysis_view(
    files: list[str],
    explicit_view: str,
    *,
    action: str,
    dimension: str | None,
    rule: str | None,
) -> str:
    if explicit_view in {"flow", "packet"}:
        return explicit_view

    lower_files = [Path(item).name.lower() for item in files]
    if any(".packet." in name or name.endswith("packet.csv") for name in lower_files):
        return "packet"
    if any(".flow." in name or name.endswith("flow.csv") for name in lower_files):
        return "flow"

    if rule in {"syn-scan", "rst-heavy", "handshake-failure", "icmp-probe", "small-packet-burst"}:
        return "packet"
    if dimension and dimension in PACKET_PREFERRED_FIELDS:
        return "packet"
    if dimension and dimension in FLOW_PREFERRED_FIELDS:
        return "flow"
    if action == "packet-review":
        return "packet"
    if action in {"overview-report", "scan-review", "session-review", "short-connection-review", "protocol-review", "dns-tunnel-review", "data-exfiltration-review", "lateral-movement-review", "signature-review", "risk-fusion-review", "summary", "topn", "distribution", "timeseries", "aggregate", "detect-anomaly"}:
        return "flow"
    return "flow"


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


def render_section(
    con: "duckdb.DuckDBPyConnection",
    title: str,
    sql: str,
    *,
    output_file: str | None = None,
) -> str:
    return title + "\n" + execute_render(con, sql, output_file)
