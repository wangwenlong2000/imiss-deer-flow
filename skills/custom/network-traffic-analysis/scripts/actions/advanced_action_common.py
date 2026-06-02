from __future__ import annotations

from typing import Any

from analysis.feature_engineering import rows_from_query
from core.schema_mapping import available_canonical_fields
from utils.formatter import _rows_to_tuples, format_rows


def fetch_rows(con: Any, sql: str) -> list[dict[str, Any]]:
    _, rows = rows_from_query(con, sql)
    return rows


def format_dict_rows(rows: list[dict[str, Any]], columns: list[str] | None = None) -> str:
    if not rows:
        return "Query returned 0 rows."
    final_columns = columns or list(rows[0].keys())
    return format_rows(final_columns, _rows_to_tuples(final_columns, rows))


def append_file_errors(output: list[str], results: dict[str, Any]) -> None:
    error_rows = []
    for file_result in results.get("files_analyzed", []):
        error = file_result.get("error")
        if error:
            error_rows.append({"file": file_result.get("file", "selected scope"), "error": error})
    if error_rows:
        output.append("\n## Errors\n")
        output.append(format_dict_rows(error_rows, ["file", "error"]))


def present_fields(mappings: dict[str, dict[str, str]]) -> set[str]:
    return available_canonical_fields(mappings)


def scoped_where(where_clause: str, predicate: str) -> str:
    return f"{where_clause} {'AND' if where_clause else 'WHERE'} {predicate}"
