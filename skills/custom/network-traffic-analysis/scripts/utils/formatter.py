from __future__ import annotations

import csv
import json
from io import StringIO
from pathlib import Path
from typing import Any


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


def execute_render(con: duckdb.DuckDBPyConnection, sql: str, output_file: str | None = None) -> str:
    # Delegate to canonical implementation in core.schema_mapping for single source of truth
    from core.schema_mapping import execute_render as _canonical_execute_render
    return _canonical_execute_render(con, sql, output_file)


def render_section(
    con: duckdb.DuckDBPyConnection,
    title: str,
    sql: str,
    *,
    output_file: str | None = None,
) -> str:
    return title + "\n" + execute_render(con, sql, output_file)


def _rows_to_tuples(columns: list[str], rows: list[dict[str, Any]]) -> list[tuple[Any, ...]]:
    return [tuple(row.get(column) for column in columns) for row in rows]
