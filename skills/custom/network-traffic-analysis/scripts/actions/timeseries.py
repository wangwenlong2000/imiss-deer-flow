"""Compatibility wrapper: timeseries action now lives in timeseries_action.py."""

from __future__ import annotations

from typing import Any

import duckdb  # type: ignore

from actions.timeseries_action import execute_timeseries as _execute_timeseries
from actions.timeseries_action import format_results as _format_timeseries


def timeseries_action(
    con: duckdb.DuckDBPyConnection,
    where_clause: str,
    interval: str = "hour",
    limit: int = 20,
    output_file: str | None = None,
) -> str:
    """Legacy entry point for timeseries action.

    Preserves the old positional-argument signature:
        timeseries_action(con, where_clause, interval, limit, output_file)

    while calling the new module implementation.
    """
    results = _execute_timeseries(
        con,
        where_clause,
        [],  # files list (unused for old signature, empty list is safe)
        interval=interval,
        limit=limit,
        output_file=output_file,
    )
    return _format_timeseries(results)
