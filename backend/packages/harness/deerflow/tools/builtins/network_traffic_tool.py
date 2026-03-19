from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Literal

from langchain.tools import tool


def _find_repo_root() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "skills" / "custom" / "network-traffic-analysis" / "scripts" / "analyze.py").exists():
            return parent
    raise FileNotFoundError("Could not locate repository root for network traffic analysis tool")


REPO_ROOT = _find_repo_root()
SKILL_SCRIPTS_DIR = REPO_ROOT / "skills" / "custom" / "network-traffic-analysis" / "scripts"
ANALYZE_SCRIPT = SKILL_SCRIPTS_DIR / "analyze.py"


def _append_optional_arg(command: list[str], flag: str, value: str | None) -> None:
    if value not in (None, ""):
        command.extend([flag, value])


def _normalize_references(reference: str | None, references: list[str] | None) -> list[str]:
    normalized: list[str] = []
    if reference not in (None, ""):
        normalized.extend(part.strip() for part in str(reference).split(",") if part.strip())
    for item in references or []:
        if item not in (None, ""):
            normalized.extend(part.strip() for part in str(item).split(",") if part.strip())
    deduped: list[str] = []
    seen: set[str] = set()
    for item in normalized:
        if item not in seen:
            deduped.append(item)
            seen.add(item)
    return deduped


def _build_command(
    references: list[str],
    action: str,
    filters: str | None,
    group_by: str | None,
    metrics: str | None,
    start_time: str | None,
    end_time: str | None,
    output_file: str | None,
    sql: str | None,
    dimension: str | None,
    metric: str | None,
    limit: int | None,
    interval: str | None,
    rule: str | None,
) -> list[str]:
    command = [sys.executable, str(ANALYZE_SCRIPT), "--files", *references, "--action", action]
    _append_optional_arg(command, "--filters", filters)
    _append_optional_arg(command, "--group-by", group_by)
    _append_optional_arg(command, "--metrics", metrics)
    _append_optional_arg(command, "--start-time", start_time)
    _append_optional_arg(command, "--end-time", end_time)
    _append_optional_arg(command, "--output-file", output_file)
    _append_optional_arg(command, "--sql", sql)
    _append_optional_arg(command, "--dimension", dimension)
    _append_optional_arg(command, "--metric", metric)
    if limit is not None:
        command.extend(["--limit", str(limit)])
    _append_optional_arg(command, "--interval", interval)
    _append_optional_arg(command, "--rule", rule)
    return command


def _run_command(command: list[str]) -> str:
    completed = subprocess.run(
        command,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    stdout = (completed.stdout or "").strip()
    stderr = (completed.stderr or "").strip()
    if completed.returncode == 0:
        if stderr:
            return f"{stderr}\n\n{stdout}".strip()
        return stdout or "Analysis completed successfully."
    parts = ["Network traffic analysis failed."]
    parts.append(f"Command: {' '.join(command)}")
    if stderr:
        parts.append(f"stderr:\n{stderr}")
    if stdout:
        parts.append(f"stdout:\n{stdout}")
    return "\n\n".join(parts)


@tool("network_traffic_analyze", parse_docstring=True)
def network_traffic_analyze_tool(
    action: Literal[
        "inspect",
        "summary",
        "query",
        "topn",
        "timeseries",
        "distribution",
        "filter",
        "aggregate",
        "detect-anomaly",
        "export",
    ],
    reference: str | None = None,
    references: list[str] | None = None,
    filters: str | None = None,
    group_by: str | None = None,
    metrics: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    output_file: str | None = None,
    sql: str | None = None,
    dimension: str | None = None,
    metric: str | None = None,
    limit: int | None = 50,
    interval: Literal["minute", "hour", "day"] | None = "hour",
    rule: str | None = None,
) -> str:
    """Analyze network traffic datasets stored on the host under datasets/network-traffic or at explicit host paths.

    Use this tool when the user wants to analyze network traffic logs that already exist on the server.
    This tool runs host-side analysis scripts directly, so it does not depend on sandbox-visible `/mnt/user-data/...` paths.

    Preferred references:
    - Exact filename like `Gmail.flow.csv`
    - Dataset-relative suffix like `ustc_tfc2016/flow/Gmail.flow.csv`
    - Full repo-relative path like `datasets/network-traffic/processed/ustc_tfc2016/flow/Gmail.flow.csv`
    - Explicit absolute or relative host path if the file is outside the default dataset roots

    Common actions:
    - `inspect`: inspect schema and sample rows
    - `summary`: overall traffic summary and protocol mix
    - `topn`: top values by dimension and metric
    - `timeseries`: traffic trend buckets
    - `distribution`: categorical breakdown
    - `filter`: return filtered rows
    - `aggregate`: grouped metrics
    - `detect-anomaly`: rule-based anomaly checks
    - `query`: run custom SQL against the unified `flows` view
    - `export`: export result rows to a file

    Args:
        reference: Single file or directory reference for the dataset. Shorthand names are resolved under `datasets/network-traffic/raw` and `datasets/network-traffic/processed`.
        references: Optional list of file or directory references to analyze together.
        action: Analysis action to perform.
        filters: Optional JSON filter payload accepted by the analysis script.
        group_by: Optional comma-separated group-by fields for aggregate actions.
        metrics: Optional comma-separated metrics such as `count,sum:bytes`.
        start_time: Optional inclusive time filter.
        end_time: Optional inclusive time filter.
        output_file: Optional export destination path, typically under `datasets/network-traffic/outputs`.
        sql: Optional SQL query for `query` or `export`.
        dimension: Optional dimension for `topn` or `distribution`.
        metric: Optional metric selector for `topn`.
        limit: Optional row limit.
        interval: Optional bucket size for `timeseries`.
        rule: Optional anomaly rule for `detect-anomaly`.
    """
    all_references = _normalize_references(reference, references)
    if not all_references:
        return "Network traffic analysis failed.\n\nNo dataset reference was provided."
    command = _build_command(
        references=all_references,
        action=action,
        filters=filters,
        group_by=group_by,
        metrics=metrics,
        start_time=start_time,
        end_time=end_time,
        output_file=output_file,
        sql=sql,
        dimension=dimension,
        metric=metric,
        limit=limit,
        interval=interval,
        rule=rule,
    )
    return _run_command(command)
