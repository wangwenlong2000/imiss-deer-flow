import csv
import json
import re
from pathlib import Path
from typing import Annotated

from langchain.tools import InjectedToolCallId, ToolRuntime, tool
from langchain_core.messages import ToolMessage
from langgraph.types import Command
from langgraph.typing import ContextT
from pydantic import BaseModel, Field, TypeAdapter

from deerflow.agents.thread_state import ThreadState
from deerflow.config.paths import VIRTUAL_PATH_PREFIX, get_paths
from deerflow.delivery import OUTPUT_PREFIX, file_digest
from deerflow.routing.turn_requirements import explicit_decimal_places, requirements_from_context

OUTPUTS_VIRTUAL_PREFIX = f"{VIRTUAL_PATH_PREFIX}/outputs"


class CsvDeliveryCheck(BaseModel):
    filepath: str
    required_columns: list[str] = Field(default_factory=list)
    decimal_places: dict[str, int] = Field(default_factory=dict)
    metric_columns: dict[str, str] = Field(default_factory=dict)


def _requested_decimal_places(runtime) -> int | None:
    intent = (runtime.state or {}).get("intent_context") or {}
    return explicit_decimal_places(str(intent.get("original_query") or ""))


def _check_csv_delivery(runtime, normalized_paths: list[str], checks) -> tuple[list[str], dict[str, dict[str, str]]]:
    if isinstance(checks, str):
        checks = json.loads(checks)
    if isinstance(checks, (CsvDeliveryCheck, dict)):
        checks = [checks]
    parsed = TypeAdapter(list[CsvDeliveryCheck]).validate_python(checks or [])
    declared = {_normalize_presented_filepath(runtime, c.filepath): c for c in parsed}
    if any(p not in normalized_paths or Path(p).suffix.lower() != ".csv" for p in declared):
        raise ValueError("Each CSV delivery check must refer to a presented CSV file")
    requirements = requirements_from_context((runtime.state or {}).get("intent_context"))
    uniform = _requested_decimal_places(runtime)
    summaries = []
    verified_mappings = {}
    outputs_dir = Path(runtime.state["thread_data"]["outputs_path"])
    for virtual in normalized_paths:
        actual = outputs_dir / virtual.removeprefix(OUTPUT_PREFIX)
        if not actual.is_file():
            raise ValueError(f"Presented file does not exist: {Path(virtual).name}")
        if Path(virtual).suffix.lower() != ".csv":
            continue
        check = declared.get(virtual, CsvDeliveryCheck(filepath=virtual))
        if requirements.required_csv_metrics and (unknown := set(check.metric_columns) - set(requirements.required_csv_metrics)):
            raise ValueError(f"Unknown metric labels {sorted(unknown)}; use exact current-turn labels {requirements.required_csv_metrics}. Each file may declare a subset and keep its existing headers.")
        with actual.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle, strict=True)
            if not reader.fieldnames:
                raise ValueError(f"CSV {actual.name} must contain a header even when there are no rows")
            if any(not column.strip() for column in reader.fieldnames) or len(set(reader.fieldnames)) != len(reader.fieldnames):
                raise ValueError(f"CSV {actual.name} must have nonempty, unique column names")
            # Exact header identity is already verifiable evidence. Preserve
            # explicit mappings for aliases, but never demand redundant LLM
            # declarations for a requested metric whose literal column exists.
            mapping = {metric: metric for metric in requirements.required_csv_metrics if metric in reader.fieldnames}
            mapping.update(check.metric_columns)
            precision = dict(check.decimal_places)
            for metric, column in mapping.items():
                if metric in requirements.csv_precision:
                    precision[column] = requirements.csv_precision[metric]
            if uniform is not None and not precision:
                raise ValueError(f"CSV delivery requires delivery_checks with decimal_places={uniform} for computed result columns")
            required = set(check.required_columns) | set(precision) | set(mapping.values())
            if missing := required - set(reader.fieldnames):
                raise ValueError(f"CSV {actual.name} missing required columns: {sorted(missing)}")
            preview = []
            for row_index, row in enumerate(reader, 2):
                if None in row or any(value is None for value in row.values()):
                    raise ValueError(f"CSV {actual.name} row ending at line {reader.line_num}: column count differs from its header. Use one rectangular table per CSV; export multiple tables as separate CSV files or JSON.")
                for column, digits in precision.items():
                    if not 0 <= digits <= 12:
                        raise ValueError("decimal_places must be between 0 and 12")
                    value = str(row.get(column) or "").strip().removesuffix("%")
                    if not value or value in requirements.csv_missing_values:
                        continue
                    value = value.replace(",", "")
                    pattern = r"[+-]?\d+" + (rf"\.\d{{{digits}}}" if digits else "")
                    if not re.fullmatch(pattern, value):
                        raise ValueError(f"CSV {actual.name} row {row_index} column {column}: expected {digits} decimal places or an explicitly requested missing-value marker")
                if len(preview) < 5:
                    preview.append({k: row.get(k) for k in (sorted(required) or reader.fieldnames[:12])})
            summaries.append(json.dumps({"file": virtual, "verified_columns": sorted(required), "precision": precision, "preview": preview}, ensure_ascii=False))
            verified_mappings[virtual] = mapping
    return summaries, verified_mappings


def _normalize_presented_filepath(
    runtime: ToolRuntime[ContextT, ThreadState],
    filepath: str,
) -> str:
    """Normalize a presented file path to the `/mnt/user-data/outputs/*` contract.

    Accepts either:
    - A virtual sandbox path such as `/mnt/user-data/outputs/report.md`
    - A host-side thread outputs path such as
      `/app/backend/.deer-flow/threads/<thread>/user-data/outputs/report.md`

    Returns:
        The normalized virtual path.

    Raises:
        ValueError: If runtime metadata is missing or the path is outside the
            current thread's outputs directory.
    """
    if runtime.state is None:
        raise ValueError("Thread runtime state is not available")

    thread_id = runtime.context.get("thread_id")
    if not thread_id:
        raise ValueError("Thread ID is not available in runtime context")

    thread_data = runtime.state.get("thread_data") or {}
    outputs_path = thread_data.get("outputs_path")
    if not outputs_path:
        raise ValueError("Thread outputs path is not available in runtime state")

    outputs_dir = Path(outputs_path).resolve()
    stripped = filepath.lstrip("/")
    virtual_prefix = VIRTUAL_PATH_PREFIX.lstrip("/")

    if stripped == virtual_prefix or stripped.startswith(virtual_prefix + "/"):
        actual_path = get_paths().resolve_virtual_path(thread_id, filepath)
    else:
        actual_path = Path(filepath).expanduser().resolve()

    try:
        relative_path = actual_path.relative_to(outputs_dir)
    except ValueError as exc:
        raise ValueError(f"Only files in {OUTPUTS_VIRTUAL_PREFIX} can be presented: {filepath}") from exc

    return f"{OUTPUTS_VIRTUAL_PREFIX}/{relative_path.as_posix()}"


@tool("present_files", parse_docstring=True)
def present_file_tool(
    runtime: ToolRuntime[ContextT, ThreadState],
    filepaths: list[str],
    tool_call_id: Annotated[str, InjectedToolCallId],
    delivery_checks: list[CsvDeliveryCheck] | CsvDeliveryCheck | str | None = None,
) -> Command:
    """Make files visible to the user for viewing and rendering in the client interface.

    When to use the present_files tool:

    - Making any file available for the user to view, download, or interact with
    - Presenting multiple related files at once
    - After creating files that should be presented to the user

    When NOT to use the present_files tool:
    - When you only need to read file contents for your own processing
    - For temporary or intermediate files not meant for user viewing

    Notes:
    - You should call this tool after creating files and moving them to the `/mnt/user-data/outputs` directory.
    - This tool can be safely called in parallel with other tools. State updates are handled by a reducer to prevent conflicts.
    - Each CSV must contain one rectangular table with a unique, nonempty header and the same number of columns in every record. Use separate CSV files or JSON for multiple tables.

    Args:
        filepaths: List of absolute file paths to present to the user. **Only** files in `/mnt/user-data/outputs` can be presented.
        delivery_checks: For CSV results specify filepath, required_columns, metric_columns (the requested metrics contained in this file to their actual column names), and decimal_places (computed result columns only, not raw year/ID columns).
            Checks read actual files; missing markers and precision come from current-turn requirements. A single object, array, or JSON string is accepted. Metrics can be split across files; completion checks aggregate verified deliveries. Map labels to existing headers; do not rename columns just to match labels.
    """
    try:
        normalized_paths = [_normalize_presented_filepath(runtime, filepath) for filepath in filepaths]
        summaries, mappings = _check_csv_delivery(runtime, normalized_paths, delivery_checks)
        source = (runtime.state.get("intent_context") or {}).get("_source_message_key")
        outputs = Path(runtime.state["thread_data"]["outputs_path"])
        run_key = (runtime.state.get('output_baseline') or {}).get('run_key')
        records = {path: {"source": source, 'run_key': run_key, "sha256": file_digest(outputs / path.removeprefix(OUTPUT_PREFIX)), "metric_columns": mappings.get(path, {})} for path in normalized_paths}
    except (ValueError, OSError, csv.Error) as exc:
        return Command(
            update={"messages": [ToolMessage(f"Error: {exc}", tool_call_id=tool_call_id, status='error', additional_kwargs={'error_code': 'DELIVERY_INVALID'})]},
        )

    # The merge_artifacts reducer will handle merging and deduplication
    return Command(
        update={
            "artifacts": normalized_paths,
            "verified_deliveries": records,
            "messages": [ToolMessage("Successfully presented files" + ("\nVerified CSV facts:\n" + "\n".join(summaries) if summaries else ""), tool_call_id=tool_call_id)],
        },
    )
