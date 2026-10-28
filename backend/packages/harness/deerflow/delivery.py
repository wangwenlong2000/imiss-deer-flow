"""Versioned evidence for files actually validated and presented this turn."""

import hashlib
from pathlib import Path

OUTPUT_PREFIX = "/mnt/user-data/outputs/"


def output_versions(state: dict) -> dict[str, list[int]]:
    """Cheap turn-start inventory; do not hash large outputs before execution."""
    outputs = (state.get("thread_data") or {}).get("outputs_path")
    if not outputs:
        return {}
    base = Path(outputs).resolve()
    versions = {}
    for path in base.rglob("*"):
        try:
            if path.is_file() and path.resolve().is_relative_to(base):
                stat = path.stat()
                versions[OUTPUT_PREFIX + path.relative_to(base).as_posix()] = [stat.st_mtime_ns, stat.st_size]
        except OSError:
            continue
    return versions


def file_digest(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def current_deliveries(state: dict) -> dict[str, dict]:
    source = (state.get("intent_context") or {}).get("_source_message_key")
    outputs = (state.get("thread_data") or {}).get("outputs_path")
    if not source or not outputs:
        return {}
    base = Path(outputs).resolve()
    records = {}
    run_key = (state.get("output_baseline") or {}).get("run_key")
    for virtual, record in (state.get("verified_deliveries") or {}).items():
        if record.get("source") != source or virtual not in (state.get("artifacts") or []) or not virtual.startswith(OUTPUT_PREFIX):
            continue
        if run_key and record.get("run_key") != run_key:
            continue
        path = (base / virtual.removeprefix(OUTPUT_PREFIX)).resolve()
        try:
            if path.is_relative_to(base) and path.is_file() and file_digest(path) == record.get("sha256"):
                records[virtual] = record
        except OSError:
            continue
    return records
