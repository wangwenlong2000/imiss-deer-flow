"""Utilities for optional local JSONL run-event logging."""

from __future__ import annotations

import json
import logging
import os
import re
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from deerflow.config.paths import get_paths

logger = logging.getLogger(__name__)

_TRUTHY_VALUES = {"1", "true", "yes", "on"}
_SAFE_FILENAME_RE = re.compile(r"[^A-Za-z0-9_.-]+")


def is_run_event_logging_enabled() -> bool:
    """Return whether local JSONL run-event logging is enabled."""
    value = os.getenv("DEERFLOW_RUN_EVENT_LOG_ENABLED", "")
    return value.strip().lower() in _TRUTHY_VALUES


def get_run_event_log_dir() -> Path:
    """Resolve the directory used for local JSONL run-event logs."""
    configured = os.getenv("DEERFLOW_RUN_EVENT_LOG_DIR")
    if configured and configured.strip():
        return Path(configured).expanduser().resolve()
    return get_paths().base_dir / "run_events"


def _sanitize_thread_id(thread_id: str) -> str:
    sanitized = _SAFE_FILENAME_RE.sub("_", thread_id).strip("._")
    return sanitized or "unknown-thread"


def to_jsonable(value: Any, *, _depth: int = 0) -> Any:
    """Best-effort conversion of runtime data into JSON-serializable values."""
    if _depth >= 6:
        return str(value)

    if value is None or isinstance(value, str | int | float | bool):
        return value

    if isinstance(value, Path):
        return str(value)

    if isinstance(value, Mapping):
        return {str(key): to_jsonable(item, _depth=_depth + 1) for key, item in value.items()}

    if isinstance(value, list | tuple | set):
        return [to_jsonable(item, _depth=_depth + 1) for item in value]

    if hasattr(value, "model_dump") and callable(value.model_dump):
        try:
            return to_jsonable(value.model_dump(), _depth=_depth + 1)
        except Exception:
            pass

    if hasattr(value, "dict") and callable(value.dict):
        try:
            return to_jsonable(value.dict(), _depth=_depth + 1)
        except Exception:
            pass

    if hasattr(value, "type") and hasattr(value, "content"):
        message_payload: dict[str, Any] = {
            "type": getattr(value, "type", None),
            "content": to_jsonable(getattr(value, "content", None), _depth=_depth + 1),
        }
        for key in ("name", "id", "tool_calls", "additional_kwargs", "response_metadata", "usage_metadata"):
            if hasattr(value, key):
                message_payload[key] = to_jsonable(getattr(value, key), _depth=_depth + 1)
        return message_payload

    if hasattr(value, "__dict__"):
        try:
            public_items = {
                key: item
                for key, item in vars(value).items()
                if not key.startswith("_") and not callable(item)
            }
            if public_items:
                return to_jsonable(public_items, _depth=_depth + 1)
        except Exception:
            pass

    return str(value)


def write_run_event_log(
    thread_id: str,
    event: str,
    payload: Mapping[str, Any] | None = None,
    *,
    source: str,
) -> Path | None:
    """Append a thread-scoped JSONL monitoring record.

    Failures are logged but never propagated, so monitoring cannot break the
    main request path.
    """
    if not is_run_event_logging_enabled() or not thread_id:
        return None

    try:
        log_dir = get_run_event_log_dir()
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"{_sanitize_thread_id(thread_id)}.jsonl"
        record = {
            "timestamp": datetime.now(UTC).isoformat(),
            "thread_id": thread_id,
            "source": source,
            "event": event,
            "payload": to_jsonable(payload or {}),
        }
        with log_path.open("a", encoding="utf-8") as file_obj:
            file_obj.write(json.dumps(record, ensure_ascii=False) + "\n")
        return log_path
    except Exception as exc:
        logger.warning("Failed to write run-event log for thread %s: %s", thread_id, exc)
        return None