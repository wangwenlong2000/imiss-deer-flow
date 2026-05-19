from __future__ import annotations

import os
import shutil
import time
from contextlib import suppress
from pathlib import Path
from typing import Any

try:
    import duckdb
except ImportError as exc:
    raise ImportError("Missing dependency 'duckdb'. Install it first: pip install duckdb") from exc


def _is_lock_conflict_error(exc: Exception) -> bool:
    message = str(exc)
    return (
        "Conflicting lock is held" in message
        or "Could not set lock on file" in message
        or "another program is using this file" in message.lower()
        or "另一个程序正在使用此文件" in message
    )


def connect_cached_db(db_path: Path, *, max_attempts: int = 5) -> tuple[duckdb.DuckDBPyConnection, Path | None]:
    """Open a cached DuckDB database with lock-aware retries.

    Prefer read-only access for cache hits. If another process briefly holds a
    write lock, retry a few times and finally fall back to a per-process copy.
    """
    last_exc: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            return duckdb.connect(str(db_path), read_only=True), None
        except Exception as exc:
            if not _is_lock_conflict_error(exc):
                raise
            last_exc = exc
            if attempt < max_attempts:
                time.sleep(min(0.5 * attempt, 2.0))

    # Final fallback: open a read-only copy to avoid cross-process lock
    # contention while preserving cached contents.
    copy_path = db_path.with_name(f"{db_path.stem}.{os.getpid()}.readonly.duckdb")
    shutil.copy2(db_path, copy_path)
    try:
        return duckdb.connect(str(copy_path), read_only=True), copy_path
    except Exception:
        with suppress(Exception):
            copy_path.unlink()
        if last_exc is not None:
            raise last_exc
        raise


def connect_build_db(
    db_path: Path,
    tables_path: Path,
    mappings_path: Path,
    *,
    max_attempts: int = 5,
) -> tuple[duckdb.DuckDBPyConnection, Path | None, bool]:
    """Open a writable cache DB for build, or attach to a cache built by another process.

    Returns `(connection, cleanup_copy, cache_ready)`. When `cache_ready` is True,
    the sidecar metadata files already exist and the caller should treat the cache as
    fully built instead of rebuilding sources.
    """
    last_exc: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            return duckdb.connect(str(db_path)), None, False
        except Exception as exc:
            if not _is_lock_conflict_error(exc):
                raise
            last_exc = exc
            if db_path.exists() and tables_path.exists() and mappings_path.exists():
                con, cleanup = connect_cached_db(db_path, max_attempts=max_attempts)
                return con, cleanup, True
            if attempt < max_attempts:
                time.sleep(min(0.5 * attempt, 2.0))

    if db_path.exists() and tables_path.exists() and mappings_path.exists():
        con, cleanup = connect_cached_db(db_path, max_attempts=max_attempts)
        return con, cleanup, True

    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"Failed to open writable cache database: {db_path}")
