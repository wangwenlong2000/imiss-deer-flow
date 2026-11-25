"""Transactional local data-source catalogue shared by Gateway workers.

JSON is read once for migration only. Ownership on legacy records is deliberately
unassigned; enabling IAM must never adopt existing files for the next caller.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class DataSourceStore:
    def __init__(self, database: Path, legacy_registry: Path | None = None) -> None:
        self.database = database
        self.database.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS data_sources "
                "(id TEXT PRIMARY KEY, record TEXT NOT NULL, registration_key TEXT UNIQUE)"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS data_source_migrations (name TEXT PRIMARY KEY)"
            )
        if legacy_registry is not None:
            self._import_legacy(legacy_registry)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.database, timeout=15)
        try:
            connection.execute("PRAGMA busy_timeout=15000")
            with connection:
                yield connection
        finally:
            connection.close()

    def _import_legacy(self, path: Path) -> None:
        marker = f"legacy-json:{path.resolve()}"
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if connection.execute("SELECT 1 FROM data_source_migrations WHERE name = ?", (marker,)).fetchone():
                return
            # Missing files are marked too: later JSON writes cannot silently
            # re-import deleted or attacker-injected catalogue entries.
            if path.exists():
                raw = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(raw, list):
                    raise ValueError("Legacy data-source registry must be a list")
                for item in raw:
                    if not isinstance(item, Mapping) or not isinstance(item.get("id"), str):
                        logger.warning("Skipping malformed legacy data-source record")
                        continue
                    record = dict(item)
                    record["owner_user_id"] = None
                    record["owner_org_id"] = None
                    registration_key = None
                    if record.get("thread_id") and isinstance(record.get("path"), str):
                        registration_key = str(Path(record["path"]).absolute())
                    connection.execute(
                        "INSERT OR IGNORE INTO data_sources (id, record, registration_key) VALUES (?, ?, ?)",
                        (record["id"], json.dumps(record, ensure_ascii=False), registration_key),
                    )
            connection.execute("INSERT INTO data_source_migrations (name) VALUES (?)", (marker,))

    def list(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            return [json.loads(row[0]) for row in connection.execute("SELECT record FROM data_sources ORDER BY rowid")]

    def get(self, source_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute("SELECT record FROM data_sources WHERE id = ?", (source_id,)).fetchone()
            return json.loads(row[0]) if row else None

    def insert_many(self, records: Sequence[Mapping[str, Any]]) -> None:
        with self._connect() as connection:
            connection.executemany(
                "INSERT INTO data_sources (id, record) VALUES (?, ?)",
                [(record["id"], json.dumps(dict(record), ensure_ascii=False)) for record in records],
            )

    def register_file(self, record: Mapping[str, Any], registration_key: str) -> dict[str, Any]:
        """Atomically deduplicate registration of the same already-owned file."""
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "INSERT INTO data_sources (id, record, registration_key) VALUES (?, ?, ?) "
                "ON CONFLICT(registration_key) DO NOTHING",
                (record["id"], json.dumps(dict(record), ensure_ascii=False), registration_key),
            )
            row = connection.execute(
                "SELECT record FROM data_sources WHERE registration_key = ?", (registration_key,)
            ).fetchone()
            return json.loads(row[0])

    @contextmanager
    def deleting(self, source_id: str) -> Iterator[dict[str, Any] | None]:
        """Hold the row's transaction while the caller stages file removal.

        The caller must restore its staged file if the context raises, including
        a failure to commit. No filesystem operation occurs after row deletion
        until the transaction has committed.
        """
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT record FROM data_sources WHERE id = ?", (source_id,)).fetchone()
            yield json.loads(row[0]) if row else None
            connection.execute("DELETE FROM data_sources WHERE id = ?", (source_id,))
