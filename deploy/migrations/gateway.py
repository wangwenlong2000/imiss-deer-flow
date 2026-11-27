"""Checksummed Gateway SQLite migrations with explicit, validated baselines."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3

MIGRATION = "gateway.initial.v1"
STATEMENTS = (
    "CREATE TABLE mobile_conversations (org_id TEXT NOT NULL,user_id TEXT NOT NULL,"
    "conversation_id TEXT NOT NULL,thread_id TEXT NOT NULL UNIQUE,"
    "PRIMARY KEY(org_id,user_id,conversation_id))",
    "CREATE TABLE data_sources (id TEXT PRIMARY KEY, record TEXT NOT NULL, registration_key TEXT UNIQUE)",
    "CREATE TABLE data_source_migrations (name TEXT PRIMARY KEY)",
)
CHECKSUM = hashlib.sha256("\n".join(STATEMENTS).encode()).hexdigest()
LEDGER = "platform_schema_migrations"
EXPECTED_COLUMNS = {
    "mobile_conversations": [
        ("org_id", "TEXT", 1, 1), ("user_id", "TEXT", 1, 2),
        ("conversation_id", "TEXT", 1, 3), ("thread_id", "TEXT", 1, 0),
    ],
    "data_sources": [("id", "TEXT", 0, 1), ("record", "TEXT", 1, 0),
                     ("registration_key", "TEXT", 0, 0)],
    "data_source_migrations": [("name", "TEXT", 0, 1)],
}
EXPECTED_UNIQUES = {
    "mobile_conversations": {("org_id", "user_id", "conversation_id"), ("thread_id",)},
    "data_sources": {("id",), ("registration_key",)},
    "data_source_migrations": {("name",)},
}


def _tables(connection: sqlite3.Connection) -> set[str]:
    return {row[0] for row in connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )}


def validate_schema(connection: sqlite3.Connection) -> None:
    """Reject missing columns, changed ownership keys, or lost unique indexes."""
    if _tables(connection) - set(EXPECTED_COLUMNS) - {LEDGER}:
        raise RuntimeError("Gateway database contains unsupported tables; use a reviewed migration")
    for table, expected in EXPECTED_COLUMNS.items():
        actual = [(r[1], r[2].upper(), r[3], r[5]) for r in connection.execute(
            f'PRAGMA table_info("{table}")'
        )]
        if actual != expected:
            raise RuntimeError(f"Gateway schema differs from supported baseline: {table}")
        uniques = set()
        for index in connection.execute(f'PRAGMA index_list("{table}")'):
            if index[2]:
                name = index[1].replace('"', '""')
                uniques.add(tuple(row[2] for row in connection.execute(
                    f'PRAGMA index_info("{name}")'
                )))
        if not EXPECTED_UNIQUES[table].issubset(uniques):
            raise RuntimeError(f"Gateway uniqueness constraints differ: {table}")


def migrate(*, baseline: bool = False, home: Path | None = None) -> dict:
    home = Path(home or os.environ.get("DEER_FLOW_HOME", "/data/deerflow"))
    home.mkdir(parents=True, exist_ok=True)
    database = home / "gateway.sqlite3"
    connection = sqlite3.connect(database, timeout=30)
    try:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("BEGIN IMMEDIATE")
        existing = _tables(connection)
        if existing and LEDGER not in existing and not baseline:
            raise RuntimeError("Existing unversioned Gateway database; run baseline explicitly after review")
        if baseline:
            validate_schema(connection)
        if LEDGER in existing:
            columns = [(r[1], r[2].upper()) for r in connection.execute(
                f'PRAGMA table_info("{LEDGER}")'
            )]
            if columns != [("name", "TEXT"), ("checksum", "TEXT")]:
                raise RuntimeError("Unsupported Gateway migration ledger")
            rows = dict(connection.execute(f"SELECT name,checksum FROM {LEDGER}"))
            if set(rows) - {MIGRATION}:
                raise RuntimeError("Gateway database contains migrations newer than this release")
            if MIGRATION in rows:
                if rows[MIGRATION] != CHECKSUM:
                    if not (baseline and rows[MIGRATION] == "builtin-v1"):
                        raise RuntimeError("Gateway migration checksum mismatch")
                validate_schema(connection)
                if baseline and rows[MIGRATION] != CHECKSUM:
                    connection.execute(f"UPDATE {LEDGER} SET checksum=? WHERE name=?", (CHECKSUM, MIGRATION))
                connection.commit()
                return {"migration": MIGRATION, "checksum": CHECKSUM,
                        "database": str(database), "status": "ready"}
            if existing - {LEDGER} and not baseline:
                raise RuntimeError("Gateway migration ledger has no matching migration; baseline is required")
        connection.execute(f"CREATE TABLE IF NOT EXISTS {LEDGER} (name TEXT PRIMARY KEY, checksum TEXT NOT NULL)")
        if not baseline:
            for statement in STATEMENTS:
                connection.execute(statement)
        validate_schema(connection)
        connection.execute(f"INSERT INTO {LEDGER}(name,checksum) VALUES(?,?)", (MIGRATION, CHECKSUM))
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return {"migration": MIGRATION, "checksum": CHECKSUM, "database": str(database),
            "status": "ready", "baseline": baseline}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", nargs="?", choices=["migrate", "baseline"], default="migrate")
    args = parser.parse_args()
    print(json.dumps(migrate(baseline=args.action == "baseline")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
