"""Recoverable, checksummed OneCity initialization and explicit baseline.

MySQL DDL commits implicitly. An applying record precedes business-table DDL;
only a matching, validated partial migration may resume after interruption.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path

MIGRATION = "onecity.initial.v1"
LEDGER = "platform_schema_migrations"


def catalogue_models():
    from onecityagent.models.db_models import (
        User, Role, Tenant, UserTenant, UserRole, TenantLlm, McpConfig,
        A2aAgent, Dialog, Conversation, PlanData,
    )
    from onecityagent.models.llm_models import LLMFactories, LLM
    # The legacy TenantLLM class has the same table name as TenantLlm.
    # Choose one definition deliberately instead of relying on sort order.
    return [User, Role, Tenant, UserTenant, UserRole, TenantLlm, LLMFactories,
            LLM, McpConfig, A2aAgent, Dialog, Conversation, PlanData]


def schema_signature(models) -> list[dict]:
    result = []
    for model in models:
        fields = model._meta.sorted_fields
        primary = model._meta.primary_key
        primary_names = (list(primary.field_names) if hasattr(primary, "field_names")
                         else [primary.name] if primary is not False else [])
        unique = {tuple(model._meta.fields[name].column_name for name in columns)
                  for columns, is_unique in model._meta.indexes if is_unique}
        unique.update((field.column_name,) for field in fields if field.unique)
        result.append({
            "table": model._meta.table_name,
            "columns": [{"name": f.column_name, "type": f.field_type,
                         "null": bool(f.null), "length": getattr(f, "max_length", None)}
                        for f in fields],
            "primary": [model._meta.fields[n].column_name for n in primary_names],
            "unique": sorted([list(columns) for columns in unique]),
        })
    return sorted(result, key=lambda record: record["table"])


def migration_checksum(models) -> str:
    raw = {"migration": MIGRATION, "schema": schema_signature(models), "revision": 1}
    return hashlib.sha256(json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _normalized_type(value: str) -> str:
    value = value.upper().split("(", 1)[0].strip()
    return {"INTEGER": "INT", "AUTO": "INT", "BOOL": "TINYINT", "BOOLEAN": "TINYINT",
            "DOUBLE": "REAL", "FLOAT": "REAL"}.get(value, value)


def model_indexes(model) -> dict:
    """Deduplicate inherited field/Meta index declarations without changing DDL.

    The runtime pins Peewee 3.18.2. Its Metadata produces ModelIndex nodes for
    these models, including duplicate names for inherited indexed fields that
    also occur in Meta.indexes. MySQL has no CREATE INDEX IF NOT EXISTS.
    """
    result = {}
    for index in model._meta.fields_to_index():
        name = index._name
        if index._where is not None or any(not hasattr(field, 'column_name') for field in index._expressions):
            raise RuntimeError('Unsupported expression index in OneCity migration')
        signature = (tuple(field.column_name for field in index._expressions), bool(index._unique))
        if name in result and result[name][1] != signature:
            raise RuntimeError('Conflicting OneCity index declarations')
        result.setdefault(name, (index, signature))
    return result


def validate_indexes(database, model, *, allow_missing: bool = False) -> None:
    actual = {index.name: (tuple(index.columns), bool(index.unique))
              for index in database.get_indexes(model._meta.table_name)}
    for name, (_index, wanted) in model_indexes(model).items():
        if name in actual and actual[name] != wanted:
            raise RuntimeError(f'OneCity index definition differs: {model._meta.table_name}.{name}')
        if wanted not in actual.values() and not allow_missing:
            raise RuntimeError(f'OneCity index is missing: {model._meta.table_name}.{name}')


def apply_schema(database, models) -> None:
    """Resume matching initial DDL by creating only missing tables/indexes."""
    tables = set(database.get_tables())
    for model in models:
        table = model._meta.table_name
        if table not in tables:
            # SchemaManager.create_table creates only the table/primary key;
            # Model.create_table would also recreate duplicated index nodes.
            model._schema.create_table(safe=False)
            tables.add(table)
        validate_indexes(database, model, allow_missing=True)
        actual = {index.name: (tuple(index.columns), bool(index.unique))
                  for index in database.get_indexes(table)}
        for name, (index, wanted) in model_indexes(model).items():
            if name in actual or wanted in actual.values():
                continue
            database.execute(model._schema._create_index(index, safe=False))
            actual[name] = wanted


def validate_schema(database, models, *, allow_missing: bool = False,
                    allow_missing_indexes: bool = False) -> None:
    expected = schema_signature(models)
    tables = set(database.get_tables())
    if tables - {item["table"] for item in expected} - {LEDGER}:
        raise RuntimeError("OneCity database contains unsupported tables; use a reviewed migration")
    sqlite = "sqlite" in database.__class__.__name__.lower()
    for item in expected:
        table = item["table"]
        if table not in tables:
            if allow_missing:
                continue
            raise RuntimeError(f"OneCity table is missing: {table}")
        actual = {column.name: column for column in database.get_columns(table)}
        if set(actual) != {column["name"] for column in item["columns"]}:
            raise RuntimeError(f"OneCity columns differ from supported baseline: {table}")
        for expected_column in item["columns"]:
            column = actual[expected_column["name"]]
            expected_type = _normalized_type(expected_column["type"])
            actual_type = _normalized_type(column.data_type)
            if sqlite and expected_type in {"TINYINT", "BIGINT", "SMALLINT", "BIGAUTO"}:
                expected_type = "INT"
            if expected_type != actual_type or bool(column.null) != expected_column["null"]:
                raise RuntimeError(f"OneCity column definition differs: {table}.{column.name}")
        if list(database.get_primary_keys(table)) != item["primary"]:
            raise RuntimeError(f"OneCity primary key differs: {table}")
        unique = {tuple(index.columns) for index in database.get_indexes(table) if index.unique}
        if not allow_missing_indexes and not {tuple(columns) for columns in item["unique"]}.issubset(unique):
            raise RuntimeError(f"OneCity unique indexes differ: {table}")
        if not sqlite:
            rows = database.execute_sql(
                "SELECT COLUMN_NAME,CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s", (table,)
            ).fetchall()
            lengths = dict(rows)
            for column in item["columns"]:
                if column["length"] is not None and lengths.get(column["name"]) != column["length"]:
                    raise RuntimeError(f"OneCity column length differs: {table}.{column['name']}")
    for model in models:
        if model._meta.table_name in tables:
            validate_indexes(database, model, allow_missing=allow_missing_indexes)


@contextmanager
def migration_lock(database):
    sqlite = "sqlite" in database.__class__.__name__.lower()
    if sqlite:
        with database.atomic("IMMEDIATE"):
            yield
        return
    name = "citybrain:" + hashlib.sha256(str(database.database).encode()).hexdigest()[:40]
    acquired = database.execute_sql("SELECT GET_LOCK(%s,30)", (name,)).fetchone()
    if not acquired or acquired[0] != 1:
        raise RuntimeError("OneCity migration is already running")
    try:
        yield
    finally:
        database.execute_sql("SELECT RELEASE_LOCK(%s)", (name,))


def _bootstrap(database) -> str:
    email = os.environ.get("ADMIN_EMAIL", "").strip()
    if not email:
        return "skipped"
    password_file = os.environ.get("ADMIN_PASSWORD_FILE")
    if not password_file:
        raise ValueError("ADMIN_PASSWORD_FILE is required with ADMIN_EMAIL")
    path = Path(password_file)
    if path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o077:
        raise ValueError("Bootstrap password file must be private and regular")
    password = path.read_text().strip()
    if len(password) < 20:
        raise ValueError("Bootstrap admin password must contain at least 20 characters")
    from onecityagent.application.bootstrap import seed_initial_data
    result = seed_initial_data(database, admin_email=email, admin_password=password,
                               admin_nickname=os.environ.get("ADMIN_NICKNAME", "Administrator"))
    return "created" if result["created"] else "preserved"


def _database(database=None):
    from onecityagent.application.database import DB, build_database
    if database is None:
        from onecityagent.application.settings import load_settings_from_env
        database = build_database(load_settings_from_env())
    DB.configure(database)
    return database


def migrate(*, database=None, baseline: bool = False) -> dict:
    database = _database(database)
    models = catalogue_models()
    checksum = migration_checksum(models)
    placeholder = database.param
    with database.connection_context(), migration_lock(database):
        tables = set(database.get_tables())
        if tables and LEDGER not in tables and not baseline:
            raise RuntimeError("Existing unversioned OneCity database; run baseline explicitly after review")
        if baseline:
            validate_schema(database, models)
        row = None
        if LEDGER in tables:
            actual = {column.name for column in database.get_columns(LEDGER)}
            if actual != {"name", "checksum", "status", "applied_at"}:
                raise RuntimeError("Unsupported OneCity migration ledger")
            rows = database.execute_sql(f"SELECT name,checksum,status FROM {LEDGER}").fetchall()
            if any(r[0] != MIGRATION for r in rows):
                raise RuntimeError("OneCity database contains migrations newer than this release")
            row = next((r for r in rows if r[0] == MIGRATION), None)
            if row and row[1] != checksum:
                raise RuntimeError("OneCity migration checksum mismatch")
            if row and row[2] not in {"applying", "ready"}:
                raise RuntimeError("Unsupported OneCity migration state")
            if row and row[2] == "ready":
                validate_schema(database, models)
                return {"migration": MIGRATION, "checksum": checksum, "status": "ready",
                        "bootstrap_admin": _bootstrap(database)}
            if not row and tables - {LEDGER} and not baseline:
                raise RuntimeError("OneCity migration ledger has no matching migration; baseline is required")
        validate_schema(database, models, allow_missing=not baseline,
                        allow_missing_indexes=not baseline)
        database.execute_sql(f"CREATE TABLE IF NOT EXISTS {LEDGER} ("
                             "name VARCHAR(191) NOT NULL PRIMARY KEY,"
                             "checksum VARCHAR(64) NOT NULL,status VARCHAR(16) NOT NULL,"
                             "applied_at VARCHAR(32))")
        if row is None:
            database.execute_sql(f"INSERT INTO {LEDGER}(name,checksum,status) "
                                 f"VALUES({placeholder},{placeholder},{placeholder})",
                                 (MIGRATION, checksum, "applying"))
        if not baseline:
            apply_schema(database, models)
        validate_schema(database, models)
        bootstrap = _bootstrap(database)
        database.execute_sql(f"UPDATE {LEDGER} SET status={placeholder},applied_at={placeholder} "
                             f"WHERE name={placeholder}",
                             ("ready", datetime.now(timezone.utc).isoformat(), MIGRATION))
    return {"migration": MIGRATION, "checksum": checksum, "status": "ready",
            "baseline": baseline, "bootstrap_admin": bootstrap}


def status(*, database=None) -> dict:
    database = _database(database)
    with database.connection_context():
        models = catalogue_models()
        validate_schema(database, models)
        row = database.execute_sql(f"SELECT checksum,status FROM {LEDGER} WHERE name={database.param}",
                                   (MIGRATION,)).fetchone()
        if row != (migration_checksum(models), "ready"):
            raise RuntimeError("OneCity migration is incomplete or its checksum differs")
    return {"migration": MIGRATION, "status": "ready"}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["migrate", "status", "baseline"])
    args = parser.parse_args()
    try:
        result = status() if args.action == "status" else migrate(baseline=args.action == "baseline")
        print(json.dumps(result))
        return 0
    except Exception as error:
        print(json.dumps({"status": "failed", "error_type": type(error).__name__,
                          "error": str(error) if isinstance(error, (RuntimeError, ValueError))
                          else "Database migration failed; inspect restricted database diagnostics"}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
