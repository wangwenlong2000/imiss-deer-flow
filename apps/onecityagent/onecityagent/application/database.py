"""Database injection for OneCity models and request handlers."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any

from peewee import DatabaseProxy


database_proxy = DatabaseProxy()

REQUIRED_CATALOGUE_TABLES = frozenset(
    {"user", "role", "tenant", "user_tenant", "dialog", "conversation"}
)


def database_readiness(database: Any) -> tuple[bool, list[str]]:
    """Check connectivity and the minimum schema required to serve requests."""
    try:
        database.execute_sql("SELECT 1").fetchone()
        missing = sorted(REQUIRED_CATALOGUE_TABLES - set(database.get_tables()))
    except Exception:
        return False, []
    return not missing, missing


class DB:
    """Compatibility facade over the explicitly injected Peewee database."""

    @classmethod
    def configure(cls, database: Any) -> None:
        if database is None:
            raise RuntimeError("A OneCity database must be injected; SQLite is never implicit")
        database_proxy.initialize(database)

    @classmethod
    def get_instance(cls):
        if database_proxy.obj is None:
            raise RuntimeError("OneCity database is not configured; use an application factory")
        return database_proxy.obj

    @classmethod
    @contextmanager
    def connection_context(cls):
        """Borrow request/transaction connections without closing their owner."""
        database = cls.get_instance()
        owns_connection = database.is_closed()
        if owns_connection:
            database.connect()
        try:
            yield database
        finally:
            if owns_connection and not database.is_closed():
                database.close()

    @classmethod
    def close_connections(cls) -> None:
        if database_proxy.obj is not None:
            database_proxy.obj.close_all()

    @classmethod
    def init_app_teardown(cls, app) -> None:
        @app.teardown_appcontext
        def close_db(_error):
            database = app.extensions.get("onecity.database")
            if database is not None and not database.is_closed():
                database.close()


def build_database(settings: dict[str, Any], injected=None):
    if injected is not None:
        return injected
    config = settings.get("DATABASE")
    if not isinstance(config, dict):
        raise RuntimeError("DATABASE settings or an explicit database dependency are required")
    kind = str(config.get("type", "")).lower()
    if kind == "mysql":
        from playhouse.pool import PooledMySQLDatabase

        return PooledMySQLDatabase(
            config["name"],
            user=config["user"],
            password=config["password"],
            host=config["host"],
            port=int(config["port"]),
            max_connections=int(config.get("max_connections", 100)),
            stale_timeout=int(config.get("timeout", 300)),
            charset="utf8mb4",
            autocommit=True,
            autoconnect=False,
        )
    if kind == "postgres":
        from playhouse.pool import PooledPostgresqlDatabase

        return PooledPostgresqlDatabase(
            config["name"],
            user=config["user"],
            password=config["password"],
            host=config["host"],
            port=int(config["port"]),
            max_connections=int(config.get("max_connections", 100)),
            stale_timeout=int(config.get("timeout", 300)),
            autorollback=True,
            autoconnect=False,
        )
    if kind == "sqlite":
        filename = config.get("name") or config.get("path")
        if not filename:
            raise ValueError("An explicit SQLite path is required")
        if filename != ":memory:":
            Path(filename).parent.mkdir(parents=True, exist_ok=True)
        from peewee import SqliteDatabase

        return SqliteDatabase(filename, pragmas={"journal_mode": "wal", "foreign_keys": 1})
    raise ValueError("DATABASE.type must be mysql, postgres, or explicitly configured sqlite")
