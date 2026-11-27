"""Service contexts must not close a caller's request or transaction connection."""

import pytest
from peewee import SqliteDatabase

from onecityagent.application.database import DB


@pytest.fixture
def database(tmp_path):
    database = SqliteDatabase(str(tmp_path / "connections.sqlite"), autoconnect=False)
    DB.configure(database)
    yield database
    if not database.is_closed():
        database.close()


def test_nested_context_closes_only_the_connection_it_opens(database):
    assert database.is_closed()
    with DB.connection_context():
        connection = database.connection()
        with DB.connection_context():
            assert database.connection() is connection
        assert not database.is_closed()
        assert database.connection() is connection
        assert database.execute_sql("SELECT 1").fetchone() == (1,)
    assert database.is_closed()


def test_context_preserves_borrowed_connection_after_error(database):
    database.connect()
    connection = database.connection()
    with pytest.raises(ValueError, match="synthetic error"):
        with DB.connection_context():
            raise ValueError("synthetic error")
    assert not database.is_closed()
    assert database.connection() is connection


def test_owned_context_closes_connection_after_error(database):
    with pytest.raises(ValueError, match="synthetic error"):
        with DB.connection_context():
            raise ValueError("synthetic error")
    assert database.is_closed()


def test_service_context_preserves_outer_transaction_and_rollback(database):
    with DB.connection_context():
        database.execute_sql("CREATE TABLE probe (value INTEGER)")
        with pytest.raises(ValueError, match="rollback"):
            with database.atomic():
                with DB.connection_context():
                    database.execute_sql("INSERT INTO probe VALUES (1)")
                assert database.in_transaction()
                raise ValueError("rollback")
        assert database.execute_sql("SELECT COUNT(*) FROM probe").fetchone() == (0,)


def test_context_remains_usable_as_service_decorator(database):
    @DB.connection_context()
    def query():
        return database.execute_sql("SELECT 1").fetchone()

    assert query() == (1,)
    assert database.is_closed()
    assert query() == (1,)
    assert database.is_closed()
