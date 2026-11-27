"""Smoke tests for the real OneCity Flask factories and SQLite injection."""

from __future__ import annotations

import pytest
from peewee import SqliteDatabase

from onecityagent.application.api import create_api_app
from onecityagent.application.coordinator import create_coordinator_app
from onecityagent.models import create_tables


@pytest.mark.parametrize("factory", [create_api_app, create_coordinator_app])
def test_factory_uses_explicit_sqlite_and_reports_schema_readiness(factory, tmp_path):
    database = SqliteDatabase(str(tmp_path / f"{factory.__name__}.sqlite"))
    settings = {
        "SECRET_KEY": "factory-smoke-test-secret",
        "DATABASE": {"type": "sqlite", "name": str(tmp_path / "unused.sqlite")},
    }
    app = factory(settings, database=database)
    client = app.test_client()

    missing_schema = client.get("/healthz")
    assert missing_schema.status_code == 503
    assert missing_schema.json["status"] == "not_ready"
    assert "user" in missing_schema.json["missing_tables"]

    with app.app_context():
        create_tables()

    ready = client.get("/healthz")
    assert ready.status_code == 200
    assert ready.json == {"status": "ok"}
    assert app.extensions["onecity.database"] is database


def test_api_factory_requires_explicit_settings_and_secret():
    with pytest.raises(RuntimeError, match="requires explicit settings"):
        create_api_app()
    with pytest.raises(RuntimeError, match="SECRET_KEY is required"):
        create_api_app({"DATABASE": {"type": "sqlite", "name": ":memory:"}})


def test_initial_admin_seed_is_idempotent_on_real_sqlite_catalogue(tmp_path):
    database = SqliteDatabase(str(tmp_path / "seed.sqlite"))
    app = create_api_app(
        {"SECRET_KEY": "factory-smoke-test-secret"},
        database=database,
    )
    with app.app_context():
        create_tables()
        from onecityagent.application.bootstrap import seed_initial_data
        from onecityagent.models.db_models import Role, Tenant, User, UserTenant

        first = seed_initial_data(
            admin_email="admin@example.test", admin_password="synthetic-password"
        )
        second = seed_initial_data(
            admin_email="admin@example.test", admin_password="another-password"
        )

        assert first["created"] is True
        assert second["created"] is False
        assert User.select().count() == 1
        assert Role.select().count() == 2
        assert Tenant.select().count() == 1
        assert UserTenant.select().count() == 1


def test_coordinator_connects_each_request_when_autoconnect_is_disabled(tmp_path):
    from deerflow_fixture import add_user
    from onecityagent.models.db_models import Dialog, Role, Tenant, User, UserTenant

    database = SqliteDatabase(str(tmp_path / "explicit-connections.sqlite"), autoconnect=False)
    app = create_coordinator_app({"SECRET_KEY": "factory-smoke-test-secret"}, database=database)
    with database.connection_context():
        create_tables()
    add_user({"database": database, "Role": Role, "Tenant": Tenant,
              "User": User, "UserTenant": UserTenant}, "alice", "org-a", "synthetic-alice-token")
    client = app.test_client()
    assert database.is_closed()

    ready = client.get("/healthz")
    assert ready.status_code == 200
    assert ready.json == {"status": "ok"}
    assert database.is_closed()

    identity = client.get("/query/deerflow/identity", headers={"Authorization": "synthetic-alice-token"})
    assert identity.status_code == 200
    assert identity.json["data"]["user_id"] == "alice"
    assert identity.json["data"]["org_id"] == "org-a"
    assert database.is_closed()

    dialog = client.post("/query/deerflow/dialog", headers={"Authorization": "synthetic-alice-token"},
                         json={"conversation_id": "citybrain-explicit", "name": "Synthetic dialog"})
    assert dialog.status_code == 200
    assert dialog.json["data"]["dialog_id"] == "citybrain-explicit"
    assert database.is_closed()
    with database.connection_context():
        persisted = Dialog.get_by_id("citybrain-explicit")
        assert persisted.creator_id == "alice"
        assert persisted.tenant_id == "org-a"


@pytest.mark.parametrize("path", ["/healthz", "/query/deerflow/identity"])
def test_coordinator_database_connection_failure_is_sanitized_503(tmp_path, monkeypatch, path):
    database = SqliteDatabase(str(tmp_path / "failed-connection.sqlite"), autoconnect=False)
    app = create_coordinator_app({"SECRET_KEY": "factory-smoke-test-secret"}, database=database)

    def unavailable(**kwargs):
        raise RuntimeError("synthetic-private-database-password")

    monkeypatch.setattr(database, "connect", unavailable)
    response = app.test_client().get(path)
    assert response.status_code == 503
    assert "synthetic-private-database-password" not in response.get_data(as_text=True)
    assert database.is_closed()
