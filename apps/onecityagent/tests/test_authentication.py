"""Authentication regressions against the real API factory and SQLite DB."""

from __future__ import annotations

from datetime import datetime

from flask_login import current_user, login_required
from peewee import SqliteDatabase
import pytest
from werkzeug.security import check_password_hash, generate_password_hash

from onecityagent.application.api import create_api_app
from onecityagent.models import create_tables
from onecityagent.models.db_models import Role, User
from onecityagent.service.authentication import (
    load_active_user,
    revoke_login_token,
    self_registration_role_id,
    token_from_header,
)
from onecityagent.service.plan_data import PlanDataService
from onecityagent.service.user_service import UserService


@pytest.fixture
def authentication(tmp_path, monkeypatch):
    database = SqliteDatabase(str(tmp_path / "authentication.sqlite"), autoconnect=False)
    app = create_api_app(
        {"SECRET_KEY": "synthetic-unit-test-secret", "ONECITY_SELF_REGISTER_ROLE_CODE": "common"},
        database=database,
    )
    app.config["TESTING"] = True

    with app.app_context():
        create_tables()
        database.connect()
        ordinary_role = Role.create(
            id="ordinary-role", code="common", name="Ordinary", sort=2,
            data_scope=1, data_scope_dept_ids="", status=1, type=1, remark="",
        )
        Role.create(
            id="privileged-role", code="admin", name="Administrator", sort=1,
            data_scope=1, data_scope_dept_ids="", status=1, type=1, remark="",
        )
        original_date = datetime(2026, 11, 26, 12, 0, 0)
        for user_id, token in (("alice", "alice-token"), ("bob", "bob-token")):
            User.create(
                id=user_id,
                email=f"{user_id}@test.invalid",
                nickname="Synthetic account",
                password=generate_password_hash("synthetic-password"),
                access_token=token,
                status="1",
                is_active="1",
                is_authenticated="1",
                is_anonymous="0",
                is_superuser=False,
                role=ordinary_role.id,
                avatar="",
                language="Chinese",
                color_schema="Bright",
                timezone="UTC+8",
                login_channel="password",
                create_date=original_date,
            )

    from onecityagent.controller import user_controller

    # Keep the controller, services, auth callbacks, Peewee models and routes real.
    # Only the external RSA-key dependency and optional default-plan side effect
    # are replaced with deterministic test behavior.
    monkeypatch.setattr(user_controller, "decrypt", lambda value: value)
    monkeypatch.setattr(PlanDataService, "create_default_plans", classmethod(lambda cls, *_args: None))

    updates = []
    real_update = UserService.update_by_id

    def track_update(user_id, values):
        updates.append((user_id, dict(values)))
        return real_update(user_id, values)

    monkeypatch.setattr(UserService, "update_by_id", staticmethod(track_update))

    @app.get("/whoami")
    @login_required
    def whoami():
        return {"user_id": current_user.id}

    yield {
        "app": app,
        "client": app.test_client(),
        "database": database,
        "User": User,
        "Role": Role,
        "UserService": UserService,
        "updates": updates,
        "load_active_user": load_active_user,
        "revoke_login_token": revoke_login_token,
        "self_registration_role_id": self_registration_role_id,
        "token_from_header": token_from_header,
        "original_date": original_date,
    }
    database.close()


@pytest.mark.parametrize(
    "header,expected",
    [
        (None, None), ("", None), ("Bearer ", None), ("Bearer a b", None),
        ("Bearer synthetic", "synthetic"), ("synthetic", "synthetic"),
        ("bearer synthetic", "synthetic"),
    ],
)
def test_token_parsing_rejects_missing_or_ambiguous_values(authentication, header, expected):
    assert authentication["token_from_header"](header) == expected


def test_explicit_account_header_overrides_other_account_cookie(authentication):
    client = authentication["client"]
    with client.session_transaction() as session:
        session["_user_id"] = "alice-token"
        session["_fresh"] = True
    assert client.get("/whoami").json == {"user_id": "alice"}
    assert client.get("/whoami", headers={"Authorization": "Bearer bob-token"}).json == {"user_id": "bob"}
    assert client.get("/whoami", headers={"Authorization": "invalid-token"}).status_code == 401


@pytest.mark.parametrize("column", ["status", "is_active", "is_authenticated"])
def test_account_invalidation_disables_token_and_cookie(authentication, column):
    fixture = authentication
    with fixture["database"].connection_context():
        fixture["User"].update(**{column: "0"}).where(fixture["User"].id == "alice").execute()
    with fixture["client"].session_transaction() as session:
        session["_user_id"] = "alice-token"
        session["_fresh"] = True
    assert fixture["load_active_user"]("alice-token") is None
    assert fixture["client"].get("/whoami").status_code == 401
    assert fixture["client"].get("/whoami", headers={"Authorization": "alice-token"}).status_code == 401


def test_login_rotates_token_and_omits_password(authentication):
    fixture = authentication
    assert fixture["database"].is_closed()
    response = fixture["client"].post(
        "/v1/user/login",
        json={"email": "alice@test.invalid", "password": "synthetic-password"},
    )
    assert response.status_code == 200
    assert fixture["database"].is_closed()
    new_token = response.headers["Authorization"]
    assert new_token != "alice-token"
    assert response.json["data"]["access_token"] == new_token
    assert "password" not in response.json["data"]
    assert fixture["load_active_user"]("alice-token") is None
    assert fixture["load_active_user"](new_token).id == "alice"
    assert fixture["client"].get("/whoami").json == {"user_id": "alice"}
    assert fixture["database"].is_closed()


def test_logout_revokes_current_token(authentication):
    fixture = authentication
    response = fixture["client"].get("/v1/user/logout", headers={"Authorization": "alice-token"})
    assert response.status_code == 200
    assert fixture["load_active_user"]("alice-token") is None
    assert fixture["client"].get("/whoami", headers={"Authorization": "alice-token"}).status_code == 401


def test_old_logout_cannot_revoke_newer_login(authentication):
    fixture = authentication
    with fixture["database"].connection_context():
        fixture["User"].update(access_token="new-login").where(fixture["User"].id == "alice").execute()
    fixture["revoke_login_token"]("alice", "alice-token")
    assert fixture["load_active_user"]("new-login").id == "alice"


def test_registration_ignores_requested_admin_role(authentication):
    fixture = authentication
    response = fixture["client"].post(
        "/v1/user/register",
        json={
            "email": "registered@test.invalid",
            "password": "synthetic-password",
            "nickname": "Test",
            "role": "privileged-role",
        },
    )
    assert response.status_code == 200
    with fixture["database"].connection_context():
        user = fixture["User"].get(fixture["User"].email == "registered@test.invalid")
        assert user.role == "ordinary-role"
        assert check_password_hash(user.password, "synthetic-password")


@pytest.mark.parametrize("role_code", ["admin", "super_admin", "superadmin", "", "missing"])
def test_registration_role_must_resolve_to_enabled_non_admin(authentication, role_code):
    with authentication["app"].app_context():
        authentication["app"].extensions["onecity.settings"]["ONECITY_SELF_REGISTER_ROLE_CODE"] = role_code
        with pytest.raises(ValueError):
            authentication["self_registration_role_id"]()


def test_profile_update_cannot_elevate_or_rebind_authenticated_account(authentication):
    fixture = authentication
    editable = {
        "nickname": "Edited synthetic profile", "avatar": "synthetic-avatar.png",
        "language": "Chinese", "color_schema": "Dark", "timezone": "UTC",
    }
    protected = {
        "id": "bob", "email": "bob@test.invalid", "role": "privileged-role",
        "access_token": "attacker-selected-token", "status": "0", "is_active": "0",
        "is_authenticated": "0", "is_anonymous": "1", "is_superuser": "1",
        "create_date": "attacker-date", "update_date": "attacker-date",
        "tenant_id": "other-tenant", "permissions": ["admin"],
        "new_password": "unverified-password",
    }
    response = fixture["client"].post(
        "/v1/user/setting", headers={"Authorization": "alice-token"},
        json={**editable, **protected},
    )
    assert response.status_code == 200
    assert response.json["code"] == 0
    assert fixture["updates"]
    user_id, written = fixture["updates"][-1]
    assert user_id == "alice"
    assert set(written) == set(editable) | {"update_date"}
    assert written["update_date"] != "attacker-date"
    with fixture["database"].connection_context():
        alice = fixture["User"].get_by_id("alice")
        bob = fixture["User"].get_by_id("bob")
        for key, value in editable.items():
            assert getattr(alice, key) == value
        assert alice.role == "ordinary-role"
        assert alice.access_token == "alice-token"
        assert alice.email == "alice@test.invalid"
        assert alice.status == alice.is_active == alice.is_authenticated == "1"
        assert alice.is_anonymous == "0" and alice.is_superuser is False
        assert alice.create_date == fixture["original_date"]
        assert check_password_hash(alice.password, "synthetic-password")
        assert bob.nickname == "Synthetic account"
    assert fixture["client"].get("/whoami", headers={"Authorization": "attacker-selected-token"}).status_code == 401


def test_profile_password_change_requires_current_password(authentication):
    fixture = authentication
    response = fixture["client"].post(
        "/v1/user/setting", headers={"Authorization": "alice-token"},
        json={"password": "wrong-password", "new_password": "replacement-password"},
    )
    assert response.status_code == 401
    assert fixture["updates"] == []
    response = fixture["client"].post(
        "/v1/user/setting", headers={"Authorization": "alice-token"},
        json={"password": "synthetic-password", "new_password": "replacement-password"},
    )
    assert response.status_code == 200
    with fixture["database"].connection_context():
        alice = fixture["User"].get_by_id("alice")
        assert check_password_hash(alice.password, "replacement-password")
        assert not check_password_hash(alice.password, "synthetic-password")


def test_profile_update_requires_authenticated_account(authentication):
    response = authentication["client"].post("/v1/user/setting", json={"nickname": "Unauthenticated edit"})
    assert response.status_code == 401
    assert authentication["updates"] == []
