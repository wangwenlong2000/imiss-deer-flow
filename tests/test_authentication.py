"""Real Flask login/token routes with an isolated SQLite account catalogue.

No production app import, credentials, models or database connections are used.
Source sections below are the actual platform auth/controller implementations.
"""

import ast
from datetime import datetime
from enum import IntEnum
import logging
import os
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from flask import Blueprint, Flask, g, jsonify, request
from flask_login import AnonymousUserMixin, LoginManager, UserMixin, current_user, login_required, login_user, logout_user
from peewee import CharField, Model, SqliteDatabase
import pytest
from werkzeug.security import check_password_hash, generate_password_hash


ROOT = Path(__file__).resolve().parents[1]


def _functions(relative_path, names, namespace):
    source = (ROOT / relative_path).read_text()
    tree = ast.parse(source)
    selected = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names]
    assert {node.name for node in selected} == set(names)
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(ROOT / relative_path), "exec"), namespace)


@pytest.fixture
def authentication(tmp_path, monkeypatch):
    monkeypatch.delenv("ONECITY_SELF_REGISTER_ROLE_CODE", raising=False)
    database = SqliteDatabase(str(tmp_path / "authentication.sqlite"))

    class Base(Model):
        class Meta:
            database = None

    Base._meta.database = database

    class User(Base, UserMixin):
        id = CharField(primary_key=True)
        email = CharField(unique=True)
        nickname = CharField(default="Synthetic account")
        password = CharField()
        access_token = CharField(null=True)
        status = CharField(default="1")
        is_active = CharField(default="1")
        is_authenticated = CharField(default="1")
        is_anonymous = CharField(default="0")
        is_superuser = CharField(default="0")
        role = CharField(default="common")
        avatar = CharField(default="")
        language = CharField(default="Chinese")
        color_schema = CharField(default="Bright")
        timezone = CharField(default="UTC+8")
        create_date = CharField(default="synthetic-original-date")
        update_date = CharField(null=True)

        def get_id(self):
            return self.access_token

        def to_json(self):
            return dict(self.__data__)

    class Role(Base):
        id = CharField(primary_key=True)
        code = CharField()
        status = CharField(default="1")
        name = CharField(default="Synthetic role")

    class Tenant(Base):
        id = CharField(primary_key=True)
        name = CharField()

    class UserTenant(Base):
        id = CharField(primary_key=True)
        user_id = CharField()
        tenant_id = CharField()

    class StatusEnum(IntEnum):
        VALID = 1

    class RetCode(IntEnum):
        SUCCESS = 0
        AUTHENTICATION_ERROR = 401
        SERVER_ERROR = 500
        PARAMS_ERROR = 400

    database.create_tables([User, Role, Tenant, UserTenant])
    User.create(id="alice", email="alice@test.invalid", password=generate_password_hash("synthetic-password"), access_token="alice-token")
    User.create(id="bob", email="bob@test.invalid", password=generate_password_hash("synthetic-password"), access_token="bob-token")
    Role.create(id="ordinary-role", code="common")
    Role.create(id="privileged-role", code="admin")
    database.close()

    namespace = {"DB": database, "User": User, "Role": Role, "StatusEnum": StatusEnum, "os": os}
    _functions("onecityagent/service/authentication.py", ["token_from_header", "load_active_user", "revoke_login_token", "self_registration_role_id"], namespace)

    class UserService:
        updates = []

        @staticmethod
        def query(email):
            with database.connection_context():
                return list(User.select().where((User.email == email) & (User.status == "1")))

        @staticmethod
        def query_user(email, password):
            users = UserService.query(email)
            return users[0] if users and check_password_hash(users[0].password, password) else None

        @staticmethod
        def get_user_role(role_id):
            with database.connection_context():
                return Role.get_or_none((Role.id == role_id) & (Role.status == "1"))

        @staticmethod
        def save(**values):
            allowed = {key: value for key, value in values.items() if key in User._meta.fields}
            with database.connection_context():
                User.create(**allowed)
            return True

        @staticmethod
        def delete(user_id):
            with database.connection_context():
                User.delete_by_id(user_id)

        @staticmethod
        def update_by_id(user_id, values):
            UserService.updates.append((user_id, dict(values)))
            with database.connection_context():
                return User.update(**values).where(User.id == user_id).execute()

    def save_model(model, **values):
        with database.connection_context():
            model.create(**{key: value for key, value in values.items() if key in model._meta.fields})
        return True

    def get_json_result(data=None, code=0, message="success"):
        response = jsonify({"data": data, "code": int(code), "message": message})
        response.status_code = 200 if int(code) == 0 else int(code)
        return response

    def construct_response(data=None, auth=None, message="success", code=0):
        response = get_json_result(data, code, message)
        if auth:
            response.headers["Authorization"] = auth
        return response

    app = Flask("isolated_authentication")
    app.secret_key = "synthetic-unit-test-secret"
    app.config["TESTING"] = True
    manager = LoginManager(app)
    namespace.update({
        "app": app, "login_manager": manager, "request": request, "g": g,
        "AnonymousUserMixin": AnonymousUserMixin, "login_required": login_required,
        "current_user": current_user, "login_user": login_user, "logout_user": logout_user,
        "user_bp": Blueprint("user", __name__), "UserService": UserService,
        "TenantService": SimpleNamespace(save=lambda **values: save_model(Tenant, **values)),
        "UserTenantService": SimpleNamespace(save=lambda **values: save_model(UserTenant, **values)),
        "PlanDataService": SimpleNamespace(create_default_plans=lambda *_args: None),
        "UserTenantRole": SimpleNamespace(OWNER=SimpleNamespace(value=1)),
        "RetCode": RetCode, "datetime": datetime, "logger": logging.getLogger(__name__),
        "datetime_format": lambda value: value.isoformat(), "get_uuid": lambda: str(uuid4()),
        "decrypt": lambda value: value, "generate_password_hash": generate_password_hash,
        "check_password_hash": check_password_hash, "logging": logging,
        "get_json_result": get_json_result, "construct_response": construct_response,
    })
    _functions("app.py", ["load_user", "load_user_from_request", "bind_explicit_authorization"], namespace)
    _functions("onecityagent/controller/user_controller.py", ["login", "logout", "register", "setting"], namespace)
    app.register_blueprint(namespace["user_bp"], url_prefix="/v1/user")

    @app.get("/whoami")
    @login_required
    def whoami():
        return jsonify({"user_id": current_user.id})

    yield {**namespace, "database": database, "client": app.test_client()}
    database.close()


@pytest.mark.parametrize("header,expected", [(None, None), ("", None), ("Bearer ", None), ("Bearer a b", None), ("Bearer synthetic", "synthetic"), ("synthetic", "synthetic"), ("bearer synthetic", "synthetic")])
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
    response = fixture["client"].post("/v1/user/login", json={"email": "alice@test.invalid", "password": "synthetic-password"})
    assert response.status_code == 200
    new_token = response.headers["Authorization"]
    assert new_token != "alice-token"
    assert response.json["data"]["access_token"] == new_token
    assert "password" not in response.json["data"]
    assert fixture["load_active_user"]("alice-token") is None
    assert fixture["load_active_user"](new_token).id == "alice"
    assert fixture["client"].get("/whoami").json == {"user_id": "alice"}


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
    response = fixture["client"].post("/v1/user/register", json={"email": "registered@test.invalid", "password": "synthetic-password", "nickname": "Test", "role": "privileged-role"})
    assert response.status_code == 200
    with fixture["database"].connection_context():
        user = fixture["User"].get(fixture["User"].email == "registered@test.invalid")
        assert user.role == "ordinary-role"
        assert check_password_hash(user.password, "synthetic-password")


@pytest.mark.parametrize("role_code", ["admin", "super_admin", "superadmin", "", "missing"])
def test_registration_role_must_resolve_to_enabled_non_admin(authentication, monkeypatch, role_code):
    monkeypatch.setenv("ONECITY_SELF_REGISTER_ROLE_CODE", role_code)
    with pytest.raises(ValueError):
        authentication["self_registration_role_id"]()


def test_profile_update_cannot_elevate_or_rebind_authenticated_account(authentication):
    fixture = authentication
    editable = {
        "nickname": "Edited synthetic profile",
        "avatar": "synthetic-avatar.png",
        "language": "Chinese",
        "color_schema": "Dark",
        "timezone": "UTC",
    }
    protected = {
        "id": "bob", "email": "bob@test.invalid", "role": "privileged-role",
        "access_token": "attacker-selected-token", "status": "0", "is_active": "0",
        "is_authenticated": "0", "is_anonymous": "1", "is_superuser": "1",
        "create_date": "attacker-date", "update_date": "attacker-date",
        "tenant_id": "other-tenant", "permissions": ["admin"],
        "new_password": "unverified-password",
    }
    response = fixture["client"].post("/v1/user/setting", headers={"Authorization": "alice-token"}, json={**editable, **protected})
    assert response.status_code == 200
    assert response.json["code"] == 0
    assert fixture["UserService"].updates
    user_id, written = fixture["UserService"].updates[-1]
    assert user_id == "alice"
    assert set(written) == set(editable) | {"update_date"}
    assert written["update_date"] != "attacker-date"
    with fixture["database"].connection_context():
        alice = fixture["User"].get_by_id("alice")
        bob = fixture["User"].get_by_id("bob")
        for key, value in editable.items():
            assert getattr(alice, key) == value
        assert alice.role == "common"
        assert alice.access_token == "alice-token"
        assert alice.email == "alice@test.invalid"
        assert alice.status == alice.is_active == alice.is_authenticated == "1"
        assert alice.is_anonymous == alice.is_superuser == "0"
        assert alice.create_date == "synthetic-original-date"
        assert check_password_hash(alice.password, "synthetic-password")
        assert bob.nickname == "Synthetic account"
    assert fixture["client"].get("/whoami", headers={"Authorization": "attacker-selected-token"}).status_code == 401


def test_profile_password_change_requires_current_password(authentication):
    fixture = authentication
    response = fixture["client"].post("/v1/user/setting", headers={"Authorization": "alice-token"}, json={"password": "wrong-password", "new_password": "replacement-password"})
    assert response.status_code == 401
    assert fixture["UserService"].updates == []
    response = fixture["client"].post("/v1/user/setting", headers={"Authorization": "alice-token"}, json={"password": "synthetic-password", "new_password": "replacement-password"})
    assert response.status_code == 200
    with fixture["database"].connection_context():
        alice = fixture["User"].get_by_id("alice")
        assert check_password_hash(alice.password, "replacement-password")
        assert not check_password_hash(alice.password, "synthetic-password")


def test_profile_update_requires_authenticated_account(authentication):
    response = authentication["client"].post("/v1/user/setting", json={"nickname": "Unauthenticated edit"})
    assert response.status_code == 401
    assert authentication["UserService"].updates == []
