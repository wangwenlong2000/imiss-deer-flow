"""Real Coordinator routes with the application factory and isolated SQLite."""

from __future__ import annotations

import os
from collections.abc import MutableMapping
from datetime import datetime
from pathlib import Path
from types import ModuleType
from uuid import uuid4

from peewee import SqliteDatabase
from werkzeug.security import generate_password_hash


class RouteGlobals(MutableMapping):
    """Dictionary facade over the actual imported route module globals."""

    def __init__(self, module: ModuleType, values: dict):
        self.module = module
        self.values = values
        self._missing = object()
        self._originals = {}

    def __getitem__(self, key):
        if key in self.values:
            return self.values[key]
        return self.module.__dict__[key]

    def __setitem__(self, key, value):
        if key in self.values:
            self.values[key] = value
        else:
            self._originals.setdefault(key, self.module.__dict__.get(key, self._missing))
            self.module.__dict__[key] = value

    def __delitem__(self, key):
        if key in self.values:
            del self.values[key]
        else:
            original = self._originals.pop(key, self._missing)
            if original is self._missing:
                self.module.__dict__.pop(key, None)
            else:
                self.module.__dict__[key] = original

    def restore(self):
        for key, value in reversed(list(self._originals.items())):
            if value is self._missing:
                self.module.__dict__.pop(key, None)
            else:
                self.module.__dict__[key] = value
        self._originals.clear()

    def __iter__(self):
        return iter(set(self.values) | set(self.module.__dict__))

    def __len__(self):
        return len(set(self.values) | set(self.module.__dict__))


def load_fixture(database_path, source_root=None):
    """Build the production Coordinator app against a temporary SQLite DB."""
    database_path = Path(database_path)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    database = SqliteDatabase(
        str(database_path), pragmas={"journal_mode": "wal", "busy_timeout": 5000}
    )
    settings = {
        "SECRET_KEY": "isolated-test-secret",
        "DATABASE": {"type": "sqlite", "name": str(database_path)},
        "ONECITY_SELF_REGISTER_ROLE_CODE": "common",
    }
    for key in (
        "DEERFLOW_API_URL",
        "DEERFLOW_PUBLIC_BASE_URL",
        "DEERFLOW_FILE_INTERNAL_BASE",
        "DEERFLOW_THREAD_ENSURE_URL",
        "DEERFLOW_THREAD_UPLOAD_URL",
        "DEERFLOW_STREAM",
        "DEERFLOW_TIMEOUT",
        "DEERFLOW_ENSURE_TIMEOUT",
        "DEERFLOW_UPLOAD_TIMEOUT",
        "DEERFLOW_PREVIEW_TIMEOUT",
        "DEERFLOW_PREVIEW_MAX_CHARS",
        "ONECITY_DEERFLOW_IDENTITY_CLASS_MAP",
        "ONECITY_DEERFLOW_ROLE_PERMISSIONS",
    ):
        if key in os.environ:
            settings[key] = os.environ[key]

    from onecityagent.application.coordinator import create_coordinator_app
    from onecityagent.models import create_tables
    from onecityagent.models.db_models import (
        Dialog,
        Role,
        Tenant,
        User,
        UserTenant,
        Conversation,
    )
    from onecityagent.integrations.deerflow import routes

    app = create_coordinator_app(settings, database=database)
    with app.app_context():
        create_tables()
    values = {
        "app": app,
        "database": database,
        "DB": database,
        "User": User,
        "Role": Role,
        "Tenant": Tenant,
        "UserTenant": UserTenant,
        "Dialog": Dialog,
        "Conversation": Conversation,
    }
    return RouteGlobals(routes, values)


def load_api_fixture(database_path):
    """Build the real account/history API against an isolated SQLite DB."""
    database_path = Path(database_path)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    database = SqliteDatabase(
        str(database_path), pragmas={"journal_mode": "wal", "busy_timeout": 5000},
        autoconnect=False,
    )
    settings = {
        "SECRET_KEY": "isolated-test-secret",
        "DATABASE": {"type": "sqlite", "name": str(database_path)},
        "ONECITY_SELF_REGISTER_ROLE_CODE": "common",
    }
    from onecityagent.application.api import create_api_app
    from onecityagent.models import create_tables
    from onecityagent.models.db_models import (
        Conversation,
        Dialog,
        Role,
        Tenant,
        User,
        UserTenant,
    )

    app = create_api_app(settings, database=database)
    with app.app_context():
        create_tables()
    return {
        "app": app,
        "database": database,
        "DB": database,
        "User": User,
        "Role": Role,
        "Tenant": Tenant,
        "UserTenant": UserTenant,
        "Dialog": Dialog,
        "Conversation": Conversation,
    }


def add_user(namespace, user_id, org_id, token, *, role_code="common"):
    """Insert synthetic users/workspaces into the isolated database only."""
    with namespace["database"].connection_context():
        role = namespace["Role"].get_or_none(namespace["Role"].code == role_code)
        if role is None:
            role = namespace["Role"].create(
                id=uuid4().hex,
                code=role_code,
                name="Synthetic role",
                sort=2,
                status=1,
                type=1,
            )
        tenant, _created = namespace["Tenant"].get_or_create(
            id=org_id,
            defaults={
                "name": "Synthetic workspace",
                "public_key": None,
                "llm_id": "",
                "embd_id": "",
                "asr_id": "",
                "img2txt_id": "",
                "rerank_id": "",
                "tts_id": "",
                "parser_ids": "",
                "credit": 0,
                "status": "1",
            },
        )
        namespace["User"].create(
            id=user_id,
            access_token=token,
            nickname=f"Synthetic {user_id}",
            password=generate_password_hash("synthetic-password"),
            email=f"{user_id}@test.invalid",
            avatar="",
            language="Chinese",
            color_schema="Bright",
            timezone="UTC+8",
            last_login_time=None,
            is_authenticated="1",
            is_active="1",
            is_anonymous="0",
            login_channel="password",
            status="1",
            role=role.id,
            is_superuser=False,
        )
        namespace["UserTenant"].create(
            id=uuid4().hex,
            user_id=user_id,
            tenant_id=tenant.id,
            role=1,
            invited_by=user_id,
            status="1",
        )
