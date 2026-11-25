"""Run the real OneCity DeerFlow routes with an isolated SQLite catalogue.

Only the integration section is loaded so tests cannot initialize production
configuration, A2A clients, model clients or the external MySQL database.
"""

from datetime import datetime
from enum import IntEnum
from pathlib import Path
from threading import Lock
from urllib.parse import quote, unquote, urlparse
import json
import os
import re
import time
import uuid

from flask import Flask, Response, jsonify, request
from loguru import logger
from peewee import CharField, DateTimeField, IntegerField, Model, SqliteDatabase, TextField
import requests


def load_fixture(database_path, source_root=None):
    """Return a namespace containing app, models and actual route functions."""
    root = Path(source_root) if source_root else Path(__file__).resolve().parents[1]
    database = SqliteDatabase(str(database_path), pragmas={"journal_mode": "wal", "busy_timeout": 5000})

    class BaseModel(Model):
        class Meta:
            database = None

    BaseModel._meta.database = database

    class User(BaseModel):
        id = CharField(primary_key=True)
        access_token = CharField(null=True)
        status = CharField(default="1")
        is_active = CharField(default="1")
        is_authenticated = CharField(default="1")
        role = CharField(default="common")

    class Role(BaseModel):
        id = CharField(primary_key=True)
        code = CharField(default="common")
        status = IntegerField(default=1)

    class Tenant(BaseModel):
        id = CharField(primary_key=True)
        status = CharField(default="1")

    class UserTenant(BaseModel):
        id = CharField(primary_key=True)
        user_id = CharField()
        tenant_id = CharField()
        role = IntegerField(default=1)
        status = CharField(default="1")

    class Dialog(BaseModel):
        id = CharField(primary_key=True)
        creator_id = CharField()
        tenant_id = CharField()
        name = CharField(default="Synthetic test dialog")
        description = TextField(default="Synthetic test fixture")
        status = IntegerField(default=1)
        finished = IntegerField(default=0)
        create_date = DateTimeField(default=datetime.now)
        update_date = DateTimeField(default=datetime.now)

    class Conversation(BaseModel):
        id = CharField(primary_key=True)
        dialog_id = CharField()
        message = TextField(default="[]")
        reference = TextField(default="[]")
        scene = CharField(default="test")
        plan = TextField(default="[]")
        sort_order = IntegerField(default=0)
        create_date = DateTimeField(default=datetime.now)
        update_date = DateTimeField(default=datetime.now)

    class StatusEnum(IntEnum):
        INVALID = 0
        VALID = 1

    models = (User, Role, Tenant, UserTenant, Dialog, Conversation)
    database.create_tables(models)
    database.close()
    app = Flask("isolated_onecity_deerflow")
    namespace = {
        "app": app, "database": database, "DB": database,
        "User": User, "Role": Role, "Tenant": Tenant, "UserTenant": UserTenant,
        "Dialog": Dialog, "Conversation": Conversation, "StatusEnum": StatusEnum,
        "request": request, "Response": Response, "jsonify": jsonify,
        "logger": logger, "requests": requests, "datetime": datetime,
        "Lock": Lock, "quote": quote, "unquote": unquote, "urlparse": urlparse,
        "json": json, "os": os, "re": re, "time": time, "uuid": uuid,
        "swag_from": lambda *_args, **_kwargs: lambda function: function,
    }
    source = (root / "onecityagent/service/a2a_service/coordinator_sse_acting.py").read_text()
    section = source[source.index("_ALLOWED_DEERFLOW_ROLES ="):source.index("# ===== DeerFlow mobile integration: end =====")]
    exec(compile(section, str(root / "onecityagent/service/a2a_service/coordinator_sse_acting.py"), "exec"), namespace)
    return namespace


def add_user(namespace, user_id, org_id, token, *, role_code="common"):
    """Insert only synthetic rows in the supplied isolated database."""
    with namespace["database"].connection_context():
        namespace["Role"].get_or_create(id=role_code, defaults={"code": role_code})
        namespace["Tenant"].get_or_create(id=org_id)
        namespace["User"].create(id=user_id, access_token=token, role=role_code)
        namespace["UserTenant"].create(id=f"{user_id}-{org_id}", user_id=user_id, tenant_id=org_id)
