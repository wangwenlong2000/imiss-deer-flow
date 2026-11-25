"""Exercise the existing platform history controllers with a real test DB."""

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
import ast
import json

from flask import Blueprint, g, jsonify, request
import pytest

from deerflow_fixture import add_user, load_fixture


def definitions(source_file, namespace, names=None):
    tree = ast.parse(source_file.read_text())
    nodes = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and (names is None or node.name in names)]
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(source_file), "exec"), namespace)


@pytest.fixture
def platform(tmp_path):
    root = Path(__file__).resolve().parents[1]
    namespace = load_fixture(tmp_path / "catalogue.sqlite")
    add_user(namespace, "alice", "org-a", "test-alice")
    add_user(namespace, "bob", "org-a", "test-bob")
    with namespace["database"].connection_context():
        for actor in ("alice", "bob"):
            namespace["Dialog"].create(id=f"citybrain-{actor}", creator_id=actor, tenant_id="org-a")
            namespace["Conversation"].create(id=f"message-{actor}", dialog_id=f"citybrain-{actor}")
    definitions(root / "onecityagent/service/dialog_service/access.py", namespace)
    namespace.update({
        "Blueprint": Blueprint, "g": g, "jsonify": jsonify, "request": request,
        "current_user": SimpleNamespace(id="alice", is_authenticated=True),
        "login_required": lambda function: function,
        "validate_request": lambda *args: lambda function: function,
        "get_json_result": lambda data=None, code=0, message="success", **kwargs: jsonify({"code": code, "message": message, "data": data}),
        "get_data_error_result": lambda message="invalid", **kwargs: (jsonify({"code": 400, "message": message}), 400),
        "server_error_response": lambda error: (jsonify({"error": type(error).__name__}), 500),
        "conversation_service": SimpleNamespace(),
        "dialog_service": SimpleNamespace(),
        "Dict": dict, "List": list, "Optional": __import__("typing").Optional, "Any": __import__("typing").Any,
        "Tuple": tuple, "datetime": datetime,
        "dialog_bp": Blueprint("dialog", __name__),
        "conversation_bp": Blueprint("conversation", __name__),
    })
    namespace["Conversation"].getter_by = classmethod(lambda cls, key: getattr(cls, key))
    definitions(root / "onecityagent/service/conversation_service/__init__.py", namespace, {"ConversationService"})
    namespace["conversation_service"] = namespace["ConversationService"]()
    definitions(root / "onecityagent/controller/dialog_controller.py", namespace)
    definitions(root / "onecityagent/controller/conversation_controller.py", namespace)
    namespace["app"].register_blueprint(namespace["dialog_bp"], url_prefix="/v1/dialog")
    namespace["app"].register_blueprint(namespace["conversation_bp"], url_prefix="/v1/conversation")
    return namespace


@pytest.mark.parametrize("method,path", [
    ("get", "/v1/dialog/get/citybrain-bob"),
    ("put", "/v1/dialog/update/citybrain-bob"),
    ("put", "/v1/dialog/finish/citybrain-bob"),
    ("delete", "/v1/dialog/delete/citybrain-bob"),
    ("get", "/v1/conversation/get?conversation_id=message-bob"),
    ("get", "/v1/conversation/list?dialogId=citybrain-bob"),
    ("get", "/v1/conversation/list_by_dialog?dialog_id=citybrain-bob"),
])
def test_history_identifier_routes_deny_foreign_owner(platform, method, path):
    response = getattr(platform["app"].test_client(), method)(path)
    assert response.status_code == 404


@pytest.mark.parametrize("path,payload", [
    ("set", {"conversation_id": "message-bob", "message": []}),
    ("set", {"conversation_id": "message-alice", "dialog_id": "citybrain-bob"}),
    ("set", {"dialog_id": "citybrain-bob", "sort_order": 1}),
    ("save", {"conversation_id": "message-bob", "messages": []}),
    ("rm", {"conversation_ids": ["message-alice", "message-bob"]}),
])
def test_history_mutations_check_all_targets_before_execution(platform, path, payload):
    response = platform["app"].test_client().post(f"/v1/conversation/{path}", json=payload)
    assert response.status_code == 404
    with platform["database"].connection_context():
        assert platform["Conversation"].select().count() == 2


def test_history_list_without_filter_is_scoped_and_camelcase_filter_works(platform):
    client = platform["app"].test_client()
    all_response = client.get("/v1/conversation/list")
    assert all_response.status_code == 200
    assert [item["id"] for item in all_response.json["data"]] == ["message-alice"]
    one_response = client.get("/v1/conversation/list?dialogId=citybrain-alice")
    assert one_response.status_code == 200
    assert [item["id"] for item in one_response.json["data"]] == ["message-alice"]


def test_history_list_rejects_revoked_workspace_membership(platform):
    with platform["database"].connection_context():
        platform["UserTenant"].update(status="0").where(platform["UserTenant"].user_id == "alice").execute()
    client = platform["app"].test_client()
    assert client.get("/v1/conversation/list?dialogId=citybrain-alice").status_code == 404
    assert client.get("/v1/conversation/list").json["data"] == []
