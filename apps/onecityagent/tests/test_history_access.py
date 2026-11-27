"""History authorization regressions through the real API and SQLite DB."""

from __future__ import annotations

import pytest

from deerflow_fixture import add_user, load_api_fixture


@pytest.fixture
def platform(tmp_path):
    namespace = load_api_fixture(tmp_path / "catalogue.sqlite")
    add_user(namespace, "alice", "org-a", "test-alice")
    add_user(namespace, "bob", "org-a", "test-bob")
    with namespace["database"].connection_context():
        for actor in ("alice", "bob"):
            namespace["Dialog"].create(
                id=f"citybrain-{actor}", name=f"Synthetic dialog {actor}",
                creator_id=actor, tenant_id="org-a", description="",
            )
            namespace["Conversation"].create(
                id=f"message-{actor}", dialog_id=f"citybrain-{actor}",
                sort_order=1, scene="synthetic", plan="synthetic plan",
            )
    return namespace


def auth_headers():
    return {"Authorization": "test-alice"}


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
    response = getattr(platform["app"].test_client(), method)(path, headers=auth_headers())
    assert response.status_code == 404


@pytest.mark.parametrize("path,payload", [
    ("set", {"conversation_id": "message-bob", "message": []}),
    ("set", {"conversation_id": "message-alice", "dialog_id": "citybrain-bob"}),
    ("set", {"dialog_id": "citybrain-bob", "sort_order": 1}),
    ("save", {"conversation_id": "message-bob", "messages": []}),
    ("rm", {"conversation_ids": ["message-alice", "message-bob"]}),
])
def test_history_mutations_check_all_targets_before_execution(platform, path, payload):
    response = platform["app"].test_client().post(
        f"/v1/conversation/{path}", headers=auth_headers(), json=payload
    )
    assert response.status_code == 404
    with platform["database"].connection_context():
        assert platform["Conversation"].select().count() == 2


def test_history_list_without_filter_is_scoped_and_camelcase_filter_works(platform):
    client = platform["app"].test_client()
    all_response = client.get("/v1/conversation/list", headers=auth_headers())
    assert all_response.status_code == 200
    assert [item["id"] for item in all_response.json["data"]] == ["message-alice"]
    one_response = client.get(
        "/v1/conversation/list?dialogId=citybrain-alice", headers=auth_headers()
    )
    assert one_response.status_code == 200
    assert [item["id"] for item in one_response.json["data"]] == ["message-alice"]


def test_history_list_rejects_revoked_workspace_membership(platform):
    with platform["database"].connection_context():
        platform["UserTenant"].update(status="0").where(
            platform["UserTenant"].user_id == "alice"
        ).execute()
    client = platform["app"].test_client()
    assert client.get(
        "/v1/conversation/list?dialogId=citybrain-alice", headers=auth_headers()
    ).status_code == 404
    assert client.get("/v1/conversation/list", headers=auth_headers()).json["data"] == []


def test_profile_and_owned_history_remain_usable_without_autoconnect(platform):
    database = platform["database"]
    client = platform["app"].test_client()
    assert database.autoconnect is False
    assert database.is_closed()

    requests = [
        ("get", "/v1/user/info", None),
        ("post", "/v1/user/setting", {"nickname": "Updated synthetic profile"}),
        ("get", "/v1/user/tenant_info", None),
        ("get", "/v1/dialog/get/citybrain-alice", None),
        ("put", "/v1/dialog/update/citybrain-alice", {"name": "Updated synthetic dialog"}),
        ("post", "/v1/conversation/set", {
            "conversation_id": "message-alice", "plan": "Updated synthetic plan",
        }),
        ("get", "/v1/conversation/get?conversation_id=message-alice", None),
        ("get", "/v1/conversation/list?dialog_id=citybrain-alice", None),
    ]
    for method, path, payload in requests:
        options = {"headers": auth_headers()}
        if payload is not None:
            options["json"] = payload
        response = getattr(client, method)(path, **options)
        assert response.status_code == 200, (method, path, response.json)
        assert response.json["code"] == 0, (method, path, response.json)
        assert database.is_closed(), (method, path)

    with database.connection_context():
        assert platform["User"].get_by_id("alice").nickname == "Updated synthetic profile"
        assert platform["Dialog"].get_by_id("citybrain-alice").name == "Updated synthetic dialog"
        assert platform["Conversation"].get_by_id("message-alice").plan == "Updated synthetic plan"
