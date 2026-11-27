"""Focused authorization regression tests using an isolated real catalogue."""

from io import BytesIO
from contextlib import contextmanager
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest
import requests

from deerflow_fixture import add_user, load_fixture


@pytest.fixture
def fixture(tmp_path, monkeypatch):
    for key in ("ONECITY_DEERFLOW_ROLE_PERMISSIONS", "ONECITY_DEERFLOW_IDENTITY_CLASS_MAP"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("DEERFLOW_API_URL", "http://deerflow.test/api/deerflow/mobile/run")
    namespace = load_fixture(tmp_path / "catalogue.sqlite")
    add_user(namespace, "alice", "org-a", "test-alice")
    add_user(namespace, "bob", "org-a", "test-bob")
    add_user(namespace, "charlie", "org-b", "test-charlie")
    with namespace["database"].connection_context():
        for user, org in (("alice", "org-a"), ("bob", "org-a"), ("charlie", "org-b")):
            namespace["Dialog"].create(
                id=f"citybrain-{user}", name=f"Synthetic dialog {user}",
                creator_id=user, tenant_id=org, description="",
            )
    yield namespace
    namespace.restore()
    namespace["database"].close()


def post_iam(fixture, token="test-alice", **payload):
    return fixture["app"].test_client().post(
        "/internal/deerflow/iam", headers={"Authorization": token}, json=payload
    )


def test_iam_resolves_database_user_and_ignores_browser_identity(fixture):
    response = post_iam(fixture, action="authenticate", user_id="bob", org_id="org-b", roles=["admin"])
    assert response.status_code == 200
    principal = response.json["principal"]
    assert principal["user_id"] == "alice"
    assert principal["org_id"] == "org-a"
    assert principal["identity_class"] == "ordinary_user"
    assert principal["permissions"] == ["analysis", "upload"]


@pytest.mark.parametrize("header", [None, "", "Bearer", "bearer ", "Bearer bad token", ["test-alice"]])
def test_platform_token_parser_rejects_malformed_headers(fixture, header):
    assert fixture["_extract_platform_token"](header) == ""


def test_owned_workspace_precedes_invited_memberships(fixture):
    with fixture["database"].connection_context():
        fixture["Tenant"].create(
            id="aaa-invited", llm_id="", embd_id="", asr_id="", img2txt_id="",
            rerank_id="", tts_id="", parser_ids="", credit=0, status="1",
        )
        fixture["UserTenant"].create(
            id="alice-invited", user_id="alice", tenant_id="aaa-invited",
            role=2, invited_by="alice", status="1",
        )
    assert post_iam(fixture, action="authenticate").json["principal"]["org_id"] == "org-a"


@pytest.mark.parametrize("change", ["disabled", "inactive", "deauthenticated", "revoked", "membership", "tenant"])
def test_iam_rejects_invalidated_account_or_workspace(fixture, change):
    with fixture["database"].connection_context():
        user = fixture["User"]
        if change == "disabled":
            user.update(status="0").where(user.id == "alice").execute()
        elif change == "inactive":
            user.update(is_active="0").where(user.id == "alice").execute()
        elif change == "deauthenticated":
            user.update(is_authenticated="0").where(user.id == "alice").execute()
        elif change == "revoked":
            user.update(access_token=None).where(user.id == "alice").execute()
        elif change == "membership":
            fixture["UserTenant"].update(status="0").where(fixture["UserTenant"].user_id == "alice").execute()
        else:
            fixture["Tenant"].update(status="0").where(fixture["Tenant"].id == "org-a").execute()
    assert post_iam(fixture, action="authenticate").status_code == 401


@pytest.mark.parametrize("resource,conversation", [("citybrain-bob", "citybrain-bob"), ("citybrain-charlie", "citybrain-charlie"), ("citybrain-alice", "citybrain-bob")])
def test_iam_rejects_foreign_or_misbound_dialog(fixture, resource, conversation):
    response = post_iam(fixture, action="authorize", operation_type="analysis", target_audience="self", resource_id=resource, conversation_id=conversation)
    assert response.status_code == 403


def test_iam_rechecks_current_dialog_and_tenant(fixture):
    body = dict(action="authorize", operation_type="analysis", target_audience="self", resource_id="citybrain-alice", conversation_id="citybrain-alice")
    assert post_iam(fixture, **body).status_code == 200
    with fixture["database"].connection_context():
        fixture["Dialog"].update(tenant_id="org-b").where(fixture["Dialog"].id == "citybrain-alice").execute()
    assert post_iam(fixture, **body).status_code == 403


def test_client_thread_uuid_never_overrides_authoritative_mapping(fixture):
    authoritative = str(uuid4())
    calls = []
    fixture["_ensure_deerflow_thread"] = lambda conversation_id, **kwargs: (calls.append((conversation_id, kwargs)) or {"thread_id": authoritative, "conversation_id": conversation_id})
    with pytest.raises(PermissionError):
        fixture["_resolve_deerflow_thread"]("citybrain-alice", user_id="alice", explicit_thread_id=str(uuid4()), authorization_header="test-alice")
    one = fixture["_resolve_deerflow_thread"]("citybrain-alice", user_id="alice", authorization_header="test-alice")
    two = fixture["_resolve_deerflow_thread"]("citybrain-alice", user_id="alice", authorization_header="test-alice")
    assert one["thread_id"] == two["thread_id"] == authoritative
    assert len(calls) == 3


@pytest.mark.parametrize("route", ["preview", "download"])
def test_artifact_route_rejects_other_thread_even_for_owned_dialog(fixture, route):
    fixture["_resolve_deerflow_thread"] = lambda *args, **kwargs: {"thread_id": str(uuid4())}
    response = fixture["app"].test_client().get(
        f"/query/deerflow/file/{route}",
        headers={"Authorization": "test-alice"},
        query_string={"conversation_id": "citybrain-alice", "url": f"/api/threads/{uuid4()}/artifacts/report.csv", "filename": "report.csv"},
    )
    assert response.status_code == 404


@pytest.mark.parametrize("url", ["https://foreign.test/api/threads/{thread}/artifacts/report.csv", "/api/threads/{thread}/artifacts/../other", "/api/threads/{thread}/artifacts/%2e%2e/other", "/api/threads/{thread}/artifacts/\\private"])
def test_artifact_route_rejects_foreign_host_and_traversal(fixture, url):
    assert fixture["_safe_deerflow_internal_file_url"](url.format(thread=uuid4())) is None


@pytest.mark.parametrize("route", ["/query/deerflow", "/query/deerflow/upload", "/query/deerflow/file/preview", "/query/deerflow/file/download"])
def test_all_routes_reject_other_user_dialog_before_upstream_call(fixture, route):
    client = fixture["app"].test_client()
    kwargs = {"headers": {"Authorization": "test-alice"}, "query_string": {"conversation_id": "citybrain-bob", "query": "synthetic"}}
    response = client.post(route, data={"files": (BytesIO(b"value\n1\n"), "same.csv")}, **kwargs) if route.endswith("upload") else client.get(route, **kwargs)
    assert response.status_code == 404


def test_upstream_429_is_preserved_before_sse_response(fixture):
    thread_id = str(uuid4())
    fixture["_resolve_deerflow_thread"] = lambda *args, **kwargs: {"thread_id": thread_id, "conversation_id": "citybrain-alice"}
    upstream = requests.Response()
    upstream.status_code = 429
    upstream.headers["Retry-After"] = "7"

    def fail(*args, **kwargs):
        raise requests.HTTPError("secret upstream url must never be exposed", response=upstream)
        yield

    fixture["_stream_deerflow_sse_events"] = fail
    response = fixture["app"].test_client().get("/query/deerflow", headers={"Authorization": "test-alice"}, query_string={"conversation_id": "citybrain-alice", "query": "synthetic"})
    assert response.status_code == 429
    assert response.headers["Retry-After"] == "7"
    assert "secret" not in response.text
    assert "并发名额已满" in response.json["message"]


def test_malformed_json_and_deleted_dialog_are_rejected(fixture):
    client = fixture["app"].test_client()
    assert client.post("/internal/deerflow/iam", headers={"Authorization": "test-alice"}, json=["bad"]).status_code == 400
    assert client.post("/query/deerflow/dialog", headers={"Authorization": "test-alice"}, json=["bad"]).status_code == 400
    with fixture["database"].connection_context():
        fixture["Dialog"].update(status=0).where(fixture["Dialog"].id == "citybrain-alice").execute()
    assert client.post("/query/deerflow/dialog", headers={"Authorization": "test-alice"}, json={"conversation_id": "citybrain-alice"}).status_code == 404


@pytest.mark.parametrize("length", [33, 34, 35, 36])
def test_dialog_ids_longer_than_mysql_column_are_rejected_without_insert(fixture, length):
    conversation_id = "citybrain-" + "a" * (length - len("citybrain-"))
    with fixture["database"].connection_context():
        before = fixture["Dialog"].select().count()
    response = fixture["app"].test_client().post(
        "/query/deerflow/dialog", headers={"Authorization": "test-alice"},
        json={"conversation_id": conversation_id},
    )
    assert response.status_code == 400
    with fixture["database"].connection_context():
        assert fixture["Dialog"].select().count() == before
        assert not fixture["Dialog"].select().where(fixture["Dialog"].id == conversation_id[:32]).exists()


def test_maximum_32_character_dialog_id_is_persisted_exactly_and_idempotent(fixture):
    conversation_id = "citybrain-" + "a" * (32 - len("citybrain-"))
    assert len(conversation_id) == 32
    client = fixture["app"].test_client()
    for _ in range(2):
        response = client.post(
            "/query/deerflow/dialog", headers={"Authorization": "test-alice"},
            json={"conversation_id": conversation_id},
        )
        assert response.status_code == 200
        assert response.json["data"]["dialog_id"] == conversation_id
    with fixture["database"].connection_context():
        saved = fixture["Dialog"].get_by_id(conversation_id)
        assert saved.id == conversation_id
        assert saved.creator_id == "alice" and saved.tenant_id == "org-a"
        assert fixture["Dialog"].select().where(fixture["Dialog"].id == conversation_id).count() == 1


def fake_stream_response(lines):
    class Upstream:
        status_code = 200

        def raise_for_status(self):
            return None

        def iter_lines(self, **kwargs):
            yield from lines

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

    return Upstream()


def parse_events(chunks):
    return [json.loads(item[6:].strip()) for item in chunks if item.startswith("data: ")]


def stream_events(fixture, *args, **kwargs):
    with fixture["app"].app_context():
        return list(fixture["_stream_deerflow_sse_events"](*args, **kwargs))


def test_exact_run_status_events_reach_frontend_without_invented_running(fixture, monkeypatch):
    captured = {}
    thread_id, run_id = str(uuid4()), str(uuid4())
    lines = []
    for event, status in (("accepted", "submitted"), ("start", "submitted"), ("status", "pending"), ("status", "running"), ("heartbeat", "running"), ("done", "success")):
        lines.extend((f"event: {event}", "data: " + json.dumps({"thread_id": thread_id, "run_id": run_id, "status": status}), ""))

    def post(url, **kwargs):
        captured.update(kwargs)
        return fake_stream_response(lines)

    monkeypatch.setattr(requests, "post", post)
    events = parse_events(stream_events(fixture, "synthetic", "citybrain-alice", "alice", thread_id=thread_id, authorization_header="test-alice"))
    statuses = [item.get("status") for item in events if item.get("status")]
    assert statuses == ["submitted", "submitted", "submitted", "pending", "running", "running"]
    assert events[-1]["finished"] is True
    assert captured["headers"] == {"Authorization": "test-alice"}
    assert captured["allow_redirects"] is False


def test_terminal_run_identity_survives_without_status_poll_events(fixture, monkeypatch):
    thread_id, run_id = str(uuid4()), str(uuid4())
    lines = ["event: done", "data: " + json.dumps({
        "thread_id": thread_id, "run_id": run_id, "status": "completed",
    })]
    monkeypatch.setattr(requests, "post", lambda *args, **kwargs: fake_stream_response(lines))
    events = parse_events(stream_events(fixture, "synthetic", "citybrain-alice", "alice", thread_id=thread_id))
    assert events[-1]["type"] == "[DONE]"
    assert events[-1]["finished"] is True
    assert events[-1]["thread_id"] == thread_id
    assert events[-1]["run_id"] == run_id


def test_unterminated_upstream_stream_is_not_reported_as_success(fixture, monkeypatch):
    monkeypatch.setattr(requests, "post", lambda *args, **kwargs: fake_stream_response(["event: status", 'data: {"status":"running"}']))
    events = parse_events(stream_events(fixture, "synthetic", "citybrain-alice", "alice"))
    assert events[-2]["type"] == "error"
    assert events[-1]["finished"] is False


@pytest.mark.parametrize("status", ["failed", "cancelled", "incomplete"])
def test_unsuccessful_done_is_not_shown_as_finished(fixture, monkeypatch, status):
    lines = ["event: done", "data: " + json.dumps({"status": status, "code": 500})]
    monkeypatch.setattr(requests, "post", lambda *args, **kwargs: fake_stream_response(lines))
    events = parse_events(stream_events(fixture, "synthetic", "citybrain-alice", "alice"))
    assert events[-2]["type"] == "error"
    assert events[-1]["finished"] is False
    assert events[-1]["status"] == status


def test_resume_uses_get_join_endpoint_without_posting_a_new_task(fixture, monkeypatch):
    captured = []
    thread_id, run_id = str(uuid4()), str(uuid4())

    def get(url, **kwargs):
        captured.append((url, kwargs))
        return fake_stream_response(["event: done", 'data: {"status":"completed"}'])

    monkeypatch.setattr(requests, "get", get)
    monkeypatch.setattr(requests, "post", lambda *args, **kwargs: pytest.fail("resume must never submit a new run"))
    events = parse_events(stream_events(fixture,
        "synthetic", "citybrain-alice", "alice", thread_id=thread_id, resume_run_id=run_id,
        authorization_header="test-alice",
    ))
    assert events[-1]["finished"] is True
    assert captured[0][0] == f"http://deerflow.test/api/deerflow/mobile/runs/{thread_id}/{run_id}/stream"
    assert captured[0][1]["params"] == {"query": "synthetic", "conversation_id": "citybrain-alice"}


def test_native_admission_429_inside_mobile_sse_is_preserved(fixture, monkeypatch):
    lines = ["event: accepted", 'data: {"status":"submitted"}', "event: error", 'data: {"code":429,"message":"并发名额已满","retry_after":"9"}']
    monkeypatch.setattr(requests, "post", lambda *args, **kwargs: fake_stream_response(lines))
    events = parse_events(stream_events(fixture, "synthetic", "citybrain-alice", "alice"))
    assert events[-2]["http_status"] == 429
    assert events[-2]["retry_after"] == "9"
    assert events[-1]["finished"] is False


def test_missing_nonstream_result_never_becomes_mock_success(fixture):
    for invalid in (None, {}, {"segments": []}):
        with pytest.raises(RuntimeError):
            fixture["_normalize_deerflow_segments"](invalid, "synthetic")


@pytest.mark.parametrize('accepted', [False, True])
def test_upload_preserves_compliance_result_and_refusal_message(fixture, monkeypatch, accepted):
    thread_id = str(uuid4())
    fixture['_resolve_deerflow_thread'] = lambda *args, **kwargs: {'thread_id': thread_id, 'conversation_id': 'citybrain-alice'}
    compliance = {'checks': [{'gate': 'InputGate', 'status': 'passed' if accepted else 'handled', 'scan_complete': accepted, 'audit_ref': 'audit-synthetic'}]}
    response = requests.Response()
    response.status_code = 200
    response._content = json.dumps({'success': accepted, 'files': [{'filename': 'safe.txt', 'virtual_path': '/mnt/user-data/uploads/safe.txt'}] if accepted else [], 'message': '安全文件已检查' if accepted else '【合规检查未完成】附件文本提取为空，已撤回上传', 'compliance': compliance}).encode()
    monkeypatch.setattr(requests, 'post', lambda *args, **kwargs: response)
    result = fixture['app'].test_client().post('/query/deerflow/upload', headers={'Authorization': 'test-alice'}, query_string={'conversation_id': 'citybrain-alice'}, data={'files': (BytesIO(b'synthetic'), 'safe.txt')})
    assert result.status_code == (200 if accepted else 422)
    assert result.json['code'] == (0 if accepted else 422)
    assert result.json['data']['compliance'] == compliance
    if not accepted:
        assert result.json['data']['files'] == []
        assert '合规检查未完成' in result.json['message']
