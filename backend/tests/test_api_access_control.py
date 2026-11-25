"""Exercise public API authorization and private graph credentials over HTTP."""

import asyncio
import json
from unittest.mock import AsyncMock

import httpx
import pytest
from starlette.requests import Request
from starlette.responses import JSONResponse

from app import graph_access, graph_http
from app.gateway import access_middleware, mobile_iam
from app.gateway.mobile_iam import MobileIAMPrincipal, authenticate_mobile_request
from app.internal_auth import INTERNAL_GRAPH_HEADER, internal_graph_headers, verify_internal_graph_header


class Provider:
    def __init__(self):
        self.revoked = set()
        self.authentications = 0
        self.permissions = []

    async def authenticate(self, authorization):
        self.authentications += 1
        if authorization != "Bearer valid-token":
            raise PermissionError("Invalid token")
        return MobileIAMPrincipal(user_id="alice", org_id="org-a", identity_class="ordinary_user", permissions=self.permissions)

    async def authorize(self, principal, request):
        if request.conversation_id in self.revoked:
            raise PermissionError("Revoked")
        return {
            "allowed": True,
            "permission_verified": True,
            "operation": {"type": request.operation_type, "target_audience": request.target_audience},
            "resource": {"resource_id": request.resource_id, "owner_org_id": principal.org_id, "classification": "internal"},
        }


@pytest.fixture
def provider(monkeypatch):
    configured = Provider()
    monkeypatch.setenv("MOBILE_IAM_PROVIDER", "tests:provider")
    monkeypatch.setenv("DEER_FLOW_ADMISSION_SIGNING_KEY", "test-secret-never-a-production-secret-1234")
    monkeypatch.setattr(mobile_iam, "get_mobile_iam_provider", lambda: configured)
    monkeypatch.setattr(
        graph_access,
        "graph_thread_metadata",
        AsyncMock(return_value={"org_id": "org-a", "user_id": "alice", "conversation_id": "dialog-a"}),
    )
    return configured


async def ok(scope, receive, send):
    await JSONResponse({"ok": True})(scope, receive, send)


def request(app, method, path, **kwargs):
    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
            return await client.request(method, path, **kwargs)
    return asyncio.run(run())


TOKEN = {"Authorization": "Bearer valid-token"}


@pytest.mark.parametrize("method,path", [
    ("GET", "/threads/t"), ("GET", "/threads/t/state"),
    ("GET", "/threads/t/runs/r/stream"), ("POST", "/threads/t/history"),
    ("POST", "/threads/t/state/checkpoint"),
    ("POST", "/threads/t/runs/r/cancel"), ("DELETE", "/threads/t"),
    ("DELETE", "/threads/t/runs/r"),
])
def test_graph_checks_current_owner_and_iam_for_each_operation(provider, method, path):
    app = graph_access.GraphAccessMiddleware(ok)
    assert request(app, method, path, headers=TOKEN).status_code == 200
    provider.revoked.add("dialog-a")
    assert request(app, method, path, headers=TOKEN).status_code == 403


@pytest.mark.parametrize("owner", [
    {"org_id": "org-b", "user_id": "alice", "conversation_id": "dialog-a"},
    {"org_id": "org-a", "user_id": "bob", "conversation_id": "dialog-a"},
    {"user_id": "alice", "conversation_id": "dialog-a"},
])
def test_graph_hides_cross_tenant_and_legacy_threads(provider, monkeypatch, owner):
    monkeypatch.setattr(graph_access, "graph_thread_metadata", AsyncMock(return_value=owner))
    assert request(graph_access.GraphAccessMiddleware(ok), "GET", "/threads/t/state", headers=TOKEN).status_code == 404


@pytest.mark.parametrize("method,path", [
    ("POST", "/threads"), ("POST", "/threads/t/runs/stream"),
    ("POST", "/runs/batch"), ("POST", "/runs/cancel"),
    ("PATCH", "/threads/t"), ("POST", "/threads/t/state"),
    ("POST", "/threads/t/copy"), ("POST", "/threads/count"),
    ("GET", "/store/items"), ("POST", "/assistants/search"),
])
def test_graph_requires_gateway_for_unscoped_or_state_mutating_operations(provider, method, path):
    assert request(graph_access.GraphAccessMiddleware(ok), method, path, json={}, headers=TOKEN).status_code == 403


def test_graph_rejects_unauthenticated_and_bad_internal_credentials(provider):
    app = graph_access.GraphAccessMiddleware(ok)
    assert request(app, "GET", "/threads/t").status_code == 401
    assert request(app, "GET", "/threads/t", headers={INTERNAL_GRAPH_HEADER: "bad", **TOKEN}).status_code == 403
    assert request(app, "GET", "/threads/t", headers=internal_graph_headers()).status_code == 200
    assert request(app, "GET", "/ok").status_code == 200
    assert verify_internal_graph_header("非ASCII签名") is False


def test_internal_run_creation_still_requires_signed_admission_in_iam_mode(provider):
    app = graph_access.GraphAccessMiddleware(graph_http.RunAdmissionMiddleware(ok))
    response = request(app, "POST", "/threads/t/runs", headers=internal_graph_headers(), json={})
    assert response.status_code == 403


def test_internal_credential_is_consumed_before_native_header_persistence(provider):
    async def native(scope, receive, send):
        assert INTERNAL_GRAPH_HEADER not in Request(scope).headers
        await ok(scope, receive, send)

    response = request(graph_access.GraphAccessMiddleware(native), "POST", "/threads", headers=internal_graph_headers(), json={})
    assert response.status_code == 200


def test_graph_search_forces_owner_filters_and_omits_revoked_resource_state(provider):
    provider.revoked.add("revoked")
    received = []

    async def search(scope, receive, send):
        received.append(json.loads((await receive())["body"]))
        records = [
            {"thread_id": "visible", "metadata": {"org_id": "org-a", "user_id": "alice", "conversation_id": "allowed"}},
            {"thread_id": "revoked", "metadata": {"org_id": "org-a", "user_id": "alice", "conversation_id": "revoked"}, "values": {"secret": "hidden"}},
            {"thread_id": "foreign", "metadata": {"org_id": "org-b", "user_id": "alice", "conversation_id": "foreign"}},
        ]
        await JSONResponse(records)(scope, receive, send)

    response = request(graph_access.GraphAccessMiddleware(search), "POST", "/threads/search", headers=TOKEN,
                       json={"limit": 10, "metadata": {"org_id": "org-b", "user_id": "bob", "tag": "work"}})
    assert response.status_code == 200
    assert [record["thread_id"] for record in response.json()] == ["visible"]
    assert received[0]["metadata"] == {"org_id": "org-a", "user_id": "alice", "tag": "work"}
    assert "hidden" not in response.text


def test_graph_dependency_errors_do_not_look_like_missing_threads(provider, monkeypatch):
    from fastapi import HTTPException
    monkeypatch.setattr(graph_access, "graph_thread_metadata", AsyncMock(side_effect=HTTPException(503, "unavailable")))
    assert request(graph_access.GraphAccessMiddleware(ok), "GET", "/threads/t", headers=TOKEN).status_code == 503


def test_search_select_still_retrieves_metadata_for_authorization(provider):
    async def search(scope, receive, send):
        payload = json.loads((await receive())["body"])
        assert payload["select"] == ["thread_id", "metadata"]
        await JSONResponse([{"thread_id": "t", "metadata": {"org_id": "org-a", "user_id": "alice", "conversation_id": "dialog-a"}}])(scope, receive, send)

    response = request(graph_access.GraphAccessMiddleware(search), "POST", "/threads/search", headers=TOKEN, json={"select": ["thread_id"]})
    assert response.json() == [{"thread_id": "t"}]


@pytest.mark.parametrize("path", ["/api/agents", "/api/mcp/config", "/api/channels", "/api/skill-router/index"])
def test_shared_platform_management_requires_explicit_admin_permission(provider, path):
    app = access_middleware.GatewayAccessMiddleware(ok)
    assert request(app, "GET", path, headers=TOKEN).status_code == 403
    provider.permissions.append("platform:admin")
    assert request(app, "GET", path, headers=TOKEN).status_code == 200


def test_gateway_authentication_is_shared_with_endpoint_and_audit_omits_credentials(provider, caplog):
    async def endpoint(scope, receive, send):
        _, principal = await authenticate_mobile_request(Request(scope))
        assert principal.user_id == "alice"
        await ok(scope, receive, send)

    with caplog.at_level("INFO", logger="deerflow.security.access"):
        response = request(access_middleware.GatewayAccessMiddleware(endpoint), "GET", "/api/models?token=query-secret", headers=TOKEN)
    assert response.status_code == 200
    assert provider.authentications == 1
    assert "access_decision" in caplog.text
    assert "valid-token" not in caplog.text
    assert "query-secret" not in caplog.text


def test_gateway_suggestions_cannot_skip_thread_authorization(provider, monkeypatch):
    from fastapi import HTTPException
    check = AsyncMock(side_effect=HTTPException(404, "Thread not found"))
    monkeypatch.setattr(access_middleware, "require_thread_access", check)
    response = request(access_middleware.GatewayAccessMiddleware(ok), "POST", "/api/threads/foreign/suggestions", headers=TOKEN, json={})
    assert response.status_code == 404
    assert check.await_count == 1


def test_unconfigured_development_keeps_existing_api_behavior(monkeypatch):
    monkeypatch.setattr(mobile_iam, "get_mobile_iam_provider", lambda: None)
    monkeypatch.setattr(mobile_iam, "mobile_iam_required", lambda: False)
    assert request(graph_access.GraphAccessMiddleware(ok), "POST", "/threads", json={}).status_code == 200
    assert request(access_middleware.GatewayAccessMiddleware(ok), "GET", "/api/agents").status_code == 200
