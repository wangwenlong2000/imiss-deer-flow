"""HTTP contract tests for the OneCity adapter, without a live IAM service."""

import asyncio
import json

import httpx
import pytest

from app.gateway import onecity_iam
from app.gateway.mobile_iam import MobileAuthorizationDecision, MobileAuthorizationInput, MobileIAMPrincipal

TOKEN = "Bearer private-test-credential"
PRINCIPAL = {"user_id": "user-a", "org_id": "org-a", "identity_class": "ordinary_user", "roles": [], "permissions": []}
DECISION = {
    "allowed": True,
    "permission_verified": True,
    "operation": {"type": "analysis", "target_audience": "self"},
    "resource": {"resource_id": "dialog-a", "owner_user_id": "user-a", "owner_org_id": "org-a", "classification": "internal", "is_anonymized": False},
}


@pytest.fixture
def mock_iam(monkeypatch):
    monkeypatch.setenv("MOBILE_IAM_ENDPOINT", "http://iam.test/internal/deerflow/iam")
    original_client = httpx.AsyncClient

    def install(handler):
        transport = httpx.MockTransport(handler)
        monkeypatch.setattr(onecity_iam.httpx, "AsyncClient", lambda *args, **kwargs: original_client(*args, transport=transport, **kwargs))
        return onecity_iam.OneCityIAMProvider()

    return install


def test_authentication_and_authorization_preserve_request_credential_without_serializing_it(mock_iam):
    requests = []

    def handler(request):
        payload = json.loads(request.content)
        requests.append((request.headers["authorization"], payload))
        return httpx.Response(200, json={"principal": PRINCIPAL} if payload["action"] == "authenticate" else {"decision": DECISION})

    provider = mock_iam(handler)

    async def scenario():
        principal = MobileIAMPrincipal.model_validate(await provider.authenticate(TOKEN))
        decision = await provider.authorize(principal, MobileAuthorizationInput(request_id="request-a", turn_id="turn-a", conversation_id="dialog-a", operation_type="analysis", target_audience="self", resource_id="dialog-a"))
        assert MobileAuthorizationDecision.model_validate(decision).allowed
        assert principal.iam_credential == TOKEN
        assert TOKEN not in principal.model_dump_json()
        assert TOKEN not in repr(principal)
        assert "iam_credential" not in principal.model_dump()

    asyncio.run(scenario())
    assert len(requests) == 2
    assert all(credential == TOKEN and TOKEN not in json.dumps(payload) for credential, payload in requests)
    assert requests[0][1] == {"action": "authenticate"}
    assert requests[1][1] == {"action": "authorize", "request_id": "request-a", "turn_id": "turn-a", "conversation_id": "dialog-a", "operation_type": "analysis", "target_audience": "self", "resource_id": "dialog-a"}


@pytest.mark.parametrize("status", [401, 403])
@pytest.mark.parametrize("action", ["authenticate", "authorize"])
def test_iam_denials_are_permission_errors(mock_iam, status, action):
    provider = mock_iam(lambda request: httpx.Response(status, json={"detail": "denied"}))

    async def scenario():
        with pytest.raises(PermissionError) as error:
            if action == "authenticate":
                await provider.authenticate(TOKEN)
            else:
                await provider.authorize(MobileIAMPrincipal(**PRINCIPAL, iam_credential=TOKEN), MobileAuthorizationInput(request_id="r", turn_id="t", conversation_id="c"))
        assert TOKEN not in str(error.value)

    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["timeout", "connection", "server_error", "invalid_json", "array", "missing_principal", "malformed_principal"])
def test_iam_transport_and_malformed_responses_fail_closed(mock_iam, failure):
    def handler(request):
        if failure == "timeout":
            raise httpx.ReadTimeout("IAM did not respond", request=request)
        if failure == "connection":
            raise httpx.ConnectError("IAM is unreachable", request=request)
        if failure == "server_error":
            return httpx.Response(503, json={"detail": "unavailable"})
        if failure == "invalid_json":
            return httpx.Response(200, content=b"not-json")
        if failure == "array":
            return httpx.Response(200, json=[])
        if failure == "malformed_principal":
            return httpx.Response(200, json={"principal": []})
        return httpx.Response(200, json={})

    provider = mock_iam(handler)

    async def scenario():
        with pytest.raises(RuntimeError) as error:
            await provider.authenticate(TOKEN)
        assert TOKEN not in str(error.value)

    asyncio.run(scenario())


@pytest.mark.parametrize("response", [{}, {"decision": []}, {"decision": None}])
def test_missing_or_malformed_authorization_decision_is_not_accepted(mock_iam, response):
    provider = mock_iam(lambda request: httpx.Response(200, json=response))

    async def scenario():
        with pytest.raises(RuntimeError):
            await provider.authorize(MobileIAMPrincipal(**PRINCIPAL, iam_credential=TOKEN), MobileAuthorizationInput(request_id="r", turn_id="t", conversation_id="c"))

    asyncio.run(scenario())


def test_missing_credentials_never_call_the_upstream(mock_iam):
    def handler(request):
        raise AssertionError("Missing credentials must be rejected before HTTP")

    provider = mock_iam(handler)

    async def scenario():
        with pytest.raises(PermissionError):
            await provider.authenticate(None)
        with pytest.raises(PermissionError):
            await provider.authorize(MobileIAMPrincipal(**PRINCIPAL), MobileAuthorizationInput(request_id="r", turn_id="t", conversation_id="c"))

    asyncio.run(scenario())
