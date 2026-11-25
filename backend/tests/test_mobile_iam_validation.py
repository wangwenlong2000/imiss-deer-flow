"""Reject malformed authoritative identities without normalizing account IDs."""

import asyncio
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from starlette.requests import Request

from app.gateway import mobile_iam


def _principal(**overrides):
    return {
        "user_id": "user",
        "org_id": "org",
        "identity_class": "ordinary_user",
        **overrides,
    }


def _resource(**overrides):
    return {
        "resource_id": "resource",
        "owner_org_id": "org",
        "classification": "internal",
        **overrides,
    }


def _authorization_input():
    return mobile_iam.MobileAuthorizationInput(
        request_id="request",
        turn_id="turn",
        conversation_id="dialog",
        resource_id="resource",
        operation_type="analysis",
        target_audience="self",
    )


def _decision(resource):
    return {
        "allowed": True,
        "permission_verified": True,
        "operation": {"type": "analysis", "target_audience": "self"},
        "resource": resource,
    }


@pytest.mark.parametrize("field", ["user_id", "org_id", "department_id"])
@pytest.mark.parametrize("value", ["", " ", "\t\n", "\u3000"])
def test_principal_rejects_blank_identifiers(field, value):
    with pytest.raises(ValidationError):
        mobile_iam.MobileIAMPrincipal.model_validate(_principal(**{field: value}))


@pytest.mark.parametrize(
    "field",
    [
        "resource_id",
        "owner_org_id",
        "owner_user_id",
        "owner_department_id",
        "target_org_id",
    ],
)
@pytest.mark.parametrize("value", ["", " ", "\t\n", "\u3000"])
def test_authorized_resource_rejects_blank_identifiers(field, value):
    with pytest.raises(ValidationError):
        mobile_iam.MobileAuthorizedResource.model_validate(_resource(**{field: value}))


def test_valid_identifiers_keep_their_exact_original_value_and_optional_none():
    principal = mobile_iam.MobileIAMPrincipal.model_validate(
        _principal(user_id=" user ", org_id=" org ")
    )
    resource = mobile_iam.MobileAuthorizedResource.model_validate(
        _resource(resource_id=" resource ", owner_org_id=" org ")
    )
    assert (principal.user_id, principal.org_id, principal.department_id) == (
        " user ",
        " org ",
        None,
    )
    assert (resource.resource_id, resource.owner_org_id, resource.owner_user_id) == (
        " resource ",
        " org ",
        None,
    )


@pytest.mark.parametrize(
    "failure,status",
    [(PermissionError("secret token"), 401), (RuntimeError("secret token"), 503)],
)
def test_authentication_errors_preserve_status_without_logging_exception_secrets(
    monkeypatch, caplog, failure, status
):
    async def authenticate(_authorization):
        raise failure

    monkeypatch.setattr(
        mobile_iam,
        "get_mobile_iam_provider",
        lambda: SimpleNamespace(authenticate=authenticate),
    )
    request = Request(
        {"type": "http", "headers": [(b"authorization", b"Bearer private-credential")]}
    )
    with (
        caplog.at_level("ERROR", logger=mobile_iam.__name__),
        pytest.raises(HTTPException) as result,
    ):
        asyncio.run(mobile_iam.authenticate_mobile_request(request))
    assert result.value.status_code == status
    assert "secret token" not in caplog.text
    assert "private-credential" not in caplog.text
    assert "secret token" not in str(result.value.detail)


def test_blank_provider_identity_is_service_error_and_is_not_cached(monkeypatch):
    async def authenticate(_authorization):
        return _principal(org_id=" ")

    monkeypatch.setattr(
        mobile_iam,
        "get_mobile_iam_provider",
        lambda: SimpleNamespace(authenticate=authenticate),
    )
    request = Request({"type": "http", "headers": []})
    with pytest.raises(HTTPException) as result:
        asyncio.run(mobile_iam.authenticate_mobile_request(request))
    assert result.value.status_code == 503
    assert "deerflow.authenticated_iam" not in request.scope


@pytest.mark.parametrize(
    "failure,status",
    [(PermissionError("secret token"), 403), (RuntimeError("secret token"), 503)],
)
def test_authorization_errors_preserve_status_without_logging_exception_secrets(
    caplog, failure, status
):
    async def authorize(_principal, _request):
        raise failure

    provider = SimpleNamespace(authorize=authorize)
    principal = mobile_iam.MobileIAMPrincipal.model_validate(_principal())
    with (
        caplog.at_level("ERROR", logger=mobile_iam.__name__),
        pytest.raises(HTTPException) as result,
    ):
        asyncio.run(
            mobile_iam.authorize_mobile_request(
                provider, principal, _authorization_input()
            )
        )
    assert result.value.status_code == status
    assert "secret token" not in caplog.text
    assert "secret token" not in str(result.value.detail)


def test_blank_authoritative_resource_is_service_error():
    async def authorize(_principal, _request):
        return _decision(_resource(owner_org_id=" "))

    provider = SimpleNamespace(authorize=authorize)
    principal = mobile_iam.MobileIAMPrincipal.model_validate(_principal())
    with pytest.raises(HTTPException) as result:
        asyncio.run(
            mobile_iam.authorize_mobile_request(
                provider, principal, _authorization_input()
            )
        )
    assert result.value.status_code == 503


def test_caller_headers_cannot_inject_scope_authentication_cache(monkeypatch):
    credentials = []

    async def authenticate(authorization):
        credentials.append(authorization)
        raise PermissionError("Denied")

    monkeypatch.setattr(
        mobile_iam,
        "get_mobile_iam_provider",
        lambda: SimpleNamespace(authenticate=authenticate),
    )
    request = Request(
        {
            "type": "http",
            "headers": [
                (b"authorization", b"Bearer invalid"),
                (b"deerflow.authenticated_iam", b'{"user_id":"admin","org_id":"org"}'),
                (b"_deerflow_thread_authorizations", b"any"),
            ],
        }
    )
    with pytest.raises(HTTPException) as result:
        asyncio.run(mobile_iam.authenticate_mobile_request(request))
    assert result.value.status_code == 401
    assert credentials == ["Bearer invalid"]
    assert "deerflow.authenticated_iam" not in request.scope
