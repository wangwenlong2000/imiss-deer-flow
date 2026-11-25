"""Server-side IAM boundary for mobile/OneCity gateway requests.

The concrete platform integration is supplied as a gateway plugin because the
OneCity tenant's authentication and IAM APIs are deployment-specific. A plugin
must authenticate the caller and perform an authorization check against the
authoritative resource catalogue before returning a trusted decision.
"""

from __future__ import annotations

import importlib
import inspect
import logging
import os
from collections.abc import Mapping
from typing import Any, Literal, Protocol

from fastapi import HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, StrictBool, field_validator

from deerflow.compliance.manual_context import MANUAL_COMPLIANCE_PROFILES

logger = logging.getLogger(__name__)

IDENTITY_CLASSES = tuple(MANUAL_COMPLIANCE_PROFILES)

OperationType = Literal["analysis", "upload", "export", "share", "public_release", "research"]
TargetAudience = Literal["self", "department", "organization", "external_org", "public", "research_group"]
ResourceClassification = Literal["public", "internal", "confidential", "restricted"]


def _nonblank_identifier(value: str | None) -> str | None:
    # Validate without normalizing: whitespace around a real identifier may be
    # meaningful upstream and must not make distinct accounts collide locally.
    if value is not None and not value.strip():
        raise ValueError("IAM identifiers must not be blank")
    return value


class MobileIAMPrincipal(BaseModel):
    """Authenticated actor attributes returned by the configured IAM plugin."""

    user_id: str = Field(min_length=1)
    org_id: str = Field(min_length=1)
    department_id: str | None = None
    identity_class: str
    roles: list[str] = Field(default_factory=list)
    permissions: list[str] = Field(default_factory=list)
    # Request-scoped credential for IAM providers that need to authorize a
    # resource against the same upstream identity service. It is excluded
    # from serialization and must never be copied into graph state or logs.
    iam_credential: str | None = Field(default=None, exclude=True, repr=False)

    model_config = ConfigDict(extra="forbid", frozen=True)

    @field_validator("user_id", "org_id", "department_id")
    @classmethod
    def validate_identifier(cls, value: str | None) -> str | None:
        return _nonblank_identifier(value)

    @field_validator("identity_class")
    @classmethod
    def validate_identity_class(cls, value: str) -> str:
        if value not in IDENTITY_CLASSES:
            raise ValueError("identity_class must use a compliance-defined identity profile")
        return value


class MobileAuthorizationInput(BaseModel):
    """Untrusted operation/resource hints for an authoritative IAM lookup."""

    request_id: str
    turn_id: str
    conversation_id: str
    operation_type: str | None = None
    target_audience: str | None = None
    resource_id: str | None = None
    target_org_id: str | None = None

    model_config = ConfigDict(extra="forbid", frozen=True)


class MobileAuthorizedOperation(BaseModel):
    type: OperationType
    target_audience: TargetAudience

    model_config = ConfigDict(extra="forbid", frozen=True)


class MobileAuthorizedResource(BaseModel):
    resource_id: str = Field(min_length=1)
    owner_user_id: str | None = None
    owner_org_id: str = Field(min_length=1)
    owner_department_id: str | None = None
    target_org_id: str | None = None
    classification: ResourceClassification
    is_anonymized: StrictBool = False

    model_config = ConfigDict(extra="forbid", frozen=True)

    @field_validator("resource_id", "owner_user_id", "owner_org_id", "owner_department_id", "target_org_id")
    @classmethod
    def validate_identifier(cls, value: str | None) -> str | None:
        return _nonblank_identifier(value)


class MobileAuthorizationDecision(BaseModel):
    """Authoritative operation, resource metadata, and permission result."""

    allowed: StrictBool
    permission_verified: StrictBool
    operation: MobileAuthorizedOperation
    resource: MobileAuthorizedResource

    model_config = ConfigDict(extra="forbid", frozen=True)


class MobileIAMProvider(Protocol):
    """Contract implemented by the deployment's authenticated IAM adapter.

    ``authenticate`` must validate the supplied Authorization credential with
    the platform. ``authorize`` must query/verify access to the requested
    operation and resolve resource ownership/classification from an authoritative
    catalogue. It must not copy caller-provided role, permission, owner, or
    classification claims into its result without verification.
    """

    async def authenticate(self, authorization_header: str | None) -> MobileIAMPrincipal | Mapping[str, Any]: ...

    async def authorize(
        self,
        principal: MobileIAMPrincipal,
        request: MobileAuthorizationInput,
    ) -> MobileAuthorizationDecision | Mapping[str, Any]: ...


class MobileIAMConfigurationError(RuntimeError):
    pass


_provider_target: str | None = None
_provider: MobileIAMProvider | None = None


def mobile_iam_required() -> bool:
    """Return whether mobile API calls require the IAM plugin and a valid token."""
    configured = os.getenv("MOBILE_IAM_REQUIRED", "false").strip().lower()
    if configured in {"1", "true", "yes", "on"}:
        return True
    if configured not in {"0", "false", "no", "off", ""}:
        raise MobileIAMConfigurationError("MOBILE_IAM_REQUIRED must be a boolean value")

    # Trusted scene resolution without a server-side IAM boundary is invalid.
    try:
        from deerflow.config.compliance_config import get_compliance_config

        return getattr(get_compliance_config(), "scene_resolver_mode", None) == "trusted_upstream"
    except Exception:
        return False


def get_mobile_iam_provider() -> MobileIAMProvider | None:
    """Load a configured ``module.path:Provider`` without logging credentials."""
    global _provider_target, _provider

    target = os.getenv("MOBILE_IAM_PROVIDER", "").strip()
    if not target:
        return None
    if target == _provider_target and _provider is not None:
        return _provider

    module_path, separator, attribute = target.partition(":")
    if not separator or not module_path or not attribute:
        raise MobileIAMConfigurationError("MOBILE_IAM_PROVIDER must use module.path:ProviderName")

    try:
        factory: Any = getattr(importlib.import_module(module_path), attribute)
        provider = factory() if inspect.isclass(factory) or not hasattr(factory, "authenticate") else factory
    except Exception as exc:
        raise MobileIAMConfigurationError("Unable to initialize the configured mobile IAM provider") from exc

    if not callable(getattr(provider, "authenticate", None)) or not callable(getattr(provider, "authorize", None)):
        raise MobileIAMConfigurationError("Mobile IAM provider must implement authenticate() and authorize()")

    _provider_target = target
    _provider = provider
    return provider


async def authenticate_mobile_request(request: Request) -> tuple[MobileIAMProvider | None, MobileIAMPrincipal | None]:
    """Authenticate a mobile caller, preserving a development-only no-IAM mode."""
    cached = request.scope.get("deerflow.authenticated_iam")
    if cached is not None:
        return cached
    try:
        provider = get_mobile_iam_provider()
    except MobileIAMConfigurationError as exc:
        logger.error("Mobile IAM provider configuration is invalid (%s)", type(exc).__name__)
        raise HTTPException(status_code=503, detail="Mobile IAM provider is unavailable") from exc

    if provider is None:
        try:
            required = mobile_iam_required()
        except MobileIAMConfigurationError as exc:
            raise HTTPException(status_code=503, detail="Mobile IAM configuration is invalid") from exc
        if required:
            raise HTTPException(status_code=503, detail="Mobile IAM is required but no provider is configured")
        return None, None

    try:
        raw_principal = await provider.authenticate(request.headers.get("authorization"))
        principal = MobileIAMPrincipal.model_validate(raw_principal)
    except PermissionError as exc:
        raise HTTPException(status_code=401, detail="Mobile authentication failed") from exc
    except Exception as exc:
        logger.error("Mobile IAM authentication failed (%s)", type(exc).__name__)
        raise HTTPException(status_code=503, detail="Mobile IAM authentication is unavailable") from exc

    request.scope["deerflow.authenticated_iam"] = (provider, principal)
    return provider, principal


async def authorize_mobile_request(
    provider: MobileIAMProvider,
    principal: MobileIAMPrincipal,
    request: MobileAuthorizationInput,
) -> MobileAuthorizationDecision:
    """Require an explicit positive, server-verified permission decision."""
    try:
        raw_decision = await provider.authorize(principal, request)
        decision = MobileAuthorizationDecision.model_validate(raw_decision)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail="Mobile operation is not authorized") from exc
    except Exception as exc:
        logger.error("Mobile IAM authorization lookup failed (%s)", type(exc).__name__)
        raise HTTPException(status_code=503, detail="Mobile IAM authorization is unavailable") from exc

    if decision.allowed is not True or decision.permission_verified is not True:
        raise HTTPException(status_code=403, detail="Mobile operation is not authorized")
    if request.resource_id and decision.resource.resource_id != request.resource_id:
        raise HTTPException(status_code=403, detail="Mobile operation is not authorized")
    if request.operation_type and decision.operation.type != request.operation_type:
        raise HTTPException(status_code=403, detail="Mobile operation is not authorized")
    if request.target_audience and decision.operation.target_audience != request.target_audience:
        raise HTTPException(status_code=403, detail="Mobile operation is not authorized")
    if request.target_org_id and decision.resource.target_org_id != request.target_org_id:
        raise HTTPException(status_code=403, detail="Mobile operation is not authorized")
    return decision


def build_trusted_compliance_request(
    *,
    principal: MobileIAMPrincipal,
    decision: MobileAuthorizationDecision,
    request_id: str,
    thread_id: str,
    turn_id: str,
) -> dict[str, Any]:
    """Create the canonical trusted contract from verified server-side facts."""
    return {
        "request_id": request_id,
        "thread_id": thread_id,
        "turn_id": turn_id,
        "trusted_source": "trusted_upstream",
        "permission_verified": True,
        "actor": principal.model_dump(mode="json"),
        "operation": decision.operation.model_dump(mode="json"),
        "resource": decision.resource.model_dump(mode="json"),
    }


__all__ = [
    "IDENTITY_CLASSES",
    "MobileAuthorizationDecision",
    "MobileAuthorizationInput",
    "MobileIAMConfigurationError",
    "MobileIAMPrincipal",
    "MobileIAMProvider",
    "authorize_mobile_request",
    "authenticate_mobile_request",
    "build_trusted_compliance_request",
    "get_mobile_iam_provider",
    "mobile_iam_required",
]
