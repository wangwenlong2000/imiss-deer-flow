"""Trusted usage-scene resolution for all compliance gates.

``scene_hint`` is never authoritative.  The resolver derives a scene from the
actor, requested operation, audience, resource ownership/classification and
the canonical compliance intent.  Incomplete or contradictory context stays
``_unknown`` so policy cannot accidentally fall back to ``self_use``.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from typing import Any, Protocol, runtime_checkable

from deerflow.compliance.contract import SCENES, UNKNOWN_SCENE_KEY, Scene
from deerflow.compliance.types import DetectionRequest

logger = logging.getLogger(__name__)

COMPLIANCE_REQUEST_KEY = "compliance_request"
SCENE_CONTEXT_KEY = "scene_context"


@dataclass(frozen=True)
class ActorContext:
    user_id: str | None = None
    org_id: str | None = None
    department_id: str | None = None
    roles: tuple[str, ...] = ()
    permissions: tuple[str, ...] = ()


@dataclass(frozen=True)
class OperationContext:
    type: str = "unknown"
    target_audience: str = "unknown"


@dataclass(frozen=True)
class ResourceContext:
    resource_id: str | None = None
    owner_user_id: str | None = None
    owner_org_id: str | None = None
    owner_department_id: str | None = None
    target_org_id: str | None = None
    classification: str = "unknown"
    is_anonymized: bool = False


@dataclass(frozen=True)
class ComplianceRequestContext:
    request_id: str | None = None
    thread_id: str | None = None
    turn_id: str | None = None
    trusted_source: str | None = None
    permission_verified: bool = False
    actor: ActorContext = field(default_factory=ActorContext)
    operation: OperationContext = field(default_factory=OperationContext)
    resource: ResourceContext = field(default_factory=ResourceContext)
    scene_hint: str | None = None
    intent: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SceneResolution:
    scene: str = UNKNOWN_SCENE_KEY
    confidence: float = 0.0
    source: str = "null_resolver"
    reason_codes: tuple[str, ...] = ()
    fallback: bool = True
    permission_checked: bool = False
    scene_hint: str | None = None
    request_id: str | None = None
    binding_hash: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "scene": self.scene,
            "confidence": self.confidence,
            "source": self.source,
            "reason_codes": list(self.reason_codes),
            "fallback": self.fallback,
            "permission_checked": self.permission_checked,
            "scene_hint": self.scene_hint,
            "request_id": self.request_id,
            "binding_hash": self.binding_hash,
        }


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def parse_compliance_request(raw: Any) -> ComplianceRequestContext:
    """Parse the OneCity/DeerFlow request contract without trusting its hint."""
    data = _mapping(raw)
    actor = _mapping(data.get("actor"))
    operation = _mapping(data.get("operation"))
    resource = _mapping(data.get("resource"))
    roles = actor.get("roles") if isinstance(actor.get("roles"), (list, tuple)) else ()
    permissions = actor.get("permissions") if isinstance(actor.get("permissions"), (list, tuple)) else ()
    return ComplianceRequestContext(
        request_id=str(data["request_id"]) if data.get("request_id") else None,
        thread_id=str(data["thread_id"]) if data.get("thread_id") else None,
        turn_id=str(data["turn_id"]) if data.get("turn_id") else None,
        trusted_source=str(data["trusted_source"]) if data.get("trusted_source") else None,
        permission_verified=data.get("permission_verified") is True,
        actor=ActorContext(
            user_id=str(actor["user_id"]) if actor.get("user_id") else None,
            org_id=str(actor["org_id"]) if actor.get("org_id") else None,
            department_id=str(actor["department_id"]) if actor.get("department_id") else None,
            roles=tuple(str(item) for item in roles),
            permissions=tuple(str(item) for item in permissions),
        ),
        operation=OperationContext(
            type=str(operation.get("type") or "unknown"),
            target_audience=str(operation.get("target_audience") or "unknown"),
        ),
        resource=ResourceContext(
            resource_id=str(resource["resource_id"]) if resource.get("resource_id") else None,
            owner_user_id=str(resource["owner_user_id"]) if resource.get("owner_user_id") else None,
            owner_org_id=str(resource["owner_org_id"]) if resource.get("owner_org_id") else None,
            owner_department_id=str(resource["owner_department_id"]) if resource.get("owner_department_id") else None,
            target_org_id=str(resource["target_org_id"]) if resource.get("target_org_id") else None,
            classification=str(resource.get("classification") or "unknown"),
            is_anonymized=resource.get("is_anonymized") is True,
        ),
        scene_hint=str(data["scene_hint"]) if data.get("scene_hint") else None,
        intent=dict(_mapping(data.get("intent"))),
    )


def compliance_request_from_state(state: Mapping[str, Any] | None) -> dict[str, Any]:
    """Return request facts with canonical recognized intent overlaid."""
    raw = dict(_mapping((state or {}).get(COMPLIANCE_REQUEST_KEY)))
    intent_context = _mapping((state or {}).get("intent_context"))
    canonical = _mapping(intent_context.get("compliance_intent"))
    if canonical:
        raw["intent"] = dict(canonical)
    return raw


def resolve_scene_from_state(
    state: Mapping[str, Any] | None,
    *,
    force: bool = False,
) -> tuple[dict[str, Any], SceneResolution]:
    """Resolve once at InputGate and reuse that result at downstream gates."""
    raw = compliance_request_from_state(state)
    existing = _mapping((state or {}).get(SCENE_CONTEXT_KEY))
    if not force and _can_reuse_trusted_scene(raw, existing):
        scene = str(existing.get("scene") or UNKNOWN_SCENE_KEY)
        if scene in (*SCENES, UNKNOWN_SCENE_KEY):
            return raw, SceneResolution(
                scene=scene,
                confidence=float(existing.get("confidence") or 0.0),
                source="trusted_upstream",
                reason_codes=tuple(str(item) for item in (existing.get("reason_codes") or ())),
                fallback=bool(existing.get("fallback", scene == UNKNOWN_SCENE_KEY)),
                permission_checked=bool(existing.get("permission_checked")),
                scene_hint=str(existing["scene_hint"]) if existing.get("scene_hint") else None,
                request_id=str(existing["request_id"]) if existing.get("request_id") else None,
                binding_hash=str(existing["binding_hash"]) if existing.get("binding_hash") else None,
            )
    if _trusted_scene_mode_enabled() and _trusted_upstream_context(parse_compliance_request(raw)):
        return raw, TrustedSceneResolver().resolve_context(raw)
    return raw, NullSceneResolver().resolve_context(raw)


def _can_reuse_trusted_scene(raw: Mapping[str, Any], existing: Mapping[str, Any]) -> bool:
    """Only reuse a scene bound to the exact authenticated upstream request."""
    if existing.get("source") != "trusted_upstream" or existing.get("permission_checked") is not True:
        return False
    ctx = parse_compliance_request(raw)
    required = (
        ctx.request_id,
        ctx.thread_id,
        ctx.turn_id,
        ctx.actor.user_id,
        ctx.actor.org_id,
        ctx.operation.type,
        ctx.resource.resource_id,
    )
    return (
        all(required)
        and ctx.trusted_source in {"onecity_iam", "trusted_upstream"}
        and ctx.permission_verified
        and str(existing.get("request_id") or "") == ctx.request_id
        and str(existing.get("binding_hash") or "") == _request_binding_hash(ctx)
    )


def _request_binding_hash(ctx: ComplianceRequestContext) -> str:
    """Hash the complete request binding used to authorize a cached scene."""
    binding = {
        "request_id": ctx.request_id,
        "thread_id": ctx.thread_id,
        "turn_id": ctx.turn_id,
        "trusted_source": ctx.trusted_source,
        "permission_verified": ctx.permission_verified,
        "actor": asdict(ctx.actor),
        "operation": asdict(ctx.operation),
        "resource": asdict(ctx.resource),
        "scene_hint": ctx.scene_hint,
        "intent": dict(ctx.intent),
    }
    encoded = json.dumps(binding, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()


def _trusted_scene_mode_enabled() -> bool:
    """Read the explicit opt-in without importing config at module load time."""
    try:
        from deerflow.config.compliance_config import get_compliance_config

        return (getattr(get_compliance_config(), "scene_resolver_mode", None) or "null") == "trusted_upstream"
    except Exception:
        return False


def scene_origin_from_state(
    state: Mapping[str, Any] | None,
    *,
    force: bool = False,
) -> tuple[dict[str, Any], SceneResolution]:
    raw, resolution = resolve_scene_from_state(state, force=force)
    return scene_audit_context(raw, resolution), resolution


class TrustedSceneResolver:
    """Resolve a policy scene from server-side request facts."""

    _KNOWN_CLASSIFICATIONS = {"public", "internal", "confidential", "restricted"}
    _INTERNAL_OPERATIONS = {"analysis", "upload", "share"}

    def resolve_context(self, raw: Any) -> SceneResolution:
        ctx = parse_compliance_request(raw)
        actor, operation, resource = ctx.actor, ctx.operation, ctx.resource
        trusted = _trusted_upstream_context(ctx)
        intent_type = str(ctx.intent.get("intent_type") or "unknown")
        base = {
            "source": "trusted_upstream" if trusted else "null_resolver",
            "scene_hint": ctx.scene_hint,
            "request_id": ctx.request_id,
            "binding_hash": _request_binding_hash(ctx),
        }

        # A scene is an authorization result, not a client hint. Until a
        # server-side IAM decision is explicitly bound to this request, all
        # real requests remain in the conservative _unknown matrix column.
        if not trusted:
            missing = ["trusted_upstream_context_missing"]
            if not ctx.request_id:
                missing.append("request_id_missing")
            if not ctx.thread_id:
                missing.append("thread_id_missing")
            if not ctx.turn_id:
                missing.append("turn_id_missing")
            if not ctx.permission_verified:
                missing.append("permission_verification_missing")
            return SceneResolution(reason_codes=tuple(missing), **base)

        # Public disclosure is deliberately first: no claimed private scene can
        # soften it.  It is safe to resolve this strict scene even when identity
        # facts are incomplete.
        if operation.type == "public_release" or operation.target_audience == "public" or intent_type == "public_release":
            reasons = ["public_release_priority"]
            if ctx.scene_hint and ctx.scene_hint != "public_release":
                reasons.append("scene_hint_conflict")
            return SceneResolution(
                scene="public_release",
                confidence=0.99,
                reason_codes=tuple(reasons),
                fallback=False,
                permission_checked=True,
                **base,
            )

        # Crossing an organisation boundary also resolves to the stricter scene
        # before considering a caller-supplied hint.
        crosses_org = (
            operation.target_audience == "external_org"
            or (resource.target_org_id is not None and actor.org_id is not None and resource.target_org_id != actor.org_id)
            or (actor.org_id is not None and resource.owner_org_id is not None and actor.org_id != resource.owner_org_id)
            or operation.type in {"cross_org", "external_share"}
        )
        if crosses_org:
            reasons = ["organization_boundary_crossed"]
            if ctx.scene_hint and ctx.scene_hint != "cross_org":
                reasons.append("scene_hint_conflict")
            return SceneResolution(
                scene="cross_org",
                confidence=0.97,
                reason_codes=tuple(reasons),
                fallback=False,
                permission_checked=True,
                **base,
            )

        missing = []
        if not actor.user_id or not actor.org_id:
            missing.append("actor_identity_missing")
        if not resource.owner_org_id or not resource.classification or resource.classification == "unknown":
            missing.append("resource_ownership_or_classification_missing")
        if operation.type == "unknown" or operation.target_audience == "unknown":
            missing.append("operation_missing")
        if resource.classification not in self._KNOWN_CLASSIFICATIONS:
            missing.append("classification_invalid")
        if missing:
            return SceneResolution(reason_codes=tuple(dict.fromkeys(missing)), **base)

        if operation.type == "research":
            research_authorized = any(
                "research" in item.lower() or "statistic" in item.lower()
                for item in (*actor.roles, *actor.permissions)
            )
            if (
                operation.target_audience == "research_group"
                and resource.is_anonymized
                and research_authorized
            ):
                return SceneResolution(
                    scene="research_anon",
                    confidence=0.96,
                    reason_codes=("research_permission_verified", "resource_anonymized"),
                    fallback=False,
                    permission_checked=True,
                    **base,
                )
            return SceneResolution(
                reason_codes=("research_anonymization_or_permission_missing",),
                permission_checked=True,
                **base,
            )

        if (
            operation.target_audience == "self"
            and actor.user_id == resource.owner_user_id
            and operation.type not in {"public_release", "share", "export"}
            and resource.classification != "restricted"
        ):
            reasons = ["owner_user_match", "permission_verified"]
            if ctx.scene_hint and ctx.scene_hint != "self_use":
                reasons.append("scene_hint_conflict")
            return SceneResolution(
                scene="self_use",
                confidence=0.96,
                reason_codes=tuple(reasons),
                fallback=False,
                permission_checked=True,
                **base,
            )

        same_org = actor.org_id == resource.owner_org_id
        internal_audience = operation.target_audience in {"department", "organization"}
        if same_org and internal_audience and operation.type in self._INTERNAL_OPERATIONS:
            reasons = ["organization_match", "permission_verified"]
            if actor.department_id and resource.owner_department_id and actor.department_id != resource.owner_department_id:
                reasons.append("cross_department_same_org")
            if ctx.scene_hint and ctx.scene_hint != "internal_org":
                reasons.append("scene_hint_conflict")
            return SceneResolution(
                scene="internal_org",
                confidence=0.95,
                reason_codes=tuple(reasons),
                fallback=False,
                permission_checked=True,
                **base,
            )

        reasons = ["scene_not_proven"]
        if ctx.scene_hint:
            reasons.append("scene_hint_unverified")
        return SceneResolution(reason_codes=tuple(reasons), permission_checked=True, **base)

    def resolve(self, request: DetectionRequest) -> Scene | None:
        existing = _mapping(request.origin.get("scene_resolution"))
        raw = _mapping(request.origin.get(COMPLIANCE_REQUEST_KEY))
        parsed = parse_compliance_request(raw)
        scene = str(existing.get("scene") or "")
        if (
            scene in SCENES
            and _trusted_scene_mode_enabled()
            and _can_reuse_trusted_scene(raw, existing)
            and request.thread_id == parsed.thread_id
        ):
            return scene  # type: ignore[return-value]
        result = self.resolve_context(raw)
        return result.scene if result.scene in SCENES else None  # type: ignore[return-value]


@runtime_checkable
class SceneResolver(Protocol):
    """Maps a request onto a usage scene, or ``None`` when it cannot tell."""

    def resolve(self, request: DetectionRequest) -> Scene | None: ...


class NullSceneResolver:
    """Explicit test/fallback resolver. Always undecided."""

    def resolve(self, request: DetectionRequest) -> Scene | None:  # noqa: ARG002
        return None

    def resolve_context(self, raw: Any) -> SceneResolution:  # noqa: ARG002
        return SceneResolution(source="null_resolver", reason_codes=("null_resolver",))


def _trusted_upstream_context(ctx: ComplianceRequestContext) -> bool:
    """Check the minimum server/IAM binding required for scene trust."""
    return bool(
        ctx.trusted_source in {"onecity_iam", "trusted_upstream"}
        and ctx.permission_verified
        and ctx.request_id
        and ctx.thread_id
        and ctx.turn_id
        and ctx.actor.user_id
        and ctx.actor.org_id
        and ctx.operation.type != "unknown"
        and ctx.resource.resource_id
        and ctx.resource.owner_org_id
    )


def resolve_scene_key(
    resolver: SceneResolver | None,
    request: DetectionRequest,
    fallback_key: str = UNKNOWN_SCENE_KEY,
) -> tuple[str, tuple[Scene, ...]]:
    """Return ``(matrix_column_key, scenes_tuple)`` for *request*."""
    if resolver is None:
        return fallback_key, ()
    try:
        scene = resolver.resolve(request)
    except Exception:
        logger.exception("SceneResolver failed; falling back to %s", fallback_key)
        return fallback_key, ()
    if not scene or scene not in SCENES:
        return fallback_key, ()
    return scene, (scene,)


def scene_audit_context(raw: Any, resolution: SceneResolution) -> dict[str, Any]:
    """Return bounded, structured scene facts for request origin/audit."""
    ctx = parse_compliance_request(raw)
    return {
        COMPLIANCE_REQUEST_KEY: {
            "request_id": ctx.request_id,
            "thread_id": ctx.thread_id,
            "turn_id": ctx.turn_id,
            "trusted_source": ctx.trusted_source,
            "permission_verified": ctx.permission_verified,
            "actor": asdict(ctx.actor),
            "operation": asdict(ctx.operation),
            "resource": asdict(ctx.resource),
            "scene_hint": ctx.scene_hint,
            "intent": dict(ctx.intent),
        },
        "scene_resolution": resolution.to_dict(),
    }


__all__ = [
    "ActorContext",
    "COMPLIANCE_REQUEST_KEY",
    "ComplianceRequestContext",
    "NullSceneResolver",
    "OperationContext",
    "ResourceContext",
    "SCENE_CONTEXT_KEY",
    "SceneResolution",
    "SceneResolver",
    "TrustedSceneResolver",
    "compliance_request_from_state",
    "parse_compliance_request",
    "resolve_scene_key",
    "resolve_scene_from_state",
    "scene_origin_from_state",
    "scene_audit_context",
]
