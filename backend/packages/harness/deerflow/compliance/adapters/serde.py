"""JSON (de)serialization for out-of-process detector adapters.

``subprocess_cli`` and ``http_service`` detectors exchange exactly these shapes;
they are the runtime counterpart of the JSON Schema exported by
``contract.export_json_schema()``, so a detector written in another language can
be validated against the schema and will interoperate here without adjustment.

Every inbound hit goes through ``contract.validate_hit`` — a detector on the
other side of a pipe is untrusted input, and a bad ``violation_type`` reaching
the policy matrix would be a security problem, not a typo.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from deerflow.compliance.contract import (
    ContractError,
    DetectContext,
    DetectionHit,
    DetectionUnit,
    IntentInfo,
    RiskLocation,
    UserContext,
    validate_hit,
)


def unit_to_json(unit: DetectionUnit) -> dict[str, Any]:
    return {
        "unit_id": unit.unit_id,
        "gate": unit.gate,
        "data_type": unit.data_type,
        "text_items": [{"item_id": t.item_id, "text": t.text, "source": t.source} for t in unit.text_items],
        "field_items": [{"item_id": f.item_id, "path": f.path, "value": f.value, "source": f.source} for f in unit.field_items],
        "raw": dict(unit.raw),
    }


def ctx_to_json(ctx: DetectContext) -> dict[str, Any]:
    user: dict[str, Any] | None = None
    if ctx.user is not None:
        user = {"user_id": ctx.user.user_id, "roles": list(ctx.user.roles), "org_id": ctx.user.org_id}
    intent: dict[str, Any] | None = None
    if ctx.intent is not None:
        intent = {
            "intent": ctx.intent.intent,
            "scene_hint": ctx.intent.scene_hint,
            "high_risk": ctx.intent.high_risk,
            "confidence": ctx.intent.confidence,
            "intent_type": ctx.intent.intent_type,
            "requested_operation": ctx.intent.requested_operation,
            "risk_level": ctx.intent.risk_level,
            "reason_codes": list(ctx.intent.reason_codes),
            "reason": ctx.intent.reason,
            "source": ctx.intent.source,
        }
    return {
        "gate": ctx.gate,
        "scenes": list(ctx.scenes),
        "user": user,
        "intent": intent,
        "model_name": ctx.model_name,
        "budget_ms": ctx.budget_ms,
    }


def request_to_json(unit: DetectionUnit, ctx: DetectContext, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {"unit": unit_to_json(unit), "ctx": ctx_to_json(ctx)}
    if params:
        payload["params"] = dict(params)
    return payload


def _risk_location_from_json(data: Any) -> RiskLocation:
    if not isinstance(data, dict):
        raise ContractError(f"risk_location must be an object, got {type(data).__name__}")
    kind = data.get("kind")
    if kind not in ("field_path", "char_span", "frame", "bbox", "coordinate"):
        raise ContractError(f"unknown risk_location kind: {kind!r}")
    return RiskLocation(
        kind=kind,
        locator=str(data.get("locator", "")),
        text=data.get("text"),
        entity_type=data.get("entity_type"),
    )


def hit_from_json(data: Any) -> DetectionHit:
    """Build and validate a ``DetectionHit`` from an untrusted JSON object."""
    if not isinstance(data, dict):
        raise ContractError(f"hit must be an object, got {type(data).__name__}")
    try:
        hit = DetectionHit(
            detector_id=str(data["detector_id"]),
            violation_type=data["violation_type"],
            confidence=float(data["confidence"]),
            severity=data["severity"],
            risk_locations=tuple(_risk_location_from_json(loc) for loc in (data.get("risk_locations") or ())),
            reason_code=str(data.get("reason_code") or ""),
            evidence=dict(data.get("evidence") or {}),
            basis=tuple(str(item) for item in (data.get("basis") or ())),
        )
    except KeyError as exc:
        raise ContractError(f"hit is missing required field: {exc}") from exc
    except (TypeError, ValueError) as exc:
        raise ContractError(f"hit has a malformed field: {exc}") from exc
    return validate_hit(hit)


def hits_from_json(payload: Any) -> tuple[DetectionHit, ...]:
    """Parse a detector response body into validated hits.

    Accepts either ``{"hits": [...]}`` or a bare ``[...]`` so that trivial CLI
    detectors do not have to wrap their output.
    """
    if isinstance(payload, dict):
        raw_hits = payload.get("hits")
        if raw_hits is None:
            raise ContractError("response object has no `hits` key")
    elif isinstance(payload, list):
        raw_hits = payload
    else:
        raise ContractError(f"response must be an object or array, got {type(payload).__name__}")

    if not isinstance(raw_hits, list):
        raise ContractError(f"`hits` must be an array, got {type(raw_hits).__name__}")
    return tuple(hit_from_json(item) for item in raw_hits)


def hit_to_json(hit: DetectionHit) -> dict[str, Any]:
    """Serialize a hit — used by CLI detectors and by the audit log."""
    return {
        "detector_id": hit.detector_id,
        "violation_type": hit.violation_type,
        "confidence": float(hit.confidence),
        "severity": hit.severity,
        "risk_locations": [
            {"kind": loc.kind, "locator": loc.locator, "text": loc.text, "entity_type": loc.entity_type} for loc in hit.risk_locations
        ],
        "reason_code": hit.reason_code,
        "evidence": dict(hit.evidence),
        "basis": list(hit.basis),
    }


def unit_from_json(data: Mapping[str, Any]) -> DetectionUnit:
    """Inverse of :func:`unit_to_json` — for detector-side CLI helpers."""
    from deerflow.compliance.contract import FieldItem, TextItem

    return DetectionUnit(
        unit_id=str(data.get("unit_id", "")),
        gate=data["gate"],
        data_type=data.get("data_type"),
        text_items=tuple(TextItem(item_id=str(t["item_id"]), text=str(t["text"]), source=str(t.get("source", ""))) for t in (data.get("text_items") or ())),
        field_items=tuple(FieldItem(item_id=str(f["item_id"]), path=str(f["path"]), value=f.get("value"), source=str(f.get("source", ""))) for f in (data.get("field_items") or ())),
        raw=dict(data.get("raw") or {}),
    )


def ctx_from_json(data: Mapping[str, Any]) -> DetectContext:
    """Inverse of :func:`ctx_to_json` — for detector-side CLI helpers."""
    user_data = data.get("user")
    intent_data = data.get("intent")
    return DetectContext(
        gate=data["gate"],
        scenes=tuple(data.get("scenes") or ()),
        user=UserContext(user_id=user_data.get("user_id"), roles=tuple(user_data.get("roles") or ()), org_id=user_data.get("org_id")) if isinstance(user_data, dict) else None,
        intent=IntentInfo(
            intent=intent_data.get("intent"),
            scene_hint=intent_data.get("scene_hint"),
            high_risk=bool(intent_data.get("high_risk")),
            confidence=float(intent_data.get("confidence") or 0.0),
            intent_type=str(intent_data.get("intent_type") or "unknown"),
            requested_operation=str(intent_data.get("requested_operation") or "unknown"),
            risk_level=str(intent_data.get("risk_level") or "unknown"),
            reason_codes=tuple(str(code) for code in (intent_data.get("reason_codes") or ())),
            reason=str(intent_data.get("reason") or ""),
            source=str(intent_data.get("source") or "legacy"),
        )
        if isinstance(intent_data, dict)
        else None,
        model_name=str(data["model_name"]) if data.get("model_name") else None,
        budget_ms=int(data.get("budget_ms") or 400),
    )


__all__ = [
    "ctx_from_json",
    "ctx_to_json",
    "hit_from_json",
    "hit_to_json",
    "hits_from_json",
    "request_to_json",
    "unit_from_json",
    "unit_to_json",
]
