"""Detector contract — the only module a detector author needs to import.

This module is deliberately **standard-library only**. A detector that runs in a
slim virtualenv, in a subprocess, or in another language (via the exported JSON
Schema) must be able to satisfy this contract without pulling in the engine, its
configuration system, or any third-party package.

Stability rules
---------------
* ``CONTRACT_VERSION`` is versioned independently from the engine. A detector
  declares the version it was written against in its ``manifest.yaml``; the
  registry refuses to load a detector whose major version does not match.
* Everything crossing the boundary is a **frozen dataclass** with tuple-typed
  collections. Detectors receive read-only data and cannot mutate engine state.
* There is deliberately **no ``tech`` field**. How a detector reaches its verdict
  (regex, dictionary, model, LLM) is its own business; the engine routes purely
  on ``violation_type x gate x data_type x cost``.

What a detector must NOT do
---------------------------
* Do not decide disposition. Emit *what was hit, where, how confident, on what
  basis*; the ``violation x scene`` matrix decides what happens next.
* Do not depend on ``DetectContext.scenes``. It is permanently empty in phase 1.
* Do not import anything else from ``deerflow.compliance``.
"""

from __future__ import annotations

import collections.abc
import dataclasses
import types
import typing
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, runtime_checkable

CONTRACT_VERSION = "1.0"

# ── Enumerations ─────────────────────────────────────────────────────────────

Gate = Literal["InputGate", "ContextGate", "OutputGate"]
GATES: tuple[str, ...] = ("InputGate", "ContextGate", "OutputGate")

#: The ten violation types from the annotation guide (7.23), section 1.2.1.
ViolationType = Literal[
    "struct_id",
    "geo_loc",
    "hardcoded_cred",
    "illegal_content",
    "political",
    "text_id",
    "confidential",
    "video_meta_leak",
    "re_identify",
    "domain",
]
VIOLATION_TYPES: tuple[str, ...] = (
    "struct_id",
    "geo_loc",
    "hardcoded_cred",
    "illegal_content",
    "political",
    "text_id",
    "confidential",
    "video_meta_leak",
    "re_identify",
    "domain",
)

#: Violations that are blocked regardless of scene — including when the scene is
#: unknown. Phase 1 leans on this to stay safe while ``scenes`` is empty (§7.2).
BASELINE_VIOLATION_TYPES: tuple[str, ...] = ("hardcoded_cred", "illegal_content", "political")

Scene = Literal["self_use", "internal_org", "cross_org", "public_release", "research_anon"]
SCENES: tuple[str, ...] = ("self_use", "internal_org", "cross_org", "public_release", "research_anon")

#: Sentinel column used by the policy matrix when no scene could be resolved.
UNKNOWN_SCENE_KEY = "_unknown"

Severity = Literal["info", "low", "medium", "high", "critical"]
SEVERITIES: tuple[str, ...] = ("info", "low", "medium", "high", "critical")

#: The ten disposition codes from the annotation guide §6.4, listed from least to
#: most disruptive. The order is load-bearing: merging several hits into one
#: decision picks the strongest action, so a weak hit can never soften a strong
#: one. The same ten codes appear in every sample's ``expected_action``, which is
#: what makes the matrix cross-validation in ``run_compliance_gate_eval.py`` work.
Action = Literal[
    "allow",
    "warn",
    "report",
    "role_check",
    "manual_review",
    "aggregate",
    "desensitize",
    "rewrite",
    "block_storage",
    "refuse",
]
ACTIONS: tuple[str, ...] = (
    "allow",
    "warn",
    "report",
    "role_check",
    "manual_review",
    "aggregate",
    "desensitize",
    "rewrite",
    "block_storage",
    "refuse",
)

#: Actions that change or withhold the payload. Anything here means the content
#: must not pass through untouched.
MUTATING_ACTIONS: tuple[str, ...] = ("aggregate", "desensitize", "rewrite", "block_storage", "refuse")

CostHint = Literal["light", "medium", "heavy"]
COST_HINTS: tuple[str, ...] = ("light", "medium", "heavy")

RiskLocationKind = Literal["field_path", "char_span", "frame", "bbox", "coordinate"]


# ── Payload carried into a detector ──────────────────────────────────────────


@dataclass(frozen=True)
class TextItem:
    """A free-text span to inspect."""

    item_id: str
    text: str
    source: str  # evidence_id / "query" / "output" / file path


@dataclass(frozen=True)
class FieldItem:
    """A structured field to inspect, addressed by dotted path."""

    item_id: str
    path: str  # e.g. "features.camera_id"
    value: Any
    source: str


@dataclass(frozen=True)
class DetectionUnit:
    """One indivisible thing to check. Detectors get a read-only copy."""

    unit_id: str
    gate: Gate
    data_type: str | None = None
    text_items: tuple[TextItem, ...] = ()
    field_items: tuple[FieldItem, ...] = ()
    raw: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class UserContext:
    """Who is asking. Phase 1 populates this only partially (risk 12)."""

    user_id: str | None = None
    roles: tuple[str, ...] = ()
    org_id: str | None = None


@dataclass(frozen=True)
class IntentInfo:
    """Recognized intent, supplied by IntentRecognitionMiddleware.

    The guide (§9.7) requires InputGate to combine *sensitive entity* with
    *high-risk intent* — a keyword hit alone is not a violation.
    """

    intent: str | None = None
    scene_hint: str | None = None
    high_risk: bool = False
    confidence: float = 0.0


@dataclass(frozen=True)
class DetectContext:
    """Ambient information for a detection call."""

    gate: Gate
    scenes: tuple[Scene, ...] = ()  # phase 1: always empty — see plan §7.2
    user: UserContext | None = None
    intent: IntentInfo | None = None
    budget_ms: int = 400


# ── A detector's only output ─────────────────────────────────────────────────


@dataclass(frozen=True)
class RiskLocation:
    """Where in the unit the risk sits, so disposition can act surgically."""

    kind: RiskLocationKind
    locator: str  # field path / "12:34" char span / frame number ...
    text: str | None = None
    entity_type: str | None = None


@dataclass(frozen=True)
class DetectionHit:
    """The single output type of every detector."""

    detector_id: str
    violation_type: ViolationType
    confidence: float  # 0-1
    severity: Severity
    risk_locations: tuple[RiskLocation, ...] = ()
    reason_code: str = ""
    evidence: Mapping[str, Any] = field(default_factory=dict)
    basis: tuple[str, ...] = ()  # compliance clauses backing this call


# ── The protocol ─────────────────────────────────────────────────────────────


@runtime_checkable
class Detector(Protocol):
    """Structural type — duck typing is enough, inheritance is not required.

    ``detector_id`` must match the ``detector_id`` in the detector's manifest.
    """

    detector_id: str

    def setup(self, params: Mapping[str, Any]) -> None:
        """Called once at registration with merged manifest + config params."""
        ...

    def detect(self, unit: DetectionUnit, ctx: DetectContext) -> Sequence[DetectionHit]:
        """Inspect one unit. Must not raise for ordinary input; must not mutate."""
        ...


# ── Validation helpers (shared by registry and adapters) ─────────────────────


class ContractError(ValueError):
    """Raised when data crossing the contract boundary is malformed."""


def is_compatible_contract_version(declared: str) -> bool:
    """Major version must match; minor may lag or lead."""
    try:
        declared_major = str(declared).split(".", 1)[0]
    except (AttributeError, IndexError):
        return False
    return declared_major == CONTRACT_VERSION.split(".", 1)[0]


def validate_hit(hit: Any) -> DetectionHit:
    """Validate an object claiming to be a ``DetectionHit``.

    Adapters that deserialize JSON (subprocess/HTTP) run everything through this
    so a malformed detector cannot inject an unknown violation type or an
    out-of-range confidence into the policy matrix.
    """
    if not isinstance(hit, DetectionHit):
        raise ContractError(f"expected DetectionHit, got {type(hit).__name__}")
    if hit.violation_type not in VIOLATION_TYPES:
        raise ContractError(f"unknown violation_type: {hit.violation_type!r}")
    if hit.severity not in SEVERITIES:
        raise ContractError(f"unknown severity: {hit.severity!r}")
    if not isinstance(hit.confidence, (int, float)) or not (0.0 <= float(hit.confidence) <= 1.0):
        raise ContractError(f"confidence must be within [0, 1], got {hit.confidence!r}")
    if not hit.detector_id:
        raise ContractError("detector_id must be non-empty")
    return hit


# ── JSON Schema export (cross-language detector authors) ─────────────────────

_PRIMITIVE_SCHEMAS: dict[Any, dict[str, Any]] = {
    str: {"type": "string"},
    bool: {"type": "boolean"},
    int: {"type": "integer"},
    float: {"type": "number"},
    type(None): {"type": "null"},
}

#: Dataclasses exported into ``$defs``.
_EXPORTED_TYPES = (
    TextItem,
    FieldItem,
    DetectionUnit,
    UserContext,
    IntentInfo,
    DetectContext,
    RiskLocation,
    DetectionHit,
)


def _schema_for(annotation: Any) -> dict[str, Any]:
    """Translate one type annotation into a JSON Schema fragment."""
    if annotation is Any:
        return {}
    if annotation in _PRIMITIVE_SCHEMAS:
        return dict(_PRIMITIVE_SCHEMAS[annotation])
    if dataclasses.is_dataclass(annotation):
        return {"$ref": f"#/$defs/{annotation.__name__}"}

    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)

    if origin is Literal:
        return {"enum": list(args)}
    # typing.get_origin() normalizes typing.Mapping -> collections.abc.Mapping,
    # so compare against the collections.abc forms rather than the typing aliases.
    if origin in (tuple, list, collections.abc.Sequence):
        # tuple[X, ...] and list[X] both export as a homogeneous array.
        item = args[0] if args else Any
        return {"type": "array", "items": _schema_for(item)}
    if origin in (dict, collections.abc.Mapping, collections.abc.MutableMapping):
        value = args[1] if len(args) == 2 else Any
        return {"type": "object", "additionalProperties": _schema_for(value) or True}
    # `X | None` evaluates to types.UnionType, while `Optional[X]` yields
    # typing.Union — accept both so the export does not depend on spelling.
    if origin is typing.Union or origin is types.UnionType:
        # Optional[X] -> anyOf including null.
        return {"anyOf": [_schema_for(arg) for arg in args]}

    return {}


def _schema_for_dataclass(cls: type) -> dict[str, Any]:
    hints = typing.get_type_hints(cls)
    properties: dict[str, Any] = {}
    required: list[str] = []
    for f in dataclasses.fields(cls):
        properties[f.name] = _schema_for(hints[f.name])
        has_default = f.default is not dataclasses.MISSING or f.default_factory is not dataclasses.MISSING  # type: ignore[misc]
        if not has_default:
            required.append(f.name)
    schema: dict[str, Any] = {"type": "object", "title": cls.__name__, "properties": properties}
    if required:
        schema["required"] = required
    if cls.__doc__:
        schema["description"] = cls.__doc__.strip().splitlines()[0]
    return schema


def export_json_schema() -> dict[str, Any]:
    """Export the contract as a JSON Schema document.

    Consumed by ``scripts/export_detector_schema.py``. Subprocess and HTTP
    detectors exchange exactly these shapes, so cross-language authors never have
    to transcribe field names by hand.
    """
    defs = {cls.__name__: _schema_for_dataclass(cls) for cls in _EXPORTED_TYPES}
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://deerflow.dev/schemas/compliance/detector-contract.json",
        "title": "DeerFlow compliance detector contract",
        "description": "Wire format for subprocess_cli and http_service detector adapters.",
        "contract_version": CONTRACT_VERSION,
        "$defs": defs,
        "type": "object",
        "properties": {
            "request": {
                "type": "object",
                "properties": {
                    "unit": {"$ref": "#/$defs/DetectionUnit"},
                    "ctx": {"$ref": "#/$defs/DetectContext"},
                    "params": {"type": "object", "additionalProperties": True},
                },
                "required": ["unit", "ctx"],
            },
            "response": {
                "type": "object",
                "properties": {"hits": {"type": "array", "items": {"$ref": "#/$defs/DetectionHit"}}},
                "required": ["hits"],
            },
        },
    }


__all__ = [
    "CONTRACT_VERSION",
    "ACTIONS",
    "Action",
    "BASELINE_VIOLATION_TYPES",
    "MUTATING_ACTIONS",
    "COST_HINTS",
    "ContractError",
    "CostHint",
    "DetectContext",
    "DetectionHit",
    "DetectionUnit",
    "Detector",
    "FieldItem",
    "GATES",
    "Gate",
    "IntentInfo",
    "RiskLocation",
    "RiskLocationKind",
    "SCENES",
    "SEVERITIES",
    "Scene",
    "Severity",
    "TextItem",
    "UNKNOWN_SCENE_KEY",
    "UserContext",
    "VIOLATION_TYPES",
    "ViolationType",
    "export_json_schema",
    "is_compatible_contract_version",
    "validate_hit",
]
