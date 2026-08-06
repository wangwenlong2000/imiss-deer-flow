"""Contract stability: frozen dataclasses and a stable JSON Schema export.

The contract is the boundary other teams build against. Anything that silently
changes shape here breaks detectors written by people who will not find out
until production.
"""

from __future__ import annotations

import dataclasses
import json

import pytest

from deerflow.compliance.contract import (
    ACTIONS,
    BASELINE_VIOLATION_TYPES,
    CONTRACT_VERSION,
    GATES,
    MUTATING_ACTIONS,
    SCENES,
    SEVERITIES,
    VIOLATION_TYPES,
    ContractError,
    DetectContext,
    DetectionHit,
    DetectionUnit,
    Detector,
    FieldItem,
    RiskLocation,
    TextItem,
    export_json_schema,
    is_compatible_contract_version,
    validate_hit,
)

FROZEN_TYPES = (TextItem, FieldItem, DetectionUnit, DetectContext, RiskLocation, DetectionHit)


# ── immutability ────────────────────────────────────────────────────────────


@pytest.mark.parametrize("cls", FROZEN_TYPES, ids=[c.__name__ for c in FROZEN_TYPES])
def test_contract_types_are_frozen_dataclasses(cls: type) -> None:
    assert dataclasses.is_dataclass(cls)
    assert cls.__dataclass_params__.frozen, f"{cls.__name__} must be frozen so detectors cannot mutate engine data"


def test_detection_unit_cannot_be_mutated() -> None:
    unit = DetectionUnit(unit_id="u1", gate="ContextGate")
    with pytest.raises(dataclasses.FrozenInstanceError):
        unit.unit_id = "u2"  # type: ignore[misc]


def test_collections_are_tuples_not_lists() -> None:
    """Tuple defaults keep collections immutable and shareable."""
    unit = DetectionUnit(unit_id="u1", gate="OutputGate")
    assert isinstance(unit.text_items, tuple)
    assert isinstance(unit.field_items, tuple)
    ctx = DetectContext(gate="OutputGate")
    assert isinstance(ctx.scenes, tuple)


# ── enumerations ────────────────────────────────────────────────────────────


def test_ten_violation_types() -> None:
    assert len(VIOLATION_TYPES) == 10
    assert len(set(VIOLATION_TYPES)) == 10


def test_three_gates_and_five_scenes() -> None:
    assert GATES == ("InputGate", "ContextGate", "OutputGate")
    assert len(SCENES) == 5


def test_baseline_types_are_a_subset_of_violation_types() -> None:
    assert set(BASELINE_VIOLATION_TYPES) <= set(VIOLATION_TYPES)
    assert set(BASELINE_VIOLATION_TYPES) == {"hardcoded_cred", "illegal_content", "political"}


def test_actions_match_the_guide_vocabulary() -> None:
    """The ten disposition codes from guide §6.4, which annotated samples also use."""
    assert set(ACTIONS) == {
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
    }
    assert ACTIONS[0] == "allow", "ordering is load-bearing: weakest first"
    assert ACTIONS[-1] == "refuse", "ordering is load-bearing: strongest last"
    assert set(MUTATING_ACTIONS) <= set(ACTIONS)


def test_contract_has_no_tech_field() -> None:
    """Plan decision: implementation technique is a detector's private business."""
    for cls in FROZEN_TYPES:
        field_names = {f.name for f in dataclasses.fields(cls)}
        assert "tech" not in field_names, f"{cls.__name__} must not expose a `tech` field"


# ── version compatibility ───────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("declared", "expected"),
    [("1.0", True), ("1.5", True), ("1", True), ("2.0", False), ("0.9", False), ("", False), ("garbage", False)],
)
def test_contract_version_compatibility(declared: str, expected: bool) -> None:
    assert is_compatible_contract_version(declared) is expected


def test_current_version_is_compatible_with_itself() -> None:
    assert is_compatible_contract_version(CONTRACT_VERSION)


# ── hit validation ──────────────────────────────────────────────────────────


def _hit(**overrides: object) -> DetectionHit:
    payload = {"detector_id": "d", "violation_type": "domain", "confidence": 0.9, "severity": "high"}
    payload.update(overrides)
    return DetectionHit(**payload)  # type: ignore[arg-type]


def test_validate_hit_accepts_a_well_formed_hit() -> None:
    assert validate_hit(_hit()) is not None


@pytest.mark.parametrize(
    "overrides",
    [
        {"violation_type": "not_a_type"},
        {"severity": "catastrophic"},
        {"confidence": 1.5},
        {"confidence": -0.1},
        {"detector_id": ""},
    ],
    ids=["bad_violation", "bad_severity", "confidence_high", "confidence_low", "empty_id"],
)
def test_validate_hit_rejects_malformed_hits(overrides: dict) -> None:
    with pytest.raises(ContractError):
        validate_hit(_hit(**overrides))


def test_validate_hit_rejects_non_hit_objects() -> None:
    with pytest.raises(ContractError):
        validate_hit({"detector_id": "d"})


# ── protocol ────────────────────────────────────────────────────────────────


def test_duck_typed_detector_satisfies_the_protocol() -> None:
    """No inheritance required — that is the point of a Protocol."""

    class Duck:
        detector_id = "duck"

        def setup(self, params):  # noqa: ANN001, ANN202
            return None

        def detect(self, unit, ctx):  # noqa: ANN001, ANN202
            return []

    assert isinstance(Duck(), Detector)


# ── JSON Schema export ──────────────────────────────────────────────────────


def test_schema_exports_every_boundary_type() -> None:
    schema = export_json_schema()
    assert schema["contract_version"] == CONTRACT_VERSION
    for name in ("TextItem", "FieldItem", "DetectionUnit", "DetectContext", "RiskLocation", "DetectionHit", "UserContext", "IntentInfo"):
        assert name in schema["$defs"], f"{name} missing from exported schema"


def test_schema_is_json_serializable_and_stable() -> None:
    """Two exports must be byte-identical — detector authors diff this file."""
    first = json.dumps(export_json_schema(), sort_keys=True, ensure_ascii=False)
    second = json.dumps(export_json_schema(), sort_keys=True, ensure_ascii=False)
    assert first == second


def test_schema_enumerates_violation_types_and_severities() -> None:
    schema = export_json_schema()
    hit = schema["$defs"]["DetectionHit"]["properties"]
    assert hit["violation_type"]["enum"] == list(VIOLATION_TYPES)
    assert hit["severity"]["enum"] == list(SEVERITIES)


def test_schema_resolves_optional_and_mapping_fields() -> None:
    """Regression: `X | None` and `Mapping[str, Any]` must not export as `{}`."""
    schema = export_json_schema()
    unit = schema["$defs"]["DetectionUnit"]["properties"]
    assert unit["data_type"] == {"anyOf": [{"type": "string"}, {"type": "null"}]}
    assert unit["raw"] == {"type": "object", "additionalProperties": True}
    assert unit["text_items"] == {"type": "array", "items": {"$ref": "#/$defs/TextItem"}}


def test_schema_declares_the_request_response_envelope() -> None:
    """Out-of-process detectors exchange exactly this envelope."""
    schema = export_json_schema()
    assert schema["properties"]["request"]["properties"]["unit"]["$ref"] == "#/$defs/DetectionUnit"
    assert schema["properties"]["response"]["properties"]["hits"]["items"]["$ref"] == "#/$defs/DetectionHit"


def test_required_fields_reflect_dataclass_defaults() -> None:
    schema = export_json_schema()
    required = schema["$defs"]["DetectionHit"]["required"]
    assert required == ["detector_id", "violation_type", "confidence", "severity"]
    assert "evidence" not in required, "fields with defaults must be optional in the schema"
