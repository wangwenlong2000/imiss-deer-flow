"""Trusted OneCity request contract and scene propagation tests."""

from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest
from langchain_core.messages import AIMessage, HumanMessage

from deerflow.agents.middlewares.compliance_context_gate_middleware import ComplianceContextGateMiddleware
from deerflow.agents.middlewares.compliance_input_gate_middleware import ComplianceInputGateMiddleware
from deerflow.agents.middlewares.compliance_output_gate_middleware import ComplianceOutputGateMiddleware
from deerflow.compliance.audit import Auditor
from deerflow.compliance.contract import IntentInfo
from deerflow.compliance.engine import ComplianceEngine
from deerflow.compliance.normalizers.user_input import UserInputNormalizer
from deerflow.compliance.policy import load_policy_matrix
from deerflow.compliance.registry import build_registry
from deerflow.compliance.scene import (
    TrustedSceneResolver,
    parse_compliance_request,
    resolve_scene_from_state,
    scene_audit_context,
    scene_origin_from_state,
)
from deerflow.config.compliance_config import ComplianceConfig, get_compliance_config, set_compliance_config

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def trusted_mode_for_contract_tests():
    original = get_compliance_config()
    set_compliance_config(ComplianceConfig(enabled=True, scene_resolver_mode="trusted_upstream"))
    yield
    set_compliance_config(original)


def _request(
    *,
    operation: str = "analysis",
    audience: str = "self",
    owner_user: str = "user-001",
    owner_org: str = "org-001",
    owner_department: str = "dept-001",
    target_org: str | None = None,
    classification: str = "internal",
    anonymized: bool = False,
    hint: str | None = None,
    actor: bool = True,
    permissions: tuple[str, ...] = ("traffic.read",),
    roles: tuple[str, ...] = ("analyst",),
    intent_type: str = "analysis",
) -> dict:
    result = {
        "request_id": "req-scene-001",
        # Positive cases model the future server/IAM contract explicitly. A
        # real request without these bindings is covered by the conservative
        # _unknown tests below.
        "thread_id": "thread-scene",
        "turn_id": "turn-001",
        "trusted_source": "onecity_iam",
        "permission_verified": True,
        "operation": {"type": operation, "target_audience": audience},
        "resource": {
            "resource_id": "resource-001",
            "owner_user_id": owner_user,
            "owner_org_id": owner_org,
            "owner_department_id": owner_department,
            "target_org_id": target_org,
            "classification": classification,
            "is_anonymized": anonymized,
        },
        "scene_hint": hint,
        "intent": {
            "intent_type": intent_type,
            "requested_operation": operation,
            "risk_level": "low",
            "is_high_risk": False,
        },
    }
    if actor:
        result["actor"] = {
            "user_id": "user-001",
            "org_id": "org-001",
            "department_id": "dept-001",
            "roles": list(roles),
            "permissions": list(permissions),
        }
    return result


@pytest.mark.parametrize(
    ("raw", "scene", "reason"),
    [
        (_request(), "self_use", "owner_user_match"),
        (_request(audience="department"), "internal_org", "organization_match"),
        (
            _request(audience="organization", owner_department="dept-002"),
            "internal_org",
            "cross_department_same_org",
        ),
        (
            _request(operation="share", audience="external_org", target_org="org-002"),
            "cross_org",
            "organization_boundary_crossed",
        ),
        (
            _request(operation="public_release", audience="public", hint="self_use", intent_type="public_release"),
            "public_release",
            "public_release_priority",
        ),
        (
            _request(
                operation="research",
                audience="research_group",
                anonymized=True,
                permissions=("research.read",),
                roles=("researcher",),
                intent_type="research",
            ),
            "research_anon",
            "resource_anonymized",
        ),
        (
            _request(
                operation="research",
                audience="research_group",
                anonymized=False,
                permissions=("research.read",),
                roles=("researcher",),
                intent_type="research",
            ),
            "_unknown",
            "research_anonymization_or_permission_missing",
        ),
        (
            _request(owner_user="user-999", hint="self_use"),
            "_unknown",
            "scene_hint_unverified",
        ),
        (
            _request(owner_org="org-999", hint="internal_org", audience="organization"),
            "cross_org",
            "scene_hint_conflict",
        ),
        (_request(actor=False), "_unknown", "actor_identity_missing"),
    ],
)
def test_trusted_scene_resolution(raw, scene, reason):
    result = TrustedSceneResolver().resolve_context(raw)
    assert result.scene == scene
    assert reason in result.reason_codes or "trusted_upstream_context_missing" in result.reason_codes
    assert result.source == ("trusted_upstream" if raw.get("actor") else "null_resolver")
    assert result.fallback is (scene == "_unknown")


def test_request_contract_parses_typed_actor_operation_and_resource():
    parsed = parse_compliance_request(_request(audience="department"))
    assert parsed.request_id == "req-scene-001"
    assert parsed.actor.permissions == ("traffic.read",)
    assert parsed.operation.target_audience == "department"
    assert parsed.resource.owner_org_id == "org-001"
    assert parsed.intent["intent_type"] == "analysis"


def test_scene_hint_cannot_make_other_users_resource_self_use():
    result = TrustedSceneResolver().resolve_context(_request(owner_user="user-999", hint="self_use"))
    assert result.scene != "self_use"
    assert result.fallback


def test_nonempty_permissions_without_iam_binding_stays_unknown():
    raw = _request()
    for key in ("thread_id", "turn_id", "trusted_source", "permission_verified"):
        raw.pop(key, None)
    result = TrustedSceneResolver().resolve_context(raw)
    assert result.scene == "_unknown"
    assert result.permission_checked is False
    assert "permission_verification_missing" in result.reason_codes


def test_server_resolver_marker_alone_cannot_reuse_cached_scene():
    raw = _request()
    for key in ("thread_id", "turn_id", "trusted_source", "permission_verified"):
        raw.pop(key, None)
    state = {
        "compliance_request": raw,
        "scene_context": {
            "scene": "public_release",
            "source": "server_resolver",
            "permission_checked": True,
            "request_id": raw["request_id"],
        },
    }
    _, resolution = resolve_scene_from_state(state)
    assert resolution.scene == "_unknown"
    assert resolution.source == "null_resolver"


@pytest.fixture()
def scene_engine(tmp_path):
    registry = build_registry(config_path=REPO_ROOT / "config/compliance/detectors.yaml", strict=True)
    model = registry.get("model_tfidf_knn")
    if model is not None:
        model.enabled = False
    auditor = Auditor(path=tmp_path / "audit", enabled=True)
    engine = ComplianceEngine(
        registry=registry,
        policy=load_policy_matrix(REPO_ROOT / "config/compliance/policy_matrix.yaml"),
        auditor=auditor,
        scene_resolver=TrustedSceneResolver(),
    )
    yield engine, auditor
    registry.close()


@pytest.mark.parametrize(
    ("raw", "expected_scene"),
    [
        (_request(), "self_use"),
        (_request(audience="organization"), "internal_org"),
        (_request(operation="share", audience="external_org", target_org="org-002"), "cross_org"),
        (
            _request(operation="public_release", audience="public", intent_type="public_release"),
            "public_release",
        ),
        (
            _request(
                operation="research",
                audience="research_group",
                anonymized=True,
                permissions=("statistics.research",),
                roles=("researcher",),
                intent_type="research",
            ),
            "research_anon",
        ),
        (_request(actor=False), "_unknown"),
    ],
)
def test_same_resolved_scene_is_used_by_all_three_gates(scene_engine, raw, expected_scene):
    engine, _ = scene_engine
    resolution = TrustedSceneResolver().resolve_context(raw)
    origin = scene_audit_context(raw, resolution)
    units = UserInputNormalizer().to_units(
        "手机号13800138000",
        gate="InputGate",
        thread_id="thread-scene",
    )
    observed = []
    for gate in ("InputGate", "ContextGate", "OutputGate"):
        gate_units = tuple(dataclasses.replace(unit, gate=gate) for unit in units)
        decision = engine.check(
            gate_units,
            gate=gate,
            intent=IntentInfo(
                intent="public_release",
                high_risk=True,
                intent_type="public_release",
                requested_operation="publish",
                risk_level="high",
            ),
            origin=origin,
        )
        observed.append(decision.scene_key)
    assert observed == [expected_scene, expected_scene, expected_scene]


def test_audit_contains_scene_permission_and_request_facts(scene_engine):
    engine, auditor = scene_engine
    raw = _request(operation="public_release", audience="public", hint="self_use", intent_type="public_release")
    resolution = TrustedSceneResolver().resolve_context(raw)
    units = UserInputNormalizer().to_units("手机号13800138000", gate="InputGate")
    decision = engine.check(
        units,
        gate="InputGate",
        intent=IntentInfo(high_risk=True, intent_type="public_release", risk_level="high"),
        origin=scene_audit_context(raw, resolution),
    )
    assert decision.audit_ref
    record = auditor.read_all()[-1]
    assert record["scene_key"] == "public_release"
    assert record["scene_resolution"]["permission_checked"] is True
    assert record["scene_resolution"]["scene_hint"] == "self_use"
    assert record["request_context"]["actor"]["org_id"] == "org-001"
    assert record["request_context"]["operation"]["target_audience"] == "public"
    assert record["request_context"]["resource"]["owner_org_id"] == "org-001"


def test_input_context_and_output_middleware_checks_share_resolved_scene(scene_engine):
    engine, _ = scene_engine
    raw = _request(operation="public_release", audience="public", intent_type="public_release")
    resolution = TrustedSceneResolver().resolve_context(raw)
    state = {
        "messages": [HumanMessage(content="公开手机号13800138000")],
        "compliance_request": raw,
        "scene_context": resolution.to_dict(),
        "intent_context": {
            "intent": "task",
            "compliance_intent": {
                "intent_type": "public_release",
                "requested_operation": "publish",
                "risk_level": "high",
                "is_high_risk": True,
                "confidence": 0.95,
                "reason_codes": ["public_disclosure"],
                "source": "intent_recognition",
            },
        },
    }
    origin, _ = scene_origin_from_state(state)
    input_decision = ComplianceInputGateMiddleware(engine=engine)._check(
        "公开手机号13800138000",
        "thread-scene",
        state,
        "req-input-scene",
        scene_origin=origin,
    )

    payload = {
        "schema_version": "1.0",
        "request_id": "req-context-scene",
        "skill_name": "scene-test",
        "result": {
            "display_text": "手机号13800138000",
            "evidence": [
                {
                    "evidence_id": "ev-1",
                    "type": "text",
                    "title": "test",
                    "data": "手机号13800138000",
                    "metadata": {"score": 1.0},
                }
            ],
            "findings": [],
        },
        "diagnostics": {"warnings": []},
    }
    context_decision = ComplianceContextGateMiddleware(engine=engine)._check(
        payload,
        "req-context-scene",
        "scene-test",
        state=state,
    )
    output_decision = ComplianceOutputGateMiddleware(engine=engine)._check(
        "手机号13800138000",
        AIMessage(content="手机号13800138000", id="scene-output"),
        "req-output-scene",
        state=state,
    )

    assert [input_decision.scene_key, context_decision.scene_key, output_decision.scene_key] == [
        "public_release",
        "public_release",
        "public_release",
    ]
