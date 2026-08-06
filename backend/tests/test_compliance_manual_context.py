from __future__ import annotations

from types import SimpleNamespace

import pytest

from deerflow.compliance.runtime import compliance_context, user_context
from deerflow.compliance.scene import ManualSceneResolver, resolve_scene_from_state
from deerflow.compliance.types import DetectionRequest
from deerflow.config.compliance_config import ComplianceConfig, get_compliance_config, set_compliance_config


def _request(origin: dict) -> DetectionRequest:
    return DetectionRequest(gate="OutputGate", units=(), request_id="req-1", origin=origin)


PROFILES = {
    "ordinary_user": ("self_use", "user", "personal"),
    "internal_operator": ("internal_org", "internal_operator", "city-governance"),
    "cross_org_operator": ("cross_org", "cross_org_operator", "partner-org"),
    "publisher": ("public_release", "publisher", "publicity"),
    "researcher": ("research_anon", "researcher", "research"),
}


def _manual_context(identity: str, *, scene: str | None = None) -> dict:
    expected_scene, role, org_id = PROFILES[identity]
    return {
        "source": "manual_ui",
        "enabled": True,
        "identity": identity,
        "scene": scene or expected_scene,
        "user_context": {
            "user_id": f"manual-{identity}",
            "roles": [role],
            "org_id": org_id,
        },
    }


def test_manual_scene_resolver_reads_the_selected_scene() -> None:
    request = _request({"compliance_context": _manual_context("publisher")})

    assert ManualSceneResolver().resolve(request) == "public_release"


def test_manual_scene_resolver_does_not_accept_unknown_scene() -> None:
    request = _request({"compliance_context": _manual_context("publisher", scene="admin")})

    assert ManualSceneResolver().resolve(request) is None


def test_manual_scene_resolver_keeps_unselected_context_unknown() -> None:
    context = _manual_context("publisher")
    context["enabled"] = False
    request = _request({"compliance_context": context})

    assert ManualSceneResolver().resolve(request) is None


def test_runtime_helpers_extract_manual_identity() -> None:
    runtime = {
        "compliance_context": {
            "source": "manual_ui",
            "scene": "internal_org",
            "user_context": {
                "user_id": "manual-internal_operator",
                "roles": ["internal_operator"],
                "org_id": "city-governance",
            },
        }
    }

    assert compliance_context(runtime)["scene"] == "internal_org"
    assert user_context(runtime).roles == ("internal_operator",)


def test_runtime_helpers_unpack_model_request_runtime() -> None:
    request = SimpleNamespace(
        runtime=SimpleNamespace(
            context={
                "thread_id": "thread-model-request",
                "compliance_context": {
                    "source": "manual_ui",
                    "scene": "cross_org",
                    "user_context": {"user_id": "u-1", "roles": ["operator"]},
                },
            }
        )
    )

    assert compliance_context(request)["scene"] == "cross_org"
    assert user_context(request).user_id == "u-1"


def test_output_gate_thread_id_comes_from_model_request_runtime() -> None:
    from deerflow.agents.middlewares.compliance_output_gate_middleware import ComplianceOutputGateMiddleware

    request = SimpleNamespace(runtime=SimpleNamespace(context={"thread_id": "thread-output"}))

    assert ComplianceOutputGateMiddleware._thread_id(request) == "thread-output"


def test_manual_ui_mode_resolves_frontend_context() -> None:
    original = get_compliance_config()
    try:
        set_compliance_config(ComplianceConfig(enabled=True, scene_resolver_mode="manual_ui"))
        _, resolution = resolve_scene_from_state(
            {},
            manual_context=_manual_context("cross_org_operator"),
        )
        assert resolution.scene == "cross_org"
        assert resolution.source == "manual_ui"
        assert resolution.permission_checked is False
        assert "guide_7_23_scene_binding" in resolution.reason_codes
        assert "not_authoritative" in resolution.reason_codes
    finally:
        set_compliance_config(original)


def test_manual_ui_mode_does_not_accept_context_without_manual_source() -> None:
    original = get_compliance_config()
    try:
        set_compliance_config(ComplianceConfig(enabled=True, scene_resolver_mode="manual_ui"))
        _, resolution = resolve_scene_from_state(
            {},
            manual_context={"enabled": True, "scene": "public_release"},
        )
        assert resolution.scene == "_unknown"
        assert resolution.source == "manual_ui"
    finally:
        set_compliance_config(original)


@pytest.mark.parametrize(("identity", "expected_scene"), [(key, value[0]) for key, value in PROFILES.items()])
def test_each_guide_profile_resolves_to_its_bound_scene(identity: str, expected_scene: str) -> None:
    resolution = ManualSceneResolver().resolve_context(_manual_context(identity))
    assert resolution.scene == expected_scene
    assert resolution.fallback is False
    assert resolution.permission_checked is False
    assert "manual_profile_validated" in resolution.reason_codes


def test_identity_cannot_claim_a_more_permissive_scene() -> None:
    resolution = ManualSceneResolver().resolve_context(_manual_context("publisher", scene="self_use"))
    assert resolution.scene == "_unknown"
    assert resolution.fallback is True
    assert resolution.reason_codes == ("manual_identity_scene_mismatch",)


def test_manual_profile_rejects_spoofed_role_or_organization() -> None:
    context = _manual_context("ordinary_user")
    context["user_context"] = {
        "user_id": "manual-ordinary_user",
        "roles": ["publisher"],
        "org_id": "publicity",
    }
    resolution = ManualSceneResolver().resolve_context(context)
    assert resolution.scene == "_unknown"
    assert resolution.reason_codes == ("manual_user_context_mismatch",)


@pytest.mark.parametrize(
    ("identity", "struct_actions", "geo_actions"),
    [
        ("ordinary_user", ("allow",), ("allow",)),
        ("internal_operator", ("role_check", "warn"), ("warn", "desensitize")),
        ("cross_org_operator", ("desensitize",), ("desensitize",)),
        ("publisher", ("refuse", "desensitize"), ("aggregate",)),
        ("researcher", ("desensitize",), ("aggregate",)),
    ],
)
def test_guide_profiles_select_type12_input_policy(
    identity: str,
    struct_actions: tuple[str, ...],
    geo_actions: tuple[str, ...],
) -> None:
    from pathlib import Path

    from deerflow.compliance.policy import load_policy_matrix

    scene = ManualSceneResolver().resolve_context(_manual_context(identity)).scene
    matrix_path = Path(__file__).resolve().parents[2] / "config/compliance/policy_matrix.yaml"
    matrix = load_policy_matrix(matrix_path)
    assert matrix.cell("struct_id", "InputGate", scene) == struct_actions
    assert matrix.cell("geo_loc", "InputGate", scene) == geo_actions
