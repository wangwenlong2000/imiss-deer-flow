from __future__ import annotations

from types import SimpleNamespace

from deerflow.compliance.runtime import compliance_context, user_context
from deerflow.compliance.scene import ManualSceneResolver, resolve_scene_from_state
from deerflow.compliance.types import DetectionRequest
from deerflow.config.compliance_config import ComplianceConfig, get_compliance_config, set_compliance_config


def _request(origin: dict) -> DetectionRequest:
    return DetectionRequest(gate="OutputGate", units=(), request_id="req-1", origin=origin)


def test_manual_scene_resolver_reads_the_selected_scene() -> None:
    request = _request({"compliance_context": {"source": "manual_ui", "enabled": True, "scene": "public_release"}})

    assert ManualSceneResolver().resolve(request) == "public_release"


def test_manual_scene_resolver_does_not_accept_unknown_scene() -> None:
    request = _request({"compliance_context": {"source": "manual_ui", "enabled": True, "scene": "admin"}})

    assert ManualSceneResolver().resolve(request) is None


def test_manual_scene_resolver_keeps_unselected_context_unknown() -> None:
    request = _request({"compliance_context": {"source": "manual_ui", "enabled": False, "scene": "public_release"}})

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
            manual_context={"source": "manual_ui", "enabled": True, "scene": "cross_org"},
        )
        assert resolution.scene == "cross_org"
        assert resolution.source == "manual_ui"
        assert resolution.permission_checked is False
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
