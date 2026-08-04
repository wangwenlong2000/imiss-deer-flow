from __future__ import annotations

from deerflow.compliance.scene import ManualSceneResolver
from deerflow.compliance.types import DetectionRequest
from deerflow.compliance.runtime import compliance_context, user_context


def _request(origin: dict) -> DetectionRequest:
    return DetectionRequest(gate="OutputGate", units=(), request_id="req-1", origin=origin)


def test_manual_scene_resolver_reads_the_selected_scene() -> None:
    request = _request({"compliance_context": {"enabled": True, "scene": "public_release"}})

    assert ManualSceneResolver().resolve(request) == "public_release"


def test_manual_scene_resolver_does_not_accept_unknown_scene() -> None:
    request = _request({"compliance_context": {"enabled": True, "scene": "admin"}})

    assert ManualSceneResolver().resolve(request) is None


def test_manual_scene_resolver_keeps_unselected_context_unknown() -> None:
    request = _request({"compliance_context": {"enabled": False, "scene": "public_release"}})

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
