"""Registry: manifest discovery, validation, and the startup guards.

A compliance detector that is quietly misconfigured is worse than one that is
missing, because the gate still looks green. So most of these tests are about
failing loudly at startup.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from deerflow.compliance.registry import (
    DetectorRegistryError,
    build_registry,
    discover_manifests,
    load_detectors_config,
)

BASE_MANIFEST = {
    "contract_version": "1.0",
    "detector_id": "demo",
    "adapter": "inprocess",
    "entry": "tests.compliance_fake_detector:FakeDetector",
    "violation_types": ["domain"],
    "gates": {"domain": ["ContextGate", "OutputGate"]},
    "data_types": None,
    "cost_hint": "light",
    "default_params": {"threshold": 0.5},
}


@pytest.fixture()
def detectors_dir(tmp_path: Path) -> Path:
    root = tmp_path / "detectors"
    root.mkdir()
    return root


def write_manifest(root: Path, name: str, **overrides: object) -> Path:
    data = {**BASE_MANIFEST, **overrides}
    if "detector_id" not in overrides:
        data["detector_id"] = name
    package = root / name
    package.mkdir(parents=True, exist_ok=True)
    manifest = package / "manifest.yaml"
    manifest.write_text(yaml.safe_dump(data, allow_unicode=True), encoding="utf-8")
    return manifest


# ── discovery ───────────────────────────────────────────────────────────────


def test_zero_detectors_is_a_valid_state(detectors_dir: Path) -> None:
    """P0 ships with no detectors registered; that must not be an error."""
    registry = build_registry(detectors_dir=detectors_dir, config_path=None, strict=True)
    assert len(registry) == 0
    assert registry.enabled() == ()


def test_manifests_are_discovered_by_convention(detectors_dir: Path) -> None:
    write_manifest(detectors_dir, "alpha")
    write_manifest(detectors_dir, "beta")
    found = discover_manifests(detectors_dir)
    assert [p.parent.name for p in found] == ["alpha", "beta"], "discovery must be sorted for determinism"


def test_discovery_ignores_directories_without_a_manifest(detectors_dir: Path) -> None:
    (detectors_dir / "not_a_detector").mkdir()
    (detectors_dir / "not_a_detector" / "readme.md").write_text("hi", encoding="utf-8")
    assert discover_manifests(detectors_dir) == []


def test_registration_carries_manifest_capabilities(detectors_dir: Path) -> None:
    write_manifest(detectors_dir, "alpha", content_text_spec="0624")
    registry = build_registry(detectors_dir=detectors_dir, config_path=None, strict=True)
    registration = registry.get("alpha")
    assert registration is not None
    assert registration.violation_types == ("domain",)
    assert registration.gates == {"domain": ("ContextGate", "OutputGate")}
    assert registration.cost_hint == "light"
    assert registration.meta["content_text_spec"] == "0624", "non-reserved manifest keys are preserved for docs/eval"


# ── validation ──────────────────────────────────────────────────────────────


def test_incompatible_contract_version_is_rejected(detectors_dir: Path) -> None:
    write_manifest(detectors_dir, "alpha", contract_version="2.0")
    with pytest.raises(DetectorRegistryError, match="incompatible contract_version"):
        build_registry(detectors_dir=detectors_dir, config_path=None, strict=True)


def test_unknown_violation_type_is_rejected(detectors_dir: Path) -> None:
    write_manifest(detectors_dir, "alpha", violation_types=["telepathy"], gates={"telepathy": ["OutputGate"]})
    with pytest.raises(DetectorRegistryError, match="unknown violation type"):
        build_registry(detectors_dir=detectors_dir, config_path=None, strict=True)


def test_unknown_gate_is_rejected(detectors_dir: Path) -> None:
    write_manifest(detectors_dir, "alpha", gates={"domain": ["SideGate"]})
    with pytest.raises(DetectorRegistryError, match="unknown gate"):
        build_registry(detectors_dir=detectors_dir, config_path=None, strict=True)


def test_gates_must_cover_every_declared_violation_type(detectors_dir: Path) -> None:
    write_manifest(detectors_dir, "alpha", violation_types=["domain", "re_identify"], gates={"domain": ["OutputGate"]})
    with pytest.raises(DetectorRegistryError, match="missing entries"):
        build_registry(detectors_dir=detectors_dir, config_path=None, strict=True)


def test_heavy_detector_on_context_gate_fails_at_startup(detectors_dir: Path) -> None:
    """Guide §1.2.3: the context gate is performance-sensitive; no heavy models.

    Declaring both is a design error, so it must fail at boot rather than
    surfacing as mysterious latency in production.
    """
    write_manifest(detectors_dir, "alpha", cost_hint="heavy", gates={"domain": ["ContextGate"]})
    with pytest.raises(DetectorRegistryError, match="ContextGate must stay light|cost_hint `heavy` on ContextGate"):
        build_registry(detectors_dir=detectors_dir, config_path=None, strict=True)


def test_heavy_detector_on_other_gates_is_fine(detectors_dir: Path) -> None:
    write_manifest(detectors_dir, "alpha", cost_hint="heavy", gates={"domain": ["OutputGate"]}, adapter="http_service", endpoint="http://localhost:9/detect", entry=None)
    registry = build_registry(detectors_dir=detectors_dir, config_path=None, strict=True)
    assert registry.get("alpha") is not None


def test_unknown_cost_hint_is_rejected(detectors_dir: Path) -> None:
    write_manifest(detectors_dir, "alpha", cost_hint="free")
    with pytest.raises(DetectorRegistryError, match="unknown cost_hint"):
        build_registry(detectors_dir=detectors_dir, config_path=None, strict=True)


def test_inprocess_adapter_requires_an_entry(detectors_dir: Path) -> None:
    write_manifest(detectors_dir, "alpha", entry=None)
    with pytest.raises(ValueError, match="`entry` is required"):
        build_registry(detectors_dir=detectors_dir, config_path=None, strict=True)


def test_a_broken_manifest_does_not_stop_the_others(detectors_dir: Path) -> None:
    """Non-strict mode: fault isolation applies to registration too."""
    write_manifest(detectors_dir, "good")
    write_manifest(detectors_dir, "bad", contract_version="99.0")
    registry = build_registry(detectors_dir=detectors_dir, config_path=None, strict=False)
    assert registry.get("good") is not None
    assert registry.get("bad") is None


# ── central config: switches and overrides only ─────────────────────────────


def test_disabled_detector_is_registered_but_not_routed(detectors_dir: Path, tmp_path: Path) -> None:
    write_manifest(detectors_dir, "alpha")
    config = tmp_path / "detectors.yaml"
    config.write_text(yaml.safe_dump({"detectors": [{"id": "alpha", "enabled": False}]}), encoding="utf-8")

    registry = build_registry(detectors_dir=detectors_dir, config_path=config, strict=True)
    assert registry.get("alpha") is not None, "still discoverable, for docs and diagnostics"
    assert registry.enabled() == (), "but never handed a unit"


def test_params_override_manifest_defaults(detectors_dir: Path, tmp_path: Path) -> None:
    write_manifest(detectors_dir, "alpha", default_params={"threshold": 0.5, "keep": "me"})
    config = tmp_path / "detectors.yaml"
    config.write_text(yaml.safe_dump({"detectors": [{"id": "alpha", "params": {"threshold": 0.9}}]}), encoding="utf-8")

    registry = build_registry(detectors_dir=detectors_dir, config_path=config, strict=True)
    params = registry.get("alpha").params
    assert params["threshold"] == 0.9, "override wins"
    assert params["keep"] == "me", "unmentioned defaults survive"


def test_detector_absent_from_config_defaults_to_enabled(detectors_dir: Path, tmp_path: Path) -> None:
    write_manifest(detectors_dir, "alpha")
    config = tmp_path / "detectors.yaml"
    config.write_text(yaml.safe_dump({"detectors": [{"id": "other", "enabled": False}]}), encoding="utf-8")
    registry = build_registry(detectors_dir=detectors_dir, config_path=config, strict=True)
    assert registry.get("alpha").enabled is True


def test_missing_config_file_is_not_an_error(tmp_path: Path) -> None:
    assert load_detectors_config(tmp_path / "nope.yaml") == {}
    assert load_detectors_config(None) == {}


def test_config_entry_without_id_is_rejected(tmp_path: Path) -> None:
    config = tmp_path / "detectors.yaml"
    config.write_text(yaml.safe_dump({"detectors": [{"enabled": True}]}), encoding="utf-8")
    with pytest.raises(DetectorRegistryError, match="needs an `id`"):
        load_detectors_config(config)


# ── data type routing ───────────────────────────────────────────────────────


def test_null_data_types_accepts_everything(detectors_dir: Path) -> None:
    write_manifest(detectors_dir, "alpha", data_types=None)
    registration = build_registry(detectors_dir=detectors_dir, config_path=None, strict=True).get("alpha")
    assert registration.accepts_data_type("surveillance")
    assert registration.accepts_data_type(None)


def test_specific_data_types_reject_unknown_units(detectors_dir: Path) -> None:
    write_manifest(detectors_dir, "alpha", data_types=["surveillance"])
    registration = build_registry(detectors_dir=detectors_dir, config_path=None, strict=True).get("alpha")
    assert registration.accepts_data_type("surveillance")
    assert not registration.accepts_data_type("telecom")
    assert not registration.accepts_data_type(None), "a type-specific detector must not guess on unknown data"


# ── the real shipped manifests ──────────────────────────────────────────────


def test_shipped_manifests_all_validate() -> None:
    """Whatever is actually in the tree must load under strict validation."""
    registry = build_registry(config_path=None, strict=True)
    for registration in registry.all():
        assert registration.detector_id
        assert set(registration.gates) == set(registration.violation_types)


def test_re_identify_is_output_gate_only_across_all_detectors() -> None:
    """Guide hard constraint: violation type 9 is detected only at the output gate."""
    registry = build_registry(config_path=None, strict=True)
    for registration in registry.all():
        gates = registration.gates.get("re_identify")
        if gates:
            assert set(gates) == {"OutputGate"}, f"{registration.detector_id} declares re_identify outside the output gate: {gates}"
