"""Compliance configuration: defaults, parsing, and the shipped example.

The defaults matter more than usual here. A compliance gate that fails open, or
one that is on before its detectors are ready, both produce a system that *looks*
protected and is not.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from deerflow.config.compliance_config import (
    ComplianceConfig,
    get_compliance_config,
    load_compliance_config_from_dict,
    set_compliance_config,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLE_CONFIG = REPO_ROOT / "config.example.yaml"


@pytest.fixture(autouse=True)
def _restore_singleton():
    original = get_compliance_config()
    yield
    set_compliance_config(original)


# ── defaults ────────────────────────────────────────────────────────────────


def test_compliance_is_off_by_default() -> None:
    """A half-configured gate that blocks real traffic is worse than none."""
    assert ComplianceConfig().enabled is False


def test_every_gate_fails_closed_by_default() -> None:
    """A gate that fails open is indistinguishable from a gate that is off."""
    config = ComplianceConfig()
    assert config.gates.input.fail_mode == "closed"
    assert config.gates.context.fail_mode == "closed"
    assert config.gates.output.fail_mode == "closed"


def test_context_gate_budget_defaults() -> None:
    config = ComplianceConfig()
    assert config.gates.context.budget_ms == 400
    assert config.gates.context.max_units == 32


def test_output_gate_streams_and_never_buffers() -> None:
    config = ComplianceConfig()
    assert config.gates.output.mode == "stream_retract"
    assert config.gates.output.incremental_scan.enabled is True
    assert config.gates.output.incremental_scan.interval_chars == 200


def test_scene_resolver_is_unset_in_phase_one() -> None:
    config = ComplianceConfig()
    assert config.scene.resolver is None
    assert config.scene.fallback_key == "_unknown"


def test_audit_is_on_by_default() -> None:
    """Guide requirement 5 — losing the trail is not an acceptable default."""
    assert ComplianceConfig().audit.enabled is True
    assert ComplianceConfig().audit.retain_days == 90


# ── parsing ─────────────────────────────────────────────────────────────────


def test_load_from_dict_populates_the_singleton() -> None:
    load_compliance_config_from_dict({"enabled": True, "gates": {"context": {"budget_ms": 900}}})
    config = get_compliance_config()
    assert config.enabled is True
    assert config.gates.context.budget_ms == 900
    assert config.gates.input.fail_mode == "closed", "unmentioned sections keep their safe defaults"


def test_empty_dict_yields_defaults() -> None:
    load_compliance_config_from_dict({})
    assert get_compliance_config().enabled is False


def test_none_yields_defaults() -> None:
    load_compliance_config_from_dict(None)
    assert get_compliance_config().enabled is False


def test_unknown_keys_are_ignored_not_fatal() -> None:
    """Forward compatibility: a newer config must not break an older binary."""
    load_compliance_config_from_dict({"enabled": True, "future_feature": {"x": 1}})
    assert get_compliance_config().enabled is True


@pytest.mark.parametrize("fail_mode", ["closed", "open"])
def test_both_fail_modes_are_accepted(fail_mode: str) -> None:
    load_compliance_config_from_dict({"gates": {"context": {"fail_mode": fail_mode}}})
    assert get_compliance_config().gates.context.fail_mode == fail_mode


def test_invalid_fail_mode_is_rejected() -> None:
    with pytest.raises(ValueError):
        ComplianceConfig(gates={"context": {"fail_mode": "maybe"}})


def test_non_positive_budget_is_rejected() -> None:
    with pytest.raises(ValueError):
        ComplianceConfig(gates={"context": {"budget_ms": 0}})


def test_non_positive_scan_interval_is_rejected() -> None:
    with pytest.raises(ValueError):
        ComplianceConfig(gates={"output": {"incremental_scan": {"interval_chars": 0}}})


# ── the shipped example ─────────────────────────────────────────────────────


def test_example_config_has_a_compliance_section() -> None:
    data = yaml.safe_load(EXAMPLE_CONFIG.read_text(encoding="utf-8"))
    assert "compliance" in data, "config.example.yaml must document the compliance section"


def test_example_config_section_parses_into_the_model() -> None:
    data = yaml.safe_load(EXAMPLE_CONFIG.read_text(encoding="utf-8"))
    config = ComplianceConfig(**data["compliance"])
    assert config.enabled is False, "the shipped example must not enable compliance implicitly"
    assert config.gates.output.mode == "stream_retract"
    assert config.scene.resolver is None


def test_example_config_paths_point_at_real_files() -> None:
    data = yaml.safe_load(EXAMPLE_CONFIG.read_text(encoding="utf-8"))
    config = ComplianceConfig(**data["compliance"])
    assert (REPO_ROOT / config.detectors_config_path).is_file()
    assert (REPO_ROOT / config.policy_matrix_path).is_file()
