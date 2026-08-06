"""Startup preflight: an install failure must not become a full outage.

The failure mode this guards against is specific and was real: `models/` and
`config/` were not mounted into the Docker containers. Because `get_engine()`
builds lazily *on the request path* and both gates fail closed, that missing
mount would have replaced every answer and every tool result with a
compliance-failure notice — a total outage traced back to a volume line.

`fail_mode: closed` means "this content could not be judged". It must not also
mean "the system was never installed, so refuse everyone".
"""

from __future__ import annotations

from pathlib import Path

import pytest

from deerflow.agents.middlewares.tool_error_handling_middleware import (
    build_compliance_flow_middlewares,
    reset_compliance_preflight,
)
from deerflow.compliance.engine import reset_engine
from deerflow.compliance.preflight import CANARY_TEXT, run_preflight
from deerflow.config.compliance_config import ComplianceConfig, get_compliance_config, set_compliance_config

REPO_ROOT = Path(__file__).resolve().parents[2]
MODEL = REPO_ROOT / "models/compliance/ml_detector_0624_fresh.json"


@pytest.fixture(autouse=True)
def _isolate():
    original = get_compliance_config()
    reset_compliance_preflight()
    reset_engine()
    yield
    set_compliance_config(original)
    reset_compliance_preflight()
    reset_engine()


def _enable(**overrides):
    set_compliance_config(ComplianceConfig(enabled=True, **overrides))


# ── the happy path ──────────────────────────────────────────────────────────


@pytest.mark.skipif(not MODEL.is_file(), reason="compliance assets missing; run `make compliance-assets`")
def test_preflight_passes_with_real_assets() -> None:
    _enable()
    result = run_preflight()
    assert result.ok, result.describe()
    assert "model_tfidf_knn" in result.detectors


@pytest.mark.skipif(not MODEL.is_file(), reason="compliance assets missing")
def test_gates_mount_when_preflight_passes() -> None:
    _enable()
    names = [type(m).__name__ for m in build_compliance_flow_middlewares()]
    assert "ComplianceOutputGateMiddleware" in names
    assert "ComplianceContextGateMiddleware" in names


@pytest.mark.skipif(not MODEL.is_file(), reason="compliance assets missing")
def test_canary_text_is_benign() -> None:
    """The canary proves the pipeline runs; it must not itself trip a detector."""
    from deerflow.compliance.contract import DetectContext, DetectionUnit, TextItem
    from deerflow.compliance.detectors.model_tfidf_knn.detector import ModelTfidfKnnDetector

    detector = ModelTfidfKnnDetector()
    detector.setup({"model_path": str(MODEL)})
    unit = DetectionUnit(
        unit_id="canary",
        gate="OutputGate",
        text_items=(TextItem(item_id="t-1", text=CANARY_TEXT, source="preflight"),),
    )
    hits = detector.detect(unit, DetectContext(gate="OutputGate"))
    assert hits == (), f"the canary text must not be flagged, got {[h.violation_type for h in hits]}"


# ── the failure path — the whole point of this file ─────────────────────────


def test_missing_policy_matrix_fails_preflight(tmp_path: Path) -> None:
    _enable(policy_matrix_path=str(tmp_path / "nope.yaml"))
    result = run_preflight()
    assert not result.ok
    assert any("engine could not be built" in e for e in result.errors)


def test_missing_assets_mount_no_gates_instead_of_refusing_everything(tmp_path: Path) -> None:
    """The critical behaviour: degrade to unprotected, never to blocking-all.

    Mounting the gates here would make every request fail closed.
    """
    _enable(policy_matrix_path=str(tmp_path / "nope.yaml"))
    assert build_compliance_flow_middlewares() == []


def test_strict_startup_refuses_to_boot_instead(tmp_path: Path) -> None:
    """Some deployments prefer not running over running unprotected."""
    _enable(strict_startup=True, policy_matrix_path=str(tmp_path / "nope.yaml"))
    with pytest.raises(RuntimeError, match="strict_startup"):
        build_compliance_flow_middlewares()


def test_strict_startup_defaults_to_off() -> None:
    """Default must not let a missing file take the whole product down."""
    assert ComplianceConfig().strict_startup is False


@pytest.mark.skipif(not MODEL.is_file(), reason="compliance assets missing")
def test_preflight_runs_once_per_process() -> None:
    """It loads a 6.7 MB model; running it per agent build would be a latency bug."""
    _enable()
    build_compliance_flow_middlewares()

    calls = {"n": 0}
    import deerflow.compliance.preflight as preflight_module

    original = preflight_module.run_preflight
    try:
        def _counting():
            calls["n"] += 1
            return original()

        preflight_module.run_preflight = _counting
        build_compliance_flow_middlewares()
        build_compliance_flow_middlewares()
        assert calls["n"] == 0, "the verdict must be cached after the first run"
    finally:
        preflight_module.run_preflight = original


def test_disabled_compliance_skips_preflight_entirely(tmp_path: Path) -> None:
    """Turning compliance off must not pay for a model load."""
    set_compliance_config(ComplianceConfig(enabled=False, policy_matrix_path=str(tmp_path / "nope.yaml")))
    assert build_compliance_flow_middlewares() == []
