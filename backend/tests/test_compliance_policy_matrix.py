"""Policy matrix: full coverage, the `_unknown` fallback, and the baseline floor.

The matrix is transcribed from the guide by hand. These tests are the safety net
for that transcription — especially the rule that the three baseline violation
types are refused in every scene, including when the scene is unknown.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from deerflow.compliance.contract import BASELINE_VIOLATION_TYPES, GATES, SCENES, VIOLATION_TYPES, DetectionHit
from deerflow.compliance.policy import PolicyMatrixError, load_policy_matrix

MATRIX_PATH = Path(__file__).resolve().parents[2] / "config/compliance/policy_matrix.yaml"


@pytest.fixture(scope="module")
def matrix():
    return load_policy_matrix(MATRIX_PATH)


def _hit(violation_type: str, detector_id: str = "test") -> DetectionHit:
    return DetectionHit(detector_id=detector_id, violation_type=violation_type, confidence=0.9, severity="high")


# ── completeness ────────────────────────────────────────────────────────────


def test_shipped_matrix_loads(matrix) -> None:
    assert matrix.version == "7.23"
    assert matrix.fallback_key == "_unknown"


def test_every_violation_type_is_covered(matrix) -> None:
    assert set(matrix.violation_types) == set(VIOLATION_TYPES)


@pytest.mark.parametrize("violation_type", VIOLATION_TYPES)
def test_every_violation_covers_all_three_gates(matrix, violation_type: str) -> None:
    for gate in GATES:
        assert matrix.scene_keys(violation_type, gate), f"{violation_type}.{gate} has no scene columns"


@pytest.mark.parametrize("violation_type", VIOLATION_TYPES)
def test_every_cell_covers_all_five_scenes_plus_fallback(matrix, violation_type: str) -> None:
    for gate in GATES:
        keys = set(matrix.scene_keys(violation_type, gate))
        assert set(SCENES) <= keys, f"{violation_type}.{gate} is missing scene(s) {set(SCENES) - keys}"
        assert "_unknown" in keys, f"{violation_type}.{gate} is missing the `_unknown` fallback column"


# ── the `_unknown` fallback column (plan §7.2) ──────────────────────────────


@pytest.mark.parametrize("violation_type", BASELINE_VIOLATION_TYPES)
def test_baseline_types_refuse_even_when_the_scene_is_unknown(matrix, violation_type: str) -> None:
    """Guide chapter 3 core rule: types 3/4/5 are refused in every scene."""
    for gate in GATES:
        resolution = matrix.resolve(violation_type, gate, "_unknown")
        assert "refuse" in resolution.actions, f"{violation_type}.{gate}._unknown must refuse, got {resolution.actions}"


@pytest.mark.parametrize("violation_type", BASELINE_VIOLATION_TYPES)
def test_baseline_types_refuse_in_every_named_scene(matrix, violation_type: str) -> None:
    for gate in ("InputGate", "OutputGate"):  # ContextGate is "—" for baseline types
        for scene in SCENES:
            actions = matrix.cell(violation_type, gate, scene)
            assert actions is not None and "refuse" in actions, f"{violation_type}.{gate}.{scene} must refuse, got {actions}"


NON_BASELINE = tuple(v for v in VIOLATION_TYPES if v not in BASELINE_VIOLATION_TYPES)


@pytest.mark.parametrize("violation_type", NON_BASELINE)
def test_non_baseline_unknown_scene_only_warns_and_reviews(matrix, violation_type: str) -> None:
    """Phase 1 principle: better to under-dispose than to misfire.

    With the scene unknown, auto-desensitizing or refusing would wreck `self_use`
    (which the guide says should be allowed). Warn + audit + human queue keeps
    every audit capability without the false-positive damage.
    """
    for gate in GATES:
        actions = matrix.cell(violation_type, gate, "_unknown")
        assert actions == ("warn", "manual_review"), f"{violation_type}.{gate}._unknown should be [warn, manual_review], got {actions}"


def test_unknown_column_is_documented_as_temporary() -> None:
    """TODO(scene) tracking: this column must disappear once scenes are real.

    Deliberately couples the removal of `_unknown` to a test failure, so nobody
    deletes the column without also revisiting the phase-1 fallbacks.
    """
    text = MATRIX_PATH.read_text(encoding="utf-8")
    assert "TODO(scene)" in text, "the `_unknown` column must carry an explicit removal TODO"
    assert "临时状态" in text


# ── lookup semantics ────────────────────────────────────────────────────────


def test_unresolved_scene_falls_back_rather_than_allowing(matrix) -> None:
    resolution = matrix.resolve("domain", "OutputGate", None)
    assert resolution.scene_key == "_unknown"
    assert resolution.actions == ("warn", "manual_review")


def test_unrecognized_scene_name_falls_back(matrix) -> None:
    resolution = matrix.resolve("domain", "OutputGate", "not_a_real_scene")
    assert resolution.scene_key == "_unknown"


def test_null_cell_is_reported_as_not_applicable(matrix) -> None:
    """"—" in the guide means "cannot happen here", not "allow"."""
    resolution = matrix.resolve("re_identify", "ContextGate", "self_use")
    assert resolution.not_applicable
    assert resolution.actions == ()


def test_matrix_supplies_the_compliance_basis(matrix) -> None:
    """Guide requirement 5: every determination cites its basis."""
    for violation_type in VIOLATION_TYPES:
        resolution = matrix.resolve(violation_type, "OutputGate", "_unknown")
        assert resolution.basis, f"{violation_type} has no compliance basis configured"


# ── merging several hits ────────────────────────────────────────────────────


def test_decide_merges_actions_weakest_first(matrix) -> None:
    actions, per_violation, basis, warnings = matrix.decide([_hit("domain"), _hit("re_identify")], "OutputGate", "_unknown")
    assert actions == ("warn", "manual_review")
    assert set(per_violation) == {"domain", "re_identify"}
    assert basis
    assert not warnings


def test_strongest_action_wins_when_hits_disagree(matrix) -> None:
    """A weak hit must never soften a strong one."""
    actions, _, _, _ = matrix.decide([_hit("domain"), _hit("political")], "OutputGate", "_unknown")
    assert actions[-1] == "refuse"


def test_allow_is_dropped_when_something_stronger_applies(matrix) -> None:
    actions, _, _, _ = matrix.decide([_hit("struct_id"), _hit("political")], "OutputGate", "self_use")
    assert "allow" not in actions
    assert "refuse" in actions


def test_hit_in_a_not_applicable_cell_warns_instead_of_acting(matrix) -> None:
    actions, per_violation, _, warnings = matrix.decide([_hit("re_identify", "rogue")], "ContextGate", "self_use")
    assert actions == ()
    assert per_violation == {}
    assert any("not applicable" in w for w in warnings)


def test_no_hits_means_no_actions(matrix) -> None:
    assert matrix.decide([], "OutputGate", "_unknown") == ((), {}, (), [])


# ── validation of malformed matrices ────────────────────────────────────────


def _write(tmp_path: Path, data: dict) -> Path:
    path = tmp_path / "matrix.yaml"
    path.write_text(yaml.safe_dump(data, allow_unicode=True), encoding="utf-8")
    return path


def _full_matrix() -> dict:
    return {
        "version": "test",
        "fallback_key": "_unknown",
        "basis": {v: ["clause"] for v in VIOLATION_TYPES},
        "matrix": {
            v: {g: {"_unknown": ["refuse", "warn"] if v in BASELINE_VIOLATION_TYPES else ["warn"], **{s: ["warn"] for s in SCENES}} for g in GATES}
            for v in VIOLATION_TYPES
        },
    }


def test_missing_violation_type_is_rejected(tmp_path: Path) -> None:
    data = _full_matrix()
    del data["matrix"]["domain"]
    with pytest.raises(PolicyMatrixError, match="missing violation type"):
        load_policy_matrix(_write(tmp_path, data))


def test_missing_gate_is_rejected(tmp_path: Path) -> None:
    data = _full_matrix()
    del data["matrix"]["domain"]["OutputGate"]
    with pytest.raises(PolicyMatrixError, match="missing gate"):
        load_policy_matrix(_write(tmp_path, data))


def test_missing_fallback_column_is_rejected(tmp_path: Path) -> None:
    data = _full_matrix()
    del data["matrix"]["domain"]["OutputGate"]["_unknown"]
    with pytest.raises(PolicyMatrixError, match="missing required fallback column"):
        load_policy_matrix(_write(tmp_path, data))


def test_unknown_action_is_rejected(tmp_path: Path) -> None:
    data = _full_matrix()
    data["matrix"]["domain"]["OutputGate"]["self_use"] = ["obliterate"]
    with pytest.raises(PolicyMatrixError, match="unknown action"):
        load_policy_matrix(_write(tmp_path, data))


def test_softening_a_baseline_type_is_rejected_at_load(tmp_path: Path) -> None:
    """The most important guard here: never let a re-transcription open a hole."""
    data = _full_matrix()
    data["matrix"]["political"]["OutputGate"]["_unknown"] = ["warn"]
    with pytest.raises(PolicyMatrixError, match="must include `refuse`"):
        load_policy_matrix(_write(tmp_path, data))


def test_missing_file_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(PolicyMatrixError, match="not found"):
        load_policy_matrix(tmp_path / "nope.yaml")
