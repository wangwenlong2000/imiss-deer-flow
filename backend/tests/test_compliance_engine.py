"""Engine orchestration: routing, fault isolation, budget honesty, audit trail.

Covers the P0 completion criteria — the engine must be useful with zero
detectors, and must never let one detector's failure become everyone's failure.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from deerflow.compliance.audit import Auditor, NullAuditor
from deerflow.compliance.contract import DetectionHit, DetectionUnit, IntentInfo, TextItem
from deerflow.compliance.engine import ComplianceEngine
from deerflow.compliance.policy import load_policy_matrix
from deerflow.compliance.registry import DetectorRegistration, DetectorRegistry
from deerflow.compliance.scene import NullSceneResolver

MATRIX_PATH = Path(__file__).resolve().parents[2] / "config/compliance/policy_matrix.yaml"


# ── test doubles ────────────────────────────────────────────────────────────


class _FakeAdapter:
    """Adapter stub: returns canned hits, or raises/sleeps on demand."""

    def __init__(self, hits=(), *, error: Exception | None = None, delay_s: float = 0.0) -> None:
        self.hits = list(hits)
        self.error = error
        self.delay_s = delay_s
        self.calls = 0
        self.seen_units: list[DetectionUnit] = []

    def setup(self) -> None:
        return None

    def detect(self, unit, ctx):  # noqa: ANN001
        self.calls += 1
        self.seen_units.append(unit)
        if self.delay_s:
            import time

            time.sleep(self.delay_s)
        if self.error:
            raise self.error
        return list(self.hits)

    def close(self) -> None:
        return None


def _registration(detector_id: str, adapter, *, gates=None, data_types=None, cost="light", enabled=True) -> DetectorRegistration:
    gates = gates or {"domain": ("ContextGate", "OutputGate")}
    return DetectorRegistration(
        detector_id=detector_id,
        adapter_name="inprocess",
        violation_types=tuple(gates),
        gates={k: tuple(v) for k, v in gates.items()},
        data_types=data_types,
        cost_hint=cost,
        params={},
        manifest_path=Path("/virtual/manifest.yaml"),
        adapter=adapter,
        enabled=enabled,
    )


def _engine(registrations, *, auditor=None) -> ComplianceEngine:
    return ComplianceEngine(
        registry=DetectorRegistry(list(registrations)),
        policy=load_policy_matrix(MATRIX_PATH),
        auditor=auditor or NullAuditor(),
        scene_resolver=NullSceneResolver(),
    )


def _unit(unit_id="u-1", gate="OutputGate", data_type=None, text="hello") -> DetectionUnit:
    return DetectionUnit(
        unit_id=unit_id,
        gate=gate,
        data_type=data_type,
        text_items=(TextItem(item_id="t-1", text=text, source="output"),),
    )


def _hit(violation_type="domain", detector_id="d1") -> DetectionHit:
    return DetectionHit(detector_id=detector_id, violation_type=violation_type, confidence=0.9, severity="high")


# ── P0: empty engine ────────────────────────────────────────────────────────


def test_engine_with_zero_detectors_allows_everything() -> None:
    """P0 completion criterion: no detectors registered must not be an error."""
    decision = _engine([]).check([_unit()], gate="OutputGate")
    assert decision.hits == ()
    assert decision.actions == ()
    assert not decision.violated
    assert not decision.blocks
    assert not decision.diagnostics.detector_errors


def test_engine_with_no_units_is_a_clean_pass() -> None:
    decision = _engine([_registration("d1", _FakeAdapter([_hit()]))]).check([], gate="OutputGate")
    assert decision.hits == ()
    assert decision.diagnostics.units_total == 0


# ── routing ─────────────────────────────────────────────────────────────────


def test_detector_is_called_only_at_its_declared_gates() -> None:
    adapter = _FakeAdapter([_hit()])
    engine = _engine([_registration("d1", adapter, gates={"domain": ("OutputGate",)})])

    engine.check([_unit(gate="ContextGate")], gate="ContextGate")
    assert adapter.calls == 0, "not routed to a gate it does not declare"

    engine.check([_unit(gate="OutputGate")], gate="OutputGate")
    assert adapter.calls == 1


def test_disabled_detector_is_never_routed() -> None:
    adapter = _FakeAdapter([_hit()])
    engine = _engine([_registration("d1", adapter, enabled=False)])
    decision = engine.check([_unit()], gate="OutputGate")
    assert adapter.calls == 0
    assert decision.hits == ()


def test_data_type_specific_detector_is_skipped_for_other_types() -> None:
    adapter = _FakeAdapter([_hit()])
    engine = _engine([_registration("d1", adapter, data_types=("surveillance",))])
    engine.check([_unit(data_type="telecom")], gate="OutputGate")
    assert adapter.calls == 0
    engine.check([_unit(data_type="surveillance")], gate="OutputGate")
    assert adapter.calls == 1


def test_cheaper_detectors_run_first() -> None:
    """Under a tight budget, spend it on the detectors most likely to finish."""
    order: list[str] = []

    class _Recorder(_FakeAdapter):
        def __init__(self, name: str) -> None:
            super().__init__([])
            self.name = name

        def detect(self, unit, ctx):  # noqa: ANN001
            order.append(self.name)
            return []

    engine = _engine(
        [
            _registration("heavy_one", _Recorder("heavy"), cost="heavy", gates={"domain": ("OutputGate",)}),
            _registration("light_one", _Recorder("light"), cost="light", gates={"domain": ("OutputGate",)}),
        ]
    )
    engine.check([_unit()], gate="OutputGate")
    assert order == ["light", "heavy"]


def test_hit_outside_the_manifest_gate_is_dropped() -> None:
    """Guide hard constraint enforced twice: a detector bug cannot smuggle it in."""
    adapter = _FakeAdapter([_hit("re_identify")])
    engine = _engine([_registration("d1", adapter, gates={"domain": ("ContextGate",)})])
    decision = engine.check([_unit(gate="ContextGate")], gate="ContextGate")
    assert decision.hits == ()
    assert any("does not declare" in w for w in decision.diagnostics.warnings)


# ── fault isolation (plan §4.1) ─────────────────────────────────────────────


def test_a_failing_detector_does_not_stop_the_others() -> None:
    broken = _FakeAdapter(error=RuntimeError("boom"))
    working = _FakeAdapter([_hit(detector_id="good")])
    engine = _engine(
        [
            _registration("broken", broken, gates={"domain": ("OutputGate",)}),
            _registration("good", working, gates={"domain": ("OutputGate",)}),
        ]
    )
    decision = engine.check([_unit()], gate="OutputGate")

    assert [h.detector_id for h in decision.hits] == ["good"], "the healthy detector still reports"
    assert "broken" in decision.diagnostics.detector_errors
    assert any("broken" in w for w in decision.diagnostics.warnings), "the failure is visible, not swallowed"


def test_detector_failure_is_recorded_with_its_exception_type() -> None:
    engine = _engine([_registration("broken", _FakeAdapter(error=ValueError("bad input")), gates={"domain": ("OutputGate",)})])
    decision = engine.check([_unit()], gate="OutputGate")
    assert "ValueError" in decision.diagnostics.detector_errors["broken"]


def test_timings_are_recorded_per_detector() -> None:
    engine = _engine([_registration("d1", _FakeAdapter([]), gates={"domain": ("OutputGate",)})])
    decision = engine.check([_unit()], gate="OutputGate")
    assert "d1" in decision.diagnostics.detector_timings_ms


# ── data boundary ───────────────────────────────────────────────────────────


def test_detectors_receive_a_read_only_copy_of_raw() -> None:
    """A detector must not be able to mutate what another detector then sees."""
    adapter = _FakeAdapter([])
    engine = _engine([_registration("d1", adapter, gates={"domain": ("OutputGate",)})])
    original = DetectionUnit(unit_id="u", gate="OutputGate", raw={"secret": "value"})

    engine.check([original], gate="OutputGate")

    seen = adapter.seen_units[0]
    with pytest.raises(TypeError):
        seen.raw["secret"] = "tampered"  # type: ignore[index]
    assert original.raw["secret"] == "value"


# ── budget honesty (plan §6.2) ──────────────────────────────────────────────


def test_unit_truncation_writes_an_explicit_warning() -> None:
    """Silent truncation is the failure mode: 32-of-400 must never read as a pass."""
    adapter = _FakeAdapter([])
    engine = _engine([_registration("d1", adapter, gates={"domain": ("OutputGate",)})])
    units = [_unit(unit_id=f"u-{i}") for i in range(10)]

    decision = engine.check(units, gate="OutputGate", max_units=3)

    assert decision.diagnostics.truncated
    assert decision.diagnostics.units_total == 10
    assert decision.diagnostics.units_checked == 3
    assert adapter.calls == 3
    warning = " ".join(decision.diagnostics.warnings)
    assert "7 unit(s) were NOT checked" in warning


def test_time_budget_exhaustion_is_reported() -> None:
    slow = _FakeAdapter([], delay_s=0.05)
    engine = _engine([_registration("slow", slow, gates={"domain": ("OutputGate",)})])
    units = [_unit(unit_id=f"u-{i}") for i in range(20)]

    decision = engine.check(units, gate="OutputGate", max_units=20, budget_ms=60)

    assert decision.diagnostics.truncated
    assert any("detection stopped after" in w for w in decision.diagnostics.warnings)
    assert slow.calls < 20


# ── policy integration ──────────────────────────────────────────────────────


def test_scene_is_unknown_in_phase_one() -> None:
    """Plan §7.2: scenes are always empty until a real resolver is wired up."""
    engine = _engine([_registration("d1", _FakeAdapter([_hit()]), gates={"domain": ("OutputGate",)})])
    decision = engine.check([_unit()], gate="OutputGate")
    assert decision.scene_key == "_unknown"
    assert decision.scenes == ()


def test_hits_resolve_to_matrix_actions_and_basis() -> None:
    engine = _engine([_registration("d1", _FakeAdapter([_hit()]), gates={"domain": ("OutputGate",)})])
    decision = engine.check([_unit()], gate="OutputGate")
    assert decision.actions == ("warn", "manual_review")
    assert decision.per_violation_actions == {"domain": ("warn", "manual_review")}
    assert decision.basis, "guide requirement 5: a determination cites its basis"
    assert not decision.blocks, "warn + manual_review must not mutate the payload"


def test_baseline_violation_blocks_even_with_unknown_scene() -> None:
    engine = _engine([_registration("d1", _FakeAdapter([_hit("political")]), gates={"political": ("OutputGate",)})])
    decision = engine.check([_unit()], gate="OutputGate")
    assert "refuse" in decision.actions
    assert decision.blocks
    assert decision.strongest == "refuse"


# ── input gate intent coupling (guide §9.7) ─────────────────────────────────


def test_benign_intent_downgrades_a_non_baseline_input_hit() -> None:
    """"Please mask the phone numbers" must not itself be a violation."""
    engine = _engine([_registration("d1", _FakeAdapter([_hit("struct_id")]), gates={"struct_id": ("InputGate",)})])
    decision = engine.check(
        [_unit(gate="InputGate")],
        gate="InputGate",
        intent=IntentInfo(intent="desensitize_request", high_risk=False),
    )
    assert decision.hits == ()
    assert any("not high-risk" in w for w in decision.diagnostics.warnings)


def test_high_risk_intent_keeps_the_hit() -> None:
    engine = _engine([_registration("d1", _FakeAdapter([_hit("struct_id")]), gates={"struct_id": ("InputGate",)})])
    decision = engine.check(
        [_unit(gate="InputGate")],
        gate="InputGate",
        intent=IntentInfo(intent="export_for_public_release", high_risk=True),
    )
    assert len(decision.hits) == 1


def test_baseline_violation_survives_benign_intent() -> None:
    """Hardcoded credentials in a query are a leak whatever the user meant."""
    engine = _engine([_registration("d1", _FakeAdapter([_hit("hardcoded_cred")]), gates={"hardcoded_cred": ("InputGate",)})])
    decision = engine.check(
        [_unit(gate="InputGate")],
        gate="InputGate",
        intent=IntentInfo(intent="just_asking", high_risk=False),
    )
    assert len(decision.hits) == 1
    assert "refuse" in decision.actions


def test_missing_intent_signal_keeps_hits_and_says_so() -> None:
    """A missing signal is not an all-clear."""
    engine = _engine([_registration("d1", _FakeAdapter([_hit("struct_id")]), gates={"struct_id": ("InputGate",)})])
    decision = engine.check([_unit(gate="InputGate")], gate="InputGate", intent=None)
    assert len(decision.hits) == 1
    assert any("no intent signal" in w for w in decision.diagnostics.warnings)


def test_intent_does_not_filter_the_output_gate() -> None:
    """Content already generated is not excused by a benign request."""
    engine = _engine([_registration("d1", _FakeAdapter([_hit()]), gates={"domain": ("OutputGate",)})])
    decision = engine.check([_unit()], gate="OutputGate", intent=IntentInfo(intent="benign", high_risk=False))
    assert len(decision.hits) == 1


def test_role_check_degrades_to_a_warning_without_an_auth_module() -> None:
    """Phase 1 risk 12: say so rather than pretending the check happened."""
    engine = _engine([_registration("d1", _FakeAdapter([_hit()]), gates={"domain": ("OutputGate",)})])
    decision = engine.check([_unit()], gate="OutputGate", intent=IntentInfo(high_risk=True))
    # domain/_unknown is [warn, manual_review]; force a role_check cell instead.
    resolution = engine.policy.resolve("domain", "OutputGate", "internal_org")
    assert resolution.actions == ("role_check",)
    assert decision.actions == ("warn", "manual_review")


# ── audit trail ─────────────────────────────────────────────────────────────


def test_audit_record_is_written_and_referenced(tmp_path: Path) -> None:
    auditor = Auditor(path=tmp_path / "audit", enabled=True)
    engine = _engine([_registration("d1", _FakeAdapter([_hit()]), gates={"domain": ("OutputGate",)})], auditor=auditor)

    decision = engine.check([_unit()], gate="OutputGate", thread_id="t-1")

    assert decision.audit_ref is not None
    records = auditor.read_all()
    assert len(records) == 1
    record = records[0]
    assert record["audit_ref"] == decision.audit_ref
    assert record["gate"] == "OutputGate"
    assert record["scene_key"] == "_unknown"
    assert record["hits"][0]["violation_type"] == "domain"
    assert record["basis"], "the basis must be persisted, not just computed"


def test_clean_pass_writes_no_audit_record(tmp_path: Path) -> None:
    auditor = Auditor(path=tmp_path / "audit", enabled=True)
    engine = _engine([_registration("d1", _FakeAdapter([]), gates={"domain": ("OutputGate",)})], auditor=auditor)
    decision = engine.check([_unit()], gate="OutputGate")
    assert decision.audit_ref is None
    assert auditor.read_all() == []


def test_audit_failure_is_reported_but_never_raises(tmp_path: Path) -> None:
    """Losing the log must not take a request down — but the gap must be visible."""
    blocker = tmp_path / "audit"
    blocker.write_text("not a directory", encoding="utf-8")
    auditor = Auditor(path=blocker, enabled=True)
    engine = _engine([_registration("d1", _FakeAdapter([_hit()]), gates={"domain": ("OutputGate",)})], auditor=auditor)

    decision = engine.check([_unit()], gate="OutputGate")

    assert any("audit write failed" in w for w in decision.diagnostics.warnings)


def test_audit_truncates_oversized_evidence(tmp_path: Path) -> None:
    """The audit log is a compliance artifact, not a second copy of the leak."""
    auditor = Auditor(path=tmp_path / "audit", enabled=True)
    big = DetectionHit(detector_id="d1", violation_type="domain", confidence=0.9, severity="high", evidence={"blob": "x" * 5000})
    engine = _engine([_registration("d1", _FakeAdapter([big]), gates={"domain": ("OutputGate",)})], auditor=auditor)

    engine.check([_unit()], gate="OutputGate")

    stored = auditor.read_all()[0]["hits"][0]["evidence"]["blob"]
    assert "truncated" in stored
    assert len(stored) < 5000


def test_decision_serializes_for_transport() -> None:
    engine = _engine([_registration("d1", _FakeAdapter([_hit()]), gates={"domain": ("OutputGate",)})])
    payload = engine.check([_unit()], gate="OutputGate").to_dict()
    assert payload["gate"] == "OutputGate"
    assert payload["hits"][0]["violation_type"] == "domain"
    assert "diagnostics" in payload
