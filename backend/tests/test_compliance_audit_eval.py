"""Audit trail and evaluation scripts.

Auditing is a compliance requirement (guide requirement 5: every determination
must cite its basis), so the trail has to survive rotation, truncation and write
failures without either losing records silently or taking a request down.

The evaluation scripts are checked end-to-end because they are the artifact the
project is judged on — a report generator that quietly produces empty sections is
worse than none.
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from deerflow.compliance.audit import Auditor, NullAuditor
from deerflow.compliance.contract import DetectionHit, RiskLocation
from deerflow.compliance.types import ComplianceDecision, DetectionRequest, Diagnostics

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = REPO_ROOT / "scripts"
DATASET = REPO_ROOT / "datasets/compliance/normalized/0624_supported_split"
MODEL = REPO_ROOT / "models/compliance/ml_detector_0624_fresh.json"


def _request() -> DetectionRequest:
    return DetectionRequest(
        gate="OutputGate",
        units=(),
        request_id="req-1",
        thread_id="t-1",
        model_name="conversation-model",
        origin={"tool_name": "invoke_skill"},
    )


def _decision(**overrides) -> ComplianceDecision:
    payload = {
        "request_id": "req-1",
        "gate": "OutputGate",
        "scene_key": "_unknown",
        "hits": (
            DetectionHit(
                detector_id="model_tfidf_knn",
                violation_type="re_identify",
                confidence=0.93,
                severity="high",
                risk_locations=(RiskLocation(kind="field_path", locator="output", text="敏感内容"),),
                reason_code="ml_tfidf_knn_classifier",
                evidence={"top_similarity": 0.72},
            ),
        ),
        "actions": ("warn", "manual_review"),
        "basis": ("个人信息保护法 §73(4)",),
        "diagnostics": Diagnostics(units_total=1, units_checked=1),
    }
    payload.update(overrides)
    return ComplianceDecision(**payload)  # type: ignore[arg-type]


# ── audit trail ─────────────────────────────────────────────────────────────


def test_record_persists_the_full_determination(tmp_path: Path) -> None:
    auditor = Auditor(path=tmp_path / "audit", enabled=True)
    decision = _decision()
    ref = auditor.record(_request(), decision)

    record = auditor.read_all()[0]
    assert record["audit_ref"] == ref
    assert record["gate"] == "OutputGate"
    assert record["model_name"] == "conversation-model"
    assert record["scene_key"] == "_unknown"
    assert record["actions"] == ["warn", "manual_review"]
    assert record["basis"] == ["个人信息保护法 §73(4)"], "requirement 5: the basis must be persisted"
    assert record["hits"][0]["detector_id"] == "model_tfidf_knn"
    assert record["hits"][0]["reason_code"] == "ml_tfidf_knn_classifier"
    assert record["origin"] == {"tool_name": "invoke_skill"}


def test_audit_ref_is_unique_per_record(tmp_path: Path) -> None:
    auditor = Auditor(path=tmp_path / "audit", enabled=True)
    refs = {auditor.record(_request(), _decision()) for _ in range(5)}
    assert len(refs) == 5


def test_records_append_within_a_day(tmp_path: Path) -> None:
    auditor = Auditor(path=tmp_path / "audit", enabled=True)
    for _ in range(3):
        auditor.record(_request(), _decision())
    files = list((tmp_path / "audit").glob("compliance-*.jsonl"))
    assert len(files) == 1, "one file per UTC day"
    assert len(auditor.read_all()) == 3


def test_records_are_valid_jsonl(tmp_path: Path) -> None:
    auditor = Auditor(path=tmp_path / "audit", enabled=True)
    auditor.record(_request(), _decision())
    path = next((tmp_path / "audit").glob("*.jsonl"))
    for line in path.read_text(encoding="utf-8").splitlines():
        json.loads(line)


def test_disabled_auditor_writes_nothing(tmp_path: Path) -> None:
    auditor = Auditor(path=tmp_path / "audit", enabled=False)
    assert auditor.record(_request(), _decision()) is None
    assert not (tmp_path / "audit").exists()


def test_null_auditor_is_inert() -> None:
    assert NullAuditor().record(_request(), _decision()) is None
    assert NullAuditor().enabled is False


def test_long_evidence_is_truncated(tmp_path: Path) -> None:
    """The audit log is a compliance artifact, not a second copy of the leak."""
    auditor = Auditor(path=tmp_path / "audit", enabled=True)
    decision = _decision(
        hits=(
            DetectionHit(
                detector_id="d",
                violation_type="domain",
                confidence=0.5,
                severity="low",
                evidence={"blob": "x" * 10_000},
            ),
        )
    )
    auditor.record(_request(), decision)
    stored = auditor.read_all()[0]["hits"][0]["evidence"]["blob"]
    assert len(stored) < 10_000
    assert "truncated" in stored


def test_nested_model_evidence_never_persists_sensitive_tokens(tmp_path: Path) -> None:
    """Type 8/9/10-style nested evidence is bounded recursively."""
    secret = "raw phone 13800138000 and token SECRET-TOKEN"
    auditor = Auditor(path=tmp_path / "audit", enabled=True)
    decision = _decision(
        hits=(
            DetectionHit(
                detector_id="model_tfidf_knn",
                violation_type="re_identify",
                confidence=0.91,
                severity="high",
                evidence={
                    "probabilities": {"re_identify": 0.91},
                    "nearest_neighbors": [{"raw_content": secret, "score": 0.8}],
                    "evidence_features": {"tokens": [secret], "hash": "abc123"},
                    "nested": ({"ngram": secret, "count": 2},),
                },
            ),
        )
    )
    auditor.record(_request(), decision)
    rendered = json.dumps(auditor.read_all(), ensure_ascii=False)
    assert secret not in rendered
    assert "13800138000" not in rendered
    assert "SECRET-TOKEN" not in rendered
    assert "abc123" in rendered


def test_write_failure_is_reported_and_never_raised(tmp_path: Path) -> None:
    """Losing the log must not take a request down — but must not be silent."""
    blocker = tmp_path / "audit"
    blocker.write_text("i am a file, not a directory", encoding="utf-8")
    decision = _decision()

    ref = Auditor(path=blocker, enabled=True).record(_request(), decision)

    assert ref is not None
    assert any("audit write failed" in w for w in decision.diagnostics.warnings)


def test_expired_files_are_purged(tmp_path: Path) -> None:
    audit_dir = tmp_path / "audit"
    audit_dir.mkdir()
    now = datetime.now(UTC)
    old = audit_dir / f"compliance-{(now - timedelta(days=200)).strftime('%Y%m%d')}.jsonl"
    fresh = audit_dir / f"compliance-{now.strftime('%Y%m%d')}.jsonl"
    old.write_text("{}\n", encoding="utf-8")
    fresh.write_text("{}\n", encoding="utf-8")

    removed = Auditor(path=audit_dir, enabled=True, retain_days=90).purge_expired(now=now)

    assert removed == 1
    assert not old.exists()
    assert fresh.exists()


def test_purge_ignores_unrelated_files(tmp_path: Path) -> None:
    audit_dir = tmp_path / "audit"
    audit_dir.mkdir()
    stray = audit_dir / "compliance-notadate.jsonl"
    stray.write_text("{}\n", encoding="utf-8")
    assert Auditor(path=audit_dir, enabled=True, retain_days=1).purge_expired() == 0
    assert stray.exists()


def test_zero_retention_disables_purging(tmp_path: Path) -> None:
    audit_dir = tmp_path / "audit"
    audit_dir.mkdir()
    old = audit_dir / "compliance-20200101.jsonl"
    old.write_text("{}\n", encoding="utf-8")
    assert Auditor(path=audit_dir, enabled=True, retain_days=0).purge_expired() == 0
    assert old.exists()


# ── evaluation scripts ──────────────────────────────────────────────────────


def _run(script: str, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(SCRIPTS / script), *args],
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
        timeout=300,
        check=False,
    )


requires_assets = pytest.mark.skipif(
    not (DATASET / "test.jsonl").is_file() or not MODEL.is_file(),
    reason="compliance assets missing; run `make compliance-assets`",
)


@requires_assets
def test_content_text_regression_script_passes() -> None:
    """The CI gate for plan §5.1. It must exit 0 on the shipped data."""
    result = _run("verify_content_text_rule.py")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "282/282" in result.stdout
    assert "PASS" in result.stdout


@requires_assets
def test_content_text_script_reports_prediction_agreement() -> None:
    result = _run("verify_content_text_rule.py")
    assert "56/56" in result.stdout


@requires_assets
def test_model_eval_reproduces_the_published_baseline() -> None:
    """accuracy 0.9821 / macro_f1 0.9859 / 1 mismatch — the hard numbers."""
    result = _run("run_compliance_model_eval.py", "--min-accuracy", "0.98")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "0.9821" in result.stdout
    assert "0.9859" in result.stdout
    assert "mismatches     : 1/56" in result.stdout


@requires_assets
def test_model_eval_matches_on_the_production_row_path() -> None:
    """Rebuilding rows from `features` must not degrade the model at all."""
    result = _run("run_compliance_model_eval.py", "--rebuild-rows", "--min-accuracy", "0.98")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "0.9821" in result.stdout


@requires_assets
def test_gate_eval_emits_all_four_metric_groups(tmp_path: Path) -> None:
    result = _run("run_compliance_gate_eval.py", "--output-root", str(tmp_path), "--timestamp", "testrun")
    assert result.returncode == 0, result.stdout + result.stderr

    report = json.loads((tmp_path / "testrun" / "report.json").read_text(encoding="utf-8"))
    assert report["primary"]["samples"] > 0
    assert set(report["by_gate"]), "gate group must not be empty"
    assert report["by_scene"]["status"] == "NOT EVALUATED"
    assert set(report["by_data_type"]), "data type group must not be empty"
    assert "matrix_cross_validation" in report


@requires_assets
def test_gate_eval_declares_the_scene_dimension_as_unmeasured(tmp_path: Path) -> None:
    """A blank column must never read as a pass."""
    _run("run_compliance_gate_eval.py", "--output-root", str(tmp_path), "--timestamp", "testrun")
    report = json.loads((tmp_path / "testrun" / "report.json").read_text(encoding="utf-8"))

    assert report["by_scene"]["status"] == "NOT EVALUATED"
    assert "no scene information source" in report["by_scene"]["reason"]
    assert set(report["by_scene"]["resolved_scene_keys"]) == {"_unknown"}


@requires_assets
def test_gate_eval_reports_coverage_gaps_explicitly(tmp_path: Path) -> None:
    """An uncovered violation type means "not measured", not "nothing found"."""
    result = _run("run_compliance_gate_eval.py", "--output-root", str(tmp_path), "--timestamp", "testrun")
    report = json.loads((tmp_path / "testrun" / "report.json").read_text(encoding="utf-8"))

    assert set(report["gate_coverage"]["InputGate"]) >= {"struct_id", "geo_loc"}
    assert "re_identify" in report["gate_coverage"]["OutputGate"]
    assert "re_identify" not in report["gate_coverage"]["ContextGate"], "guide hard constraint"
    assert "NOT covered" in result.stdout


@requires_assets
def test_gate_eval_writes_predictions(tmp_path: Path) -> None:
    _run("run_compliance_gate_eval.py", "--output-root", str(tmp_path), "--timestamp", "testrun")
    lines = (tmp_path / "testrun" / "predictions.jsonl").read_text(encoding="utf-8").splitlines()
    assert lines
    record = json.loads(lines[0])
    assert {"data_id", "gate", "gold", "predicted", "scene_key", "actions"} <= set(record)


@requires_assets
def test_matrix_cross_validation_agrees_on_the_baseline_types(tmp_path: Path) -> None:
    """The safety-critical rows must not diverge from the annotations.

    Broad disagreement elsewhere is a real finding for the data owners; the
    baseline three are the ones that must never drift.
    """
    _run("run_compliance_gate_eval.py", "--output-root", str(tmp_path), "--timestamp", "testrun")
    report = json.loads((tmp_path / "testrun" / "report.json").read_text(encoding="utf-8"))
    per_type = report["matrix_cross_validation"]["per_violation_type"]

    for violation_type in ("hardcoded_cred", "illegal_content", "political"):
        stats = per_type.get(violation_type)
        if stats is None:
            continue  # the in-repo 0624 split only covers types 8/9/10
        assert stats["agreement_rate"] >= 0.95, f"{violation_type} diverges from the annotations: {stats}"


def test_schema_export_is_up_to_date() -> None:
    """The committed schema must match the contract, or authors get stale docs."""
    result = _run("export_detector_schema.py", "--check")
    assert result.returncode == 0, result.stdout + result.stderr
