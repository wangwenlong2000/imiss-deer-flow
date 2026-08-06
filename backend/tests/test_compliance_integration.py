"""Real integration: no stubs anywhere in this file.

Every other compliance test injects a fake engine so it can assert one behaviour
in isolation. That leaves exactly one path untested — and it is the one that
matters in production:

    middleware -> get_engine() -> build_engine() -> registry -> real detector -> real model

This file exercises that path end to end with the real config, the real manifest,
the real 6.7 MB model and real annotated samples. It is slower than the rest of
the suite on purpose.

It also pins down the residual risk from plan §11 item 4: whether the field paths
a live ``SkillResult`` produces are close enough to the training samples'
``features`` keys for the model to still fire.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from deerflow.agents.middlewares.compliance_context_gate_middleware import ComplianceContextGateMiddleware
from deerflow.agents.middlewares.compliance_output_gate_middleware import ComplianceOutputGateMiddleware
from deerflow.compliance.contract import DetectContext
from deerflow.compliance.detectors.model_tfidf_knn.detector import ModelTfidfKnnDetector, clear_model_cache
from deerflow.compliance.engine import build_engine, reset_engine
from deerflow.compliance.normalizers.skill_result import SkillResultNormalizer
from deerflow.config.compliance_config import ComplianceConfig, get_compliance_config, set_compliance_config

REPO_ROOT = Path(__file__).resolve().parents[2]
MODEL = REPO_ROOT / "models/compliance/ml_detector_0624_fresh.json"
SAMPLES = REPO_ROOT / "datasets/compliance/normalized/0624_supported_split/all_supported_deduplicated.jsonl"

pytestmark = pytest.mark.skipif(
    not MODEL.is_file() or not SAMPLES.is_file(),
    reason="compliance assets missing; run `make compliance-assets`",
)


@pytest.fixture()
def live_config(tmp_path):
    """Enable compliance for real, with the audit trail pointed at tmp_path.

    The default audit directory lives under ``backend/.deer-flow/``, which in a
    Docker deployment is owned by root; writing there from a test would fail for
    reasons that have nothing to do with the code under test.
    """
    original = get_compliance_config()
    set_compliance_config(
        ComplianceConfig(
            enabled=True,
            audit={"enabled": True, "path": str(tmp_path / "audit")},
        )
    )
    reset_engine()
    yield get_compliance_config()
    set_compliance_config(original)
    reset_engine()
    clear_model_cache()


@pytest.fixture(scope="module")
def samples() -> list[dict]:
    return [json.loads(line) for line in SAMPLES.read_text(encoding="utf-8").splitlines() if line.strip()]


def _positive(samples: list[dict], violation_type: str, gate: str) -> dict:
    return next(
        s for s in samples if s["final_violation_type"] == violation_type and (s.get("gate") or s.get("trigger_gate")) == gate
    )


def _skill_result(payload: object, display_text: str = "") -> dict:
    """Wrap a payload the way a real skill returns structured evidence."""
    return {
        "schema_version": "1.0",
        "request_id": "req-integration",
        "skill_name": "city-video-intelligence",
        "status": "success",
        "result": {
            "display_text": display_text,
            "summary": {"title": "result", "overview": "overview"},
            "findings": [],
            "evidence": [{"evidence_id": "e-001", "type": "table", "title": "record", "data": payload, "metadata": {"score": 0.9}}],
            "artifacts": [],
        },
        "diagnostics": {"warnings": []},
        "errors": [],
    }


class _Request:
    tool_call = {"name": "invoke_skill", "id": "tc-1"}


# ── the production assembly path ────────────────────────────────────────────


def test_build_engine_assembles_the_real_detector(live_config) -> None:
    """`get_engine()` -> `build_engine()` -> registry -> real detector.

    Every other test constructs `ComplianceEngine(...)` by hand, so this wiring
    was never executed until it was exercised here.
    """
    engine = build_engine(live_config)

    registered = [(r.detector_id, r.adapter_name, r.enabled) for r in engine.registry.all()]
    assert ("model_tfidf_knn", "inprocess", True) in registered
    assert engine.policy.version == "7.23"


def test_engine_singleton_is_reused(live_config) -> None:
    """Loading a 6.7 MB model per request would be a latency bug."""
    from deerflow.compliance.engine import get_engine

    assert get_engine() is get_engine()


# ── ContextGate, unstubbed ──────────────────────────────────────────────────


def test_context_gate_detects_a_real_sample_through_the_real_engine(live_config, samples) -> None:
    """The whole stack, no fakes: real config, real model, real annotated data."""
    sample = _positive(samples, "domain", "ContextGate")
    payload = _skill_result(sample["raw_content"])
    message = ToolMessage(content=json.dumps(payload, ensure_ascii=False, default=str), tool_call_id="tc-1", name="invoke_skill")

    result = ComplianceContextGateMiddleware().wrap_tool_call(_Request(), lambda r: message)

    assert result is not message, "a real violating skill result must be acted on"
    compliance = json.loads(result.content)["diagnostics"]["compliance"]
    assert compliance["violation_types"] == ["domain"]
    assert compliance["actions"] == ["warn", "manual_review"], "the _unknown scene column"
    assert compliance["audit_ref"], "a determination must be traceable"


def test_context_gate_leaves_a_clean_result_untouched(live_config, samples) -> None:
    """No false positive on an annotated negative — the identity check matters."""
    negative = next(s for s in samples if s["final_violation_type"] == "none" and (s.get("gate") or s.get("trigger_gate")) == "ContextGate")
    payload = _skill_result(negative["raw_content"])
    message = ToolMessage(content=json.dumps(payload, ensure_ascii=False, default=str), tool_call_id="tc-1", name="invoke_skill")

    assert ComplianceContextGateMiddleware().wrap_tool_call(_Request(), lambda r: message) is message


# ── OutputGate, unstubbed ───────────────────────────────────────────────────


def test_output_gate_retracts_a_real_re_identify_answer(live_config, samples, monkeypatch) -> None:
    """Both retraction layers fire against the real model."""
    sample = _positive(samples, "re_identify", "OutputGate")
    answer = sample["content_text"]

    events: list[dict] = []
    monkeypatch.setattr("langgraph.config.get_stream_writer", lambda: events.append)

    state = {"messages": [HumanMessage(content="q"), AIMessage(content=answer, id="m-1")]}
    result = ComplianceOutputGateMiddleware().after_model(state, runtime=None)

    assert result is not None, "a real re_identify answer must be retracted"
    rewritten = result["messages"][0]
    assert rewritten.response_metadata["compliance"]["violation_types"] == ["re_identify"]
    assert rewritten.id == "m-1", "same id so the reducer replaces the original"
    assert len(events) == 1, "layer 1: the screen retraction event"
    assert events[0]["type"] == "compliance_retract"


def test_audit_record_is_actually_written(live_config, samples, tmp_path) -> None:
    """End to end: a determination reaches disk with its compliance basis."""
    sample = _positive(samples, "domain", "ContextGate")
    payload = _skill_result(sample["raw_content"])
    message = ToolMessage(content=json.dumps(payload, ensure_ascii=False, default=str), tool_call_id="tc-1", name="invoke_skill")

    ComplianceContextGateMiddleware().wrap_tool_call(_Request(), lambda r: message)

    files = list((tmp_path / "audit").glob("compliance-*.jsonl"))
    assert files, "the audit trail must exist on disk"
    record = json.loads(files[0].read_text(encoding="utf-8").splitlines()[0])
    assert record["hits"][0]["violation_type"] == "domain"
    assert record["basis"], "guide requirement 5: the basis is persisted"


# ── plan §11 risk 4: do live field paths still detect? ──────────────────────


def test_skill_result_field_paths_do_not_break_detection(samples) -> None:
    """The residual risk from plan §11 item 4, measured rather than assumed.

    A live ``SkillResult`` nests business fields under ``evidence[].data``, so the
    normalizer emits ``data.source_split`` where the training sample had plain
    ``source_split``. Every runtime feature key therefore carries a prefix the
    model never saw.

    It turns out not to matter: character n-grams over the *values* dominate the
    path features. All 140 annotated positives are still detected.

    What this does NOT prove: that an unseen skill emitting different field
    *values* would be detected. This re-wraps content the model was trained on,
    so it validates the plumbing, not generalization.
    """
    detector = ModelTfidfKnnDetector()
    detector.setup({"model_path": str(MODEL)})
    normalizer = SkillResultNormalizer()

    checked = 0
    missed: list[tuple[str, str, str]] = []
    for sample in samples:
        gate = sample.get("gate") or sample.get("trigger_gate")
        gold = sample["final_violation_type"]
        if gate not in ("ContextGate", "OutputGate") or gold not in ("domain", "re_identify", "video_meta_leak"):
            continue
        units = [u for u in normalizer.to_units(_skill_result(sample["raw_content"]), gate=gate) if u.raw.get("kind") == "skill_evidence"]
        if not units:
            missed.append((sample["data_id"], gold, "no_unit"))
            continue
        hits = detector.detect(units[0], DetectContext(gate=gate))
        predicted = hits[0].violation_type if hits else "none"
        checked += 1
        if predicted != gold:
            missed.append((sample["data_id"], gold, predicted))

    assert checked >= 140, f"expected at least 140 comparable positives, got {checked}"
    assert not missed, f"{len(missed)}/{checked} positives lost through the live payload shape: {missed[:5]}"


def test_prefixed_field_paths_are_what_the_normalizer_actually_emits(samples) -> None:
    """Pin the prefix behaviour so a normalizer change is a visible decision."""
    sample = _positive(samples, "domain", "ContextGate")
    units = SkillResultNormalizer().to_units(_skill_result(sample["raw_content"]), gate="ContextGate")
    evidence_unit = next(u for u in units if u.raw.get("kind") == "skill_evidence")

    paths = {item.path for item in evidence_unit.field_items}
    assert paths, "evidence payload must flatten into field items"
    assert all(p.startswith("data.") or p.startswith("metadata.") for p in paths), f"unexpected path shape: {sorted(paths)[:5]}"


def test_detection_confidence_stays_high_on_the_live_shape(samples) -> None:
    """A detected-but-barely result would be a silent degradation.

    Guards the margin, not just the label: if a normalizer change pushed
    similarities toward the 0.20 threshold, everything would degrade to `low`
    severity and quietly stop being disposed of.
    """
    import statistics

    detector = ModelTfidfKnnDetector()
    detector.setup({"model_path": str(MODEL)})
    normalizer = SkillResultNormalizer()

    similarities = []
    for sample in samples:
        gate = sample.get("gate") or sample.get("trigger_gate")
        if gate not in ("ContextGate", "OutputGate") or sample["final_violation_type"] != "re_identify":
            continue
        units = [u for u in normalizer.to_units(_skill_result(sample["raw_content"]), gate=gate) if u.raw.get("kind") == "skill_evidence"]
        if not units:
            continue
        hits = detector.detect(units[0], DetectContext(gate=gate))
        if hits:
            similarities.append(hits[0].evidence["top_similarity"])

    assert similarities, "no re_identify positives were detected at all"
    median = statistics.median(similarities)
    assert median > 0.80, f"median nearest-neighbour similarity dropped to {median:.4f}; detection is degrading toward the threshold"
