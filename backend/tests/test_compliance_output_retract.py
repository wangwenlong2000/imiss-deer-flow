"""OutputGate retraction: event shape, persistence hygiene, scan timing.

The property that matters most is the second one: after ``after_model`` runs,
nothing that gets persisted may contain the violating text. Retracting it from
the screen but leaving it in the checkpoint means a page refresh brings it back.
"""

from __future__ import annotations

import json

import pytest
from langchain_core.messages import AIMessage, HumanMessage

from deerflow.agents.middlewares.compliance_output_gate_middleware import (
    RETRACT_EVENT,
    ComplianceOutputGateMiddleware,
    build_retract_event,
)
from deerflow.compliance.contract import DetectionHit, RiskLocation
from deerflow.compliance.types import ComplianceDecision, Diagnostics
from deerflow.config.compliance_config import ComplianceConfig, get_compliance_config, set_compliance_config

VIOLATING_TEXT = "监控点位 HK-XC-0431 位于虹桥路 1234 号，坐标 31.194512,121.432187，可定位到具体个人。"
SECRET = "HK-XC-0431"


@pytest.fixture(autouse=True)
def _config():
    original = get_compliance_config()
    set_compliance_config(ComplianceConfig(enabled=True))
    yield
    set_compliance_config(original)


class _StubEngine:
    def __init__(self, decision=None):
        self.decision = decision
        self.calls = []

    def check(self, units, **kwargs):  # noqa: ANN001, ANN003
        self.calls.append({"units": list(units), **kwargs})
        return self.decision or ComplianceDecision(request_id="r", gate="OutputGate", scene_key="_unknown")


def _decision(actions=("rewrite",), violation="video_meta_leak", locations=None) -> ComplianceDecision:
    if locations is None:
        locations = [RiskLocation(kind="field_path", locator="output", text=SECRET)]
    return ComplianceDecision(
        request_id="r-1",
        gate="OutputGate",
        scene_key="_unknown",
        hits=(
            DetectionHit(
                detector_id="model_tfidf_knn",
                violation_type=violation,
                confidence=0.95,
                severity="high",
                risk_locations=tuple(locations),
                reason_code="ml_tfidf_knn_classifier",
            ),
        ),
        actions=tuple(actions),
        per_violation_actions={violation: tuple(actions)},
        basis=("数据安全法 §21 重要数据",),
        audit_ref="audit-abc123",
        diagnostics=Diagnostics(units_total=1, units_checked=1),
    )


def _state(text=VIOLATING_TEXT, message_id="m-1"):
    return {"messages": [HumanMessage(content="问一下"), AIMessage(content=text, id=message_id)]}


# ── event structure (the frontend contract) ─────────────────────────────────


def test_event_type_is_stable() -> None:
    """The frontend keys off this exact string."""
    assert RETRACT_EVENT == "compliance_retract"


def test_event_carries_everything_the_frontend_needs() -> None:
    event = build_retract_event(AIMessage(content="x", id="m-9"), "safe text", _decision())

    assert event["type"] == "compliance_retract"
    assert event["message_id"] == "m-9", "the frontend locates the message by id"
    assert event["replacement"] == "safe text", "and replaces its content with this"
    assert event["violation_types"] == ["video_meta_leak"]
    assert event["action"] == ["rewrite"]
    assert event["audit_ref"] == "audit-abc123"
    assert event["basis"] == ["数据安全法 §21 重要数据"]
    assert event["notice"]


def test_event_is_json_serializable() -> None:
    """It travels over SSE."""
    event = build_retract_event(AIMessage(content="x", id="m-9"), "safe", _decision())
    assert json.loads(json.dumps(event, ensure_ascii=False))["type"] == "compliance_retract"


def test_event_never_leaks_the_original_text() -> None:
    """The retraction must not itself re-transmit what it is retracting."""
    event = build_retract_event(AIMessage(content=VIOLATING_TEXT, id="m-9"), "safe replacement", _decision())
    assert SECRET not in json.dumps(event, ensure_ascii=False)


def test_event_deduplicates_violation_types() -> None:
    decision = _decision()
    decision.hits = decision.hits + decision.hits
    assert build_retract_event(AIMessage(content="x", id="m"), "safe", decision)["violation_types"] == ["video_meta_leak"]


# ── persistence hygiene (layer 2) ───────────────────────────────────────────


def test_persisted_message_does_not_contain_the_violation() -> None:
    """The single most important assertion in this file."""
    engine = _StubEngine(_decision(actions=("rewrite",)))
    result = ComplianceOutputGateMiddleware(engine=engine).after_model(_state(), runtime=None)

    persisted = result["messages"][0]
    assert SECRET not in persisted.content
    assert "31.194512" not in persisted.content or "*" in persisted.content


def test_refuse_replaces_the_content_entirely() -> None:
    engine = _StubEngine(_decision(actions=("refuse",)))
    result = ComplianceOutputGateMiddleware(engine=engine).after_model(_state(), runtime=None)
    content = result["messages"][0].content
    assert SECRET not in content
    assert "虹桥路" not in content


def test_message_id_is_preserved_so_the_reducer_replaces() -> None:
    """A new id would append a second message and keep the original."""
    engine = _StubEngine(_decision(actions=("refuse",)))
    result = ComplianceOutputGateMiddleware(engine=engine).after_model(_state(message_id="m-42"), runtime=None)
    assert result["messages"][0].id == "m-42"


def test_rewritten_message_is_marked_in_metadata() -> None:
    """So a report exporter can tell an altered answer from an original one."""
    engine = _StubEngine(_decision(actions=("refuse",)))
    result = ComplianceOutputGateMiddleware(engine=engine).after_model(_state(), runtime=None)

    compliance = result["messages"][0].response_metadata["compliance"]
    assert compliance["retracted"] is True
    assert compliance["gate"] == "OutputGate"
    assert compliance["violation_types"] == ["video_meta_leak"]
    assert compliance["audit_ref"] == "audit-abc123"


def test_rewrite_is_labelled_as_altered() -> None:
    """Never hand the reader a silently doctored answer."""
    engine = _StubEngine(_decision(actions=("rewrite",)))
    result = ComplianceOutputGateMiddleware(engine=engine).after_model(_state(), runtime=None)
    assert "合规改写" in result["messages"][0].content


def test_aggregate_is_labelled_as_aggregated() -> None:
    engine = _StubEngine(_decision(actions=("aggregate",)))
    result = ComplianceOutputGateMiddleware(engine=engine).after_model(_state(), runtime=None)
    assert "合规聚合" in result["messages"][0].content


def test_unlocatable_risk_falls_back_to_refusal() -> None:
    """The matrix said do not release this as-is; returning it unchanged would
    defeat the decision just because offsets could not be resolved."""
    engine = _StubEngine(_decision(actions=("desensitize",), locations=[]))
    result = ComplianceOutputGateMiddleware(engine=engine).after_model(_state(), runtime=None)
    assert SECRET not in result["messages"][0].content


# ── event emission (layer 1) ────────────────────────────────────────────────


def test_event_is_emitted_alongside_the_rewrite(monkeypatch) -> None:
    """Both layers fire for the same violation."""
    captured: list[dict] = []
    monkeypatch.setattr("langgraph.config.get_stream_writer", lambda: captured.append)

    engine = _StubEngine(_decision(actions=("refuse",)))
    result = ComplianceOutputGateMiddleware(engine=engine).after_model(_state(), runtime=None)

    assert len(captured) == 1, "layer 1: screen retraction"
    assert result is not None, "layer 2: persistence rewrite"
    assert captured[0]["replacement"] == result["messages"][0].content


def test_no_event_for_a_clean_answer(monkeypatch) -> None:
    captured: list[dict] = []
    monkeypatch.setattr("langgraph.config.get_stream_writer", lambda: captured.append)
    ComplianceOutputGateMiddleware(engine=_StubEngine()).after_model(_state(), runtime=None)
    assert captured == []


# ── incremental scanning cadence ────────────────────────────────────────────


def test_scan_fires_once_per_interval() -> None:
    set_compliance_config(ComplianceConfig(enabled=True, gates={"output": {"incremental_scan": {"interval_chars": 100}}}))
    engine = _StubEngine()
    middleware = ComplianceOutputGateMiddleware(engine=engine)

    for length in (50, 99):
        assert middleware.scan_increment("m-1", "x" * length) is None
    assert engine.calls == []

    middleware.scan_increment("m-1", "x" * 100)
    assert len(engine.calls) == 1


def test_scan_reports_a_violation_as_soon_as_it_appears() -> None:
    set_compliance_config(ComplianceConfig(enabled=True, gates={"output": {"incremental_scan": {"interval_chars": 10}}}))
    engine = _StubEngine(_decision(actions=("refuse",)))
    decision = ComplianceOutputGateMiddleware(engine=engine).scan_increment("m-1", VIOLATING_TEXT)

    assert decision is not None
    assert "refuse" in decision.actions


def test_scan_marks_the_request_as_incremental_for_audit() -> None:
    set_compliance_config(ComplianceConfig(enabled=True, gates={"output": {"incremental_scan": {"interval_chars": 10}}}))
    engine = _StubEngine()
    ComplianceOutputGateMiddleware(engine=engine).scan_increment("m-1", "x" * 50)
    assert engine.calls[0]["origin"]["incremental"] is True


def test_scan_state_is_per_message() -> None:
    set_compliance_config(ComplianceConfig(enabled=True, gates={"output": {"incremental_scan": {"interval_chars": 50}}}))
    engine = _StubEngine()
    middleware = ComplianceOutputGateMiddleware(engine=engine)

    middleware.scan_increment("m-1", "x" * 60)
    middleware.scan_increment("m-2", "y" * 60)
    assert len(engine.calls) == 2, "a second message starts from zero"


def test_scan_state_can_be_reset() -> None:
    set_compliance_config(ComplianceConfig(enabled=True, gates={"output": {"incremental_scan": {"interval_chars": 50}}}))
    engine = _StubEngine()
    middleware = ComplianceOutputGateMiddleware(engine=engine)

    middleware.scan_increment("m-1", "x" * 60)
    middleware.reset_scan_state("m-1")
    middleware.scan_increment("m-1", "x" * 60)
    assert len(engine.calls) == 2


def test_scan_failure_falls_back_to_the_gate_fail_mode() -> None:
    class _Broken:
        def check(self, units, **kwargs):  # noqa: ANN001, ANN003
            raise RuntimeError("boom")

    set_compliance_config(ComplianceConfig(enabled=True, gates={"output": {"incremental_scan": {"interval_chars": 10}}}))
    decision = ComplianceOutputGateMiddleware(engine=_Broken()).scan_increment("m-1", "x" * 50)
    assert decision is not None
    assert "refuse" in decision.actions, "fail_mode closed must apply to the incremental path too"


# ── frontend contract mirror ────────────────────────────────────────────────


def test_frontend_handler_matches_the_backend_event_shape() -> None:
    """Guards the two halves of the contract against drifting apart."""
    from pathlib import Path

    compliance_ts = Path(__file__).resolve().parents[2] / "frontend/src/core/threads/compliance.ts"
    if not compliance_ts.is_file():
        pytest.skip("frontend not present in this checkout")

    source = compliance_ts.read_text(encoding="utf-8")
    for field in ("compliance_retract", "message_id", "replacement", "violation_types", "audit_ref"):
        assert field in source, f"the frontend handler does not reference `{field}`"
