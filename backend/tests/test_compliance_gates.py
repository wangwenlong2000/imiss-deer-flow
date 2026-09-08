"""Gate middlewares: mount order, fail-mode semantics, budget truncation.

The ordering assertions here are the point of the file — pin the order with a
test rather than guessing, because the orderings are subtle: ``wrap_tool_call``
and ``wrap_model_call`` both compose first-in-list-outermost, while
``after_model`` runs in reverse list order.

Note these are *positional* assertions. The stronger, position-independent
guarantee — that no downstream middleware can observe an unsanitized answer —
lives in ``test_compliance_output_gate_isolation.py``.
"""

from __future__ import annotations

import asyncio
import inspect
import json
from typing import Any

import pytest
from langchain.agents import create_agent
from langchain.agents.middleware import AgentMiddleware
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from deerflow.agents.middlewares.compliance_context_gate_middleware import ComplianceContextGateMiddleware
from deerflow.agents.middlewares.compliance_input_gate_middleware import ComplianceInputGateMiddleware
from deerflow.agents.middlewares.compliance_output_gate_middleware import ComplianceOutputGateMiddleware
from deerflow.agents.middlewares.tool_error_handling_middleware import (
    build_compliance_flow_middlewares,
    build_compliance_input_gate_middlewares,
    build_lead_runtime_middlewares,
)
from deerflow.compliance.actions import REFUSAL_TEXT
from deerflow.compliance.contract import DetectionHit, RiskLocation
from deerflow.compliance.types import ComplianceDecision, Diagnostics
from deerflow.config.compliance_config import (
    ComplianceConfig,
    get_compliance_config,
    set_compliance_config,
)


@pytest.fixture()
def enabled_config():
    """Turn compliance on for the duration of a test."""
    original = get_compliance_config()
    set_compliance_config(ComplianceConfig(enabled=True))
    yield get_compliance_config()
    set_compliance_config(original)


@pytest.fixture(autouse=True)
def _restore_config():
    original = get_compliance_config()
    yield
    set_compliance_config(original)


# ── test doubles ────────────────────────────────────────────────────────────


class _StubEngine:
    """Returns a canned decision, or raises."""

    def __init__(self, decision: ComplianceDecision | None = None, error: Exception | None = None) -> None:
        self.decision = decision
        self.error = error
        self.calls: list[dict[str, Any]] = []

    def check(self, units, **kwargs):  # noqa: ANN001, ANN003
        self.calls.append({"units": list(units), **kwargs})
        if self.error:
            raise self.error
        return self.decision or ComplianceDecision(request_id="r", gate=kwargs.get("gate", "ContextGate"), scene_key="_unknown")


def _decision(gate="ContextGate", actions=("warn",), violation="domain", locations=()) -> ComplianceDecision:
    return ComplianceDecision(
        request_id="r-1",
        gate=gate,
        scene_key="_unknown",
        hits=(
            DetectionHit(
                detector_id="model_tfidf_knn",
                violation_type=violation,
                confidence=0.9,
                severity="high",
                risk_locations=tuple(locations),
            ),
        ),
        actions=tuple(actions),
        per_violation_actions={violation: tuple(actions)},
        basis=("测试依据条款",),
        audit_ref="audit-test",
        diagnostics=Diagnostics(units_total=1, units_checked=1),
    )


def _skill_result(display_text="camera HK-0431 at 31.19,121.43", evidence_data="camera_id HK-0431") -> dict:
    return {
        "schema_version": "1.0",
        "request_id": "req-1",
        "skill_name": "video-analysis",
        "status": "success",
        "result": {
            "display_text": display_text,
            "summary": {"title": "t", "overview": "o"},
            "findings": [],
            "evidence": [
                {
                    "evidence_id": "e-001",
                    "type": "text",
                    "title": "Camera record",
                    "data": evidence_data,
                    "metadata": {"camera_id": "HK-0431", "score": 0.9},
                }
            ],
            "artifacts": [],
        },
        "diagnostics": {"warnings": []},
        "errors": [],
    }


def _tool_message(payload: dict) -> ToolMessage:
    return ToolMessage(content=json.dumps(payload, ensure_ascii=False), tool_call_id="tc-1", name="invoke_skill")


class _Request:
    def __init__(self, name="invoke_skill") -> None:
        self.tool_call = {"name": name, "id": "tc-1"}


# ══ ordering ════════════════════════════════════════════════════════════════


def test_wrap_tool_call_composes_first_in_list_as_outermost() -> None:
    """Documents the LangChain contract this design depends on."""
    from langchain.agents.factory import _chain_tool_call_wrappers

    assert "first = outermost" in (_chain_tool_call_wrappers.__doc__ or "")


def test_after_model_runs_in_reverse_list_order() -> None:
    """The other half of the contract: reverse order for after_model.

    langchain wires `model -> middleware_w_after_model[-1].after_model` and walks
    down to index 0, so the FIRST middleware in the list runs LAST.
    """
    from langchain.agents import factory

    source = inspect.getsource(factory)
    assert 'graph.add_edge("model", f"{middleware_w_after_model[-1].name}.after_model")' in source
    assert "for idx in range(len(middleware_w_after_model) - 1, 0, -1):" in source


def test_context_and_output_gates_are_first_in_the_runtime_chain(enabled_config) -> None:
    """Both gates rely on first-in-list = outermost.

    ContextGate for `wrap_tool_call` (outside ToolErrorHandling), OutputGate for
    `wrap_model_call` (its result is the only version that reaches graph state).
    """
    middlewares = build_lead_runtime_middlewares(lazy_init=True)
    names = [type(m).__name__ for m in middlewares]

    assert names[0] == "ComplianceOutputGateMiddleware"
    assert names[1] == "ComplianceContextGateMiddleware"


def test_context_gate_is_outside_tool_error_handling(enabled_config) -> None:
    """The critical one.

    ToolErrorHandlingMiddleware converts any exception into an error ToolMessage.
    If the compliance gate sat inside it, a detection failure would silently
    become "tool failed, carry on" — a gate that fails open without saying so.
    """
    middlewares = build_lead_runtime_middlewares(lazy_init=True)
    names = [type(m).__name__ for m in middlewares]
    assert names.index("ComplianceContextGateMiddleware") < names.index("ToolErrorHandlingMiddleware")


def test_output_gate_precedes_every_other_after_model_middleware(enabled_config) -> None:
    """Front-of-list puts the gate LAST in after_model — correct for a backstop.

    The authoritative rewrite is in `wrap_model_call` (see
    test_compliance_output_gate_isolation.py); `after_model` only catches content
    mutated after the model node, so running last is exactly what it wants.
    """
    middlewares = build_lead_runtime_middlewares(lazy_init=True)
    after_model_owners = [
        type(m).__name__
        for m in middlewares
        if type(m).after_model is not AgentMiddleware.after_model or type(m).aafter_model is not AgentMiddleware.aafter_model
    ]
    assert after_model_owners[0] == "ComplianceOutputGateMiddleware", f"OutputGate must come first to rewrite last, got {after_model_owners}"


def test_input_gate_is_not_in_the_runtime_chain(enabled_config) -> None:
    """It mounts after IntentRecognitionMiddleware instead — before_agent runs forward."""
    names = [type(m).__name__ for m in build_lead_runtime_middlewares(lazy_init=True)]
    assert "ComplianceInputGateMiddleware" not in names


def test_input_gate_is_built_separately(enabled_config) -> None:
    built = build_compliance_input_gate_middlewares()
    assert [type(m).__name__ for m in built] == ["ComplianceInputGateMiddleware"]


def test_selected_model_is_bound_to_all_three_gates(enabled_config) -> None:
    model_name = "conversation-model"
    flow_gates = build_lead_runtime_middlewares(
        lazy_init=True,
        model_name=model_name,
    )[:2]
    input_gates = build_compliance_input_gate_middlewares(model_name=model_name)

    assert [gate._model_name for gate in [*flow_gates, *input_gates]] == [
        model_name,
        model_name,
        model_name,
    ]


def test_agent_mounts_the_input_gate_after_intent_recognition() -> None:
    """Guide §9.7 needs the recognized intent, so order is load-bearing."""
    from deerflow.agents.lead_agent import agent as agent_module

    source = inspect.getsource(agent_module._build_middlewares)
    intent_at = source.index("IntentRecognitionMiddleware(model_name=model_name)")
    gate_at = source.index("build_compliance_input_gate_middlewares(model_name=model_name)")
    assert intent_at < gate_at, "the input gate must be appended after intent recognition"


def test_no_gates_are_mounted_when_compliance_is_disabled() -> None:
    set_compliance_config(ComplianceConfig(enabled=False))
    names = [type(m).__name__ for m in build_lead_runtime_middlewares(lazy_init=True)]
    assert not any(name.startswith("Compliance") for name in names)
    assert build_compliance_input_gate_middlewares() == []


def test_individually_disabled_gates_are_not_mounted() -> None:
    set_compliance_config(ComplianceConfig(enabled=True, gates={"context": {"enabled": False}, "input": {"enabled": False}}))
    names = [type(m).__name__ for m in build_compliance_flow_middlewares()]
    assert names == ["ComplianceOutputGateMiddleware"]
    assert build_compliance_input_gate_middlewares() == []


def test_gate_construction_failure_does_not_break_agent_building(monkeypatch) -> None:
    """Compliance is a feature; it must not become a single point of failure."""
    import deerflow.agents.middlewares.tool_error_handling_middleware as module

    monkeypatch.setattr(module, "logger", module.logger)
    monkeypatch.setattr("deerflow.config.compliance_config.get_compliance_config", lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    assert build_compliance_flow_middlewares() == []
    assert build_compliance_input_gate_middlewares() == []


# ══ ContextGate ═════════════════════════════════════════════════════════════


def test_non_skill_result_tool_output_passes_through_untouched(enabled_config) -> None:
    middleware = ComplianceContextGateMiddleware(engine=_StubEngine(_decision()))
    original = ToolMessage(content="just some text", tool_call_id="tc-1", name="bash")
    assert middleware.wrap_tool_call(_Request("bash"), lambda r: original) is original


def test_disabled_gate_passes_through(_restore_config) -> None:
    set_compliance_config(ComplianceConfig(enabled=False))
    middleware = ComplianceContextGateMiddleware(engine=_StubEngine(_decision()))
    original = _tool_message(_skill_result())
    assert middleware.wrap_tool_call(_Request(), lambda r: original) is original


def test_clean_skill_result_is_returned_unchanged(enabled_config) -> None:
    engine = _StubEngine(ComplianceDecision(request_id="r", gate="ContextGate", scene_key="_unknown"))
    middleware = ComplianceContextGateMiddleware(engine=engine)
    original = _tool_message(_skill_result())
    assert middleware.wrap_tool_call(_Request(), lambda r: original) is original
    assert engine.calls, "the engine must actually have been consulted"


def test_skill_result_is_normalized_into_units(enabled_config) -> None:
    engine = _StubEngine()
    ComplianceContextGateMiddleware(engine=engine).wrap_tool_call(_Request(), lambda r: _tool_message(_skill_result()))
    units = engine.calls[0]["units"]
    assert units, "evidence must produce at least one detection unit"
    assert engine.calls[0]["gate"] == "ContextGate"
    paths = {item.path for unit in units for item in unit.field_items}
    assert "metadata.camera_id" in paths, "metadata business fields must be flattened into dotted paths"


def test_refuse_blanks_the_evidence_but_keeps_the_envelope(enabled_config) -> None:
    """The model still gets a well-formed SkillResult it can reason about."""
    engine = _StubEngine(_decision(actions=("refuse",)))
    result = ComplianceContextGateMiddleware(engine=engine).wrap_tool_call(_Request(), lambda r: _tool_message(_skill_result()))

    payload = json.loads(result.content)
    assert payload["result"]["evidence"] == []
    assert "合规" in payload["result"]["display_text"]
    assert payload["schema_version"] == "1.0", "envelope shape must survive"
    assert result.tool_call_id == "tc-1"


def test_desensitize_masks_only_the_reported_locations(enabled_config) -> None:
    """Surgical beats wholesale: the model keeps what it may legitimately use."""
    decision = _decision(
        actions=("desensitize",),
        locations=[RiskLocation(kind="field_path", locator="metadata.camera_id", text="HK-0431")],
    )
    engine = _StubEngine(decision)
    result = ComplianceContextGateMiddleware(engine=engine).wrap_tool_call(_Request(), lambda r: _tool_message(_skill_result()))

    payload = json.loads(result.content)
    evidence = payload["result"]["evidence"][0]
    assert evidence["metadata"]["camera_id"] == "*" * len("HK-0431")
    assert evidence["metadata"]["score"] == 0.9, "unrelated fields stay readable"
    assert "HK-0431" not in payload["result"]["display_text"]
    assert evidence["title"] == "Camera record", "the envelope is not masked"


def test_warn_only_annotates_without_touching_evidence(enabled_config) -> None:
    engine = _StubEngine(_decision(actions=("warn", "manual_review")))
    result = ComplianceContextGateMiddleware(engine=engine).wrap_tool_call(_Request(), lambda r: _tool_message(_skill_result()))

    payload = json.loads(result.content)
    assert payload["result"]["evidence"][0]["data"] == "camera_id HK-0431", "warn must not mutate the payload"
    assert any("合规" in w for w in payload["diagnostics"]["warnings"])
    assert payload["diagnostics"]["compliance"]["actions"] == ["warn", "manual_review"]


def test_determination_is_surfaced_to_the_model(enabled_config) -> None:
    """So a downstream answer can say evidence was withheld, not reason over a hole."""
    engine = _StubEngine(_decision(actions=("refuse",)))
    result = ComplianceContextGateMiddleware(engine=engine).wrap_tool_call(_Request(), lambda r: _tool_message(_skill_result()))

    diagnostics = json.loads(result.content)["diagnostics"]
    assert diagnostics["compliance"]["gate"] == "ContextGate"
    assert diagnostics["compliance"]["violation_types"] == ["domain"]
    assert diagnostics["compliance"]["audit_ref"] == "audit-test"


def test_fail_mode_closed_blocks_when_detection_errors(enabled_config) -> None:
    """A gate that fails open is indistinguishable from a gate that is off."""
    engine = _StubEngine(error=RuntimeError("detector exploded"))
    result = ComplianceContextGateMiddleware(engine=engine).wrap_tool_call(_Request(), lambda r: _tool_message(_skill_result()))

    payload = json.loads(result.content)
    assert payload["result"]["evidence"] == []
    assert "合规检查失败" in payload["result"]["display_text"]
    assert any("detection failed" in w for w in payload["diagnostics"]["warnings"])


def test_fail_mode_open_passes_through_and_says_so(_restore_config) -> None:
    set_compliance_config(ComplianceConfig(enabled=True, gates={"context": {"fail_mode": "open"}}))
    engine = _StubEngine(error=RuntimeError("detector exploded"))
    result = ComplianceContextGateMiddleware(engine=engine).wrap_tool_call(_Request(), lambda r: _tool_message(_skill_result()))

    payload = json.loads(result.content)
    assert payload["result"]["evidence"][0]["data"] == "camera_id HK-0431", "content passes through"
    assert any("UNCHECKED" in w for w in payload["diagnostics"]["warnings"]), "but never silently"


def test_budget_config_is_passed_to_the_engine(enabled_config) -> None:
    set_compliance_config(ComplianceConfig(enabled=True, gates={"context": {"budget_ms": 123, "max_units": 7}}))
    engine = _StubEngine()
    ComplianceContextGateMiddleware(engine=engine).wrap_tool_call(_Request(), lambda r: _tool_message(_skill_result()))
    assert engine.calls[0]["budget_ms"] == 123
    assert engine.calls[0]["max_units"] == 7


def test_units_are_prioritized_by_evidence_score(enabled_config) -> None:
    """Under budget, keep the highest-scoring evidence, not an arbitrary prefix."""
    payload = _skill_result()
    payload["result"]["evidence"] = [
        {"evidence_id": f"e-{i:03d}", "type": "text", "title": f"t{i}", "data": f"text {i}", "metadata": {"score": i / 10}}
        for i in range(1, 6)
    ]
    set_compliance_config(ComplianceConfig(enabled=True, gates={"context": {"max_units": 2}}))
    engine = _StubEngine()
    ComplianceContextGateMiddleware(engine=engine).wrap_tool_call(_Request(), lambda r: _tool_message(payload))

    ordered_ids = [unit.unit_id.rsplit(":", 1)[-1] for unit in engine.calls[0]["units"]]
    assert ordered_ids[0] == "e-005", f"highest score must be checked first, got {ordered_ids}"


def test_async_path_behaves_identically(enabled_config) -> None:
    """Both call paths must apply the same disposition; agents use the async one."""
    engine = _StubEngine(_decision(actions=("refuse",)))
    middleware = ComplianceContextGateMiddleware(engine=engine)

    async def handler(request):  # noqa: ANN001, ANN202
        return _tool_message(_skill_result())

    result = asyncio.run(middleware.awrap_tool_call(_Request(), handler))
    assert json.loads(result.content)["result"]["evidence"] == []


def test_async_output_gate_behaves_identically(enabled_config) -> None:
    engine = _StubEngine(_decision(gate="OutputGate", actions=("refuse",), violation="re_identify"))
    result = asyncio.run(ComplianceOutputGateMiddleware(engine=engine).aafter_model(_state(), runtime=None))
    assert "HK-0431" not in result["messages"][0].content


def test_async_input_gate_behaves_identically(enabled_config) -> None:
    engine = _StubEngine(_decision(gate="InputGate", actions=("refuse",), violation="hardcoded_cred"))
    result = asyncio.run(ComplianceInputGateMiddleware(engine=engine).abefore_agent({"messages": [HumanMessage(content="q")]}, runtime=None))
    assert result["jump_to"] == "end"


# ══ OutputGate ══════════════════════════════════════════════════════════════


def _state(text="camera HK-0431 at 31.19,121.43", message_id="m-1", tool_calls=None):
    return {"messages": [HumanMessage(content="q"), AIMessage(content=text, id=message_id, tool_calls=tool_calls or [])]}


def test_clean_output_is_annotated_without_changing_content(enabled_config) -> None:
    middleware = ComplianceOutputGateMiddleware(engine=_StubEngine())
    result = middleware.after_model(_state(), runtime=None)
    message = result["messages"][0]
    assert message.content == "camera HK-0431 at 31.19,121.43"
    metadata = message.response_metadata["compliance"]
    assert metadata["evaluated"] is True
    assert metadata["retracted"] is False
    assert metadata["checks"][0]["status"] == "passed"
    assert metadata["checks"][0]["actions"] == ["allow"]


def test_intermediate_tool_call_message_is_skipped(enabled_config) -> None:
    """Rewriting it would break tool-call/response pairing."""
    state = _state(text="calling a tool", tool_calls=[{"name": "bash", "args": {}, "id": "t1"}])
    middleware = ComplianceOutputGateMiddleware(engine=_StubEngine(_decision(gate="OutputGate", actions=("refuse",))))
    assert middleware.after_model(state, runtime=None) is None


def test_refuse_replaces_the_persisted_message(enabled_config) -> None:
    """Layer 2: the checkpoint must not keep the original."""
    engine = _StubEngine(_decision(gate="OutputGate", actions=("refuse",), violation="re_identify"))
    result = ComplianceOutputGateMiddleware(engine=engine).after_model(_state(), runtime=None)

    message = result["messages"][0]
    assert "HK-0431" not in message.content
    assert "合规" in message.content
    assert message.id == "m-1", "same id so the reducer replaces rather than appends"
    assert message.response_metadata["compliance"]["retracted"] is True


def test_checkpoint_never_holds_the_violating_text(enabled_config) -> None:
    secret = "身份证 310101199001011234 属于张三"
    engine = _StubEngine(
        _decision(
            gate="OutputGate",
            actions=("desensitize",),
            violation="re_identify",
            locations=[RiskLocation(kind="char_span", locator="3:21", text="310101199001011234")],
        )
    )
    result = ComplianceOutputGateMiddleware(engine=engine).after_model(_state(text=secret), runtime=None)
    assert "310101199001011234" not in result["messages"][0].content


def test_warn_only_keeps_the_answer_and_appends_a_notice(enabled_config) -> None:
    engine = _StubEngine(_decision(gate="OutputGate", actions=("warn", "manual_review"), violation="re_identify"))
    result = ComplianceOutputGateMiddleware(engine=engine).after_model(_state(text="benign looking answer"), runtime=None)

    content = result["messages"][0].content
    assert "benign looking answer" in content, "warn must not destroy a usable answer"
    assert "合规提示" in content


def test_retract_event_structure(enabled_config) -> None:
    """The frontend contract. Changing this breaks message replacement."""
    from deerflow.agents.middlewares.compliance_output_gate_middleware import RETRACT_EVENT, build_retract_event

    decision = _decision(gate="OutputGate", actions=("rewrite",), violation="re_identify")
    event = build_retract_event(AIMessage(content="original", id="m-42"), "safe replacement", decision)

    assert event["type"] == RETRACT_EVENT == "compliance_retract"
    assert event["message_id"] == "m-42"
    assert event["violation_types"] == ["re_identify"]
    assert event["action"] == ["rewrite"]
    assert event["replacement"] == "safe replacement"
    assert event["audit_ref"] == "audit-test"
    assert event["basis"] == ["测试依据条款"]


def test_retract_event_is_emitted_through_the_stream_writer(enabled_config, monkeypatch) -> None:
    captured: list[dict] = []
    monkeypatch.setattr("langgraph.config.get_stream_writer", lambda: captured.append)

    engine = _StubEngine(_decision(gate="OutputGate", actions=("refuse",), violation="re_identify"))
    ComplianceOutputGateMiddleware(engine=engine).after_model(_state(), runtime=None)

    assert len(captured) == 1
    assert captured[0]["type"] == "compliance_retract"
    assert captured[0]["message_id"] == "m-1"


def test_missing_stream_writer_still_rewrites_the_message(enabled_config, monkeypatch) -> None:
    """A non-streaming caller (runs.wait) just receives the safe message."""
    monkeypatch.setattr("langgraph.config.get_stream_writer", lambda: (_ for _ in ()).throw(RuntimeError("no runtime")))
    engine = _StubEngine(_decision(gate="OutputGate", actions=("refuse",), violation="re_identify"))
    result = ComplianceOutputGateMiddleware(engine=engine).after_model(_state(), runtime=None)
    assert "HK-0431" not in result["messages"][0].content


def test_writer_failure_does_not_break_the_turn(enabled_config, monkeypatch) -> None:
    """The rewritten message is authoritative; a failed screen update is not fatal."""

    def _explode(_event):
        raise RuntimeError("socket closed")

    monkeypatch.setattr("langgraph.config.get_stream_writer", lambda: _explode)
    engine = _StubEngine(_decision(gate="OutputGate", actions=("refuse",), violation="re_identify"))
    result = ComplianceOutputGateMiddleware(engine=engine).after_model(_state(), runtime=None)
    assert result is not None


def test_output_gate_fail_mode_closed_blocks(enabled_config) -> None:
    engine = _StubEngine(error=RuntimeError("boom"))
    result = ComplianceOutputGateMiddleware(engine=engine).after_model(_state(), runtime=None)
    assert "合规检查失败" in result["messages"][0].content


def test_output_gate_fail_mode_open_lets_the_answer_through(_restore_config) -> None:
    set_compliance_config(ComplianceConfig(enabled=True, gates={"output": {"fail_mode": "open"}}))
    engine = _StubEngine(error=RuntimeError("boom"))
    assert ComplianceOutputGateMiddleware(engine=engine).after_model(_state(), runtime=None) is None


# ── incremental scanning (layer 1) ──────────────────────────────────────────


def test_incremental_scan_waits_for_the_interval(enabled_config) -> None:
    set_compliance_config(ComplianceConfig(enabled=True, gates={"output": {"incremental_scan": {"interval_chars": 50}}}))
    engine = _StubEngine(_decision(gate="OutputGate", actions=("refuse",)))
    middleware = ComplianceOutputGateMiddleware(engine=engine)

    assert middleware.scan_increment("m-1", "x" * 10) is None, "below the interval, no scan"
    assert engine.calls == []

    assert middleware.scan_increment("m-1", "x" * 60) is not None, "interval reached, scan runs"
    assert len(engine.calls) == 1


def test_incremental_scan_only_rescans_new_characters(enabled_config) -> None:
    """Cost tracks output length, not token count."""
    set_compliance_config(ComplianceConfig(enabled=True, gates={"output": {"incremental_scan": {"interval_chars": 50}}}))
    engine = _StubEngine()
    middleware = ComplianceOutputGateMiddleware(engine=engine)

    middleware.scan_increment("m-1", "x" * 60)
    middleware.scan_increment("m-1", "x" * 80)  # only 20 new chars
    assert len(engine.calls) == 1
    middleware.scan_increment("m-1", "x" * 120)  # now 60 new chars
    assert len(engine.calls) == 2


def test_incremental_scan_can_be_disabled(_restore_config) -> None:
    set_compliance_config(ComplianceConfig(enabled=True, gates={"output": {"incremental_scan": {"enabled": False}}}))
    engine = _StubEngine(_decision(gate="OutputGate", actions=("refuse",)))
    middleware = ComplianceOutputGateMiddleware(engine=engine)
    assert middleware.scan_increment("m-1", "x" * 5000) is None
    assert engine.calls == []


def test_incremental_scan_returns_none_when_clean(enabled_config) -> None:
    set_compliance_config(ComplianceConfig(enabled=True, gates={"output": {"incremental_scan": {"interval_chars": 10}}}))
    middleware = ComplianceOutputGateMiddleware(engine=_StubEngine())
    assert middleware.scan_increment("m-1", "x" * 100) is None


# ══ InputGate ═══════════════════════════════════════════════════════════════


def test_input_gate_allows_a_clean_query(enabled_config) -> None:
    middleware = ComplianceInputGateMiddleware(engine=_StubEngine())
    result = middleware.before_agent({"messages": [HumanMessage(content="hello")]}, runtime=None)
    assert result["compliance_input_result"]["status"] == "passed"
    assert result["compliance_input_result"]["actions"] == ["allow"]


def test_input_gate_refusal_ends_the_turn(enabled_config) -> None:
    engine = _StubEngine(_decision(gate="InputGate", actions=("refuse", "warn"), violation="hardcoded_cred"))
    result = ComplianceInputGateMiddleware(engine=engine).before_agent({"messages": [HumanMessage(content="here is my aws key")]}, runtime=None)

    assert result["jump_to"] == "end"
    message = result["messages"][0]
    assert message.content == REFUSAL_TEXT
    metadata = message.response_metadata["compliance"]
    assert metadata["evaluated"] is True
    assert metadata["retracted"] is True
    assert metadata["gate"] == "InputGate"
    assert metadata["scene"] == "_unknown"
    assert metadata["streaming_mode"] == "pre_model_refusal"
    assert metadata["transient_exposure_possible"] is False
    assert metadata["actions"] == ["refuse", "warn"]
    assert metadata["violation_types"] == ["hardcoded_cred"]
    assert metadata["audit_ref"] == "audit-test"
    assert metadata["basis"] == ["测试依据条款"]
    assert metadata["notice"] != message.content


def test_input_gate_refusal_stamps_the_frontend_compliance_contract(enabled_config) -> None:
    """A pre-model refusal must render as the same red card as OutputGate."""
    decision = _decision(gate="InputGate", actions=("aggregate", "desensitize", "refuse"), violation="struct_id")
    decision.scene_key = "public_release"
    result = ComplianceInputGateMiddleware(engine=_StubEngine(decision)).before_agent(
        {"messages": [HumanMessage(content="publish this phone number")]},
        runtime=None,
    )

    metadata = result["messages"][0].response_metadata["compliance"]
    assert metadata["retracted"] is True
    assert metadata["gate"] == "InputGate"
    assert metadata["scene"] == "public_release"
    assert metadata["actions"] == ["aggregate", "desensitize", "refuse"]
    assert metadata["violation_types"] == ["struct_id"]
    assert metadata["audit_ref"] == "audit-test"


def test_input_gate_refusal_does_not_reach_the_model(enabled_config) -> None:
    """The declared ``end`` jump must be a real graph edge, not state only."""

    def messages():
        while True:
            yield AIMessage(content="MODEL_MUST_NOT_RUN", id="blocked-model")

    engine = _StubEngine(_decision(gate="InputGate", actions=("refuse",), violation="hardcoded_cred"))
    agent = create_agent(
        model=GenericFakeChatModel(messages=messages()),
        tools=[],
        middleware=[ComplianceInputGateMiddleware(engine=engine)],
    )

    result = agent.invoke({"messages": [HumanMessage(content="blocked input")]})
    rendered = "\n".join(
        message.content for message in result["messages"] if isinstance(message.content, str)
    )

    assert "合规" in rendered
    assert "MODEL_MUST_NOT_RUN" not in rendered
    blocked = next(message for message in result["messages"] if isinstance(message, AIMessage))
    assert blocked.response_metadata["compliance"]["gate"] == "InputGate"
    assert blocked.response_metadata["compliance"]["retracted"] is True


def test_input_gate_warn_does_not_block(enabled_config) -> None:
    """Only `refuse` stops the request; warn/manual_review are recorded and pass."""
    engine = _StubEngine(_decision(gate="InputGate", actions=("warn", "manual_review")))
    result = ComplianceInputGateMiddleware(engine=engine).before_agent({"messages": [HumanMessage(content="q")]}, runtime=None)
    assert result["compliance_input_result"]["status"] == "handled"
    assert result["compliance_input_result"]["actions"] == ["warn", "manual_review"]


def test_output_summary_combines_input_and_output_checks(enabled_config) -> None:
    input_check = {
        "gate": "InputGate",
        "scene": "self_use",
        "status": "passed",
        "actions": ["allow"],
        "violation_types": [],
        "basis": [],
        "audit_ref": "audit-input",
    }
    state = _state()
    state["compliance_input_result"] = input_check
    result = ComplianceOutputGateMiddleware(engine=_StubEngine()).after_model(state, runtime=None)
    checks = result["messages"][0].response_metadata["compliance"]["checks"]
    assert [check["gate"] for check in checks] == ["InputGate", "OutputGate"]
    assert checks[0]["audit_ref"] == "audit-input"


def test_input_gate_passes_recognized_intent_to_the_engine(enabled_config) -> None:
    """Guide §9.7: the joint judgement needs the intent."""
    engine = _StubEngine()
    state = {
        "messages": [HumanMessage(content="extract every phone number for public release")],
        "intent_context": {"intent": "data_export", "high_risk": True, "confidence": 0.9, "scenes": ["public"]},
    }
    ComplianceInputGateMiddleware(engine=engine).before_agent(state, runtime=None)

    intent = engine.calls[0]["intent"]
    assert intent is not None
    assert intent.intent == "data_export"
    assert intent.high_risk is True


def test_input_gate_tolerates_a_missing_intent_context(enabled_config) -> None:
    engine = _StubEngine()
    ComplianceInputGateMiddleware(engine=engine).before_agent({"messages": [HumanMessage(content="q")]}, runtime=None)
    assert engine.calls[0]["intent"] is None


def test_input_gate_ignores_empty_queries(enabled_config) -> None:
    engine = _StubEngine()
    assert ComplianceInputGateMiddleware(engine=engine).before_agent({"messages": []}, runtime=None) is None
    assert engine.calls == []


def test_input_gate_fail_mode_closed_blocks(enabled_config) -> None:
    engine = _StubEngine(error=RuntimeError("boom"))
    result = ComplianceInputGateMiddleware(engine=engine).before_agent({"messages": [HumanMessage(content="q")]}, runtime=None)
    assert result["jump_to"] == "end"
    message = result["messages"][0]
    assert "合规检查失败" in message.content
    assert message.response_metadata["compliance"]["gate"] == "InputGate"
    assert message.response_metadata["compliance"]["actions"] == ["refuse"]
    assert message.response_metadata["compliance"]["retracted"] is True


def test_upload_scan_helper_is_exposed_for_the_app_layer() -> None:
    """app/gateway/routers/uploads.py calls harness — direction stays legal."""
    from deerflow.agents.middlewares.compliance_input_gate_middleware import scan_upload_paths

    assert callable(scan_upload_paths)


def test_upload_scan_returns_a_clean_decision_for_no_readable_files(enabled_config, tmp_path) -> None:
    from deerflow.agents.middlewares.compliance_input_gate_middleware import scan_upload_paths

    decision = scan_upload_paths([str(tmp_path / "nope.txt")], thread_id="t-1", engine=_StubEngine())
    assert decision.actions == ()


def test_upload_scan_reads_text_files(enabled_config, tmp_path) -> None:
    from deerflow.agents.middlewares.compliance_input_gate_middleware import scan_upload_paths

    upload = tmp_path / "data.csv"
    upload.write_text("phone,name\n13800138000,张三\n", encoding="utf-8")
    engine = _StubEngine()
    scan_upload_paths([str(upload)], thread_id="t-1", engine=engine)

    units = engine.calls[0]["units"]
    assert len(units) == 1
    assert "13800138000" in units[0].text_items[0].text
