"""Cross-stage regressions from the thinking-on complete service campaign."""

from types import SimpleNamespace
from typing import TypedDict

import pytest
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

from app.gateway.routers.mobile import adapt_events_to_mobile
from deerflow.agents.middlewares import intent_recognition_middleware as intent_module
from deerflow.agents.middlewares.computation_guard_middleware import ComputationGuardMiddleware
from deerflow.routing.dialogue_act import DialogueActResult
from deerflow.routing.intent import RoutingIntentResult
from deerflow.routing.intent.structured import _grounded_evidence
from deerflow.routing.turn_requirements import TurnRequirements
from deerflow.tools.builtins.present_file_tool import present_file_tool


class DeliveryTestContext(TypedDict):
    thread_id: str


def test_code_layout_differences_do_not_drop_a_real_task():
    assert _grounded_evidence("def positives(xs): return len([x for x in xs if x>0])", "def positives(xs):\n    return len([x for x in xs if x>0])")
    assert not _grounded_evidence("def positives(xs): return len([x for x in xs if x<0])", "def positives(xs):\n    return len([x for x in xs if x>0])")


def test_explanation_resume_replaces_old_execution_and_delivery_requirements(monkeypatch):
    previous = RoutingIntentResult(intent="task", original_query="计算均值并导出CSV", normalized_query="计算均值并导出CSV", routing_query="计算均值并导出CSV", scenes=["road_traffic"], scene="road_traffic", requires_computation=True, required_csv_metrics=["均值"]).model_dump()
    decision = DialogueActResult(act="task_followup", confidence=0.9, reason="explanation", resume_intent_context=previous, metadata={"turn_requirements": {"requires_computation": False, "allow_tools": False, "required_csv_metrics": [], "required_artifact_types": [], "csv_precision": {}, "csv_missing_values": []}})
    monkeypatch.setattr(intent_module, "classify_dialogue_act", lambda *a, **k: decision)
    monkeypatch.setattr(intent_module, "create_chat_model", lambda **k: object())
    monkeypatch.setattr(intent_module.IntentRecognitionMiddleware, "_compliance_intent", staticmethod(lambda _: None))
    result = intent_module.IntentRecognitionMiddleware().before_agent({"messages": [HumanMessage(content="只解释，不调用工具、不导出", id="now")], "intent_context": previous}, None)
    assert result["intent_context"]["scenes"] == ["road_traffic"]
    assert result["intent_context"]["requires_computation"] is False
    assert result["intent_context"]["allow_tools"] is False
    assert result["intent_context"]["required_csv_metrics"] == []


def test_retry_never_forces_an_incompatible_tool_choice():
    request = SimpleNamespace(state={"intent_context": {"requires_computation": True, "_source_message_key": "id:q"}, "messages": [HumanMessage(content="计算", id="q"), SystemMessage(content="retry", name="computation_guard_retry")]}, tools=[SimpleNamespace(name="bash")], tool_choice=None)
    request.override = lambda **kwargs: SimpleNamespace(**{**vars(request), **kwargs})
    assert ComputationGuardMiddleware._execution_request(request).tool_choice is None


def runtime_for(tmp_path, **intent):
    return SimpleNamespace(state={"thread_data": {"outputs_path": str(tmp_path)}, "intent_context": {"_source_message_key": "id:q", **intent}}, context={"thread_id": "q"})


def test_explicit_na_is_valid_but_wrong_numeric_precision_still_fails(tmp_path):
    path = tmp_path / "growth.csv"
    runtime = runtime_for(tmp_path, original_query="保留两位小数，分母为0写NA", required_csv_metrics=["同比"], csv_precision={"同比": 2}, csv_missing_values=["NA"])
    check = {"filepath": str(path), "metric_columns": {"同比": "growth"}, "decimal_places": {"growth": 2}}
    path.write_text("year,growth\n2024,NA\n2025,10.00\n")
    result = present_file_tool.func(runtime=runtime, filepaths=[str(path)], tool_call_id="p", delivery_checks=check)
    assert "artifacts" in result.update
    path.write_text("year,growth\n2024,NA\n2025,10.0\n")
    result = present_file_tool.func(runtime=runtime, filepaths=[str(path)], tool_call_id="p", delivery_checks=check)
    assert "artifacts" not in result.update


@pytest.mark.parametrize("explicit_contract", [False, True])
def test_three_decimal_requirement_cannot_be_bypassed_by_omitting_check(tmp_path, explicit_contract):
    path = tmp_path / "growth.csv"
    path.write_text("year,growth\n2025,10.0\n")
    runtime = runtime_for(tmp_path, original_query="把增长率的小数精度改成三位", required_csv_metrics=["同比"], **({"csv_precision": {"同比": 3}} if explicit_contract else {}))
    result = present_file_tool.func(runtime=runtime, filepaths=[str(path)], tool_call_id="p", delivery_checks={"filepath": str(path), "metric_columns": {"同比": "growth"}})
    assert "artifacts" not in result.update


def test_metrics_can_be_delivered_in_separate_files(tmp_path):
    path = tmp_path / "counts.csv"
    path.write_text("user,count\nA,0\nB,0\n")
    runtime = runtime_for(tmp_path, required_csv_metrics=["次数", "开始时间", "结束时间"])
    result = present_file_tool.func(runtime=runtime, filepaths=[str(path)], tool_call_id="p", delivery_checks={"filepath": str(path), "metric_columns": {"次数": "count"}})
    assert result.update["artifacts"] == ["/mnt/user-data/outputs/counts.csv"]


def test_missing_delivery_retries_even_if_computation_succeeded():
    state = {"intent_context": {"requires_computation": False, "required_artifact_types": ["json"], "_source_message_key": "id:q"}, "messages": [HumanMessage(content="导出AST JSON", id="q"), AIMessage(content="文件已生成", id="answer")]}
    result = ComputationGuardMiddleware._check(state)
    assert result and result["jump_to"] == "model"


def test_sse_error_cannot_be_forwarded_as_completed_final_answer():
    events = [{"event": "values", "data": {"messages": [{"type": "human", "content": "解释"}, {"type": "ai", "content": "正在通过工具核验计算结果。"}]}}, {"event": "error", "data": {"error": "BadRequestError", "message": "synthetic error"}}]
    result = adapt_events_to_mobile(None, "q", events, "解释")
    assert any(s["status"] == "failed" for s in result["segments"])
    assert not any(s["agent_id"] == "final" and s["status"] == "completed" for s in result["segments"])


def test_guard_failure_is_forwarded_as_failed_feedback():
    events = [{"event": "values", "data": {"messages": [{"type": "human", "content": "计算"}, {"type": "ai", "content": "未取得核验结果", "response_metadata": {"execution_status": "failed"}}]}}]
    result = adapt_events_to_mobile(None, "q", events, "计算")
    assert any(s["status"] == "failed" for s in result["segments"])
    assert not any(s["agent_id"] == "final" and s["status"] == "completed" for s in result["segments"])


def deliver(runtime, path, metrics=None):
    update = present_file_tool.func(runtime=runtime, filepaths=[str(path)], tool_call_id="p", delivery_checks={"filepath": str(path), "metric_columns": metrics or {}} if path.suffix == ".csv" else None).update
    assert "artifacts" in update, update
    runtime.state["artifacts"] = list(dict.fromkeys(runtime.state.get("artifacts", []) + update["artifacts"]))
    runtime.state.setdefault("verified_deliveries", {}).update(update["verified_deliveries"])
    return update


def terminal(runtime):
    runtime.state["messages"] = [HumanMessage(content="本轮任务", id="q"), AIMessage(content="已完成")]
    return ComputationGuardMiddleware._check(runtime.state)


def test_split_csvs_only_satisfy_completion_after_all_metrics_are_verified(tmp_path):
    runtime = runtime_for(tmp_path, required_csv_metrics=["次数", "开始", "结束"], required_artifact_types=["csv"])
    counts = tmp_path / "counts.csv"
    counts.write_text("user,count\nA,0\n")
    deliver(runtime, counts, {"次数": "count"})
    assert terminal(runtime)["jump_to"] == "model"
    details = tmp_path / "details.csv"
    details.write_text("user,start,end\n")
    deliver(runtime, details, {"开始": "start", "结束": "end"})
    assert terminal(runtime) is None
    assert counts.read_text() == "user,count\nA,0\n"


@pytest.mark.parametrize("change", ["modified", "deleted", "prior_turn", "unregistered"])
def test_stale_or_changed_files_cannot_satisfy_current_delivery(tmp_path, change):
    runtime = runtime_for(tmp_path, required_artifact_types=["json"])
    path = tmp_path / "ast.json"
    path.write_text("{}")
    deliver(runtime, path)
    assert terminal(runtime) is None
    if change == "modified":
        path.write_text('{"changed":true}')
    elif change == "deleted":
        path.unlink()
    elif change == "prior_turn":
        runtime.state["verified_deliveries"]["/mnt/user-data/outputs/ast.json"]["source"] = "id:previous"
    else:
        runtime.state["artifacts"] = []
    assert terminal(runtime)["jump_to"] == "model"


def test_parallel_delivery_reducer_preserves_independent_files():
    from deerflow.agents.thread_state import merge_delivery_records

    assert merge_delivery_records({"a": {"source": "q"}}, {"b": {"source": "q"}}) == {"a": {"source": "q"}, "b": {"source": "q"}}


@pytest.mark.parametrize("value,ok", [("NA", True), ("BAD", False), ("10.0", False), ("10.000", True)])
def test_updated_precision_uses_contract_and_only_approved_missing_markers(tmp_path, value, ok):
    path = tmp_path / "growth.csv"
    path.write_text("year,growth\n2024," + value + "\n")
    runtime = runtime_for(tmp_path, original_query="改为三位", required_csv_metrics=["同比"], csv_precision={"同比": 3}, csv_missing_values=["NA"])
    result = present_file_tool.func(runtime=runtime, filepaths=[str(path)], tool_call_id="p", delivery_checks={"filepath": str(path), "metric_columns": {"同比": "growth"}, "decimal_places": {"growth": 1}})
    assert ("artifacts" in result.update) is ok


def test_different_metric_precisions_do_not_override_each_other(tmp_path):
    path = tmp_path / "mixed.csv"
    path.write_text("gdp,population\n10.000,2.00\n")
    runtime = runtime_for(tmp_path, required_csv_metrics=["GDP", "人口"], csv_precision={"GDP": 3, "人口": 2})
    deliver(runtime, path, {"GDP": "gdp", "人口": "population"})
    assert terminal(runtime) is None


def test_numeric_values_cannot_be_declared_as_missing_to_bypass_precision():
    with pytest.raises(ValueError):
        TurnRequirements(csv_missing_values=["10.0"])


def test_actual_graph_blocks_tools_on_explicit_no_tool_turn():
    from langchain.agents import create_agent
    from langchain.tools import tool
    from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel

    from deerflow.agents.thread_state import ThreadState

    class Model(FakeMessagesListChatModel):
        def bind_tools(self, tools, **kwargs):
            return self

    calls = []

    @tool
    def bash(command: str) -> str:
        """Synthetic computation."""
        calls.append(command)
        return "200"

    # Even a model that ignores disabled tools cannot execute them.
    model = Model(responses=[AIMessage(content="", tool_calls=[{"name": "bash", "id": "calc", "args": {"command": "ignored"}}])])
    agent = create_agent(model, tools=[bash], middleware=[ComputationGuardMiddleware()], state_schema=ThreadState)
    result = agent.invoke({"messages": [HumanMessage(content="只解释，不调用工具", id="q")], "intent_context": {"allow_tools": False, "_source_message_key": "id:q"}})
    assert calls == []
    assert result["messages"][-1].response_metadata["execution_status"] == "failed"


def test_actual_graph_stops_failed_recognition_before_business_model_or_tools():
    from langchain.agents import create_agent
    from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel

    from deerflow.agents.thread_state import ThreadState

    model = FakeMessagesListChatModel(responses=[])
    agent = create_agent(model, middleware=[ComputationGuardMiddleware()], state_schema=ThreadState)
    result = agent.invoke({"messages": [HumanMessage(content="多目标任务", id="q")], "intent_context": {"recognition_status": "failed", "_source_message_key": "id:q"}})
    assert result["messages"][-1].response_metadata["execution_status"] == "failed"
    assert model.i == 0


def test_identical_delivery_failures_are_bounded_but_distinct_errors_are_not():
    state = {"intent_context": {"_source_message_key": "id:q"}, "messages": [HumanMessage(content="导出", id="q")]}
    for i in range(3):
        state["messages"].append(ToolMessage(content="same validation error", name="present_files", tool_call_id=str(i), status="error", additional_kwargs={"error_code": "DELIVERY_INVALID"}))
    state["messages"].append(AIMessage(content="", tool_calls=[{"name": "present_files", "id": "p", "args": {}}]))
    assert ComputationGuardMiddleware._check(state)["jump_to"] == "end"
    for i, msg in enumerate(state["messages"][1:-1]):
        msg.content = f"different error {i}"
    assert ComputationGuardMiddleware._check(state) is None


def test_canonical_failure_overrides_old_transcript_placeholder():
    events = [{"event": "values", "data": {"messages": [{"type": "human", "content": "计算"}, {"type": "ai", "content": "本轮失败", "response_metadata": {"execution_status": "failed"}}], "raw_messages": [{"type": "human", "content": "计算"}, {"type": "ai", "content": "旧的成功答复"}]}}]
    result = adapt_events_to_mobile(None, "q", events, "计算")
    assert result["status"] == "failed"
    assert not any(s["agent_id"] == "final" for s in result["segments"])


def test_incomplete_placeholder_is_never_a_completed_answer():
    events = [{"event": "values", "data": {"messages": [{"type": "human", "content": "计算"}, {"type": "ai", "content": "核验中", "response_metadata": {"execution_status": "needs_verification"}}]}}]
    result = adapt_events_to_mobile(None, "q", events, "计算")
    assert result["status"] == "incomplete"
    assert not any(s["agent_id"] == "final" for s in result["segments"])


def test_prior_failure_does_not_poison_a_later_successful_turn():
    events = [{"event": "values", "data": {"messages": [{"type": "human", "content": "旧请求"}, {"type": "ai", "content": "失败", "response_metadata": {"execution_status": "failed"}}, {"type": "human", "content": "新请求"}, {"type": "ai", "content": "本轮成功"}]}}]
    result = adapt_events_to_mobile(None, "q", events, "新请求")
    assert result.get("status") != "failed"
    assert [s["content"] for s in result["segments"] if s["agent_id"] == "final"] == ["本轮成功"]


def test_terminal_failure_is_not_reopened_by_the_completion_guard():
    state = {"intent_context": {"requires_computation": True, "required_artifact_types": ["csv"], "_source_message_key": "id:q"}, "messages": [HumanMessage(content="计算并导出", id="q"), AIMessage(content="重复调用已停止", response_metadata={"execution_status": "failed"})]}
    assert ComputationGuardMiddleware._check(state) is None


@pytest.mark.parametrize("complete", [False, True])
def test_followup_requires_a_complete_fresh_contract(complete):
    import json

    from deerflow.routing.dialogue_act import classify_dialogue_act

    response = {"act": "task_followup", "confidence": 0.9, "should_reclassify_intent": False, "should_reroute_skills": False}
    if complete:
        response["turn_requirements"] = TurnRequirements(allow_tools=False).model_dump()

    class Model:
        def with_config(self, config):
            return self

        def invoke(self, prompt):
            return SimpleNamespace(content=json.dumps(response))

    result = classify_dialogue_act("解释刚才的均值，不调用工具", previous_intent_context={"intent": "task", "original_query": "计算均值并导出CSV", "requires_computation": True}, llm=Model())
    assert result.act == ("task_followup" if complete else "new_task")
    if complete:
        assert result.metadata["turn_requirements"]["requires_computation"] is False
        assert result.metadata["turn_requirements"]["allow_tools"] is False


@pytest.mark.parametrize("missing", ["allow_tools", "csv_precision", "csv_missing_values", "required_artifact_types"])
def test_recognition_cannot_silently_default_a_missing_contract_field(missing):
    from deerflow.routing.intent.structured import Recognition

    data = {**TurnRequirements().model_dump(), "tasks": [], "planning_steps": ["处理请求"], "confidence": 0.9}
    del data[missing]
    with pytest.raises(ValueError):
        Recognition.model_validate(data)


def test_generated_computation_contract_cannot_disable_required_tools():
    from deerflow.routing.intent.structured import Recognition

    data = {**TurnRequirements(requires_computation=True, allow_tools=False).model_dump(), "tasks": [], "planning_steps": ["计算"], "confidence": 0.9}
    with pytest.raises(ValueError, match="contradicts"):
        Recognition.model_validate(data)
    # Historical checkpoints must remain readable so a new turn can recover.
    assert TurnRequirements.model_validate(data).requires_computation


def test_prior_metric_identifiers_are_supplied_but_never_count_as_user_quotes():
    import json

    from deerflow.routing.intent.structured import _input_context

    text, evidence = _input_context("保留指标，改为三位", {"original_query": "导出同比", "required_csv_metrics": ["2024年人口同比"]}, [])
    assert json.loads(text)["previous_turn_requirements"]["required_csv_metrics"] == ["2024年人口同比"]
    assert "2024年人口同比" not in evidence


def test_old_metric_aliases_fail_early_with_actionable_current_labels(tmp_path):
    path = tmp_path / "growth.csv"
    path.write_text("year,growth\n2025,10.000\n")
    runtime = runtime_for(tmp_path, required_csv_metrics=["2025年人口同比"], csv_precision={"2025年人口同比": 3})
    result = present_file_tool.func(runtime=runtime, filepaths=[str(path)], tool_call_id="p", delivery_checks={"filepath": str(path), "metric_columns": {"同比": "growth"}})
    assert "artifacts" not in result.update
    assert "2025年人口同比" in result.update["messages"][0].content


@pytest.mark.parametrize("asynchronous", [False, True])
def test_actual_graph_corrects_tool_metadata_without_executing_malformed_command(asynchronous):
    import asyncio

    from langchain.agents import create_agent
    from langchain.tools import tool
    from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel

    from deerflow.agents.middlewares.tool_error_handling_middleware import ToolErrorHandlingMiddleware
    from deerflow.agents.thread_state import ThreadState

    class Model(FakeMessagesListChatModel):
        def bind_tools(self, tools, **kwargs):
            return self

    calls = []

    @tool
    def bash(description: str, command: str) -> str:
        """Calculate a synthetic mean."""
        calls.append((description, command))
        return "260"

    model = Model(responses=[AIMessage(content="", tool_calls=[{"name": "bash", "id": "bad", "args": {"command": "description embedded; command=invalid"}}]), AIMessage(content="", tool_calls=[{"name": "bash", "id": "good", "args": {"description": "计算均值", "command": "calculate"}}]), AIMessage(content="均值260")])
    agent = create_agent(model, tools=[bash], middleware=[ToolErrorHandlingMiddleware(), ComputationGuardMiddleware()], state_schema=ThreadState)
    state = {"messages": [HumanMessage(content="算均值", id="q")], "intent_context": {"requires_computation": True, "_source_message_key": "id:q"}}
    result = asyncio.run(agent.ainvoke(state)) if asynchronous else agent.invoke(state)
    assert calls == [("计算均值", "calculate")]
    assert result["messages"][-1].content == "均值260"
    errors = [m for m in result["messages"] if isinstance(m, ToolMessage) and m.status == "error"]
    assert len(errors) == 1
    assert "separate top-level fields" in errors[0].content
    assert "command=invalid" not in errors[0].content


def test_removed_draft_cannot_survive_a_canonical_state_snapshot():
    events = [
        {"event": "values", "data": {"messages": [{"type": "human", "content": "计算"}, {"type": "ai", "id": "draft", "content": "未核验的300"}]}},
        {"event": "updates", "data": {"ComputationGuardMiddleware.after_model": {"messages": [{"type": "remove", "id": "draft", "content": ""}, {"type": "system", "name": "computation_guard_retry", "content": "核验"}]}}},
        {"event": "values", "data": {"messages": [{"type": "human", "content": "计算"}, {"type": "system", "name": "computation_guard_retry", "content": "核验"}]}},
    ]
    result = adapt_events_to_mobile(None, "q", events, "计算")
    assert result["status"] == "incomplete"
    assert not any(s["agent_id"] == "final" for s in result["segments"])


def test_current_scope_is_injected_for_explanation_without_changing_persisted_history():
    import json

    state = {"intent_context": {"allow_tools": False, "params": {"时间范围": "2026-10-27", "分析区域": "支路乙"}}, "messages": [HumanMessage(content="解释刚才结果", id="q")]}
    request = SimpleNamespace(state=state, messages=state["messages"], tools=[], tool_choice=None)
    request.override = lambda **kwargs: SimpleNamespace(**{**vars(request), **kwargs})
    result = ComputationGuardMiddleware._execution_request(request)
    data = json.loads(result.messages[-1].content[result.messages[-1].content.index("{") :])
    assert data["current_scope"]["params"] == {"时间范围": "2026-10-27", "分析区域": "支路乙"}
    assert len(state["messages"]) == 1


@pytest.mark.parametrize("asynchronous", [False, True])
def test_validated_scene_clarification_creates_pending_action_without_business_model(asynchronous):
    import asyncio

    from langchain.agents import create_agent
    from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel

    from deerflow.agents.middlewares.clarification_middleware import ClarificationMiddleware
    from deerflow.agents.middlewares.raw_transcript_middleware import RawTranscriptMiddleware
    from deerflow.agents.thread_state import ThreadState
    from deerflow.tools.builtins.clarification_tool import ask_clarification_tool

    model = FakeMessagesListChatModel(responses=[])
    agent = create_agent(model, tools=[ask_clarification_tool], middleware=[RawTranscriptMiddleware(), ComputationGuardMiddleware(), ClarificationMiddleware()], state_schema=ThreadState)
    state = {"messages": [HumanMessage(content="流量异常吗", id="q")], "intent_context": {"scenes": [], "clarification_question": "道路、网络还是电话流量？", "_source_message_key": "id:q"}}
    result = asyncio.run(agent.ainvoke(state)) if asynchronous else agent.invoke(state)
    assert model.i == 0
    assert result["pending_action"]["question"] == "道路、网络还是电话流量？"
    assert result["pending_action"]["resume_intent_context"]["_source_message_key"] == "id:q"
    assert result["messages"][-1].name == "ask_clarification"
    raw_calls = [call["id"] for msg in result["raw_messages"] if isinstance(msg, AIMessage) for call in msg.tool_calls]
    assert result["messages"][-1].tool_call_id in raw_calls


def test_exact_verified_header_is_evidence_even_when_mapping_omits_that_metric(tmp_path):
    runtime = runtime_for(tmp_path, required_csv_metrics=["流量", "最高时刻"], required_artifact_types=["csv"])
    path = tmp_path / "summary.csv"
    path.write_text("road,flow,最高时刻\nA,600,09:00\n")
    update = deliver(runtime, path, {"流量": "flow"})
    assert update["verified_deliveries"]["/mnt/user-data/outputs/summary.csv"]["metric_columns"] == {"流量": "flow", "最高时刻": "最高时刻"}
    assert terminal(runtime) is None


def test_exact_header_inference_still_enforces_precision(tmp_path):
    runtime = runtime_for(tmp_path, required_csv_metrics=["同比"], csv_precision={"同比": 3})
    path = tmp_path / "summary.csv"
    path.write_text("year,同比\n2025,10.0\n")
    result = present_file_tool.func(runtime=runtime, filepaths=[str(path)], tool_call_id="p")
    assert "artifacts" not in result.update
    path.write_text("year,同比\n2025,10.000\n")
    deliver(runtime, path)
    assert terminal(runtime) is None


@pytest.mark.parametrize(
    "query,expected",
    [
        ("这轮不用生成文件。", {"*"}),
        ("不用再生成新文件，只给出数字。", {"*"}),
        ("不要重新导出任何文件。", {"*"}),
        ("无需输出文件。", {"*"}),
        ("不再导出CSV，但请生成JSON供下载。", {"csv"}),
        ("解释为什么之前没有生成文件。", set()),
        ("不要修改文件名。", set()),
        ("解释“不要生成文件”这句话。", set()),
        ("请生成JSON，但不要生成其他文件。", set()),
        ("请生成JSON但不要生成文件。", set()),
    ],
)
def test_literal_export_cancellations_do_not_become_business_keyword_routing(query, expected):
    from deerflow.routing.turn_requirements import explicit_export_cancellations

    assert explicit_export_cancellations(query) == expected


def test_explicit_no_file_request_clears_stale_exports_but_keeps_computation():
    from deerflow.routing.turn_requirements import requirements_from_context

    raw = {"original_query": "回到第一轮网络文件，只统计53端口的流数和字节。不用再生成新文件。", "requires_computation": True, "required_artifact_types": ["json"], "required_csv_metrics": ["旧指标"], "csv_precision": {"旧指标": 2}, "csv_missing_values": ["NA"]}
    current = requirements_from_context(raw)
    assert current.requires_computation
    assert current.required_artifact_types == []
    assert current.required_csv_metrics == []
    assert current.csv_precision == {}
    assert current.csv_missing_values == []


def test_type_specific_cancellation_preserves_other_requested_deliverables():
    from deerflow.routing.turn_requirements import requirements_from_context

    current = requirements_from_context({"original_query": "不再导出CSV，但请生成JSON供下载。", "required_artifact_types": ["csv", "json"], "required_csv_metrics": ["旧指标"]})
    assert current.required_artifact_types == ["json"]
    assert current.required_csv_metrics == []


@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize('missing_contract', [False, True])
def test_actual_graph_registers_current_generated_files_without_an_llm_bookkeeping_retry(tmp_path, monkeypatch, existing, missing_contract):
    import importlib

    from langchain.agents import create_agent
    from langchain.tools import tool
    from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel

    from deerflow.agents.thread_state import ThreadState

    present = importlib.import_module("deerflow.tools.builtins.present_file_tool")
    monkeypatch.setattr(present, "get_paths", lambda: SimpleNamespace(resolve_virtual_path=lambda thread, path: tmp_path / path.removeprefix("/mnt/user-data/outputs/")))
    (tmp_path / "old.json").write_text("{}")
    if existing:
        (tmp_path / "ast.json").write_text("{}")

    class Model(FakeMessagesListChatModel):
        def bind_tools(self, tools, **kwargs):
            return self

    @tool
    def bash(command: str) -> str:
        """Generate the requested synthetic AST file."""
        (tmp_path / "ast.json").write_text('{"type":"Module","body":[]}')
        return "AST generated"

    model = Model(responses=[AIMessage(content="", tool_calls=[{"name": "bash", "id": "calc", "args": {"command": "create"}}]), AIMessage(content="AST文件已经生成：/mnt/user-data/outputs/ast.json"), AIMessage(content="AST已交付")])
    agent = create_agent(model, tools=[bash, present_file_tool], middleware=[ComputationGuardMiddleware()], state_schema=ThreadState, context_schema=DeliveryTestContext)
    state = {"messages": [HumanMessage(content="生成AST供下载", id="q")], "thread_data": {"outputs_path": str(tmp_path)}, "intent_context": {"requires_computation": True, "required_artifact_types": [] if missing_contract else ["json"], "_source_message_key": "id:q"}}
    result = agent.invoke(state, context={"thread_id": "q"})
    assert result["artifacts"] == ["/mnt/user-data/outputs/ast.json"]
    assert result["verified_deliveries"]["/mnt/user-data/outputs/ast.json"]["source"] == "id:q"
    assert result["messages"][-1].content == "AST已交付"
    assert sum(isinstance(m, AIMessage) and m.response_metadata.get("auto_delivery", False) for m in result["messages"]) == 1
    assert not any(m.name == "computation_guard_retry" for m in result["messages"])


def test_automatic_delivery_does_not_reuse_an_unchanged_old_file(tmp_path):
    from langchain.agents import create_agent
    from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel

    from deerflow.agents.thread_state import ThreadState

    (tmp_path / "old.json").write_text("{}")

    class Model(FakeMessagesListChatModel):
        def bind_tools(self, tools, **kwargs):
            return self

    model = Model(responses=[AIMessage(content="文件已存在"), AIMessage(content="文件已存在")])
    agent = create_agent(model, tools=[present_file_tool], middleware=[ComputationGuardMiddleware()], state_schema=ThreadState)
    state = {"messages": [HumanMessage(content="生成新的AST", id="q")], "thread_data": {"outputs_path": str(tmp_path)}, "intent_context": {"required_artifact_types": ["json"], "_source_message_key": "id:q"}}
    result = agent.invoke(state)
    assert not result.get("artifacts")
    assert result["messages"][-1].response_metadata["execution_status"] == "failed"


@pytest.mark.parametrize('missing_contract', [False, True])
def test_automatic_delivery_still_rejects_wrong_precision_and_stops(tmp_path, monkeypatch, missing_contract):
    import importlib

    from langchain.agents import create_agent
    from langchain.tools import tool
    from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel

    from deerflow.agents.thread_state import ThreadState

    present = importlib.import_module("deerflow.tools.builtins.present_file_tool")
    monkeypatch.setattr(present, "get_paths", lambda: SimpleNamespace(resolve_virtual_path=lambda thread, path: tmp_path / path.removeprefix("/mnt/user-data/outputs/")))

    class Model(FakeMessagesListChatModel):
        def bind_tools(self, tools, **kwargs):
            return self

    @tool
    def bash(command: str) -> str:
        """Generate an intentionally invalid CSV."""
        (tmp_path / "bad.csv").write_text("同比\n10.0\n")
        return "generated"

    model = Model(responses=[AIMessage(content="", tool_calls=[{"name": "bash", "id": "calc", "args": {"command": "create"}}]), AIMessage(content="已生成/mnt/user-data/outputs/bad.csv"), AIMessage(content="生成好了"), AIMessage(content="生成好了")])
    agent = create_agent(model, tools=[bash, present_file_tool], middleware=[ComputationGuardMiddleware()], state_schema=ThreadState, context_schema=DeliveryTestContext)
    state = {"messages": [HumanMessage(content="导出三位小数CSV", id="q")], "thread_data": {"outputs_path": str(tmp_path)}, "intent_context": {"required_artifact_types": ["csv"], "required_csv_metrics": ["同比"], "csv_precision": {"同比": 3}, "_source_message_key": "id:q"}}
    if missing_contract:
        state['intent_context'] = {'original_query': '导出CSV，保留三位小数', '_source_message_key': 'id:q'}
    result = agent.invoke(state, context={"thread_id": "q"})
    assert not result.get("artifacts")
    assert result["messages"][-1].response_metadata["execution_status"] == "failed"
    assert sum(isinstance(m, AIMessage) and m.response_metadata.get("auto_delivery", False) for m in result["messages"]) == 1


def test_automatic_delivery_does_not_override_an_outer_output_refusal(tmp_path):
    from dataclasses import replace

    from langchain.agents import create_agent
    from langchain.agents.middleware import AgentMiddleware
    from langchain.tools import tool
    from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel

    from deerflow.agents.thread_state import ThreadState

    seen = []

    class OutputWrapper(AgentMiddleware):
        def wrap_model_call(self, request, handler):
            response = handler(request)
            last = response.result[-1]
            if last.content == "blocked draft":
                seen.append(last.content)
                return replace(response, result=[last.model_copy(update={"content": "refused", "response_metadata": {"compliance": {"actions": ["refuse"]}}})])
            return response

    class Model(FakeMessagesListChatModel):
        def bind_tools(self, tools, **kwargs):
            return self

    @tool
    def bash(command: str) -> str:
        """Generate a file that the output gate subsequently refuses to release."""
        (tmp_path / "ast.json").write_text("{}")
        return "generated"

    model = Model(responses=[AIMessage(content="", tool_calls=[{"name": "bash", "id": "calc", "args": {"command": "create"}}]), AIMessage(content="blocked draft")])
    agent = create_agent(model, tools=[bash, present_file_tool], middleware=[OutputWrapper(), ComputationGuardMiddleware()], state_schema=ThreadState)
    result = agent.invoke({"messages": [HumanMessage(content="export", id="q")], "thread_data": {"outputs_path": str(tmp_path)}, "intent_context": {"required_artifact_types": ["json"], "_source_message_key": "id:q"}})
    assert seen == ["blocked draft"]
    assert result["messages"][-1].content == "refused"
    assert not result.get("artifacts")


def test_actual_graph_finalizes_repeated_successful_delivery_without_more_tools(tmp_path, monkeypatch):
    import importlib

    from langchain.agents import create_agent
    from langchain.agents.middleware import AgentMiddleware
    from langchain.tools import tool
    from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel

    from deerflow.agents.middlewares.loop_detection_middleware import LoopDetectionMiddleware
    from deerflow.agents.thread_state import ThreadState

    present = importlib.import_module("deerflow.tools.builtins.present_file_tool")
    monkeypatch.setattr(present, "get_paths", lambda: SimpleNamespace(resolve_virtual_path=lambda thread, path: tmp_path / path.removeprefix("/mnt/user-data/outputs/")))
    seen_tools = []

    class Observe(AgentMiddleware):
        def wrap_model_call(self, request, handler):
            seen_tools.append([t.name for t in request.tools])
            return handler(request)

    class Model(FakeMessagesListChatModel):
        def bind_tools(self, tools, **kwargs):
            return self

    @tool
    def bash(command: str) -> str:
        """Generate and verify synthetic output."""
        (tmp_path / "ast.json").write_text("{}")
        return "GDP同比10%、10%；positives返回2；AST已生成"

    calls = [AIMessage(content="", tool_calls=[{"name": "bash", "id": "calc", "args": {"command": "create"}}])]
    calls += [AIMessage(content="", tool_calls=[{"name": "present_files", "id": f"present{i}", "args": {"filepaths": ["/mnt/user-data/outputs/ast.json"]}}]) for i in range(2)]
    model = Model(responses=[*calls, AIMessage(content="同比10%、10%，返回2，AST已交付")])
    agent = create_agent(model, tools=[bash, present_file_tool], middleware=[LoopDetectionMiddleware(warn_threshold=2, hard_limit=3), ComputationGuardMiddleware(), Observe()], state_schema=ThreadState, context_schema=DeliveryTestContext)
    result = agent.invoke({"messages": [HumanMessage(content="计算并导出AST", id="q")], "thread_data": {"outputs_path": str(tmp_path)}, "intent_context": {"requires_computation": True, "required_artifact_types": ["json"], "_source_message_key": "id:q"}}, context={"thread_id": "q"})
    assert seen_tools[-1] == []
    assert all("present_files" in names for names in seen_tools[:-1])
    assert result["messages"][-1].content == "同比10%、10%，返回2，AST已交付"
    assert result["artifacts"] == ["/mnt/user-data/outputs/ast.json"]


@pytest.mark.parametrize("problem", ["missing_artifact", "no_execution", "error", "old_source", "changed"])
def test_duplicate_delivery_cannot_finalize_incomplete_or_invalid_work(tmp_path, problem):
    from deerflow.routing.turn_requirements import requirements_from_context

    runtime = runtime_for(tmp_path, required_artifact_types=["json"])
    path = tmp_path / "ast.json"
    path.write_text("{}")
    deliver(runtime, path)
    messages = [HumanMessage(content="导出", id="q")]
    for i in range(2):
        messages.append(AIMessage(content="", tool_calls=[{"name": "present_files", "id": str(i), "args": {"filepaths": ["/mnt/user-data/outputs/ast.json"]}}]))
        messages.append(ToolMessage(content="Successfully presented files", name="present_files", tool_call_id=str(i), status="error" if problem == "error" else "success"))
    runtime.state["messages"] = messages
    if problem == "missing_artifact":
        runtime.state["intent_context"]["required_artifact_types"].append("py")
    elif problem == "no_execution":
        runtime.state["intent_context"]["requires_computation"] = True
    elif problem == "old_source":
        runtime.state["verified_deliveries"]["/mnt/user-data/outputs/ast.json"]["source"] = "id:old"
    elif problem == "changed":
        path.write_text('{"changed":true}')
    assert not ComputationGuardMiddleware._ready_after_duplicate_delivery(runtime.state, requirements_from_context(runtime.state["intent_context"]))


def test_reused_user_message_id_cannot_reuse_a_previous_runs_computation():
    from langchain.agents import create_agent
    from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel

    from deerflow.agents.thread_state import ThreadState

    model = FakeMessagesListChatModel(responses=[AIMessage(content="300"), AIMessage(content="300")])
    agent = create_agent(model, middleware=[ComputationGuardMiddleware()], state_schema=ThreadState)
    state = {
        "messages": [HumanMessage(content="编辑后的新计算", id="q"), AIMessage(content="", id="old_call", tool_calls=[{"name": "bash", "id": "old", "args": {}}]), ToolMessage(content="旧结果260", name="bash", tool_call_id="old"), AIMessage(content="旧答案260", id="old_answer")],
        "intent_context": {"requires_computation": True, "_source_message_key": "id:q"},
    }
    result = agent.invoke(state)
    assert result["messages"][-1].response_metadata["execution_status"] == "failed"


def test_same_message_id_cannot_reuse_a_previous_runs_delivery(tmp_path):
    from deerflow.delivery import current_deliveries

    runtime = runtime_for(tmp_path, required_artifact_types=["json"])
    runtime.state["output_baseline"] = {"source": "id:q", "run_key": "first"}
    path = tmp_path / "ast.json"
    path.write_text("{}")
    deliver(runtime, path)
    assert current_deliveries(runtime.state)
    runtime.state["output_baseline"]["run_key"] = "second"
    assert current_deliveries(runtime.state) == {}
