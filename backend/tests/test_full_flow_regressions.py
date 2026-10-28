"""Regressions reproduced from full-flow runs; all inputs are synthetic."""

import asyncio
import importlib
import json
import runpy
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from langchain_core.messages import HumanMessage
from pydantic import TypeAdapter

from deerflow.agents.middlewares import intent_recognition_middleware as intent_module
from deerflow.agents.middlewares.uploads_middleware import UploadsMiddleware
from deerflow.community.aio_sandbox.aio_sandbox_provider import AioSandboxProvider
from deerflow.community.aio_sandbox.local_backend import LocalContainerBackend
from deerflow.config.paths import Paths
from deerflow.routing.dialogue_act import DialogueActResult
from deerflow.routing.intent import RoutingIntentResult
from deerflow.routing.intent.structured import _grounded_evidence, _input_context

invoke_module = importlib.import_module("deerflow.tools.builtins.invoke_skill_tool")
present_module = importlib.import_module("deerflow.tools.builtins.present_file_tool")


@pytest.mark.parametrize("evidence", [
    ["话单", "异常外联"],
    "一份是网络抓包...异常外联",
    "一份是网络抓包…异常外联",
    "这次只审查合同。然后只需要列出修改建议。",
])
def test_discontinuous_real_quotes_do_not_discard_semantically_valid_tasks(evidence):
    source = "一份是话单，一份是网络抓包，分别找共享终端和异常外联。\n然后只需要列出修改建议。\n这次只审查合同。"
    assert _grounded_evidence(evidence, source)


@pytest.mark.parametrize("evidence", [[], [""], ["话单", "卫星影像"], "话单...已发现网络攻击", "话单...", " "])
def test_quote_recovery_never_ignores_an_unsupported_fragment(evidence):
    assert not _grounded_evidence(evidence, "只有话单数据")


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("known_scene", [False, True])
def test_real_clarification_reply_resolves_unknown_scene_but_known_parameters_stay_fast(monkeypatch, asynchronous, known_scene):
    previous = RoutingIntentResult(intent="task", original_query="流量有没有异常", normalized_query="流量有没有异常", routing_query="流量有没有异常", scenes=["network_traffic"] if known_scene else []).model_dump()
    if known_scene:
        previous["_confirmed_task_query"] = "分析网络流量，检查异常外联"
    dialogue = DialogueActResult(act="clarification_answer", confidence=1, reason="pending_choice", parsed_answer={"label": "网络抓包", "value": "network"}, resume_intent_context=previous)
    monkeypatch.setattr(intent_module, "classify_dialogue_act", lambda *args, **kwargs: dialogue)
    monkeypatch.setattr(intent_module, "create_chat_model", lambda **kwargs: object())
    calls = []

    def classify(query, **kwargs):
        calls.append(query)
        assert "流量有没有异常" in query and "网络抓包" in query
        return RoutingIntentResult(intent="task", original_query=query, normalized_query=query, routing_query=query, scene="network_traffic", scenes=["network_traffic"], planning_steps=[{"text": "核验抓包并分析异常"}])

    async def aclassify(query, **kwargs):
        return classify(query, **kwargs)

    monkeypatch.setattr(intent_module, "classify_routing_intent_with_llm", classify)
    monkeypatch.setattr(intent_module, "aclassify_routing_intent_with_llm", aclassify)
    middleware = intent_module.IntentRecognitionMiddleware()
    state = {"messages": [HumanMessage(content="2", id="reply-id")], "intent_context": previous, "pending_action": {"id": "pending"}}
    runtime = SimpleNamespace(context={})
    result = asyncio.run(middleware.abefore_agent(state, runtime)) if asynchronous else middleware.before_agent(state, runtime)
    assert result["intent_context"]["scenes"] == ["network_traffic"]
    assert result["intent_context"]["original_query"] == "2"
    assert result["intent_context"]["_source_message_key"] == "id:reply-id"
    assert result["pending_action"] is None
    assert len(calls) == (0 if known_scene else 1)
    if not known_scene:
        assert "网络抓包" in result["intent_context"]["_confirmed_task_query"]
        assert not result["intent_context"].get("_suppress_hidden_steps")
    else:
        assert result["intent_context"]["_confirmed_task_query"] == previous["_confirmed_task_query"]


def test_upload_history_survives_a_turn_without_new_uploads(tmp_path):
    uploads = Paths(str(tmp_path)).sandbox_uploads_dir("history")
    uploads.mkdir(parents=True)
    (uploads / "capture.pcap").write_bytes(b"synthetic")
    middleware = UploadsMiddleware(base_dir=str(tmp_path))
    state = {"messages": [HumanMessage(content="回到最早上传的文件")], "uploaded_files": []}
    result = middleware.before_agent(state, SimpleNamespace(context={"thread_id": "history"}))
    assert result["uploaded_files"][0]["filename"] == "capture.pcap"
    assert "synthetic" not in str(result["uploaded_files"])
    (uploads / "capture.pcap").unlink()
    result = middleware.before_agent({**state, "uploaded_files": result["uploaded_files"]}, SimpleNamespace(context={"thread_id": "history"}))
    assert result["uploaded_files"] == []


def test_revisit_context_includes_earlier_actual_user_goals_without_model_summaries():
    text, evidence = _input_context("回到最早上传的文件", {"original_query": "审查合同", "routing_query": "invented summary", "_user_task_history": ["检查抓包中有没有异常通信", "审查合同"]}, [{"filename": "capture.pcap"}])
    context = json.loads(text)
    assert context["historical_user_tasks"][0] == "检查抓包中有没有异常通信"
    assert "检查抓包" in evidence
    assert "invented summary" not in text


def test_actual_tool_schema_accepts_the_array_string_that_failed_live():
    annotation = invoke_module.invoke_skill_tool.args_schema.model_fields["legacy_artifacts"].annotation
    raw = '["/mnt/user-data/outputs/ast.json"]'
    parsed = TypeAdapter(annotation).validate_python(raw)
    assert invoke_module._normalize_legacy_artifacts(parsed) == ["/mnt/user-data/outputs/ast.json"]


def test_wrap_output_completes_with_the_failed_live_parameter_shape(monkeypatch, tmp_path):
    skill = SimpleNamespace(name="network-traffic-analysis", category="custom", skill_path="network-traffic-analysis", skill_dir=tmp_path)
    monkeypatch.setattr(invoke_module, "_skill_lookup", lambda: {skill.name: skill})
    runtime = SimpleNamespace(state={}, context={"thread_id": "wrap"})
    result = json.loads(invoke_module.invoke_skill_tool.func(runtime=runtime, skill_name=skill.name, tool_call_id="wrap", mode="wrap_output", input_envelope={"scenario": "network_traffic"}, legacy_output="verified", legacy_artifacts='["/mnt/user-data/outputs/report.csv"]'))
    assert result["status"] == "wrapped"
    assert result["skill_result"]["result"]["artifacts"][0]["uri"] == "/mnt/user-data/outputs/report.csv"


def test_wrapper_does_not_report_empty_execution_as_success():
    with pytest.raises(ValueError, match="actual legacy_output"):
        invoke_module._wrap_skill_result(envelope={}, skill_name="example", legacy_output=None, legacy_artifacts=[], status="success")


@pytest.mark.parametrize("raw", ['{"path":"ast.json"}', '[7]', '[""]', 'not-json'])
def test_artifact_array_normalization_rejects_malformed_paths(raw):
    with pytest.raises(ValueError):
        invoke_module._normalize_legacy_artifacts(raw)


def test_skill_envelope_uses_actual_user_text_instead_of_internal_guidance():
    runtime = SimpleNamespace(state={"messages": [HumanMessage(content="分析这段代码"), HumanMessage(content="long internal routing prompt", name="todo_routing_guidance")]})
    assert invoke_module._last_user_text(runtime) == "分析这段代码"


@pytest.mark.parametrize("skill", ["extract_stay_points", "clean_trajectory"])
@pytest.mark.parametrize("minutes,expected", [(19, 0), (20, 1), (21, 1)])
def test_terminal_stay_and_inclusive_duration_in_both_shipped_libraries(skill, minutes, expected):
    root = Path(__file__).resolve().parents[2]
    lib = runpy.run_path(str(root / f"skills/custom/spatiotemporal_trajectory/{skill}/scripts/_preprocess_common/trajectory_preprocess_lib.py"))
    start = datetime(2026, 10, 20, 8)
    points = [lib["Point"](user_id="A", trajectory_id="a", timestamp=start + timedelta(minutes=n), lat=30, lon=120) for n in [0, minutes]]
    stays = lib["extract_staypoints"](points, 200, 20, 6)
    assert len(stays) == expected
    if stays:
        assert stays[0]["duration_minutes"] == minutes
        assert stays[0]["point_count"] == 2


@pytest.mark.parametrize("body,columns,digits,success", [
    ("year,yoy,cumulative\n2024,10.00,21.00\n", ["yoy", "cumulative"], {"yoy": 2}, True),
    ("year,yoy\n2024,10.00\n", ["yoy", "cumulative"], {}, False),
    ("year,yoy,cumulative\n2024,10.0,21.00\n", ["yoy", "cumulative"], {"yoy": 2}, False),
    ("year,yoy,cumulative\n2022,,\n2024,10.00,21.00\n", ["yoy", "cumulative"], {"yoy": 2}, True),
])
def test_delivery_checks_validate_actual_csv_not_just_a_download_path(tmp_path, body, columns, digits, success):
    path = tmp_path / "growth.csv"
    path.write_text(body)
    runtime = SimpleNamespace(state={"thread_data": {"outputs_path": str(tmp_path)}}, context={"thread_id": "csv"})
    result = present_module.present_file_tool.func(runtime=runtime, filepaths=[str(path)], tool_call_id="csv-check", delivery_checks=json.dumps([{"filepath": str(path), "required_columns": columns, "decimal_places": digits}]))
    assert ("artifacts" in result.update) is success
    assert ("Verified CSV facts" in result.update["messages"][0].content) is success


def test_explicit_precision_cannot_bypass_csv_delivery_checks(tmp_path):
    path = tmp_path / "growth.csv"
    path.write_text("year,yoy\n2024,10.0\n")
    runtime = SimpleNamespace(state={"thread_data": {"outputs_path": str(tmp_path)}, "intent_context": {"original_query": "计算同比，结果保留两位小数，输出CSV"}}, context={"thread_id": "precision"})
    result = present_module.present_file_tool.func(runtime=runtime, filepaths=[str(path)], tool_call_id="precision")
    assert "artifacts" not in result.update
    assert "decimal_places=2" in result.update["messages"][0].content


def test_shared_upload_does_not_allocate_a_container_even_when_ports_are_full(monkeypatch, tmp_path):
    from app.gateway.routers import uploads

    provider = SimpleNamespace(uploads_are_shared=True)
    monkeypatch.setattr(uploads, "get_uploads_dir", lambda thread: tmp_path)
    monkeypatch.setattr(uploads, "get_sandbox_provider", lambda: provider)
    monkeypatch.setattr(uploads, "convert_file_to_markdown", AsyncMock(return_value=None))
    result = asyncio.run(uploads.save_thread_upload_from_bytes("shared", "capture.pcap", b"synthetic"))
    assert result["filename"] == "capture.pcap"
    assert (tmp_path / "capture.pcap").read_bytes() == b"synthetic"


def test_only_local_container_backends_skip_upload_sync():
    provider = object.__new__(AioSandboxProvider)
    provider._backend = object.__new__(LocalContainerBackend)
    assert provider.uploads_are_shared
    provider._backend = object()
    assert not provider.uploads_are_shared


def test_sandbox_command_logs_never_include_environment_values(monkeypatch, caplog):
    import logging

    from deerflow.community.aio_sandbox import local_backend

    backend = object.__new__(LocalContainerBackend)
    backend._runtime = "docker"
    backend._image = "synthetic-image"
    backend._environment = {"API_KEY": "synthetic-secret-value"}
    backend._config_mounts = []
    seen = []

    def run(command, **kwargs):
        seen.extend(command)
        return SimpleNamespace(stdout="synthetic-container")

    monkeypatch.setattr(local_backend.subprocess, "run", run)
    caplog.set_level(logging.INFO)
    backend._start_container(container_name="synthetic", port=8180)
    assert "API_KEY=synthetic-secret-value" in seen
    assert "synthetic-secret-value" not in caplog.text
    assert "API_KEY=<redacted>" in caplog.text


def test_csv_metric_mapping_cannot_omit_a_requested_growth_column(tmp_path):
    from langchain_core.messages import AIMessage

    from deerflow.agents.middlewares.computation_guard_middleware import ComputationGuardMiddleware

    path = tmp_path / "growth.csv"
    path.write_text("year,yoy\n2024,10.00\n")
    runtime = SimpleNamespace(state={"thread_data": {"outputs_path": str(tmp_path)}, "intent_context": {"_source_message_key": "id:q", "required_csv_metrics": ["同比", "累计增幅"]}}, context={"thread_id": "metric"})
    result = present_module.present_file_tool.func(runtime=runtime, filepaths=[str(path)], tool_call_id="metric", delivery_checks=[{"filepath": str(path), "metric_columns": {"同比": "yoy"}}])
    assert result.update['artifacts'] == ['/mnt/user-data/outputs/growth.csv']
    state = {**runtime.state, **result.update, 'messages': [HumanMessage(content='计算同比和累计增幅并导出', id='q'), AIMessage(content='完成')]}
    guarded = ComputationGuardMiddleware._check(state)
    assert guarded['jump_to'] == 'model'
    assert '累计增幅' in guarded['messages'][-1].content


@pytest.mark.parametrize("as_json", [False, True])
def test_csv_single_check_accepts_existing_english_headers(tmp_path, as_json):
    import json

    path = tmp_path / "growth.csv"
    path.write_text("year,yoy,cumulative\n2024,10.00,21.00\n")
    runtime = SimpleNamespace(state={"thread_data": {"outputs_path": str(tmp_path)}, "intent_context": {"original_query": "结果保留两位小数", "required_csv_metrics": ["同比", "累计增幅"]}}, context={"thread_id": "metric"})
    check = {"filepath": str(path), "metric_columns": {"同比": "yoy", "累计增幅": "cumulative"}, "decimal_places": {"yoy": 2, "cumulative": 2}}
    parameter = json.dumps(check) if as_json else check
    annotation = present_module.present_file_tool.args_schema.model_fields["delivery_checks"].annotation
    parsed = TypeAdapter(annotation).validate_python(parameter)
    result = present_module.present_file_tool.func(runtime=runtime, filepaths=[str(path)], tool_call_id="metric", delivery_checks=parsed)
    assert result.update["artifacts"] == ["/mnt/user-data/outputs/growth.csv"]
    assert "21.00" in result.update["messages"][0].content


def test_csv_delivery_internals_stay_outside_user_visible_planning_steps():
    import re

    from deerflow.agents.middlewares.todo_middleware import _build_routing_guidance_message

    message = _build_routing_guidance_message({"intent_context": {"intent": "task", "requires_computation": True, "required_csv_metrics": ["GDP同比", "GDP累计增幅"]}})
    assert "metric_columns" in message.content
    assert '"GDP累计增幅"' in message.content
    for step in re.findall(r"<hidden_step[^>]*>(.*?)</hidden_step>", message.content, flags=re.S):
        assert "metric_columns" not in step


def test_computation_guard_ignores_previous_turn_results_and_preserves_refusal():
    from langchain_core.messages import AIMessage, ToolMessage

    from deerflow.agents.middlewares.computation_guard_middleware import ComputationGuardMiddleware

    state = {"intent_context": {"requires_computation": True, "_source_message_key": "id:new"}, "messages": [ToolMessage(content="260", name="bash", tool_call_id="old"), HumanMessage(content="计算新的平均值", id="new"), AIMessage(content="300", id="bad")]}
    result = ComputationGuardMiddleware._check(state)
    assert result["jump_to"] == "model"
    assert "300" not in result["messages"][0].content
    state["messages"][-1] = AIMessage(content="拒绝", response_metadata={"compliance": {"actions": ["refuse"]}})
    assert ComputationGuardMiddleware._check(state) is None


def test_actual_graph_retries_unexecuted_arithmetic_once_and_uses_tool_result():
    from langchain.agents import create_agent
    from langchain.tools import tool
    from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
    from langchain_core.messages import AIMessage

    from deerflow.agents.middlewares.computation_guard_middleware import ComputationGuardMiddleware
    from deerflow.agents.thread_state import ThreadState

    choices = []

    class Model(FakeMessagesListChatModel):
        def bind_tools(self, tools, **kwargs):
            choices.append(kwargs.get("tool_choice"))
            return self

    calls = []

    @tool
    def bash(command: str) -> str:
        """Compute the synthetic test average."""
        calls.append(command)
        return str((100 + 100 + 900 + 100 + 100) / 5)

    model = Model(responses=[AIMessage(content="平均值是300"), AIMessage(content="", tool_calls=[{"name": "bash", "id": "calc", "args": {"command": "calculate"}}]), AIMessage(content="平均值是260")])
    agent = create_agent(model, tools=[bash], middleware=[ComputationGuardMiddleware()], state_schema=ThreadState)
    result = agent.invoke({"messages": [HumanMessage(content="计算五点均值", id="q")], "intent_context": {"requires_computation": True, "_source_message_key": "id:q"}})
    assert len(calls) == 1
    assert result["messages"][-1].content == "平均值是260"
    assert all("300" not in str(m.content) for m in result["messages"])
    assert choices == [None, None, None]


def test_computation_guard_stops_after_one_unsuccessful_retry():
    from langchain_core.messages import AIMessage, SystemMessage

    from deerflow.agents.middlewares.computation_guard_middleware import ComputationGuardMiddleware

    state = {"intent_context": {"requires_computation": True, "_source_message_key": "id:q"}, "messages": [HumanMessage(content="计算", id="q"), SystemMessage(content="retry", name="computation_guard_retry"), AIMessage(content="300")]}
    result = ComputationGuardMiddleware._check(state)
    assert result["jump_to"] == "end"
    assert "无法交付可靠数值" in result["messages"][0].content
