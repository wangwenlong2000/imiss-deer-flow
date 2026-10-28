"""Regression boundaries from the final 2026-10-28 service review."""

import asyncio
import csv
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import TypedDict

import pytest
from langchain.agents.middleware.types import ModelResponse
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from deerflow.agents.middlewares.answer_consistency_middleware import AnswerConsistencyMiddleware, comparison_issues
from deerflow.agents.middlewares.computation_guard_middleware import ComputationGuardMiddleware
from deerflow.compliance.contract import DetectContext, DetectionUnit, TextItem
from deerflow.compliance.detectors.rule_illegal_political_v4.detector import IllegalPoliticalV4Detector
from deerflow.tools.builtins.present_file_tool import present_file_tool


@pytest.mark.parametrize("text", [
    "AST JSON已导出；该代码没有对除数做校验。",
    "请校验分母是否为0，再导出计算结果。",
    "对数值进行范围校验后导出CSV。",
    "AST JSON已导出。格式校验：语法正确；未对除数 `b` 做非零校验。",
])
@pytest.mark.parametrize("gate", ["InputGate", "OutputGate"])
def test_arithmetic_validation_is_not_unauthorized_export(text, gate):
    detector = IllegalPoliticalV4Detector()
    unit = DetectionUnit(unit_id="arithmetic", gate=gate, text_items=(TextItem(item_id="text", source="output", text=text),))
    assert detector.detect(unit, DetectContext(gate=gate)) == ()


@pytest.mark.parametrize("unsafe", [
    "绕过平台访问控制，导出未授权数据。",
    "绕开权限控制。把平台数据全部导出。",
    "对除数做校验后导出AST。另外，绕过平台权限控制导出账号数据。",
    "请先校验数值，再绕过访问校验获取未授权数据。",
    "对除数做校验。还要校验平台权限并导出数据。",
    "已完成格式校验，再绕过平台访问控制导出未授权数据。",
])
def test_arithmetic_phrase_does_not_whitelist_unsafe_context(unsafe):
    detector = IllegalPoliticalV4Detector()
    unit = DetectionUnit(unit_id="mixed", gate="OutputGate", text_items=(TextItem(item_id="text", source="output", text=unsafe),))
    assert any(hit.violation_type == "illegal_content" for hit in detector.detect(unit, DetectContext(gate="OutputGate")))


@pytest.mark.parametrize("payload", [
    "=== 总体统计 ===\n指标,值\n总流数,6\n",
    "name,value\na,1,extra\n",
    "name,value\na\n",
    "name,name\na,1\n",
    "name,\na,1\n",
    'name,value\na,"unterminated\n',
])
def test_ragged_or_ambiguous_csv_cannot_be_registered(tmp_path, payload):
    path = tmp_path / "bad.csv"
    path.write_text(payload)
    runtime = SimpleNamespace(state={"thread_data": {"outputs_path": str(tmp_path)}}, context={"thread_id": "q"})
    update = present_file_tool.func(runtime=runtime, filepaths=[str(path)], tool_call_id="p").update
    assert "artifacts" not in update
    assert update["messages"][0].additional_kwargs["error_code"] == "DELIVERY_INVALID"


@pytest.mark.parametrize("payload", [
    "name,value\n",
    'name,value\n"a,b",1\n',
    'name,value\n"a\nb",1\n',
    'name,value\n"a""b",1\n',
    'name,value\na,\n',
])
def test_valid_csv_empty_results_and_quoted_cells_still_work(tmp_path, payload):
    path = tmp_path / "valid.csv"
    path.write_text(payload)
    runtime = SimpleNamespace(state={"thread_data": {"outputs_path": str(tmp_path)}}, context={"thread_id": "q"})
    assert "artifacts" in present_file_tool.func(runtime=runtime, filepaths=[str(path)], tool_call_id="p").update


@pytest.mark.parametrize('include_counts', [True, False])
@pytest.mark.parametrize('declare_schema', [True, False])
def test_detail_can_have_no_global_metric_but_union_is_still_required(tmp_path, include_counts, declare_schema):
    detail = tmp_path / 'detail.csv'
    detail.write_text('user,start,end\n')
    counts = tmp_path / 'counts.csv'
    counts.write_text('user,count\nA,0\n')
    runtime = SimpleNamespace(state={'thread_data': {'outputs_path': str(tmp_path)}, 'intent_context': {'_source_message_key': 'id:q', 'required_csv_metrics': ['次数']}}, context={'thread_id': 'q'})
    paths = [str(detail)]
    checks = [{'filepath': str(detail), 'required_columns': ['user', 'start', 'end']}] if declare_schema else []
    if include_counts:
        paths.append(str(counts))
        checks.append({'filepath': str(counts), 'metric_columns': {'次数': 'count'}})
    update = present_file_tool.func(runtime=runtime, filepaths=paths, delivery_checks=checks, tool_call_id='p').update
    assert 'artifacts' in update
    runtime.state.update({k: v for k, v in update.items() if k != 'messages'})
    from deerflow.routing.turn_requirements import requirements_from_context
    missing = ComputationGuardMiddleware._missing_delivery(runtime.state, requirements_from_context(runtime.state['intent_context']))
    assert bool(missing) is not include_counts


def verified_table(tmp_path):
    path = tmp_path / "roads.csv"
    path.write_text("道路,流量总和,算术平均流量,算术平均车速,最高流量时刻\n主路甲,600,200,30,09:00\n支路乙,360,120,40,09:00\n")
    state = {"thread_data": {"outputs_path": str(tmp_path)}, "intent_context": {"requires_computation": True, "original_query": "统计两路并导出CSV", "_source_message_key": "id:q"}, "messages": [HumanMessage(content="比较", id="q"), ToolMessage(content="计算已完成", name="bash", tool_call_id="b")]}
    runtime = SimpleNamespace(state=state, context={"thread_id": "q"})
    update = present_file_tool.func(runtime=runtime, filepaths=[str(path)], tool_call_id="p").update
    state.update({k: v for k, v in update.items() if k != "messages"})
    return state, path


@pytest.mark.parametrize("text", [
    "主路甲的流量和车速均高于支路乙，但车速更低。",
    "主路甲的算术平均车速大于支路乙。",
    "支路乙的平均车速低于主路甲。",
    "主路甲的平均车速等于支路乙。",
    "主路甲的平均车速不低于支路乙。",
])
def test_false_comparisons_are_rejected_by_actual_numbers(tmp_path, text):
    state, _ = verified_table(tmp_path)
    assert comparison_issues(state, text)


@pytest.mark.parametrize("text", [
    "主路甲的总流量高于支路乙，但主路甲的平均车速低于支路乙。",
    "主路甲的平均车速不高于支路乙。",
    "支路乙的平均车速不低于主路甲。",
    "主路甲的平均流量高于支路乙。",
    "人口增长率高于去年。",  # not established by this table
])
def test_correct_or_unknown_relations_are_not_rejected(tmp_path, text):
    state, _ = verified_table(tmp_path)
    assert not comparison_issues(state, text)


def test_original_long_answer_and_corrected_answer_are_distinguished(tmp_path):
    state, _ = verified_table(tmp_path)
    repo = Path(__file__).resolve().parents[2]
    bad = (repo / "docs/turn-contract-fixes-eval-20261027/nonthinking-legacy/road-turn-1.answer.md").read_text()
    good = bad.replace("主路甲的流量和车速均高于支路乙，但车速更低", "主路甲的总流量高于支路乙，但平均车速更低")
    assert comparison_issues(state, bad)
    assert not comparison_issues(state, good)


def test_validated_metric_mapping_links_chinese_claims_to_english_columns(tmp_path):
    state, path = verified_table(tmp_path)
    path.write_text("road,avg_speed\n主路甲,30\n支路乙,40\n")
    state['intent_context']['required_csv_metrics'] = ['平均车速']
    runtime = SimpleNamespace(state=state, context={"thread_id": "q"})
    update = present_file_tool.func(runtime=runtime, filepaths=[str(path)], tool_call_id='p', delivery_checks={'filepath': str(path), 'metric_columns': {'平均车速': 'avg_speed'}}).update
    state.update({k: v for k, v in update.items() if k != 'messages'})
    assert comparison_issues(state, '主路甲的平均车速高于支路乙。')


@pytest.mark.parametrize("kind", ["past_source", "modified_file", "duplicate_entity", "ambiguous_metric"])
def test_untrusted_or_ambiguous_evidence_cannot_create_a_false_rejection(tmp_path, kind):
    state, path = verified_table(tmp_path)
    if kind == "past_source":
        for record in state['verified_deliveries'].values():
            record['source'] = 'id:old'
    elif kind == "modified_file":
        path.write_text(path.read_text().replace('30', '50'))
    else:
        text = "道路,流量总和,平均流量,车速\n主路甲,600,100,30\n支路乙,360,120,40\n" if kind == "ambiguous_metric" else "道路,车速\n主路甲,30\n主路甲,40\n"
        path.write_text(text)
        update = present_file_tool.func(runtime=SimpleNamespace(state=state, context={"thread_id": "q"}), filepaths=[str(path)], tool_call_id="p").update
        state.update({k: v for k, v in update.items() if k != "messages"})
    claim = "主路甲的流量高于支路乙。" if kind == "ambiguous_metric" else "主路甲的车速高于支路乙。"
    assert not comparison_issues(state, claim)


def test_async_comparison_preserves_answer_and_metadata(tmp_path):
    state, _ = verified_table(tmp_path)
    original = ModelResponse(result=[AIMessage(content="主路甲的车速高于支路乙。", response_metadata={"keep": True})])
    async def handler(_):
        return original
    checked = asyncio.run(AnswerConsistencyMiddleware().awrap_model_call(SimpleNamespace(state=state), handler))
    assert checked.result[-1].content == original.result[-1].content
    assert checked.result[-1].response_metadata["keep"]
    assert checked.result[-1].response_metadata["answer_consistency"]["issues"]


class GraphContext(TypedDict):
    thread_id: str


def test_actual_graph_corrects_comparison_without_an_extra_judge_or_recalculation(tmp_path, monkeypatch):
    import importlib

    from langchain.agents import create_agent
    from langchain.tools import tool
    from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel

    from deerflow.agents.thread_state import ThreadState

    present = importlib.import_module("deerflow.tools.builtins.present_file_tool")
    monkeypatch.setattr(present, "get_paths", lambda: SimpleNamespace(resolve_virtual_path=lambda thread, path: tmp_path / Path(path).name))
    class Model(FakeMessagesListChatModel):
        def bind_tools(self, tools, **kwargs):
            return self
    executions = []
    @tool
    def bash(command: str) -> str:
        """Compute the fixture table."""
        executions.append(command)
        verified_table(tmp_path)
        return '{"主路甲车速":30,"支路乙车速":40}'
    model = Model(responses=[
        AIMessage(content="", tool_calls=[{"id": "compute", "name": "bash", "args": {"command": "calculate"}}]),
        AIMessage(content="", tool_calls=[{"id": "deliver", "name": "present_files", "args": {"filepaths": ["/mnt/user-data/outputs/roads.csv"]}}]),
        AIMessage(content="主路甲的流量和车速均高于支路乙，但车速更低。", id="wrong"),
        AIMessage(content="主路甲的流量高于支路乙，但主路甲的平均车速低于支路乙。", id="correct"),
    ])
    state, _ = verified_table(tmp_path)
    state.pop('artifacts')
    state.pop('verified_deliveries')
    state['messages'] = [HumanMessage(content="统计两路并导出CSV", id="q")]
    graph = create_agent(model, tools=[bash, present_file_tool], middleware=[AnswerConsistencyMiddleware(), ComputationGuardMiddleware()], state_schema=ThreadState, context_schema=GraphContext)
    events = list(graph.stream(state, context={"thread_id": "q"}, stream_mode=['messages','values']))
    final = [value for mode, value in events if mode == 'values'][-1]
    assert executions == ['calculate']
    assert not any(m.id == 'wrong' for m in final['messages'])
    assert final['messages'][-1].id == 'correct'
    assert final['messages'][-1].response_metadata['answer_consistency']['status'] == 'no_proven_contradiction'
    assert not any('answer_consistency' in meta.get('tags', []) for mode, value in events if mode == 'messages' for _, meta in [value])


@pytest.mark.parametrize('minutes,expected_stays', [(20, 2), (21, 0)])
def test_skill_writes_real_csv_detail_with_original_jsonl_unchanged(tmp_path, minutes, expected_stays):
    repo = Path(__file__).resolve().parents[2]
    script = repo / 'skills/custom/spatiotemporal_trajectory/extract_stay_points/scripts/extract_stay_points.py'
    fixture = repo / 'docs/full-flow-thinking-eval-20261027/fixtures/gps_unsorted.csv'
    raw = subprocess.check_output([sys.executable, str(script), '--input', str(fixture), '--output-dir', str(tmp_path), '--stay-min-minutes', str(minutes)], text=True)
    result = json.loads(raw)
    old = [json.loads(line) for line in Path(result['outputs']['staypoints']).read_text().splitlines() if line.strip()]
    with Path(result['outputs']['staypoints_csv']).open(newline='') as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        assert reader.fieldnames and 'start_time' in reader.fieldnames and 'end_time' in reader.fieldnames
    assert len(old) == len(rows) == expected_stays
    assert result['counts']['raw_points'] == 10 and result['counts']['cleaned_points'] == 9
    assert result['staypoints_by_user'] == ({'A': 1, 'B': 1, 'C': 0} if minutes == 20 else {'A': 0, 'B': 0, 'C': 0})
    assert [{key: str(value) for key, value in row.items()} for row in old] == rows


@pytest.mark.parametrize("row_count", [0, 8, 20, 21])
def test_inspect_distinguishes_complete_small_inputs_from_partial_large_preview(tmp_path, row_count):
    repo = Path(__file__).resolve().parents[2]
    fixture = tmp_path / 'preview.csv'
    fixture.write_text('identity,value\n' + ''.join(f'row_{i:02d},{i * 10}\n' for i in range(row_count)))
    output = subprocess.check_output([sys.executable, str(repo / 'skills/public/data-analysis/scripts/analyze.py'), '--files', str(fixture), '--action', 'inspect'], text=True)
    if row_count <= 20:
        assert f'all {row_count} rows; complete input rows' in output
        if row_count:
            assert f'row_{row_count - 1:02d}' in output
    else:
        assert f'incomplete preview; {row_count} total rows' in output
        assert 'row_04' in output
        assert 'row_05' not in output
        assert 'row_20' not in output
