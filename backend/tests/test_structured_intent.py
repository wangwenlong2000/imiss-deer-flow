"""Behavioral coverage for the compact recognition and mobile planning contract."""

import asyncio
import json
from types import SimpleNamespace

import pytest

from app.gateway.routers.mobile import _segments_to_ordered_segments, adapt_events_to_mobile
from deerflow.agents.middlewares.todo_middleware import _build_routing_guidance_message
from deerflow.routing.intent import structured


class Model:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []
        self.settings = {}

    def bind(self, **kwargs):
        self.settings.update(kwargs)
        return self

    def with_config(self, config):
        return self

    def invoke(self, messages):
        self.calls.append(messages)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return SimpleNamespace(content=response)

    async def ainvoke(self, messages):
        return self.invoke(messages)


def payload(*tasks, steps=None, confidence=0.85, clarification=None):
    from deerflow.routing.turn_requirements import REQUIREMENTS_EXAMPLE
    return json.dumps({**REQUIREMENTS_EXAMPLE, "tasks": list(tasks), "planning_steps": steps or ["读取输入并核验字段", "完成专业分析并交付结果"], "confidence": confidence, "clarification_question": clarification}, ensure_ascii=False)


def task(scene, text, *, evidence=None, **params):
    return {"scene": scene, "text": text, "evidence": evidence or text, "params": params}


def recognize(query, llm, asynchronous=False, **kwargs):
    if asynchronous:
        return asyncio.run(structured.aclassify(query, llm=llm, **kwargs))
    return structured.classify(query, llm=llm, **kwargs)


@pytest.mark.parametrize("asynchronous", [False, True])
def test_one_call_preserves_constraints_parameters_and_actual_mobile_plan(asynchronous):
    query = "统计解放路路口晚高峰车流量，生成两种图表，并保留可追溯数据来源。"
    steps = ["确认'晚高峰'范围并读取路口观测数据", "统计车流量并交付图表与来源索引"]
    model = Model(payload(task("road_traffic", "统计车流量", evidence="解放路路口晚高峰车流量", 分析区域="解放路路口", 时间范围="晚高峰", invented="discard"), steps=steps))

    result = recognize(query, model, asynchronous)

    assert len(model.calls) == 1
    assert result.scene == "road_traffic"
    assert result.scene_tasks[0].text == query
    assert result.params == {"分析区域": "解放路路口", "时间范围": "晚高峰"}
    assert [s.text for s in result.planning_steps] == steps
    assert query.rstrip("。") in result.routing_query
    assert model.settings["temperature"] == 0
    assert model.settings["max_tokens"] == 3072
    message = _build_routing_guidance_message({"intent_context": result.model_dump(), "routing_context": {"route_mode": "scene_filter"}})
    events = [{"event": "values", "data": {"messages": [message.model_dump()]}}]
    segments = _segments_to_ordered_segments(adapt_events_to_mobile("eval", "eval", events, query)["segments"])
    planner = [s for s in segments if s["role"] == "规划智能体"]
    assert len(planner) == 1
    assert planner[0]["metadata"]["source_event"] == "task_planning"
    assert all(s in planner[0]["content"] for s in steps)


@pytest.mark.parametrize("asynchronous", [False, True])
def test_three_domains_are_retained_in_one_call(asynchronous):
    query = "分析 pcap 异常通信、审查合同，并识别 GPS 轨迹停留点。"
    model = Model(payload(task("network_traffic", "分析 pcap 异常通信"), task("policy_regulation", "审查合同"), task("spatiotemporal_trajectory", "识别 GPS 轨迹停留点")))
    result = recognize(query, model, asynchronous)
    assert result.scenes == ["network_traffic", "policy_regulation", "spatiotemporal_trajectory"]
    assert result.scene_mode == "multi"
    assert len(result.scene_tasks) == 3
    assert result.routing_query == query
    assert result.params == {}
    assert len(model.calls) == 1


@pytest.mark.parametrize("query", ["你好", "列出你的全部技能"])
def test_obvious_control_turns_need_no_model(query):
    model = Model()
    result = recognize(query, model)
    assert result.intent in {"chitchat", "capability_inventory"}
    assert model.calls == []


def test_unknown_domain_keeps_task_and_plan_without_guessing_scene():
    query = "请生成一份普通材料总结，包含图表、风险和来源。"
    result = recognize(query, Model(payload(steps=["阅读材料并归纳主题", "整理结论与来源并交付总结"])))
    assert result.scene is None
    assert result.intent == "task"
    assert result.routing_query == query
    assert len(result.planning_steps) == 2


@pytest.mark.parametrize("bad", ["not json", '{"tasks":[],"planning_steps":[]}', payload(task("invented", "审查合同")), payload(task("policy_regulation", "审查合同", evidence=" "))])
def test_only_one_repair_for_invalid_output(bad):
    model = Model(bad, payload(task("policy_regulation", "审查采购合同")))
    result = recognize("审查采购合同", model)
    assert len(model.calls) == 2
    assert result.reason == "structured_intent"
    assert result.scene == "policy_regulation"


def test_invalid_output_does_not_start_the_legacy_pipeline():
    model = Model("invalid", "invalid")
    result = recognize("分析这个 pcap 文件", model)
    assert len(model.calls) == 2
    assert result.scene is None
    assert result.recognition_status == 'failed'
    assert result.original_query == "分析这个 pcap 文件"
    assert result.planning_steps == []
    assert result.reason == "structured_fallback_validation"


def test_transport_failure_is_not_retried_by_the_classifier():
    model = Model(RuntimeError("offline"))
    result = recognize("审查采购合同", model)
    assert len(model.calls) == 1
    assert result.scene is None
    assert result.recognition_status == 'failed'
    assert result.original_query == "审查采购合同"
    assert result.reason == "structured_fallback_model_error"


def test_hallucinated_quote_cannot_supply_domain_evidence():
    bad = payload(task("remote_sensing_image", "分析卫星遥感影像"))
    model = Model(bad, bad)
    result = recognize("提取手机号13800138000并分析号码画像", model)
    assert result.scene is None
    assert result.recognition_status == 'failed'
    assert "remote_sensing_image" not in result.scenes
    assert len(model.calls) == 2


def test_default_scenes_also_need_a_quote_from_the_input():
    bad = payload(task("fire_alarm", "处置火灾"))
    result = recognize("总结这份普通文本", Model(bad, payload()))
    assert result.scene is None


def test_upload_metadata_is_not_treated_as_a_task():
    query = "<uploaded_files>fire_alarm.pcap</uploaded_files>\n审查合同并生成报告"
    model = Model(payload(task("policy_regulation", "审查合同")))
    result = recognize(query, model)
    assert result.scene_tasks[0].text == "审查合同并生成报告"
    assert "<uploaded_files>" not in str(model.calls[0])


def test_new_business_task_does_not_inherit_an_unrelated_previous_scene():
    model = Model(payload(task("network_traffic", "分析 pcap 通信")))
    result = recognize("分析 pcap 通信", model, previous_intent={"scene": "policy_regulation", "scenes": ["policy_regulation"]})
    assert result.scenes == ["network_traffic"]
    assert len(model.calls) == 1


def test_configured_public_scene_id_is_preserved():
    templates = {"legal": {"name": "合同审查", "scene": "policy_regulation", "domain_anchors": ["合同"], "parameters": []}}
    result = recognize("审查合同", Model(payload(task("legal", "审查合同"))), scene_templates=templates)
    assert result.scene == "policy_regulation"
    assert result.scenes == ["policy_regulation"]


def test_missing_model_cannot_silently_execute_a_keyword_subset():
    result = recognize("分析 pcap 文件", None)
    assert result.scene is None
    assert result.recognition_status == 'failed'


@pytest.mark.parametrize("asynchronous", [False, True])
def test_same_scene_independent_tasks_are_retained(asynchronous):
    model = Model(payload(task("network_traffic", "分析 A.pcap 的 DNS"), task("network_traffic", "分析 B.pcap 的 TLS", evidence="B.pcap 的 TLS")))
    result = recognize("分析 A.pcap 的 DNS 和 B.pcap 的 TLS", model, asynchronous)
    assert len(result.scene_tasks) == 2
    assert len(model.calls) == 1


def test_apostrophes_in_valid_json_are_not_replaced():
    raw = payload(task("policy_regulation", "核验合同"), steps=["读取名为 'purchase' 的合同并核验条款"])
    result = recognize("核验合同", Model(raw))
    assert result.planning_steps[0].text == "读取名为 'purchase' 的合同并核验条款"


def test_all_candidates_reach_semantic_recognition_without_keyword_filtering():
    query = "依据法规核验索引，分析整治台账中的执法合规风险，交付含图表的简报。"
    model = Model(payload(task("policy_regulation", "核验执法合规风险", evidence="依据法规核验索引")))
    result = recognize(query, model)
    prompt = model.calls[0][0].content
    assert '"id":"policy_regulation"' in prompt
    for scene in structured.core.load_scene_templates():
        assert f'"id":"{scene}"' in prompt
    assert len(result.planning_steps) == 2


def test_public_entry_uses_the_structured_pipeline():
    from deerflow.routing.intent import aclassify_routing_intent_with_llm, classify_routing_intent_with_llm

    assert classify_routing_intent_with_llm is structured.classify
    assert aclassify_routing_intent_with_llm is structured.aclassify


def test_explicit_reference_can_ground_an_omitted_object_without_an_extra_call():
    query = "基于刚才的结果，预测这些对象的下一处位置。"
    previous = {"original_query": "对上传 GPS 轨迹提取停留点", "scene": "spatiotemporal_trajectory"}
    model = Model(payload(task("spatiotemporal_trajectory", "预测下一处位置", evidence="预测这些对象的下一处位置")))
    result = recognize(query, model, previous_intent=previous)
    assert result.scene == "spatiotemporal_trajectory"
    assert result.original_query == query
    assert len(model.calls) == 1


def test_model_generated_history_cannot_ground_a_missing_domain():
    previous = {"scene": "remote_sensing_image", "routing_query": "分析卫星遥感影像"}
    bad = payload(task("remote_sensing_image", "分析卫星遥感影像"))
    result = recognize("基于刚才结果继续分析", Model(bad, payload()), previous_intent=previous)
    assert result.scene is None


@pytest.mark.parametrize("asynchronous", [False, True])
def test_semantic_scene_is_accepted_without_a_domain_keyword(asynchronous):
    query = "看看每天早上大家从哪里出发，又在哪些片区落脚。"
    model = Model(payload(task("mobility", "分析跨区域移动", evidence="从哪里出发"), confidence=0.81))
    templates = {"mobility": {"name": "时空轨迹", "scene": "spatiotemporal_trajectory", "domain_anchors": ["GPS"], "parameters": []}}
    result = recognize(query, model, asynchronous, scene_templates=templates)
    assert result.scene == "spatiotemporal_trajectory"
    assert result.confidence == 0.81
    assert result.reason == "structured_intent"
    assert len(model.calls) == 1


def test_short_request_receives_bounded_file_metadata_without_attachment_bodies():
    model = Model(payload(task("video_surveillance", "找出异常片段", evidence="gate.mp4")))
    files = [{"filename": "gate.mp4", "content_type": "video/mp4", "content": "private attachment body", "path": "/private/mount/gate.mp4"}]
    result = recognize("看看这里面有没有不正常的片段", model, uploaded_files=files)
    context = json.loads(model.calls[0][1].content)
    assert context["uploaded_files"] == [{"filename": "gate.mp4", "content_type": "video/mp4"}]
    assert "private" not in model.calls[0][1].content
    assert result.scene == "video_surveillance"


def test_implicit_continuation_has_prior_user_task_without_reference_keywords():
    query = "那他们一般几点回去？"
    previous = {"original_query": "分析 GPS 记录中的人员通勤规律", "routing_query": "invented domain", "scene": "spatiotemporal_trajectory"}
    model = Model(payload(task("spatiotemporal_trajectory", "分析返回时间", evidence="人员通勤规律")))
    result = recognize(query, model, previous_intent=previous)
    context = json.loads(model.calls[0][1].content)
    assert context["previous_user_task"] == previous["original_query"]
    assert "invented domain" not in model.calls[0][1].content
    assert result.scene == "spatiotemporal_trajectory"


def test_ambiguous_request_keeps_a_clarification_for_the_execution_agent():
    question = "您想分析哪类数据或业务的异常？"
    model = Model(payload(confidence=0.2, clarification=question, steps=["先确认数据对象", "根据确认的对象制定分析方案"]))
    result = recognize("帮我看看有没有异常", model)
    assert result.scenes == []
    assert result.clarification_question == question
    message = _build_routing_guidance_message({"intent_context": result.model_dump(), "routing_context": {"route_mode": "scene_filter"}})
    assert question in message.content
    assert "ask_clarification" in message.content
    assert len(model.calls) == 1


def test_new_task_does_not_receive_prior_model_scene_or_generated_evidence():
    model = Model(payload(task("network_traffic", "分析 pcap", evidence="分析 pcap")))
    result = recognize("分析 pcap", model, previous_intent={"original_query": "审查合同", "scene": "policy_regulation", "routing_query": "generated text"})
    assert result.scenes == ["network_traffic"]
    context = json.loads(model.calls[0][1].content)
    assert set(context) == {"current_query", "previous_user_task", "previous_turn_requirements"}
    assert 'scene' not in context['previous_turn_requirements']
    assert "generated text" not in model.calls[0][1].content


def test_extra_planning_steps_are_compacted_without_a_repair_call():
    steps = [f"规划步骤{i}" for i in range(9)]
    model = Model(payload(task("network_traffic", "分析 pcap"), steps=steps))
    result = recognize("分析 pcap", model)
    assert len(result.planning_steps) == 7
    assert all(step in "\n".join(s.text for s in result.planning_steps) for step in steps)
    assert len(model.calls) == 1


def test_async_middleware_does_not_run_sync_dialogue_model_on_event_loop(monkeypatch):
    import threading

    from langchain_core.messages import HumanMessage

    from deerflow.agents.middlewares import intent_recognition_middleware as module
    from deerflow.routing.dialogue_act import DialogueActResult

    loop_thread = threading.get_ident()

    def dialogue(*args, **kwargs):
        assert threading.get_ident() != loop_thread
        return DialogueActResult(act="new_task", confidence=0.9, reason="test")

    async def routing(query, **kwargs):
        return structured.core.RoutingIntentResult(intent="task", original_query=query, normalized_query=query, routing_query=query)

    monkeypatch.setattr(module, "classify_dialogue_act", dialogue)
    monkeypatch.setattr(module, "aclassify_routing_intent_with_llm", routing)
    monkeypatch.setattr(module, "create_chat_model", lambda **kwargs: None)
    middleware = module.IntentRecognitionMiddleware()
    result = asyncio.run(middleware.abefore_agent({"messages": [HumanMessage(content="分析 pcap", id="current")]}, runtime=None))
    assert result["intent_context"]["_source_message_key"] == "id:current"
