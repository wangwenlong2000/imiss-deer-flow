"""Tests for the routing intent classifier."""

from deerflow.routing.intent import classify_routing_intent, load_scene_templates
from deerflow.routing.intent.classifier import classify_routing_intent_with_llm


class _FakeResponse:
    def __init__(self, content: str):
        self.content = content


class _FakeIntentLLM:
    def __init__(self):
        self.calls: list[str] = []

    def invoke(self, messages):
        system_text = str(messages[0].content)
        self.calls.append(system_text)
        if "关联性" in system_text:
            return _FakeResponse("0.0")
        if "只答选项序号" in system_text:
            return _FakeResponse("1")
        if "信息抽取机器人" in system_text:
            return _FakeResponse('[{"name":"告警类型","value":"火灾"},{"name":"告警地点","value":"阳光小区3号楼2单元"}]')
        if "第一人称总结" in system_text:
            return _FakeResponse("我要处理阳光小区3号楼2单元的火灾告警")
        return _FakeResponse("")


def test_capability_inventory_query():
    result = classify_routing_intent("你具备哪些能力")

    assert result.intent == "capability_inventory"
    assert "可用的技能能力" in result.routing_query


def test_all_skills_query():
    result = classify_routing_intent("列出你的全部skill")

    assert result.intent == "capability_inventory"


def test_chitchat_query():
    result = classify_routing_intent("你好")

    assert result.intent == "chitchat"


def test_fire_alarm_scene_match():
    result = classify_routing_intent(
        "阳光小区3号楼2单元发生火灾告警",
        scene_templates=load_scene_templates(),
    )

    assert result.intent == "task"
    assert result.scene == "fire_alarm"
    assert result.scene_name == "消防告警"
    assert "消防告警" in result.routing_query
    assert "告警地点" in result.params


def test_other_scene_still_routes_as_task():
    result = classify_routing_intent(
        "西安市今天天气怎么样",
        scene_templates=load_scene_templates(),
    )

    assert result.intent == "task"
    assert result.scene is None
    assert result.routing_query == "西安市今天天气怎么样"


def test_llm_scene_slot_and_rewrite():
    result = classify_routing_intent_with_llm(
        "阳光小区3号楼2单元发生火灾告警",
        llm=_FakeIntentLLM(),
        scene_templates=load_scene_templates(),
    )

    assert result.scene == "fire_alarm"
    assert result.scene_name == "消防告警"
    assert result.params["告警类型"] == "火灾"
    assert result.params["告警地点"] == "阳光小区3号楼2单元"
    assert "我要处理阳光小区3号楼2单元的火灾告警" in result.routing_query
    assert result.reason == "llm_scene_slot_rewrite"


def test_user_scene_templates_are_merged():
    templates = load_scene_templates()

    assert "fire_alarm" in templates
    assert "network_traffic" in templates
    assert "policy_regulation" in templates
    assert "spatiotemporal_trajectory" in templates
    assert "street_view_image" in templates
    assert "traffic_flow" in templates
    assert templates["network_traffic"]["scene"] == "network_traffic"


def test_user_network_traffic_scene_match():
    result = classify_routing_intent(
        "使用网络流量分析 skill 对 Neris 做 summary",
        scene_templates=load_scene_templates(),
    )

    assert result.scene == "network_traffic"
    assert result.scene_name == "网络流量"
    assert "网络流量" in result.routing_query


def test_user_traffic_flow_scene_match():
    result = classify_routing_intent(
        "统计昨天晚高峰解放路路口交通流量并生成趋势表",
        scene_templates=load_scene_templates(),
    )

    assert result.scene == "traffic_flow"
    assert result.scene_name == "交通流量"


def test_configured_scene_field_is_emitted():
    result = classify_routing_intent(
        "统计昨天晚高峰解放路路口交通流量并生成趋势表",
        scene_templates=load_scene_templates(),
    )

    assert result.scene == load_scene_templates()["traffic_flow"]["scene"]
