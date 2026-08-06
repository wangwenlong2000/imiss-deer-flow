"""Tests for the routing intent classifier."""

from deerflow.routing.intent import classify_routing_intent, load_scene_templates
from deerflow.routing.dialogue_act import classify_dialogue_act
from deerflow.routing.intent.classifier import TaskSpan, _normalize_task_span_texts, _resolve_scene_for_span, classify_routing_intent_with_llm


class _FakeResponse:
    def __init__(self, content: str):
        self.content = content


class _FakeIntentLLM:
    def __init__(self, *, file_prep_rewrite: bool = False, turn_decision: str | None = None):
        self.calls: list[str] = []
        self.file_prep_rewrite = file_prep_rewrite
        self.turn_decision = turn_decision

    def with_config(self, config):
        return self

    def invoke(self, messages):
        system_text = str(messages[0].content)
        user_text = str(messages[1].content) if len(messages) > 1 else ""
        self.calls.append(system_text)
        if "对话轮次决策器" in system_text or "对话轮次决策器" in user_text:
            return _FakeResponse(self.turn_decision or '{"act":"new_task","should_reclassify_intent":true,"should_reroute_skills":true,"confidence":0.9,"reason":"默认新任务"}')
        if "关联性" in system_text:
            return _FakeResponse("0.0")
        if "信息抽取机器人" in system_text:
            if "电动自行车违规停放" in user_text:
                return _FakeResponse('[{"name":"政策主题","value":"电动自行车违规停放与飞线充电整治"},{"name":"分析目标","value":"专项分析报告、合规风险判断"}]')
            return _FakeResponse('[{"name":"告警类型","value":"火灾"},{"name":"告警地点","value":"阳光小区3号楼2单元"}]')
        if "任务切分器" in system_text:
            if "视频监控" in user_text and "网络流量" in user_text and "异常峰值" in user_text:
                return _FakeResponse('[{"text":"分析视频监控的内容和网络流量的异常峰值"}]')
            if "pcap" in user_text and "合同" in user_text:
                return _FakeResponse(
                    '[{"text":"分析这个 pcap 文件中的异常通信"},{"text":"判断这份合同的违约责任和争议解决条款风险"}]'
                )
            if "电动自行车违规停放" in user_text and "可视化图表" in user_text:
                return _FakeResponse(
                    '[{"text":"请基于我上传的 2026 年一季度各区“电动自行车违规停放与飞线充电整治台账”、近期市级消防安全整治通知以及相关法律法规，分析目前哪些区整治压力最大、哪些问题最突出、现有处置措施是否存在执法或程序合规风险，并形成一份面向市领导的专项分析报告。"},'
                    '{"text":"报告需包含：各区问题数量、环比变化和重点风险类型；与现行法规、政策要求的逐条对应依据；需要优先督办的区县名单及原因；可直接落地的下一步行动建议。"},'
                    '{"text":"同时请给出至少 2 种可视化图表，并在结论中保留可回溯的数据来源位置。"}]'
                )
            if "消防安全整治通知" in user_text and "网络流量" in user_text:
                return _FakeResponse(
                    '[{"text":"基于我上传的 2026 年一季度各区“电动自行车违规停放与飞线充电整治台账”、近期市级消防安全整治通知以及相关法律法规，分析目前哪些区整治压力最大、哪些问题最突出、现有处置措施是否存在执法或程序合规风险，并形成一份面向市领导的专项分析报告。"},{"text":"对我上传的网络流量数据分析异常短连接"}]'
                )
            return _FakeResponse(f'[{{"text":"{user_text.strip()}"}}]')
        if "多场景任务拆分器" in system_text:
            if "视频监控" in user_text and "网络流量" in user_text and "异常峰值" in user_text:
                return _FakeResponse(
                    '[{"scene":"video_surveillance","scene_name":"视频监控","task_text":"分析视频监控的内容","params":{"视频对象":"视频监控"}},'
                    '{"scene":"network_traffic","scene_name":"网络流量","task_text":"分析网络流量的异常峰值","params":{"分析目标":"异常峰值"}}]'
                )
            if "pcap" in user_text and "合同" in user_text:
                return _FakeResponse(
                    '[{"scene":"network_traffic","scene_name":"网络流量","task_text":"分析这个 pcap 文件中的异常通信","params":{"分析对象":"pcap 文件","分析目标":"异常通信"}},'
                    '{"scene":"policy_regulation","scene_name":"政策法规","task_text":"判断这份合同的违约责任和争议解决条款风险","params":{"分析目标":"合同审查"}}]'
                )
            return _FakeResponse('[]')
        if "JSON 数组" in system_text:
            if "视频监控" in user_text and "网络流量" in user_text and "异常峰值" in user_text:
                return _FakeResponse('["video_surveillance","network_traffic"]')
            if "电动自行车违规停放" in user_text and "可视化图表" in user_text:
                return _FakeResponse('["policy_regulation"]')
            if "火灾" in user_text or "告警" in user_text:
                return _FakeResponse('["fire_alarm"]')
            if "pcap" in user_text and "合同" in user_text:
                return _FakeResponse('["network_traffic","policy_regulation"]')
            if "消防安全整治通知" in user_text and "网络流量" in user_text:
                return _FakeResponse('["policy_regulation","network_traffic"]')
            return _FakeResponse('["remote_sensing_image"]')
        if "只答选项序号" in system_text:
            if "消防告警" in system_text and ("火灾" in user_text or "告警" in user_text):
                return _FakeResponse("1")
            if "政策法规" in system_text and ("台账" in user_text or "法规" in user_text or "通知" in user_text):
                return _FakeResponse("2" if "消防告警" in system_text else "1")
            if "网络流量" in system_text and ("pcap" in user_text or "网络流量" in user_text):
                return _FakeResponse("1")
            return _FakeResponse("1")
        if "第一人称总结" in system_text:
            if self.file_prep_rewrite:
                return _FakeResponse("我需要先读取所有相关文件，以获取完整数据进行分析。")
            if "电动自行车违规停放" in user_text:
                return _FakeResponse(user_text)
            return _FakeResponse("我要处理阳光小区3号楼2单元的火灾告警")
        return _FakeResponse("")


class _FailingLLM:
    def with_config(self, config):
        return self

    def invoke(self, messages):
        raise AssertionError("LLM should not be called for clear deterministic scene matches")


class _FakeTurnLLM:
    def __init__(self, content: str):
        self.content = content
        self.calls: list[str] = []

    def with_config(self, config):
        return self

    def invoke(self, prompt):
        self.calls.append(str(prompt))
        return _FakeResponse(self.content)


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


def test_short_option_reply_without_context_does_not_select_scene():
    result = classify_dialogue_act("1")

    assert result.act == "chitchat"
    assert result.reason == "option_reply_without_pending_action"


def test_short_option_reply_is_bound_to_pending_action_without_scene_selection():
    llm = _FakeIntentLLM()
    pending_action = {
        "id": "clarify_threshold",
        "type": "clarification",
        "question": "您倾向哪种方式？",
        "expected_answer_type": "single_choice",
        "options": [
            {"id": "auto", "label": "使用脚本自动阈值", "value": "auto", "index": 1},
            {"id": "manual", "label": "我指定具体阈值", "value": "manual", "index": 2},
        ],
        "resume_intent_context": {"routing_query": "筛选夜间通话异常、联系人广度高、共享设备多的重点号码"},
        "resume_routing_context": {"global_selected_skills": ["condition-based-screening"]},
    }

    result = classify_dialogue_act(
        "1",
        pending_action=pending_action,
        llm=llm,
    )

    assert result.act == "clarification_answer"
    assert result.parsed_answer["id"] == "auto"
    assert result.resume_intent_context == pending_action["resume_intent_context"]
    assert result.resume_routing_context == pending_action["resume_routing_context"]
    assert llm.calls == []


def test_previous_task_followup_does_not_select_program_snippet_scene():
    previous_intent = {
        "intent": "task",
        "original_query": "请对该影像覆盖范围进行城市绿地生态评估",
        "normalized_query": "请对该影像覆盖范围进行城市绿地生态评估",
        "routing_query": "请对该影像覆盖范围进行城市绿地生态评估",
        "scene": "remote_sensing_image",
        "confidence": 0.95,
    }
    previous_routing = {"global_selected_skills": ["urban-greenspace-assessment"]}

    llm = _FakeTurnLLM(
        '{"act":"task_followup","should_reclassify_intent":false,'
        '"should_reroute_skills":false,"confidence":0.93,'
        '"reason":"用户在追问上一轮为什么未使用 skill 脚本"}'
    )

    result = classify_dialogue_act(
        "这次为什么不直接使用skill的脚本而是重新写脚本？",
        previous_intent_context=previous_intent,
        previous_routing_context=previous_routing,
        llm=llm,
    )

    assert result.act == "task_followup"
    assert result.reason == "llm_previous_task_followup"
    assert result.resume_intent_context == previous_intent
    assert result.resume_routing_context == previous_routing
    assert result.metadata["turn_decision"]["should_reclassify_intent"] is False
    assert any("对话轮次决策器" in call for call in llm.calls)


def test_code_compliance_query_prefers_program_snippet_scene():
    result = classify_routing_intent(
        "请你调用opengrep-compliance这个skill，对以下代码进行违规分析",
        scene_templates=load_scene_templates(),
    )

    assert result.scene == "program_snippet"
    assert result.scene_mode == "single"
    assert result.scenes == ["program_snippet"]
    assert [task.scene for task in result.scene_tasks] == ["program_snippet"]


def test_previous_context_new_task_turn_allows_reclassification():
    previous_intent = {
        "intent": "task",
        "original_query": "请对该影像覆盖范围进行城市绿地生态评估",
        "normalized_query": "请对该影像覆盖范围进行城市绿地生态评估",
        "routing_query": "请对该影像覆盖范围进行城市绿地生态评估",
        "scene": "remote_sensing_image",
        "confidence": 0.95,
    }
    previous_routing = {"global_selected_skills": ["urban-greenspace-assessment"]}
    llm = _FakeTurnLLM(
        '{"act":"new_task","should_reclassify_intent":true,'
        '"should_reroute_skills":true,"confidence":0.91,'
        '"reason":"用户提出新的网络流量分析对象"}'
    )

    result = classify_dialogue_act(
        "帮我分析这个 pcap 文件有没有异常通信",
        previous_intent_context=previous_intent,
        previous_routing_context=previous_routing,
        llm=llm,
    )

    assert result.act == "new_task"
    assert result.reason == "llm_new_task_or_scene_shift"
    assert result.metadata["turn_decision"]["should_reclassify_intent"] is True


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
    assert result.scene_mode == "single"
    assert len(result.scene_tasks) == 1


def test_user_scene_templates_are_merged():
    templates = load_scene_templates()

    assert "fire_alarm" in templates
    assert "network_traffic" in templates
    assert "policy_regulation" in templates
    assert "spatiotemporal_trajectory" in templates
    assert "street_view_image" in templates
    assert "remote_sensing_image" in templates
    assert "video_surveillance" in templates
    assert "road_traffic" in templates
    assert templates["network_traffic"]["scene"] == "network_traffic"


def test_user_network_traffic_scene_match():
    result = classify_routing_intent(
        "使用网络流量分析 skill 对 Neris 做 summary",
        scene_templates=load_scene_templates(),
    )

    assert result.scene == "network_traffic"
    assert result.scene_name == "网络流量"
    assert "网络流量" in result.routing_query


def test_user_road_traffic_scene_match():
    result = classify_routing_intent(
        "统计昨天晚高峰解放路路口交通流量并生成趋势表",
        scene_templates=load_scene_templates(),
    )

    assert result.scene == "road_traffic"
    assert result.scene_name == "交通流量"
    assert "road_traffic" in result.routing_query


def test_scene_anti_keywords_are_applied_in_deterministic_match():
    result = classify_routing_intent(
        "分析 pcap 网络流量是否异常",
        scene_templates=load_scene_templates(),
    )

    assert result.scene == "network_traffic"
    assert result.scene != "road_traffic"


def test_clear_runner_up_gap_does_not_fall_through_to_llm():
    templates = {
        "top_scene": {
            "name": "甲场景",
            "keywords": ["甲关键词", "甲任务"],
            "description": "",
        },
        "runner_up": {
            "name": "乙场景",
            "keywords": ["乙关键词"],
            "description": "",
        },
    }

    scene_id, config = _resolve_scene_for_span(
        "请处理甲关键词和甲任务，同时参考乙关键词",
        llm=_FailingLLM(),
        scene_templates=templates,
    )

    assert scene_id == "top_scene"
    assert config == templates["top_scene"]


def test_user_video_surveillance_scene_match():
    result = classify_routing_intent(
        "帮我分析昨晚 8 点到 10 点东门摄像头监控录像里是否有人员聚集和异常停留",
        scene_templates=load_scene_templates(),
    )

    assert result.scene == "video_surveillance"
    assert result.scene_name == "视频监控"
    assert "video_surveillance" in result.routing_query


def test_user_remote_sensing_image_scene_match():
    result = classify_routing_intent(
        "帮我分析这两期卫星遥感影像里建设用地扩张变化，并输出变化检测结果",
        scene_templates=load_scene_templates(),
    )

    assert result.scene == "remote_sensing_image"
    assert result.scene_name == "遥感图像"
    assert "remote_sensing_image" in result.scenes
    assert len(result.scenes) == 1


def test_multi_scene_match_with_llm():
    result = classify_routing_intent_with_llm(
        "分析这个 pcap 文件中的异常通信，并判断这份合同的违约责任和争议解决条款风险",
        llm=_FakeIntentLLM(),
        scene_templates=load_scene_templates(),
    )

    assert result.scene == "network_traffic"
    assert result.scenes == ["network_traffic", "policy_regulation"]
    assert result.routing_query == "分析这个 pcap 文件中的异常通信，并判断这份合同的违约责任和争议解决条款风险"
    assert result.params == {}
    assert result.task_hints == []
    assert result.scene_mode == "multi"
    assert len(result.scene_tasks) == 2
    assert result.scene_tasks[0].scene == "network_traffic"
    assert result.scene_tasks[1].scene == "policy_regulation"


def test_single_span_multi_scene_query_keeps_video_and_network_scenes():
    result = classify_routing_intent_with_llm(
        "分析视频监控的内容和网络流量的异常峰值",
        llm=_FakeIntentLLM(),
        scene_templates=load_scene_templates(),
        previous_intent={
            "intent": "task",
            "scene": "network_traffic",
            "scenes": ["network_traffic"],
            "routing_query": "分析网络流量异常峰值",
        },
    )

    assert result.scene_mode == "multi"
    assert result.scenes == ["video_surveillance", "network_traffic"]
    assert [task.scene for task in result.scene_tasks] == ["video_surveillance", "network_traffic"]
    assert result.scene_tasks[0].text == "分析视频监控的内容"
    assert result.scene_tasks[1].text == "分析网络流量的异常峰值"
    assert result.scene == "video_surveillance"


def test_fire_alarm_not_selected_for_governance_plus_network_query():
    result = classify_routing_intent_with_llm(
        "请基于我上传的 2026 年一季度各区“电动自行车违规停放与飞线充电整治台账”、近期市级消防安全整治通知以及相关法律法规，分析目前哪些区整治压力最大、哪些问题最突出、现有处置措施是否存在执法或程序合规风险，并形成一份面向市领导的专项分析报告。同时进行网络流量分析，对我上传的网络流量数据分析异常短连接",
        llm=_FakeIntentLLM(),
        scene_templates=load_scene_templates(),
    )

    assert result.scene_mode == "multi"
    assert result.scenes == ["policy_regulation", "network_traffic"]
    assert [task.scene for task in result.scene_tasks] == ["policy_regulation", "network_traffic"]
    assert "fire_alarm" not in result.scenes
    assert result.scene == "policy_regulation"


def test_uploaded_files_block_is_not_used_as_task_text():
    query = (
        "<uploaded_files>\n"
        "policy_demo_fire_safety_notice(1).pdf (6.2 KB)\n"
        "policy_demo_ebike_governance_ledger(1).xlsx (24.3 KB)\n"
        "</uploaded_files>\n"
        "请基于我上传的 2026 年一季度各区“电动自行车违规停放与飞线充电整治台账”、"
        "近期市级消防安全整治通知以及相关法律法规，分析各区整治压力、合规风险，"
        "形成面向市领导的专项分析报告，并给出至少 2 种可视化图表。"
    )

    result = classify_routing_intent_with_llm(
        query,
        llm=_FakeIntentLLM(file_prep_rewrite=True),
        scene_templates=load_scene_templates(),
        uploaded_files=[
            {"filename": "policy_demo_fire_safety_notice(1).pdf"},
            {"filename": "policy_demo_ebike_governance_ledger(1).xlsx"},
        ],
    )

    assert result.scene == "policy_regulation"
    assert len(result.scene_tasks) == 1
    assert "读取所有相关文件" not in result.routing_query
    assert "读取所有相关文件" not in result.scene_tasks[0].text
    assert "专项分析报告" in result.scene_tasks[0].text
    assert "<uploaded_files>" not in result.original_query


def test_policy_report_output_requirements_are_not_split_into_scene_tasks():
    query = (
        "请基于我上传的 2026 年一季度各区“电动自行车违规停放与飞线充电整治台账”、"
        "近期市级消防安全整治通知以及相关法律法规，分析目前哪些区整治压力最大、"
        "哪些问题最突出、现有处置措施是否存在执法或程序合规风险，并形成一份面向市领导的专项分析报告。"
        "报告需包含：各区问题数量、环比变化和重点风险类型；"
        "与现行法规、政策要求的逐条对应依据；"
        "需要优先督办的区县名单及原因；"
        "可直接落地的下一步行动建议。"
        "同时请给出至少 2 种可视化图表，并在结论中保留可回溯的数据来源位置。"
    )

    result = classify_routing_intent_with_llm(
        query,
        llm=_FakeIntentLLM(),
        scene_templates=load_scene_templates(),
    )

    assert result.scene_mode == "single"
    assert result.scenes == ["policy_regulation"]
    assert [task.scene for task in result.scene_tasks] == ["policy_regulation"]
    assert "street_view_image" not in result.scenes
    assert "可视化图表" in result.scene_tasks[0].text
    assert "可回溯的数据来源位置" in result.scene_tasks[0].text


def test_task_span_header_fragment_falls_back_to_full_query():
    query = (
        "请基于我上传的 2026 年一季度各区“电动自行车违规停放与飞线充电整治台账”、"
        "近期市级消防安全整治通知以及相关法律法规，分析目前哪些区整治压力最大、"
        "哪些问题最突出、现有处置措施是否存在执法或程序合规风险，并形成一份面向市领导的专项分析报告。"
        "报告需包含：各区问题数量、环比变化和重点风险类型；与现行法规、政策要求的逐条对应依据。"
    )
    spans = [
        TaskSpan(text="请基于我上传的 2026 年一季度各区“电动自行车违规停放与飞线充电整治台账”、近期市级消防安全整治通知以及相关法律法规，分析目前哪些区整治压力最大、哪些问题最突出、现有处置措施是否存在执法或程序合规风险，并形成一份面向市领导的专项分析报告。"),
        TaskSpan(text="报告需包含："),
    ]

    assert _normalize_task_span_texts(spans, query) == [query]


def test_task_span_output_requirements_merge_back_to_full_query():
    query = (
        "请基于我上传的 2026 年一季度各区“电动自行车违规停放与飞线充电整治台账”、"
        "近期市级消防安全整治通知以及相关法律法规，分析目前哪些区整治压力最大、"
        "哪些问题最突出、现有处置措施是否存在执法或程序合规风险，并形成一份面向市领导的专项分析报告。"
        "报告需包含：各区问题数量、环比变化和重点风险类型；与现行法规、政策要求的逐条对应依据。"
        "同时请给出至少 2 种可视化图表，并在结论中保留可回溯的数据来源位置。"
    )
    spans = [
        TaskSpan(text="请基于我上传的 2026 年一季度各区“电动自行车违规停放与飞线充电整治台账”、近期市级消防安全整治通知以及相关法律法规，分析目前哪些区整治压力最大、哪些问题最突出、现有处置措施是否存在执法或程序合规风险，并形成一份面向市领导的专项分析报告。"),
        TaskSpan(text="报告需包含：各区问题数量、环比变化和重点风险类型；与现行法规、政策要求的逐条对应依据。"),
        TaskSpan(text="同时请给出至少 2 种可视化图表，并在结论中保留可回溯的数据来源位置。"),
    ]

    assert _normalize_task_span_texts(spans, query) == [query]


def test_configured_scene_field_is_emitted():
    result = classify_routing_intent(
        "统计昨天晚高峰解放路路口交通流量并生成趋势表",
        scene_templates=load_scene_templates(),
    )

    assert result.scene == load_scene_templates()["road_traffic"]["scene"]
