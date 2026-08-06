import importlib.util
import typing

if not hasattr(typing, "Self"):
    typing.Self = typing.Any

from deerflow.routing.dialogue_act import classify_dialogue_act

_clarification_spec = importlib.util.spec_from_file_location(
    "_clarification_middleware_under_test",
    "packages/harness/deerflow/agents/middlewares/clarification_middleware.py",
)
_clarification_module = importlib.util.module_from_spec(_clarification_spec)
assert _clarification_spec.loader is not None
_clarification_spec.loader.exec_module(_clarification_module)
ClarificationMiddleware = _clarification_module.ClarificationMiddleware


class _FakeResponse:
    def __init__(self, content: str):
        self.content = content


class _FakeDialogueLLM:
    def __init__(self, content: str):
        self.content = content
        self.calls: list[str] = []

    def with_config(self, config):
        return self

    def invoke(self, prompt):
        self.calls.append(str(prompt))
        return _FakeResponse(self.content)


def test_clarification_middleware_persists_pending_action_with_resume_context():
    args = {
        "question": "您倾向哪种方式？",
        "clarification_type": "approach_choice",
        "options": ["使用脚本自动阈值", "我指定具体阈值"],
    }
    state = {
        "intent_context": {"routing_query": "筛选重点号码"},
        "routing_context": {"global_selected_skills": ["condition-based-screening"]},
    }

    pending = ClarificationMiddleware._build_pending_action(args, state)

    assert pending["type"] == "clarification"
    assert pending["expected_answer_type"] == "single_choice"
    assert pending["options"][0]["label"] == "使用脚本自动阈值"
    assert "min_night_ratio" in pending["parameter_schema"]
    assert pending["resume_intent_context"] == state["intent_context"]
    assert pending["resume_routing_context"] == state["routing_context"]


def test_dialogue_act_binds_option_reply_to_pending_action():
    pending = {
        "id": "clarify_1",
        "type": "clarification",
        "question": "您倾向哪种方式？",
        "expected_answer_type": "single_choice",
        "options": [
            {"id": "1", "label": "使用脚本自动阈值", "value": "使用脚本自动阈值", "index": 1},
            {"id": "2", "label": "我指定具体阈值", "value": "我指定具体阈值", "index": 2},
        ],
        "resume_intent_context": {"routing_query": "筛选重点号码"},
        "resume_routing_context": {"global_selected_skills": ["condition-based-screening"]},
    }

    result = classify_dialogue_act("1", pending_action=pending)

    assert result.act == "clarification_answer"
    assert result.parsed_answer["label"] == "使用脚本自动阈值"
    assert result.resume_intent_context == pending["resume_intent_context"]
    assert result.resume_routing_context == pending["resume_routing_context"]


def test_dialogue_act_extracts_choice_and_parameter_updates():
    pending = {
        "id": "clarify_1",
        "type": "clarification",
        "question": "您倾向哪种方式？",
        "expected_answer_type": "single_choice",
        "options": [
            {"id": "1", "label": "使用脚本自动阈值", "value": "使用脚本自动阈值", "index": 1},
            {"id": "2", "label": "我指定具体阈值", "value": "我指定具体阈值", "index": 2},
        ],
        "resume_intent_context": {"routing_query": "筛选重点号码"},
        "resume_routing_context": {"global_selected_skills": ["condition-based-screening"]},
    }

    result = classify_dialogue_act("选1，不过夜间通话占比改成40%，联系人数量不少于50，共享设备数量不少于2", pending_action=pending)

    assert result.act == "clarification_answer"
    assert result.parsed_answer["label"] == "使用脚本自动阈值"
    assert result.metadata["parameter_updates"] == {
        "min_night_ratio": 0.4,
        "min_counterparties": 50,
        "min_shared_device_count": 2,
    }
    assert result.metadata["resume_plan"]["next_action"] == "resume_previous_task_with_parameter_updates"
    assert result.metadata["resume_plan"]["tool_args_patch"]["min_night_ratio"] == 0.4


def test_dialogue_act_extracts_parameter_update_without_choice():
    pending = {
        "id": "clarify_1",
        "type": "clarification",
        "question": "请给出筛选阈值",
        "expected_answer_type": "free_text",
        "resume_intent_context": {"routing_query": "筛选重点号码"},
        "resume_routing_context": {"global_selected_skills": ["condition-based-screening"]},
    }

    result = classify_dialogue_act("夜间次数不少于8次，联系人数量不少于60", pending_action=pending)

    assert result.act == "parameter_update"
    assert result.metadata["parameter_updates"] == {
        "min_night_count": 8,
        "min_counterparties": 60,
    }


def test_dialogue_act_validates_parameter_updates_against_schema():
    pending = {
        "id": "clarify_1",
        "type": "clarification",
        "question": "请给出筛选阈值",
        "expected_answer_type": "free_text",
        "parameter_schema": {
            "min_night_ratio": {"type": "float", "min": 0.0, "max": 1.0},
            "min_counterparties": {"type": "int", "min": 0},
        },
        "resume_intent_context": {"routing_query": "筛选重点号码"},
        "resume_routing_context": {"global_selected_skills": ["condition-based-screening"]},
    }

    result = classify_dialogue_act("夜间通话占比改成140%，联系人数量不少于50，共享设备数量不少于2", pending_action=pending)

    assert result.act == "parameter_update"
    assert result.metadata["parameter_updates"] == {"min_counterparties": 50}
    assert "Value above maximum for min_night_ratio" in result.metadata["conflicts"][0]
    assert any("Unsupported parameter ignored: min_shared_device_count" in item for item in result.metadata["conflicts"])
    assert result.metadata["resume_plan"]["next_action"] == "resolve_parameter_conflicts"


def test_dialogue_act_uses_llm_structured_parser_with_schema():
    pending = {
        "id": "clarify_1",
        "type": "clarification",
        "question": "您倾向哪种方式？",
        "expected_answer_type": "single_choice",
        "options": [
            {"id": "auto", "label": "使用脚本自动阈值", "value": "auto", "index": 1},
            {"id": "manual", "label": "我指定具体阈值", "value": "manual", "index": 2},
        ],
        "parameter_schema": {
            "threshold_mode": {"type": "enum", "values": ["auto", "manual", "inspect_distribution"]},
            "min_night_ratio": {"type": "float", "min": 0.0, "max": 1.0},
        },
        "resume_intent_context": {"routing_query": "筛选重点号码"},
        "resume_routing_context": {"global_selected_skills": ["condition-based-screening"]},
    }
    llm = _FakeDialogueLLM(
        '{"is_answer": true, "selected_option_id": "auto", '
        '"parameter_updates": {"threshold_mode": "auto", "min_night_ratio": 0.45}, '
        '"conflicts": [], "confidence": 0.91}'
    )

    result = classify_dialogue_act("自动，但夜间稍微严格一点", pending_action=pending, llm=llm)

    assert result.act == "clarification_answer"
    assert result.reason == "llm_structured_pending_action_reply"
    assert result.parsed_answer["id"] == "auto"
    assert result.metadata["parameter_updates"] == {"threshold_mode": "auto", "min_night_ratio": 0.45}
    assert result.metadata["resume_plan"]["selected_option_id"] == "auto"
