from langchain_core.messages import HumanMessage

from deerflow.agents.middlewares import intent_recognition_middleware as intent_module
from deerflow.agents.middlewares.intent_recognition_middleware import IntentRecognitionMiddleware


def test_intent_recognition_ignores_internal_view_image_context_for_same_user_message():
    middleware = IntentRecognitionMiddleware(model_name="unused")

    update = middleware.before_agent(
        {
            "messages": [
                HumanMessage(content="分析视频中的事件", id="user-1"),
                HumanMessage(
                    content=[{"type": "text", "text": "Here are the images you've viewed:"}],
                    id="view-image-context-1",
                    additional_kwargs={"message_type": "view_image_context", "internal": True},
                ),
            ],
            "intent_context": {
                "routing_query": "分析视频中的事件",
                "scene": "video_surveillance",
                "_source_message_key": "id:user-1",
            },
        },
        runtime=None,
    )

    assert update is None


def test_intent_recognition_prepare_input_uses_last_external_user_message():
    middleware = IntentRecognitionMiddleware(model_name="unused")

    prepared = middleware._prepare_input(
        {
            "messages": [
                HumanMessage(content="分析视频中的事件", id="user-1"),
                HumanMessage(
                    content=[{"type": "text", "text": "Here are the images you've viewed:"}],
                    id="view-image-context-1",
                    additional_kwargs={"message_type": "view_image_context", "internal": True},
                ),
            ],
        },
        runtime=None,
    )

    assert prepared is not None
    query, *_rest, source_message_key = prepared
    assert query == "分析视频中的事件"
    assert source_message_key == "id:user-1"


def test_intent_recognition_marks_clarification_answer_hidden_steps_suppressed(monkeypatch):
    middleware = IntentRecognitionMiddleware(model_name="unused")
    monkeypatch.setattr(intent_module, "create_chat_model", lambda **_kwargs: None)

    update = middleware.before_agent(
        {
            "messages": [HumanMessage(content="1", id="user-2")],
            "pending_action": {
                "id": "clarify_1",
                "type": "clarification",
                "question": "确认分析这个视频吗？",
                "expected_answer_type": "single_choice",
                "options": [{"id": "1", "label": "是", "value": "是", "index": 1}],
                "resume_intent_context": {
                    "intent": "task",
                    "original_query": "分析视频监控的内容",
                    "normalized_query": "分析视频监控的内容",
                    "routing_query": "分析视频监控的内容",
                    "scene": "video_surveillance",
                    "confidence": 1.0,
                },
                "resume_routing_context": {"global_selected_skills": ["analyze-video"]},
            },
        },
        runtime=None,
    )

    assert update is not None
    assert update["dialogue_context"]["act"] == "clarification_answer"
    assert update["intent_context"]["_source_message_key"] == "id:user-2"
    assert update["intent_context"]["_suppress_hidden_steps"] is True


def test_intent_recognition_reuses_previous_intent_for_task_followup(monkeypatch):
    middleware = IntentRecognitionMiddleware(model_name="unused")
    monkeypatch.setattr(intent_module, "create_chat_model", lambda **_kwargs: None)

    previous_intent = {
        "intent": "task",
        "original_query": "请对该影像覆盖范围进行城市绿地生态评估",
        "normalized_query": "请对该影像覆盖范围进行城市绿地生态评估",
        "routing_query": "请对该影像覆盖范围进行城市绿地生态评估",
        "scene": "remote_sensing_image",
        "confidence": 0.95,
    }

    update = middleware.before_agent(
        {
            "messages": [HumanMessage(content="这次为什么不直接使用skill的脚本而是重新写脚本？", id="user-3")],
            "intent_context": previous_intent,
            "routing_context": {"global_selected_skills": ["urban-greenspace-assessment"]},
        },
        runtime=None,
    )

    assert update is not None
    assert update["dialogue_context"]["act"] == "task_followup"
    assert update["intent_context"]["scene"] == "remote_sensing_image"
    assert update["intent_context"]["_source_message_key"] == "id:user-3"
    assert update["intent_context"]["_suppress_hidden_steps"] is True
