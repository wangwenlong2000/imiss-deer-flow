from __future__ import annotations

import importlib.util
import typing

from langchain_core.messages import HumanMessage, SystemMessage

if not hasattr(typing, "Self"):
    typing.Self = typing.Any

_skill_router_spec = importlib.util.spec_from_file_location(
    "_skill_router_middleware_under_test",
    "packages/harness/deerflow/agents/middlewares/skill_router_middleware.py",
)
_skill_router_module = importlib.util.module_from_spec(_skill_router_spec)
assert _skill_router_spec.loader is not None
_skill_router_spec.loader.exec_module(_skill_router_module)
SkillRouterMiddleware = _skill_router_module.SkillRouterMiddleware

_todo_spec = importlib.util.spec_from_file_location(
    "_todo_middleware_under_test",
    "packages/harness/deerflow/agents/middlewares/todo_middleware.py",
)
_todo_module = importlib.util.module_from_spec(_todo_spec)
assert _todo_spec.loader is not None
_todo_spec.loader.exec_module(_todo_module)
TodoMiddleware = _todo_module.TodoMiddleware
RoutingHiddenStepMiddleware = _todo_module.RoutingHiddenStepMiddleware


def test_old_hidden_guidance_does_not_block_current_turn_guidance() -> None:
    middleware = TodoMiddleware()
    state = {
        "messages": [
            HumanMessage(content="上一轮政策法规分析", id="user-1"),
            HumanMessage(name="todo_routing_guidance", content="old guidance", id="hidden-1"),
            HumanMessage(content="分析 Neris 网络流量", id="user-2"),
        ],
        "intent_context": {
            "routing_query": "分析 Neris 网络流量",
            "scene": "network_traffic",
            "scene_name": "网络流量",
            "scenes": ["network_traffic"],
            "scene_tasks": [
                {
                    "scene": "network_traffic",
                    "text": "分析 Neris 网络流量",
                    "params": {},
                }
            ],
        },
        "routing_context": {
            "route_reason": "Matched 1 task segment(s) from user query",
            "primary_goal": "分析 Neris 网络流量",
            "scene_tasks": [
                {
                    "segment_text": "分析 Neris 网络流量",
                    "scene": "network_traffic",
                    "selected_skills": [{"id": "network-traffic-analysis"}],
                }
            ],
            "global_selected_skills": ["network-traffic-analysis"],
        },
    }

    update = middleware.before_model(state, None)  # type: ignore[arg-type]

    assert update is not None
    messages = update["messages"]
    assert len(messages) == 1
    assert messages[0].name == "todo_routing_guidance"
    assert "network_traffic" in messages[0].content
    assert "network-traffic-analysis" in messages[0].content


def test_current_turn_hidden_guidance_is_not_duplicated() -> None:
    middleware = TodoMiddleware()
    state = {
        "messages": [
            HumanMessage(content="分析 Neris 网络流量", id="user-1"),
            HumanMessage(name="todo_routing_guidance", content="current guidance", id="hidden-1"),
        ],
        "intent_context": {
            "routing_query": "分析 Neris 网络流量",
            "scene": "network_traffic",
        },
        "routing_context": {},
    }

    assert middleware.before_model(state, None) is None  # type: ignore[arg-type]


def test_clarification_answer_does_not_emit_repeated_intent_or_routing_hidden_steps() -> None:
    middleware = TodoMiddleware()
    state = {
        "messages": [
            HumanMessage(content="分析视频监控的内容", id="user-1"),
            HumanMessage(content="1", id="user-2"),
        ],
        "dialogue_context": {"act": "clarification_answer"},
        "intent_context": {
            "routing_query": "分析视频监控的内容",
            "scene": "video_surveillance",
            "_source_message_key": "id:user-2",
            "_suppress_hidden_steps": True,
        },
        "routing_context": {
            "route_reason": "Reused previous route for clarification answer",
            "global_selected_skills": ["analyze-video"],
            "_source_message_key": "id:user-2",
            "_suppress_hidden_steps": True,
        },
    }

    assert middleware.before_model(state, None) is None  # type: ignore[arg-type]


def test_scene_filter_mode_does_not_emit_skill_router_hidden_step() -> None:
    middleware = TodoMiddleware()
    state = {
        "messages": [
            HumanMessage(content="分析视频监控的内容", id="user-1"),
        ],
        "intent_context": {
            "routing_query": "分析视频监控的内容",
            "scene": "video_surveillance",
            "scene_name": "视频监控",
            "scenes": ["video_surveillance"],
        },
        "routing_context": {
            "route_mode": "scene_filter",
            "route_reason": "SkillRouter disabled; injected all custom skills for detected intent scene",
            "primary_goal": "分析视频监控的内容",
            "global_selected_skills": ["analyze-video", "privacy-masking"],
        },
    }

    update = middleware.before_model(state, None)  # type: ignore[arg-type]

    assert update is not None
    content = update["messages"][0].content
    assert 'source="intent_recognition"' in content
    assert 'source="skill_router"' not in content
    assert "SkillRouter 路由" not in content


def test_non_plan_routing_hidden_step_middleware_emits_intent_only_for_scene_filter() -> None:
    middleware = RoutingHiddenStepMiddleware()
    state = {
        "messages": [
            HumanMessage(content="分析视频监控的内容", id="user-1"),
        ],
        "intent_context": {
            "routing_query": "场景：视频监控。场景ID：video_surveillance",
            "scene": "video_surveillance",
            "scene_name": "视频监控",
            "scene_mode": "single",
            "scenes": ["video_surveillance"],
        },
        "routing_context": {
            "route_mode": "scene_filter",
            "route_reason": "SkillRouter disabled; injected all custom skills for detected intent scene",
            "primary_goal": "分析视频监控的内容",
            "global_selected_skills": ["analyze-video", "privacy-masking"],
        },
    }

    update = middleware.before_model(state, None)  # type: ignore[arg-type]

    assert update is not None
    content = update["messages"][0].content
    assert update["messages"][0].name == "todo_routing_guidance"
    assert 'source="intent_recognition"' in content
    assert "视频监控" in content
    assert 'source="skill_router"' not in content


def test_skill_router_builds_remove_messages_for_previous_router_guidance() -> None:
    old_skill_prompt = SystemMessage(
        content="old routed skill prompt",
        additional_kwargs={"message_type": "routed_skill_prompt"},
        id="router-1",
    )
    old_hidden_guidance = HumanMessage(
        name="todo_routing_guidance",
        content="old hidden guidance",
        id="hidden-1",
    )
    user_message = HumanMessage(content="分析 Neris 网络流量", id="user-1")

    cleaned, removals = SkillRouterMiddleware._clean_previous_router_messages(
        [old_skill_prompt, old_hidden_guidance, user_message]
    )

    assert cleaned == [user_message]
    assert [removal.id for removal in removals] == ["router-1", "hidden-1"]


def test_skill_router_skips_internal_view_image_context_when_reusing_current_route() -> None:
    middleware = object.__new__(SkillRouterMiddleware)
    user_message = HumanMessage(content="分析视频中的事件", id="user-1")
    image_context = HumanMessage(
        content=[{"type": "text", "text": "Here are the images you've viewed:"}],
        id="view-image-context-1",
        additional_kwargs={"message_type": "view_image_context", "internal": True},
    )

    update = middleware.before_agent(
        {
            "messages": [user_message, image_context],
            "routing_context": {"global_selected_skills": ["analyze-video"], "_source_message_key": "id:user-1"},
            "intent_context": {"routing_query": "分析视频中的事件", "scene": "video_surveillance"},
        },
        None,
    )

    assert update is None
