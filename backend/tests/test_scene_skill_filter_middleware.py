from pathlib import Path

from langchain_core.messages import HumanMessage, SystemMessage

from deerflow.agents.middlewares import scene_skill_filter_middleware as scene_filter_module
from deerflow.agents.middlewares.scene_skill_filter_middleware import SceneSkillFilterMiddleware
from deerflow.skills.types import Skill


def _skill(tmp_path: Path, name: str, scenes: list[str]) -> Skill:
    skill_dir = tmp_path / name
    skill_dir.mkdir()
    (skill_dir / "router_card.json").write_text(
        '{"scope":{"scenes":%s}}' % str(scenes).replace("'", '"'),
        encoding="utf-8",
    )
    return Skill(
        name=name,
        description=f"{name} desc",
        license=None,
        skill_dir=skill_dir,
        skill_file=skill_dir / "SKILL.md",
        relative_path=Path(name),
        category="custom",
        enabled=True,
    )


def test_scene_skill_filter_injects_only_detected_scene_custom_skills(monkeypatch, tmp_path):
    video_skill = _skill(tmp_path, "analyze-video", ["video_surveillance"])
    network_skill = _skill(tmp_path, "network-traffic-analysis", ["network_traffic"])

    monkeypatch.setattr(scene_filter_module, "load_custom_skills", lambda enabled_only=True: [video_skill, network_skill])

    middleware = SceneSkillFilterMiddleware()
    result = middleware.before_agent(
        {
            "messages": [HumanMessage(content="分析视频")],
            "intent_context": {"scene": "video_surveillance", "scenes": ["video_surveillance"]},
        },
        runtime=None,
    )

    assert result is not None
    assert result["final_scope_skill_ids"] == ["analyze-video"]
    assert result["routing_context"]["global_selected_skills"] == ["analyze-video"]
    routed_prompt = result["messages"][-1]
    assert isinstance(routed_prompt, HumanMessage)
    assert routed_prompt.name == "routing_skill_guidance"
    assert routed_prompt.additional_kwargs["message_type"] == "routed_skill_prompt"
    assert routed_prompt.additional_kwargs["internal"] is True
    prompt = routed_prompt.content
    assert "analyze-video" in prompt
    assert "network-traffic-analysis" not in prompt


def test_scene_skill_filter_removes_previous_routed_prompt(monkeypatch, tmp_path):
    video_skill = _skill(tmp_path, "analyze-video", ["video_surveillance"])
    monkeypatch.setattr(scene_filter_module, "load_custom_skills", lambda enabled_only=True: [video_skill])

    old = SystemMessage(
        content="old routed prompt",
        id="old-router-message",
        additional_kwargs={"message_type": "routed_skill_prompt"},
    )

    middleware = SceneSkillFilterMiddleware()
    result = middleware.before_agent(
        {
            "messages": [old, HumanMessage(content="分析视频")],
            "intent_context": {"scene": "video_surveillance"},
        },
        runtime=None,
    )

    assert result is not None
    assert result["messages"][0].id == "old-router-message"
    assert "analyze-video" in result["messages"][-1].content


def test_scene_skill_filter_removes_previous_internal_human_prompt(monkeypatch, tmp_path):
    video_skill = _skill(tmp_path, "analyze-video", ["video_surveillance"])
    monkeypatch.setattr(scene_filter_module, "load_custom_skills", lambda enabled_only=True: [video_skill])

    old = HumanMessage(
        content="old routed prompt",
        name="routing_skill_guidance",
        id="old-human-router-message",
        additional_kwargs={"message_type": "routed_skill_prompt", "internal": True},
    )

    middleware = SceneSkillFilterMiddleware()
    result = middleware.before_agent(
        {
            "messages": [old, HumanMessage(content="分析视频")],
            "intent_context": {"scene": "video_surveillance"},
        },
        runtime=None,
    )

    assert result is not None
    assert result["messages"][0].id == "old-human-router-message"
    assert result["messages"][-1].id != "old-human-router-message"
    assert "analyze-video" in result["messages"][-1].content


def test_scene_skill_filter_skips_internal_view_image_context_for_same_user_message(monkeypatch, tmp_path):
    video_skill = _skill(tmp_path, "analyze-video", ["video_surveillance"])
    monkeypatch.setattr(scene_filter_module, "load_custom_skills", lambda enabled_only=True: [video_skill])

    middleware = SceneSkillFilterMiddleware()
    result = middleware.before_agent(
        {
            "messages": [
                HumanMessage(content="分析视频中的事件", id="user-1"),
                HumanMessage(
                    content=[{"type": "text", "text": "Here are the images you've viewed:"}],
                    additional_kwargs={"message_type": "view_image_context", "internal": True},
                ),
            ],
            "intent_context": {"scene": "video_surveillance"},
            "routing_context": {"global_selected_skills": ["analyze-video"], "_source_message_key": "id:user-1"},
        },
        runtime=None,
    )

    assert result is None
