"""Scene-scoped custom skill injection used when SkillRouter ranking is disabled."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

try:
    from typing import override
except ImportError:
    from typing_extensions import override

from langchain.agents import AgentState
from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import HumanMessage, RemoveMessage, SystemMessage
from langgraph.runtime import Runtime

from deerflow.agents.lead_agent.prompt import _render_skill_system_section
from deerflow.routing.schema import RoutingContext, SceneTask, SelectedSkill
from deerflow.skills.loader import load_custom_skills

logger = logging.getLogger(__name__)


class SceneSkillFilterMiddleware(AgentMiddleware[AgentState]):
    """Inject all enabled custom skills that belong to the detected scene.

    This is a lightweight fallback for ``skill_router.enabled = false``.  It
    does not call ES, embedding, or reranker services. Public skills remain in
    the stable base prompt; this middleware only adds scene-matched custom
    skills for the current turn.
    """

    state_schema = AgentState

    @override
    def before_agent(self, state: AgentState, runtime: Runtime) -> dict[str, Any] | None:
        messages = state.get("messages") or []
        if not messages:
            return None

        cleaned_messages, removal_messages = self._clean_previous_scene_filter_messages(messages)
        query = self._last_user_text(cleaned_messages)
        source_message_key = self._last_external_user_source_key(cleaned_messages, query)
        previous_routing_context = state.get("routing_context")
        if source_message_key and self._same_source(previous_routing_context, source_message_key):
            logger.debug("SceneSkillFilter: reuse existing routing_context for source=%s", source_message_key)
            return None

        intent_context = state.get("intent_context") or {}
        if not isinstance(intent_context, dict):
            intent_context = {}

        scenes = self._intent_scenes(intent_context)
        if not scenes:
            routing_context = {"trigger": False, "route_reason": "No intent scene for scene skill filter"}
            if source_message_key:
                routing_context["_source_message_key"] = source_message_key
            return {
                "routing_context": routing_context,
                "final_scope_skill_ids": [],
                "messages": removal_messages + cleaned_messages + [self._build_scene_skills_prompt([], scenes=[], query=query)],
            }

        selected_skills = self._scene_custom_skills(scenes)
        selected_ids = [skill.name for skill in selected_skills]

        routing_ctx = RoutingContext(
            route_mode="scene_filter",
            trigger=bool(selected_ids),
            primary_goal=query,
            scene_tasks=[
                SceneTask(
                    scene_task_id="task_001",
                    segment_id="seg_001",
                    segment_text=query or "",
                    scene=scenes[0],
                    selected_skills=[
                        SelectedSkill(id=skill_id, role="primary" if index == 0 else "supporting", score=1.0)
                        for index, skill_id in enumerate(selected_ids)
                    ],
                )
            ],
            global_selected_skills=selected_ids,
            default_public_skill_ids=[],
            global_allowed_tools=[],
            confidence=1.0 if selected_ids else 0.0,
            route_reason="SkillRouter disabled; injected all custom skills for detected intent scene",
        )
        routing_data = routing_ctx.model_dump()
        if source_message_key:
            routing_data["_source_message_key"] = source_message_key

        logger.info(
            "SceneSkillFilter: scenes=%s selected_custom_skills=%s",
            scenes,
            selected_ids,
        )

        return {
            "routing_context": routing_data,
            "final_scope_skill_ids": selected_ids,
            "allowed_tool_names": [],
            "messages": removal_messages + cleaned_messages + [self._build_scene_skills_prompt(selected_skills, scenes=scenes, query=query)],
        }

    @override
    async def abefore_agent(self, state: AgentState, runtime: Runtime) -> dict[str, Any] | None:
        return self.before_agent(state, runtime)

    @staticmethod
    def _intent_scenes(intent_context: dict[str, Any]) -> list[str]:
        scenes: list[str] = []
        scene = intent_context.get("scene")
        if isinstance(scene, str) and scene.strip():
            scenes.append(scene.strip())
        raw_scenes = intent_context.get("scenes")
        if isinstance(raw_scenes, list):
            for item in raw_scenes:
                if isinstance(item, str) and item.strip() and item.strip() not in scenes:
                    scenes.append(item.strip())
        return scenes

    @staticmethod
    def _last_user_text(messages: list[Any]) -> str | None:
        for msg in reversed(messages):
            if isinstance(msg, HumanMessage) and not SceneSkillFilterMiddleware._is_internal_human_message(msg):
                content = msg.content
                if isinstance(content, str):
                    return content
                if isinstance(content, list):
                    parts: list[str] = []
                    for part in content:
                        if isinstance(part, dict) and part.get("type") == "text" and isinstance(part.get("text"), str):
                            parts.append(part["text"])
                    return "\n".join(parts)
        return None

    @staticmethod
    def _last_external_user_source_key(messages: list[Any], query: str | None) -> str | None:
        for msg in reversed(messages):
            if isinstance(msg, HumanMessage) and not SceneSkillFilterMiddleware._is_internal_human_message(msg):
                message_id = getattr(msg, "id", None)
                if message_id:
                    return f"id:{message_id}"
                if query and query.strip():
                    return f"text:{query.strip()}"
                return None
        return None

    @staticmethod
    def _is_internal_human_message(message: HumanMessage) -> bool:
        additional_kwargs = getattr(message, "additional_kwargs", {}) or {}
        if isinstance(additional_kwargs, dict):
            if additional_kwargs.get("internal") is True:
                return True
            if additional_kwargs.get("message_type") in {"view_image_context", "routed_skill_prompt"}:
                return True
        name = getattr(message, "name", None)
        return isinstance(name, str) and (
            name.endswith("_guidance")
            or name in {"todo_reminder", "routing_skill_guidance", "todo_routing_guidance"}
        )

    @staticmethod
    def _same_source(previous_context: Any, source_message_key: str) -> bool:
        return isinstance(previous_context, dict) and previous_context.get("_source_message_key") == source_message_key

    @staticmethod
    def _skill_scenes(skill_dir: Path) -> set[str]:
        card_path = skill_dir / "router_card.json"
        if not card_path.exists():
            return set()
        try:
            card = json.loads(card_path.read_text(encoding="utf-8"))
        except Exception:
            logger.exception("Failed to read router card: %s", card_path)
            return set()

        scope = card.get("scope") if isinstance(card, dict) else {}
        if not isinstance(scope, dict):
            return set()

        scenes: set[str] = set()
        for key in ("scenes", "scene"):
            value = scope.get(key)
            if isinstance(value, str) and value.strip():
                scenes.add(value.strip())
            elif isinstance(value, list):
                scenes.update(item.strip() for item in value if isinstance(item, str) and item.strip())
        return scenes

    def _scene_custom_skills(self, scenes: list[str]):
        scene_set = set(scenes)
        matched = []
        for skill in load_custom_skills(enabled_only=True):
            skill_scenes = self._skill_scenes(skill.skill_dir)
            if skill_scenes & scene_set:
                matched.append(skill)
        return matched

    @staticmethod
    def _container_base_path() -> str:
        try:
            from deerflow.config import get_app_config

            return get_app_config().skills.container_path
        except Exception:
            return "/mnt/skills"

    def _build_scene_skills_prompt(self, skills, *, scenes: list[str], query: str | None) -> HumanMessage:
        container_base_path = self._container_base_path()
        skill_items = ""
        for skill in skills:
            skill_items += (
                "    <skill>\n"
                f"      <name>{skill.name}</name>\n"
                f"      <description>{skill.description}</description>\n"
                f"      <location>{skill.get_container_file_path(container_base_path)}</location>\n"
                "    </skill>\n"
            )

        skills_list = f"<available_skills>\n{skill_items}</available_skills>"
        content = _render_skill_system_section(
            skills_list=skills_list,
            container_base_path=container_base_path,
            empty_available_skills=(len(skills) == 0),
            routed_mode=True,
        )
        scene_text = ", ".join(scenes) if scenes else "none"
        note = (
            "\nScene skill filter result: SkillRouter ranking is disabled. "
            f"Detected scene(s): {scene_text}. "
            "The listed custom/domain skills are all enabled skills for the detected scene; "
            "public skills from the base prompt remain available.\n"
        )
        if query:
            note += f"Task: {query}\n"
        content = content.replace("\n**Current Available Skills:**", note + "\n**Current Available Skills:**")
        return HumanMessage(
            name="routing_skill_guidance",
            content=content,
            additional_kwargs={"message_type": "routed_skill_prompt", "internal": True},
        )

    @staticmethod
    def _clean_previous_scene_filter_messages(messages: list[Any]) -> tuple[list[Any], list[RemoveMessage]]:
        cleaned_messages: list[Any] = []
        removal_messages: list[RemoveMessage] = []
        for msg in messages:
            additional_kwargs = getattr(msg, "additional_kwargs", {}) or {}
            message_type = additional_kwargs.get("message_type") if isinstance(additional_kwargs, dict) else None
            is_scene_filter_message = (
                (
                    isinstance(msg, SystemMessage)
                    and message_type == "routed_skill_prompt"
                )
                or (
                    isinstance(msg, HumanMessage)
                    and (
                        message_type == "routed_skill_prompt"
                        or getattr(msg, "name", None) in {"todo_routing_guidance", "routing_skill_guidance"}
                    )
                )
            )
            if is_scene_filter_message:
                msg_id = getattr(msg, "id", None)
                if msg_id:
                    removal_messages.append(RemoveMessage(id=msg_id))
                continue
            cleaned_messages.append(msg)
        return cleaned_messages, removal_messages
