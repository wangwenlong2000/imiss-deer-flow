"""SkillRouterMiddleware.

Inserts into the middleware chain between SummarizationMiddleware and
TodoMiddleware.  On each turn it:

1. Reads the user's last message and uploaded file info.
2. Performs a lightweight ``should_route`` check.
3. Segments the query into coarse task segments.
4. Uses scene filtering as the coarse candidate set when configured.
5. Reranks candidates via the Reranker API.
6. Falls back to legacy embedding + ES recall when no scene is available.
7. Resolves final skill selections.
8. Writes ``routing_context`` and ``skills_override`` into state.
"""

from __future__ import annotations

import logging
import json
import re
import time
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

from deerflow.config.skill_router_config import get_skill_router_config
from deerflow.routing.embedding_client import SkillRouterEmbeddingClient
from deerflow.routing.es_store import SkillRouterElasticStore
from deerflow.routing.metrics import (
    record_es_error,
    record_embedding_error,
    record_request,
    record_reranker_error,
    record_skill_hit,
)
from deerflow.routing.query_segmenter import segment_query, should_route
from deerflow.routing.reranker_client import SkillRouterRerankerClient
from deerflow.routing.resolver import resolve
from deerflow.routing.schema import RoutingContext, SceneTask, SelectedSkill
from deerflow.skills.loader import load_custom_skills, load_skills

logger = logging.getLogger(__name__)
_UPLOAD_BLOCK_RE = re.compile(r"<uploaded_files>[\s\S]*?</uploaded_files>\s*", re.IGNORECASE)


class SkillRouterMiddleware(AgentMiddleware[AgentState]):
    """Middleware that routes user queries to the most relevant Skills."""

    state_schema = AgentState

    def __init__(self) -> None:
        super().__init__()
        config = get_skill_router_config()
        self.mode = config.mode
        self.scene_prefilter_enabled = config.scene_prefilter_enabled
        self.fallback_global_when_no_scene = config.fallback_global_when_no_scene
        self.embedding_client = SkillRouterEmbeddingClient(
            base_url=config.embedding.get_base_url(),
            api_key=config.embedding.get_api_key(),
        )
        self.reranker_client = SkillRouterRerankerClient(
            base_url=config.reranker.get_base_url(),
            api_key=config.reranker.get_api_key(),
        )
        self.es_store = SkillRouterElasticStore(
            es_url=config.vector_store.get_es_url(),
            username=config.vector_store.get_es_username(),
            password=config.vector_store.get_es_password(),
            index=config.vector_store.get_es_index(),
        )
        self.top_k = config.vector_store.top_k
        self.max_public_skills = 2

    @override
    def before_agent(self, state: AgentState, runtime: Runtime) -> dict[str, Any] | None:
        """Execute routing pipeline before the agent runs."""
        start = time.monotonic()
        messages = state.get("messages") or []
        if not messages:
            return None

        # Compute base_scope from frontend input
        from deerflow.routing.scope_resolver import SkillScopeResolver

        frontend_ids = state.get("frontend_enabled_skill_ids")
        base_scope_ids, scope_mode = SkillScopeResolver.resolve_base_scope(frontend_enabled_skill_ids=frontend_ids)

        # Remove previous-turn routed prompts/guidance explicitly. LangGraph's
        # message reducer appends/merges by id; omitting an old message from a
        # returned list is not enough to delete it from state.
        cleaned_messages, removal_messages = self._clean_previous_router_messages(messages)

        # Find the last user message in the cleaned list
        last_user_msg = None
        for msg in reversed(cleaned_messages):
            if isinstance(msg, HumanMessage) and not self._is_internal_human_message(msg):
                last_user_msg = msg
                break

        if last_user_msg is None:
            return None

        query = self._extract_text(last_user_msg)
        if not query or not query.strip():
            return None
        source_message_key = self._source_message_key(last_user_msg, query)
        previous_routing_context = state.get("routing_context")
        if self._same_source(previous_routing_context, source_message_key):
            logger.debug("SkillRouter: reuse existing routing_context for source=%s", source_message_key)
            return None

        intent_context = state.get("intent_context") or {}
        if not isinstance(intent_context, dict):
            intent_context = {}
        routing_query = intent_context.get("routing_query")
        if not isinstance(routing_query, str) or not routing_query.strip():
            routing_query = query
        intent_scene = intent_context.get("scene")
        if not isinstance(intent_scene, str) or not intent_scene.strip():
            intent_scene = None
        intent_scenes = intent_context.get("scenes") or []
        if isinstance(intent_scenes, list):
            intent_scenes = [scene for scene in intent_scenes if isinstance(scene, str) and scene.strip()]
        else:
            intent_scenes = []
        if not intent_scene and intent_scenes:
            intent_scene = intent_scenes[0]
        intent_scene_tasks = intent_context.get("scene_tasks") or []
        if not isinstance(intent_scene_tasks, list):
            intent_scene_tasks = []

        uploaded_files = state.get("uploaded_files") or []
        base_scope_set = set(base_scope_ids)
        default_public_skill_ids = self._default_public_skill_ids(base_scope_set)

        dialogue_context = state.get("dialogue_context") or {}
        if isinstance(dialogue_context, dict) and dialogue_context.get("act") in {"clarification_answer", "parameter_update", "task_followup"}:
            resumed_routing = dialogue_context.get("resume_routing_context")
            if isinstance(resumed_routing, dict):
                try:
                    routing_ctx = RoutingContext.model_validate(resumed_routing)
                    final_skill_ids = SkillScopeResolver.resolve_final_scope(
                        skill_router_enabled=True,
                        base_scope_ids=base_scope_ids,
                        routed_skill_ids=routing_ctx.global_selected_skills,
                    )
                    routing_ctx.global_selected_skills = self._dedupe_skill_ids(final_skill_ids)
                    if not routing_ctx.default_public_skill_ids:
                        routing_ctx.default_public_skill_ids = default_public_skill_ids
                    routing_ctx.route_reason = "Reused previous route for follow-up turn"
                    skills_override_msg = self._build_skills_override(routing_ctx)
                    dialogue_msg = self._build_dialogue_action_prompt(dialogue_context)
                    routing_data = routing_ctx.model_dump()
                    routing_data["_source_message_key"] = source_message_key
                    routing_data["_suppress_hidden_steps"] = True
                    elapsed = (time.monotonic() - start) * 1000
                    record_request(trigger=True, latency_ms=elapsed)
                    return {
                        "routing_context": routing_data,
                        "frontend_enabled_skill_ids": frontend_ids,
                        "frontend_scope_mode": scope_mode,
                        "base_scope_skill_ids": base_scope_ids,
                        "final_scope_skill_ids": routing_ctx.global_selected_skills,
                        "allowed_tool_names": routing_ctx.global_allowed_tools,
                        "messages": removal_messages + cleaned_messages + [
                            SystemMessage(content=skills_override_msg, additional_kwargs={"message_type": "routed_skill_prompt"}),
                            dialogue_msg,
                        ],
                    }
                except Exception:
                    logger.exception("Failed to reuse routing context for clarification answer")

        # When frontend explicitly disabled all skills, there's no valid routing scope
        if frontend_ids is not None and len(base_scope_ids) == 0:
            elapsed = (time.monotonic() - start) * 1000
            record_request(trigger=False, latency_ms=elapsed)
            logger.debug("SkillRouter: empty base_scope (all skills disabled)")
            return {
                "routing_context": {"trigger": False, "_source_message_key": source_message_key},
                "frontend_enabled_skill_ids": frontend_ids,
                "frontend_scope_mode": scope_mode,
                "base_scope_skill_ids": [],
                "final_scope_skill_ids": [],
                "allowed_tool_names": [],
                "messages": removal_messages + cleaned_messages + [self._build_no_skill_prompt(reason="All skills explicitly disabled by user")],
            }

        # L0: should_route check
        if not should_route(routing_query, uploaded_files):
            elapsed = (time.monotonic() - start) * 1000
            record_request(trigger=False, latency_ms=elapsed)
            logger.debug("should_route=False query=%r routing_query=%r", query[:80], routing_query[:120])
            return {
                "routing_context": {"trigger": False, "_source_message_key": source_message_key},
                "frontend_enabled_skill_ids": frontend_ids,
                "frontend_scope_mode": scope_mode,
                "base_scope_skill_ids": base_scope_ids,
                "messages": removal_messages + cleaned_messages + [self._build_no_skill_prompt(reason="Query does not match any skill scope")],
            }

        # L1: task segmentation
        segments = self._build_intent_segments(intent_scene_tasks, routing_query, intent_scene, intent_scenes)
        if not segments:
            segments = segment_query(routing_query, scene_hint=intent_scene, scene_hints=intent_scenes)
        if not segments:
            elapsed = (time.monotonic() - start) * 1000
            record_request(trigger=False, latency_ms=elapsed)
            return {
                "routing_context": {"trigger": False, "_source_message_key": source_message_key},
                "frontend_enabled_skill_ids": frontend_ids,
                "frontend_scope_mode": scope_mode,
                "base_scope_skill_ids": base_scope_ids,
                "messages": removal_messages + cleaned_messages + [self._build_no_skill_prompt(reason="Query cannot be segmented into skill-scoped tasks")],
            }

        # Process each segment
        scene_tasks: list[SceneTask] = []
        all_selected: dict[str, SelectedSkill] = {}  # skill_id -> SelectedSkill
        all_input_refs: list[str] = []
        all_allowed_tools: set[str] = set()

        for seg in segments:
            seg_text = seg["text"]
            seg_scene = seg.get("scene") or intent_scene
            candidates = self._candidate_skills_for_segment(
                segment_text=seg_text,
                scene=seg_scene,
                base_scope_set=base_scope_set,
            )

            if not candidates:
                continue

            # L4: Reranker — pass full context (scenes, task_types, routing_text, etc.)
            reranker_input = []
            for c in candidates:
                reranker_input.append({
                    "skill_id": c.get("skill_id", ""),
                    "name": c.get("name", ""),
                    "description": c.get("description", ""),
                    "routing_text": c.get("routing_text", ""),
                    "body": c.get("body", ""),
                    "scenes": c.get("scenes", []),
                    "task_types": c.get("task_types", []),
                    "input_types": c.get("input_types", []),
                    "output_types": c.get("output_types", []),
                    "is_public": c.get("is_public", False),
                    "positive_triggers": c.get("positive_triggers", []),
                    "negative_triggers": c.get("negative_triggers", []),
                    "keywords": c.get("keywords", []),
                    "anti_keywords": c.get("anti_keywords", []),
                    "prefer_when": c.get("prefer_when", []),
                    "defer_when": c.get("defer_when", []),
                    "can_compose_with": c.get("can_compose_with", []),
                })

            try:
                reranked = self.reranker_client.rerank(query=seg_text, candidates=reranker_input)
            except Exception:
                record_reranker_error()
                logger.exception("Reranker API failed for segment: %s", seg_text[:80])
                reranked = []

            # L5: resolve with scene constraint
            resolved = resolve(query=seg_text, reranked=reranked, scene=seg_scene)

            # Build scene task
            input_refs = seg.get("input_refs", []) or []
            all_input_refs.extend(input_refs)

            selected_skills_list: list[SelectedSkill] = []
            for r in resolved:
                skill_id = r["id"]
                ss = SelectedSkill(id=skill_id, role=r["role"], score=r["score"])
                all_selected[skill_id] = ss
                selected_skills_list.append(ss)
                record_skill_hit(skill_id)
                # Collect allowed tools from candidates
                self._collect_allowed_tools(candidates, skill_id, all_allowed_tools)

            # Collect task_types only from selected skills' candidates
            task_types = self._collect_task_types(candidates, selected_skills_list)

            scene_tasks.append(SceneTask(
                scene_task_id=f"task_{len(scene_tasks)+1:03d}",
                segment_id=seg["segment_id"],
                segment_text=seg_text,
                scene=seg_scene,
                input_refs=input_refs,
                task_types=task_types,
                selected_skills=selected_skills_list,
                expected_outputs=self._collect_output_types(candidates, selected_skills_list),
                depends_on=[],
            ))

        if not scene_tasks:
            if default_public_skill_ids:
                routing_ctx = RoutingContext(
                    route_mode=self._route_mode_name(len(segments)),
                    trigger=True,
                    primary_goal=routing_query,
                    scene_tasks=[],
                    global_selected_skills=[],
                    default_public_skill_ids=default_public_skill_ids,
                    global_allowed_tools=[],
                    confidence=0.0,
                    route_reason="No domain-specific skills matched; default public skills are available",
                )
                skills_override_msg = self._build_skills_override(routing_ctx)
                routing_data = routing_ctx.model_dump()
                routing_data["_source_message_key"] = source_message_key
                elapsed = (time.monotonic() - start) * 1000
                record_request(trigger=True, latency_ms=elapsed)
                final_skill_ids = []
                return {
                    "routing_context": routing_data,
                    "frontend_enabled_skill_ids": frontend_ids,
                    "base_scope_skill_ids": base_scope_ids,
                    "final_scope_skill_ids": final_skill_ids,
                    "allowed_tool_names": [],
                    "messages": removal_messages + cleaned_messages + [
                        SystemMessage(content=skills_override_msg, additional_kwargs={"message_type": "routed_skill_prompt"})
                    ],
                }

            elapsed = (time.monotonic() - start) * 1000
            record_request(trigger=False, latency_ms=elapsed)
            return {
                "routing_context": {"trigger": False, "_source_message_key": source_message_key},
                "frontend_enabled_skill_ids": frontend_ids,
                "frontend_scope_mode": scope_mode,
                "base_scope_skill_ids": base_scope_ids,
                "messages": removal_messages + cleaned_messages + [self._build_no_skill_prompt(reason="No skill candidates found for query segments")],
            }

        # L6: build routing_context with final_scope filtering
        global_skills = list(all_selected.keys())
        final_skill_ids = SkillScopeResolver.resolve_final_scope(
            skill_router_enabled=True,
            base_scope_ids=base_scope_ids,
            routed_skill_ids=global_skills,
        )
        injected_skill_ids = self._dedupe_skill_ids(final_skill_ids)
        confidence = self._compute_confidence(all_selected)

        # allowed_tools collected during resolution.
        # Phase 2b will add per-tool hard enforcement at the execution layer.
        final_allowed_tools = sorted(all_allowed_tools)

        routing_ctx = RoutingContext(
            route_mode=self._route_mode_name(len(scene_tasks)),
            trigger=True,
            primary_goal=self._infer_primary_goal(scene_tasks),
            scene_tasks=scene_tasks,
            global_selected_skills=final_skill_ids,
            default_public_skill_ids=default_public_skill_ids,
            global_allowed_tools=final_allowed_tools,
            confidence=confidence,
            route_reason=f"Matched {len(scene_tasks)} task segment(s) from user query",
        )

        # L7: build skills_override system message
        skills_override_msg = self._build_skills_override(routing_ctx)

        elapsed = (time.monotonic() - start) * 1000
        record_request(trigger=True, latency_ms=elapsed)
        logger.info(
            "SkillRouter: query=%r routing_query=%r intent_scene=%r trigger=%s mode=%s skills=%s latency_ms=%d",
            query[:80], routing_query[:120], intent_scene, routing_ctx.trigger, routing_ctx.route_mode,
            injected_skill_ids, round(elapsed),
        )
        logger.info(
            "SkillRouter scope: frontend=%r base_scope=%d routed=%d final=%d allowed_tools=%d trigger=%s",
            frontend_ids, len(base_scope_ids), len(global_skills),
            len(injected_skill_ids), len(final_allowed_tools),
            routing_ctx.trigger,
        )

        new_routed_skill_msg = SystemMessage(content=skills_override_msg, additional_kwargs={"message_type": "routed_skill_prompt"})
        routing_data = routing_ctx.model_dump()
        routing_data["_source_message_key"] = source_message_key

        return {
            "routing_context": routing_data,
            "frontend_enabled_skill_ids": frontend_ids,
            "base_scope_skill_ids": base_scope_ids,
            "final_scope_skill_ids": injected_skill_ids,
            "allowed_tool_names": final_allowed_tools,
            "messages": removal_messages + cleaned_messages + [new_routed_skill_msg],
        }

    @override
    async def abefore_agent(self, state: AgentState, runtime: Runtime) -> dict[str, Any] | None:
        return self.before_agent(state, runtime)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _clean_previous_router_messages(messages: list[Any]) -> tuple[list[Any], list[RemoveMessage]]:
        cleaned_messages: list[Any] = []
        removal_messages: list[RemoveMessage] = []
        for msg in messages:
            is_previous_router_message = (
                (
                    isinstance(msg, SystemMessage)
                    and msg.additional_kwargs.get("message_type") in {"routed_skill_prompt", "dialogue_action_context"}
                )
                or (
                    isinstance(msg, HumanMessage)
                    and getattr(msg, "name", None) == "todo_routing_guidance"
                )
            )
            if is_previous_router_message:
                msg_id = getattr(msg, "id", None)
                if msg_id:
                    removal_messages.append(RemoveMessage(id=msg_id))
                continue
            cleaned_messages.append(msg)
        return cleaned_messages, removal_messages

    @staticmethod
    def _build_intent_segments(
        intent_scene_tasks: list[dict[str, Any]],
        routing_query: str,
        intent_scene: str | None,
        intent_scenes: list[str],
    ) -> list[dict[str, Any]]:
        if not intent_scene_tasks:
            return []

        segments: list[dict[str, Any]] = []
        for item in intent_scene_tasks:
            if not isinstance(item, dict):
                continue
            scene = item.get("scene")
            if not isinstance(scene, str) or not scene.strip():
                continue
            text = item.get("task_text")
            if not isinstance(text, str) or not text.strip():
                text = item.get("text") if isinstance(item.get("text"), str) else routing_query
            segments.append({
                "segment_id": item.get("sub_task_id") or item.get("scene_task_id") or f"seg_{len(segments)+1:03d}",
                "text": text.strip(),
                "scene": scene.strip(),
                "input_refs": item.get("input_refs", []) if isinstance(item.get("input_refs"), list) else [],
            })

        if segments:
            return segments

        if intent_scene and intent_scenes:
            return [{
                "segment_id": "seg_000",
                "text": routing_query,
                "scene": intent_scene,
                "input_refs": [],
            }]
        return []

    @staticmethod
    def _extract_text(message: HumanMessage) -> str:
        """Extract plain text from a HumanMessage."""
        content = message.content
        if isinstance(content, str):
            return _UPLOAD_BLOCK_RE.sub("", content).strip()
        if isinstance(content, list):
            parts = []
            for block in content:
                if isinstance(block, dict) and block.get("type") == "text":
                    parts.append(block.get("text", ""))
                else:
                    t = getattr(block, "text", None)
                    if isinstance(t, str):
                        parts.append(t)
            return _UPLOAD_BLOCK_RE.sub("", "\n".join(parts)).strip()
        return _UPLOAD_BLOCK_RE.sub("", str(content)).strip() if content else ""

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
    def _source_message_key(message: HumanMessage, query: str) -> str:
        message_id = getattr(message, "id", None)
        if message_id:
            return f"id:{message_id}"
        return f"text:{query.strip()}"

    @staticmethod
    def _same_source(previous_context: Any, source_message_key: str) -> bool:
        return isinstance(previous_context, dict) and previous_context.get("_source_message_key") == source_message_key

    @staticmethod
    def _build_embedding_query(segment_text: str, scene: str | None) -> str:
        """Build a scene-aware query for embedding retrieval."""
        text = segment_text.strip()
        if not scene:
            return text
        return f"Scene: {scene.strip()}\nTask: {text}"

    def _route_mode_name(self, task_count: int) -> str:
        if self.mode == "scene_rerank" and self.scene_prefilter_enabled:
            return "scene_rerank_multi" if task_count > 1 else "scene_rerank"
        return "multi_segment" if task_count > 1 else "single_segment"

    def _candidate_skills_for_segment(
        self,
        *,
        segment_text: str,
        scene: str | None,
        base_scope_set: set[str],
    ) -> list[dict[str, Any]]:
        if self.mode == "scene_rerank" and self.scene_prefilter_enabled:
            scenes = [scene] if scene else []
            if scenes:
                candidates = self._scene_candidates(scenes)
                if base_scope_set:
                    candidates = [c for c in candidates if c.get("skill_id") in base_scope_set]
                return candidates
            if not self.fallback_global_when_no_scene:
                return []

        embedding_query = self._build_embedding_query(segment_text, scene)
        try:
            query_vec = self.embedding_client.embed_text(embedding_query)
        except Exception:
            record_embedding_error()
            logger.exception("Embedding API failed for segment: %s", segment_text[:80])
            return []

        filters = {"enabled": True, "is_public": False}
        try:
            candidates = self._search_candidates_for_segment(query_vec, scene, filters)
        except Exception:
            record_es_error()
            logger.exception("ES search failed for segment: %s", segment_text[:80])
            return []

        if base_scope_set:
            candidates = [c for c in candidates if c.get("skill_id") in base_scope_set]
        return candidates

    def _scene_candidates(self, scenes: list[str]) -> list[dict[str, Any]]:
        scene_set = {scene.strip() for scene in scenes if isinstance(scene, str) and scene.strip()}
        if not scene_set:
            return []

        candidates: list[dict[str, Any]] = []
        for skill in load_custom_skills(enabled_only=True):
            card = self._load_router_card(skill.skill_dir)
            skill_scenes = self._card_scenes(card)
            if not (skill_scenes & scene_set):
                continue
            candidates.append(self._candidate_from_skill_card(skill, card))
        return candidates

    @staticmethod
    def _load_router_card(skill_dir: Path) -> dict[str, Any]:
        card_path = skill_dir / "router_card.json"
        if not card_path.exists():
            return {}
        try:
            card = json.loads(card_path.read_text(encoding="utf-8"))
        except Exception:
            logger.exception("Failed to read router card: %s", card_path)
            return {}
        return card if isinstance(card, dict) else {}

    @staticmethod
    def _card_scenes(card: dict[str, Any]) -> set[str]:
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

    @staticmethod
    def _candidate_from_skill_card(skill: Any, card: dict[str, Any]) -> dict[str, Any]:
        identity = card.get("identity") if isinstance(card.get("identity"), dict) else {}
        scope = card.get("scope") if isinstance(card.get("scope"), dict) else {}
        routing = card.get("routing") if isinstance(card.get("routing"), dict) else {}
        body = card.get("body") if isinstance(card.get("body"), dict) else {}
        execution = card.get("execution") if isinstance(card.get("execution"), dict) else {}
        routing_policy = card.get("routing_policy") if isinstance(card.get("routing_policy"), dict) else {}
        return {
            "skill_id": identity.get("id") or skill.name,
            "name": identity.get("name") or skill.name,
            "description": identity.get("description") or skill.description,
            "routing_text": routing.get("routing_text", ""),
            "body": body.get("content", ""),
            "scenes": scope.get("scenes", []),
            "task_types": scope.get("task_types", []),
            "input_types": scope.get("input_types", []),
            "output_types": scope.get("output_types", []),
            "is_public": bool(scope.get("is_public", False)),
            "positive_triggers": routing.get("positive_triggers", []),
            "negative_triggers": routing.get("negative_triggers", []),
            "keywords": routing.get("keywords", []),
            "anti_keywords": routing.get("anti_keywords", []),
            "prefer_when": routing_policy.get("prefer_when", []),
            "defer_when": routing_policy.get("defer_when", []),
            "can_compose_with": execution.get("can_compose_with", []),
            "required_tools": execution.get("required_tools", []),
            "optional_tools": execution.get("optional_tools", []),
        }

    def _search_candidates_for_segment(
        self,
        query_vec: list[float],
        scene: str | None,
        filters: dict,
    ) -> list[dict]:
        """Search non-public domain candidates for one segment.

        Public skills are injected separately as default shared capabilities
        and do not participate in SkillRouter ranking.
        """
        custom_filters = dict(filters)
        custom_filters["is_public"] = False
        if scene:
            scene_filters = dict(custom_filters)
            scene_filters["scenes"] = scene
            scene_candidates = self.es_store.search(
                query_vector=query_vec,
                top_k=self._scene_recall_top_k(scene),
                filters=scene_filters,
            )
            return [c for c in scene_candidates if not c.get("is_public", False)]

        candidates = self.es_store.search(
            query_vector=query_vec,
            top_k=self.top_k,
            filters=custom_filters,
        )
        return [c for c in candidates if not c.get("is_public", False)]

    def _scene_recall_top_k(self, scene: str) -> int:
        """Use a larger same-scene recall window for broad skill families."""
        if scene == "policy_regulation":
            return max(self.top_k, 10)
        return max(self.top_k, 12)

    @staticmethod
    def _dedupe_skill_ids(skill_ids: list[str]) -> list[str]:
        seen: set[str] = set()
        deduped: list[str] = []
        for skill_id in skill_ids:
            if not skill_id or skill_id in seen:
                continue
            seen.add(skill_id)
            deduped.append(skill_id)
        return deduped

    @staticmethod
    def _default_public_skill_ids(base_scope_set: set[str]) -> list[str]:
        public_ids: list[str] = []
        for skill in load_skills(enabled_only=True):
            if skill.category != "public":
                continue
            if base_scope_set and skill.name not in base_scope_set:
                continue
            public_ids.append(skill.name)
        return sorted(public_ids)

    @staticmethod
    def _collect_task_types(candidates: list[dict], selected: list[SelectedSkill]) -> list[str]:
        """Collect unique task_types only from candidates matching selected skills."""
        selected_ids = {s.id for s in selected}
        types: list[str] = []
        for c in candidates:
            if c.get("skill_id") not in selected_ids:
                continue
            for tt in c.get("task_types", []):
                if tt not in types:
                    types.append(tt)
        return types

    @staticmethod
    def _collect_allowed_tools(candidates: list[dict], skill_id: str, out: set[str]) -> None:
        """Accumulate allowed_tools for *skill_id* from candidates."""
        for c in candidates:
            if c.get("skill_id") == skill_id:
                for tool in c.get("required_tools", []) + c.get("optional_tools", []):
                    out.add(tool)

    @staticmethod
    def _collect_output_types(candidates: list[dict], selected: list[SelectedSkill]) -> list[str]:
        types: list[str] = []
        for c in candidates:
            if c.get("skill_id") in {s.id for s in selected}:
                for ot in c.get("output_types", []):
                    if ot not in types:
                        types.append(ot)
        return types

    @staticmethod
    def _compute_confidence(selected: dict[str, SelectedSkill]) -> float:
        if not selected:
            return 0.0
        scores = [s.score for s in selected.values()]
        return round(sum(scores) / len(scores), 2)

    @staticmethod
    def _infer_primary_goal(scene_tasks: list[SceneTask]) -> str:
        if len(scene_tasks) == 1:
            return scene_tasks[0].segment_text or ""
        segments = [st.segment_text for st in scene_tasks if st.segment_text]
        return " + ".join(segments) if segments else "Unknown"

    def _build_skills_override(self, ctx: RoutingContext) -> str:
        """Build a current-turn authoritative <skill_system> block with routed skill details.

        This message only overrides the available skill list for the current turn.
        It does NOT replace the base system prompt.  All base rules such as
        language_policy, clarification_system, working_directory, response_style,
        and critical_reminders remain active.
        """
        from deerflow.agents.lead_agent.prompt import _render_skill_system_section

        all_skills = load_skills(enabled_only=True)
        # Build dual-key lookup: index by both name (frontmatter) and skill_path (directory)
        # ES skill_id typically matches skill_path (e.g., "network-traffic-analysis")
        skill_map: dict[str, Any] = {}
        for s in all_skills:
            skill_map[s.name] = s
            if s.skill_path:
                skill_map[s.skill_path] = s

        try:
            from deerflow.config import get_app_config

            container_base_path = get_app_config().skills.container_path
        except Exception:
            container_base_path = "/mnt/skills"

        injected_skill_ids = self._dedupe_skill_ids(
            ctx.global_selected_skills
        )

        # Build <available_skills> XML for routed custom/domain skills only.
        # Public skills are already present in the stable base system prompt.
        skill_items = ""
        for idx, skill_id in enumerate(injected_skill_ids, 1):
            skill = skill_map.get(skill_id)
            if skill:
                location = skill.get_container_file_path(container_base_path)
                skill_items += (
                    f"    <skill>\n"
                    f"      <name>{skill.name}</name>\n"
                    f"      <description>{skill.description}</description>\n"
                    f"      <location>{location}</location>\n"
                    f"    </skill>\n"
                )

        skills_list = f"<available_skills>\n{skill_items}</available_skills>"

        # Append task package info when present
        task_pkg_text = ""
        if ctx.scene_tasks:
            task_pkg_lines = [
                "",
                "任务包：",
            ]
            for st in ctx.scene_tasks:
                task_pkg_lines.append(f"- {st.scene_task_id}：{st.segment_text}")
            task_pkg_text = "\n".join(task_pkg_lines) + "\n"

        # Build the routed <skill_system> using the shared renderer
        base_system = _render_skill_system_section(
            skills_list=skills_list,
            container_base_path=container_base_path,
            empty_available_skills=(len(injected_skill_ids) == 0),
            routed_mode=True,
        )

        # Insert task package text before the closing tag
        if task_pkg_text:
            base_system = base_system.replace("\n</skill_system>", task_pkg_text + "\n</skill_system>")

        return base_system

    @staticmethod
    def _build_dialogue_action_prompt(dialogue_context: dict[str, Any]) -> SystemMessage:
        pending = {}
        metadata = dialogue_context.get("metadata")
        if isinstance(metadata, dict):
            pending = metadata.get("pending_action") if isinstance(metadata.get("pending_action"), dict) else {}
            parameter_updates = metadata.get("parameter_updates") if isinstance(metadata.get("parameter_updates"), dict) else {}
            raw_answer = metadata.get("raw_answer")
        else:
            parameter_updates = {}
            raw_answer = None

        parsed_answer = dialogue_context.get("parsed_answer")
        lines = [
            "<dialogue_action_context>",
            "The current user turn continues, asks about, or updates a previous task. Continue the previous routed task; do not reinterpret this reply as a new business scene.",
            f"act: {dialogue_context.get('act')}",
            f"pending_action_id: {dialogue_context.get('pending_action_id')}",
        ]
        if pending.get("question"):
            lines.append(f"pending_question: {pending.get('question')}")
        if raw_answer:
            lines.append(f"raw_user_answer: {raw_answer}")
        if parsed_answer is not None:
            lines.append(f"parsed_answer: {parsed_answer}")
        if parameter_updates:
            lines.append(f"parameter_updates: {parameter_updates}")
            lines.append("Apply parameter_updates when constructing tool/script arguments unless they conflict with explicit safety or data constraints.")
        resume_plan = metadata.get("resume_plan") if isinstance(metadata, dict) else None
        if isinstance(resume_plan, dict):
            lines.append(f"resume_plan: {resume_plan}")
        conflicts = metadata.get("conflicts") if isinstance(metadata, dict) else None
        if conflicts:
            lines.append(f"conflicts: {conflicts}")
            lines.append("Resolve conflicts explicitly before running tools.")
        lines.append("</dialogue_action_context>")
        return SystemMessage(
            content="\n".join(lines),
            additional_kwargs={"message_type": "dialogue_action_context"},
        )

    def _build_no_skill_prompt(self, *, reason: str) -> SystemMessage:
        """Build a current-turn authoritative empty <skill_system> message.

        Used when SkillRouter is enabled but no skills are available for this
        turn (query is a greeting, no segmentation matches, or no candidates
        found).  Makes the skill state explicit instead of silent.
        """
        from deerflow.agents.lead_agent.prompt import _render_skill_system_section

        try:
            from deerflow.config import get_app_config

            container_base_path = get_app_config().skills.container_path
        except Exception:
            container_base_path = "/mnt/skills"

        skills_list = "<available_skills>\n</available_skills>"

        content = _render_skill_system_section(
            skills_list=skills_list,
            container_base_path=container_base_path,
            empty_available_skills=True,
            routed_mode=True,
        )
        # Inject the reason for this turn having no skills
        reason_tag = f"\nRouting result: no matched skills. Reason: {reason}\n"
        content = content.replace(
            "\n**Current Available Skills:**",
            reason_tag + "\n**Current Available Skills:**",
        )

        return SystemMessage(content=content, additional_kwargs={"message_type": "routed_skill_prompt"})

    def _build_fallback_skill_prompt(self, *, fallback_skill_ids: list[str], reason: str) -> SystemMessage:
        """Build a current-turn fallback <skill_system> with a reduced skill set.

        Used when SkillRouter has no confident match but still wants to provide
        a small set of base skills for this turn.
        """
        from deerflow.agents.lead_agent.prompt import _render_skill_system_section

        all_skills = load_skills(enabled_only=True)
        skill_map: dict[str, Any] = {}
        for s in all_skills:
            skill_map[s.name] = s
            if s.skill_path:
                skill_map[s.skill_path] = s

        try:
            from deerflow.config import get_app_config

            container_base_path = get_app_config().skills.container_path
        except Exception:
            container_base_path = "/mnt/skills"

        skill_items = ""
        for skill_id in fallback_skill_ids:
            skill = skill_map.get(skill_id)
            if skill:
                location = skill.get_container_file_path(container_base_path)
                skill_items += (
                    f"    <skill>\n"
                    f"      <name>{skill.name}</name>\n"
                    f"      <description>{skill.description}</description>\n"
                    f"      <location>{location}</location>\n"
                    f"    </skill>\n"
                )

        skills_list = f"<available_skills>\n{skill_items}</available_skills>"

        content = _render_skill_system_section(
            skills_list=skills_list,
            container_base_path=container_base_path,
            empty_available_skills=(len(fallback_skill_ids) == 0),
            routed_mode=True,
        )
        reason_tag = f"\nRouting result: no confident match. Fallback mode: {reason}\n"
        content = content.replace(
            "\n**Current Available Skills:**",
            reason_tag + "\n**Current Available Skills:**",
        )

        return SystemMessage(content=content, additional_kwargs={"message_type": "routed_skill_prompt"})
