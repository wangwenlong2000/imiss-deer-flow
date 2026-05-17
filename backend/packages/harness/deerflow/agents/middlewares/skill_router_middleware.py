"""SkillRouterMiddleware.

Inserts into the middleware chain between SummarizationMiddleware and
TodoMiddleware.  On each turn it:

1. Reads the user's last message and uploaded file info.
2. Performs a lightweight ``should_route`` check.
3. Segments the query into coarse task segments.
4. Calls the Embedding API for each segment.
5. Searches the SkillRouter ES index for Top-K candidates.
6. Reranks candidates via the Reranker API.
7. Resolves final skill selections.
8. Writes ``routing_context`` and ``skills_override`` into state.
"""

from __future__ import annotations

import logging
import time
from typing import Any

try:
    from typing import override
except ImportError:
    from typing_extensions import override

from langchain.agents import AgentState
from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import HumanMessage, SystemMessage
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
from deerflow.skills.loader import load_skills

logger = logging.getLogger(__name__)


class SkillRouterMiddleware(AgentMiddleware[AgentState]):
    """Middleware that routes user queries to the most relevant Skills."""

    state_schema = AgentState

    def __init__(self) -> None:
        super().__init__()
        config = get_skill_router_config()
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
        self.es_min_score = config.vector_store.min_score
        self.reranker_min_score = config.reranker.get("reranker", {}).get("min_score", 0.65) if isinstance(config.reranker, dict) else 0.65
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

        # Filter old routed_skill_prompt SystemMessages from the message list.
        # The filtered list is returned as the full messages state, which instructs
        # LangGraph's reducer to replace accumulated messages.  This prevents
        # old skill prompts from previous turns from reaching the agent core.
        cleaned_messages = [
            msg for msg in messages
            if not (isinstance(msg, SystemMessage)
                    and msg.additional_kwargs.get("message_type") == "routed_skill_prompt")
        ]

        # Find the last user message in the cleaned list
        last_user_msg = None
        for msg in reversed(cleaned_messages):
            if isinstance(msg, HumanMessage):
                last_user_msg = msg
                break

        if last_user_msg is None:
            return None

        query = self._extract_text(last_user_msg)
        if not query or not query.strip():
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

        uploaded_files = state.get("uploaded_files") or []

        # When frontend explicitly disabled all skills, there's no valid routing scope
        if frontend_ids is not None and len(base_scope_ids) == 0:
            elapsed = (time.monotonic() - start) * 1000
            record_request(trigger=False, latency_ms=elapsed)
            logger.debug("SkillRouter: empty base_scope (all skills disabled)")
            return {
                "routing_context": {"trigger": False},
                "frontend_enabled_skill_ids": frontend_ids,
                "frontend_scope_mode": scope_mode,
                "base_scope_skill_ids": [],
                "final_scope_skill_ids": [],
                "allowed_tool_names": [],
                "messages": cleaned_messages + [self._build_no_skill_prompt(reason="All skills explicitly disabled by user")],
            }

        # L0: should_route check
        if not should_route(routing_query, uploaded_files):
            elapsed = (time.monotonic() - start) * 1000
            record_request(trigger=False, latency_ms=elapsed)
            logger.debug("should_route=False query=%r routing_query=%r", query[:80], routing_query[:120])
            return {
                "routing_context": {"trigger": False},
                "frontend_enabled_skill_ids": frontend_ids,
                "frontend_scope_mode": scope_mode,
                "base_scope_skill_ids": base_scope_ids,
                "messages": cleaned_messages + [self._build_no_skill_prompt(reason="Query does not match any skill scope")],
            }

        # L1: task segmentation
        segments = segment_query(routing_query)
        if not segments:
            elapsed = (time.monotonic() - start) * 1000
            record_request(trigger=False, latency_ms=elapsed)
            return {
                "routing_context": {"trigger": False},
                "frontend_enabled_skill_ids": frontend_ids,
                "frontend_scope_mode": scope_mode,
                "base_scope_skill_ids": base_scope_ids,
                "messages": cleaned_messages + [self._build_no_skill_prompt(reason="Query cannot be segmented into skill-scoped tasks")],
            }

        # Process each segment
        scene_tasks: list[SceneTask] = []
        all_selected: dict[str, SelectedSkill] = {}  # skill_id -> SelectedSkill
        all_input_refs: list[str] = []
        all_allowed_tools: set[str] = set()

        base_scope_set = set(base_scope_ids)

        for seg in segments:
            seg_text = seg["text"]
            seg_scene = seg.get("scene") or intent_scene

            # L2: embedding
            try:
                query_vec = self.embedding_client.embed_text(seg_text)
            except Exception:
                record_embedding_error()
                logger.exception("Embedding API failed for segment: %s", seg_text[:80])
                continue

            # L3: ES Top-K
            filters = {"enabled": True}
            try:
                candidates = self.es_store.search(query_vector=query_vec, top_k=self.top_k, filters=filters)
            except Exception:
                record_es_error()
                logger.exception("ES search failed for segment: %s", seg_text[:80])
                candidates = []

            if not candidates:
                continue

            # v1: Post-filter — guarantee correctness regardless of ES index state
            if base_scope_set:
                candidates = [c for c in candidates if c.get("skill_id") in base_scope_set]

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
            elapsed = (time.monotonic() - start) * 1000
            record_request(trigger=False, latency_ms=elapsed)
            return {
                "routing_context": {"trigger": False},
                "frontend_enabled_skill_ids": frontend_ids,
                "frontend_scope_mode": scope_mode,
                "base_scope_skill_ids": base_scope_ids,
                "messages": cleaned_messages + [self._build_no_skill_prompt(reason="No skill candidates found for query segments")],
            }

        # L6: build routing_context with final_scope filtering
        global_skills = list(all_selected.keys())
        final_skill_ids = SkillScopeResolver.resolve_final_scope(
            skill_router_enabled=True,
            base_scope_ids=base_scope_ids,
            routed_skill_ids=global_skills,
        )
        confidence = self._compute_confidence(all_selected)

        # allowed_tools collected during resolution.
        # Phase 2b will add per-tool hard enforcement at the execution layer.
        final_allowed_tools = sorted(all_allowed_tools)

        routing_ctx = RoutingContext(
            route_mode="multi_segment" if len(scene_tasks) > 1 else "single_segment",
            trigger=True,
            primary_goal=self._infer_primary_goal(scene_tasks),
            scene_tasks=scene_tasks,
            global_selected_skills=final_skill_ids,
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
            routing_ctx.global_selected_skills, round(elapsed),
        )
        logger.info(
            "SkillRouter scope: frontend=%r base_scope=%d routed=%d final=%d allowed_tools=%d trigger=%s",
            frontend_ids, len(base_scope_ids), len(global_skills),
            len(final_skill_ids), len(final_allowed_tools),
            routing_ctx.trigger,
        )

        new_routed_skill_msg = SystemMessage(content=skills_override_msg, additional_kwargs={"message_type": "routed_skill_prompt"})

        return {
            "routing_context": routing_ctx.model_dump(),
            "frontend_enabled_skill_ids": frontend_ids,
            "base_scope_skill_ids": base_scope_ids,
            "final_scope_skill_ids": final_skill_ids,
            "allowed_tool_names": final_allowed_tools,
            "messages": cleaned_messages + [new_routed_skill_msg],
        }

    @override
    async def abefore_agent(self, state: AgentState, runtime: Runtime) -> dict[str, Any] | None:
        return self.before_agent(state, runtime)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_text(message: HumanMessage) -> str:
        """Extract plain text from a HumanMessage."""
        content = message.content
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts = []
            for block in content:
                if isinstance(block, dict) and block.get("type") == "text":
                    parts.append(block.get("text", ""))
                else:
                    t = getattr(block, "text", None)
                    if isinstance(t, str):
                        parts.append(t)
            return "\n".join(parts)
        return str(content) if content else ""

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

        # Build <available_skills> XML for routed skills
        skill_items = ""
        for idx, skill_id in enumerate(ctx.global_selected_skills, 1):
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
            empty_available_skills=(len(ctx.global_selected_skills) == 0),
            routed_mode=True,
        )

        # Insert task package text before the closing tag
        if task_pkg_text:
            base_system = base_system.replace("\n</skill_system>", task_pkg_text + "\n</skill_system>")

        return base_system

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
