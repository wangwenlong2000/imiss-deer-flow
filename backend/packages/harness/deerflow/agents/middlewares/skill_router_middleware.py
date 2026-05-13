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

        # Find the last user message
        last_user_msg = None
        for msg in reversed(messages):
            if isinstance(msg, HumanMessage):
                last_user_msg = msg
                break

        if last_user_msg is None:
            return None

        query = self._extract_text(last_user_msg)
        if not query or not query.strip():
            return None

        uploaded_files = state.get("uploaded_files") or []

        # L0: should_route check
        if not should_route(query, uploaded_files):
            elapsed = (time.monotonic() - start) * 1000
            record_request(trigger=False, latency_ms=elapsed)
            logger.debug("should_route=False query=%r", query[:80])
            return {"routing_context": {"trigger": False}}

        # L1: task segmentation
        segments = segment_query(query)
        if not segments:
            elapsed = (time.monotonic() - start) * 1000
            record_request(trigger=False, latency_ms=elapsed)
            return {"routing_context": {"trigger": False}}

        # Process each segment
        scene_tasks: list[SceneTask] = []
        all_selected: dict[str, SelectedSkill] = {}  # skill_id -> SelectedSkill
        all_input_refs: list[str] = []
        all_allowed_tools: set[str] = set()

        for seg in segments:
            seg_text = seg["text"]
            seg_scene = seg.get("scene")

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
            return {"routing_context": {"trigger": False}}

        # L6: build routing_context
        global_skills = list(all_selected.keys())
        confidence = self._compute_confidence(all_selected)

        routing_ctx = RoutingContext(
            route_mode="multi_segment" if len(scene_tasks) > 1 else "single_segment",
            trigger=True,
            primary_goal=self._infer_primary_goal(scene_tasks),
            scene_tasks=scene_tasks,
            global_selected_skills=global_skills,
            global_allowed_tools=sorted(all_allowed_tools),
            confidence=confidence,
            route_reason=f"Matched {len(scene_tasks)} task segment(s) from user query",
        )

        # L7: build skills_override system message
        skills_override_msg = self._build_skills_override(routing_ctx)

        elapsed = (time.monotonic() - start) * 1000
        record_request(trigger=True, latency_ms=elapsed)
        logger.info(
            "SkillRouter: query=%r trigger=%s mode=%s skills=%s latency_ms=%d",
            query[:80], routing_ctx.trigger, routing_ctx.route_mode,
            routing_ctx.global_selected_skills, round(elapsed),
        )
        logger.info(
            "SkillRouter scope: routed=%d, allowed_tools=%d, trigger=%s",
            len(ctx.global_selected_skills),
            len(ctx.global_allowed_tools),
            ctx.trigger,
        )

        return {
            "routing_context": routing_ctx.model_dump(),
            "messages": [SystemMessage(content=skills_override_msg, additional_kwargs={"message_type": "routed_skill_prompt"})],
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
        """Build a full <skill_system> block with routed skill details.

        When SkillRouter is enabled, the base system prompt contains zero skills.
        This method generates the authoritative skill list for the current turn,
        including skill name, description, and container location.
        """
        from deerflow.skills import load_skills

        all_skills = load_skills(enabled_only=True)
        skill_map = {s.name: s for s in all_skills}

        try:
            from deerflow.config import get_app_config

            container_base_path = get_app_config().skills.container_path
        except Exception:
            container_base_path = "/mnt/skills"

        lines = [
            "<skill_system>",
            "本次请求已由 SkillRouter 路由。以下为本轮可用的 Skills，仅可使用列出的 Skill：",
            "",
            "Only skills listed in the current 'Available Skills' section are available.",
            "Ignore any skills mentioned in previous turns if they are not listed here.",
            "Do not call tools associated with unavailable skills.",
            "",
        ]

        for idx, skill_id in enumerate(ctx.global_selected_skills, 1):
            skill = skill_map.get(skill_id)
            if skill:
                location = skill.get_container_file_path(container_base_path)
                lines.append(f"  <skill>")
                lines.append(f"    <name>{skill.name}</name>")
                lines.append(f"    <description>{skill.description}</description>")
                lines.append(f"    <location>{location}</location>")
                lines.append(f"  </skill>")
                lines.append("")

        # Task packages
        if ctx.scene_tasks:
            lines.append("任务包：")
            for st in ctx.scene_tasks:
                lines.append(f"- {st.scene_task_id}：{st.segment_text}")
            lines.append("")

        lines.extend([
            "Progressive Loading Pattern: Load skill files via read_file using the location path above.",
            "</skill_system>",
        ])
        return "\n".join(lines)
