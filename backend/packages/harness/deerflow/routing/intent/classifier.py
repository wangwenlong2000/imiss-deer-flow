"""Intent recognition for SkillRouter.

This module extracts the reusable design from the backup intent service:
scene classification, slot metadata, and query rewriting.  It deliberately
does not own chat state or inject prompts; middleware can consume the returned
``RoutingIntentResult`` and decide how to route the current turn.
"""

from __future__ import annotations

import json
import os
import re
import uuid
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

from langchain_core.messages import HumanMessage, SystemMessage
from pydantic import BaseModel, Field

from deerflow.routing.query_segmenter import is_obvious_chitchat
from deerflow.routing.intent_policy import policy_float, policy_int, policy_list, policy_value

_UPLOAD_BLOCK_RE = re.compile(r"<uploaded_files>[\s\S]*?</uploaded_files>\s*", re.IGNORECASE)


RoutingIntent = Literal[
    "chitchat",
    "capability_inventory",
    "task",
    "explicit_skill_request",
]


class SubTask(BaseModel):
    """A split sub-task from the original user query."""
    sub_task_id: str = Field(default_factory=lambda: f"sub_{uuid.uuid4().hex[:6]}")
    scene: str
    scene_name: str | None = None
    text: str  # The sub-task text (e.g., "审查合同的违约条款")
    params: dict[str, Any] = Field(default_factory=dict)


class TaskSpan(BaseModel):
    """A semantic task span extracted from the original query."""
    task_span_id: str = Field(default_factory=lambda: f"span_{uuid.uuid4().hex[:6]}")
    text: str


class RoutingIntentResult(BaseModel):
    intent: RoutingIntent
    original_query: str
    normalized_query: str
    routing_query: str
    # Legacy single-scene fields (kept for backward compatibility)
    scene: str | None = None
    scene_name: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    scene_mode: Literal["single", "multi"] | None = None
    # New multi-scene fields
    scenes: list[str] = Field(default_factory=list)  # All matched scenes
    task_spans: list[TaskSpan] = Field(default_factory=list)
    scene_tasks: list[SubTask] = Field(default_factory=list)
    sub_tasks: list[SubTask] = Field(default_factory=list)  # Split sub-tasks
    # Common fields
    task_hints: list[str] = Field(default_factory=list)
    mentioned_skill_ids: list[str] = Field(default_factory=list)
    confidence: float = 0.0
    reason: str | None = None


def _default_scene_templates_path() -> Path:
    return Path(__file__).with_name("scene_templates.json")


def _custom_scene_templates_paths() -> list[Path]:
    paths: list[Path] = []

    env_value = os.getenv("DEERFLOW_INTENT_SCENE_TEMPLATES")
    if env_value:
        for item in env_value.split(os.pathsep):
            if item.strip():
                paths.append(Path(item.strip()))

    # Repo-local user extension file.  This keeps packaged defaults stable and
    # lets deployments add or replace scenes with a single JSON file.
    backend_root = Path(__file__).resolve().parents[5]
    paths.append(backend_root / "config" / "intent_scene_templates.json")
    return paths


@lru_cache(maxsize=8)
def load_scene_templates(path: str | None = None) -> dict[str, dict[str, Any]]:
    template_path = Path(path) if path else _default_scene_templates_path()
    try:
        with template_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        data = {}
    if not isinstance(data, dict):
        data = {}

    merged = {str(k): v for k, v in data.items() if isinstance(v, dict)}
    if path is None:
        for custom_path in _custom_scene_templates_paths():
            try:
                with custom_path.open("r", encoding="utf-8") as f:
                    custom_data = json.load(f)
            except FileNotFoundError:
                continue
            if not isinstance(custom_data, dict):
                continue
            # Custom scenes intentionally override packaged defaults by id.
            merged.update({str(k): v for k, v in custom_data.items() if isinstance(v, dict)})
    return merged


def classify_routing_intent(
    query: str,
    *,
    scene_templates: dict[str, dict[str, Any]] | None = None,
    uploaded_files: list[dict] | None = None,
    available_skill_ids: list[str] | None = None,
) -> RoutingIntentResult:
    original_query = _strip_uploaded_files_block(query or "")
    normalized_query = _normalize(original_query)

    if is_obvious_chitchat(original_query, uploaded_files):
        return RoutingIntentResult(
            intent="chitchat",
            original_query=original_query,
            normalized_query=normalized_query,
            routing_query=original_query.strip(),
            confidence=1.0,
            reason="obvious_chitchat",
        )

    inventory_confidence = _capability_inventory_confidence(original_query)
    if inventory_confidence > 0:
        return RoutingIntentResult(
            intent="capability_inventory",
            original_query=original_query,
            normalized_query=normalized_query,
            routing_query="列出当前前端授权范围内可用的技能能力。用户原始问题：" + original_query.strip(),
            confidence=inventory_confidence,
            reason="capability_inventory",
        )

    mentioned_skill_ids = _find_mentioned_skill_ids(original_query, available_skill_ids or [])
    intent: RoutingIntent = "explicit_skill_request" if mentioned_skill_ids else "task"

    templates = scene_templates if scene_templates is not None else load_scene_templates()
    scene_id, scene_config, scene_score = _match_scene(original_query, templates)
    if scene_id and scene_config:
        primary_scene_id = _public_scene_id(scene_id, scene_config)
        public_scene_ids = [primary_scene_id]
        routing_query = _build_scene_routing_query(original_query, scene_config)
        return RoutingIntentResult(
            intent=intent,
            original_query=original_query,
            normalized_query=normalized_query,
            routing_query=routing_query,
            scene=primary_scene_id,
            scene_name=str(scene_config.get("name") or scene_id),
            params=_empty_params(scene_config),
            scene_mode="single",
            scene_tasks=[_build_intent_scene_task(primary_scene_id, scene_config, original_query, _empty_params(scene_config))],
            sub_tasks=[_build_intent_scene_task(primary_scene_id, scene_config, original_query, _empty_params(scene_config))],
            task_hints=_build_task_hints(scene_config),
            mentioned_skill_ids=mentioned_skill_ids,
            confidence=scene_score,
            reason="matched_configured_scene",
            scenes=public_scene_ids,
        )

    return RoutingIntentResult(
        intent=intent,
        original_query=original_query,
        normalized_query=normalized_query,
        routing_query=original_query.strip(),
        mentioned_skill_ids=mentioned_skill_ids,
        confidence=0.5 if mentioned_skill_ids else 0.3,
        reason="no_configured_scene_match",
    )


def classify_routing_intent_with_llm(
    query: str,
    *,
    llm: Any,
    scene_templates: dict[str, dict[str, Any]] | None = None,
    uploaded_files: list[dict] | None = None,
    available_skill_ids: list[str] | None = None,
    previous_intent: dict[str, Any] | None = None,
) -> RoutingIntentResult:
    """Run full intent recognition with LLM scene selection and slot filling.

    Falls back to the deterministic classifier if the model call fails or
    returns unusable output.
    """
    query = _strip_uploaded_files_block(query or "")
    baseline = classify_routing_intent(
        query,
        scene_templates=scene_templates,
        uploaded_files=uploaded_files,
        available_skill_ids=available_skill_ids,
    )
    if baseline.intent in {"chitchat", "capability_inventory"}:
        return baseline

    templates = scene_templates if scene_templates is not None else load_scene_templates()
    if not templates:
        return baseline

    try:
        related_scene = _reuse_related_previous_scene(
            query=query,
            llm=llm,
            scene_templates=templates,
            previous_intent=previous_intent,
        )
        task_spans = _extract_task_spans_with_llm(
            query,
            llm=llm,
            scene_templates=templates,
            baseline_scene=baseline.scene_name if baseline.scene else None,
            baseline_confidence=baseline.confidence,
        )
        normalized_spans = _normalize_task_span_texts(task_spans, query)
        if not normalized_spans:
            normalized_spans = [query.strip()]

        if len(normalized_spans) <= 1:
            span_text = normalized_spans[0]
            recognized_scenes = _normalize_scene_list(
                _recognize_scenes_with_llm(
                    span_text,
                    llm=llm,
                    scene_templates=templates,
                    baseline_scene=baseline.scene_name if baseline.scene else None,
                    baseline_confidence=baseline.confidence,
                )
                + ([related_scene] if related_scene else [])
            )
            if len(recognized_scenes) > 1:
                scene_tasks = _recognize_scene_tasks_with_llm(
                    span_text,
                    llm=llm,
                    scene_templates=templates,
                    recognized_scenes=recognized_scenes,
                    baseline_scene=baseline.scene_name if baseline.scene else None,
                    baseline_confidence=baseline.confidence,
                )
                scene_tasks = _ensure_scene_tasks_for_scenes(
                    scene_tasks,
                    recognized_scenes,
                    span_text,
                    templates,
                )
                scenes = _normalize_scene_list([task.scene for task in scene_tasks])
                first_scene = scene_tasks[0].scene
                first_scene_config = templates.get(first_scene, {})
                task_span_models = [TaskSpan(text=span_text)]
                task_hints = []
                params = {}
                routing_query = query.strip()
                scene_id = first_scene
                scene_config = first_scene_config
                public_scene_id = first_scene
                is_multi_scene = True
            else:
                scene_id, scene_config = _resolve_scene_for_span(
                    span_text,
                    llm=llm,
                    scene_templates=templates,
                    baseline_scene=baseline.scene_name if baseline.scene else None,
                    baseline_confidence=baseline.confidence,
                )
                if not scene_id or not scene_config:
                    scene_id = related_scene or _recognize_scene_with_llm(
                        span_text,
                        llm=llm,
                        scene_templates=templates,
                        baseline_scene=baseline.scene_name if baseline.scene else None,
                        baseline_confidence=baseline.confidence,
                    )
                    scene_config = templates.get(scene_id or "", {}) if scene_id else None
                if not scene_id or not scene_config:
                    return _rewrite_non_builtin_question(query, baseline, llm=llm)

                public_scene_id = _public_scene_id(scene_id, scene_config)
                params = _extract_scene_params_with_llm(span_text, llm=llm, scene_config=scene_config)
                rewritten = _rewrite_question_with_llm(span_text, llm=llm, scene_config=scene_config, params=params)
                routing_query = _build_scene_routing_query(rewritten or span_text, scene_config)
                if rewritten and rewritten.strip() != span_text.strip():
                    routing_query = _append_original_query_once(routing_query, span_text)
                scene_task = _build_intent_scene_task(public_scene_id, scene_config, rewritten or span_text, params)
                task_span_models = [TaskSpan(text=span_text)]
                scenes = [public_scene_id]
                scene_tasks = [scene_task]
                task_hints = _build_task_hints(scene_config)
                is_multi_scene = False
        else:
            scene_tasks = []
            task_span_models = [TaskSpan(text=span_text) for span_text in normalized_spans]
            for span_text in normalized_spans:
                scene_id, scene_config = _resolve_scene_for_span(
                    span_text,
                    llm=llm,
                    scene_templates=templates,
                    baseline_scene=baseline.scene_name if baseline.scene else None,
                    baseline_confidence=baseline.confidence,
                )
                if not scene_id or not scene_config:
                    continue
                public_scene_id = _public_scene_id(scene_id, scene_config)
                params = _extract_scene_params_with_llm(span_text, llm=llm, scene_config=scene_config)
                scene_tasks.append(
                    _build_intent_scene_task(public_scene_id, scene_config, span_text, params)
                )

            if not scene_tasks:
                return _rewrite_non_builtin_question(query, baseline, llm=llm)

            scene_tasks = _dedupe_scene_tasks(scene_tasks)
            scenes = _normalize_scene_list([task.scene for task in scene_tasks])
            first_scene = scene_tasks[0].scene
            first_scene_config = templates.get(first_scene, {})
            task_hints = []
            params = {}
            routing_query = query.strip()
            scene_id = first_scene
            scene_config = first_scene_config
            public_scene_id = first_scene
            is_multi_scene = True

        if not scenes:
            scenes = [public_scene_id]

        return RoutingIntentResult(
            intent=baseline.intent,
            original_query=query,
            normalized_query=_normalize(query),
            routing_query=routing_query,
            scene=public_scene_id,
            scene_name=str(scene_config.get("name") or scene_id),
            params=params if is_multi_scene else (params or _empty_params(scene_config)),
            task_hints=task_hints if is_multi_scene else task_hints,
            mentioned_skill_ids=baseline.mentioned_skill_ids,
            confidence=0.9,
            reason="llm_scene_slot_rewrite",
            scenes=scenes,
            scene_mode="multi" if is_multi_scene else "single",
            scene_tasks=scene_tasks,
            sub_tasks=scene_tasks,
            task_spans=task_span_models,
        )
    except Exception:
        return baseline


async def aclassify_routing_intent_with_llm(
    query: str,
    *,
    llm: Any,
    scene_templates: dict[str, dict[str, Any]] | None = None,
    uploaded_files: list[dict] | None = None,
    available_skill_ids: list[str] | None = None,
    previous_intent: dict[str, Any] | None = None,
) -> RoutingIntentResult:
    query = _strip_uploaded_files_block(query or "")
    baseline = classify_routing_intent(
        query,
        scene_templates=scene_templates,
        uploaded_files=uploaded_files,
        available_skill_ids=available_skill_ids,
    )
    if baseline.intent in {"chitchat", "capability_inventory"}:
        return baseline

    templates = scene_templates if scene_templates is not None else load_scene_templates()
    if not templates:
        return baseline

    try:
        related_scene = await _areuse_related_previous_scene(
            query=query,
            llm=llm,
            scene_templates=templates,
            previous_intent=previous_intent,
        )
        task_spans = await _aextract_task_spans_with_llm(
            query,
            llm=llm,
            scene_templates=templates,
            baseline_scene=baseline.scene_name if baseline.scene else None,
            baseline_confidence=baseline.confidence,
        )
        normalized_spans = _normalize_task_span_texts(task_spans, query)
        if not normalized_spans:
            normalized_spans = [query.strip()]

        if len(normalized_spans) <= 1:
            span_text = normalized_spans[0]
            recognized_scenes = _normalize_scene_list(
                await _arecognize_scenes_with_llm(
                    span_text,
                    llm=llm,
                    scene_templates=templates,
                    baseline_scene=baseline.scene_name if baseline.scene else None,
                    baseline_confidence=baseline.confidence,
                )
                + ([related_scene] if related_scene else [])
            )
            if len(recognized_scenes) > 1:
                scene_tasks = await _arecognize_scene_tasks_with_llm(
                    span_text,
                    llm=llm,
                    scene_templates=templates,
                    recognized_scenes=recognized_scenes,
                    baseline_scene=baseline.scene_name if baseline.scene else None,
                    baseline_confidence=baseline.confidence,
                )
                scene_tasks = _ensure_scene_tasks_for_scenes(
                    scene_tasks,
                    recognized_scenes,
                    span_text,
                    templates,
                )
                scenes = _normalize_scene_list([task.scene for task in scene_tasks])
                first_scene = scene_tasks[0].scene
                first_scene_config = templates.get(first_scene, {})
                task_span_models = [TaskSpan(text=span_text)]
                task_hints = []
                params = {}
                routing_query = query.strip()
                scene_id = first_scene
                scene_config = first_scene_config
                public_scene_id = first_scene
                is_multi_scene = True
            else:
                scene_id, scene_config = await _aresolve_scene_for_span(
                    span_text,
                    llm=llm,
                    scene_templates=templates,
                    baseline_scene=baseline.scene_name if baseline.scene else None,
                    baseline_confidence=baseline.confidence,
                )
                if not scene_id or not scene_config:
                    scene_id = related_scene or await _arecognize_scene_with_llm(
                        span_text,
                        llm=llm,
                        scene_templates=templates,
                        baseline_scene=baseline.scene_name if baseline.scene else None,
                        baseline_confidence=baseline.confidence,
                    )
                    scene_config = templates.get(scene_id or "", {}) if scene_id else None
                if not scene_id or not scene_config:
                    return await _arewrite_non_builtin_question(query, baseline, llm=llm)

                public_scene_id = _public_scene_id(scene_id, scene_config)
                params = await _aextract_scene_params_with_llm(span_text, llm=llm, scene_config=scene_config)
                rewritten = await _arewrite_question_with_llm(span_text, llm=llm, scene_config=scene_config, params=params)
                routing_query = _build_scene_routing_query(rewritten or span_text, scene_config)
                if rewritten and rewritten.strip() != span_text.strip():
                    routing_query = _append_original_query_once(routing_query, span_text)
                scene_task = _build_intent_scene_task(public_scene_id, scene_config, rewritten or span_text, params)
                task_span_models = [TaskSpan(text=span_text)]
                scenes = [public_scene_id]
                scene_tasks = [scene_task]
                task_hints = _build_task_hints(scene_config)
                is_multi_scene = False
        else:
            scene_tasks = []
            task_span_models = [TaskSpan(text=span_text) for span_text in normalized_spans]
            for span_text in normalized_spans:
                scene_id, scene_config = await _aresolve_scene_for_span(
                    span_text,
                    llm=llm,
                    scene_templates=templates,
                    baseline_scene=baseline.scene_name if baseline.scene else None,
                    baseline_confidence=baseline.confidence,
                )
                if not scene_id or not scene_config:
                    continue
                public_scene_id = _public_scene_id(scene_id, scene_config)
                params = await _aextract_scene_params_with_llm(span_text, llm=llm, scene_config=scene_config)
                scene_tasks.append(
                    _build_intent_scene_task(public_scene_id, scene_config, span_text, params)
                )

            if not scene_tasks:
                return await _arewrite_non_builtin_question(query, baseline, llm=llm)

            scene_tasks = _dedupe_scene_tasks(scene_tasks)
            scenes = _normalize_scene_list([task.scene for task in scene_tasks])
            first_scene = scene_tasks[0].scene
            first_scene_config = templates.get(first_scene, {})
            task_hints = []
            params = {}
            routing_query = query.strip()
            scene_id = first_scene
            scene_config = first_scene_config
            public_scene_id = first_scene
            is_multi_scene = True

        if not scenes:
            scenes = [public_scene_id]

        return RoutingIntentResult(
            intent=baseline.intent,
            original_query=query,
            normalized_query=_normalize(query),
            routing_query=routing_query,
            scene=public_scene_id,
            scene_name=str(scene_config.get("name") or scene_id),
            params=params if is_multi_scene else (params or _empty_params(scene_config)),
            task_hints=task_hints if is_multi_scene else task_hints,
            mentioned_skill_ids=baseline.mentioned_skill_ids,
            confidence=0.9,
            reason="llm_scene_slot_rewrite",
            scenes=scenes,
            scene_mode="multi" if is_multi_scene else "single",
            scene_tasks=scene_tasks,
            sub_tasks=scene_tasks,
            task_spans=task_span_models,
        )
    except Exception:
        return baseline


def _normalize(text: str) -> str:
    lowered = text.lower().replace("_", " ").replace("-", " ")
    return re.sub(r"\s+", " ", lowered).strip()


def _strip_uploaded_files_block(text: str) -> str:
    """Remove UploadsMiddleware context from text before intent routing."""
    return _UPLOAD_BLOCK_RE.sub("", text or "").strip()


def _invoke_text(llm: Any, system_prompt: str, user_prompt: str) -> str:
    tagged_llm = llm.with_config(
        {
            "tags": ["intent_recognition_internal"],
            "metadata": {
                "intent_recognition_internal": True,
                "internal_visibility": "hidden",
            },
            "run_name": "intent_recognition_internal",
        }
    )
    response = tagged_llm.invoke([
        SystemMessage(content=system_prompt),
        HumanMessage(content=user_prompt),
    ])
    return _response_text(response)


async def _ainvoke_text(llm: Any, system_prompt: str, user_prompt: str) -> str:
    tagged_llm = llm.with_config(
        {
            "tags": ["intent_recognition_internal"],
            "metadata": {
                "intent_recognition_internal": True,
                "internal_visibility": "hidden",
            },
            "run_name": "intent_recognition_internal",
        }
    )
    response = await tagged_llm.ainvoke([
        SystemMessage(content=system_prompt),
        HumanMessage(content=user_prompt),
    ])
    return _response_text(response)


def _response_text(response: Any) -> str:
    content = getattr(response, "content", response)
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                text = item.get("text")
                if isinstance(text, str):
                    parts.append(text)
            else:
                text = getattr(item, "text", None)
                if isinstance(text, str):
                    parts.append(text)
        return "\n".join(parts)
    return str(content) if content is not None else ""


def _reuse_related_previous_scene(
    *,
    query: str,
    llm: Any,
    scene_templates: dict[str, dict[str, Any]],
    previous_intent: dict[str, Any] | None,
) -> str | None:
    scene_id = _previous_scene_id(previous_intent, scene_templates)
    if not scene_id:
        return None
    prompt = _related_scene_prompt(query, scene_templates[scene_id])
    raw = _invoke_text(llm, prompt, query)
    if _extract_float(raw) >= policy_float("thresholds", "related_previous_scene"):
        return scene_id
    return None


async def _areuse_related_previous_scene(
    *,
    query: str,
    llm: Any,
    scene_templates: dict[str, dict[str, Any]],
    previous_intent: dict[str, Any] | None,
) -> str | None:
    scene_id = _previous_scene_id(previous_intent, scene_templates)
    if not scene_id:
        return None
    prompt = _related_scene_prompt(query, scene_templates[scene_id])
    raw = await _ainvoke_text(llm, prompt, query)
    if _extract_float(raw) >= policy_float("thresholds", "related_previous_scene"):
        return scene_id
    return None


def _previous_scene_id(
    previous_intent: dict[str, Any] | None,
    scene_templates: dict[str, dict[str, Any]],
) -> str | None:
    if not previous_intent:
        return None
    scene_id = previous_intent.get("scene")
    if isinstance(scene_id, str) and scene_id in scene_templates:
        return scene_id
    if isinstance(scene_id, str):
        for template_id, config in scene_templates.items():
            if _public_scene_id(template_id, config) == scene_id:
                return template_id
    return None


def _public_scene_id(scene_id: str, scene_config: dict[str, Any]) -> str:
    configured_scene = scene_config.get("scene")
    if isinstance(configured_scene, str) and configured_scene.strip():
        return configured_scene.strip()
    return scene_id


def _related_scene_prompt(query: str, scene_config: dict[str, Any]) -> str:
    return _render_policy_prompt(
        "related_scene",
        scene_name=scene_config.get("name"),
        scene_description=scene_config.get("description"),
        query=query,
    )


def _recognize_scene_with_llm(
    query: str,
    *,
    llm: Any,
    scene_templates: dict[str, dict[str, Any]],
    baseline_scene: str | None = None,
    baseline_confidence: float = 0.0,
) -> str | None:
    options, option_map = _scene_options(scene_templates)
    system_prompt = _render_policy_prompt(
        "recognize_scene",
        options=options,
        query=query,
        baseline_scene=baseline_scene or "无",
        baseline_confidence=baseline_confidence,
    )
    raw = _invoke_text(llm, system_prompt, query)
    return _parse_scene_choice(raw, option_map)


async def _arecognize_scene_with_llm(
    query: str,
    *,
    llm: Any,
    scene_templates: dict[str, dict[str, Any]],
    baseline_scene: str | None = None,
    baseline_confidence: float = 0.0,
) -> str | None:
    options, option_map = _scene_options(scene_templates)
    system_prompt = _render_policy_prompt(
        "recognize_scene",
        options=options,
        query=query,
        baseline_scene=baseline_scene or "无",
        baseline_confidence=baseline_confidence,
    )
    raw = await _ainvoke_text(llm, system_prompt, query)
    return _parse_scene_choice(raw, option_map)


def _scene_options(scene_templates: dict[str, dict[str, Any]]) -> tuple[str, dict[str, str]]:
    """Generate LLM scene selection options with enriched context.

    Each option includes name, description, key task hints from parameters,
    and a brief example excerpt to help LLM distinguish similar scenes.
    """
    lines: list[str] = []
    option_map: dict[str, str] = {}
    for idx, (scene_id, config) in enumerate(scene_templates.items(), 1):
        option = str(idx)
        option_map[option] = scene_id
        name = config.get("name") or scene_id
        description = config.get("description") or ""

        # Extract key task hints from parameter descriptions
        param_hints: list[str] = []
        for param in config.get("parameters", []) or []:
            if not isinstance(param, dict):
                continue
            param_name = param.get("name", "")
            param_desc = param.get("desc", "")
            if param_name and param_desc:
                param_hints.append(f"{param_name}({param_desc})")
            # Include option values if present
            for opt_val in param.get("options", []) or []:
                if opt_val and len(param_hints) < 5:  # Limit to avoid overload
                    param_hints.append(str(opt_val))

        # Extract brief example excerpt (first 50 chars of the answer part)
        example_excerpt = ""
        example = config.get("example") or ""
        if "答：" in example:
            example_excerpt = example.split("答：")[1][:50].strip()
        elif example:
            example_excerpt = example[:50].strip()

        # Build enriched option text
        parts = [f"{idx}. {name} - {description}"]
        if param_hints:
            parts.append(f"参数: {', '.join(param_hints[:3])}")
        if example_excerpt:
            parts.append(f"示例: {example_excerpt}")
        positive_examples = _list_preview(config.get("positive_examples"), limit=2)
        if positive_examples:
            parts.append(f"正例: {positive_examples}")
        negative_examples = _list_preview(config.get("negative_examples"), limit=2)
        if negative_examples:
            parts.append(f"反例: {negative_examples}")
        keywords = _list_preview(config.get("keywords"), limit=8)
        if keywords:
            parts.append(f"关键词: {keywords}")
        anti_keywords = _list_preview(config.get("anti_keywords"), limit=8)
        if anti_keywords:
            parts.append(f"排除词: {anti_keywords}")
        parts.append(f"请回复{idx}")
        lines.append(" | ".join(parts))
    return "\n".join(lines), option_map


def _list_preview(value: Any, *, limit: int) -> str:
    if not isinstance(value, list):
        return ""
    items = [str(item).strip() for item in value if str(item).strip()]
    return "；".join(items[:limit])


def _parse_scene_choice(raw: str, option_map: dict[str, str]) -> str | None:
    digits = re.findall(r"\d+", raw or "")
    if not digits:
        return None
    choice = digits[0]
    if choice == "0":
        return None
    return option_map.get(choice)


def _recognize_scenes_with_llm(
    query: str,
    *,
    llm: Any,
    scene_templates: dict[str, dict[str, Any]],
    baseline_scene: str | None = None,
    baseline_confidence: float = 0.0,
) -> list[str]:
    options, option_map = _scene_options(scene_templates)
    system_prompt = _render_policy_prompt(
        "recognize_scenes",
        options=options,
        query=query,
        baseline_scene=baseline_scene or "无",
        baseline_confidence=baseline_confidence,
        max_scenes=int(policy_value("thresholds", "multi_scene_max_count", default=2)),
    )
    raw = _invoke_text(llm, system_prompt, query)
    return _parse_scene_choices(raw, option_map)


async def _arecognize_scenes_with_llm(
    query: str,
    *,
    llm: Any,
    scene_templates: dict[str, dict[str, Any]],
    baseline_scene: str | None = None,
    baseline_confidence: float = 0.0,
) -> list[str]:
    options, option_map = _scene_options(scene_templates)
    system_prompt = _render_policy_prompt(
        "recognize_scenes",
        options=options,
        query=query,
        baseline_scene=baseline_scene or "无",
        baseline_confidence=baseline_confidence,
        max_scenes=int(policy_value("thresholds", "multi_scene_max_count", default=2)),
    )
    raw = await _ainvoke_text(llm, system_prompt, query)
    return _parse_scene_choices(raw, option_map)


def _recognize_scene_tasks_with_llm(
    query: str,
    *,
    llm: Any,
    scene_templates: dict[str, dict[str, Any]],
    recognized_scenes: list[str],
    baseline_scene: str | None = None,
    baseline_confidence: float = 0.0,
) -> list[SubTask]:
    options, option_map = _scene_options(scene_templates)
    system_prompt = _render_policy_prompt(
        "recognize_scene_tasks",
        options=options,
        query=query,
        baseline_scene=baseline_scene or "无",
        baseline_confidence=baseline_confidence,
        scene_ids=", ".join(recognized_scenes),
    )
    raw = _invoke_text(llm, system_prompt, query)
    return _parse_scene_task_items(raw, option_map, scene_templates)


async def _arecognize_scene_tasks_with_llm(
    query: str,
    *,
    llm: Any,
    scene_templates: dict[str, dict[str, Any]],
    recognized_scenes: list[str],
    baseline_scene: str | None = None,
    baseline_confidence: float = 0.0,
) -> list[SubTask]:
    options, option_map = _scene_options(scene_templates)
    system_prompt = _render_policy_prompt(
        "recognize_scene_tasks",
        options=options,
        query=query,
        baseline_scene=baseline_scene or "无",
        baseline_confidence=baseline_confidence,
        scene_ids=", ".join(recognized_scenes),
    )
    raw = await _ainvoke_text(llm, system_prompt, query)
    return _parse_scene_task_items(raw, option_map, scene_templates)


def _extract_task_spans_with_llm(
    query: str,
    *,
    llm: Any,
    scene_templates: dict[str, dict[str, Any]],
    baseline_scene: str | None = None,
    baseline_confidence: float = 0.0,
) -> list[TaskSpan]:
    _ = scene_templates
    system_prompt = _render_policy_prompt(
        "extract_task_spans",
        query=query,
        baseline_scene=baseline_scene or "无",
        baseline_confidence=baseline_confidence,
    )
    raw = _invoke_text(llm, system_prompt, query)
    return _parse_task_span_items(raw)


async def _aextract_task_spans_with_llm(
    query: str,
    *,
    llm: Any,
    scene_templates: dict[str, dict[str, Any]],
    baseline_scene: str | None = None,
    baseline_confidence: float = 0.0,
) -> list[TaskSpan]:
    _ = scene_templates
    system_prompt = _render_policy_prompt(
        "extract_task_spans",
        query=query,
        baseline_scene=baseline_scene or "无",
        baseline_confidence=baseline_confidence,
    )
    raw = await _ainvoke_text(llm, system_prompt, query)
    return _parse_task_span_items(raw)


def _parse_scene_choices(raw: str, option_map: dict[str, str]) -> list[str]:
    parsed = _extract_json_value(raw)
    candidates: list[Any]
    if isinstance(parsed, list):
        candidates = parsed
    elif isinstance(parsed, dict):
        value = parsed.get("scenes") or parsed.get("scene") or parsed.get("items")
        candidates = value if isinstance(value, list) else [value]
    else:
        candidates = []

    if not candidates:
        text = (raw or "").strip()
        if not text:
            return []
        candidates = re.findall(r"\d+", text)

    scene_ids: list[str] = []
    for item in candidates:
        scene_id = None
        if isinstance(item, str):
            token = item.strip().strip('"').strip("'")
            if token in option_map:
                scene_id = option_map[token]
            elif token in option_map.values():
                scene_id = token
        elif isinstance(item, int):
            scene_id = option_map.get(str(item))
        if scene_id and scene_id not in scene_ids:
            scene_ids.append(scene_id)
    return scene_ids


def _parse_scene_task_items(
    raw: str,
    option_map: dict[str, str],
    scene_templates: dict[str, dict[str, Any]],
) -> list[SubTask]:
    parsed = _extract_json_value(raw)
    items: list[Any]
    if isinstance(parsed, list):
        items = parsed
    elif isinstance(parsed, dict):
        nested = parsed.get("scene_tasks") or parsed.get("tasks") or parsed.get("items")
        items = nested if isinstance(nested, list) else [parsed]
    else:
        items = []

    tasks: list[SubTask] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        scene_id = _normalize_scene_token(item.get("scene"), option_map)
        if not scene_id:
            continue
        scene_config = scene_templates.get(scene_id, {})
        scene_name = item.get("scene_name")
        if not isinstance(scene_name, str) or not scene_name.strip():
            scene_name = str(scene_config.get("name") or scene_id)
        text = item.get("task_text")
        if not isinstance(text, str) or not text.strip():
            text = item.get("text")
        if not isinstance(text, str) or not text.strip():
            text = ""
        params = item.get("params") if isinstance(item.get("params"), dict) else {}
        tasks.append(SubTask(scene=scene_id, scene_name=scene_name, text=text.strip(), params=params))
    return _dedupe_scene_tasks(tasks)


def _parse_task_span_items(raw: str) -> list[TaskSpan]:
    parsed = _extract_json_value(raw)
    items: list[Any]
    if isinstance(parsed, list):
        items = parsed
    elif isinstance(parsed, dict):
        nested = parsed.get("task_spans") or parsed.get("spans") or parsed.get("items")
        items = nested if isinstance(nested, list) else [parsed]
    else:
        items = []

    spans: list[TaskSpan] = []
    for item in items:
        if isinstance(item, str):
            text = item.strip()
        elif isinstance(item, dict):
            text = item.get("task_text") or item.get("text") or item.get("span_text") or ""
            if not isinstance(text, str):
                text = ""
        else:
            continue
        text = text.strip()
        if text:
            spans.append(TaskSpan(text=text))
    return _dedupe_task_spans(spans)


def _normalize_scene_token(value: Any, option_map: dict[str, str]) -> str | None:
    if isinstance(value, str):
        token = value.strip().strip('"').strip("'")
        if token in option_map:
            return option_map[token]
        if token in option_map.values():
            return token
        return token if token else None
    if isinstance(value, int):
        return option_map.get(str(value))
    return None


def _dedupe_scene_tasks(tasks: list[SubTask]) -> list[SubTask]:
    seen: set[tuple[str, str]] = set()
    result: list[SubTask] = []
    for task in tasks:
        key = (task.scene, task.text)
        if key in seen:
            continue
        seen.add(key)
        result.append(task)
    return result


def _ensure_scene_tasks_for_scenes(
    tasks: list[SubTask],
    scene_ids: list[str],
    fallback_text: str,
    scene_templates: dict[str, dict[str, Any]],
) -> list[SubTask]:
    """Keep every recognized scene visible even if LLM task splitting omits one."""
    ordered_scene_ids = _normalize_scene_list(scene_ids)
    normalized_tasks = _dedupe_scene_tasks(tasks)
    existing_scenes = {task.scene for task in normalized_tasks}

    for scene_id in ordered_scene_ids:
        if scene_id in existing_scenes:
            continue
        scene_config = scene_templates.get(scene_id)
        if not scene_config:
            continue
        public_scene_id = _public_scene_id(scene_id, scene_config)
        normalized_tasks.append(
            _build_intent_scene_task(public_scene_id, scene_config, fallback_text, {})
        )
        existing_scenes.add(public_scene_id)

    order = {scene_id: index for index, scene_id in enumerate(ordered_scene_ids)}
    return sorted(
        _dedupe_scene_tasks(normalized_tasks),
        key=lambda task: order.get(task.scene, len(order)),
    )


def _dedupe_task_spans(spans: list[TaskSpan]) -> list[TaskSpan]:
    seen: set[str] = set()
    result: list[TaskSpan] = []
    for span in spans:
        text = span.text.strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(TaskSpan(text=text))
    return result


def _normalize_task_span_texts(task_spans: list[TaskSpan], query: str) -> list[str]:
    texts: list[str] = []
    for span in task_spans:
        text = span.text.strip()
        if _is_incomplete_task_span(text):
            continue
        if _is_output_requirement_span(text):
            continue
        if text and text not in texts:
            texts.append(text)
    query_text = query.strip()
    if not texts and query_text:
        texts.append(query_text)
    elif query_text and texts and _has_detached_output_requirements(task_spans):
        texts = _merge_spans_with_output_requirements(texts, query_text)
    elif query_text and len(texts) > 1 and _span_coverage_ratio(texts, query_text) < 0.82:
        return [query_text]
    elif query_text and len(texts) == 1 and texts[0] != query_text and _span_coverage_ratio(texts, query_text) < 0.82:
        return [query_text]
    return texts


def _is_incomplete_task_span(text: str) -> bool:
    value = text.strip()
    if not value:
        return True
    if len(value) <= 40 and value.endswith((":", "：")):
        return True
    return False


def _is_output_requirement_span(text: str) -> bool:
    value = text.strip()
    if not value:
        return True

    compact = _coverage_key(value)
    output_markers = (
        "报告需包含",
        "报告包含",
        "报告要求包含",
        "需包含",
        "包括",
        "请给出至少",
        "给出至少",
        "可视化图表",
        "可视化",
        "图表",
        "表格",
        "结论中保留",
        "保留可回溯",
        "数据来源位置",
        "可回溯的数据来源",
        "下一步行动建议",
        "行动建议",
        "优先督办",
    )
    task_action_markers = (
        "分析",
        "判断",
        "识别",
        "检索",
        "查询",
        "审查",
        "处理",
        "解析",
        "预测",
        "评估",
    )
    report_markers = ("报告", "专项分析报告", "材料", "方案", "意见")

    has_output_marker = any(marker in value or marker in compact for marker in output_markers)
    has_report_marker = any(marker in value or marker in compact for marker in report_markers)
    has_task_action = any(marker in value or marker in compact for marker in task_action_markers)

    if has_output_marker and not has_task_action:
        return True
    if has_output_marker and has_report_marker:
        return True
    if value.startswith(("报告需包含", "报告包含", "同时请给出", "并在结论中", "在结论中")):
        return True
    return False


def _has_detached_output_requirements(task_spans: list[TaskSpan]) -> bool:
    return any(_is_output_requirement_span(span.text) for span in task_spans)


def _merge_spans_with_output_requirements(texts: list[str], query: str) -> list[str]:
    if len(texts) <= 1:
        return [query]
    if _span_coverage_ratio(texts, query) < 0.92:
        return [query]
    return texts


def _span_coverage_ratio(spans: list[str], query: str) -> float:
    query_key = _coverage_key(query)
    if not query_key:
        return 1.0
    covered = 0
    for span in spans:
        span_key = _coverage_key(span)
        if not span_key:
            continue
        if span_key in query_key:
            covered += len(span_key)
        else:
            # LLM may lightly normalize punctuation or pronouns. Count it as
            # partial coverage instead of rejecting otherwise good splits.
            covered += int(len(span_key) * 0.5)
    return min(covered, len(query_key)) / len(query_key)


def _coverage_key(text: str) -> str:
    return re.sub(r"[\s，。；;：:、,.!?！？“”\"'（）()【】\[\]<>《》]+", "", text.strip())


def _resolve_scene_for_span(
    span_text: str,
    *,
    llm: Any,
    scene_templates: dict[str, dict[str, Any]],
    baseline_scene: str | None = None,
    baseline_confidence: float = 0.0,
) -> tuple[str | None, dict[str, Any] | None]:
    ranked = _rank_scene_candidates(span_text, scene_templates)
    if ranked:
        top_scene_id, top_config, top_score, _top_raw_score = ranked[0]
        second_score = ranked[1][2] if len(ranked) > 1 else 0.0
        if top_score >= policy_float("thresholds", "scene_match_min_score") and (
            len(ranked) == 1 or top_score - second_score >= policy_float("thresholds", "multi_scene_score_gap")
        ):
            return top_scene_id, top_config

        candidate_scene_ids = [
            scene_id
            for scene_id, _config, score, _raw_score in ranked[: int(policy_value("thresholds", "multi_scene_max_count", default=2)) + 1]
            if score > 0
        ]
        candidate_templates = {
            scene_id: scene_templates[scene_id]
            for scene_id in candidate_scene_ids
            if scene_id in scene_templates
        }
        if candidate_templates:
            scene_id = _recognize_scene_with_llm(
                span_text,
                llm=llm,
                scene_templates=candidate_templates,
                baseline_scene=baseline_scene,
                baseline_confidence=max(baseline_confidence, top_score),
            )
            if scene_id and scene_id in candidate_templates:
                return scene_id, candidate_templates[scene_id]
        return top_scene_id, top_config

    scene_id = _recognize_scene_with_llm(
        span_text,
        llm=llm,
        scene_templates=scene_templates,
        baseline_scene=baseline_scene,
        baseline_confidence=baseline_confidence,
    )
    if scene_id and scene_id in scene_templates:
        return scene_id, scene_templates[scene_id]
    return None, None


async def _aresolve_scene_for_span(
    span_text: str,
    *,
    llm: Any,
    scene_templates: dict[str, dict[str, Any]],
    baseline_scene: str | None = None,
    baseline_confidence: float = 0.0,
) -> tuple[str | None, dict[str, Any] | None]:
    ranked = _rank_scene_candidates(span_text, scene_templates)
    if ranked:
        top_scene_id, top_config, top_score, _top_raw_score = ranked[0]
        second_score = ranked[1][2] if len(ranked) > 1 else 0.0
        if top_score >= policy_float("thresholds", "scene_match_min_score") and (
            len(ranked) == 1 or top_score - second_score >= policy_float("thresholds", "multi_scene_score_gap")
        ):
            return top_scene_id, top_config

        candidate_scene_ids = [
            scene_id
            for scene_id, _config, score, _raw_score in ranked[: int(policy_value("thresholds", "multi_scene_max_count", default=2)) + 1]
            if score > 0
        ]
        candidate_templates = {
            scene_id: scene_templates[scene_id]
            for scene_id in candidate_scene_ids
            if scene_id in scene_templates
        }
        if candidate_templates:
            scene_id = await _arecognize_scene_with_llm(
                span_text,
                llm=llm,
                scene_templates=candidate_templates,
                baseline_scene=baseline_scene,
                baseline_confidence=max(baseline_confidence, top_score),
            )
            if scene_id and scene_id in candidate_templates:
                return scene_id, candidate_templates[scene_id]
        return top_scene_id, top_config

    scene_id = await _arecognize_scene_with_llm(
        span_text,
        llm=llm,
        scene_templates=scene_templates,
        baseline_scene=baseline_scene,
        baseline_confidence=baseline_confidence,
    )
    if scene_id and scene_id in scene_templates:
        return scene_id, scene_templates[scene_id]
    return None, None


def _rank_scene_candidates(
    query: str,
    scene_templates: dict[str, dict[str, Any]],
) -> list[tuple[str, dict[str, Any], float, float]]:
    query_compact = _compact(query)
    scored: list[tuple[str, dict[str, Any], float, float]] = []
    for scene_id, config in scene_templates.items():
        score, raw_score = _score_scene(query_compact, config)
        if score > 0:
            scored.append((scene_id, config, score, raw_score))
    scored.sort(key=lambda item: (item[2], item[3]), reverse=True)
    return scored


def _score_scene(query_compact: str, config: dict[str, Any]) -> tuple[float, float]:
    terms = _scene_terms(config)
    raw_score = 0.0
    matched = 0
    for term, weight in terms.items():
        if term and term in query_compact:
            matched += 1
            raw_score += weight
    if not matched:
        return 0.0, 0.0
    raw_score += _scene_penalty(query_compact, config)
    if raw_score <= 0:
        return 0.0, raw_score
    score = min(
        policy_float("thresholds", "scene_match_max_score"),
        policy_float("thresholds", "scene_match_base_score")
        + raw_score / policy_float("thresholds", "scene_match_score_divisor"),
    )
    return round(score, 2), raw_score


def _extract_scene_params_with_llm(
    query: str,
    *,
    llm: Any,
    scene_config: dict[str, Any],
) -> dict[str, Any]:
    prompt = _slot_update_prompt(scene_config, query)
    raw = _invoke_text(llm, prompt, query)
    return _parse_name_value_json(raw)


async def _aextract_scene_params_with_llm(
    query: str,
    *,
    llm: Any,
    scene_config: dict[str, Any],
) -> dict[str, Any]:
    prompt = _slot_update_prompt(scene_config, query)
    raw = await _ainvoke_text(llm, prompt, query)
    return _parse_name_value_json(raw)


def _slot_update_prompt(scene_config: dict[str, Any], query: str) -> str:
    scene_name = scene_config.get("name") or "业务场景"
    dynamic_example = scene_config.get("example") or '答：{"name":"xx","value":"xx"}'
    slot_template = _slot_template(scene_config)
    current_date = datetime.now().strftime("%Y-%m-%d")
    return _render_policy_prompt(
        "slot_update",
        scene_name=scene_name,
        current_date=current_date,
        dynamic_example=dynamic_example,
        slot_template_json=json.dumps(slot_template, ensure_ascii=False),
        query=query,
    )


def _slot_template(scene_config: dict[str, Any]) -> list[dict[str, Any]]:
    slot: list[dict[str, Any]] = []
    for param in scene_config.get("parameters", []) or []:
        if not isinstance(param, dict):
            continue
        slot.append({
            "name": param.get("name", ""),
            "desc": param.get("desc", ""),
            "type": param.get("type", "string"),
            "required": bool(param.get("required", False)),
            "value": "",
        })
    return slot


def _parse_name_value_json(raw: str) -> dict[str, Any]:
    parsed = _extract_json_value(raw)
    items: list[Any]
    if isinstance(parsed, list):
        items = parsed
    elif isinstance(parsed, dict):
        items = [parsed]
    else:
        items = []

    params: dict[str, Any] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        value = item.get("value")
        if isinstance(name, str) and value not in (None, ""):
            params[name] = value
    return params


def _extract_json_value(raw: str) -> Any:
    text = (raw or "").strip()
    if not text:
        return None
    candidates = [
        text,
        _strip_code_fence(text),
        *_json_spans(text),
    ]
    for candidate in candidates:
        if not candidate:
            continue
        fixed = candidate.replace("'", '"')
        try:
            return json.loads(fixed)
        except json.JSONDecodeError:
            continue
    return None


def _strip_code_fence(text: str) -> str:
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = re.sub(r"^```(?:json)?", "", stripped, flags=re.IGNORECASE).strip()
        stripped = re.sub(r"```$", "", stripped).strip()
    return stripped


def _json_spans(text: str) -> list[str]:
    spans: list[str] = []
    for start_char, end_char in (("[", "]"), ("{", "}")):
        start = text.find(start_char)
        end = text.rfind(end_char)
        if start != -1 and end != -1 and end > start:
            spans.append(text[start:end + 1])
    return spans


def _rewrite_question_with_llm(
    query: str,
    *,
    llm: Any,
    scene_config: dict[str, Any],
    params: dict[str, Any],
) -> str:
    prompt = _rewrite_prompt(query, scene_config=scene_config, params=params)
    raw = _invoke_text(llm, prompt, query)
    rewritten = raw.strip().strip('"').strip("'")
    if _is_file_prep_rewrite(rewritten, query):
        return query
    return rewritten


async def _arewrite_question_with_llm(
    query: str,
    *,
    llm: Any,
    scene_config: dict[str, Any],
    params: dict[str, Any],
) -> str:
    prompt = _rewrite_prompt(query, scene_config=scene_config, params=params)
    raw = await _ainvoke_text(llm, prompt, query)
    rewritten = raw.strip().strip('"').strip("'")
    if _is_file_prep_rewrite(rewritten, query):
        return query
    return rewritten


def _is_file_prep_rewrite(rewritten: str, original_query: str) -> bool:
    """Detect LLM rewrites that replaced the real task with file-reading prep."""
    normalized = re.sub(r"\s+", "", rewritten or "")
    if not normalized:
        return False

    prep_terms = (
        "读取所有相关文件",
        "读取全部相关文件",
        "读取所有文件",
        "读取全部文件",
        "先读取",
        "先查看",
        "加载",
        "获取完整数据",
        "获取完整资料",
    )
    if not any(term in normalized for term in prep_terms):
        return False

    original = original_query or ""
    business_terms = (
        "分析", "报告", "合规", "法规", "政策", "台账", "图表",
        "可视化", "风险", "判断", "生成", "统计", "依据", "督办",
    )
    return any(term in original for term in business_terms)


def _rewrite_prompt(query: str, *, scene_config: dict[str, Any], params: dict[str, Any]) -> str:
    scene_name = scene_config.get("name") or "业务场景"
    return _render_policy_prompt(
        "rewrite_question",
        scene_name=scene_name,
        params_json=json.dumps(params, ensure_ascii=False),
        query=query,
    )


def _rewrite_non_builtin_question(
    query: str,
    baseline: RoutingIntentResult,
    *,
    llm: Any,
) -> RoutingIntentResult:
    # Keep routing_query conservative for non-built-in scenes.  The rewrite is
    # exposed as normalized_query only; SkillRouter still searches the original
    # task text to avoid over-normalizing open-domain requests.
    try:
        raw = _invoke_text(
            llm,
            _policy_prompt_template("rewrite_non_builtin"),
            query,
        ).strip()
    except Exception:
        raw = ""
    if raw:
        baseline.normalized_query = raw
        baseline.reason = "llm_other_scene_rewrite"
    return baseline


async def _arewrite_non_builtin_question(
    query: str,
    baseline: RoutingIntentResult,
    *,
    llm: Any,
) -> RoutingIntentResult:
    try:
        raw = (await _ainvoke_text(
            llm,
            _policy_prompt_template("rewrite_non_builtin"),
            query,
        )).strip()
    except Exception:
        raw = ""
    if raw:
        baseline.normalized_query = raw
        baseline.reason = "llm_other_scene_rewrite"
    return baseline


def _extract_float(text: str) -> float:
    found = re.findall(r"-?\d+(?:\.\d+)?", text or "")
    if not found:
        return 0.0
    try:
        return float(found[0])
    except ValueError:
        return 0.0


def _compact(text: str) -> str:
    return re.sub(r"\s+", "", _normalize(text))


def _policy_prompt_template(name: str) -> str:
    template = policy_value("prompts", name, default="")
    return str(template) if template else ""


def _render_policy_prompt(name: str, **values: Any) -> str:
    return _policy_prompt_template(name).format(**{key: "" if value is None else value for key, value in values.items()})


def _capability_inventory_confidence(query: str) -> float:
    compact = _compact(query)
    if any(_compact(str(pattern)) in compact for pattern in policy_list("capability_inventory", "phrase_patterns")):
        return policy_float("capability_inventory", "exact_match_confidence")

    normalized = _normalize(query)
    if any(str(pattern) in normalized for pattern in policy_list("capability_inventory", "english_patterns")):
        return policy_float("capability_inventory", "exact_match_confidence")

    # Broader Chinese fallback: ability/capability questions without a concrete
    # task should inventory authorized skills instead of returning no-skill.
    subject_terms = [str(term) for term in policy_list("capability_inventory", "chinese_subject_terms")]
    question_terms = [str(term) for term in policy_list("capability_inventory", "chinese_question_terms")]
    if any(term in compact for term in subject_terms) and any(term in compact for term in question_terms):
        return policy_float("capability_inventory", "broad_match_confidence")
    return 0.0


def _find_mentioned_skill_ids(query: str, available_skill_ids: list[str]) -> list[str]:
    normalized_query = _normalize(query)
    compact_query = _compact(query)
    mentioned: list[str] = []
    for skill_id in available_skill_ids:
        aliases = {
            skill_id,
            skill_id.replace("-", " "),
            skill_id.replace("_", " "),
        }
        for alias in aliases:
            normalized_alias = _normalize(alias)
            compact_alias = _compact(alias)
            if len(normalized_alias) >= 4 and (
                normalized_alias in normalized_query or compact_alias in compact_query
            ):
                mentioned.append(skill_id)
                break
    return sorted(set(mentioned))


def _match_scene(
    query: str,
    scene_templates: dict[str, dict[str, Any]],
) -> tuple[str | None, dict[str, Any] | None, float]:
    if not scene_templates:
        return None, None, 0.0

    best_scene: str | None = None
    best_config: dict[str, Any] | None = None
    best_score = 0.0
    best_raw_score = 0.0
    query_compact = _compact(query)

    for scene_id, config in scene_templates.items():
        terms = _scene_terms(config)
        if not terms:
            continue

        raw_score = 0.0
        matched = 0
        for term, weight in terms.items():
            if term and term in query_compact:
                matched += 1
                raw_score += weight

        score = 0.0
        if matched:
            raw_score += _scene_penalty(query_compact, config)
            if raw_score <= 0:
                continue
            # Normalize enough to compare scenes, but retain a high confidence
            # when exact option/example terms match.
            score = min(
                policy_float("thresholds", "scene_match_max_score"),
                policy_float("thresholds", "scene_match_base_score")
                + raw_score / policy_float("thresholds", "scene_match_score_divisor"),
            )

        if score > best_score or (score == best_score and matched and score and score > 0 and raw_score > best_raw_score):
            best_scene = scene_id
            best_config = config
            best_score = score
            best_raw_score = raw_score

    if best_score < policy_float("thresholds", "scene_match_min_score"):
        return None, None, 0.0
    return best_scene, best_config, round(best_score, 2)


def _scene_terms(config: dict[str, Any]) -> dict[str, float]:
    terms: dict[str, float] = {}

    def add(value: Any, weight: float, *, expand_cjk: bool = True) -> None:
        if not isinstance(value, str):
            return
        for term in _extract_terms(value, expand_cjk=expand_cjk):
            terms[term] = max(terms.get(term, 0.0), weight)

    add(config.get("name"), 2.5)
    add(config.get("description"), 1.5)
    add(config.get("example"), 1.0, expand_cjk=False)
    for keyword in config.get("keywords", []) or []:
        add(keyword, 2.7, expand_cjk=False)
    for positive_example in config.get("positive_examples", []) or []:
        add(positive_example, 1.2, expand_cjk=False)

    for param in config.get("parameters", []) or []:
        if not isinstance(param, dict):
            continue
        add(param.get("name"), 1.2)
        add(param.get("desc"), 0.9)
        for option in param.get("options", []) or []:
            add(option, 2.0)

    return terms


def _scene_penalty(query_compact: str, config: dict[str, Any]) -> float:
    penalty = 0.0
    for anti_keyword in config.get("anti_keywords", []) or []:
        term = _compact(str(anti_keyword))
        if term and term in query_compact:
            penalty -= 3.0
    for negative_example in config.get("negative_examples", []) or []:
        for term in _extract_terms(str(negative_example), expand_cjk=False):
            if term and term in query_compact:
                penalty -= 0.8
    return penalty


def _normalize_scene_list(scene_ids: list[str]) -> list[str]:
    normalized: list[str] = []
    for scene_id in scene_ids:
        if isinstance(scene_id, str) and scene_id.strip() and scene_id.strip() not in normalized:
            normalized.append(scene_id.strip())
    return normalized


def _build_intent_scene_task(
    scene_id: str,
    scene_config: dict[str, Any],
    text: str,
    params: dict[str, Any],
) -> SubTask:
    return SubTask(
        scene=scene_id,
        scene_name=str(scene_config.get("name") or scene_id),
        text=text.strip(),
        params=params,
    )


def _extract_terms(text: str, *, expand_cjk: bool = True) -> set[str]:
    normalized = _compact(text)
    terms: set[str] = set()

    # Chinese and mixed-language tokens split on punctuation-like chars.
    for token in re.split(r"[，。；、：:,.!?！？\[\]\{\}'\"（）()\s]+", normalized):
        token = token.strip()
        stop_terms = _cjk_stop_terms()
        if len(token) >= 2 and token not in stop_terms:
            terms.add(token)
            if expand_cjk:
                terms.update(_extract_cjk_ngrams(token))

    # English terms.
    for token in re.findall(r"[a-z][a-z0-9_ -]{2,}", text.lower()):
        token = _normalize(token)
        if len(token) >= policy_int("term_extraction", "english_min_length"):
            terms.add(token.replace(" ", ""))

    return terms


def _cjk_stop_terms() -> set[str]:
    return {str(term) for term in policy_list("term_extraction", "cjk_stop_terms")}


def _extract_cjk_ngrams(token: str) -> set[str]:
    """Extract short Chinese key phrases from long JSON descriptions."""
    grams: set[str] = set()
    min_sequence_length = policy_int("term_extraction", "cjk_min_sequence_length")
    min_ngram = policy_int("term_extraction", "cjk_min_ngram")
    max_ngram = policy_int("term_extraction", "cjk_max_ngram")
    stop_terms = _cjk_stop_terms()
    for seq in re.findall(rf"[\u4e00-\u9fff]{{{min_sequence_length},}}", token):
        max_n = min(max_ngram, len(seq))
        for n in range(min_ngram, max_n + 1):
            for i in range(0, len(seq) - n + 1):
                gram = seq[i:i + n]
                if gram not in stop_terms:
                    grams.add(gram)
    return grams


def _empty_params(scene_config: dict[str, Any]) -> dict[str, Any]:
    params: dict[str, Any] = {}
    for param in scene_config.get("parameters", []) or []:
        if isinstance(param, dict) and param.get("name"):
            params[str(param["name"])] = ""
    return params


def _build_task_hints(scene_config: dict[str, Any]) -> list[str]:
    hints: list[str] = []
    for param in scene_config.get("parameters", []) or []:
        if not isinstance(param, dict) or not param.get("name"):
            continue
        required = "必填" if param.get("required") else "可选"
        desc = param.get("desc") or ""
        hints.append(f"提取{required}参数：{param['name']}。{desc}")
    return hints


def _build_scene_routing_query(query: str, scene_config: dict[str, Any]) -> str:
    scene_name = str(scene_config.get("name") or "业务场景")
    scene_id = str(scene_config.get("scene") or "").strip()
    description = str(scene_config.get("description") or "")
    param_names = [
        str(param.get("name"))
        for param in scene_config.get("parameters", []) or []
        if isinstance(param, dict) and param.get("name")
    ]
    param_text = "、".join(param_names)
    parts = [f"场景：{scene_name}"]
    if scene_id:
        parts.append(f"场景ID：{scene_id}")
    if description:
        parts.append(f"场景说明：{description}")
    if param_text:
        parts.append(f"需要识别参数：{param_text}")
    parts.append(f"用户原始问题：{query.strip()}")
    return _dedupe_original_query_markers("。".join(parts))


def _append_original_query_once(routing_query: str, query: str) -> str:
    candidate = f"{routing_query.rstrip('。')}。用户原始问题：{query.strip()}"
    return _dedupe_original_query_markers(candidate)


def _dedupe_original_query_markers(text: str) -> str:
    marker = "用户原始问题："
    if marker not in text:
        return text

    prefix, first_tail = text.split(marker, 1)
    first_tail = first_tail.strip().strip("。")
    if not first_tail:
        return prefix.rstrip("。")

    # Keep only one "用户原始问题" marker.
    first_tail = first_tail.split(marker, 1)[0].strip().strip("。")
    return f"{prefix.rstrip('。')}。{marker}{first_tail}"
