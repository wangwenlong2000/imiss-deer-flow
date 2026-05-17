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
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

from langchain_core.messages import HumanMessage, SystemMessage
from pydantic import BaseModel, Field

from deerflow.routing.query_segmenter import is_obvious_chitchat


RoutingIntent = Literal[
    "chitchat",
    "capability_inventory",
    "task",
    "explicit_skill_request",
]


class RoutingIntentResult(BaseModel):
    intent: RoutingIntent
    original_query: str
    normalized_query: str
    routing_query: str
    scene: str | None = None
    scene_name: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
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
    original_query = query or ""
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
        public_scene_id = _public_scene_id(scene_id, scene_config)
        routing_query = _build_scene_routing_query(original_query, scene_config)
        return RoutingIntentResult(
            intent=intent,
            original_query=original_query,
            normalized_query=normalized_query,
            routing_query=routing_query,
            scene=public_scene_id,
            scene_name=str(scene_config.get("name") or scene_id),
            params=_empty_params(scene_config),
            task_hints=_build_task_hints(scene_config),
            mentioned_skill_ids=mentioned_skill_ids,
            confidence=scene_score,
            reason="matched_configured_scene",
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
        scene_id = related_scene or _recognize_scene_with_llm(query, llm=llm, scene_templates=templates)
        if not scene_id:
            return _rewrite_non_builtin_question(query, baseline, llm=llm)

        scene_config = templates.get(scene_id)
        if not scene_config:
            return baseline

        public_scene_id = _public_scene_id(scene_id, scene_config)
        params = _extract_scene_params_with_llm(query, llm=llm, scene_config=scene_config)
        rewritten = _rewrite_question_with_llm(query, llm=llm, scene_config=scene_config, params=params)
        routing_query = _build_scene_routing_query(rewritten or query, scene_config)
        if rewritten and rewritten.strip() != query.strip():
            routing_query = _append_original_query_once(routing_query, query)

        return RoutingIntentResult(
            intent=baseline.intent,
            original_query=query,
            normalized_query=_normalize(query),
            routing_query=routing_query,
            scene=public_scene_id,
            scene_name=str(scene_config.get("name") or scene_id),
            params=params or _empty_params(scene_config),
            task_hints=_build_task_hints(scene_config),
            mentioned_skill_ids=baseline.mentioned_skill_ids,
            confidence=0.9,
            reason="llm_scene_slot_rewrite",
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
        scene_id = related_scene or await _arecognize_scene_with_llm(query, llm=llm, scene_templates=templates)
        if not scene_id:
            return await _arewrite_non_builtin_question(query, baseline, llm=llm)

        scene_config = templates.get(scene_id)
        if not scene_config:
            return baseline

        public_scene_id = _public_scene_id(scene_id, scene_config)
        params = await _aextract_scene_params_with_llm(query, llm=llm, scene_config=scene_config)
        rewritten = await _arewrite_question_with_llm(query, llm=llm, scene_config=scene_config, params=params)
        routing_query = _build_scene_routing_query(rewritten or query, scene_config)
        if rewritten and rewritten.strip() != query.strip():
            routing_query = _append_original_query_once(routing_query, query)

        return RoutingIntentResult(
            intent=baseline.intent,
            original_query=query,
            normalized_query=_normalize(query),
            routing_query=routing_query,
            scene=public_scene_id,
            scene_name=str(scene_config.get("name") or scene_id),
            params=params or _empty_params(scene_config),
            task_hints=_build_task_hints(scene_config),
            mentioned_skill_ids=baseline.mentioned_skill_ids,
            confidence=0.9,
            reason="llm_scene_slot_rewrite",
        )
    except Exception:
        return baseline


def _normalize(text: str) -> str:
    lowered = text.lower().replace("_", " ").replace("-", " ")
    return re.sub(r"\s+", " ", lowered).strip()


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
    if _extract_float(raw) >= 0.72:
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
    if _extract_float(raw) >= 0.72:
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
    return (
        "判断当前用户输入内容与上一轮业务场景的关联性。"
        "只输出 0.0 到 1.0 的小数，不要输出解释。\n\n"
        f"上一轮业务场景：{scene_config.get('name')}\n"
        f"场景说明：{scene_config.get('description')}\n"
        f"当前用户输入：{query}"
    )


def _recognize_scene_with_llm(
    query: str,
    *,
    llm: Any,
    scene_templates: dict[str, dict[str, Any]],
) -> str | None:
    options, option_map = _scene_options(scene_templates)
    system_prompt = (
        "有下面多种场景，需要你根据用户输入进行判断，只答选项序号，不要解释。\n"
        f"{options}\n"
        "0. 其他场景 - 请回复0\n"
        f"用户输入：{query}\n"
        "请回复序号："
    )
    raw = _invoke_text(llm, system_prompt, query)
    return _parse_scene_choice(raw, option_map)


async def _arecognize_scene_with_llm(
    query: str,
    *,
    llm: Any,
    scene_templates: dict[str, dict[str, Any]],
) -> str | None:
    options, option_map = _scene_options(scene_templates)
    system_prompt = (
        "有下面多种场景，需要你根据用户输入进行判断，只答选项序号，不要解释。\n"
        f"{options}\n"
        "0. 其他场景 - 请回复0\n"
        f"用户输入：{query}\n"
        "请回复序号："
    )
    raw = await _ainvoke_text(llm, system_prompt, query)
    return _parse_scene_choice(raw, option_map)


def _scene_options(scene_templates: dict[str, dict[str, Any]]) -> tuple[str, dict[str, str]]:
    lines: list[str] = []
    option_map: dict[str, str] = {}
    for idx, (scene_id, config) in enumerate(scene_templates.items(), 1):
        option = str(idx)
        option_map[option] = scene_id
        name = config.get("name") or scene_id
        description = config.get("description") or ""
        lines.append(f"{idx}. {name} - {description} - 请回复{idx}")
    return "\n".join(lines), option_map


def _parse_scene_choice(raw: str, option_map: dict[str, str]) -> str | None:
    digits = re.findall(r"\d+", raw or "")
    if not digits:
        return None
    choice = digits[0]
    if choice == "0":
        return None
    return option_map.get(choice)


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
    return (
        "你是一个信息抽取机器人。\n"
        f"当前问答场景是：【{scene_name}】\n"
        f"当前日期是：{current_date}\n\n"
        "JSON中每个元素代表一个参数信息：\n"
        "name是参数名称；desc是参数注释；required代表参数是否必须。\n\n"
        "需求：\n"
        "#01 根据用户输入内容提取有用的信息到 value 值，严格提取，没有提及就丢弃该元素。\n"
        "#02 返回 JSON 数组，数组元素只包含 name 和 value。\n"
        "#03 如果涉及日期，用户提及今天，则使用当前日期补全；没有提及日期，则不补全。\n"
        "#04 不要输出解释，不要使用 Markdown。\n\n"
        f"返回样例：\n{dynamic_example}\n\n"
        f"JSON：{json.dumps(slot_template, ensure_ascii=False)}\n"
        f"输入：{query}\n"
        "答："
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
    return raw.strip().strip('"').strip("'")


async def _arewrite_question_with_llm(
    query: str,
    *,
    llm: Any,
    scene_config: dict[str, Any],
    params: dict[str, Any],
) -> str:
    prompt = _rewrite_prompt(query, scene_config=scene_config, params=params)
    raw = await _ainvoke_text(llm, prompt, query)
    return raw.strip().strip('"').strip("'")


def _rewrite_prompt(query: str, *, scene_config: dict[str, Any], params: dict[str, Any]) -> str:
    scene_name = scene_config.get("name") or "业务场景"
    return (
        "请根据以下内容，以第一人称总结为一句可执行任务描述。"
        "不要遗漏信息，不要扩展，不要解释，不要使用 Markdown。\n\n"
        f"场景：{scene_name}\n"
        f"已提取参数：{json.dumps(params, ensure_ascii=False)}\n"
        f"用户输入：{query}"
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
            "请将用户输入改写为一句清晰、完整、可执行的任务描述。不要扩展，不要解释。",
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
            "请将用户输入改写为一句清晰、完整、可执行的任务描述。不要扩展，不要解释。",
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


def _capability_inventory_confidence(query: str) -> float:
    compact = _compact(query)
    phrase_patterns = (
        "列出你的全部skill",
        "列出全部skill",
        "全部skill",
        "所有skill",
        "可用skill",
        "技能列表",
        "列出你的全部技能",
        "列出全部技能",
        "全部技能",
        "所有技能",
        "可用技能",
        "你具备哪些能力",
        "你有哪些能力",
        "你有什么能力",
        "你能做什么",
        "你会什么",
        "有哪些能力",
        "能力列表",
        "当前能力",
        "可用能力",
    )
    if any(_compact(pattern) in compact for pattern in phrase_patterns):
        return 1.0

    normalized = _normalize(query)
    english_patterns = (
        "list all skills",
        "show all skills",
        "available skills",
        "what can you do",
        "what are your capabilities",
        "your capabilities",
    )
    if any(pattern in normalized for pattern in english_patterns):
        return 1.0

    # Broader Chinese fallback: ability/capability questions without a concrete
    # task should inventory authorized skills instead of returning no-skill.
    if ("能力" in compact or "功能" in compact) and any(token in compact for token in ("哪些", "什么", "所有", "全部", "介绍")):
        return 0.85
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
    query_compact = _compact(query)

    for scene_id, config in scene_templates.items():
        terms = _scene_terms(config)
        if not terms:
            continue

        score = 0.0
        matched = 0
        for term, weight in terms.items():
            if term and term in query_compact:
                matched += 1
                score += weight

        if matched:
            # Normalize enough to compare scenes, but retain a high confidence
            # when exact option/example terms match.
            score = min(0.95, 0.45 + score / 10.0)

        if score > best_score:
            best_scene = scene_id
            best_config = config
            best_score = score

    if best_score < 0.62:
        return None, None, 0.0
    return best_scene, best_config, round(best_score, 2)


def _scene_terms(config: dict[str, Any]) -> dict[str, float]:
    terms: dict[str, float] = {}

    def add(value: Any, weight: float) -> None:
        if not isinstance(value, str):
            return
        for term in _extract_terms(value):
            terms[term] = max(terms.get(term, 0.0), weight)

    add(config.get("name"), 2.5)
    add(config.get("description"), 1.5)
    add(config.get("example"), 1.0)

    for param in config.get("parameters", []) or []:
        if not isinstance(param, dict):
            continue
        add(param.get("name"), 1.2)
        add(param.get("desc"), 0.9)
        for option in param.get("options", []) or []:
            add(option, 2.0)

    return terms


def _extract_terms(text: str) -> set[str]:
    normalized = _compact(text)
    terms: set[str] = set()

    # Chinese and mixed-language tokens split on punctuation-like chars.
    for token in re.split(r"[，。；、：:,.!?！？\[\]\{\}'\"（）()\s]+", normalized):
        token = token.strip()
        if len(token) >= 2:
            terms.add(token)

    # English terms.
    for token in re.findall(r"[a-z][a-z0-9_ -]{2,}", text.lower()):
        token = _normalize(token)
        if len(token) >= 3:
            terms.add(token.replace(" ", ""))

    return terms


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
    description = str(scene_config.get("description") or "")
    param_names = [
        str(param.get("name"))
        for param in scene_config.get("parameters", []) or []
        if isinstance(param, dict) and param.get("name")
    ]
    param_text = "、".join(param_names)
    parts = [f"场景：{scene_name}"]
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
