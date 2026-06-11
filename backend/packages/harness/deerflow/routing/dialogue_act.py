"""Dialogue-act classification before business intent routing.

This module handles conversational control turns, such as answers to a
clarification question, before the SkillRouter attempts business scene
recognition.  The router should only classify new tasks.
"""

from __future__ import annotations

import re
import json
from dataclasses import dataclass, field
from typing import Any, Literal


DialogueAct = Literal[
    "clarification_answer",
    "confirmation",
    "parameter_update",
    "task_followup",
    "new_task",
    "chitchat",
]


@dataclass
class DialogueActResult:
    act: DialogueAct
    confidence: float
    reason: str
    parsed_answer: Any = None
    pending_action_id: str | None = None
    resume_intent_context: dict[str, Any] | None = None
    resume_routing_context: dict[str, Any] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


_OPTION_NUMBER_RE = re.compile(
    r"^\s*(?:选|选择|第)?\s*([1-9]\d?|[A-Da-d]|[一二三四五六七八九十])\s*(?:个|项|种|方案|）|\)|\.|。)?\s*$"
)
_OPTION_REFERENCE_RE = re.compile(
    r"(?:用|按|选|选择|第)\s*([1-9]\d?|[A-Da-d]|[一二三四五六七八九十])\s*(?:个|项|种|方案)?"
)
_AFFIRMATIVE_RE = re.compile(r"^\s*(是|对|确认|继续|可以|好的|好|ok|yes|y|同意|执行)\s*$", re.IGNORECASE)
_NEGATIVE_RE = re.compile(r"^\s*(否|不|取消|不用|不要|no|n|stop|停止)\s*$", re.IGNORECASE)
_PREVIOUS_TASK_REFERENCE_RE = re.compile(
    r"(这次|刚才|刚刚|上一(?:个|次|轮)?|上个|前面|之前|刚才那次|这个任务|这个结果|这个分析|这个报告|为什么不|为什么没有|为啥不|为啥没有|怎么没|怎么没有)"
)
_FOLLOWUP_ACTION_RE = re.compile(
    r"(为什么|为啥|原因|解释|复盘|继续|接着|补充|修改|改成|换成|重新|再|用|不用|直接使用|脚本|skill|技能|结果|报告|输出)"
)


def classify_dialogue_act(
    query: str,
    *,
    pending_action: dict[str, Any] | None = None,
    previous_intent_context: dict[str, Any] | None = None,
    previous_routing_context: dict[str, Any] | None = None,
    llm: Any | None = None,
) -> DialogueActResult:
    """Classify whether *query* is dialogue control or a new task.

    The primary signal is explicit state: when a pending action exists, the
    user turn is interpreted against that action's expected answer schema.
    Regexes are only parsers for the pending schema, not business routing
    rules.  If deterministic parsing fails, an optional LLM can judge whether
    the reply addresses the pending question.
    """
    text = (query or "").strip()
    if not text:
        return DialogueActResult(act="chitchat", confidence=1.0, reason="empty_query")

    if isinstance(pending_action, dict) and pending_action:
        parsed = _parse_pending_action_answer(text, pending_action)
        if parsed["matched"]:
            if (
                parsed.get("reason") == "parameter_update_for_pending_action"
                and pending_action.get("options")
                and llm is not None
            ):
                parsed_llm = _parse_pending_action_with_llm(text, pending_action, llm)
                if parsed_llm["matched"]:
                    parameter_updates = parsed_llm.get("parameter_updates") or {}
                    act = "parameter_update" if parameter_updates and parsed_llm.get("answer") is None else "clarification_answer"
                    metadata = _build_dialogue_metadata(
                        pending_action=pending_action,
                        parameter_updates=parameter_updates,
                        raw_answer=text,
                        conflicts=parsed_llm.get("conflicts") or [],
                        selected_answer=parsed_llm.get("answer"),
                    )
                    return DialogueActResult(
                        act=act,
                        confidence=parsed_llm.get("confidence", 0.78),
                        reason=parsed_llm.get("reason", "llm_structured_pending_action_reply"),
                        parsed_answer=parsed_llm.get("answer") if parsed_llm.get("answer") is not None else text,
                        pending_action_id=pending_action.get("id"),
                        resume_intent_context=_dict_or_none(pending_action.get("resume_intent_context")),
                        resume_routing_context=_dict_or_none(pending_action.get("resume_routing_context")),
                        metadata=metadata,
                    )
            act: DialogueAct = "parameter_update" if parsed.get("parameter_updates") and not parsed.get("answer") else "clarification_answer"
            metadata = _build_dialogue_metadata(
                pending_action=pending_action,
                parameter_updates=parsed.get("parameter_updates") or {},
                raw_answer=text,
                conflicts=parsed.get("conflicts") or [],
                selected_answer=parsed.get("answer"),
            )
            return DialogueActResult(
                act=act,
                confidence=parsed["confidence"],
                reason=parsed["reason"],
                parsed_answer=parsed.get("answer"),
                pending_action_id=pending_action.get("id"),
                resume_intent_context=_dict_or_none(pending_action.get("resume_intent_context")),
                resume_routing_context=_dict_or_none(pending_action.get("resume_routing_context")),
                metadata=metadata,
            )

        if llm is not None:
            parsed_llm = _parse_pending_action_with_llm(text, pending_action, llm)
            if parsed_llm["matched"]:
                parameter_updates = parsed_llm.get("parameter_updates") or {}
                act = "parameter_update" if parameter_updates and parsed_llm.get("answer") is None else "clarification_answer"
                metadata = _build_dialogue_metadata(
                    pending_action=pending_action,
                    parameter_updates=parameter_updates,
                    raw_answer=text,
                    conflicts=parsed_llm.get("conflicts") or [],
                    selected_answer=parsed_llm.get("answer"),
                )
                return DialogueActResult(
                    act=act,
                    confidence=parsed_llm.get("confidence", 0.78),
                    reason=parsed_llm.get("reason", "llm_structured_pending_action_reply"),
                    parsed_answer=parsed_llm.get("answer") if parsed_llm.get("answer") is not None else text,
                    pending_action_id=pending_action.get("id"),
                    resume_intent_context=_dict_or_none(pending_action.get("resume_intent_context")),
                    resume_routing_context=_dict_or_none(pending_action.get("resume_routing_context")),
                    metadata=metadata,
                )

        if llm is not None and _looks_like_reply_to_pending_question(text, pending_action, llm):
            metadata = _build_dialogue_metadata(
                pending_action=pending_action,
                parameter_updates={},
                raw_answer=text,
                conflicts=[],
                selected_answer=text,
            )
            return DialogueActResult(
                act="clarification_answer",
                confidence=0.75,
                reason="llm_pending_action_reply",
                parsed_answer=text,
                pending_action_id=pending_action.get("id"),
                resume_intent_context=_dict_or_none(pending_action.get("resume_intent_context")),
                resume_routing_context=_dict_or_none(pending_action.get("resume_routing_context")),
                metadata=metadata,
            )

    if is_option_like_reply(text):
        return DialogueActResult(act="chitchat", confidence=0.9, reason="option_reply_without_pending_action")

    if llm is not None:
        turn_decision = _classify_previous_task_turn_with_llm(
            text,
            previous_intent_context=previous_intent_context,
            previous_routing_context=previous_routing_context,
            llm=llm,
        )
        if turn_decision is not None:
            return turn_decision

    if _looks_like_previous_task_followup(text, previous_intent_context, previous_routing_context):
        return DialogueActResult(
            act="task_followup",
            confidence=0.86,
            reason="fallback_previous_task_followup",
            parsed_answer=text,
            resume_intent_context=_dict_or_none(previous_intent_context),
            resume_routing_context=_dict_or_none(previous_routing_context),
            metadata={"raw_answer": text},
        )

    if _AFFIRMATIVE_RE.match(text):
        return DialogueActResult(act="confirmation", confidence=0.8, reason="affirmative_reply")
    if _NEGATIVE_RE.match(text):
        return DialogueActResult(act="confirmation", confidence=0.8, reason="negative_reply")

    return DialogueActResult(act="new_task", confidence=0.7, reason="no_pending_action_match")


def is_option_like_reply(query: str) -> bool:
    """Return True for compact option replies like ``1`` or ``选 A``."""
    text = (query or "").strip()
    if len(text) > 8:
        return False
    return bool(_OPTION_NUMBER_RE.match(text))


def _looks_like_previous_task_followup(
    text: str,
    previous_intent_context: dict[str, Any] | None,
    previous_routing_context: dict[str, Any] | None,
) -> bool:
    if not isinstance(previous_intent_context, dict) and not isinstance(previous_routing_context, dict):
        return False

    compact = _compact(text)
    if not compact:
        return False

    if _PREVIOUS_TASK_REFERENCE_RE.search(compact) and _FOLLOWUP_ACTION_RE.search(compact):
        return True

    # Short imperative continuations are often follow-ups, but avoid swallowing
    # long, fully specified new business tasks.
    if len(compact) <= 32 and re.search(r"(继续|接着|再来|补充|改成|换成|重新输出|重新生成|不用|用刚才|按刚才)", compact):
        return True

    return False


def _classify_previous_task_turn_with_llm(
    text: str,
    *,
    previous_intent_context: dict[str, Any] | None,
    previous_routing_context: dict[str, Any] | None,
    llm: Any,
) -> DialogueActResult | None:
    if not isinstance(previous_intent_context, dict) and not isinstance(previous_routing_context, dict):
        return None

    previous_summary = _build_previous_task_summary(previous_intent_context, previous_routing_context)
    if not previous_summary:
        return None

    prompt = (
        "你是对话轮次决策器，只判断当前用户消息是否需要重新做业务意图识别和 skill 路由。\n"
        "不要判断具体业务场景，不要选择 skill，只输出 JSON 对象。\n\n"
        "可选 act：\n"
        "- task_followup: 当前消息只是追问、解释、复盘、继续输出、修改格式、补充说明或评价上一轮任务/结果/执行过程，不需要重新选择业务场景或 skill。\n"
        "- new_task: 当前消息提出了新的处理目标、分析动作、判断维度、输出要求，或需要在上一轮结果基础上重新选择业务场景、工具、Skill 或 RAG 能力。\n"
        "- chitchat: 闲聊、感谢、无任务内容。\n"
        "- confirmation: 简短确认/否认，但没有 pending_action 可绑定。\n\n"
        "判断规则：\n"
        "1. 必须优先判断当前用户消息的“本轮任务目标”，不能因为出现“刚才、上述、这些、对应、基于刚才结果”等上下文指代词，就直接沿用上一轮任务。\n"
        "2. 历史对话只用于补全当前任务依赖的对象、数据或结果；当前用户最新消息决定本轮是否需要重新意图识别和重新 skill 路由。\n"
        "3. 如果当前消息只是追问原因、解释过程、复盘执行、评价上一轮结果、修改表达方式、调整输出格式，或继续输出上一轮未完成内容，且没有提出新的处理目标，判 task_followup。\n"
        "4. 如果当前消息提出新的处理目标、新的分析动作、新的判断维度、新的输出要求，或明显需要重新选择工具、Skill、RAG、检索、分析、生成、可视化、评估、预测、归因、报告等能力，判 new_task。\n"
        "5. “基于刚才结果/根据上述结果/针对这些对象”只表示当前任务依赖上一轮输出作为输入，不代表当前任务一定是 task_followup。\n"
        "6. 如果当前消息是在上一轮结果基础上继续做新的检索、分析、判断、评估、对比、归因、预测、报告生成、图表生成、方案制定、风险研判等，应判 new_task。\n"
        "7. 如果无法确定是否需要重新选择 skill，但当前消息包含新的业务目标或新的处理动作，应优先判 new_task，避免错误沿用上一轮路由。\n"
        "8. task_followup 时 should_reclassify_intent=false, should_reroute_skills=false。\n"
        "9. new_task 时 should_reclassify_intent=true, should_reroute_skills=true。\n\n"
        "输出 JSON schema:\n"
        "{\n"
        '  "act": "task_followup|new_task|chitchat|confirmation",\n'
        '  "should_reclassify_intent": true|false,\n'
        '  "should_reroute_skills": true|false,\n'
        '  "confidence": number,\n'
        '  "reason": string\n'
        "}\n\n"
        f"上一轮任务摘要：\n{previous_summary}\n\n"
        f"当前用户消息：{text}\n"
    )

    try:
        response = llm.with_config({"run_name": "DialogueTurnClassifier"}).invoke(prompt)
        raw = getattr(response, "content", response)
        data = _parse_json_object(str(raw))
    except Exception:
        return None

    if not isinstance(data, dict):
        return None

    act = str(data.get("act") or "").strip()
    confidence = _coerce_confidence(data.get("confidence"), default=0.7)
    reason = str(data.get("reason") or "llm_turn_decision").strip()
    should_reclassify = bool(data.get("should_reclassify_intent"))
    should_reroute = bool(data.get("should_reroute_skills"))
    metadata = {
        "raw_answer": text,
        "turn_decision": {
            "act": act,
            "should_reclassify_intent": should_reclassify,
            "should_reroute_skills": should_reroute,
            "confidence": confidence,
            "reason": reason,
        },
    }

    if act == "task_followup" and confidence >= 0.55:
        return DialogueActResult(
            act="task_followup",
            confidence=confidence,
            reason="llm_previous_task_followup",
            parsed_answer=text,
            resume_intent_context=_dict_or_none(previous_intent_context),
            resume_routing_context=_dict_or_none(previous_routing_context),
            metadata=metadata,
        )

    if act == "chitchat" and confidence >= 0.7:
        return DialogueActResult(
            act="chitchat",
            confidence=confidence,
            reason="llm_chitchat",
            parsed_answer=text,
            metadata=metadata,
        )

    if act == "confirmation" and confidence >= 0.7:
        return DialogueActResult(
            act="confirmation",
            confidence=confidence,
            reason="llm_confirmation_without_pending_action",
            parsed_answer=text,
            metadata=metadata,
        )

    if act == "new_task" and confidence >= 0.55:
        return DialogueActResult(
            act="new_task",
            confidence=confidence,
            reason="llm_new_task_or_scene_shift",
            parsed_answer=text,
            metadata=metadata,
        )

    return None


def _build_previous_task_summary(
    previous_intent_context: dict[str, Any] | None,
    previous_routing_context: dict[str, Any] | None,
) -> str:
    lines: list[str] = []
    if isinstance(previous_intent_context, dict):
        for label, key in (
            ("intent", "intent"),
            ("scene", "scene"),
            ("scene_name", "scene_name"),
            ("routing_query", "routing_query"),
            ("original_query", "original_query"),
        ):
            value = previous_intent_context.get(key)
            if value:
                lines.append(f"{label}: {_truncate(str(value), 600)}")
        scene_tasks = previous_intent_context.get("scene_tasks")
        if isinstance(scene_tasks, list) and scene_tasks:
            compact_tasks = []
            for item in scene_tasks[:5]:
                if not isinstance(item, dict):
                    continue
                compact_tasks.append({
                    "scene": item.get("scene"),
                    "text": item.get("text") or item.get("task_text"),
                    "params": item.get("params"),
                })
            if compact_tasks:
                lines.append(f"scene_tasks: {json.dumps(compact_tasks, ensure_ascii=False)}")

    if isinstance(previous_routing_context, dict):
        for label, key in (
            ("route_mode", "route_mode"),
            ("primary_goal", "primary_goal"),
            ("selected_skills", "global_selected_skills"),
        ):
            value = previous_routing_context.get(key)
            if value:
                lines.append(f"{label}: {_truncate(str(value), 600)}")
        route_tasks = previous_routing_context.get("scene_tasks")
        if isinstance(route_tasks, list) and route_tasks:
            compact_route_tasks = []
            for item in route_tasks[:5]:
                if not isinstance(item, dict):
                    continue
                compact_route_tasks.append({
                    "scene": item.get("scene"),
                    "segment_text": item.get("segment_text"),
                    "selected_skills": item.get("selected_skills"),
                })
            if compact_route_tasks:
                lines.append(f"route_tasks: {json.dumps(compact_route_tasks, ensure_ascii=False)}")

    return "\n".join(lines)


def _truncate(text: str, limit: int) -> str:
    value = text.strip()
    if len(value) <= limit:
        return value
    return value[:limit].rstrip() + "..."


def _parse_pending_action_answer(text: str, pending_action: dict[str, Any]) -> dict[str, Any]:
    expected = str(pending_action.get("expected_answer_type") or "")
    options = _normalize_options(pending_action.get("options"))
    raw_updates = _extract_parameter_updates(text)
    parameter_updates, conflicts = _validate_parameter_updates(raw_updates, pending_action)

    if expected == "single_choice" or options:
        option_match = _OPTION_NUMBER_RE.match(text) or _OPTION_REFERENCE_RE.search(text)
        if option_match and options:
            index = _option_token_to_index(option_match.group(1))
            if index is not None and 0 <= index < len(options):
                return {
                    "matched": True,
                    "confidence": 0.98,
                    "reason": "single_choice_index_with_parameter_updates" if parameter_updates else "single_choice_index",
                    "answer": options[index],
                    "parameter_updates": parameter_updates,
                    "conflicts": conflicts,
                }

        normalized_text = _compact(text)
        for option in options:
            label = _compact(str(option.get("label") or ""))
            value = _compact(str(option.get("value") or option.get("id") or ""))
            if normalized_text and (
                normalized_text == label
                or normalized_text == value
                or (len(normalized_text) >= 2 and normalized_text in label)
            ):
                return {
                    "matched": True,
                    "confidence": 0.9,
                    "reason": "single_choice_label_with_parameter_updates" if parameter_updates else "single_choice_label",
                    "answer": option,
                    "parameter_updates": parameter_updates,
                    "conflicts": conflicts,
                }

    if expected in {"confirmation", "risk_confirmation"}:
        if _AFFIRMATIVE_RE.match(text):
            return {"matched": True, "confidence": 0.95, "reason": "confirmation_yes", "answer": True, "parameter_updates": parameter_updates, "conflicts": conflicts}
        if _NEGATIVE_RE.match(text):
            return {"matched": True, "confidence": 0.95, "reason": "confirmation_no", "answer": False, "parameter_updates": parameter_updates, "conflicts": conflicts}

    if parameter_updates or conflicts:
        return {
            "matched": True,
            "confidence": 0.82,
            "reason": "parameter_update_for_pending_action",
            "answer": None,
            "parameter_updates": parameter_updates,
            "conflicts": conflicts,
        }

    return {"matched": False}


def _extract_parameter_updates(text: str) -> dict[str, Any]:
    updates: dict[str, Any] = {}
    compact = _compact(text)

    percent_patterns = [
        ("min_night_ratio", r"(?:夜间|夜里|凌晨|深夜)(?:通话)?(?:占比|比例|阈值)?(?:超过|大于|>=|不少于|改成|设为|设置为)?(\d+(?:\.\d+)?)%"),
    ]
    number_patterns = [
        ("min_night_count", r"(?:夜间|夜里|凌晨|深夜)(?:通话)?(?:次数|数量)(?:超过|大于|>=|不少于|改成|设为|设置为)?(\d+)"),
        ("min_counterparties", r"(?:联系人|对端|联系号码)(?:广度|数量|个数|数)?(?:超过|大于|>=|不少于|改成|设为|设置为)?(\d+)"),
        ("min_shared_device_count", r"(?:共享设备|共用设备)(?:数量|个数|数)?(?:超过|大于|>=|不少于|改成|设为|设置为)?(\d+)"),
        ("min_shared_peer_total", r"(?:共享关联|共享号码|共享对端|共享设备关联号码)(?:数量|个数|数)?(?:超过|大于|>=|不少于|改成|设为|设置为)?(\d+)"),
        ("top_k", r"(?:topk|top|前)(\d+)(?:个|名|条)?"),
    ]

    for key, pattern in percent_patterns:
        match = re.search(pattern, compact)
        if match:
            updates[key] = round(float(match.group(1)) / 100, 4)

    for key, pattern in number_patterns:
        match = re.search(pattern, compact)
        if match:
            updates[key] = int(match.group(1))

    if "自动阈值" in compact or "自动" in compact:
        updates["threshold_mode"] = "auto"
    if "看分布" in compact or "数据分布" in compact or "先看" in compact:
        updates["threshold_mode"] = "inspect_distribution"

    return updates


def _parse_pending_action_with_llm(text: str, pending_action: dict[str, Any], llm: Any) -> dict[str, Any]:
    options = _normalize_options(pending_action.get("options"))
    schema = _parameter_schema(pending_action)
    prompt = (
        "你是对话状态解析器。判断用户是否在回答上一轮 pending_action，并只输出 JSON 对象，不要 Markdown。\n"
        "JSON schema:\n"
        "{\n"
        '  "is_answer": true|false,\n'
        '  "selected_option_id": string|null,\n'
        '  "parameter_updates": object,\n'
        '  "conflicts": [string],\n'
        '  "confidence": number\n'
        "}\n"
        f"pending_question: {pending_action.get('question')}\n"
        f"options: {json.dumps(options, ensure_ascii=False)}\n"
        f"allowed_parameter_schema: {json.dumps(schema, ensure_ascii=False)}\n"
        f"user_reply: {text}\n"
    )
    try:
        response = llm.with_config({"run_name": "DialogueActStructuredParser"}).invoke(prompt)
        raw = getattr(response, "content", response)
        data = _parse_json_object(str(raw))
    except Exception:
        return {"matched": False}

    if not isinstance(data, dict) or not data.get("is_answer"):
        return {"matched": False}

    answer = None
    selected_id = data.get("selected_option_id")
    if selected_id is not None:
        selected_id_text = str(selected_id)
        for option in options:
            if selected_id_text in {str(option.get("id")), str(option.get("value")), str(option.get("index"))}:
                answer = option
                break

    raw_updates = data.get("parameter_updates") if isinstance(data.get("parameter_updates"), dict) else {}
    parameter_updates, conflicts = _validate_parameter_updates(raw_updates, pending_action)
    for conflict in data.get("conflicts") or []:
        if isinstance(conflict, str) and conflict.strip():
            conflicts.append(conflict.strip())

    return {
        "matched": True,
        "confidence": _coerce_confidence(data.get("confidence"), default=0.78),
        "reason": "llm_structured_pending_action_reply",
        "answer": answer,
        "parameter_updates": parameter_updates,
        "conflicts": conflicts,
    }


def _parse_json_object(raw: str) -> dict[str, Any]:
    text = raw.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end >= start:
        text = text[start:end + 1]
    data = json.loads(text)
    return data if isinstance(data, dict) else {}


def _coerce_confidence(value: Any, *, default: float) -> float:
    try:
        confidence = float(value)
    except (TypeError, ValueError):
        return default
    return max(0.0, min(1.0, confidence))


def _validate_parameter_updates(
    updates: dict[str, Any],
    pending_action: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    schema = _parameter_schema(pending_action)
    if not updates:
        return {}, []

    valid: dict[str, Any] = {}
    conflicts: list[str] = []
    for key, value in updates.items():
        spec = schema.get(key)
        if not spec:
            conflicts.append(f"Unsupported parameter ignored: {key}")
            continue
        coerced, error = _coerce_parameter_value(key, value, spec)
        if error:
            conflicts.append(error)
            continue
        valid[key] = coerced

    if valid.get("threshold_mode") == "auto":
        manual_keys = sorted(k for k in valid if k.startswith("min_"))
        if manual_keys:
            conflicts.append(
                "Mixed threshold strategy: threshold_mode=auto with manual overrides "
                + ", ".join(manual_keys)
            )

    return valid, conflicts


def _parameter_schema(pending_action: dict[str, Any]) -> dict[str, dict[str, Any]]:
    raw_schema = pending_action.get("parameter_schema")
    if isinstance(raw_schema, dict) and raw_schema:
        return {str(k): v for k, v in raw_schema.items() if isinstance(v, dict)}
    return _default_parameter_schema()


def _default_parameter_schema() -> dict[str, dict[str, Any]]:
    return {
        "threshold_mode": {"type": "enum", "values": ["auto", "manual", "inspect_distribution"]},
        "min_night_ratio": {"type": "float", "min": 0.0, "max": 1.0},
        "min_night_count": {"type": "int", "min": 0},
        "min_counterparties": {"type": "int", "min": 0},
        "min_shared_device_count": {"type": "int", "min": 0},
        "min_shared_peer_total": {"type": "int", "min": 0},
        "top_k": {"type": "int", "min": 1, "max": 500},
    }


def _coerce_parameter_value(key: str, value: Any, spec: dict[str, Any]) -> tuple[Any, str | None]:
    param_type = spec.get("type")
    try:
        if param_type == "int":
            coerced = int(value)
        elif param_type == "float":
            coerced = float(value)
        elif param_type == "enum":
            coerced = str(value)
            values = [str(v) for v in spec.get("values", [])]
            if coerced not in values:
                return None, f"Invalid enum value for {key}: {value}"
            return coerced, None
        else:
            coerced = value
    except (TypeError, ValueError):
        return None, f"Invalid value for {key}: {value}"

    minimum = spec.get("min")
    maximum = spec.get("max")
    if minimum is not None and coerced < minimum:
        return None, f"Value below minimum for {key}: {coerced} < {minimum}"
    if maximum is not None and coerced > maximum:
        return None, f"Value above maximum for {key}: {coerced} > {maximum}"
    return coerced, None


def _looks_like_reply_to_pending_question(text: str, pending_action: dict[str, Any], llm: Any) -> bool:
    question = str(pending_action.get("question") or "")
    if not question:
        return False
    options = _normalize_options(pending_action.get("options"))
    option_lines = "\n".join(f"- {opt.get('label')}" for opt in options)
    prompt = (
        "判断用户回复是否是在回答上一轮澄清问题。只输出 0 或 1。\n"
        f"上一轮问题：{question}\n"
        f"选项：\n{option_lines or '无'}\n"
        f"用户回复：{text}\n"
        "如果是回答上一轮问题输出 1；如果是新任务、闲聊或无关内容输出 0。"
    )
    try:
        response = llm.with_config({"run_name": "DialogueActClassifier"}).invoke(prompt)
        raw = getattr(response, "content", response)
    except Exception:
        return False
    return str(raw).strip().startswith("1")


def _normalize_options(raw_options: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_options, list):
        return []
    normalized: list[dict[str, Any]] = []
    for index, option in enumerate(raw_options, 1):
        if isinstance(option, dict):
            label = str(option.get("label") or option.get("value") or option.get("id") or "").strip()
            value = option.get("value", label)
            option_id = str(option.get("id") or value or index).strip()
        else:
            label = str(option).strip()
            value = label
            option_id = str(index)
        if not label:
            continue
        normalized.append({"id": option_id, "label": label, "value": value, "index": index})
    return normalized


def _option_token_to_index(token: str) -> int | None:
    token = token.strip()
    if token.isdigit():
        return int(token) - 1
    letters = "abcdefghijklmnopqrstuvwxyz"
    lower = token.lower()
    if lower in letters:
        return letters.index(lower)
    numerals = {
        "一": 0,
        "二": 1,
        "三": 2,
        "四": 3,
        "五": 4,
        "六": 5,
        "七": 6,
        "八": 7,
        "九": 8,
        "十": 9,
    }
    return numerals.get(token)


def _compact(text: str) -> str:
    return re.sub(r"[\s，,。.!！?？：:;；（）()\[\]【】\"'“”‘’]+", "", text.lower())


def _dict_or_none(value: Any) -> dict[str, Any] | None:
    return value if isinstance(value, dict) else None


def _public_pending_action(pending_action: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": pending_action.get("id"),
        "type": pending_action.get("type"),
        "question": pending_action.get("question"),
        "expected_answer_type": pending_action.get("expected_answer_type"),
        "options": _normalize_options(pending_action.get("options")),
        "parameter_schema": _parameter_schema(pending_action),
    }


def _build_dialogue_metadata(
    *,
    pending_action: dict[str, Any],
    parameter_updates: dict[str, Any],
    raw_answer: str,
    conflicts: list[str],
    selected_answer: Any,
) -> dict[str, Any]:
    resume_plan = _build_resume_plan(
        pending_action=pending_action,
        parameter_updates=parameter_updates,
        conflicts=conflicts,
        selected_answer=selected_answer,
    )
    return {
        "pending_action": _public_pending_action(pending_action),
        "parameter_updates": parameter_updates,
        "conflicts": conflicts,
        "raw_answer": raw_answer,
        "resume_plan": resume_plan,
    }


def _build_resume_plan(
    *,
    pending_action: dict[str, Any],
    parameter_updates: dict[str, Any],
    conflicts: list[str],
    selected_answer: Any,
) -> dict[str, Any]:
    selected_skill_ids = _selected_skill_ids(pending_action.get("resume_routing_context"))
    selected_option_id = None
    selected_option_label = None
    if isinstance(selected_answer, dict):
        selected_option_id = selected_answer.get("id")
        selected_option_label = selected_answer.get("label")
    elif selected_answer is not None:
        selected_option_label = str(selected_answer)

    next_action = "resume_previous_task"
    if conflicts:
        next_action = "resolve_parameter_conflicts"
    elif parameter_updates:
        next_action = "resume_previous_task_with_parameter_updates"
    elif selected_option_id is not None:
        next_action = "resume_previous_task_with_selected_option"

    return {
        "resume_from": pending_action.get("id"),
        "source_task": pending_action.get("source_task_id"),
        "selected_skill_ids": selected_skill_ids,
        "selected_option_id": selected_option_id,
        "selected_option_label": selected_option_label,
        "next_action": next_action,
        "tool_args_patch": parameter_updates,
        "conflicts": conflicts,
    }


def _selected_skill_ids(routing_context: Any) -> list[str]:
    if not isinstance(routing_context, dict):
        return []
    values = routing_context.get("global_selected_skills")
    if not isinstance(values, list):
        return []
    return [str(value) for value in values if str(value).strip()]
