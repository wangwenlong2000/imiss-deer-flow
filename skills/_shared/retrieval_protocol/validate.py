"""融合前校验 —— task.md §7 校验规则。

每个 ``validate_*`` 函数返回错误信息列表；**空列表表示合规**。错误信息统一带
``[rule N]`` 前缀，对应 task.md §7 的十条规则编号。

十条规则的可机械校验范围：

- 规则 1 —— evidence wrapper 含 ``type="retrieval"``、``score``（或 ``retrieval.raw_scores``）、``payload``；
- 规则 2 —— ``payload`` 含 ``evidence_id`` / ``data_type`` / ``text`` / ``meta.source_id`` / ``meta.features``；
- 规则 3 —— ``evidence_id`` 在同一 SkillResult / 同一 JSONL 内唯一；
- 规则 4 —— ``data_type`` 取自 task.md §2 登记取值；
- 规则 5 —— ``meta.geo_scope`` 给出时必须是对象（可为 ``{}``，不得为 null/字符串）；
- 规则 6 —— ``meta.time_range`` 给出时必须含 ``mode`` 且字段组（绝对/相对）完整；
- 规则 7 —— 隐私字段 ``sensitivity_level`` 与 ``access_policy`` 成组且取值合法；
- 规则 8 —— 附件登记进 ``result.artifacts[]`` 且形状合法；``evidence_unit`` 内禁止
  出现 ``artifacts`` / ``attachments`` 键（「附件混进 text」无法可靠机检）；
- 规则 9 —— 输出是结构完整的 SkillResult JSON，而非纯自然语言；
- 规则 10 —— 路由经 SkillRouter（``scenes`` + ``task_types``）的设计约束，不引入
  ``data_type`` 路由分支；该规则无逐条数据校验项，仅在此说明。

规则 6 / 7 依赖「数据是否有时间属性 / 是否隐私敏感」这一语义判断，无法纯结构推断，
故采用「给出即从严」策略：相关字段一旦出现就必须合法且成组完整。

本模块仅依赖 Python 标准库。
"""

from __future__ import annotations

import json
from typing import Any

from .errors import validate_errors_block
from .schema import (
    ACCESS_POLICIES,
    DATA_TYPES,
    EVIDENCE_TYPE_RETRIEVAL,
    RETRIEVAL_STRATEGIES,
    RUN_MODES,
    SENSITIVITY_LEVELS,
    TIME_RANGE_MODES,
)

# evidence_unit 内禁止出现的附件类键名（task.md §7 规则 8）。
_ATTACHMENT_KEYS = ("artifacts", "attachments")


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_nonempty_str(value: Any) -> bool:
    return isinstance(value, str) and value.strip() != ""


# ---------------------------------------------------------------------------
# time_range（task.md §3.3 / §7 规则 6）
# ---------------------------------------------------------------------------


def validate_time_range(time_range: Any, *, path: str, rule: str = "rule 6") -> list[str]:
    """校验 time_range 对象：``mode`` 必填，并据 absolute/relative 填对应字段组。"""
    if not isinstance(time_range, dict):
        return [f"[{rule}] {path} must be an object"]
    mode = time_range.get("mode")
    if mode not in TIME_RANGE_MODES:
        return [f"[{rule}] {path}.mode must be 'absolute' or 'relative', got {mode!r}"]

    errors: list[str] = []
    if mode == "absolute":
        # 绝对时间：真实墙钟时间，start/end 为 ISO 8601 字符串 + timezone。
        for field in ("start", "end", "timezone"):
            if not _is_nonempty_str(time_range.get(field)):
                errors.append(f"[{rule}] {path} mode=absolute requires non-empty string {field!r}")
    else:
        # 相对时间：相对采集起点的整数秒偏移。
        for field in ("start_offset_s", "end_offset_s"):
            value = time_range.get(field)
            if value is None:
                errors.append(f"[{rule}] {path} mode=relative requires {field!r}")
            elif not isinstance(value, int) or isinstance(value, bool):
                errors.append(f"[{rule}] {path}.{field} must be an integer number of seconds")
    return errors


# ---------------------------------------------------------------------------
# evidence_unit（task.md §7 规则 2、4、5、6、7、8 的 payload 部分）
# ---------------------------------------------------------------------------


def _validate_privacy(meta: dict[str, Any], *, path: str) -> list[str]:
    """规则 7：隐私字段成组且取值合法。"""
    errors: list[str] = []
    has_sensitivity = "sensitivity_level" in meta
    has_access = "access_policy" in meta
    if has_sensitivity and meta.get("sensitivity_level") not in SENSITIVITY_LEVELS:
        errors.append(
            f"[rule 7] {path}.meta.sensitivity_level {meta.get('sensitivity_level')!r} is invalid; "
            f"expected one of {sorted(SENSITIVITY_LEVELS)}"
        )
    if has_access and meta.get("access_policy") not in ACCESS_POLICIES:
        errors.append(
            f"[rule 7] {path}.meta.access_policy {meta.get('access_policy')!r} is invalid; "
            f"expected one of {sorted(ACCESS_POLICIES)}"
        )
    # 声明了隐私意图就必须成组：sensitivity_level 与 access_policy 同时出现。
    if has_sensitivity and not has_access:
        errors.append(f"[rule 7] {path}.meta declares sensitivity_level but is missing access_policy")
    if has_access and not has_sensitivity:
        errors.append(f"[rule 7] {path}.meta declares access_policy but is missing sensitivity_level")
    return errors


def validate_evidence_unit(payload: Any, *, path: str = "payload") -> list[str]:
    """校验 evidence_unit（task.md §4.2）。"""
    if not isinstance(payload, dict):
        return [f"[rule 2] {path} must be a JSON object"]

    errors: list[str] = []

    # 规则 2：顶层必填字段。
    if not _is_nonempty_str(payload.get("evidence_id")):
        errors.append(f"[rule 2] {path}.evidence_id is required and must be a non-empty string")
    data_type = payload.get("data_type")
    if not _is_nonempty_str(data_type):
        errors.append(f"[rule 2] {path}.data_type is required and must be a non-empty string")
    if not _is_nonempty_str(payload.get("text")):
        errors.append(f"[rule 2] {path}.text is required and must be a non-empty string")

    # 规则 2：meta 及其必填子字段。
    meta = payload.get("meta")
    if not isinstance(meta, dict):
        errors.append(f"[rule 2] {path}.meta is required and must be an object")
        meta = {}
    else:
        if not _is_nonempty_str(meta.get("source_id")):
            errors.append(f"[rule 2] {path}.meta.source_id is required and must be a non-empty string")
        if not isinstance(meta.get("features"), dict):
            errors.append(f"[rule 2] {path}.meta.features is required and must be an object")

    # 规则 4：data_type 必须登记。
    if isinstance(data_type, str) and data_type and data_type not in DATA_TYPES:
        errors.append(f"[rule 4] {path}.data_type {data_type!r} is not a registered data_type (task.md §2)")

    # 规则 5：geo_scope 给出时必须是对象。
    if "geo_scope" in meta and not isinstance(meta["geo_scope"], dict):
        errors.append(
            f"[rule 5] {path}.meta.geo_scope must be an object (use {{}} for non-spatial data); "
            f"got {type(meta['geo_scope']).__name__}"
        )

    # 规则 6：time_range 给出时必须合法。
    if "time_range" in meta:
        errors.extend(validate_time_range(meta["time_range"], path=f"{path}.meta.time_range"))

    # 规则 7：隐私字段。
    errors.extend(_validate_privacy(meta, path=path))

    # 规则 8：evidence_unit 内禁止附件键。
    for key in _ATTACHMENT_KEYS:
        if key in payload:
            errors.append(
                f"[rule 8] {path}.{key} is forbidden; attachments must go in SkillResult.result.artifacts[]"
            )
        if key in meta:
            errors.append(
                f"[rule 8] {path}.meta.{key} is forbidden; attachments must go in SkillResult.result.artifacts[]"
            )

    return errors


# ---------------------------------------------------------------------------
# evidence wrapper（task.md §7 规则 1）
# ---------------------------------------------------------------------------


def validate_evidence(wrapper: Any, *, path: str = "evidence") -> list[str]:
    """校验一条 evidence wrapper 及其 payload（task.md §4.1）。"""
    if not isinstance(wrapper, dict):
        return [f"[rule 1] {path} must be a JSON object"]

    errors: list[str] = []

    # 规则 1：type 必须为 retrieval。
    if wrapper.get("type") != EVIDENCE_TYPE_RETRIEVAL:
        errors.append(f"[rule 1] {path}.type must be 'retrieval', got {wrapper.get('type')!r}")

    # 规则 1：必须携带 score 或 retrieval.raw_scores。
    has_score = _is_number(wrapper.get("score"))
    retrieval = wrapper.get("retrieval")
    has_raw_scores = (
        isinstance(retrieval, dict)
        and isinstance(retrieval.get("raw_scores"), dict)
        and len(retrieval["raw_scores"]) > 0
    )
    if not has_score and not has_raw_scores:
        errors.append(f"[rule 1] {path} must carry a numeric 'score' or a non-empty 'retrieval.raw_scores'")

    # 规则 1：payload 必填，并下钻校验 evidence_unit。
    if "payload" not in wrapper:
        errors.append(f"[rule 1] {path}.payload is required")
    else:
        errors.extend(validate_evidence_unit(wrapper["payload"], path=f"{path}.payload"))

    return errors


def is_valid_evidence(wrapper: Any) -> bool:
    """便捷判定：一条 evidence wrapper 是否合规。"""
    return not validate_evidence(wrapper)


def validate_evidence_list(wrappers: Any, *, path: str = "evidence") -> list[str]:
    """校验一组 evidence wrapper，并检查 ``evidence_id`` 在组内唯一（规则 3）。"""
    if not isinstance(wrappers, list):
        return [f"[rule 9] {path} must be a list"]

    errors: list[str] = []
    seen: dict[str, str] = {}
    for index, wrapper in enumerate(wrappers):
        item_path = f"{path}[{index}]"
        errors.extend(validate_evidence(wrapper, path=item_path))
        if isinstance(wrapper, dict) and isinstance(wrapper.get("payload"), dict):
            evidence_id = wrapper["payload"].get("evidence_id")
            if _is_nonempty_str(evidence_id):
                if evidence_id in seen:
                    errors.append(
                        f"[rule 3] duplicate evidence_id {evidence_id!r} at {item_path} "
                        f"(first seen at {seen[evidence_id]})"
                    )
                else:
                    seen[evidence_id] = item_path
    return errors


# ---------------------------------------------------------------------------
# SkillResult（task.md §7 规则 9、8、3）
# ---------------------------------------------------------------------------


def _validate_artifacts(artifacts: Any) -> list[str]:
    """规则 8：result.artifacts[] 必须是合法的附件列表。"""
    if artifacts is None:
        return []
    if not isinstance(artifacts, list):
        return ["[rule 8] SkillResult.result.artifacts must be a list"]
    errors: list[str] = []
    for index, artifact in enumerate(artifacts):
        item_path = f"result.artifacts[{index}]"
        if not isinstance(artifact, dict):
            errors.append(f"[rule 8] {item_path} must be an object")
            continue
        for field in ("artifact_id", "type", "uri"):
            if not _is_nonempty_str(artifact.get(field)):
                errors.append(f"[rule 8] {item_path}.{field} is required and must be a non-empty string")
    return errors


def validate_skill_result(skill_result: Any) -> list[str]:
    """校验完整 SkillResult（task.md §4.1）。

    覆盖规则 9（结构 = SkillResult JSON，非自然语言）、规则 8（附件登记）、
    规则 3（evidence_id 唯一）、schema v1.1 errors 标准化及逐条 evidence 的规则 1–8。
    """
    if not isinstance(skill_result, dict):
        return ["[rule 9] SkillResult must be a JSON object, not natural-language text"]

    errors: list[str] = []

    # 规则 9：固定头。
    for field in ("schema_version", "request_id", "skill_name", "scenario", "capability", "status"):
        if not _is_nonempty_str(skill_result.get(field)):
            errors.append(f"[rule 9] SkillResult.{field} is required and must be a non-empty string")

    result = skill_result.get("result")
    if not isinstance(result, dict):
        errors.append("[rule 9] SkillResult.result is required and must be an object")
        return errors

    # 规则 8：附件。
    errors.extend(_validate_artifacts(result.get("artifacts")))

    # 规则 9 + 3：result.evidence 列表与逐条校验。
    errors.extend(validate_evidence_list(result.get("evidence"), path="result.evidence"))

    # schema v1.1：errors[] 与 status 联动校验。
    status = skill_result.get("status")
    if isinstance(status, str) and status:
        errors.extend(validate_errors_block(skill_result.get("errors"), status=status))
    return errors


def validate_jsonl_lines(lines: Any) -> list[str]:
    """校验跨 skill 交换的 JSONL：每行 = 一条 evidence wrapper，``evidence_id`` 全局唯一。

    ``lines`` 可以是整段 JSONL 文本字符串，或由「JSON 字符串 / 已解析 dict」组成的
    可迭代对象（task.md §5）。
    """
    if isinstance(lines, str):
        raw_items: list[Any] = [line for line in lines.splitlines() if line.strip()]
    else:
        raw_items = [item for item in (lines or []) if not (isinstance(item, str) and not item.strip())]

    errors: list[str] = []
    wrappers: list[Any] = []
    for index, raw in enumerate(raw_items):
        if isinstance(raw, (dict, list)):
            wrappers.append(raw)
            continue
        try:
            wrappers.append(json.loads(raw))
        except (TypeError, ValueError) as exc:
            errors.append(f"[rule 9] JSONL line {index} is not valid JSON: {exc}")
    errors.extend(validate_evidence_list(wrappers, path="jsonl"))
    return errors


# ---------------------------------------------------------------------------
# Input Envelope（task.md §3）
# ---------------------------------------------------------------------------


def validate_input_envelope(envelope: Any) -> list[str]:
    """校验统一检索输入 Input Envelope（task.md §3.1 / §3.2）。"""
    if not isinstance(envelope, dict):
        return ["[§3] Input Envelope must be a JSON object"]

    errors: list[str] = []
    for field in ("schema_version", "request_id", "skill_name", "scenario", "capability"):
        if not _is_nonempty_str(envelope.get(field)):
            errors.append(f"[§3.2] Input Envelope.{field} is required and must be a non-empty string")

    payload_input = envelope.get("input")
    if not isinstance(payload_input, dict):
        errors.append("[§3.2] Input Envelope.input is required and must be an object")
        return errors

    parameters = payload_input.get("parameters")
    if not isinstance(parameters, dict):
        errors.append("[§3.2] input.parameters is required and must be an object")
    else:
        if not _is_nonempty_str(parameters.get("query")):
            errors.append("[§3.2] input.parameters.query is required and must be a non-empty string")
        data_type = parameters.get("data_type")
        if not _is_nonempty_str(data_type):
            errors.append("[§3.2] input.parameters.data_type is required and must be a non-empty string")
        elif data_type not in DATA_TYPES:
            errors.append(
                f"[§3.2/§7.4] input.parameters.data_type {data_type!r} is not a registered data_type (task.md §2)"
            )
        mode = parameters.get("mode")
        if mode is not None and mode not in RUN_MODES:
            errors.append(f"[§3.2] input.parameters.mode {mode!r} is invalid; expected one of {sorted(RUN_MODES)}")
        retrieval = parameters.get("retrieval")
        if isinstance(retrieval, dict):
            strategy = retrieval.get("strategy")
            if strategy is not None and strategy not in RETRIEVAL_STRATEGIES:
                errors.append(
                    f"[§3.2] input.parameters.retrieval.strategy {strategy!r} is invalid; "
                    f"expected one of {sorted(RETRIEVAL_STRATEGIES)}"
                )

    filters = payload_input.get("filters")
    if filters is not None and not isinstance(filters, dict):
        errors.append("[§3.2] input.filters must be an object")
    elif isinstance(filters, dict) and "time_range" in filters:
        errors.extend(validate_time_range(filters["time_range"], path="input.filters.time_range", rule="§3.3"))

    # schema v1.1：顶层 budget(可选)。
    if "budget" in envelope:
        errors.extend(validate_budget(envelope["budget"], path="budget"))

    return errors


def validate_budget(budget: Any, *, path: str = "budget") -> list[str]:
    """校验 Envelope.budget(schema v1.1)。两个字段都可选,至少一个必须给出。"""
    if not isinstance(budget, dict):
        return [f"[v1.1] {path} must be an object"]
    issues: list[str] = []
    allowed = {"max_evidence_count", "max_token_estimate"}
    seen_known = False
    for key in allowed:
        if key in budget:
            seen_known = True
            value = budget[key]
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                issues.append(f"[v1.1] {path}.{key} must be a positive integer, got {value!r}")
    if not seen_known:
        issues.append(f"[v1.1] {path} requires at least one of {sorted(allowed)}")
    return issues
