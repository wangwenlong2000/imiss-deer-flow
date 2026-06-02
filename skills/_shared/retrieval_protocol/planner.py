"""检索计划编排（纯 stdlib）—— task.md 反馈 #2/#3 的 Planner 侧。

把一条复合检索的总预算拆给若干并行 retrieve 任务,并把已路由好的子问题
组织成结构化 Plan。**不含** LLM 拆问题语义与 Skill 路由 —— 那是上层
PlannerMiddleware(S3)与 SkillRouter 的职责。本模块只做结构化输出。

设计来源:历史 spec docs/superpowers/specs/2026-05-27-... §4.1。
本模块仅依赖 Python 标准库。
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any

_BUDGET_FIELDS = ("max_evidence_count", "max_token_estimate")


def _split_one(total_value: int, fan_out: int, weights: list[float] | None) -> list[int]:
    """把单个整数预算按权重拆成 fan_out 份,余数给第一个,保证 sum 一致。"""
    if weights is None:
        base = total_value // fan_out
        parts = [base] * fan_out
    else:
        weight_sum = sum(weights)
        parts = [int(total_value * w / weight_sum) for w in weights]
    # 余数(等权的整除余数,或加权的取整损失)统一补给第一个,保证 sum == total_value
    remainder = total_value - sum(parts)
    parts[0] += remainder
    return parts


def split_budget(
    total: dict[str, Any],
    fan_out: int,
    weights: list[float] | None = None,
) -> list[dict[str, Any]]:
    """把总 budget 拆给 ``fan_out`` 个并行任务。

    - ``total``: 含 ``max_evidence_count`` / ``max_token_estimate`` 之一或两者。
    - ``weights=None``: 等权;否则按权重比例,长度须等于 ``fan_out``。
    - 整除/取整余数统一补给第一个任务,保证每个字段拆分后 **sum 与 total 一致**。

    返回长度为 ``fan_out`` 的 budget 列表;只拆 ``total`` 实际给出的字段。
    """
    if not isinstance(fan_out, int) or isinstance(fan_out, bool) or fan_out < 1:
        raise ValueError("fan_out must be a positive integer")
    if weights is not None and len(weights) != fan_out:
        raise ValueError(f"weights length {len(weights)} != fan_out {fan_out}")
    if weights is not None and (sum(weights) <= 0 or any(w < 0 for w in weights)):
        raise ValueError("weights must be non-negative with a positive sum")

    present = [f for f in _BUDGET_FIELDS if f in total]
    if not present:
        raise ValueError(
            f"total budget must contain at least one of {_BUDGET_FIELDS}"
        )

    result: list[dict[str, Any]] = [{} for _ in range(fan_out)]
    for field_name in present:
        value = total[field_name]
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ValueError(f"{field_name} must be a non-negative integer")
        for i, part_value in enumerate(_split_one(value, fan_out, weights)):
            result[i][field_name] = part_value
    return result


@dataclass(frozen=True)
class RetrievalTask:
    """一个并行可调度的 retrieve 任务。"""
    query_id: str
    envelope: dict[str, Any]
    parallel_group: int = 0


@dataclass
class Plan:
    """一条复合检索的拆解结果。"""
    parent_query_id: str
    tasks: list[RetrievalTask] = field(default_factory=list)
    total_budget: dict[str, Any] | None = None


def build_plan(
    *,
    parent_query_id: str,
    subqueries: list[dict[str, Any]],
    total_budget: dict[str, Any] | None = None,
) -> Plan:
    """把已路由好的子问题组织成 Plan;给定 total_budget 时按 weight 注入拆分预算。

    ``subqueries`` 每条 = ``{query_id, envelope, parallel_group?, weight?}``。
    本函数**深拷贝**每个 envelope,不修改调用方入参。若 ``total_budget`` 给出,
    按各子问题的 ``weight``(缺省 1.0)调用 :func:`split_budget`,把拆分后的
    budget 注入对应 task 的 ``envelope["budget"]``。
    """
    if not subqueries:
        raise ValueError("subqueries must be non-empty")

    envelopes = [copy.deepcopy(sq["envelope"]) for sq in subqueries]

    if total_budget is not None:
        weights = [float(sq.get("weight", 1.0)) for sq in subqueries]
        # 全部缺省权重时退化为等权(传 None 让 split_budget 走整除路径)
        use_weights = None if all(w == 1.0 for w in weights) else weights
        parts = split_budget(total_budget, len(subqueries), weights=use_weights)
        for env, part in zip(envelopes, parts):
            env["budget"] = part

    tasks = [
        RetrievalTask(
            query_id=sq["query_id"],
            envelope=env,
            parallel_group=int(sq.get("parallel_group", 0)),
        )
        for sq, env in zip(subqueries, envelopes)
    ]
    return Plan(parent_query_id=parent_query_id, tasks=tasks, total_budget=total_budget)
