"""检索结果聚合（纯 stdlib）—— task.md 反馈 #3 的聚合层。

把并行 retrieve 返回的多份 SkillResult 按 query_id 对齐成桶,桶间全局去重,
全局排序,按 Plan 总预算兜底裁剪,产出一份统一 SkillResult。

对齐契约: ``skill_results`` 每条 = ``{"query_id": str, "skill_result": dict}``
(上层 PlannerMiddleware 派发时天然知道每个 result 对应哪个 query_id)。

设计来源:历史 spec docs/superpowers/specs/2026-05-27-... §4.2。
本模块仅依赖 Python 标准库 + 包内 evidence/errors 构造器。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from .errors import build_error, STATUS_PARTIAL
from .evidence import build_skill_result, build_summary
from .planner import Plan


def _default_dedup_key(wrapper: dict[str, Any]) -> str:
    return str(wrapper.get("payload", {}).get("evidence_id", ""))


def _default_sort_key(wrapper: dict[str, Any]) -> float:
    # 升序排序键:取负分 → score 高者在前
    return -float(wrapper.get("score") or 0.0)


def _default_token_estimator(wrapper: dict[str, Any]) -> int:
    text = wrapper.get("payload", {}).get("text", "")
    return len(text) // 4 if isinstance(text, str) else 0


@dataclass
class AggregatorHooks:
    """可插拔策略;任一为 None 走对应默认实现。"""
    dedup_key: Callable[[dict[str, Any]], str] | None = None
    sort_key: Callable[[dict[str, Any]], float] | None = None
    token_estimator: Callable[[dict[str, Any]], int] | None = None


class Aggregator:
    """按 Plan 把多份 SkillResult 对齐/去重/排序/裁剪成一份。"""

    def __init__(self, hooks: AggregatorHooks | None = None) -> None:
        hooks = hooks or AggregatorHooks()
        self._dedup_key = hooks.dedup_key or _default_dedup_key
        self._sort_key = hooks.sort_key or _default_sort_key
        self._token_estimator = hooks.token_estimator or _default_token_estimator

    def aggregate(
        self,
        *,
        plan: Plan,
        skill_results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """对齐 → 去重 → 排序 → 按 plan.total_budget 兜底裁剪。返回统一 SkillResult。"""
        deduped = self._collect_and_dedup(skill_results)
        deduped.sort(key=self._sort_key)

        kept, clipped = self._clip_to_budget(deduped, plan.total_budget)

        errors: list[dict[str, Any]] = []
        status = "success"
        if clipped:
            errors.append(build_error(
                code="E_OUT_OF_BUDGET",
                message=(
                    f"聚合证据超预算,已裁剪 {clipped} 条,保留 {len(kept)} 条。"
                ),
                detail={"kept": len(kept), "clipped": clipped},
            ))
            status = STATUS_PARTIAL
        return self._build_result(kept, errors=errors, status=status)

    def _clip_to_budget(
        self,
        evidence: list[dict[str, Any]],
        total_budget: dict[str, Any] | None,
    ) -> tuple[list[dict[str, Any]], int]:
        """按 total_budget 裁剪;返回 (保留列表, 被裁剪条数)。无预算则不裁。

        budget 是**硬上限**(task.md #2:避免撑爆上下文)。先按 max_evidence_count
        截前 N,再按 max_token_estimate 累计裁剪。token 裁剪用严格 `>`:
        累计 token 一旦超过 max_token_estimate 即停,不收当前及后续条目。

        边界:若**单条** evidence 的 token 估计已超过 max_token_estimate,
        则一条都不保留(返回空列表 + 全部计入 clipped)。这是有意为之 ——
        预算是硬约束,宁可返回空也不送一条撑爆上下文的证据;调用方据此
        E_OUT_OF_BUDGET + status=partial 可判断是预算过紧。
        """
        if not total_budget:
            return evidence, 0
        original = len(evidence)
        kept = evidence

        max_count = total_budget.get("max_evidence_count")
        if isinstance(max_count, int) and not isinstance(max_count, bool):
            kept = kept[:max_count]

        max_token = total_budget.get("max_token_estimate")
        if isinstance(max_token, int) and not isinstance(max_token, bool):
            budgeted: list[dict[str, Any]] = []
            running = 0
            for wrapper in kept:
                running += self._token_estimator(wrapper)
                if running > max_token:
                    break
                budgeted.append(wrapper)
            kept = budgeted

        return kept, original - len(kept)

    def _collect_and_dedup(
        self, skill_results: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """跨所有桶单趟按 dedup_key 全局去重,同键保留 sort_key 最优一条;

        保持首次出现顺序(后续全局排序会再覆盖,此处仅作稳定的并列次序)。
        """
        best: dict[str, dict[str, Any]] = {}
        order: list[str] = []
        for item in skill_results:
            sr = item.get("skill_result") or {}
            evidence = sr.get("result", {}).get("evidence", []) or []
            for wrapper in evidence:
                key = self._dedup_key(wrapper)
                if key not in best:
                    best[key] = wrapper
                    order.append(key)
                else:
                    # 保留 sort_key 更优(更小)的一条
                    if self._sort_key(wrapper) < self._sort_key(best[key]):
                        best[key] = wrapper
        return [best[k] for k in order]

    def _build_result(
        self,
        evidence: list[dict[str, Any]],
        *,
        errors: list[dict[str, Any]],
        status: str,
    ) -> dict[str, Any]:
        summary = build_summary(
            title="聚合检索证据",
            overview=f"对齐去重排序后保留 {len(evidence)} 条证据。",
        )
        return build_skill_result(
            skill_name="retrieval-aggregator",
            scenario="aggregate",
            capability="evidence_aggregate",
            status=status,
            summary=summary,
            evidence=evidence,
            errors=errors,
        )
