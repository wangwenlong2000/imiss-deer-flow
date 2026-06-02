"""统一检索输入 —— Input Envelope 构造器（task.md §3）。

检索 skill 接收的就是标准 Input Envelope；检索专用内容只占用 ``input.parameters``
与 ``input.filters``。本模块提供按 task.md §3.1 信封形状装配的纯函数构造器。

所有构造器只把**显式给出**的可选字段写进结果，未给出的可选字段直接省略，保持
JSON 干净。本模块仅依赖 Python 标准库。
"""

from __future__ import annotations

import uuid
from typing import Any

from .schema import DATA_TYPES, RETRIEVAL_STRATEGIES, RUN_MODES, SCHEMA_VERSION


def new_request_id() -> str:
    """生成 task.md §3.1 要求的 uuid-v4 ``request_id``。"""
    return str(uuid.uuid4())


def build_time_range_absolute(*, start: str, end: str, timezone: str = "Asia/Shanghai") -> dict[str, Any]:
    """task.md §3.3：绝对时间。真实墙钟时间，``start`` / ``end`` 为 ISO 8601 字符串。

    时空轨迹、交通流量、政策、视频用此。
    """
    return {"mode": "absolute", "start": start, "end": end, "timezone": timezone}


def build_time_range_relative(*, start_offset_s: int, end_offset_s: int) -> dict[str, Any]:
    """task.md §3.3：相对时间。相对抓包 / 采集起点的整数秒偏移。

    network-traffic 必须用此（``time_is_relative=true`` 时）。相对时间数据禁止被
    解释成真实日期。
    """
    return {
        "mode": "relative",
        "start_offset_s": int(start_offset_s),
        "end_offset_s": int(end_offset_s),
    }


def build_retrieval_params(
    *,
    strategy: str = "hybrid",
    rerank: bool = True,
    score_threshold: float = 0.0,
) -> dict[str, Any]:
    """task.md §3.2：``input.parameters.retrieval`` 子对象。"""
    if strategy not in RETRIEVAL_STRATEGIES:
        raise ValueError(f"unknown retrieval strategy {strategy!r}; expected one of {sorted(RETRIEVAL_STRATEGIES)}")
    return {"strategy": strategy, "rerank": bool(rerank), "score_threshold": score_threshold}


def build_budget(
    *,
    max_evidence_count: int | None = None,
    max_token_estimate: int | None = None,
) -> dict[str, Any]:
    """schema v1.1：``Envelope.budget``（可选）。

    Planner 在并行调多个 retrieve 时,通过本字段提前声明本次检索的硬上限,
    各 RAG 必须**尊重**预算 —— 在内部排序后只回 ``max_evidence_count`` 条,
    并按 ``max_token_estimate`` 截断 text/summary 长度,避免撑爆上下文。

    两个字段都可单独给出;至少必须给出一个,否则不构造 budget 对象。
    """
    budget: dict[str, Any] = {}
    if max_evidence_count is not None:
        if not isinstance(max_evidence_count, int) or isinstance(max_evidence_count, bool) or max_evidence_count <= 0:
            raise ValueError("max_evidence_count must be a positive integer")
        budget["max_evidence_count"] = max_evidence_count
    if max_token_estimate is not None:
        if not isinstance(max_token_estimate, int) or isinstance(max_token_estimate, bool) or max_token_estimate <= 0:
            raise ValueError("max_token_estimate must be a positive integer")
        budget["max_token_estimate"] = max_token_estimate
    if not budget:
        raise ValueError("budget requires at least one of max_evidence_count/max_token_estimate")
    return budget


def build_data_source(
    *,
    source_id: str,
    source_type: str,
    uri: str,
    media_type: str,
    data_type: str,
    role: str = "primary",
) -> dict[str, Any]:
    """task.md §3.1：``input.data_sources[]`` 中一条数据源（如 local 模式的本地文件）。

    注意：这里的 ``data_type`` 是数据源的**物理形态**（如 ``jsonl``），与
    ``input.parameters.data_type`` 的数据类型标签不是同一回事。
    """
    return {
        "source_id": source_id,
        "source_type": source_type,
        "uri": uri,
        "media_type": media_type,
        "data_type": data_type,
        "role": role,
    }


def build_filters(
    *,
    city: str | None = None,
    time_range: dict[str, Any] | None = None,
    geohash: str | None = None,
    bbox: list[float] | None = None,
    anomaly_only: bool | None = None,
    where: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """task.md §3.2：``input.filters``。

    非时空数据不需要填 ``city`` / ``time_range`` / ``geohash``；type-specific 的结构化
    过滤统一进 ``where``（与 network-traffic 已实现的 ``filters.where`` 对齐）。
    """
    filters: dict[str, Any] = {}
    if city is not None:
        filters["city"] = city
    if time_range is not None:
        filters["time_range"] = time_range
    if geohash is not None:
        filters["geohash"] = geohash
    if bbox is not None:
        filters["bbox"] = bbox
    if anomaly_only is not None:
        filters["anomaly_only"] = anomaly_only
    if where is not None:
        filters["where"] = where
    return filters


def build_input_envelope(
    *,
    skill_name: str,
    scenario: str,
    capability: str,
    query: str,
    data_type: str,
    request_id: str | None = None,
    top_k: int = 10,
    mode: str = "auto",
    retrieval: dict[str, Any] | None = None,
    filters: dict[str, Any] | None = None,
    data_sources: list[dict[str, Any]] | None = None,
    context: dict[str, Any] | None = None,
    budget: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """task.md §3.1：完整 Input Envelope。

    ``schema_version`` / ``request_id`` / ``skill_name`` / ``scenario`` / ``capability``
    是 Input Envelope 固定头；检索参数进 ``input.parameters``，过滤进 ``input.filters``。

    ``data_type`` 是数据类型标签（用于结果分桶 / 校验），**不是路由键** —— 路由仍走
    SkillRouter 的 ``scenes`` + ``task_types``（task.md §7 规则 10）。

    schema v1.1 起新增可选顶层 ``budget`` —— Planner 主动声明的预算上限,
    各 retrieve 必须尊重(见 :func:`build_budget`)。
    """
    if data_type not in DATA_TYPES:
        raise ValueError(f"unregistered data_type {data_type!r} (task.md §2)")
    if mode not in RUN_MODES:
        raise ValueError(f"unknown mode {mode!r}; expected one of {sorted(RUN_MODES)}")

    parameters: dict[str, Any] = {
        "query": query,
        "data_type": data_type,
        "top_k": top_k,
        "mode": mode,
    }
    if retrieval is not None:
        parameters["retrieval"] = retrieval

    envelope: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "request_id": request_id or new_request_id(),
        "skill_name": skill_name,
        "scenario": scenario,
        "capability": capability,
        "input": {
            "data_sources": list(data_sources) if data_sources else [],
            "parameters": parameters,
            "filters": filters if filters is not None else {},
            "context": context if context is not None else {},
        },
    }
    if budget is not None:
        if not isinstance(budget, dict) or not budget:
            raise ValueError("budget must be a non-empty dict; use build_budget() to construct it")
        envelope["budget"] = dict(budget)
    return envelope
