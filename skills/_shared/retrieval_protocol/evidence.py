"""统一结果证据 —— evidence_unit / evidence wrapper / SkillResult 构造器（task.md §4）。

检索 skill 的最终输出是标准 SkillResult。每条命中是 ``result.evidence[]`` 中一条
``retrieval`` 型 evidence wrapper，其 ``payload`` 即 ``evidence_unit``。

约定（task.md §4.1 / §4.3）：

- 检索分 ``score`` / ``rank`` / ``method`` 放在 **evidence wrapper**，不进 ``evidence_unit``；
- 附件（图片 / 热力图 / 视频帧 / 报告文件）进 ``SkillResult.result.artifacts[]``，
  不进 ``text`` 也不进 ``evidence_unit``。

本模块仅依赖 Python 标准库。
"""

from __future__ import annotations

from typing import Any

from .envelope import new_request_id
from .schema import (
    ACCESS_POLICIES,
    DATA_TYPES,
    EVIDENCE_TYPE_RETRIEVAL,
    SCHEMA_VERSION,
    SENSITIVITY_LEVELS,
)

# ---------------------------------------------------------------------------
# evidence_unit（task.md §4.2）
# ---------------------------------------------------------------------------


def build_geo_scope(
    *,
    city: str | None = None,
    geohash: str | None = None,
    bbox: list[float] | None = None,
    landmark: str | None = None,
    district: str | None = None,
    lat: float | None = None,
    lon: float | None = None,
    tags: list[str] | None = None,
) -> dict[str, Any]:
    """task.md §4.2：``meta.geo_scope``。

    非时空数据请直接传 ``geo_scope={}`` 给 :func:`build_evidence_unit` —— 它必须是
    对象类型，不得为 ``null`` 或字符串（task.md §7 规则 5）。
    """
    scope: dict[str, Any] = {}
    if city is not None:
        scope["city"] = city
    if geohash is not None:
        scope["geohash"] = geohash
    if bbox is not None:
        scope["bbox"] = bbox
    if landmark is not None:
        scope["landmark"] = landmark
    if district is not None:
        scope["district"] = district
    if lat is not None:
        scope["lat"] = lat
    if lon is not None:
        scope["lon"] = lon
    if tags is not None:
        scope["tags"] = tags
    return scope


def build_locator(
    *,
    file_path: str | None = None,
    line_start: int | None = None,
    line_end: int | None = None,
    page: Any | None = None,
    paragraph_id: str | None = None,
    section: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """task.md §4.3：``meta.locator``（文本 / 代码 / 政策定位）。

    支持 ``file_path`` / ``line_start`` / ``line_end`` / ``page`` / ``paragraph_id`` /
    ``section``，其余特化定位字段经 ``**extra`` 传入（``None`` 值会被丢弃）。
    """
    locator: dict[str, Any] = {}
    if file_path is not None:
        locator["file_path"] = file_path
    if line_start is not None:
        locator["line_start"] = line_start
    if line_end is not None:
        locator["line_end"] = line_end
    if page is not None:
        locator["page"] = page
    if paragraph_id is not None:
        locator["paragraph_id"] = paragraph_id
    if section is not None:
        locator["section"] = section
    locator.update({key: value for key, value in extra.items() if value is not None})
    return locator


def build_evidence_unit(
    *,
    evidence_id: str,
    data_type: str,
    text: str,
    source_id: str,
    features: dict[str, Any],
    source_path: str | None = None,
    time_range: dict[str, Any] | None = None,
    geo_scope: dict[str, Any] | None = None,
    granularity: str | None = None,
    sensitivity_level: str | None = None,
    access_policy: str | None = None,
    locator: dict[str, Any] | None = None,
    schema_version: str = SCHEMA_VERSION,
) -> dict[str, Any]:
    """task.md §4.2：标准 ``evidence_unit``，与 citybench 现有 JSONL 完全兼容。

    必填：``evidence_id`` / ``data_type`` / ``text`` / ``source_id`` / ``features``。
    其余字段仅在显式给出时写入。检索分与附件**不进**本结构。
    """
    if data_type not in DATA_TYPES:
        raise ValueError(f"unregistered data_type {data_type!r} (task.md §2)")
    if sensitivity_level is not None and sensitivity_level not in SENSITIVITY_LEVELS:
        raise ValueError(f"unknown sensitivity_level {sensitivity_level!r}; expected one of {sorted(SENSITIVITY_LEVELS)}")
    if access_policy is not None and access_policy not in ACCESS_POLICIES:
        raise ValueError(f"unknown access_policy {access_policy!r}; expected one of {sorted(ACCESS_POLICIES)}")

    meta: dict[str, Any] = {"source_id": source_id}
    if source_path is not None:
        meta["source_path"] = source_path
    if time_range is not None:
        meta["time_range"] = time_range
    if geo_scope is not None:
        meta["geo_scope"] = geo_scope
    if granularity is not None:
        meta["granularity"] = granularity
    if sensitivity_level is not None:
        meta["sensitivity_level"] = sensitivity_level
    if access_policy is not None:
        meta["access_policy"] = access_policy
    if locator is not None:
        meta["locator"] = locator
    # features 始终最后写入，且必为对象（task.md §7 规则 2）。
    meta["features"] = dict(features) if features is not None else {}

    return {
        "schema_version": schema_version,
        "evidence_id": evidence_id,
        "data_type": data_type,
        "text": text,
        "meta": meta,
    }


# ---------------------------------------------------------------------------
# evidence wrapper（task.md §4.1）
# ---------------------------------------------------------------------------


def build_retrieval_block(
    *,
    method: str,
    raw_scores: dict[str, Any] | None = None,
    rrf_k: int | None = None,
    matched_fields: list[str] | None = None,
) -> dict[str, Any]:
    """task.md §4.1：evidence wrapper 里的 ``retrieval`` 子对象。"""
    block: dict[str, Any] = {"method": method}
    if raw_scores is not None:
        block["raw_scores"] = raw_scores
    if rrf_k is not None:
        block["rrf_k"] = rrf_k
    if matched_fields is not None:
        block["matched_fields"] = matched_fields
    return block


def build_retrieval_evidence(
    *,
    evidence_ref: str,
    payload: dict[str, Any],
    rank: int | None = None,
    score: float | None = None,
    method: str | None = None,
    retrieval: dict[str, Any] | None = None,
    raw_scores: dict[str, Any] | None = None,
    rrf_k: int | None = None,
    matched_fields: list[str] | None = None,
) -> dict[str, Any]:
    """task.md §4.1：一条 ``retrieval`` 型 evidence wrapper。

    检索分（``score`` / ``rank`` / ``retrieval``）由 wrapper 携带，``payload`` 为
    兼容 citybench 的 ``evidence_unit``。``retrieval`` 子对象可直接传入；否则按
    ``method`` / ``raw_scores`` / ``rrf_k`` / ``matched_fields`` 自动拼装。
    """
    wrapper: dict[str, Any] = {"evidence_ref": evidence_ref, "type": EVIDENCE_TYPE_RETRIEVAL}
    if rank is not None:
        wrapper["rank"] = rank
    if score is not None:
        wrapper["score"] = score
    if retrieval is not None:
        wrapper["retrieval"] = retrieval
    elif method is not None or raw_scores is not None or rrf_k is not None or matched_fields is not None:
        wrapper["retrieval"] = build_retrieval_block(
            method=method or "unknown",
            raw_scores=raw_scores,
            rrf_k=rrf_k,
            matched_fields=matched_fields,
        )
    wrapper["payload"] = payload
    return wrapper


# ---------------------------------------------------------------------------
# SkillResult（task.md §4.1）
# ---------------------------------------------------------------------------


def build_key_metric(*, name: str, value: Any) -> dict[str, Any]:
    """task.md §4.1：``result.summary.key_metrics[]`` 中一项。"""
    return {"name": name, "value": value}


def build_summary(
    *,
    title: str,
    overview: str,
    key_metrics: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """task.md §4.1：``result.summary``。"""
    return {
        "title": title,
        "overview": overview,
        "key_metrics": key_metrics if key_metrics is not None else [],
    }


def build_artifact(
    *,
    artifact_id: str,
    type: str,  # noqa: A002 - 对齐 task.md §4.1 字段名
    title: str,
    uri: str,
    media_type: str,
) -> dict[str, Any]:
    """task.md §4.1 / §7 规则 8：``result.artifacts[]`` 中一个附件。

    图片 / 热力图 / 视频帧 / 报告文件一律登记进 artifacts，禁止混进 ``text`` 或
    ``evidence_unit``。
    """
    return {
        "artifact_id": artifact_id,
        "type": type,
        "title": title,
        "uri": uri,
        "media_type": media_type,
    }


def build_skill_result(
    *,
    skill_name: str,
    scenario: str,
    capability: str,
    summary: dict[str, Any],
    evidence: list[dict[str, Any]],
    request_id: str | None = None,
    status: str = "success",
    findings: list[dict[str, Any]] | None = None,
    artifacts: list[dict[str, Any]] | None = None,
    diagnostics: dict[str, Any] | None = None,
    errors: list[Any] | None = None,
    schema_version: str = SCHEMA_VERSION,
) -> dict[str, Any]:
    """task.md §4.1：标准 SkillResult 信封。

    ``evidence`` 即各条标准化命中（:func:`build_retrieval_evidence` 的结果）。
    """
    return {
        "schema_version": schema_version,
        "request_id": request_id or new_request_id(),
        "skill_name": skill_name,
        "scenario": scenario,
        "capability": capability,
        "status": status,
        "result": {
            "summary": summary,
            "findings": findings if findings is not None else [],
            "evidence": list(evidence),
            "artifacts": artifacts if artifacts is not None else [],
        },
        "diagnostics": diagnostics if diagnostics is not None else {},
        "errors": errors if errors is not None else [],
    }
