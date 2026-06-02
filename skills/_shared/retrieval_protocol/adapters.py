"""与现有实现的兼容映射 —— task.md §6。

每个 ``adapt_*_hit`` 函数把一个检索 skill 的**原生命中**转换成 task.md §4.1 的
evidence wrapper（``payload`` 为 §4.2 的 ``evidence_unit``）：

- :func:`adapt_citybench_hit`        —— §6.1 时空轨迹（``source`` 已是合规 evidence_unit，零改动直通）；
- :func:`adapt_network_traffic_hit`  —— §6.2 网络流量（薄映射，相对时间）；
- :func:`adapt_road_traffic_hit`     —— §6.3 交通流量年报 RAG（gazetteer）；
- :func:`adapt_policy_hit`           —— §6.4 政策法规 RAG（policy）。

:func:`build_network_traffic_skill_result` 进一步把 ``rag_search.py`` 的整份结果
字典包成完整 SkillResult，供该 skill 直接调用（task.md §1「薄映射接入」）。

本模块仅依赖 Python 标准库。
"""

from __future__ import annotations

import hashlib
import re
from typing import Any

from .evidence import (
    build_evidence_unit,
    build_key_metric,
    build_locator,
    build_retrieval_evidence,
    build_skill_result,
    build_summary,
)
from .sensitivity_rules import classify_sensitivity

# build_rag_docs.py 的 relative_hour_bucket 用 3600s 的小时桶；t+<n>s 标签据此还原。
_RELATIVE_BUCKET_RE = re.compile(r"^t\+(\d+(?:\.\d+)?)s$")
_RELATIVE_BUCKET_SECONDS = 3600


def _first_nonempty(*values: Any) -> str:
    """返回第一个非空字符串；都为空则返回空串。"""
    for value in values:
        if isinstance(value, str) and value.strip():
            return value
    return ""


# ---------------------------------------------------------------------------
# §6.1 citybench-rag-search（时空轨迹）
# ---------------------------------------------------------------------------


def adapt_citybench_hit(hit: dict[str, Any], *, rank: int | None = None) -> dict[str, Any]:
    """task.md §6.1：citybench ``search.py`` 的 ``{id, rrf_score, source}``。

    ``source`` 每条已是合规 ``evidence_unit``，**零改动**直通为 ``payload``：
    ``id → evidence_ref``、``rrf_score → score``、``source → payload``。
    """
    return build_retrieval_evidence(
        evidence_ref=hit.get("id") or hit.get("evidence_id") or "",
        rank=rank,
        score=hit.get("rrf_score", hit.get("score")),
        method="bm25_vector_rrf",
        rrf_k=60,
        payload=hit.get("source") or {},
    )


def adapt_citybench_result(hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """把 citybench 的命中列表整体映射成 evidence wrapper 列表。"""
    return [adapt_citybench_hit(hit, rank=index) for index, hit in enumerate(hits, start=1)]


# ---------------------------------------------------------------------------
# §6.2 network-traffic-analysis（网络流量）
# ---------------------------------------------------------------------------


def _network_traffic_time_range(metadata: dict[str, Any]) -> dict[str, Any] | None:
    """从 ``metadata.time_bucket``（如 ``t+3600s``）还原相对时间（task.md §3.3 / §6.2）。

    network-traffic 的桶标签是相对采集起点的偏移，**禁止**被解释成真实日期。
    """
    bucket = str(metadata.get("time_bucket") or "").strip()
    matched = _RELATIVE_BUCKET_RE.match(bucket)
    if not matched:
        return None
    start = int(float(matched.group(1)))
    return {
        "mode": "relative",
        "start_offset_s": start,
        "end_offset_s": start + _RELATIVE_BUCKET_SECONDS,
    }


def adapt_network_traffic_hit(hit: dict[str, Any], *, rank: int | None = None) -> dict[str, Any]:
    """task.md §6.2：network-traffic ``rag_search.py`` 命中映射。

    ``doc_id → evidence_id``、``summary → text``、``dataset_name → meta.source_id``、
    ``source_file → meta.source_path``、``metadata`` 字段字典 ``→ meta.features``。
    时间用 ``mode=relative``；``geo_scope`` 置 ``{}``（无空间属性但保持对象类型）。
    """
    metadata = dict(hit.get("metadata") or {})

    # metadata 即 netflow 字段字典；time_bucket 归入 time_range，不留在 features。
    features = {key: value for key, value in metadata.items() if key != "time_bucket"}
    if "anomaly_flag" not in features:
        risk = str(metadata.get("risk_level") or metadata.get("risk_bucket") or "").lower()
        features["anomaly_flag"] = bool(metadata.get("is_scan_like")) or risk in {"high", "critical", "severe"}

    payload = build_evidence_unit(
        evidence_id=hit.get("doc_id") or "",
        data_type="netflow",
        text=_first_nonempty(hit.get("summary"), hit.get("content"), hit.get("title")),
        source_id=hit.get("dataset_name") or "",
        source_path=hit.get("source_file") or None,
        time_range=_network_traffic_time_range(metadata),
        geo_scope={},  # §6.2：网络流量无空间属性，但 geo_scope 必须是对象（§7 规则 5）。
        granularity=hit.get("doc_type") or "flow_summary",
        features=features,
    )
    return build_retrieval_evidence(
        evidence_ref=hit.get("doc_id") or "",
        rank=rank,
        score=hit.get("score"),
        method="bm25_vector_rrf",
        rrf_k=60,
        matched_fields=["summary", "content"],
        payload=payload,
    )


def adapt_network_traffic_result(hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """把 network-traffic 的命中列表整体映射成 evidence wrapper 列表。"""
    return [adapt_network_traffic_hit(hit, rank=index) for index, hit in enumerate(hits, start=1)]


def build_network_traffic_skill_result(result: dict[str, Any]) -> dict[str, Any]:
    """把 network-traffic ``rag_search.py`` 的整份结果字典包成完整 SkillResult。

    ``result`` 是 ``rag_search.py`` ``--format json`` 的输出（含 ``query`` / ``hits`` /
    ``hit_count`` / ``index_name`` / ``retrieval_strategy`` / ``embedding_model`` 等）。
    """
    hits = result.get("hits") or []
    evidence = adapt_network_traffic_result(hits)
    anomaly_count = sum(
        1 for wrapper in evidence if wrapper["payload"]["meta"]["features"].get("anomaly_flag")
    )
    hit_count = result.get("hit_count", len(hits))
    query = result.get("query", "")

    summary = build_summary(
        title=f"网络流量证据检索：{query}" if query else "网络流量证据检索",
        overview=f"命中 {hit_count} 条证据，其中 {anomaly_count} 条标记异常。",
        key_metrics=[
            build_key_metric(name="result_count", value=hit_count),
            build_key_metric(name="anomaly_count", value=anomaly_count),
        ],
    )
    return build_skill_result(
        skill_name="network-traffic-analysis",
        scenario="netflow",
        capability="evidence_search",
        status="success",
        summary=summary,
        evidence=evidence,
        diagnostics={
            "data_quality": {
                "hit_count": hit_count,
                "retrieval_strategy": result.get("retrieval_strategy", "hybrid-text-plus-vector"),
            },
            "runtime": {
                "mode": "es",
                "index_name": result.get("index_name", ""),
                "embedding_model": result.get("embedding_model", ""),
            },
        },
    )


# ---------------------------------------------------------------------------
# §6.3 road-traffic 年报 RAG（gazetteer）
# ---------------------------------------------------------------------------


def _road_traffic_evidence_id(hit: dict[str, Any]) -> str:
    """稳定 evidence_id：优先用原生 id，否则由 section_path + pages 哈希得出。"""
    explicit = hit.get("evidence_id") or hit.get("doc_id") or hit.get("id")
    if explicit:
        return str(explicit)
    seed = f"{hit.get('section_path', '')}#p{hit.get('pages', '')}"
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
    return f"gazetteer_{digest}"


def adapt_road_traffic_hit(
    hit: dict[str, Any],
    *,
    rank: int | None = None,
    source_id: str = "road_traffic_annual_report",
) -> dict[str, Any]:
    """task.md §6.3：road-traffic 年报 RAG ``rag_xian2024_min.py`` 命中映射。

    ``preview → text``、``section_path + pages → meta.locator``、
    ``distance / rerank_score → evidence wrapper.retrieval.raw_scores``、
    ``data_type = "gazetteer"``。

    spec 2026-05-27 §3：``sensitivity_level`` / ``access_policy`` 由
    :func:`classify_sensitivity` 按字段内容预标。
    """
    raw_scores: dict[str, Any] = {}
    if hit.get("distance") is not None:
        raw_scores["distance"] = hit["distance"]
    if hit.get("rerank_score") is not None:
        raw_scores["rerank_score"] = hit["rerank_score"]

    features = {
        key: hit[key]
        for key in (
            "section_path", "pages", "year", "region", "metric_name",
            "value", "unit", "source_kind", "public", "unique_targets",
        )
        if hit.get(key) is not None
    }
    text = _first_nonempty(hit.get("preview"), hit.get("text"), hit.get("section_path"))
    level, policy = classify_sensitivity(
        data_type="gazetteer",
        evidence_unit={"text": text, "features": features},
    )

    payload = build_evidence_unit(
        evidence_id=_road_traffic_evidence_id(hit),
        data_type="gazetteer",
        text=text,
        source_id=hit.get("source_id") or source_id,
        source_path=hit.get("source_path") or None,
        granularity="paragraph",
        locator=build_locator(section=hit.get("section_path"), page=hit.get("pages")),
        sensitivity_level=level,
        access_policy=policy,
        features=features,
    )
    return build_retrieval_evidence(
        evidence_ref=_road_traffic_evidence_id(hit),
        rank=rank,
        score=hit.get("rerank_score", hit.get("score")),
        method="vector_rerank",
        raw_scores=raw_scores or None,
        matched_fields=["preview"],
        payload=payload,
    )


def adapt_road_traffic_result(hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """把 road-traffic 年报 RAG 的命中列表整体映射成 evidence wrapper 列表。"""
    return [adapt_road_traffic_hit(hit, rank=index) for index, hit in enumerate(hits, start=1)]


# ---------------------------------------------------------------------------
# §6.4 policies-regulations RAG（policy）
# ---------------------------------------------------------------------------


def adapt_policy_hit(
    hit: dict[str, Any],
    *,
    rank: int | None = None,
    source_id: str = "policies_regulations",
) -> dict[str, Any]:
    """task.md §6.4：policies-regulations RAG 命中映射。

    ``doc_id → evidence_id``、``text → text``（必要时裁成摘要）、
    ``title / policy_number / page / paragraph → meta.locator + meta.features``、
    ``bm25_score / vector_score → evidence wrapper.retrieval.raw_scores``、
    ``data_type = "policy"``。
    """
    raw_scores: dict[str, Any] = {}
    if hit.get("bm25_score") is not None:
        raw_scores["bm25_score"] = hit["bm25_score"]
    if hit.get("vector_score") is not None:
        raw_scores["vector_score"] = hit["vector_score"]

    features = {
        key: hit[key]
        for key in ("doc_id", "policy_number", "effective_date", "issuer", "title")
        if hit.get(key) is not None
    }

    payload = build_evidence_unit(
        evidence_id=hit.get("doc_id") or "",
        data_type="policy",
        text=_first_nonempty(hit.get("summary"), hit.get("text"), hit.get("title")),
        source_id=hit.get("source_id") or source_id,
        source_path=hit.get("source_path") or None,
        granularity="paragraph",
        locator=build_locator(page=hit.get("page"), paragraph_id=hit.get("paragraph")),
        features=features,
    )
    return build_retrieval_evidence(
        evidence_ref=hit.get("doc_id") or "",
        rank=rank,
        score=hit.get("score"),
        method="bm25_vector_rrf",
        raw_scores=raw_scores or None,
        matched_fields=["text", "title"],
        payload=payload,
    )


def adapt_policy_result(hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """把 policies-regulations RAG 的命中列表整体映射成 evidence wrapper 列表。"""
    return [adapt_policy_hit(hit, rank=index) for index, hit in enumerate(hits, start=1)]


# ===========================================================================
# schema v1.1：剩余 6 类数据 adapter
#
# 设计原则:
# 1. 每个 adapter 接受**原生命中字典**(各 RAG 自己的输出形态),返回标准 evidence
#    wrapper(payload 为 §4.2 evidence_unit)。
# 2. 图像 / 视频的二进制附件由上层 skill 在装配 SkillResult 时挂到
#    ``result.artifacts[]``,本层只在 evidence_unit 里保留可定位指针(bbox、tile_id、
#    clip_start/end)和文本描述。
# 3. 字段缺失一律采用「不写」策略 —— 校验时 §7 各规则会自动放行可选项。
# ===========================================================================


# ---------------------------------------------------------------------------
# §v1.1 traffic_flow（交通流量,结构化 + 时空）
# ---------------------------------------------------------------------------


def adapt_traffic_flow_hit(
    hit: dict[str, Any],
    *,
    rank: int | None = None,
    source_id: str = "traffic_flow_stream",
) -> dict[str, Any]:
    """traffic_flow 命中映射。预期字段:``doc_id`` / ``summary`` / ``geohash`` /
    ``city`` / ``time_bucket`` / ``flow_count`` / ``peak_hour`` / ``avg_speed`` /
    ``congestion_level`` / ``wow_change_pct`` / ``anomaly_flag``。
    """
    feature_keys = ("flow_count", "peak_hour", "avg_speed", "congestion_level",
                    "wow_change_pct", "anomaly_flag")
    features = {key: hit[key] for key in feature_keys if hit.get(key) is not None}

    geo: dict[str, Any] = {}
    if hit.get("city"):
        geo["city"] = hit["city"]
    if hit.get("geohash"):
        geo["geohash"] = hit["geohash"]
    if hit.get("bbox"):
        geo["bbox"] = hit["bbox"]

    time_range = None
    if hit.get("time_start") and hit.get("time_end"):
        time_range = {
            "mode": "absolute",
            "start": hit["time_start"],
            "end": hit["time_end"],
            "timezone": hit.get("timezone") or "Asia/Shanghai",
        }

    payload = build_evidence_unit(
        evidence_id=hit.get("doc_id") or hit.get("evidence_id") or "",
        data_type="traffic_flow",
        text=_first_nonempty(hit.get("summary"), hit.get("text"), hit.get("title")),
        source_id=hit.get("source_id") or source_id,
        source_path=hit.get("source_path") or None,
        time_range=time_range,
        geo_scope=geo,
        granularity=hit.get("granularity") or "road_segment_hour",
        features=features,
    )
    return build_retrieval_evidence(
        evidence_ref=hit.get("doc_id") or "",
        rank=rank,
        score=hit.get("score"),
        method="bm25_vector_rrf",
        rrf_k=60,
        matched_fields=["summary"],
        payload=payload,
    )


def adapt_traffic_flow_result(hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [adapt_traffic_flow_hit(hit, rank=index) for index, hit in enumerate(hits, start=1)]


# ---------------------------------------------------------------------------
# §v1.1 telecom（电话网络,结构化 + 社区/聚合特征）
# ---------------------------------------------------------------------------


def adapt_telecom_hit(
    hit: dict[str, Any],
    *,
    rank: int | None = None,
    source_id: str = "telecom_cdr",
) -> dict[str, Any]:
    """telecom 命中映射。features 采用「灵活字典 + 保留字段保护」策略,
    兼容 4 类 evidence(用户节点 / 通话边 / 设备关系 / 聚合统计)。

    保留字段(不进 features): doc_id / evidence_id / summary / text / title /
    score / source_id / source_path / granularity / time_start / time_end /
    timezone。其余字段一律透传进 features,由 :func:`classify_sensitivity`
    按字段内容预标级别。

    spec 2026-05-27 §3 + 补充回复 5/29: 级别由 classify_sensitivity 按字段
    内容预标(明文 ID 升 restricted; 哈希 user_id + event_time + station/cell
    组合升 restricted; 聚合统计 k>=10 降 aggregated_safe)。
    """
    _WRAPPER_RESERVED = {
        "doc_id", "evidence_id", "summary", "text", "title", "score",
        "source_id", "source_path", "granularity",
        "time_start", "time_end", "timezone",
    }
    features = {k: v for k, v in hit.items() if k not in _WRAPPER_RESERVED and v is not None}

    time_range = None
    if hit.get("time_start") and hit.get("time_end"):
        time_range = {
            "mode": "absolute",
            "start": hit["time_start"],
            "end": hit["time_end"],
            "timezone": hit.get("timezone") or "Asia/Shanghai",
        }

    text = _first_nonempty(hit.get("summary"), hit.get("text"), hit.get("title"))
    level, policy = classify_sensitivity(
        data_type="telecom",
        evidence_unit={"text": text, "features": features},
    )

    payload = build_evidence_unit(
        evidence_id=hit.get("doc_id") or hit.get("evidence_id") or "",
        data_type="telecom",
        text=text,
        source_id=hit.get("source_id") or source_id,
        source_path=hit.get("source_path") or None,
        time_range=time_range,
        geo_scope={},
        granularity=hit.get("granularity") or "user_window",
        sensitivity_level=level,
        access_policy=policy,
        features=features,
    )
    return build_retrieval_evidence(
        evidence_ref=hit.get("doc_id") or "",
        rank=rank,
        score=hit.get("score"),
        method="bm25_vector_rrf",
        rrf_k=60,
        matched_fields=["summary"],
        payload=payload,
    )


def adapt_telecom_result(hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [adapt_telecom_hit(hit, rank=index) for index, hit in enumerate(hits, start=1)]


# ---------------------------------------------------------------------------
# §v1.1 code（代码片段,文本 + 精确定位）
# ---------------------------------------------------------------------------


def adapt_code_hit(
    hit: dict[str, Any],
    *,
    rank: int | None = None,
    source_id: str = "code_index",
) -> dict[str, Any]:
    """code 命中映射。预期字段:``doc_id`` / ``snippet`` / ``file_path`` /
    ``line_start`` / ``line_end`` / ``lang`` / ``ast_node_type`` / ``symbol``。

    ``snippet → text``;``file_path + line_start/end → locator``;
    ``lang / ast_node_type / symbol`` 进 ``features``。

    **5/29 备注**: 代码片段 skill 本期仅做敏感词分析,不参与 retrieval。
    本 adapter 接口保留,供未来检索接入。当前调用方应直接调用敏感词分析
    skill, 而不经本 adapter。
    """
    locator = build_locator(
        file_path=hit.get("file_path"),
        line_start=hit.get("line_start"),
        line_end=hit.get("line_end"),
        symbol=hit.get("symbol"),
    )
    features = {
        key: hit[key]
        for key in (
            "doc_id", "snippet", "lang", "ast_node_type", "symbol",
            "repo", "source_kind", "public",
        )
        if hit.get(key) is not None
    }

    raw_scores: dict[str, Any] = {}
    if hit.get("bm25_score") is not None:
        raw_scores["bm25_score"] = hit["bm25_score"]
    if hit.get("vector_score") is not None:
        raw_scores["vector_score"] = hit["vector_score"]

    text = _first_nonempty(hit.get("snippet"), hit.get("text"), hit.get("summary"))
    level, policy = classify_sensitivity(
        data_type="code",
        evidence_unit={"text": text, "features": features},
    )

    payload = build_evidence_unit(
        evidence_id=hit.get("doc_id") or hit.get("evidence_id") or "",
        data_type="code",
        text=text,
        source_id=hit.get("source_id") or source_id,
        source_path=hit.get("source_path") or hit.get("file_path") or None,
        geo_scope={},  # 代码无空间属性;保持对象。
        granularity=hit.get("granularity") or "snippet",
        locator=locator if locator else None,
        sensitivity_level=level,
        access_policy=policy,
        features=features,
    )
    return build_retrieval_evidence(
        evidence_ref=hit.get("doc_id") or "",
        rank=rank,
        score=hit.get("score"),
        method="bm25_vector_rrf",
        raw_scores=raw_scores or None,
        matched_fields=["snippet", "symbol"],
        payload=payload,
    )


def adapt_code_result(hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [adapt_code_hit(hit, rank=index) for index, hit in enumerate(hits, start=1)]


# ---------------------------------------------------------------------------
# §v1.1 streetview（街景图像,目标检测 + bbox）
# ---------------------------------------------------------------------------


def adapt_streetview_hit(
    hit: dict[str, Any],
    *,
    rank: int | None = None,
    source_id: str = "streetview_index",
) -> dict[str, Any]:
    """streetview 命中映射。

    支持两种输入形态:
    1. 旧形态(顶层): doc_id / caption / objects / bbox / taken_at / lat / lon /
       city / image_uri
    2. 5/29 新形态(嵌套): _id / source_path / metadata.{latitude, longitude,
       address.{formatted_address, business, country, province, city,
       district, street, street_number, adcode, sematic_description,
       pois[], roads[]}}; 无 bbox / 无人脸号牌遮挡

    优先取嵌套形态字段;两种形态可并存。
    """
    metadata = hit.get("metadata") or {}
    if not isinstance(metadata, dict):
        metadata = {}
    address = metadata.get("address")

    # features: 既有字段 + 新增 address 嵌套结构
    features: dict[str, Any] = {}
    for key in (
        "objects", "bbox", "taken_at", "heading", "image_uri",
        "target_count", "category", "source_kind", "public", "unique_targets",
    ):
        if hit.get(key) is not None:
            features[key] = hit[key]
    if isinstance(address, dict):
        features["address"] = address

    # geo_scope: 优先 metadata.* > 顶层
    geo: dict[str, Any] = {}
    lat = metadata.get("latitude")
    if lat is None:
        lat = hit.get("lat")
    lon = metadata.get("longitude")
    if lon is None:
        lon = hit.get("lon")
    if lat is not None:
        geo["lat"] = lat
    if lon is not None:
        geo["lon"] = lon
    city = address.get("city") if isinstance(address, dict) else None
    if not city:
        city = hit.get("city")
    if city:
        geo["city"] = city
    district = address.get("district") if isinstance(address, dict) else None
    if not district:
        district = hit.get("district")
    if district:
        geo["district"] = district
    if hit.get("bbox"):
        geo["bbox"] = hit["bbox"]

    time_range = None
    if hit.get("taken_at"):
        time_range = {
            "mode": "absolute",
            "start": hit["taken_at"],
            "end": hit["taken_at"],
            "timezone": hit.get("timezone") or "Asia/Shanghai",
        }

    text = _first_nonempty(
        hit.get("caption"),
        hit.get("text"),
        hit.get("summary"),
        address.get("formatted_address") if isinstance(address, dict) else "",
    )
    level, policy = classify_sensitivity(
        data_type="streetview",
        evidence_unit={"text": text, "features": features},
    )

    payload = build_evidence_unit(
        evidence_id=hit.get("doc_id") or hit.get("evidence_id") or hit.get("_id") or "",
        data_type="streetview",
        text=text,
        source_id=hit.get("source_id") or source_id,
        source_path=hit.get("source_path") or hit.get("image_uri") or None,
        time_range=time_range,
        geo_scope=geo,
        granularity=hit.get("granularity") or "image",
        sensitivity_level=level,
        access_policy=policy,
        features=features,
    )
    return build_retrieval_evidence(
        evidence_ref=hit.get("doc_id") or hit.get("_id") or "",
        rank=rank,
        score=hit.get("score"),
        method="clip_vector",
        matched_fields=["caption", "objects"],
        payload=payload,
    )


def adapt_streetview_result(hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [adapt_streetview_hit(hit, rank=index) for index, hit in enumerate(hits, start=1)]


# ---------------------------------------------------------------------------
# §v1.1 remote_sensing（卫星遥感,瓦片 + 变化检测）
# ---------------------------------------------------------------------------


def adapt_remote_sensing_hit(
    hit: dict[str, Any],
    *,
    rank: int | None = None,
    source_id: str = "remote_sensing_index",
) -> dict[str, Any]:
    """remote_sensing 命中映射。

    支持两种输入形态:
    1. 旧形态: doc_id / caption / objects / bbox / tile_id / taken_at /
       change_score / cloud_cover / image_uri
    2. 5/29 新形态: id / title / content / similarity / rank / url / hash /
       resolution / exif

    优先取新形态字段;两种形态可并存。
    """
    # features: 旧字段 + 新字段
    features: dict[str, Any] = {}
    for key in (
        "objects", "bbox", "tile_id", "taken_at", "change_score",
        "cloud_cover", "image_uri", "sensitive_facility", "internal_annotation",
        "source_kind", "public",
        "id", "title", "content", "similarity", "url", "hash",
        "resolution", "exif",
    ):
        if hit.get(key) is not None:
            features[key] = hit[key]

    geo: dict[str, Any] = {}
    if hit.get("bbox"):
        geo["bbox"] = hit["bbox"]
    if hit.get("city"):
        geo["city"] = hit["city"]
    if hit.get("district"):
        geo["district"] = hit["district"]

    time_range = None
    if hit.get("taken_at"):
        time_range = {
            "mode": "absolute",
            "start": hit["taken_at"],
            "end": hit.get("taken_at_end") or hit["taken_at"],
            "timezone": hit.get("timezone") or "Asia/Shanghai",
        }

    # text: 优先 title + content (新形态), 回落 caption / text / summary (旧形态)
    title = hit.get("title")
    content = hit.get("content")
    if isinstance(title, str) and isinstance(content, str) and title.strip() and content.strip():
        text = f"{title}\n{content}"
    else:
        text = _first_nonempty(
            title, content,
            hit.get("caption"), hit.get("text"), hit.get("summary"),
        )

    level, policy = classify_sensitivity(
        data_type="remote_sensing",
        evidence_unit={"text": text, "features": features},
    )

    # evidence_id: 优先 id (新), 回落 doc_id / evidence_id (旧)
    evidence_id = hit.get("id") or hit.get("doc_id") or hit.get("evidence_id") or ""
    # source_path: 优先 url (新), 回落 source_path / image_uri (旧)
    source_path = hit.get("url") or hit.get("source_path") or hit.get("image_uri") or None
    # score: 优先 similarity (新), 回落 score (旧)
    # similarity 可能为 0.0 (perfectly dissimilar), 不能用 or 短路, 必须用 is not None
    score = hit.get("similarity") if hit.get("similarity") is not None else hit.get("score")

    payload = build_evidence_unit(
        evidence_id=evidence_id,
        data_type="remote_sensing",
        text=text,
        source_id=hit.get("source_id") or source_id,
        source_path=source_path,
        time_range=time_range,
        geo_scope=geo,
        granularity=hit.get("granularity") or "tile",
        sensitivity_level=level,
        access_policy=policy,
        features=features,
    )
    return build_retrieval_evidence(
        evidence_ref=evidence_id,
        rank=rank,
        score=score,
        method="clip_vector",
        matched_fields=["caption", "objects"],
        payload=payload,
    )


def adapt_remote_sensing_result(hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [adapt_remote_sensing_hit(hit, rank=index) for index, hit in enumerate(hits, start=1)]


# ---------------------------------------------------------------------------
# §v1.1 surveillance（视频监控,片段 + 行为识别）
# ---------------------------------------------------------------------------


def adapt_surveillance_hit(
    hit: dict[str, Any],
    *,
    rank: int | None = None,
    source_id: str = "surveillance_index",
) -> dict[str, Any]:
    """surveillance 命中映射。

    支持两种输入形态:
    1. 旧形态: doc_id / summary / objects / bbox / clip_start / clip_end /
       behavior / camera_id / camera_lat / camera_lon / stream_url / ...
    2. 5/29 新形态: video_id / camera_id / filename / raw_segment_uri /
       started_at / ended_at / labels / object_summary / location.{city,
       camera_lat, camera_lon} / metadata.{...}; 无逐帧 bbox。

    敏感度: 默认 restricted; 已打码 + 无 streaming + 无具体点位才降到
    pii_masked; 仅聚合统计 + k>=10 + 无 streaming/未打码才降到
    aggregated_safe (见 sensitivity_rules._classify_surveillance)。
    """
    location = hit.get("location") or {}
    if not isinstance(location, dict):
        location = {}

    # features: 旧字段 + 新字段一并保留
    feature_keys = (
        # 旧形态
        "objects", "bbox", "clip_start", "clip_end", "behavior",
        "video_uri", "stream_url", "playback_url", "channel_id",
        "camera_location", "target_count", "people_count", "vehicle_count",
        "unique_targets", "source_kind", "public",
        # 5/29 新形态
        "video_id", "camera_id", "filename", "raw_segment_uri",
        "started_at", "ended_at", "labels", "object_summary",
        "location", "metadata",
    )
    features = {key: hit[key] for key in feature_keys if hit.get(key) is not None}

    # geo_scope: 优先 location.* > 顶层
    geo: dict[str, Any] = {}
    city = location.get("city") or hit.get("city")
    if city:
        geo["city"] = city
    camera_lat = location.get("camera_lat", hit.get("camera_lat"))
    if camera_lat is not None:
        geo["lat"] = camera_lat
    camera_lon = location.get("camera_lon", hit.get("camera_lon"))
    if camera_lon is not None:
        geo["lon"] = camera_lon
    if hit.get("landmark"):
        geo["landmark"] = hit["landmark"]

    # time_range: 优先 started_at/ended_at > clip_start/clip_end
    start = hit.get("started_at") or hit.get("clip_start")
    end = hit.get("ended_at") or hit.get("clip_end")
    time_range = None
    if start and end:
        time_range = {
            "mode": "absolute",
            "start": start,
            "end": end,
            "timezone": hit.get("timezone") or "Asia/Shanghai",
        }

    labels = hit.get("labels")
    labels_text = " ".join(str(item) for item in labels) if isinstance(labels, list) and labels else ""
    text = _first_nonempty(
        hit.get("caption"),
        hit.get("text"),
        hit.get("summary"),
        hit.get("filename"),
        labels_text,
    )
    level, policy = classify_sensitivity(
        data_type="surveillance",
        evidence_unit={"text": text, "features": features},
    )

    payload = build_evidence_unit(
        evidence_id=hit.get("video_id") or hit.get("doc_id") or hit.get("evidence_id") or "",
        data_type="surveillance",
        text=text,
        source_id=hit.get("source_id") or source_id,
        source_path=hit.get("raw_segment_uri") or hit.get("source_path") or hit.get("video_uri") or None,
        time_range=time_range,
        geo_scope=geo,
        granularity=hit.get("granularity") or "clip",
        sensitivity_level=level,
        access_policy=policy,
        features=features,
    )
    return build_retrieval_evidence(
        evidence_ref=hit.get("video_id") or hit.get("doc_id") or "",
        rank=rank,
        score=hit.get("score"),
        method="clip_vector_temporal",
        matched_fields=["caption", "behavior"],
        payload=payload,
    )


def adapt_surveillance_result(hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [adapt_surveillance_hit(hit, rank=index) for index, hit in enumerate(hits, start=1)]
