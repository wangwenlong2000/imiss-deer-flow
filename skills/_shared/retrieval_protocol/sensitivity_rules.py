"""敏感度落档规则 —— spec 2026-05-27 §3。

把师兄 2026-05-27 给出的四档语义与 6 类升降级规则锚定为代码常量与判定函数。
adapter 在构造 evidence_unit 时调用 :func:`classify_sensitivity` 做保守预标;
最终违规判定 / 处置仍由合规检测器统一决策。本模块仅依赖 Python 标准库。
"""

from __future__ import annotations

import re
from typing import Any

# spec §3.2.1: 6 类受改 data_type 默认级别表。
# (gazetteer 由 adapt_road_traffic_hit 输出 —— task.md §6.3 既有实现)
DEFAULT_SENSITIVITY: dict[str, tuple[str, str]] = {
    "gazetteer":      ("aggregated_safe", "open"),
    "telecom":        ("pii_masked",      "internal_only"),
    "code":           ("pii_masked",      "internal_only"),
    "streetview":     ("pii_masked",      "internal_only"),
    "remote_sensing": ("aggregated_safe", "open"),
    "surveillance":   ("restricted",      "restricted"),
    "spatiotemporal_trajectory": ("pii_masked", "internal_only"),
}

# spec §3.1: 第一阶段 k 阈值 —— 主体数 >= 10 才算安全聚合。
K_THRESHOLD_AGGREGATED_SAFE: int = 10

# 本次不动的 3 类(保持现有 adapter 硬编码默认): netflow / policy / traffic_flow
# 师兄未给 traffic_flow 的升降级规则; 其余 2 类暂沿用 adapters.py 既有默认。
# spatiotemporal_trajectory: 2026-05-30 新增分类器 —— 个体轨迹(对象+时间+位置可重识别)
# 升 restricted; 安全聚合(主体数>=k)降 aggregated_safe; 其余保守 pii_masked。

# 保守正则:命中即视为含明文结构化 ID。宁可误升档,不漏敏感。
_RE_PHONE   = re.compile(r"(?<!\d)1[3-9]\d{9}(?!\d)")
_RE_ID_CARD = re.compile(r"(?<!\d)\d{17}[\dXx](?!\d)")
_RE_IMEI    = re.compile(r"(?<!\d)\d{15}(?!\d)")
_RE_EMAIL   = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
_RE_IPV4    = re.compile(r"(?<!\d)(?:\d{1,3}\.){3}\d{1,3}(?!\d)")
_RE_LATLON  = re.compile(r"(?<!\d)-?\d{1,3}\.\d{4,}\s*,\s*-?\d{1,3}\.\d{4,}(?!\d)")
_RE_MAC     = re.compile(r"(?<![\w:\-])[\dA-Fa-f]{2}(?:[:\-][\dA-Fa-f]{2}){5}(?![\w:\-])")

_STRUCT_ID_PATTERNS = (_RE_PHONE, _RE_ID_CARD, _RE_IMEI, _RE_EMAIL, _RE_IPV4, _RE_LATLON, _RE_MAC)

_RE_SECRET_KV = re.compile(
    r"(?i)(?:password|passwd|secret|token|api[_-]?key|access[_-]?token|"
    r"private[_-]?key|client[_-]?secret)\s*[:=]"
)
_RE_DB_URL = re.compile(r"(?i)(?:postgres(?:ql)?|mysql|mongodb|redis)://[^\s]+:[^\s]*@")

_K_SUBJECT_FIELDS = (
    "unique_users", "unique_contacts", "unique_devices",
    "unique_targets", "community_size", "sample_count",
)

_RISK_LABEL_FIELDS = (
    "risk_label", "purefraud_flag", "mutation_flag", "case_priority",
)

_STREAMING_FIELDS = ("stream_url", "playback_url", "channel_id", "camera_id")

_OBJECT_TIME_GEO_TRIPLE = (
    # 对象组: 包含原有 + telecom 真实哈希字段
    ("target_id", "object_id", "user_id_hash",
     "src_user_id", "dst_counterparty_id"),
    # 时间组: 包含原有 + telecom 真实字段
    ("timestamp", "time_start", "captured_at", "ts",
     "event_time", "event_date", "event_hour"),
    # 位置组: 包含原有 + telecom 真实字段 (无 _id 后缀)
    ("cell_id", "station_id", "geohash", "bbox", "roaming_place",
     "station", "cell"),
)


def _text(unit: dict[str, Any]) -> str:
    """安全取 text；缺失返回空串。"""
    value = unit.get("text")
    return value if isinstance(value, str) else ""


def _features(unit: dict[str, Any]) -> dict[str, Any]:
    """安全取 features；缺失返回空 dict。"""
    value = unit.get("features")
    return value if isinstance(value, dict) else {}


def _has_struct_id(unit: dict[str, Any]) -> bool:
    """text 中是否含明文结构化 ID(手机/身份证/IMEI/邮箱/IP/精确经纬度/MAC)。"""
    text = _text(unit)
    if not text:
        return False
    return any(p.search(text) for p in _STRUCT_ID_PATTERNS)


def _has_aggregated_k_ge(unit: dict[str, Any], k: int) -> bool:
    """features 是否含可表达 k 的主体数字段且 >= k。"""
    feats = _features(unit)
    for field in _K_SUBJECT_FIELDS:
        value = feats.get(field)
        if isinstance(value, (int, float)) and not isinstance(value, bool) and value >= k:
            return True
    return False


def _has_secret_token(unit: dict[str, Any]) -> bool:
    """text 中是否含 password / token / api_key / secret / DB 连接串等。"""
    text = _text(unit)
    if not text:
        return False
    return bool(_RE_SECRET_KV.search(text) or _RE_DB_URL.search(text))


def _has_object_time_geo_combo(unit: dict[str, Any]) -> bool:
    """features 中是否同时给出对象 + 时间 + 位置三元组。"""
    feats = _features(unit)
    return all(any(field in feats for field in group) for group in _OBJECT_TIME_GEO_TRIPLE)


def _has_risk_label(unit: dict[str, Any]) -> bool:
    """features 中是否含对象级风险标签。"""
    feats = _features(unit)
    return any(field in feats for field in _RISK_LABEL_FIELDS)


def _has_streaming_link(unit: dict[str, Any]) -> bool:
    """features 中是否含视频接入链接 / 通道号 / camera_id。"""
    feats = _features(unit)
    return any(feats.get(field) for field in _STREAMING_FIELDS)


def _has_unmasked_face_plate(unit: dict[str, Any]) -> bool:
    """features.objects 中是否含未打码的人脸 / 车牌。

    objects[*] 形如 {"label": "face", "masked": True} 或纯字符串 "face"。
    未显式 ``masked=True`` 视为未打码(保守原则)。
    """
    objects = _features(unit).get("objects")
    if not isinstance(objects, list):
        return False
    targets = {"face", "license_plate"}
    for obj in objects:
        if isinstance(obj, dict):
            label = obj.get("label")
            masked = obj.get("masked")
            if label in targets and not masked:
                return True
        elif isinstance(obj, str) and obj in targets:
            return True
    return False


def _marked_public(unit: dict[str, Any]) -> bool:
    """features 中是否明确标 ``public=True`` 或 ``source_kind="public"``。"""
    feats = _features(unit)
    if feats.get("public") is True:
        return True
    return feats.get("source_kind") == "public"


_RESTRICTED = ("restricted", "restricted")
_PII = ("pii_masked", "internal_only")
_AGGREGATED_SAFE = ("aggregated_safe", "open")
_OPEN = ("open", "open")


def classify_sensitivity(
    *,
    data_type: str,
    evidence_unit: dict[str, Any],
    k_threshold: int = K_THRESHOLD_AGGREGATED_SAFE,
) -> tuple[str, str]:
    """按 data_type 与 evidence_unit 字段做保守敏感度预标。

    返回 ``(sensitivity_level, access_policy)``;未登记 data_type 抛 ``ValueError``。
    判定不确定时升一档(保守原则)。本函数只读 ``evidence_unit``,不修改入参。
    """
    if data_type not in DEFAULT_SENSITIVITY:
        raise ValueError(
            f"unknown data_type {data_type!r}; expected one of {sorted(DEFAULT_SENSITIVITY)}"
        )
    return _DISPATCH[data_type](evidence_unit, k_threshold)


def _classify_gazetteer(unit: dict[str, Any], k: int) -> tuple[str, str]:
    # 升级:小样本(k<10) / 含明文结构化 ID(可定位到具体对象或设施)
    if _has_struct_id(unit):
        return _RESTRICTED
    feats = _features(unit)
    if any(field in feats for field in _K_SUBJECT_FIELDS) and not _has_aggregated_k_ge(unit, k):
        return _RESTRICTED
    # 降级:已公开 且 无敏感信号
    if _marked_public(unit):
        return _OPEN
    return _AGGREGATED_SAFE


def _classify_telecom(unit: dict[str, Any], k: int) -> tuple[str, str]:
    # 升级:明文结构化 ID / 对象+时间+位置组合 / 对象级风险标签
    if _has_struct_id(unit) or _has_object_time_geo_combo(unit) or _has_risk_label(unit):
        return _RESTRICTED
    # 降级:聚合 + k>=10 + 不绑定单对象(_has_object_time_geo_combo 已在升级里挡掉)
    if _has_aggregated_k_ge(unit, k):
        return _AGGREGATED_SAFE
    return _PII


def _classify_code(unit: dict[str, Any], k: int) -> tuple[str, str]:
    # 升级:含密钥 / token / DB 连接串 / 内部 IP(后者由 struct_id IPv4 命中)
    if _has_secret_token(unit) or _has_struct_id(unit):
        return _RESTRICTED
    # 降级:已公开 且 无密钥
    if _marked_public(unit):
        return _OPEN
    return _PII


def _classify_streetview(unit: dict[str, Any], k: int) -> tuple[str, str]:
    # 升级:未打码人脸 / 车牌 / 精确位置(明文经纬度等)
    if _has_unmasked_face_plate(unit) or _has_struct_id(unit):
        return _RESTRICTED
    # 降级:仅目标计数 / 类别统计 + 无可识别对象 + 无精确位置 + k>=10
    feats = _features(unit)
    objects = feats.get("objects")
    only_counts = (
        ("target_count" in feats or "category" in feats)
        and (objects is None or objects == [])
    )
    if only_counts and _has_aggregated_k_ge(unit, k):
        return _AGGREGATED_SAFE
    return _PII


def _classify_remote_sensing(unit: dict[str, Any], k: int) -> tuple[str, str]:
    # 升级:精确坐标 / 内部标注 / 敏感设施点位(由 struct_id LATLON + sensitive_facility 字段)
    feats = _features(unit)
    if _has_struct_id(unit) or feats.get("sensitive_facility") or feats.get("internal_annotation"):
        return _RESTRICTED
    # 降级:已公开 且 无敏感信号
    if _marked_public(unit):
        return _OPEN
    return _AGGREGATED_SAFE


def _classify_surveillance(unit: dict[str, Any], k: int) -> tuple[str, str]:
    # 默认顶档,只允许两条降级路径
    feats = _features(unit)
    objects = feats.get("objects")
    has_streaming = _has_streaming_link(unit)
    has_unmasked = _has_unmasked_face_plate(unit)
    has_specific_location = _has_struct_id(unit) or feats.get("camera_location")

    # 降级 1:打码 + 无 streaming + 无具体点位 → pii_masked+internal_only
    if (
        isinstance(objects, list) and objects
        and not has_unmasked
        and not has_streaming
        and not has_specific_location
    ):
        return _PII

    # 降级 2:仅聚合统计(人数/车流量/目标数量) + k>=10 + 无 streaming / 未打码
    only_counts = (
        ("target_count" in feats or "people_count" in feats or "vehicle_count" in feats)
        and (objects is None or objects == [])
    )
    if only_counts and _has_aggregated_k_ge(unit, k) and not has_streaming and not has_unmasked:
        return _AGGREGATED_SAFE

    return _RESTRICTED


# 个体轨迹主体字段:命中其一即视为绑定单一个体(可重识别风险)。
# 仅用于 spatiotemporal_trajectory,不并入共享 _OBJECT_TIME_GEO_TRIPLE(避免影响 telecom)。
_TRAJ_INDIVIDUAL_FIELDS = (
    "user_id", "track_id", "trajectory_id", "device_id", "imsi", "user_id_hash",
)


def _classify_spatiotemporal_trajectory(unit: dict[str, Any], k: int) -> tuple[str, str]:
    # 升级:明文结构化 ID(含精确经纬度,由 struct_id LATLON 命中) / 对象级风险标签
    if _has_struct_id(unit) or _has_risk_label(unit):
        return _RESTRICTED
    feats = _features(unit)
    has_individual = any(field in feats for field in _TRAJ_INDIVIDUAL_FIELDS)
    # 个体轨迹(绑定单一对象)且非安全聚合 → 可重识别 → restricted
    if has_individual and not _has_aggregated_k_ge(unit, k):
        return _RESTRICTED
    # 对象+时间+位置三元组(覆盖 hash 化对象)且非安全聚合 → restricted
    if _has_object_time_geo_combo(unit) and not _has_aggregated_k_ge(unit, k):
        return _RESTRICTED
    # 降级:安全聚合(主体数 >= k 且未绑定单一个体)
    if _has_aggregated_k_ge(unit, k):
        return _AGGREGATED_SAFE
    # 其余:保守 pii_masked
    return _PII


_DISPATCH = {
    "gazetteer":      _classify_gazetteer,
    "telecom":        _classify_telecom,
    "code":           _classify_code,
    "streetview":     _classify_streetview,
    "remote_sensing": _classify_remote_sensing,
    "surveillance":   _classify_surveillance,
    "spatiotemporal_trajectory": _classify_spatiotemporal_trajectory,
}

assert _DISPATCH.keys() == DEFAULT_SENSITIVITY.keys(), (
    f"_DISPATCH keys {set(_DISPATCH)} != DEFAULT_SENSITIVITY keys {set(DEFAULT_SENSITIVITY)}"
)

__all__ = [
    "DEFAULT_SENSITIVITY",
    "K_THRESHOLD_AGGREGATED_SAFE",
    "classify_sensitivity",
]
