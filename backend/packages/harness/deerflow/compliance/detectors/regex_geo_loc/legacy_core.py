#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
geo_loc_detector.py

违规类型二：精确地理位置泄露检测器。

本版本为“最终 geo_loc 阶段版”：
- 覆盖 spatiotemporal_trajectory、telecom、remote_sensing/satellite_remote_sensing。
- 图像类数据不直接做 OCR；要求遥感 adapter/人工预抽取已把图片中的经纬度写入 captions、content_text 或 features。

实现思路：
1. 统一读取 normalized JSONL，也兼容原始标注 JSONL。
2. 检测 content_text + features + raw_content/content 中的经纬度、详细地址、通信位置字段。
3. 采用字段规则 + 正则 + data_type 组合规则。
4. Microsoft Presidio LOCATION 仅作为可选辅助证据，不作为默认触发依据。
"""
from __future__ import annotations

import argparse
import json
import math
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

TARGET = "geo_loc"
DETECTOR_VERSION = "geo_loc_detector_rule_final_v2.0"

# 当前已确认接入第 2 类的数据类型。
SUPPORTED_DATA_TYPES = {"spatiotemporal_trajectory", "telecom", "remote_sensing"}
DATA_TYPE_ALIASES = {
    "phone_network": "telecom",
    "satellite_remote_sensing": "remote_sensing",
    "satellite_remote_sensing_image": "remote_sensing",
}

# 字段名规则：精确坐标字段。
LAT_FIELDS = {"lat", "latitude", "gps_lat"}
LON_FIELDS = {"lon", "lng", "longitude", "gps_lon", "gps_lng"}
COORD_FIELDS = {"coordinate", "coordinates", "gps", "gps_coordinate", "location_coord", "visible_coordinate_text", "synthetic_coordinate"}

# 字段名规则：详细地址/语义位置字段。
ADDRESS_FIELDS = {
    "address", "detail_address", "detailed_address", "full_address", "doorplate", "house_no",
    "semantic_location", "precise_location", "raw_precise_location", "poi", "poi_name", "location_name",
}

# 电话网络通信位置字段。单独出现不一定判正，通常要求对象+时间组合。
TELECOM_GEO_FIELDS = {"station", "base_station", "cell", "cell_id", "roaming_place", "lac", "ci"}

# 粗粒度地理字段，单独出现不判 geo_loc。
COARSE_GEO_FIELDS = {
    "province", "city", "county", "district", "region", "area", "country", "prefecture",
    "location_granularity", "granularity", "geohash", "geohash5", "grid", "grid_level",
}

OBJECT_FIELDS = {
    "object_id", "user_id", "src_user_id", "dst_user_id", "person_id", "track_id", "vehicle_id",
    "phone", "mobile", "imei", "device_id", "plate_no", "license_plate",
}

TIME_FIELDS = {
    "timestamp", "event_time", "event_date", "event_hour", "call_time", "start_time", "end_time",
    "record_time", "capture_time", "time", "datetime", "date_time",
}

# 文本规则：4 位及以上小数的坐标对。示例：30.001137,120.001219。
GPS_PAIR_RE = re.compile(r"(?<!\d)([-+]?\d{1,2}\.\d{4,})\s*[,，]\s*([-+]?\d{1,3}\.\d{4,})(?!\d)")

# Python's Unicode ``\b`` does not see a boundary between Chinese text and
# ``lat``/``lon`` because both are classified as word characters.  Use an
# ASCII-identifier boundary on the left so natural text such as ``位置为lat=...``
# is detected without matching inside names such as ``user_lat``.
LAT_ASSIGN_RE = re.compile(r"(?i)(?<![A-Za-z0-9_])(?:lat|latitude)\b\s*[:=：]\s*([-+]?\d{1,2}\.\d{4,})")
LON_ASSIGN_RE = re.compile(r"(?i)(?<![A-Za-z0-9_])(?:lon|lng|longitude)\b\s*[:=：]\s*([-+]?\d{1,3}\.\d{4,})")

# 电话网络字段式表达：station=xxx、cell=xxx、roaming_place=xxx。
TELECOM_FIELD_ASSIGN_RE = re.compile(
    r"(?i)\b(station|cell|cell_id|base_station|roaming_place|lac|ci)\b\s*[:=：]\s*([A-Za-z0-9_\-\.]{6,})"
)

# 详细地址规则：强调“路/街/巷/小区/号/栋/单元/室”等组合，避免只把“示例市”这类城市级词判正。
CN_DETAIL_ADDRESS_RE = re.compile(
    r"[\u4e00-\u9fffA-Za-z0-9]{2,}(?:路|街|巷|大道|小区|园区|社区|村|号|栋|幢|单元|室|楼|门牌)"
    r"[\u4e00-\u9fffA-Za-z0-9\-号栋幢单元室楼附近]*"
)

TIME_TEXT_RE = re.compile(r"\b20\d{2}[-/]\d{1,2}[-/]\d{1,2}(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?\b")
OBJECT_TEXT_RE = re.compile(r"\b(?:user|hash_u|U|obj|object|src|device)[A-Za-z0-9_\-]{2,}\b|某对象")
TRAJECTORY_OBJECT_RE = re.compile(r"\b(?:TRK|GEO|SP)-\d{2,4}\b", re.I)
HHMM_RE = re.compile(r"(?<!\d)(?:[01]\d|2[0-3]):[0-5]\d(?!\d)")
ROUTE_SEP_RE = re.compile(r"(?:->|→|➡|至|到)")
PUBLIC_FACILITY_HINTS = ["政务服务大厅", "政务服务中心", "民政局", "政府办公", "公共服务窗口"]
PUBLIC_NAV_HINTS = ["公开", "公众导航", "导航指引", "办公地址"]
PUBLIC_TRAFFIC_INFRA_HINTS = ["立交桥", "互通", "高速公路", "车流量", "通行状况"]

# 负向/泛化/聚合语境：出现这些词时，要谨慎，不应单凭字段名或文本中的 station/cell/坐标词判正。
NEGATION_OR_SAFE_HINTS = [
    "不包含", "未包含", "没有", "未出现", "不展示", "未展示", "不输出", "未输出", "隐藏", "移除",
    "仅展示", "仅保留", "只返回", "只输出", "仅写", "不会输出", "脱敏", "已脱敏",
    "省级", "城市级", "市级", "区县级", "行政区", "粗粒度", "聚合", "汇总", "统计", "趋势",
    "geohash5", "unique_users", "k匿名", "2 公里网格", "2公里网格", "网格", "虚构", "示意", "不代表真实点位",
    "坐标已移除", "coordinates_removed", "coordinate_removed", "coordinates removed",
]

ADDRESS_FALSE_POSITIVE_HINTS = ["号码", "编号", "节点", "字段", "数据集", "样本", "表头", "指标", "统计", "汇总"]

PLACEHOLDER_VALUES = {"", "none", "null", "nan", "n/a", "na", "unknown", "未知", "空", "masked", "脱敏", "***", "******"}


def read_jsonl(path: str | Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with Path(path).open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON at {path}:{line_no}: {e}")
    return rows


def write_jsonl(path: str | Path, rows: Iterable[Dict[str, Any]]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def flatten(obj: Any, prefix: str = "") -> List[Tuple[str, Any]]:
    out: List[Tuple[str, Any]] = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f"{prefix}.{k}" if prefix else str(k)
            out.extend(flatten(v, key))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            key = f"{prefix}[{i}]" if prefix else f"[{i}]"
            out.extend(flatten(v, key))
    else:
        out.append((prefix, obj))
    return out


def stringify(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and math.isnan(v):
        return ""
    return str(v)


def canonical_data_type(value: Any) -> str:
    dt = str(value or "")
    return DATA_TYPE_ALIASES.get(dt, dt)


def field_basename(path: str) -> str:
    name = path.split(".")[-1]
    name = re.sub(r"\[\d+\]$", "", name)
    return name.lower()


def has_meaningful_value(v: Any) -> bool:
    text = stringify(v).strip()
    if not text:
        return False
    low = text.lower()
    if low in PLACEHOLDER_VALUES:
        return False
    if low in LAT_FIELDS | LON_FIELDS | COORD_FIELDS | ADDRESS_FIELDS | TELECOM_GEO_FIELDS | COARSE_GEO_FIELDS:
        return False
    return True


def is_safe_or_negated_text(text: str) -> bool:
    low = text.lower()
    return any(h.lower() in low for h in NEGATION_OR_SAFE_HINTS)


def is_plausible_lat(lat: float) -> bool:
    return -90 <= lat <= 90


def is_plausible_lon(lon: float) -> bool:
    return -180 <= lon <= 180


def decimal_places(num_text: str) -> int:
    return len(num_text.split(".")[-1]) if "." in num_text else 0


def build_risk(field_path: str, text: str, risk_type: str, method: str, score: float, reason: str) -> Dict[str, Any]:
    return {
        "field_path": field_path,
        "text": text,
        "risk_type": risk_type,
        "method": method,
        "score": round(float(score), 4),
        "reason": reason,
    }


class OptionalPresidio:
    def __init__(self, mode: str = "never") -> None:
        self.mode = mode
        self.enabled = False
        self.init_error: Optional[str] = None
        self.analyzer = None
        if mode == "never":
            return
        try:
            from presidio_analyzer import AnalyzerEngine  # type: ignore
            self.analyzer = AnalyzerEngine()
            self.enabled = True
        except Exception as e:
            self.init_error = repr(e)
            if mode == "require":
                raise

    def analyze_locations(self, text: str, field_path: str) -> List[Dict[str, Any]]:
        if not self.enabled or not self.analyzer or not text:
            return []
        try:
            results = self.analyzer.analyze(text=text, language="en")
        except Exception:
            return []
        risks: List[Dict[str, Any]] = []
        for r in results:
            if getattr(r, "entity_type", "") != "LOCATION":
                continue
            start = int(getattr(r, "start", 0))
            end = int(getattr(r, "end", 0))
            snippet = text[start:end]
            score = float(getattr(r, "score", 0.0))
            risks.append(build_risk(field_path, snippet, "LOCATION", "presidio_location_aux", score, "Presidio LOCATION auxiliary evidence only"))
        return risks


def collect_items(sample: Dict[str, Any]) -> Tuple[List[Tuple[str, Any]], List[Tuple[str, str]]]:
    """Return field/value items and text blobs. Supports normalized and original samples."""
    items: List[Tuple[str, Any]] = []
    text_blobs: List[Tuple[str, str]] = []

    # normalized fields
    if sample.get("content_text"):
        text = stringify(sample.get("content_text"))
        items.append(("content_text", text))
        text_blobs.append(("content_text", text))

    features = sample.get("features")
    if isinstance(features, dict):
        for k, v in features.items():
            items.append((str(k), v))
            if isinstance(v, str):
                text_blobs.append((str(k), v))

    # original/raw fallback
    for root_name in ["content", "raw_content"]:
        if root_name in sample:
            for p, v in flatten(sample[root_name], root_name):
                items.append((p, v))
                if isinstance(v, str):
                    text_blobs.append((p, v))

    return items, text_blobs


def detect_coordinate_fields(items: List[Tuple[str, Any]]) -> Tuple[List[Dict[str, Any]], List[str], List[str]]:
    risks: List[Dict[str, Any]] = []
    lat_paths: List[str] = []
    lon_paths: List[str] = []
    for path, value in items:
        base = field_basename(path)
        if not has_meaningful_value(value):
            continue
        text = stringify(value).strip()
        if base in LAT_FIELDS | LON_FIELDS:
            try:
                val = float(text)
            except Exception:
                continue
            if base in LAT_FIELDS and is_plausible_lat(val):
                lat_paths.append(path)
                risks.append(build_risk(path, text, "latitude", "field_coordinate", 0.93, "latitude field contains high precision coordinate"))
            elif base in LON_FIELDS and is_plausible_lon(val):
                lon_paths.append(path)
                risks.append(build_risk(path, text, "longitude", "field_coordinate", 0.93, "longitude field contains high precision coordinate"))
        elif base in COORD_FIELDS:
            # coordinate field with GPS-like pair in value, including LAT=... LON=... text.
            for m in GPS_PAIR_RE.finditer(text):
                try:
                    lat = float(m.group(1)); lon = float(m.group(2))
                except Exception:
                    continue
                if is_plausible_lat(lat) and is_plausible_lon(lon):
                    risks.append(build_risk(path, m.group(0), "gps_coordinate", "field_coordinate_pair", 0.95, "coordinate field contains high precision lat/lon pair"))
            lat_hits = list(LAT_ASSIGN_RE.finditer(text))
            lon_hits = list(LON_ASSIGN_RE.finditer(text))
            if lat_hits and lon_hits:
                coord_text = f"{lat_hits[0].group(0)} {lon_hits[0].group(0)}"
                risks.append(build_risk(path, coord_text, "gps_coordinate", "field_coordinate_pair", 0.95, "coordinate field contains explicit LAT/LON assignments"))
    return risks, lat_paths, lon_paths


def detect_address_fields(items: List[Tuple[str, Any]]) -> Tuple[List[Dict[str, Any]], List[str]]:
    risks: List[Dict[str, Any]] = []
    address_paths: List[str] = []
    for path, value in items:
        base = field_basename(path)
        if base not in ADDRESS_FIELDS:
            continue
        if not has_meaningful_value(value):
            continue
        text = stringify(value).strip()
        if is_safe_or_negated_text(text):
            continue
        # 只要 address 字段里出现门牌/楼栋/小区/附近等具体描述即可。
        if CN_DETAIL_ADDRESS_RE.search(text) or any(k in text for k in ["raw_precise_location", "具体地点", "详细地址"]):
            address_paths.append(path)
            risks.append(build_risk(path, text[:200], "detail_address", "field_address", 0.90, "address-like field contains detailed place or doorplate-level description"))
    return risks, address_paths


def detect_telecom_fields(data_type: str, items: List[Tuple[str, Any]], text_blobs: List[Tuple[str, str]]) -> Tuple[List[Dict[str, Any]], List[str], List[str], List[str]]:
    risks: List[Dict[str, Any]] = []
    object_paths: List[str] = []
    time_paths: List[str] = []
    telecom_geo_paths: List[str] = []

    for path, value in items:
        base = field_basename(path)
        if not has_meaningful_value(value):
            continue
        if base in OBJECT_FIELDS:
            object_paths.append(path)
        if base in TIME_FIELDS:
            time_paths.append(path)
        if base in TELECOM_GEO_FIELDS:
            telecom_geo_paths.append(path)

    if data_type == "telecom":
        # 字段组合：对象 + 时间 + station/cell/roaming_place。
        if object_paths and time_paths and telecom_geo_paths:
            for path, value in items:
                base = field_basename(path)
                if base in TELECOM_GEO_FIELDS and has_meaningful_value(value):
                    risks.append(build_risk(path, stringify(value).strip(), base, "telecom_object_time_geo_field", 0.94,
                                            "telecom network-location field is bound with object identifier and time"))
        # 文本组合：某对象 + 时间 + station/cell=...
        for path, text in text_blobs:
            if is_safe_or_negated_text(text):
                continue
            assigns = list(TELECOM_FIELD_ASSIGN_RE.finditer(text))
            if not assigns:
                continue
            has_time = bool(TIME_TEXT_RE.search(text))
            has_object = bool(OBJECT_TEXT_RE.search(text))
            if has_time and has_object:
                for m in assigns:
                    risks.append(build_risk(path, m.group(0), m.group(1).lower(), "telecom_text_combo", 0.92,
                                            "text mentions object/time together with station/cell/roaming_place"))

    return risks, object_paths, time_paths, telecom_geo_paths


def is_public_non_sensitive_address_context(text: str) -> bool:
    return (
        any(h in text for h in PUBLIC_FACILITY_HINTS)
        and any(h in text for h in PUBLIC_NAV_HINTS)
    )


def detect_single_object_route(path: str, text: str) -> List[Dict[str, Any]]:
    """Detect a single trajectory object's multi-stop route with explicit times."""
    if not TRAJECTORY_OBJECT_RE.search(text):
        return []
    if len(HHMM_RE.findall(text)) < 2:
        return []
    if len(ROUTE_SEP_RE.findall(text)) < 1:
        return []
    # Require multiple place-like administrative/location tokens to avoid generic itinerary text.
    place_token_count = len(re.findall(r"(?:北京市|上海市|[\u4e00-\u9fff]{2,}(?:区|县|商圈|片区|通勤区|金融区|交通枢纽))", text))
    if place_token_count < 2:
        return []
    m = TRAJECTORY_OBJECT_RE.search(text)
    obj = m.group(0) if m else "trajectory_object"
    return [build_risk(path, obj, "single_object_precise_route", "trajectory_route_combo", 0.92,
                       "single trajectory object is linked to multiple explicit times and route locations")]


def detect_text_geo(text_blobs: List[Tuple[str, str]], data_type: str) -> List[Dict[str, Any]]:
    risks: List[Dict[str, Any]] = []
    for path, text in text_blobs:
        if not text or is_safe_or_negated_text(text):
            # 对负向/泛化文本，不做正则坐标和地址触发。
            continue
        for m in GPS_PAIR_RE.finditer(text):
            try:
                lat = float(m.group(1)); lon = float(m.group(2))
            except Exception:
                continue
            if not (is_plausible_lat(lat) and is_plausible_lon(lon)):
                continue
            # 至少 4 位小数，已由正则保证。虚构/示意在 safe hints 里已过滤。
            risks.append(build_risk(path, m.group(0), "gps_coordinate", "regex_gps_pair", 0.95, "text contains high precision latitude/longitude pair"))

        # LAT=... LON=... 这种形式。当前非遥感数据通常少见，但保留给时空文本。
        lat_hits = list(LAT_ASSIGN_RE.finditer(text))
        lon_hits = list(LON_ASSIGN_RE.finditer(text))
        if lat_hits and lon_hits:
            for lm in lat_hits[:2]:
                risks.append(build_risk(path, lm.group(0), "latitude", "regex_lat_assignment", 0.93, "text contains explicit latitude assignment"))
            for lm in lon_hits[:2]:
                risks.append(build_risk(path, lm.group(0), "longitude", "regex_lon_assignment", 0.93, "text contains explicit longitude assignment"))

        # 中文详细地址。对 telecom 文本中“station/cell”这类不走地址规则；主要用于轨迹。
        if data_type == "spatiotemporal_trajectory":
            # 公开政务服务窗口地址属于公开机构信息，不按本类敏感精确位置处理。
            public_safe = is_public_non_sensitive_address_context(text)
            for m in CN_DETAIL_ADDRESS_RE.finditer(text):
                snippet = m.group(0)
                window = text[max(0, m.start()-30):m.end()+30]
                if is_safe_or_negated_text(window):
                    continue
                if "泛化为" in window or "已泛化" in window:
                    continue
                if public_safe:
                    continue
                if any(h in snippet for h in ADDRESS_FALSE_POSITIVE_HINTS):
                    continue
                if any(h in snippet for h in PUBLIC_TRAFFIC_INFRA_HINTS):
                    continue
                # 要有门牌/楼栋/室等较具体粒度，避免只到“中心城区”。
                if not any(k in snippet for k in ["号", "栋", "幢", "单元", "室", "楼", "附近"]):
                    continue
                risks.append(build_risk(path, snippet, "detail_address", "regex_detail_address", 0.88, "text contains doorplate/building-level address expression"))

            # 单对象 + 多个明确时间 + 连续多地点路线，也属于精确位置/轨迹泄露。
            risks.extend(detect_single_object_route(path, text))
    return risks


def apply_data_type_policy(sample: Dict[str, Any], exclude_data_types: set[str]) -> Optional[Dict[str, Any]]:
    dt = canonical_data_type(sample.get("data_type") or sample.get("original_data_type") or "")
    if dt in exclude_data_types:
        return {
            "sample_id": sample.get("sample_id"),
            "data_id": sample.get("data_id"),
            "data_type": dt,
            "gate": sample.get("gate") or sample.get("trigger_gate"),
            "target_violation_type": sample.get("target_violation_type"),
            "predicted_violation_type": "none",
            "is_positive_pred": False,
            "confidence": 0.0,
            "risk_locations_pred": [],
            "reason_code": "excluded_data_type_pending_fields",
            "debug": {"excluded": True, "message": f"{dt} is excluded from current formal evaluation"},
        }
    return None


def detect_geo_loc(sample: Dict[str, Any], presidio: OptionalPresidio, exclude_data_types: set[str]) -> Dict[str, Any]:
    pre = apply_data_type_policy(sample, exclude_data_types)
    if pre is not None:
        return pre

    data_type = canonical_data_type(sample.get("data_type") or sample.get("original_data_type") or "")
    items, text_blobs = collect_items(sample)
    risks: List[Dict[str, Any]] = []

    # 通用经纬度字段/文本规则。
    coord_risks, lat_paths, lon_paths = detect_coordinate_fields(items)
    addr_risks, address_paths = detect_address_fields(items)
    text_risks = detect_text_geo(text_blobs, data_type)

    # telecom 专有组合。
    telecom_risks, object_paths, time_paths, telecom_geo_paths = detect_telecom_fields(data_type, items, text_blobs)

    risks.extend(coord_risks)
    risks.extend(addr_risks)
    risks.extend(text_risks)
    risks.extend(telecom_risks)

    # Presidio LOCATION 只作为辅助证据，不作为正式触发，避免城市/州名误报。
    aux_risks: List[Dict[str, Any]] = []
    if presidio.enabled:
        for path, text in text_blobs:
            if not is_safe_or_negated_text(text):
                aux_risks.extend(presidio.analyze_locations(text, path))

    # 去重。
    uniq: List[Dict[str, Any]] = []
    seen = set()
    for r in risks:
        key = (r.get("field_path"), r.get("text"), r.get("risk_type"), r.get("method"))
        if key in seen:
            continue
        seen.add(key)
        uniq.append(r)
    risks = uniq

    # 决策：
    # spatiotemporal: 经纬度字段或文本坐标、详细地址均可触发。
    # telecom: 必须命中 telecom_object_time_geo_field 或 telecom_text_combo；单独 city/county 不触发。
    if data_type == "telecom":
        strong_risks = [r for r in risks if str(r.get("method")) in {"telecom_object_time_geo_field", "telecom_text_combo"}]
    else:
        strong_risks = [r for r in risks if str(r.get("method")) in {
            "field_coordinate", "field_coordinate_pair", "field_address", "regex_gps_pair", "regex_lat_assignment", "regex_lon_assignment", "regex_detail_address", "trajectory_route_combo"
        }]

    pred_positive = bool(strong_risks)
    confidence = max([float(r.get("score", 0.0)) for r in strong_risks], default=0.0)
    reason_code = "geo_loc_detected" if pred_positive else "no_precise_geo_loc_detected"

    return {
        "sample_id": sample.get("sample_id"),
        "data_id": sample.get("data_id"),
        "data_type": data_type,
        "gate": sample.get("gate") or sample.get("trigger_gate"),
        "trigger_gate": sample.get("trigger_gate") or sample.get("gate"),
        "target_violation_type": sample.get("target_violation_type"),
        "predicted_violation_type": TARGET if pred_positive else "none",
        "is_positive_pred": pred_positive,
        "confidence": round(confidence, 4),
        "risk_locations_pred": strong_risks if pred_positive else [],
        "reason_code": reason_code,
        "auxiliary_risks": aux_risks[:20],
        "debug": {
            "lat_paths": lat_paths,
            "lon_paths": lon_paths,
            "address_paths": address_paths,
            "object_paths": object_paths,
            "time_paths": time_paths,
            "telecom_geo_paths": telecom_geo_paths,
            "rule_count": len(strong_risks),
        },
    }


def metric_bucket(pairs: List[Tuple[Dict[str, Any], Dict[str, Any]]]) -> Dict[str, Any]:
    tp = fp = fn = tn = 0
    for gold, pred in pairs:
        g = bool(gold.get("is_positive"))
        p = bool(pred.get("is_positive_pred"))
        if g and p:
            tp += 1
        elif (not g) and p:
            fp += 1
        elif g and (not p):
            fn += 1
        else:
            tn += 1
    total = tp + fp + fn + tn
    acc = (tp + tn) / total if total else 0.0
    prec = tp / (tp + fp) if (tp + fp) else 0.0
    rec = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * prec * rec / (prec + rec) if (prec + rec) else 0.0
    return {"TP": tp, "FP": fp, "FN": fn, "TN": tn, "accuracy": acc, "precision": prec, "recall": rec, "f1": f1, "samples": total}


def calc_report(samples: List[Dict[str, Any]], preds: List[Dict[str, Any]], excluded: set[str]) -> Dict[str, Any]:
    pairs = list(zip(samples, preds))
    report: Dict[str, Any] = {
        "target": TARGET,
        "detector_version": DETECTOR_VERSION,
        "excluded_data_types": sorted(excluded),
        "overall": metric_bucket(pairs),
    }
    for group_name, getter in [
        ("by_data_type", lambda s: s.get("data_type", "unknown")),
        ("by_gate", lambda s: s.get("gate") or s.get("trigger_gate") or "unknown"),
        ("by_sample_kind", lambda s: s.get("sample_kind", "unknown")),
    ]:
        d: Dict[str, List[Tuple[Dict[str, Any], Dict[str, Any]]]] = defaultdict(list)
        for s, p in pairs:
            d[str(getter(s))].append((s, p))
        report[group_name] = {k: metric_bucket(v) for k, v in sorted(d.items())}
    fp = []
    fn = []
    for s, p in pairs:
        g = bool(s.get("is_positive"))
        pred = bool(p.get("is_positive_pred"))
        if pred and not g:
            fp.append((s, p))
        elif g and not pred:
            fn.append((s, p))
    report["false_positive_count"] = len(fp)
    report["false_negative_count"] = len(fn)
    return report


def enriched_error_rows(samples: List[Dict[str, Any]], preds: List[Dict[str, Any]], error_type: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for s, p in zip(samples, preds):
        g = bool(s.get("is_positive"))
        pred = bool(p.get("is_positive_pred"))
        if error_type == "false_positive" and not (pred and not g):
            continue
        if error_type == "false_negative" and not (g and not pred):
            continue
        out.append({
            "error_type": error_type,
            "sample_id": s.get("sample_id"),
            "data_id": s.get("data_id"),
            "data_type": s.get("data_type"),
            "gate": s.get("gate") or s.get("trigger_gate"),
            "sample_kind": s.get("sample_kind"),
            "gold": {
                "target_violation_type": s.get("target_violation_type"),
                "final_violation_type": s.get("final_violation_type"),
                "is_positive": s.get("is_positive"),
                "risk_locations": s.get("risk_locations", []),
                "content_text": s.get("content_text") or (json.dumps(s.get("content", ""), ensure_ascii=False) if "content" in s else ""),
                "features": s.get("features", {}),
                "judgement_reason": s.get("judgement_reason"),
            },
            "prediction": p,
        })
    return out


def parse_csv_set(text: str) -> set[str]:
    if not text:
        return set()
    return {x.strip() for x in text.split(",") if x.strip()}


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Geo location leak detector: final rule version")
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--report", required=True)
    ap.add_argument("--false-positives", default=None)
    ap.add_argument("--false-negatives", default=None)
    ap.add_argument("--use-presidio", choices=["never", "auto", "require"], default="never")
    ap.add_argument("--exclude-data-types", default="", help="comma-separated data types excluded from formal evaluation")
    args = ap.parse_args(argv)

    samples_all = read_jsonl(args.input)
    excluded = parse_csv_set(args.exclude_data_types)
    # 对于正式评测，我们直接过滤掉 excluded 数据，而不是把它们当负类，以免污染指标。
    samples = [s for s in samples_all if canonical_data_type(s.get("data_type")) not in excluded]

    presidio = OptionalPresidio(args.use_presidio)
    preds = [detect_geo_loc(s, presidio, exclude_data_types=set()) for s in samples]
    write_jsonl(args.output, preds)

    report = calc_report(samples, preds, excluded)
    report["input_samples_total"] = len(samples_all)
    report["evaluated_samples"] = len(samples)
    report["skipped_samples"] = len(samples_all) - len(samples)
    report["presidio"] = {"requested_mode": args.use_presidio, "enabled": presidio.enabled, "init_error": presidio.init_error,
                           "note": "Presidio LOCATION is auxiliary only; formal decision is based on project field/regex/combination rules."}

    Path(args.report).parent.mkdir(parents=True, exist_ok=True)
    Path(args.report).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.false_positives:
        write_jsonl(args.false_positives, enriched_error_rows(samples, preds, "false_positive"))
    if args.false_negatives:
        write_jsonl(args.false_negatives, enriched_error_rows(samples, preds, "false_negative"))

    print(f"wrote {len(preds)} predictions to {args.output}")
    print(f"wrote report to {args.report}")
    if args.false_positives:
        print(f"wrote {report['false_positive_count']} false positives to {args.false_positives}")
    if args.false_negatives:
        print(f"wrote {report['false_negative_count']} false negatives to {args.false_negatives}")
    print(json.dumps(report["overall"], ensure_ascii=False, indent=2))
    print(f"evaluated_samples={len(samples)}, skipped_samples={len(samples_all)-len(samples)}, excluded={sorted(excluded)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
