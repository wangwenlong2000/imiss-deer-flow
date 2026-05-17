#!/usr/bin/env python3
"""
trajectory_tasks.py — CityBench 时空轨迹分析共享算法库
========================================================

被以下 14 个 skill 调用：
  search_spatiotemporal, detect_flow_anomaly, fuse_spatial_evidence,
  search_similar_trajectory, detect_route_anomaly, forecast_region_flow,
  profile_urban_region, analyze_od_flow, mine_trajectory_patterns,
  detect_trajectory_cooccurrence, analyze_spatiotemporal_accessibility,
  classify_trajectory_state, predict_next_location, measure_spatiotemporal_entropy,
  analyze_event_impact

设计原则：
  - 纯 Python 标准库，零第三方依赖
  - 输入支持 CSV / TSV / JSON / JSONL / CityBench evidence.jsonl 多种格式
  - 输出统一为 JSONL 主结果 + summary.json 报告
"""
from __future__ import annotations

import csv
import hashlib
import json
import math
import re
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional


# ════════════════════════════════════════════════════════════════════
#  基础工具：Geohash / 距离 / 时间解析 / IO
# ════════════════════════════════════════════════════════════════════

_GEOHASH_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"
_BASE32_DECODE = {c: i for i, c in enumerate(_GEOHASH_BASE32)}


def encode_geohash(lat: float, lon: float, precision: int = 6) -> str:
    lat_lo, lat_hi = -90.0, 90.0
    lon_lo, lon_hi = -180.0, 180.0
    is_lon = True
    bits, bit_count, out = 0, 0, []
    while len(out) < precision:
        if is_lon:
            mid = (lon_lo + lon_hi) / 2
            if lon >= mid:
                bits = (bits << 1) | 1; lon_lo = mid
            else:
                bits = bits << 1; lon_hi = mid
        else:
            mid = (lat_lo + lat_hi) / 2
            if lat >= mid:
                bits = (bits << 1) | 1; lat_lo = mid
            else:
                bits = bits << 1; lat_hi = mid
        is_lon = not is_lon
        bit_count += 1
        if bit_count == 5:
            out.append(_GEOHASH_BASE32[bits])
            bits, bit_count = 0, 0
    return "".join(out)


def decode_geohash(geohash: str) -> tuple[float, float]:
    lat_lo, lat_hi = -90.0, 90.0
    lon_lo, lon_hi = -180.0, 180.0
    is_lon = True
    for c in geohash.lower():
        if c not in _BASE32_DECODE:
            continue
        cd = _BASE32_DECODE[c]
        for mask in (16, 8, 4, 2, 1):
            if is_lon:
                mid = (lon_lo + lon_hi) / 2
                if cd & mask: lon_lo = mid
                else: lon_hi = mid
            else:
                mid = (lat_lo + lat_hi) / 2
                if cd & mask: lat_lo = mid
                else: lat_hi = mid
            is_lon = not is_lon
    return (lat_lo + lat_hi) / 2, (lon_lo + lon_hi) / 2


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def parse_timestamp(s: Any) -> Optional[datetime]:
    if s is None or s == "":
        return None
    if isinstance(s, datetime):
        return s
    s = str(s).strip()
    if s.isdigit() and len(s) >= 10:
        try:
            return datetime.utcfromtimestamp(int(s[:10]))
        except (ValueError, OSError):
            pass
    for fmt in (
        "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f",
        "%Y/%m/%d %H:%M:%S", "%Y-%m-%d", "%a %b %d %H:%M:%S %z %Y",
    ):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


TIME_SLOTS = [
    ("凌晨", 23, 7), ("早高峰", 7, 9), ("上午", 9, 11), ("午间", 11, 13),
    ("下午", 13, 17), ("晚高峰", 17, 19), ("夜间", 19, 23),
]


def get_time_slot(hour: int) -> str:
    for name, start, end in TIME_SLOTS:
        if start > end:
            if hour >= start or hour < end:
                return name
        elif start <= hour < end:
            return name
    return "凌晨"


def read_records(path: Path) -> list[dict]:
    """读 CSV / TSV / JSON / JSONL 文件 → list[dict]。"""
    suffix = path.suffix.lower()
    if suffix in (".jsonl", ".ndjson"):
        out = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        out.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return out
    if suffix == ".json":
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else [data]
    delim = "\t" if suffix == ".tsv" else ","
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter=delim))


def write_jsonl(path: Path, records: Iterable[dict]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
            n += 1
    return n


def write_summary(output_dir: Path, summary: dict) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def hash_uid(uid: str, salt: str = "citybench_v1") -> str:
    return hashlib.sha256((salt + str(uid)).encode("utf-8")).hexdigest()[:12]


# ════════════════════════════════════════════════════════════════════
#  Geohash → 业务地名查表（与 citybench-rag-search v2 兼容）
# ════════════════════════════════════════════════════════════════════
# 同 citybench-rag-search/scripts/landmarks.py 一致：
# 把 wtw3s 翻译成 "陆家嘴金融区"，把 wx4g0 翻译成 "国贸-CBD核心区"
# 所有下游 skill 的输出 jsonl 都会自动带 landmark / district / lat / lon

_LANDMARK_CACHE: Optional[dict] = None


def _load_landmarks() -> dict:
    """加载 geohash_landmarks.json。fallback：路径找不到就返回空 dict（不报错）。"""
    global _LANDMARK_CACHE
    if _LANDMARK_CACHE is not None:
        return _LANDMARK_CACHE
    # 本文件作为 citybench-skills-pack-new 的共享实现被根目录 skill 调用。
    # citybench-rag-search 的真实数据目录是 skills/custom/citybench-rag-search/skill/data/。
    candidates = [
        Path(__file__).resolve().parents[3] / "citybench-rag-search" / "skill" / "data" / "geohash_landmarks.json",
        Path(__file__).resolve().parents[3] / "citybench-rag-search" / "data" / "geohash_landmarks.json",
    ]
    for c in candidates:
        if c.exists():
            try:
                _LANDMARK_CACHE = json.loads(c.read_text(encoding="utf-8"))
                return _LANDMARK_CACHE
            except (json.JSONDecodeError, OSError):
                continue
    _LANDMARK_CACHE = {}
    return _LANDMARK_CACHE


def lookup_landmark(city: Optional[str], geohash: Optional[str]) -> Optional[dict]:
    """给定 (city, geohash) → {landmark, district, lat, lon, tags}。支持前缀匹配。"""
    if not geohash:
        return None
    data = _load_landmarks()
    # 如果传了 city 就只查那个城市；否则 4 城都试
    cities = [city] if city else ["Shanghai", "Beijing", "Guangzhou", "Shenzhen"]
    for c in cities:
        if not c:
            continue
        cmap = data.get(c, {})
        if geohash in cmap:
            return cmap[geohash]
        # 前缀匹配：调用方传 6 位 geohash，但表里只到 5 位
        for prefix, info in cmap.items():
            if isinstance(info, dict) and geohash.startswith(prefix):
                return info
    return None


def enrich_with_landmark(record: dict, city: Optional[str] = None,
                         geohash: Optional[str] = None) -> dict:
    """给一条记录加上 landmark/district/lat/lon 字段（原地修改并返回）。"""
    gh = geohash or record.get("geohash") or record.get("region_geohash") \
        or record.get("origin_geohash")
    info = lookup_landmark(city or record.get("city"), gh)
    if info:
        record["landmark"] = info["landmark"]
        record["district"] = info["district"]
        if "lat" not in record:
            record["lat"] = info["lat"]
            record["lon"] = info["lon"]
    return record


def format_label(city: Optional[str], geohash: Optional[str]) -> str:
    """返回 '陆家嘴金融区(wtw3s)' 形式的展示串；查不到就 'Shanghai-wtw3s'。"""
    if not geohash:
        return "(unknown)"
    info = lookup_landmark(city, geohash)
    if info:
        return f"{info['landmark']}({geohash})"
    return f"{city or '?'}-{geohash}"


def to_viz_record(geohash: str, city: Optional[str], metric: float,
                  metric_name: str = "score", anomaly: bool = False,
                  extra_text: str = "") -> dict:
    """把一个分析结果包装成 search_results.jsonl 兼容格式，供 render_heatmap.py 出图。"""
    info = lookup_landmark(city, geohash) or {}
    geo_scope = {"city": city, "geohash": geohash}
    if info:
        geo_scope.update({"landmark": info["landmark"],
                          "district": info["district"],
                          "lat": info["lat"], "lon": info["lon"],
                          "tags": info.get("tags", [])})
    return {
        "score": round(float(metric), 4),
        "evidence_id": f"viz_{geohash}_{metric_name}",
        "data_type": "spatiotemporal_analysis_viz",
        "text": extra_text or f"{format_label(city, geohash)}: {metric_name}={metric}",
        "meta": {
            "source_id": f"skill_output_viz_{metric_name}",
            "geo_scope": geo_scope,
            "features": {
                "checkin_count": float(metric) if metric > 0 else 1.0,
                "wow_change_pct": 0.0,
                "anomaly_flag": anomaly,
            },
        },
    }


def enrich_records(records: list[dict], geohash_keys: list[str] = None,
                   city_key: str = "city") -> list[dict]:
    """批量给一组记录注入 landmark 字段。
    geohash_keys: 候选 geohash 字段列表，如 ['geohash', 'region_geohash', 'origin_geohash']
    自动找第一个非空的，并加 <key>_landmark / <key>_district / lat / lon。
    """
    if not geohash_keys:
        geohash_keys = ["geohash", "region_geohash", "origin_geohash",
                        "destination_geohash", "current_geohash", "next_geohash"]
    for r in records:
        city = r.get(city_key)
        for k in geohash_keys:
            gh = r.get(k)
            if not gh:
                continue
            info = lookup_landmark(city, gh)
            if info:
                # 给字段加后缀，避免覆盖（OD 有两个 geohash）
                if k in ("origin_geohash", "destination_geohash", "current_geohash",
                         "next_geohash"):
                    prefix = k.replace("_geohash", "")
                    r[f"{prefix}_landmark"] = info["landmark"]
                    r[f"{prefix}_district"] = info["district"]
                    r[f"{prefix}_lat"] = info["lat"]
                    r[f"{prefix}_lon"] = info["lon"]
                else:
                    r["landmark"] = info["landmark"]
                    r["district"] = info["district"]
                    if "lat" not in r:
                        r["lat"] = info["lat"]
                        r["lon"] = info["lon"]
    return records


def write_viz_input(output_dir: Path, viz_records: list[dict]) -> Path:
    """写 viz_input.jsonl（兼容 render_heatmap.py），下游 agent 可串调出图。"""
    p = output_dir / "viz_input.jsonl"
    write_jsonl(p, viz_records)
    return p


# ════════════════════════════════════════════════════════════════════
#  字段自适应：在 CityBench evidence / 原始 GPS / region_heat 等
#  多种 schema 中自动定位 lat/lon/time/geohash/metric/group 列
# ════════════════════════════════════════════════════════════════════

_LAT_KEYS = ["lat", "latitude", "y", "centroid_lat", "center_lat"]
_LON_KEYS = ["lon", "lng", "longitude", "x", "centroid_lon", "center_lon"]
_TIME_KEYS = ["timestamp", "time", "datetime", "checkin_time", "ts",
              "arrive_time", "start_time"]
_USER_KEYS = ["user_id", "uid", "userid", "device_id", "trip_id", "trajectory_id"]
_GEOHASH_KEYS = ["geohash", "geohash6", "geohash5", "geohash7", "region"]


def _pick(rec: dict, candidates: list[str], override: Optional[str] = None) -> Any:
    if override and override in rec:
        return rec[override]
    for k in candidates:
        if k in rec and rec[k] not in (None, ""):
            return rec[k]
    return None


def _evidence_meta(rec: dict) -> dict:
    """CityBench evidence schema：信息嵌在 meta.{geo_scope, time_range, features}。"""
    meta = rec.get("meta") or {}
    geo = meta.get("geo_scope") or {}
    tr = meta.get("time_range") or {}
    feat = meta.get("features") or {}
    return {
        "city": geo.get("city"),
        "geohash": geo.get("geohash"),
        "lat": geo.get("lat"),
        "lon": geo.get("lon"),
        "time": tr.get("start"),
        "checkin_count": feat.get("checkin_count"),
        "unique_users": feat.get("unique_users"),
        "wow_change_pct": feat.get("wow_change_pct"),
        "anomaly_flag": feat.get("anomaly_flag"),
        "top_categories": feat.get("top_categories") or [],
        "text": rec.get("text", ""),
    }


def normalize_record(
    rec: dict,
    lat_col: Optional[str] = None,
    lon_col: Optional[str] = None,
    time_col: Optional[str] = None,
    geohash_col: Optional[str] = None,
    user_col: Optional[str] = None,
    geohash_precision: int = 6,
) -> dict:
    """归一化任意 schema → 统一字段。"""
    if "meta" in rec and isinstance(rec.get("meta"), dict):
        m = _evidence_meta(rec)
        ts = parse_timestamp(m["time"])
        return {
            "lat": float(m["lat"]) if m["lat"] is not None else None,
            "lon": float(m["lon"]) if m["lon"] is not None else None,
            "geohash": m["geohash"] or "",
            "city": m["city"],
            "ts": ts,
            "user_id": rec.get("evidence_id"),
            "metric": m["checkin_count"] if m["checkin_count"] is not None else 0.0,
            "unique_users": m["unique_users"],
            "anomaly_flag": m["anomaly_flag"],
            "wow_change_pct": m["wow_change_pct"],
            "top_categories": m["top_categories"],
            "text": m["text"],
            "_raw": rec,
        }
    lat = _pick(rec, _LAT_KEYS, lat_col)
    lon = _pick(rec, _LON_KEYS, lon_col)
    t = _pick(rec, _TIME_KEYS, time_col)
    gh = _pick(rec, _GEOHASH_KEYS, geohash_col)
    u = _pick(rec, _USER_KEYS, user_col)
    try:
        lat_f = float(lat) if lat is not None else None
        lon_f = float(lon) if lon is not None else None
    except (ValueError, TypeError):
        lat_f, lon_f = None, None
    if not gh and lat_f is not None and lon_f is not None:
        gh = encode_geohash(lat_f, lon_f, geohash_precision)
    return {
        "lat": lat_f, "lon": lon_f, "geohash": gh or "",
        "city": rec.get("city"), "ts": parse_timestamp(t),
        "user_id": u, "metric": None, "_raw": rec,
    }


def text_score(query: str, text: str) -> float:
    """简单 BM25-lite：tokens 命中数 / max(len(text_tokens), 1)。"""
    if not query or not text:
        return 0.0
    q_tokens = set(re.split(r"[\s,，。、:：]+", query.lower())) - {""}
    t_tokens = re.split(r"[\s,，。、:：]+", text.lower())
    if not t_tokens or not q_tokens:
        return 0.0
    hits = sum(1 for t in t_tokens if t in q_tokens)
    return hits / math.sqrt(max(len(t_tokens), 1))


# ════════════════════════════════════════════════════════════════════
#  1. search_records — 时空检索
# ════════════════════════════════════════════════════════════════════

def search_records(
    input: Path, output_dir: Path, *,
    query: Optional[str] = None, city: Optional[str] = None,
    geohash: Optional[str] = None, geohash_col: Optional[str] = None,
    time_col: Optional[str] = None,
    time_start: Optional[str] = None, time_end: Optional[str] = None,
    hour_start: Optional[int] = None, hour_end: Optional[int] = None,
    bbox: Optional[str] = None, lat_col: Optional[str] = None,
    lon_col: Optional[str] = None, top_k: int = 50,
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    records = read_records(input)
    ts_start = parse_timestamp(time_start) if time_start else None
    ts_end = parse_timestamp(time_end) if time_end else None
    bbox_t = None
    if bbox:
        try:
            mn_lon, mn_lat, mx_lon, mx_lat = [float(x) for x in bbox.split(",")]
            bbox_t = (mn_lon, mn_lat, mx_lon, mx_lat)
        except ValueError:
            bbox_t = None

    matched = []
    for rec in records:
        n = normalize_record(rec, lat_col, lon_col, time_col, geohash_col)
        if city and n["city"] and city.lower() not in str(n["city"]).lower():
            continue
        if geohash and not (n["geohash"] or "").startswith(geohash):
            continue
        if ts_start and n["ts"] and n["ts"] < ts_start:
            continue
        if ts_end and n["ts"] and n["ts"] > ts_end:
            continue
        if hour_start is not None and hour_end is not None and n["ts"]:
            h = n["ts"].hour
            if hour_start <= hour_end:
                if not (hour_start <= h < hour_end):
                    continue
            else:
                if not (h >= hour_start or h < hour_end):
                    continue
        if bbox_t and n["lat"] is not None and n["lon"] is not None:
            mn_lon, mn_lat, mx_lon, mx_lat = bbox_t
            if not (mn_lon <= n["lon"] <= mx_lon and mn_lat <= n["lat"] <= mx_lat):
                continue
        score = text_score(query or "", n.get("text", "") or "") if query else 1.0
        matched.append({"score": round(score, 4), "record": rec})

    if query:
        matched.sort(key=lambda x: -x["score"])
    matched = matched[:top_k]
    # 给每条 evidence 的 meta.geo_scope 注入 landmark（与 search.py 行为一致）
    out_records = []
    for m in matched:
        rec = m["record"]
        if "meta" in rec and isinstance(rec["meta"], dict):
            geo = rec["meta"].setdefault("geo_scope", {})
            info = lookup_landmark(geo.get("city"), geo.get("geohash"))
            if info:
                geo.update({"landmark": info["landmark"],
                            "district": info["district"],
                            "lat": info["lat"], "lon": info["lon"],
                            "tags": info.get("tags", [])})
        out_records.append({"score": m["score"], **rec})
    write_jsonl(output_dir / "search_results.jsonl", out_records)

    summary = {
        "ok": True, "skill": "search_spatiotemporal",
        "input": str(input), "output_dir": str(output_dir),
        "filters": {"query": query, "city": city, "geohash": geohash,
                    "time_start": time_start, "time_end": time_end,
                    "hour_start": hour_start, "hour_end": hour_end, "bbox": bbox},
        "n_total": len(records), "n_matched": len(matched),
        "outputs": {"search_results": str(output_dir / "search_results.jsonl"),
                    "summary": str(output_dir / "summary.json")},
    }
    write_summary(output_dir, summary)
    return summary


# ════════════════════════════════════════════════════════════════════
#  2. flow_anomaly — 区域流量异常检测（z-score）
# ════════════════════════════════════════════════════════════════════

def flow_anomaly(
    input: Path, output_dir: Path,
    group_col: Optional[str], metric_col: Optional[str],
    threshold: float = 3.5, top_k: int = 20,
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    records = read_records(input)
    # group_col / metric_col 自适应：CityBench evidence 用 geohash + checkin_count
    series: dict[str, list[float]] = defaultdict(list)
    flagged_known: list[dict] = []
    for rec in records:
        n = normalize_record(rec)
        gid = (rec.get(group_col) if group_col else None) or n["geohash"] or n["city"]
        if not gid:
            continue
        v = (rec.get(metric_col) if metric_col else None)
        if v is None:
            v = n["metric"]
        try:
            v = float(v) if v is not None else None
        except (ValueError, TypeError):
            v = None
        if v is None:
            continue
        series[gid].append(v)
        if n.get("anomaly_flag"):
            flagged_known.append({"group": gid, "value": v,
                                  "wow_change_pct": n.get("wow_change_pct"),
                                  "evidence_id": rec.get("evidence_id")})

    anomalies = []
    for gid, vals in series.items():
        if len(vals) < 3:
            continue
        mu = statistics.mean(vals)
        try:
            sigma = statistics.stdev(vals)
        except statistics.StatisticsError:
            sigma = 0.0
        if sigma <= 1e-9:
            continue
        for v in vals:
            z = (v - mu) / sigma
            if abs(z) >= threshold:
                anomalies.append({"group": gid, "value": v,
                                  "z_score": round(z, 3),
                                  "mean": round(mu, 3), "std": round(sigma, 3)})
    anomalies.sort(key=lambda x: -abs(x["z_score"]))
    anomalies = anomalies[:top_k]
    # 给每条 anomaly 注入 landmark（group 字段是 geohash）
    for a in anomalies:
        info = lookup_landmark(None, a.get("group", ""))
        if info:
            a["landmark"] = info["landmark"]
            a["district"] = info["district"]
            a["lat"] = info["lat"]; a["lon"] = info["lon"]
    write_jsonl(output_dir / "anomalies.jsonl", anomalies)
    write_jsonl(output_dir / "flagged_in_evidence.jsonl", flagged_known[:top_k])
    # viz_input.jsonl: 异常区域作为热点（异常标记 = 红色气泡）
    viz = [to_viz_record(a["group"], None, abs(a["z_score"]),
                         "z_score", anomaly=True,
                         extra_text=f"{format_label(None, a['group'])}: 异常 z={a['z_score']}, 值={a['value']}")
           for a in anomalies if a.get("group")]
    write_viz_input(output_dir, viz)

    summary = {
        "ok": True, "skill": "detect_flow_anomaly",
        "input": str(input), "output_dir": str(output_dir),
        "params": {"group_col": group_col, "metric_col": metric_col,
                   "threshold": threshold, "top_k": top_k},
        "n_groups": len(series), "n_anomalies": len(anomalies),
        "n_evidence_pre_flagged": len(flagged_known),
        "outputs": {"anomalies": str(output_dir / "anomalies.jsonl"),
                    "flagged_in_evidence": str(output_dir / "flagged_in_evidence.jsonl"),
                    "viz_input": str(output_dir / "viz_input.jsonl"),
                    "summary": str(output_dir / "summary.json")},
    }
    write_summary(output_dir, summary)
    return summary


# ════════════════════════════════════════════════════════════════════
#  3. fuse_spatial_evidence — 多源 evidence 文件融合到 region 级
# ════════════════════════════════════════════════════════════════════

def fuse_spatial_evidence(inputs: list[Path], output_dir: Path, top_k: int = 20) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    region_acc: dict[str, dict] = defaultdict(lambda: {
        "geohash": None, "city": None,
        "total_checkin": 0.0, "total_unique_users": 0.0,
        "n_evidence": 0, "categories": Counter(),
        "anomaly_count": 0, "wow_change_pcts": [], "sources": set(),
    })
    n_total = 0
    for src in inputs:
        records = read_records(src)
        n_total += len(records)
        for rec in records:
            n = normalize_record(rec)
            gh = n["geohash"]
            if not gh:
                continue
            a = region_acc[gh]
            a["geohash"] = gh
            a["city"] = a["city"] or n["city"]
            if isinstance(n.get("metric"), (int, float)):
                a["total_checkin"] += float(n["metric"])
            if isinstance(n.get("unique_users"), (int, float)):
                a["total_unique_users"] += float(n["unique_users"])
            a["n_evidence"] += 1
            for c in n.get("top_categories", []) or []:
                a["categories"][str(c)] += 1
            if n.get("anomaly_flag"):
                a["anomaly_count"] += 1
            if isinstance(n.get("wow_change_pct"), (int, float)):
                a["wow_change_pcts"].append(float(n["wow_change_pct"]))
            a["sources"].add(str(src))

    fused = []
    for gh, a in region_acc.items():
        wow = a["wow_change_pcts"]
        fused.append({
            "geohash": a["geohash"], "city": a["city"],
            "total_checkin": round(a["total_checkin"], 2),
            "total_unique_users": round(a["total_unique_users"], 2),
            "n_evidence": a["n_evidence"],
            "anomaly_count": a["anomaly_count"],
            "avg_wow_change_pct": round(statistics.mean(wow), 2) if wow else None,
            "top_categories": [c for c, _ in a["categories"].most_common(5)],
            "n_sources": len(a["sources"]),
        })
    fused.sort(key=lambda x: -x["total_checkin"])
    enrich_records(fused)
    fused_top = fused[:top_k] if top_k > 0 else fused
    write_jsonl(output_dir / "fused_regions.jsonl", fused)
    # viz_input: 用 total_checkin 作为热度
    viz = [to_viz_record(r["geohash"], r.get("city"),
                         r["total_checkin"], "total_checkin",
                         anomaly=(r.get("anomaly_count", 0) > 0),
                         extra_text=f"{format_label(r.get('city'), r['geohash'])}: 总签到 {int(r['total_checkin'])} 次, "
                                    f"异常 {r['anomaly_count']} 次")
           for r in fused_top if r.get("geohash")]
    write_viz_input(output_dir, viz)

    summary = {
        "ok": True, "skill": "fuse_spatial_evidence",
        "inputs": [str(p) for p in inputs], "output_dir": str(output_dir),
        "n_input_records": n_total, "n_fused_regions": len(fused),
        "top_regions_preview": fused_top[:5],
        "outputs": {"fused_regions": str(output_dir / "fused_regions.jsonl"),
                    "viz_input": str(output_dir / "viz_input.jsonl"),
                    "summary": str(output_dir / "summary.json")},
    }
    write_summary(output_dir, summary)
    return summary


# ════════════════════════════════════════════════════════════════════
#  4. similarity_search — DTW 相似 trip 检索
# ════════════════════════════════════════════════════════════════════

def _trip_points(trip: dict) -> list[tuple[float, float]]:
    """从 trip 记录里抽取 (lat, lon) 序列。"""
    pts = trip.get("points") or trip.get("path") or trip.get("trajectory")
    if isinstance(pts, list) and pts and isinstance(pts[0], dict):
        return [(float(p.get("lat") or p.get("latitude") or 0),
                 float(p.get("lon") or p.get("longitude") or 0))
                for p in pts if (p.get("lat") or p.get("latitude")) is not None]
    if isinstance(pts, list) and pts and isinstance(pts[0], (list, tuple)):
        return [(float(p[0]), float(p[1])) for p in pts]
    if "lat" in trip and "lon" in trip:
        return [(float(trip["lat"]), float(trip["lon"]))]
    return []


def _downsample(seq: list, max_len: int) -> list:
    if len(seq) <= max_len:
        return seq
    step = len(seq) / max_len
    return [seq[int(i * step)] for i in range(max_len)]


def _dtw_avg_m(a: list[tuple[float, float]], b: list[tuple[float, float]],
               window: int = 10) -> float:
    n, m = len(a), len(b)
    if n == 0 or m == 0:
        return float("inf")
    INF = float("inf")
    w = max(window, abs(n - m))
    dp = [[INF] * (m + 1) for _ in range(n + 1)]
    dp[0][0] = 0.0
    for i in range(1, n + 1):
        for j in range(max(1, i - w), min(m, i + w) + 1):
            cost = haversine_m(a[i - 1][0], a[i - 1][1], b[j - 1][0], b[j - 1][1])
            dp[i][j] = cost + min(dp[i - 1][j], dp[i][j - 1], dp[i - 1][j - 1])
    return dp[n][m] / max(n + m, 1) if dp[n][m] != INF else INF


def similarity_search(
    input: Path, output_dir: Path,
    target_trip_id: Optional[str] = None, top_k: int = 10,
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    trips = read_records(input)
    # 找 target trip
    target = None
    if target_trip_id:
        for t in trips:
            if str(t.get("trip_id") or t.get("id") or t.get("user_id")) == str(target_trip_id):
                target = t; break
    if target is None and trips:
        target = trips[0]
    if not target:
        summary = {"ok": False, "skill": "search_similar_trajectory",
                   "error": "no trips in input"}
        write_summary(output_dir, summary)
        return summary
    target_seq = _downsample(_trip_points(target), 100)
    target_id = str(target.get("trip_id") or target.get("id") or target.get("user_id") or "trip0")

    # 如果输入是 staypoints.jsonl（没有 trip 概念），按 user_id 聚合
    if not target_seq:
        by_user: dict[str, list[tuple]] = defaultdict(list)
        for r in trips:
            n = normalize_record(r)
            if n["lat"] is not None and n["user_id"]:
                by_user[str(n["user_id"])].append((n["ts"] or datetime.min,
                                                   n["lat"], n["lon"]))
        for uid in by_user:
            by_user[uid].sort(key=lambda x: x[0])
        if not by_user:
            summary = {"ok": False, "skill": "search_similar_trajectory",
                       "error": "no extractable trajectory points"}
            write_summary(output_dir, summary)
            return summary
        target_id = target_trip_id if target_trip_id and target_trip_id in by_user \
                    else next(iter(by_user))
        target_seq = _downsample([(la, lo) for _, la, lo in by_user[target_id]], 100)
        candidates = [(uid, _downsample([(la, lo) for _, la, lo in pts], 100))
                      for uid, pts in by_user.items() if uid != target_id]
    else:
        candidates = []
        for t in trips:
            tid = str(t.get("trip_id") or t.get("id") or t.get("user_id"))
            if tid == target_id:
                continue
            seq = _downsample(_trip_points(t), 100)
            if seq:
                candidates.append((tid, seq))

    results = []
    for tid, seq in candidates:
        d = _dtw_avg_m(target_seq, seq)
        results.append({"trip_id": tid, "dtw_avg_meters": round(d, 2),
                        "n_points_used": len(seq)})
    results.sort(key=lambda x: x["dtw_avg_meters"])
    top = results[:top_k]
    write_jsonl(output_dir / "similar_trips.jsonl", top)

    summary = {
        "ok": True, "skill": "search_similar_trajectory",
        "input": str(input), "output_dir": str(output_dir),
        "target_trip_id": target_id, "n_candidates": len(candidates),
        "top_k": top_k, "best_match": top[0] if top else None,
        "outputs": {"similar_trips": str(output_dir / "similar_trips.jsonl"),
                    "summary": str(output_dir / "summary.json")},
    }
    write_summary(output_dir, summary)
    return summary


# ════════════════════════════════════════════════════════════════════
#  5. route_anomaly — 单 trip 绕路率异常
# ════════════════════════════════════════════════════════════════════

def route_anomaly(
    input: Path, output_dir: Path,
    group_col: str = "user_id", threshold: float = 3.5, top_k: int = 20,
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    records = read_records(input)
    # 聚合：每个 trip / user_id 一条 (实际路径, 直线距离, 绕路率)
    by_group: dict[str, list[tuple]] = defaultdict(list)
    for r in records:
        # 支持两种输入：trip 形式（含 path/points）、或 staypoints/cleaned_points
        pts = _trip_points(r)
        if pts:
            by_group[str(r.get(group_col) or r.get("trip_id") or r.get("user_id"))] = pts
        else:
            n = normalize_record(r)
            if n["lat"] is None or n["lon"] is None:
                continue
            gid = str(r.get(group_col) or n["user_id"] or "")
            if not gid:
                continue
            by_group[gid].append((n["ts"] or datetime.min, n["lat"], n["lon"]))

    metrics = []
    for gid, pts in by_group.items():
        if len(pts) < 2:
            continue
        # 如果是带时间的元组 → 排序
        if pts and len(pts[0]) == 3:
            pts = [(la, lo) for _, la, lo in sorted(pts, key=lambda x: x[0])]
        actual = sum(haversine_m(pts[i - 1][0], pts[i - 1][1], pts[i][0], pts[i][1])
                     for i in range(1, len(pts)))
        straight = haversine_m(pts[0][0], pts[0][1], pts[-1][0], pts[-1][1])
        if straight < 1.0:
            continue
        ratio = actual / straight
        metrics.append({"group": gid, "actual_m": round(actual, 2),
                        "straight_m": round(straight, 2),
                        "detour_ratio": round(ratio, 3),
                        "n_points": len(pts)})
    if metrics:
        ratios = [m["detour_ratio"] for m in metrics]
        mu = statistics.mean(ratios)
        try:
            sigma = statistics.stdev(ratios)
        except statistics.StatisticsError:
            sigma = 0.0
        for m in metrics:
            m["z_score"] = round((m["detour_ratio"] - mu) / sigma, 3) if sigma > 1e-9 else 0.0
            m["is_anomaly"] = abs(m["z_score"]) >= threshold or m["detour_ratio"] > 2.5
        metrics.sort(key=lambda x: -abs(x["z_score"]))
    anomalies = [m for m in metrics if m.get("is_anomaly")][:top_k]
    write_jsonl(output_dir / "route_anomalies.jsonl", anomalies)
    write_jsonl(output_dir / "all_route_metrics.jsonl", metrics)

    summary = {
        "ok": True, "skill": "detect_route_anomaly",
        "input": str(input), "output_dir": str(output_dir),
        "params": {"group_col": group_col, "threshold": threshold, "top_k": top_k},
        "n_groups": len(by_group), "n_metrics": len(metrics), "n_anomalies": len(anomalies),
        "outputs": {"route_anomalies": str(output_dir / "route_anomalies.jsonl"),
                    "all_route_metrics": str(output_dir / "all_route_metrics.jsonl"),
                    "summary": str(output_dir / "summary.json")},
    }
    write_summary(output_dir, summary)
    return summary


# ════════════════════════════════════════════════════════════════════
#  6. forecast_region_flow — Holt-Winters 区域流量预测
# ════════════════════════════════════════════════════════════════════

def _holt_winters(series: list[float], season: int, horizon: int,
                  alpha: float = 0.3, beta: float = 0.1, gamma: float = 0.2
                  ) -> tuple[list[float], list[float]]:
    n = len(series)
    if n == 0:
        return [], [0.0] * horizon
    if n < 2 * season or season < 2:
        s = series[0]; fit = []
        for v in series:
            s = alpha * v + (1 - alpha) * s; fit.append(s)
        return fit, [max(0.0, s)] * horizon
    season_avgs = [statistics.mean([series[k + i * season] for i in range(n // season)
                                    if k + i * season < n]) for k in range(season)]
    overall = statistics.mean(series[:season * (n // season)])
    seasonals = [a - overall for a in season_avgs]
    L = sum(series[:season]) / season
    T = (sum(series[season:2 * season]) - sum(series[:season])) / (season ** 2)
    fit = []
    for i in range(n):
        if i == 0:
            fit.append(L + T + seasonals[i % season]); continue
        last_L, last_T = L, T
        L = alpha * (series[i] - seasonals[i % season]) + (1 - alpha) * (last_L + last_T)
        T = beta * (L - last_L) + (1 - beta) * last_T
        seasonals[i % season] = gamma * (series[i] - L) + (1 - gamma) * seasonals[i % season]
        fit.append(L + T + seasonals[i % season])
    fc = [max(0.0, L + h * T + seasonals[(n + h - 1) % season])
          for h in range(1, horizon + 1)]
    return fit, fc


def forecast_region_flow(
    input: Path, output_dir: Path, *,
    precision: int = 6, bucket: str = "day",
    forecast_steps: int = 3, history_window: int = 7, top_k: int = 20,
    group_col: Optional[str] = None, metric_col: Optional[str] = None,
    time_col: Optional[str] = None, geohash_col: Optional[str] = None,
    lat_col: Optional[str] = None, lon_col: Optional[str] = None,
    user_col: Optional[str] = None,
    query: Optional[str] = None, city: Optional[str] = None,
    geohash: Optional[str] = None,
    time_start: Optional[str] = None, time_end: Optional[str] = None,
    hour_start: Optional[int] = None, hour_end: Optional[int] = None,
    bbox: Optional[str] = None,
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    records = read_records(input)
    series_by_region: dict[str, list[tuple[datetime, float]]] = defaultdict(list)
    for rec in records:
        n = normalize_record(rec, lat_col, lon_col, time_col, geohash_col,
                             user_col, precision)
        gh = n["geohash"]
        if not gh:
            continue
        if geohash and not gh.startswith(geohash):
            continue
        if city and n["city"] and city.lower() not in str(n["city"]).lower():
            continue
        ts = n["ts"]
        if not ts:
            continue
        v = (rec.get(metric_col) if metric_col else None)
        if v is None:
            v = n["metric"] if n["metric"] is not None else 1.0
        try:
            v = float(v)
        except (ValueError, TypeError):
            continue
        # 时间桶化
        if bucket == "hour":
            tb = ts.replace(minute=0, second=0, microsecond=0)
        elif bucket == "month":
            tb = ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        else:
            tb = ts.replace(hour=0, minute=0, second=0, microsecond=0)
        series_by_region[gh].append((tb, v))

    season = {"hour": 24, "day": 7, "month": 12}.get(bucket, 7)
    forecasts = []
    region_summaries = []
    for gh, items in series_by_region.items():
        # 桶聚合（同桶内 sum）
        bucket_map: dict[datetime, float] = defaultdict(float)
        for t, v in items:
            bucket_map[t] += v
        sorted_items = sorted(bucket_map.items())
        ts_list = [t for t, _ in sorted_items]
        series = [v for _, v in sorted_items]
        if len(series) < 3:
            continue
        if history_window and len(series) > history_window * 4:
            ts_list = ts_list[-history_window * 4:]
            series = series[-history_window * 4:]
        _, fc = _holt_winters(series, season, forecast_steps)
        delta = (ts_list[-1] - ts_list[-2]) if len(ts_list) >= 2 else timedelta(days=1)
        for h, val in enumerate(fc, start=1):
            forecasts.append({
                "region_geohash": gh,
                "future_timestamp": (ts_list[-1] + delta * h).strftime("%Y-%m-%dT%H:%M:%S"),
                "horizon_step": h, "predicted_value": round(val, 2),
            })
        region_summaries.append({
            "region_geohash": gh, "history_length": len(series),
            "next_step_predicted": round(fc[0], 2) if fc else None,
        })
    region_summaries.sort(key=lambda x: -(x["next_step_predicted"] or 0))
    region_summaries = region_summaries[:top_k] if top_k > 0 else region_summaries
    enrich_records(forecasts); enrich_records(region_summaries)
    write_jsonl(output_dir / "forecast.jsonl", forecasts)
    write_jsonl(output_dir / "region_top.jsonl", region_summaries)
    # viz_input: 用 next_step_predicted 作为热度展示
    viz = [to_viz_record(r["region_geohash"], None,
                         r["next_step_predicted"] or 0, "predicted_flow",
                         extra_text=f"{format_label(None, r['region_geohash'])}: 预测下一步 {r['next_step_predicted']}")
           for r in region_summaries[:24] if r.get("region_geohash")]
    write_viz_input(output_dir, viz)

    summary = {
        "ok": True, "skill": "forecast_region_flow",
        "input": str(input), "output_dir": str(output_dir),
        "params": {"precision": precision, "bucket": bucket, "season": season,
                   "forecast_steps": forecast_steps, "history_window": history_window,
                   "city": city, "geohash": geohash},
        "n_regions_forecasted": len(region_summaries),
        "n_forecast_records": len(forecasts),
        "outputs": {"forecast": str(output_dir / "forecast.jsonl"),
                    "region_top": str(output_dir / "region_top.jsonl"),
                    "viz_input": str(output_dir / "viz_input.jsonl"),
                    "summary": str(output_dir / "summary.json")},
    }
    write_summary(output_dir, summary)
    return summary


# ════════════════════════════════════════════════════════════════════
#  7. profile_urban_region — K-Means 功能区聚类
# ════════════════════════════════════════════════════════════════════

OFFICE_KEYS = ["写字楼", "办公", "company", "office", "work"]
RESIDENTIAL_KEYS = ["住宅", "小区", "公寓", "residential", "apartment", "home"]
DINING_ENT_KEYS = ["餐厅", "餐饮", "咖啡", "酒吧", "夜店", "ktv", "影院", "电影",
                   "商场", "购物", "bar", "cafe", "restaurant", "shop", "mall"]


def _categorize_poi(s: str) -> str:
    sl = str(s).lower()
    for k in OFFICE_KEYS:
        if k in sl: return "office"
    for k in RESIDENTIAL_KEYS:
        if k in sl: return "residential"
    for k in DINING_ENT_KEYS:
        if k in sl: return "dining_ent"
    return "other"


PROFILE_SLOTS = ["凌晨", "早高峰", "上午", "午间", "下午", "晚高峰", "夜间"]


def _kmeans(points: list[list[float]], k: int, max_iter: int = 100, seed: int = 42):
    import random as _r
    rng = _r.Random(seed)
    n = len(points)
    if n == 0: return [], []
    if n <= k:
        return list(range(n)), [list(p) for p in points]
    centroids = [list(points[i]) for i in rng.sample(range(n), k)]
    labels = [0] * n; dim = len(points[0])
    for _ in range(max_iter):
        changed = False
        for idx, p in enumerate(points):
            best, best_d = 0, float("inf")
            for ci, c in enumerate(centroids):
                d = sum((p[i] - c[i]) ** 2 for i in range(dim))
                if d < best_d: best_d, best = d, ci
            if labels[idx] != best: labels[idx] = best; changed = True
        sums = [[0.0] * dim for _ in range(k)]; cnts = [0] * k
        for idx, p in enumerate(points):
            l = labels[idx]
            for i in range(dim): sums[l][i] += p[i]
            cnts[l] += 1
        for ci in range(k):
            if cnts[ci] > 0:
                centroids[ci] = [sums[ci][i] / cnts[ci] for i in range(dim)]
        if not changed: break
    return labels, centroids


def _explain_cluster(c: list[float]) -> str:
    morning, evening, night, midday = c[1], c[5], c[6], c[3]
    office_r, resi_r, dining_r = c[7], c[8], c[9]
    if (morning > 0.20 or evening > 0.20) and office_r > 0.15:
        return "核心商务区"
    if night > 0.25 and resi_r > 0.20:
        return "纯住宅区"
    if dining_r > 0.20 and (midday > 0.15 or evening > 0.15):
        return "商业娱乐区"
    return "商住混合区"


def profile_urban_region(
    input: Path, output_dir: Path, *,
    precision: int = 6, poi_input: Optional[Path] = None,
    poi_lat_col: Optional[str] = None, poi_lon_col: Optional[str] = None,
    poi_category_col: Optional[str] = None,
    geohash_col: Optional[str] = None,
    lat_col: Optional[str] = None, lon_col: Optional[str] = None,
    time_col: Optional[str] = None, user_col: Optional[str] = None,
    top_k: int = 20, k: int = 4, min_evidence: int = 2,
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    records = read_records(input)
    region_slot: dict[str, dict[str, float]] = defaultdict(
        lambda: {s: 0.0 for s in PROFILE_SLOTS}
    )
    region_poi: dict[str, dict[str, float]] = defaultdict(
        lambda: {"office": 0, "residential": 0, "dining_ent": 0, "other": 0}
    )
    region_meta: dict[str, dict] = {}
    region_count: dict[str, int] = defaultdict(int)
    for rec in records:
        n = normalize_record(rec, lat_col, lon_col, time_col, geohash_col,
                             user_col, precision)
        gh = n["geohash"]
        if not gh or not n["ts"]:
            continue
        slot = get_time_slot(n["ts"].hour)
        v = float(n["metric"]) if isinstance(n.get("metric"), (int, float)) else 1.0
        region_slot[gh][slot] += v
        region_count[gh] += 1
        region_meta[gh] = {"city": n.get("city")}
        for c in n.get("top_categories", []) or []:
            region_poi[gh][_categorize_poi(c)] += 1
    # 外部 POI 文件融合
    if poi_input and Path(poi_input).exists():
        for rec in read_records(Path(poi_input)):
            la = _pick(rec, _LAT_KEYS, poi_lat_col)
            lo = _pick(rec, _LON_KEYS, poi_lon_col)
            cat = _pick(rec, ["category", "type", "poi_type"], poi_category_col)
            if la is None or lo is None or not cat:
                continue
            try:
                gh = encode_geohash(float(la), float(lo), precision)
            except (ValueError, TypeError):
                continue
            region_poi[gh][_categorize_poi(cat)] += 1

    region_ids, feats = [], []
    for gh, slots in region_slot.items():
        if region_count[gh] < min_evidence:
            continue
        total = sum(slots.values())
        if total <= 0:
            continue
        slot_pct = [slots[s] / total for s in PROFILE_SLOTS]
        poi = region_poi[gh]
        poi_total = sum(poi.values())
        poi_pct = ([poi["office"] / poi_total, poi["residential"] / poi_total,
                    poi["dining_ent"] / poi_total] if poi_total > 0 else [0, 0, 0])
        feats.append(slot_pct + poi_pct)
        region_ids.append(gh)
    if not feats:
        summary = {"ok": False, "skill": "profile_urban_region",
                   "error": "no eligible region after filtering",
                   "input": str(input), "output_dir": str(output_dir)}
        write_summary(output_dir, summary)
        return summary
    labels, centroids = _kmeans(feats, k)
    cluster_labels_cn = [_explain_cluster(c) for c in centroids]

    profiles = []
    for gh, f, lab in zip(region_ids, feats, labels):
        profiles.append({
            "geohash": gh, "city": region_meta.get(gh, {}).get("city"),
            "cluster_id": lab, "cluster_label_cn": cluster_labels_cn[lab],
            "slot_share": dict(zip(PROFILE_SLOTS, [round(x, 3) for x in f[:7]])),
            "poi_share": {"office": round(f[7], 3), "residential": round(f[8], 3),
                          "dining_ent": round(f[9], 3)},
        })
    enrich_records(profiles)
    write_jsonl(output_dir / "region_profiles.jsonl", profiles)
    # viz_input: 每个 region 用 cluster_id 作 metric（让相同簇用相近颜色）
    viz = [to_viz_record(p["geohash"], p.get("city"),
                         p["cluster_id"] + 1, p["cluster_label_cn"],
                         extra_text=f"{format_label(p.get('city'), p['geohash'])}: 簇 {p['cluster_id']} = {p['cluster_label_cn']}")
           for p in profiles if p.get("geohash")]
    write_viz_input(output_dir, viz)

    summary = {
        "ok": True, "skill": "profile_urban_region",
        "input": str(input), "output_dir": str(output_dir),
        "params": {"k": k, "precision": precision, "min_evidence": min_evidence},
        "n_regions_clustered": len(profiles),
        "cluster_summary": [
            {"cluster_id": ci, "label_cn": cluster_labels_cn[ci],
             "n_regions": labels.count(ci),
             "centroid_slot_share": dict(zip(PROFILE_SLOTS,
                                             [round(centroids[ci][i], 3) for i in range(7)])),
             "centroid_poi_share": {"office": round(centroids[ci][7], 3),
                                    "residential": round(centroids[ci][8], 3),
                                    "dining_ent": round(centroids[ci][9], 3)}}
            for ci in range(len(centroids))
        ],
        "outputs": {"region_profiles": str(output_dir / "region_profiles.jsonl"),
                    "viz_input": str(output_dir / "viz_input.jsonl"),
                    "summary": str(output_dir / "summary.json")},
    }
    write_summary(output_dir, summary)
    return summary


# ════════════════════════════════════════════════════════════════════
#  以下是 8 个全新 skill 的算法实现
# ════════════════════════════════════════════════════════════════════

#  8. analyze_od_flow — OD 起终点流向矩阵
# ────────────────────────────────────────────────────────────────────

def analyze_od_flow(
    input: Path, output_dir: Path, *,
    precision: int = 5, top_k: int = 20,
    hour_start: Optional[int] = None, hour_end: Optional[int] = None,
    max_od_hours: float = 12.0, geohash_col: Optional[str] = None,
    user_col: Optional[str] = None,
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    records = read_records(input)
    by_user: dict[str, list[dict]] = defaultdict(list)
    for r in records:
        n = normalize_record(r, geohash_col=geohash_col, user_col=user_col,
                             geohash_precision=precision)
        if not n["user_id"] or n["lat"] is None:
            continue
        # staypoint 形式：取 arrive_time / leave_time
        arr = parse_timestamp(r.get("arrive_time")) or n["ts"]
        lv = parse_timestamp(r.get("leave_time")) or n["ts"]
        if not (arr and lv):
            continue
        gh = encode_geohash(n["lat"], n["lon"], precision)
        by_user[str(n["user_id"])].append({
            "lat": n["lat"], "lon": n["lon"], "arrive": arr, "leave": lv,
            "geohash": gh,
        })
    for uid in by_user:
        by_user[uid].sort(key=lambda x: x["arrive"])

    od_acc: dict[tuple, dict] = defaultdict(lambda: {
        "flow": 0, "users": set(), "durations": [],
        "o_lat_sum": 0.0, "o_lon_sum": 0.0, "d_lat_sum": 0.0, "d_lon_sum": 0.0,
    })
    n_od = 0
    for uid, stays in by_user.items():
        for i in range(len(stays) - 1):
            o, d = stays[i], stays[i + 1]
            if o["geohash"] == d["geohash"]:
                continue
            travel_h = (d["arrive"] - o["leave"]).total_seconds() / 3600
            if travel_h <= 0 or travel_h > max_od_hours:
                continue
            if hour_start is not None and hour_end is not None:
                hr = o["leave"].hour
                ok = (hour_start <= hr < hour_end) if hour_start <= hour_end \
                    else (hr >= hour_start or hr < hour_end)
                if not ok:
                    continue
            n_od += 1
            key = (o["geohash"], d["geohash"])
            b = od_acc[key]
            b["flow"] += 1; b["users"].add(uid); b["durations"].append(travel_h)
            b["o_lat_sum"] += o["lat"]; b["o_lon_sum"] += o["lon"]
            b["d_lat_sum"] += d["lat"]; b["d_lon_sum"] += d["lon"]

    od_records = []
    for (og, dg), b in od_acc.items():
        n = b["flow"]
        od_records.append({
            "origin_geohash": og, "destination_geohash": dg,
            "flow": n, "unique_users": len(b["users"]),
            "avg_duration_h": round(sum(b["durations"]) / n, 3),
            "origin_lat": round(b["o_lat_sum"] / n, 6),
            "origin_lon": round(b["o_lon_sum"] / n, 6),
            "destination_lat": round(b["d_lat_sum"] / n, 6),
            "destination_lon": round(b["d_lon_sum"] / n, 6),
        })
    od_records.sort(key=lambda r: -r["flow"])
    enrich_records(od_records)  # 自动加 origin_landmark / destination_landmark
    write_jsonl(output_dir / "od_matrix.jsonl", od_records)
    write_jsonl(output_dir / "top_corridors.jsonl", od_records[:top_k])
    # viz_input: 用 destination 作热点（通勤目的地热度）
    dest_heat: dict = defaultdict(float)
    for r in od_records:
        dest_heat[r["destination_geohash"]] += r["flow"]
    viz = []
    for gh, total_flow in sorted(dest_heat.items(), key=lambda x: -x[1])[:24]:
        info = lookup_landmark(None, gh)
        city = None
        if info:
            for c in ("Shanghai", "Beijing", "Guangzhou", "Shenzhen"):
                cmap = _load_landmarks().get(c, {})
                if any(gh.startswith(p) for p in cmap):
                    city = c; break
        viz.append(to_viz_record(gh, city, total_flow, "inflow",
                                 extra_text=f"{format_label(city, gh)}: 累计入流 {int(total_flow)} 次"))
    write_viz_input(output_dir, viz)

    summary = {
        "ok": True, "skill": "analyze_od_flow",
        "input": str(input), "output_dir": str(output_dir),
        "params": {"precision": precision, "top_k": top_k,
                   "hour_start": hour_start, "hour_end": hour_end},
        "n_users": len(by_user), "n_od_total": n_od,
        "n_unique_corridors": len(od_records),
        "top_3_corridors": od_records[:3],
        "outputs": {"od_matrix": str(output_dir / "od_matrix.jsonl"),
                    "top_corridors": str(output_dir / "top_corridors.jsonl"),
                    "viz_input": str(output_dir / "viz_input.jsonl"),
                    "summary": str(output_dir / "summary.json")},
    }
    write_summary(output_dir, summary)
    return summary


#  9. mine_trajectory_patterns — 频繁出行序列挖掘
# ────────────────────────────────────────────────────────────────────

def mine_trajectory_patterns(
    input: Path, output_dir: Path, *,
    min_support: float = 0.05, max_length: int = 4, top_k: int = 30,
    geohash_col: Optional[str] = None, user_col: Optional[str] = None,
    precision: int = 5,
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    records = read_records(input)
    by_user: dict[str, list[tuple]] = defaultdict(list)
    for r in records:
        n = normalize_record(r, geohash_col=geohash_col, user_col=user_col,
                             geohash_precision=precision)
        if not n["user_id"] or not n["geohash"]:
            continue
        ts = parse_timestamp(r.get("arrive_time")) or n["ts"]
        if not ts:
            continue
        by_user[str(n["user_id"])].append((ts, n["geohash"]))

    seqs = {}
    for uid, items in by_user.items():
        items.sort(key=lambda x: x[0])
        seq = []
        for _, gh in items:
            if not seq or seq[-1] != gh:
                seq.append(gh)
        if len(seq) >= 2:
            seqs[uid] = seq
    n_users = len(seqs)
    min_count = max(2, int(n_users * min_support))

    by_length: dict[int, list[dict]] = {}
    all_top: list[dict] = []
    for L in range(2, max_length + 1):
        pat_users: dict[tuple, set] = defaultdict(set)
        pat_total: Counter = Counter()
        for uid, seq in seqs.items():
            for i in range(len(seq) - L + 1):
                pat = tuple(seq[i:i + L])
                pat_total[pat] += 1
                pat_users[pat].add(uid)
        ps = []
        for pat, users in pat_users.items():
            if len(users) >= min_count:
                pat_landmarks = [format_label(None, gh) for gh in pat]
                ps.append({"pattern": list(pat), "pattern_str": " → ".join(pat),
                           "pattern_landmarks": pat_landmarks,
                           "pattern_str_cn": " → ".join(pat_landmarks),
                           "length": L, "support_count": len(users),
                           "support_ratio": round(len(users) / max(n_users, 1), 4),
                           "total_occurrences": pat_total[pat]})
        ps.sort(key=lambda p: -p["support_count"])
        by_length[L] = ps
        write_jsonl(output_dir / f"patterns_L{L}.jsonl", ps)
        all_top.extend(ps[:top_k])
    all_top.sort(key=lambda p: -p["support_count"])
    write_jsonl(output_dir / "top_patterns.jsonl", all_top[:top_k])

    summary = {
        "ok": True, "skill": "mine_trajectory_patterns",
        "input": str(input), "output_dir": str(output_dir),
        "params": {"min_support": min_support, "min_count": min_count,
                   "max_length": max_length, "top_k": top_k, "precision": precision},
        "n_users_with_sequence": n_users,
        "patterns_by_length": {L: len(by_length[L]) for L in by_length},
        "top_3_patterns": all_top[:3],
        "outputs": {"top_patterns": str(output_dir / "top_patterns.jsonl"),
                    "summary": str(output_dir / "summary.json")},
    }
    write_summary(output_dir, summary)
    return summary


# 10. detect_trajectory_cooccurrence — 时空伴行
# ────────────────────────────────────────────────────────────────────

def detect_trajectory_cooccurrence(
    input: Path, output_dir: Path, *,
    precision: int = 6, min_overlap_min: float = 15.0,
    target_users: Optional[list[str]] = None, max_pairs: int = 10000,
    geohash_col: Optional[str] = None, user_col: Optional[str] = None,
) -> dict:
    from itertools import combinations
    output_dir.mkdir(parents=True, exist_ok=True)
    records = read_records(input)
    by_user: dict[str, list[dict]] = defaultdict(list)
    for r in records:
        n = normalize_record(r, geohash_col=geohash_col, user_col=user_col,
                             geohash_precision=precision)
        if not n["user_id"] or n["lat"] is None:
            continue
        arr = parse_timestamp(r.get("arrive_time")) or n["ts"]
        lv = parse_timestamp(r.get("leave_time")) or arr
        if not (arr and lv) or lv <= arr:
            continue
        by_user[str(n["user_id"])].append({
            "lat": n["lat"], "lon": n["lon"], "arrive": arr, "leave": lv,
            "gh": encode_geohash(n["lat"], n["lon"], precision),
        })

    if target_users:
        ts_set = set(target_users)
        pairs = [(u, v) for u in ts_set for v in by_user
                 if u != v and u in by_user]
    else:
        pairs = list(combinations(by_user.keys(), 2))
    if len(pairs) > max_pairs:
        pairs = pairs[:max_pairs]

    pair_summary, events = [], []
    for u, v in pairs:
        a_by_gh: dict[str, list] = defaultdict(list)
        b_by_gh: dict[str, list] = defaultdict(list)
        for s in by_user[u]: a_by_gh[s["gh"]].append(s)
        for s in by_user[v]: b_by_gh[s["gh"]].append(s)
        evs = []
        for gh in a_by_gh.keys() & b_by_gh.keys():
            for sa in a_by_gh[gh]:
                for sb in b_by_gh[gh]:
                    start = max(sa["arrive"], sb["arrive"])
                    end = min(sa["leave"], sb["leave"])
                    if end > start:
                        ov = (end - start).total_seconds() / 60
                        if ov >= min_overlap_min:
                            evs.append({
                                "user_a": u, "user_b": v, "geohash": gh,
                                "overlap_start": start.strftime("%Y-%m-%dT%H:%M:%S"),
                                "overlap_end": end.strftime("%Y-%m-%dT%H:%M:%S"),
                                "overlap_minutes": round(ov, 2),
                                "lat": round((sa["lat"] + sb["lat"]) / 2, 6),
                                "lon": round((sa["lon"] + sb["lon"]) / 2, 6),
                            })
        if evs:
            events.extend(evs)
            pair_summary.append({
                "user_a": u, "user_b": v, "cooccurrence_count": len(evs),
                "total_overlap_min": round(sum(e["overlap_minutes"] for e in evs), 2),
                "shared_geohashes": sorted({e["geohash"] for e in evs}),
            })
    pair_summary.sort(key=lambda x: -x["total_overlap_min"])
    enrich_records(events)  # events 里有 geohash 字段
    # pair_summary 的 shared_geohashes 是列表，需要单独处理
    for ps in pair_summary:
        ps["shared_landmarks"] = [format_label(None, gh) for gh in ps["shared_geohashes"]]
    write_jsonl(output_dir / "cooccurrence_pairs.jsonl", pair_summary)
    write_jsonl(output_dir / "cooccurrence_events.jsonl", events)

    summary = {
        "ok": True, "skill": "detect_trajectory_cooccurrence",
        "input": str(input), "output_dir": str(output_dir),
        "params": {"precision": precision, "min_overlap_min": min_overlap_min,
                   "target_users": target_users},
        "n_candidate_pairs": len(pairs), "n_pairs_with_cooc": len(pair_summary),
        "n_events": len(events), "top_3_pairs": pair_summary[:3],
        "outputs": {"cooccurrence_pairs": str(output_dir / "cooccurrence_pairs.jsonl"),
                    "cooccurrence_events": str(output_dir / "cooccurrence_events.jsonl"),
                    "summary": str(output_dir / "summary.json")},
    }
    write_summary(output_dir, summary)
    return summary


# 11. analyze_spatiotemporal_accessibility — 等时圈
# ────────────────────────────────────────────────────────────────────

def analyze_spatiotemporal_accessibility(
    od_matrix: Path, output_dir: Path, *,
    origin_geohash: str, budgets_min: list[float] = [15, 30, 60],
    min_flow: int = 2,
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    edges = read_records(od_matrix)
    graph: dict[str, list[tuple[str, float]]] = defaultdict(list)
    cells: set[str] = set()
    for e in edges:
        og = e.get("origin_geohash"); dg = e.get("destination_geohash")
        flow = e.get("flow", 0); dur_h = e.get("avg_duration_h")
        if not (og and dg) or flow < min_flow or dur_h is None:
            continue
        graph[og].append((dg, dur_h * 60))
        cells.add(og); cells.add(dg)

    INF = float("inf")
    best = {c: INF for c in cells}
    if origin_geohash in best:
        best[origin_geohash] = 0.0
    visited = set()
    while True:
        nxt, nxt_t = None, INF
        for c in cells - visited:
            if best.get(c, INF) < nxt_t:
                nxt_t, nxt = best[c], c
        if nxt is None or nxt_t == INF:
            break
        visited.add(nxt)
        for d, t_min in graph.get(nxt, []):
            new_t = nxt_t + t_min
            if new_t < best.get(d, INF):
                best[d] = new_t

    isochrones = {}
    rows = []
    for b in budgets_min:
        reachable = sorted([c for c, t in best.items()
                            if t <= b and c != origin_geohash],
                           key=lambda x: best[x])
        cell_info = []
        for c in reachable:
            la, lo = decode_geohash(c)
            cell_info.append({"geohash": c, "travel_min": round(best[c], 2),
                              "lat": round(la, 5), "lon": round(lo, 5)})
            rows.append({"budget_min": b, "geohash": c,
                         "travel_min": round(best[c], 2),
                         "lat": round(la, 5), "lon": round(lo, 5)})
        isochrones[str(int(b))] = {"budget_min": b,
                                   "n_reachable_cells": len(cell_info),
                                   "cells": cell_info}
    o_lat, o_lon = decode_geohash(origin_geohash)
    o_info = lookup_landmark(None, origin_geohash) or {}
    enrich_records(rows)
    for b in isochrones.values():
        enrich_records(b["cells"])
    write_jsonl(output_dir / "reachable_cells.jsonl", rows)
    (output_dir / "isochrone.json").write_text(
        json.dumps({"origin": {"geohash": origin_geohash,
                               "landmark": o_info.get("landmark"),
                               "district": o_info.get("district"),
                               "lat": round(o_lat, 5), "lon": round(o_lon, 5)},
                    "isochrones": isochrones}, ensure_ascii=False, indent=2),
        encoding="utf-8")
    # viz_input: 起点（异常红）+ 各预算到达的网格（按 budget_min 着色）
    viz = [to_viz_record(origin_geohash, None, 0.0, "origin",
                         anomaly=True,
                         extra_text=f"起点: {format_label(None, origin_geohash)}")]
    for r in rows[:24]:
        viz.append(to_viz_record(r["geohash"], None, r["travel_min"], "travel_min",
                                 extra_text=f"{format_label(None, r['geohash'])}: "
                                            f"{r['travel_min']}min 内可达"))
    write_viz_input(output_dir, viz)

    summary = {
        "ok": True, "skill": "analyze_spatiotemporal_accessibility",
        "od_matrix": str(od_matrix), "output_dir": str(output_dir),
        "origin_geohash": origin_geohash,
        "origin_landmark": o_info.get("landmark"),
        "params": {"budgets_min": budgets_min, "min_flow": min_flow},
        "n_total_cells": len(cells),
        "n_edges": sum(len(v) for v in graph.values()),
        "isochrone_summary": {b: isochrones[b]["n_reachable_cells"] for b in isochrones},
        "outputs": {"isochrone": str(output_dir / "isochrone.json"),
                    "reachable_cells": str(output_dir / "reachable_cells.jsonl"),
                    "viz_input": str(output_dir / "viz_input.jsonl"),
                    "summary": str(output_dir / "summary.json")},
    }
    write_summary(output_dir, summary)
    return summary


# 12. classify_trajectory_state — 运动状态分类
# ────────────────────────────────────────────────────────────────────

def classify_trajectory_state(
    input: Path, output_dir: Path, *, precision: int = 6,
    geohash_col: Optional[str] = None, user_col: Optional[str] = None,
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    records = read_records(input)
    by_user: dict[str, list[dict]] = defaultdict(list)
    for r in records:
        n = normalize_record(r, geohash_col=geohash_col, user_col=user_col,
                             geohash_precision=precision)
        if not n["user_id"] or n["lat"] is None or not n["ts"]:
            continue
        by_user[str(n["user_id"])].append({"lat": n["lat"], "lon": n["lon"],
                                            "ts": n["ts"]})
    results = []
    for uid, pts in by_user.items():
        pts.sort(key=lambda x: x["ts"])
        if len(pts) < 2:
            results.append({"user_id": uid, "state": "insufficient_data",
                            "state_cn": "数据不足", "n_points": len(pts)})
            continue
        total_d = sum(haversine_m(pts[i - 1]["lat"], pts[i - 1]["lon"],
                                  pts[i]["lat"], pts[i]["lon"])
                      for i in range(1, len(pts)))
        net = haversine_m(pts[0]["lat"], pts[0]["lon"], pts[-1]["lat"], pts[-1]["lon"])
        dur_h = (pts[-1]["ts"] - pts[0]["ts"]).total_seconds() / 3600
        avg_kmh = (total_d / 1000) / max(dur_h, 0.01)
        direct = net / total_d if total_d > 0 else 0.0
        ghs = [encode_geohash(p["lat"], p["lon"], precision) for p in pts]
        unique_gh = len(set(ghs))
        dom = Counter(ghs).most_common(1)[0][1] / len(ghs)
        sample = pts if len(pts) <= 200 else [pts[i] for i in range(0, len(pts), len(pts) // 200)]
        max_span = max((haversine_m(sample[i]["lat"], sample[i]["lon"],
                                    sample[j]["lat"], sample[j]["lon"])
                        for i in range(len(sample)) for j in range(i + 1, len(sample))),
                       default=0.0)
        if dom >= 0.9 or max_span < 200:
            state = "stationary"; cn = "长时静止"
        elif direct >= 0.7 and max_span >= 3000:
            state = "directed_migration"; cn = "定向快速迁徙"
        elif unique_gh >= 5 and direct < 0.3:
            state = "frequent_circulation"; cn = "高频巡游"
        else:
            state = "random_walk"; cn = "随机游走"
        results.append({
            "user_id": uid, "state": state, "state_cn": cn,
            "n_points": len(pts), "duration_h": round(dur_h, 2),
            "total_distance_m": round(total_d, 2),
            "net_displacement_m": round(net, 2),
            "max_span_m": round(max_span, 2),
            "avg_speed_kmh": round(avg_kmh, 2),
            "directionality": round(direct, 3),
            "n_unique_geohash": unique_gh,
            "dominant_geohash_share": round(dom, 3),
        })
    write_jsonl(output_dir / "state_classification.jsonl", results)
    dist = Counter(r["state"] for r in results)
    summary = {
        "ok": True, "skill": "classify_trajectory_state",
        "input": str(input), "output_dir": str(output_dir),
        "n_users": len(results), "state_distribution": dict(dist),
        "outputs": {"state_classification": str(output_dir / "state_classification.jsonl"),
                    "summary": str(output_dir / "summary.json")},
    }
    write_summary(output_dir, summary)
    return summary


# 13. predict_next_location — 马尔可夫预测
# ────────────────────────────────────────────────────────────────────

def predict_next_location(
    input: Path, output_dir: Path, *,
    current_geohash: Optional[str] = None, current_time: Optional[str] = None,
    top_k: int = 5, precision: int = 5, train_only: bool = False,
    geohash_col: Optional[str] = None, user_col: Optional[str] = None,
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    records = read_records(input)
    by_user: dict[str, list[tuple]] = defaultdict(list)
    for r in records:
        n = normalize_record(r, geohash_col=geohash_col, user_col=user_col,
                             geohash_precision=precision)
        if not n["user_id"] or not n["geohash"]:
            continue
        ts = parse_timestamp(r.get("arrive_time")) or n["ts"]
        if ts:
            by_user[str(n["user_id"])].append((ts, n["geohash"]))
    trans: dict[str, Counter] = defaultdict(Counter)
    trans_slot: dict[tuple, Counter] = defaultdict(Counter)
    n_pairs = 0
    for uid, items in by_user.items():
        items.sort(key=lambda x: x[0])
        for i in range(len(items) - 1):
            cur_ts, cur = items[i]; _, nxt = items[i + 1]
            if cur == nxt: continue
            slot = get_time_slot(cur_ts.hour)
            trans[cur][nxt] += 1
            trans_slot[(cur, slot)][nxt] += 1
            n_pairs += 1
    trans_records = []
    for cur, nxts in trans.items():
        total = sum(nxts.values())
        for nxt, cnt in nxts.most_common():
            trans_records.append({"current_geohash": cur, "next_geohash": nxt,
                                  "count": cnt,
                                  "probability": round(cnt / total, 4)})
    enrich_records(trans_records)  # 自动加 current_landmark / next_landmark
    write_jsonl(output_dir / "transition_matrix.jsonl", trans_records)

    predictions = None
    if not train_only and current_geohash:
        slot = None
        if current_time:
            ts = parse_timestamp(current_time)
            if ts: slot = get_time_slot(ts.hour)
        cands: Counter = Counter()
        used_slot = False
        if slot and (current_geohash, slot) in trans_slot:
            cs = trans_slot[(current_geohash, slot)]
            if sum(cs.values()) >= 3:
                cands = cs; used_slot = True
        if not cands and current_geohash in trans:
            cands = trans[current_geohash]
        total = sum(cands.values())
        out = []
        if total > 0:
            for nxt, cnt in cands.most_common(top_k):
                out.append({"next_geohash": nxt,
                            "next_landmark": format_label(None, nxt),
                            "probability": round(cnt / total, 4),
                            "support_count": cnt, "used_time_slot": used_slot})
        predictions = {"current_geohash": current_geohash,
                       "current_landmark": format_label(None, current_geohash),
                       "current_time": current_time, "time_slot": slot,
                       "top_k": out}
    write_jsonl(output_dir / "predictions.jsonl",
                [predictions] if predictions else [])

    summary = {
        "ok": True, "skill": "predict_next_location",
        "input": str(input), "output_dir": str(output_dir),
        "params": {"current_geohash": current_geohash, "current_time": current_time,
                   "top_k": top_k, "train_only": train_only, "precision": precision},
        "n_unique_states": len(trans), "n_total_transitions": n_pairs,
        "predictions": predictions,
        "outputs": {"transition_matrix": str(output_dir / "transition_matrix.jsonl"),
                    "predictions": str(output_dir / "predictions.jsonl"),
                    "summary": str(output_dir / "summary.json")},
    }
    write_summary(output_dir, summary)
    return summary


# 14. measure_spatiotemporal_entropy — 时空分布熵 / 活力指数
# ────────────────────────────────────────────────────────────────────

def _shannon(counts: list[float]) -> float:
    total = sum(counts)
    if total <= 0: return 0.0
    return -sum((c / total) * math.log2(c / total) for c in counts if c > 0)


def measure_spatiotemporal_entropy(
    output_dir: Path, *,
    od_matrix: Optional[Path] = None, evidence: Optional[Path] = None,
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    inflow: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    outflow: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    timeflow: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    visit_count: dict[str, float] = defaultdict(float)
    if od_matrix:
        for e in read_records(od_matrix):
            og, dg = e.get("origin_geohash"), e.get("destination_geohash")
            f = float(e.get("flow", 0))
            if og and dg and f > 0:
                inflow[dg][og] += f; outflow[og][dg] += f
                visit_count[dg] += f; visit_count[og] += f
    if evidence:
        for r in read_records(evidence):
            n = normalize_record(r)
            if not n["geohash"] or not n["ts"]:
                continue
            v = float(n["metric"]) if isinstance(n.get("metric"), (int, float)) else 0
            if v > 0:
                timeflow[n["geohash"]][get_time_slot(n["ts"].hour)] += v
                visit_count[n["geohash"]] += v
    records = []
    for gh in set(inflow) | set(outflow) | set(timeflow):
        h_in = _shannon(list(inflow[gh].values())) if gh in inflow else 0.0
        h_out = _shannon(list(outflow[gh].values())) if gh in outflow else 0.0
        h_t = _shannon(list(timeflow[gh].values())) if gh in timeflow else 0.0
        cnt = visit_count.get(gh, 0)
        avg_h = (h_in + h_out + h_t) / 3
        records.append({
            "region_geohash": gh,
            "n_origins": len(inflow.get(gh, {})),
            "n_destinations": len(outflow.get(gh, {})),
            "n_active_time_slots": len(timeflow.get(gh, {})),
            "H_inflow": round(h_in, 3), "H_outflow": round(h_out, 3),
            "H_time": round(h_t, 3), "H_avg": round(avg_h, 3),
            "visit_count": round(cnt, 1),
            "vitality_index": round(avg_h * math.log1p(cnt), 3),
        })
    records.sort(key=lambda x: -x["vitality_index"])
    enrich_records(records)  # region_geohash → landmark
    write_jsonl(output_dir / "entropy_by_region.jsonl", records)
    # viz_input: 用 vitality_index 作为热度
    viz = [to_viz_record(r["region_geohash"], None,
                         r["vitality_index"], "vitality",
                         extra_text=f"{format_label(None, r['region_geohash'])}: 活力指数 {r['vitality_index']}")
           for r in records[:24] if r.get("region_geohash")]
    write_viz_input(output_dir, viz)
    summary = {
        "ok": True, "skill": "measure_spatiotemporal_entropy",
        "od_matrix": str(od_matrix) if od_matrix else None,
        "evidence": str(evidence) if evidence else None,
        "output_dir": str(output_dir), "n_regions": len(records),
        "top_5_vitality": records[:5],
        "outputs": {"entropy_by_region": str(output_dir / "entropy_by_region.jsonl"),
                    "viz_input": str(output_dir / "viz_input.jsonl"),
                    "summary": str(output_dir / "summary.json")},
    }
    write_summary(output_dir, summary)
    return summary


# 15. analyze_event_impact — 外部事件影响 DID
# ────────────────────────────────────────────────────────────────────

def _t_stat(a: list[float], b: list[float]) -> tuple[float, float]:
    if len(a) < 2 or len(b) < 2:
        return 0.0, 1.0
    ma, mb = statistics.mean(a), statistics.mean(b)
    va, vb = statistics.variance(a), statistics.variance(b)
    se = math.sqrt(va / len(a) + vb / len(b))
    if se < 1e-9: return 0.0, 1.0
    t = (ma - mb) / se
    p = 2 * (1 - 0.5 * (1 + math.erf(abs(t) / math.sqrt(2))))
    return round(t, 3), round(p, 4)


def analyze_event_impact(
    input: Path, output_dir: Path, *,
    event_start: str, event_end: str,
    treatment_geohash: list[str], control_geohash: Optional[list[str]] = None,
    event_name: str = "未知事件", metric: str = "checkin_count",
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    es = parse_timestamp(event_start); ee = parse_timestamp(event_end)
    if not (es and ee):
        summary = {"ok": False, "skill": "analyze_event_impact",
                   "error": "invalid event_start/event_end"}
        write_summary(output_dir, summary)
        return summary
    treatment = set(treatment_geohash)
    records = read_records(input)
    bins: dict[str, dict[str, list[float]]] = defaultdict(
        lambda: {"pre": [], "post": []}
    )
    for r in records:
        n = normalize_record(r)
        gh = n["geohash"]; ts = n["ts"]
        if not (gh and ts):
            continue
        meta = r.get("meta", {}) or {}
        feat = meta.get("features", {}) if "meta" in r else {}
        v = feat.get(metric) if feat else r.get(metric)
        if v is None:
            v = n["metric"]
        try:
            v = float(v) if v is not None else None
        except (ValueError, TypeError):
            continue
        if v is None: continue
        if es <= ts <= ee: bins[gh]["post"].append(v)
        elif ts < es: bins[gh]["pre"].append(v)
    if not control_geohash:
        control = set(bins.keys()) - treatment
    else:
        control = set(control_geohash)

    region_impacts = []
    for gh in (treatment | control):
        pre = bins.get(gh, {"pre": [], "post": []})["pre"]
        post = bins.get(gh, {"pre": [], "post": []})["post"]
        if not pre and not post: continue
        pre_m = statistics.mean(pre) if pre else 0.0
        post_m = statistics.mean(post) if post else 0.0
        pct = ((post_m - pre_m) / pre_m * 100) if pre_m > 0 else None
        t_v, p_v = _t_stat(post, pre)
        region_impacts.append({
            "region_geohash": gh,
            "group": "treatment" if gh in treatment else "control",
            "pre_event_mean": round(pre_m, 2),
            "during_event_mean": round(post_m, 2),
            "pre_n": len(pre), "post_n": len(post),
            "change_pct": round(pct, 2) if pct is not None else None,
            "t_stat": t_v, "p_value": p_v,
        })
    t_pre = [statistics.mean(bins[g]["pre"]) for g in treatment if bins[g]["pre"]]
    t_post = [statistics.mean(bins[g]["post"]) for g in treatment if bins[g]["post"]]
    c_pre = [statistics.mean(bins[g]["pre"]) for g in control if bins[g]["pre"]]
    c_post = [statistics.mean(bins[g]["post"]) for g in control if bins[g]["post"]]
    did = None
    if t_pre and t_post and c_pre and c_post:
        did = (statistics.mean(t_post) - statistics.mean(t_pre)) - \
              (statistics.mean(c_post) - statistics.mean(c_pre))
    treatment_pcts = [r["change_pct"] for r in region_impacts
                      if r["group"] == "treatment" and r["change_pct"] is not None]
    avg_pct = round(sum(treatment_pcts) / len(treatment_pcts), 2) if treatment_pcts else None

    enrich_records(region_impacts)
    write_jsonl(output_dir / "region_impacts.jsonl", region_impacts)
    # viz_input: 处理组红色，对照组按 |change_pct| 着色
    viz = []
    for r in region_impacts:
        gh = r.get("region_geohash")
        if not gh: continue
        is_tr = r["group"] == "treatment"
        viz.append(to_viz_record(
            gh, None, abs(r["change_pct"] or 0), "change_pct",
            anomaly=is_tr,  # 处理组标记红
            extra_text=f"{format_label(None, gh)}: [{r['group']}] "
                       f"事前 {r['pre_event_mean']} → 事中 {r['during_event_mean']} "
                       f"({'+' if (r['change_pct'] or 0) >= 0 else ''}{r['change_pct']}%)"))
    write_viz_input(output_dir, viz)
    summary = {
        "ok": True, "skill": "analyze_event_impact",
        "input": str(input), "output_dir": str(output_dir),
        "event": {"name": event_name, "start": event_start, "end": event_end,
                  "metric": metric},
        "treatment_geohash": sorted(treatment),
        "treatment_landmarks": [format_label(None, g) for g in sorted(treatment)],
        "control_geohash_sample": sorted(control)[:50],
        "did_estimate": round(did, 2) if did is not None else None,
        "treatment_avg_change_pct": avg_pct,
        "n_region_impacts": len(region_impacts),
        "outputs": {"region_impacts": str(output_dir / "region_impacts.jsonl"),
                    "viz_input": str(output_dir / "viz_input.jsonl"),
                    "summary": str(output_dir / "summary.json")},
    }
    write_summary(output_dir, summary)
    return summary
