"""
Geohash → 业务地名 查表工具。
独立模块，被 search.py / render_heatmap.py 共用。
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Optional


_CACHE: Optional[dict] = None


def _load() -> dict:
    global _CACHE
    if _CACHE is not None:
        return _CACHE
    path = Path(__file__).parent.parent / "data" / "geohash_landmarks.json"
    if not path.exists():
        _CACHE = {}
        return _CACHE
    with open(path, "r", encoding="utf-8") as f:
        _CACHE = json.load(f)
    return _CACHE


def lookup(city: str, geohash: str) -> Optional[dict]:
    """
    给定 city + geohash，返回 {landmark, district, lat, lon, tags} 或 None。
    支持前缀匹配：传 wtw3sxxx 也能命中 wtw3s。
    """
    data = _load()
    city_map = data.get(city, {})
    if not city_map:
        return None
    # 精确匹配
    if geohash in city_map:
        return city_map[geohash]
    # 前缀匹配（用户给了 6 位 geohash，但表里只到 5 位）
    for prefix, info in city_map.items():
        if isinstance(info, dict) and geohash.startswith(prefix):
            return info
    return None


def enrich_evidence(evidence: dict) -> dict:
    """给 evidence 记录原地添加 landmark 字段，并把展示文本改成业务地名。"""
    geo = evidence.get("meta", {}).get("geo_scope", {})
    city = geo.get("city", "")
    geohash = geo.get("geohash", "")
    info = lookup(city, geohash)
    if info:
        geo["landmark"] = info["landmark"]
        geo["district"] = info["district"]
        geo["lat"] = info["lat"]
        geo["lon"] = info["lon"]
        geo["tags"] = info["tags"]
        geo["technical_geohash"] = geohash

        text = evidence.get("text")
        if isinstance(text, str) and geohash:
            landmark = info["landmark"]
            pattern = re.compile(
                rf"{re.escape(city)}\s*[\(（]\s*geohash\s*[:：]\s*{re.escape(geohash)}\s*[\)）]\s*区域"
            )
            display_text = pattern.sub(f"{landmark}区域", text)
            if display_text == text:
                display_text = text.replace(f"geohash:{geohash}", landmark).replace(f"geohash：{geohash}", landmark)
            evidence["text"] = display_text
            evidence["display_text"] = display_text
    return evidence


def format_label(city: str, geohash: str) -> str:
    """
    返回 '陆家嘴金融区(wtw3s)' 形式的展示串；查不到则退化为 'Shanghai-wtw3s'。
    """
    info = lookup(city, geohash)
    if info:
        return f"{info['landmark']}({geohash})"
    return f"{city}-{geohash}"
