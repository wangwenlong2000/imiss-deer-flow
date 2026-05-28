#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import json
import os
import re
import time
from hashlib import sha1
from pathlib import Path
from typing import Any

try:
    import requests
except Exception:  # pragma: no cover
    requests = None  # type: ignore[assignment]


SUPPORTED_ROUTE_MODES = {"driving", "walking", "riding", "transit"}
SUPPORTED_ACTIONS = ["capabilities", "route", "weather", "road-traffic", "geocode", "around-traffic"]
SUPPORTED_INPUT_COORD_TYPES = {"bd09ll", "gcj02", "wgs84"}
SUPPORTED_OUTPUT_COORD_TYPES = {"bd09ll", "gcj02"}
DEFAULT_AROUND_TRAFFIC_RADIUS = 500
DEFAULT_ROAD_GRADE = "0"
CITY_ALIASES = {
    "xian": "西安市",
    "xi'an": "西安市",
    "xi an": "西安市",
    "xian city": "西安市",
    "西安": "西安市",
}
TRAFFIC_STATUS_MAP = {
    0: "未知",
    1: "畅通",
    2: "缓行",
    3: "拥堵",
    4: "严重拥堵",
}


def _require_requests():
    if requests is None:
        raise RuntimeError("requests is required. Please run: pip install requests")
    return requests


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


def _to_int(value: Any) -> int | None:
    try:
        return int(float(value))
    except Exception:
        return None


def _positive_float(value: Any) -> float | None:
    number = _to_float(value)
    if number is None or number <= 0:
        return None
    return number


def _safe_slug(text: str) -> str:
    slug = re.sub(r"[^0-9A-Za-z_-]+", "-", str(text or "").strip())
    slug = re.sub(r"-{2,}", "-", slug).strip("-")
    return slug or "traffic"


def _traffic_cache_dir() -> Path:
    return Path(__file__).resolve().parents[4] / "datasets" / "road-traffic" / "cache" / "baidu_traffic"


def _collect_congestion_sections(traffic_data: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not traffic_data:
        return []
    sections: list[dict[str, Any]] = []
    for item in traffic_data.get("road_traffic", []) or []:
        if not isinstance(item, dict):
            continue
        for section in item.get("congestion_sections", []) or []:
            if isinstance(section, dict):
                sections.append(section)
    return sections


def extract_speed_kmh(traffic_data: dict[str, Any] | None) -> float | None:
    if not traffic_data:
        return None
    evaluation = traffic_data.get("evaluation", {}) or {}
    eval_speed = _positive_float(evaluation.get("speed"))
    if eval_speed is not None:
        return round(eval_speed, 2)

    speeds = [_positive_float(section.get("speed")) for section in _collect_congestion_sections(traffic_data)]
    numeric_speeds = [speed for speed in speeds if speed is not None]
    if not numeric_speeds:
        return None
    return round(sum(numeric_speeds) / len(numeric_speeds), 2)


def predict_trend(traffic_data: dict[str, Any] | None, horizon_minutes: int = 30) -> dict[str, Any]:
    horizon = int(horizon_minutes)
    result: dict[str, Any] = {
        "horizon_minutes": horizon,
        "trend": "未知",
        "summary": f"未来{horizon}分钟趋势未知",
        "confidence": "低",
        "basis": [],
    }
    if not traffic_data:
        result["summary"] = f"缺少实时数据，无法判断未来{horizon}分钟趋势"
        return result

    sections = _collect_congestion_sections(traffic_data)
    evaluation = traffic_data.get("evaluation", {}) or {}
    trend_score = 0
    basis: list[str] = []
    for section in sections:
        trend = str(section.get("congestion_trend", "")).strip()
        desc = str(section.get("section_desc", "")).strip() or "未标注路段"
        speed = _positive_float(section.get("speed"))
        speed_text = f"{speed:.2f} km/h" if speed is not None else "速度未知"
        if trend == "加重":
            trend_score += 1
        elif trend == "缓解":
            trend_score -= 1
        if trend:
            basis.append(f"{desc}（{trend}，{speed_text}）")
        else:
            basis.append(f"{desc}（{speed_text}）")

    if trend_score > 0:
        result.update({"trend": "加重", "summary": f"未来{horizon}分钟预计拥堵有加重风险", "confidence": "中"})
    elif trend_score < 0:
        result.update({"trend": "缓解", "summary": f"未来{horizon}分钟预计拥堵可能缓解", "confidence": "中"})
    else:
        status = evaluation.get("status")
        if status in {3, 4}:
            result.update({"trend": "持平", "summary": f"未来{horizon}分钟预计拥堵高位持平", "confidence": "中" if sections else "低"})
        elif status == 2:
            result.update({"trend": "持平", "summary": f"未来{horizon}分钟预计缓行态势持平", "confidence": "中" if sections else "低"})
        elif status == 1:
            result.update({"trend": "持平", "summary": f"未来{horizon}分钟预计总体维持畅通", "confidence": "低"})

    if basis:
        result["basis"] = basis[:5]
    return result


def format_traffic_summary(traffic_data: dict[str, Any] | None) -> str:
    if not traffic_data:
        return "当前无实时路况数据"
    evaluation = traffic_data.get("evaluation", {}) or {}
    status = TRAFFIC_STATUS_MAP.get(evaluation.get("status"), "未知")
    speed = extract_speed_kmh(traffic_data)
    speed_text = f"{speed:.2f}" if speed is not None else "未知"
    desc = (
        evaluation.get("description")
        or evaluation.get("status_desc")
        or traffic_data.get("description")
        or "无整体描述"
    )
    return f"整体态势：{status}（均速 {speed_text} km/h，{desc}）"


class BaiduTrafficClient:
    def __init__(self, ak: str, qps_limit: float = 3.0, ttl_seconds: int = 300):
        self.ak = str(ak or "").strip()
        self.qps_limit = max(float(qps_limit), 0.2)
        self.ttl_seconds = int(ttl_seconds)
        self.connect_timeout = float(os.getenv("ROAD_TRAFFIC_HTTP_CONNECT_TIMEOUT", "3"))
        self.read_timeout = float(os.getenv("ROAD_TRAFFIC_HTTP_READ_TIMEOUT", "8"))
        self.min_interval = 1.0 / self.qps_limit
        self._last_request_ts = 0.0
        self.cache_root = _traffic_cache_dir()
        self.cache_root.mkdir(parents=True, exist_ok=True)

    def _cache_path(self, city: str, road_name: str) -> Path:
        digest = sha1(f"{city}::{road_name}".encode("utf-8")).hexdigest()[:12]
        return self.cache_root / f"{_safe_slug(city)}_{_safe_slug(road_name)}_{digest}.json"

    def _sleep_if_needed(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_request_ts
        if self._last_request_ts > 0 and elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last_request_ts = time.monotonic()

    def get_road_traffic(self, road_name: str, city: str) -> dict[str, Any] | None:
        if not self.ak or not road_name:
            return None

        cache_path = self._cache_path(city, road_name)
        if cache_path.exists():
            try:
                cached = json.loads(cache_path.read_text(encoding="utf-8"))
                if time.time() - float(cached.get("_fetched_at", 0)) <= self.ttl_seconds:
                    return cached
            except Exception:
                pass

        self._sleep_if_needed()
        payload = _request_json(
            "https://api.map.baidu.com/traffic/v1/road",
            params={"road_name": road_name, "city": city, "ak": self.ak},
        )
        if payload.get("status") == 0 and payload.get("evaluation"):
            payload["_fetched_at"] = time.time()
            cache_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            return payload
        return None


def _to_text_payload(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _normalize_city_hint(city_hint: str) -> str:
    text = str(city_hint or "").strip()
    if not text:
        return ""
    lowered = re.sub(r"\s+", " ", text.lower().replace("-", " ").strip())
    return CITY_ALIASES.get(lowered, text)


def _load_local_env() -> None:
    if os.getenv("BAIDU_AK"):
        return

    repo_env = Path(__file__).resolve().parents[4] / ".env"
    if not repo_env.exists():
        return

    try:
        from dotenv import load_dotenv

        load_dotenv(repo_env, override=False)
        return
    except Exception:
        pass

    try:
        for line in repo_env.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            key = key.strip()
            if key != "BAIDU_AK":
                continue
            value = value.strip().strip("\"'")
            if value:
                os.environ.setdefault(key, value)
            return
    except Exception:
        return


def _resolve_ak(ak: str | None) -> str:
    if not ak:
        _load_local_env()
    return str(ak or os.getenv("BAIDU_AK", "")).strip()


def _missing_payload(
    action: str,
    *,
    missing_fields: list[str],
    reason: str,
    ask_user: str,
    options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "ok": False,
        "action": action,
        "can_proceed": False,
        "missing_fields": missing_fields,
        "reason": reason,
        "ask_user": ask_user,
    }
    if options:
        payload["options"] = options
    return payload


def _parse_lat_lng(raw: str) -> tuple[float, float] | None:
    matched = re.match(r"^\s*([+-]?\d+(?:\.\d+)?)\s*[,，]\s*([+-]?\d+(?:\.\d+)?)\s*$", str(raw or ""))
    if not matched:
        return None

    first = _to_float(matched.group(1))
    second = _to_float(matched.group(2))
    if first is None or second is None:
        return None

    lat, lng = first, second
    if abs(lat) > 90 and abs(lng) <= 90:
        lat, lng = lng, lat
    if abs(lat) > 90 or abs(lng) > 180:
        return None
    return lat, lng


def _normalize_center(raw: str) -> dict[str, Any]:
    lat_lng = _parse_lat_lng(raw)
    if lat_lng is None:
        raise ValueError("center must be a coordinate pair like '34.245,108.945' or '108.945,34.245'")
    lat, lng = lat_lng
    return {
        "raw": str(raw or "").strip(),
        "lat": round(lat, 8),
        "lng": round(lng, 8),
        "baidu_center": f"{round(lat, 8)},{round(lng, 8)}",
    }


def _normalize_radius(value: Any, default: int = DEFAULT_AROUND_TRAFFIC_RADIUS) -> int:
    radius = _to_int(value)
    if radius is None:
        radius = default
    if radius < 1 or radius > 1000:
        raise ValueError("radius must be in range [1,1000] meters")
    return radius


def _normalize_road_grade(value: str) -> str:
    text = str(value or "").strip() or DEFAULT_ROAD_GRADE
    parts = [part.strip() for part in text.split(",") if part.strip()]
    if not parts:
        return DEFAULT_ROAD_GRADE
    invalid = [part for part in parts if part not in {"0", "1", "2", "3", "4", "5"}]
    if invalid:
        raise ValueError("road_grade must use values 0,1,2,3,4,5 separated by commas")
    return ",".join(parts)


def _normalize_coord_type(value: str, supported: set[str], default: str) -> str:
    text = str(value or "").strip().lower() or default
    if text not in supported:
        raise ValueError(f"unsupported coord type: {text}")
    return text


def _request_json(url: str, params: dict[str, Any], timeout: tuple[float, float] | None = None) -> dict[str, Any]:
    requests_mod = _require_requests()
    connect_timeout = float(os.getenv("ROAD_TRAFFIC_HTTP_CONNECT_TIMEOUT", "3"))
    read_timeout = float(os.getenv("ROAD_TRAFFIC_HTTP_READ_TIMEOUT", "8"))
    resp = requests_mod.get(url, params=params, timeout=timeout or (connect_timeout, read_timeout))
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError("Baidu API response is not a JSON object")
    return data


def _resolve_place_to_point(place: str, city_hint: str, ak: str) -> dict[str, Any]:
    text = str(place or "").strip()
    if not text:
        raise ValueError("place is empty")

    lat_lng = _parse_lat_lng(text)
    if lat_lng is not None:
        lat, lng = lat_lng
        return {
            "query": text,
            "provider": "direct_coordinate",
            "lat": round(lat, 8),
            "lng": round(lng, 8),
            "adcode": None,
        }

    params = {"address": text, "output": "json", "ak": ak}
    city = _normalize_city_hint(city_hint)
    if city:
        params["city"] = city

    payload = _request_json("https://api.map.baidu.com/geocoding/v3/", params=params)
    status = int(payload.get("status", -1))
    if status != 0:
        message = str(payload.get("msg") or payload.get("message") or payload.get("status_desc") or f"status={status}")
        raise ValueError(f"geocoding failed for '{text}': {message}")

    result = payload.get("result") or {}
    location = result.get("location") or {}
    lat = _to_float(location.get("lat"))
    lng = _to_float(location.get("lng"))
    if lat is None or lng is None:
        raise ValueError(f"geocoding failed for '{text}': invalid location")

    return {
        "query": text,
        "provider": "baidu_geocoding_v3",
        "lat": round(lat, 8),
        "lng": round(lng, 8),
        "adcode": result.get("adcode"),
        "level": str(result.get("level") or ""),
        "confidence": _to_int(result.get("confidence")),
    }


def _reverse_geocode_adcode(*, lat: float, lng: float, ak: str) -> dict[str, Any]:
    payload = _request_json(
        "https://api.map.baidu.com/reverse_geocoding/v3/",
        params={
            "location": f"{lat},{lng}",
            "coordtype": "bd09ll",
            "output": "json",
            "extensions_poi": "0",
            "ak": ak,
        },
    )
    status = int(payload.get("status", -1))
    if status != 0:
        message = str(payload.get("msg") or payload.get("message") or payload.get("status_desc") or f"status={status}")
        raise ValueError(f"reverse geocoding failed: {message}")

    result = payload.get("result") or {}
    address_component = result.get("addressComponent") or {}
    return {
        "adcode": str(address_component.get("adcode") or "").strip(),
        "formatted_address": result.get("formatted_address") or "",
        "province": address_component.get("province") or "",
        "city": address_component.get("city") or "",
        "district": address_component.get("district") or "",
    }


def _looks_like_city_level_location(location: str, city_hint: str) -> bool:
    text = str(location or "").strip()
    city = _normalize_city_hint(city_hint)
    if city and text in {city, city.removesuffix("市")}:
        return True
    if text.endswith(("自治州", "地区", "盟")):
        return True
    if text.endswith("市") and not any(token in text for token in ("区", "县", "旗")):
        return True
    return False


def _city_level_adcode_from_district(adcode: str) -> str:
    code = str(adcode or "").strip()
    if re.fullmatch(r"\d{6}", code):
        return f"{code[:4]}00"
    return code


def geocode_by_baidu(
    *,
    address: str,
    city_hint: str = "",
    ak: str,
    ret_coordtype: str = "",
) -> dict[str, Any]:
    api_key = str(ak or "").strip()
    if not api_key:
        raise ValueError("missing BAIDU_AK")

    address_text = str(address or "").strip()
    if not address_text:
        raise ValueError("address is empty")

    ret_type = str(ret_coordtype or "").strip().lower()
    if ret_type == "bd09ll":
        ret_type = ""
    if ret_type and ret_type not in {"gcj02ll", "bd09mc"}:
        raise ValueError("ret_coordtype must be bd09ll, gcj02ll, or bd09mc when provided")

    lat_lng = _parse_lat_lng(address_text)
    if lat_lng is not None:
        lat, lng = lat_lng
        return {
            "ok": True,
            "action": "geocode",
            "input_params": {
                "address": address_text,
                "city": _normalize_city_hint(city_hint),
                "ret_coordtype": ret_type or "bd09ll",
            },
            "provider": "direct_coordinate",
            "location": {
                "lat": round(lat, 8),
                "lng": round(lng, 8),
                "center": f"{round(lat, 8)},{round(lng, 8)}",
            },
            "confidence": None,
            "comprehension": None,
            "level": "",
            "adcode": None,
            "formatted_address": address_text,
            "summary": "Input is already a valid coordinate pair.",
        }

    params = {"address": address_text, "output": "json", "ak": api_key}
    city = _normalize_city_hint(city_hint)
    if city:
        params["city"] = city
    if ret_type:
        params["ret_coordtype"] = ret_type

    payload = _request_json("https://api.map.baidu.com/geocoding/v3/", params=params)
    status = int(payload.get("status", -1))
    if status != 0:
        message = str(payload.get("msg") or payload.get("message") or payload.get("status_desc") or f"status={status}")
        raise RuntimeError(f"geocoding failed: {message}")

    result = payload.get("result") or {}
    location = result.get("location") or {}
    lat = _to_float(location.get("lat"))
    lng = _to_float(location.get("lng"))
    if lat is None or lng is None:
        raise RuntimeError("geocoding failed: invalid location")

    return {
        "ok": True,
        "action": "geocode",
        "input_params": {
            "address": address_text,
            "city": city,
            "ret_coordtype": ret_type or "bd09ll",
        },
        "provider": "baidu_geocoding_v3",
        "location": {
            "lat": round(lat, 8),
            "lng": round(lng, 8),
            "center": f"{round(lat, 8)},{round(lng, 8)}",
        },
        "confidence": _to_int(result.get("confidence")),
        "comprehension": _to_int(result.get("comprehension")),
        "level": str(result.get("level") or ""),
        "adcode": result.get("adcode"),
        "formatted_address": str(result.get("formatted_address") or address_text),
        "summary": f"Geocoded '{address_text}' to {round(lat, 8)},{round(lng, 8)}.",
    }


def _flatten_steps(steps_raw: Any) -> list[dict[str, Any]]:
    flattened: list[dict[str, Any]] = []
    if isinstance(steps_raw, list):
        for row in steps_raw:
            if isinstance(row, dict):
                flattened.append(row)
            elif isinstance(row, list):
                for item in row:
                    if isinstance(item, dict):
                        flattened.append(item)
    return flattened


def _strip_instruction(raw: Any) -> str:
    text = str(raw or "")
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _traffic_preview(road_traffic: Any, *, max_roads: int = 8, max_sections: int = 3) -> list[dict[str, Any]]:
    preview: list[dict[str, Any]] = []
    if not isinstance(road_traffic, list):
        return preview
    items = [item for item in road_traffic if isinstance(item, dict)]
    items.sort(key=lambda item: 0 if item.get("congestion_sections") else 1)
    for item in items[:max_roads]:
        sections = []
        for sec in (item.get("congestion_sections") or [])[:max_sections]:
            if not isinstance(sec, dict):
                continue
            sections.append(
                {
                    "section_desc": sec.get("section_desc") or "",
                    "status": sec.get("status"),
                    "speed": sec.get("speed"),
                    "congestion_distance": sec.get("congestion_distance"),
                    "trend": sec.get("congestion_trend") or "",
                }
            )
        preview.append(
            {
                "road_name": item.get("road_name") or "",
                "congestion_sections_count": len(item.get("congestion_sections") or []),
                "congestion_sections_preview": sections,
            }
        )
    return preview


def _build_traffic_summary(payload: dict[str, Any]) -> dict[str, Any]:
    evaluation = payload.get("evaluation") or {}
    road_traffic = payload.get("road_traffic") or []
    congested_count = 0
    if isinstance(road_traffic, list):
        congested_count = sum(1 for item in road_traffic if isinstance(item, dict) and item.get("congestion_sections"))
    return {
        "description": payload.get("description") or "",
        "status": evaluation.get("status"),
        "status_desc": evaluation.get("status_desc") or "",
        "road_count": len(road_traffic) if isinstance(road_traffic, list) else 0,
        "congested_road_count": congested_count,
    }


def plan_route_by_baidu(
    *,
    origin: str,
    destination: str,
    city_hint: str = "",
    mode: str = "driving",
    ak: str,
) -> dict[str, Any]:
    api_key = str(ak or "").strip()
    if not api_key:
        raise ValueError("missing BAIDU_AK")

    route_mode = str(mode or "").strip().lower() or "driving"
    if route_mode not in SUPPORTED_ROUTE_MODES:
        raise ValueError(f"unsupported route mode: {route_mode}")

    origin_point = _resolve_place_to_point(origin, city_hint=city_hint, ak=api_key)
    destination_point = _resolve_place_to_point(destination, city_hint=city_hint, ak=api_key)

    url = f"https://api.map.baidu.com/directionlite/v1/{route_mode}"
    params = {
        "origin": f"{origin_point['lat']},{origin_point['lng']}",
        "destination": f"{destination_point['lat']},{destination_point['lng']}",
        "ak": api_key,
    }
    route_payload = _request_json(url, params=params)
    status = int(route_payload.get("status", -1))
    if status != 0:
        message = str(route_payload.get("message") or route_payload.get("msg") or route_payload.get("status_desc") or f"status={status}")
        raise RuntimeError(f"directionlite failed: {message}")

    result = route_payload.get("result") or {}
    routes = result.get("routes") or []
    if not routes:
        raise RuntimeError("directionlite returned no routes")

    first_route = routes[0] if isinstance(routes[0], dict) else {}
    distance_m = _to_int(first_route.get("distance")) or 0
    duration_s = _to_int(first_route.get("duration")) or 0
    toll_cny = _to_float(first_route.get("toll"))

    steps = []
    for idx, step in enumerate(_flatten_steps(first_route.get("steps")), start=1):
        instruction = _strip_instruction(step.get("instruction"))
        if not instruction:
            continue
        steps.append(
            {
                "index": idx,
                "instruction": instruction,
                "distance_m": _to_int(step.get("distance")),
                "duration_s": _to_int(step.get("duration")),
            }
        )

    return {
        "ok": True,
        "action": "route",
        "mode": route_mode,
        "origin": origin_point,
        "destination": destination_point,
        "summary": {
            "distance_m": distance_m,
            "distance_km": round(distance_m / 1000.0, 3),
            "duration_s": duration_s,
            "duration_min": round(duration_s / 60.0, 1),
            "toll_cny": None if toll_cny is None else round(toll_cny, 2),
            "route_count": int(len(routes)),
        },
        "steps_preview": steps[:15],
    }


def query_weather_by_baidu(
    *,
    location: str,
    city_hint: str = "",
    ak: str,
) -> dict[str, Any]:
    api_key = str(ak or "").strip()
    if not api_key:
        raise ValueError("missing BAIDU_AK")

    location_text = str(location or "").strip()
    if not location_text:
        raise ValueError("location is empty")

    point = _resolve_place_to_point(location_text, city_hint=city_hint, ak=api_key)
    district_id = str(point.get("adcode") or "").strip()
    reverse_info: dict[str, Any] = {}

    if not district_id:
        reverse_info = _reverse_geocode_adcode(lat=float(point["lat"]), lng=float(point["lng"]), ak=api_key)
        district_id = str(reverse_info.get("adcode") or "").strip()
        if district_id and _looks_like_city_level_location(location_text, city_hint):
            district_id = _city_level_adcode_from_district(district_id)

    if not district_id and city_hint:
        city = _normalize_city_hint(city_hint)
        city_point = _resolve_place_to_point(city, city_hint=city, ak=api_key)
        district_id = str(city_point.get("adcode") or "").strip()
        if not district_id:
            reverse_info = _reverse_geocode_adcode(lat=float(city_point["lat"]), lng=float(city_point["lng"]), ak=api_key)
            district_id = str(reverse_info.get("adcode") or "").strip()
            if district_id:
                district_id = _city_level_adcode_from_district(district_id)

    if not district_id:
        raise RuntimeError("weather query failed: cannot resolve district_id")

    payload = _request_json(
        "https://api.map.baidu.com/weather/v1/",
        params={
            "district_id": district_id,
            "data_type": "all",
            "ak": api_key,
        },
    )
    status = int(payload.get("status", -1))
    if status != 0:
        message = str(payload.get("message") or payload.get("msg") or payload.get("status_desc") or f"status={status}")
        raise RuntimeError(f"weather api failed: {message}")

    result = payload.get("result") or {}
    now = result.get("now") or {}
    location_info = result.get("location") or {}
    forecasts = result.get("forecasts") or []

    forecast_preview: list[dict[str, Any]] = []
    if isinstance(forecasts, list):
        for item in forecasts[:3]:
            if not isinstance(item, dict):
                continue
            forecast_preview.append(
                {
                    "date": item.get("date") or item.get("week") or "",
                    "text_day": item.get("text_day") or item.get("text") or "",
                    "text_night": item.get("text_night") or "",
                    "high": item.get("high") or "",
                    "low": item.get("low") or "",
                    "wc_day": item.get("wc_day") or item.get("wc") or "",
                    "wd_day": item.get("wd_day") or item.get("wd") or "",
                }
            )

    return {
        "ok": True,
        "action": "weather",
        "district_id": district_id,
        "input_params": {
            "location": location_text,
            "city": _normalize_city_hint(city_hint),
        },
        "resolved_place": {
            "lat": point.get("lat"),
            "lng": point.get("lng"),
            "adcode_source": "geocoding" if point.get("adcode") else "reverse_geocoding",
            "reverse_formatted_address": reverse_info.get("formatted_address") or "",
        },
        "location": {
            "country": location_info.get("country") or "",
            "province": location_info.get("province") or "",
            "city": location_info.get("city") or _normalize_city_hint(city_hint),
            "name": location_info.get("name") or location_text,
        },
        "now": {
            "text": now.get("text") or "",
            "temp": now.get("temp"),
            "feels_like": now.get("feels_like"),
            "rh": now.get("rh"),
            "wind_class": now.get("wind_class"),
            "wind_dir": now.get("wind_dir"),
            "uptime": now.get("uptime"),
        },
        "forecast_preview": forecast_preview,
    }


def query_single_road_traffic_by_baidu(
    *,
    road_name: str,
    city: str,
    ak: str,
    horizon_minutes: int = 30,
) -> dict[str, Any]:
    api_key = str(ak or "").strip()
    if not api_key:
        raise ValueError("missing BAIDU_AK")

    road = str(road_name or "").strip()
    if not road:
        raise ValueError("road_name is empty")

    city_text = _normalize_city_hint(city)
    if not city_text:
        raise ValueError("city is empty")

    client = BaiduTrafficClient(ak=api_key)
    traffic_data = client.get_road_traffic(road_name=road, city=city_text)
    if not traffic_data:
        return {
            "ok": False,
            "action": "road-traffic",
            "road_name": road,
            "city": city_text,
            "reason": "no realtime traffic data returned by Baidu traffic API",
        }

    evaluation = traffic_data.get("evaluation") or {}
    road_traffic = traffic_data.get("road_traffic") or []

    sections_preview: list[dict[str, Any]] = []
    if isinstance(road_traffic, list):
        for item in road_traffic[:2]:
            if not isinstance(item, dict):
                continue
            for sec in (item.get("congestion_sections") or [])[:5]:
                if not isinstance(sec, dict):
                    continue
                sections_preview.append(
                    {
                        "section_desc": sec.get("section_desc") or "",
                        "status": sec.get("status"),
                        "speed": sec.get("speed"),
                        "trend": sec.get("congestion_trend") or "",
                    }
                )

    return {
        "ok": True,
        "action": "road-traffic",
        "road_name": road,
        "city": city_text,
        "summary": format_traffic_summary(traffic_data),
        "evaluation": {
            "status": evaluation.get("status"),
            "status_desc": evaluation.get("status_desc") or "",
            "speed": evaluation.get("speed"),
            "congestion_sections_count": evaluation.get("congestion_sections_count"),
        },
        "speed_kmh": extract_speed_kmh(traffic_data),
        "trend_prediction": predict_trend(traffic_data, horizon_minutes=horizon_minutes),
        "sections_preview": sections_preview,
    }


def query_around_traffic_by_baidu(
    *,
    center: str,
    ak: str,
    radius: int = DEFAULT_AROUND_TRAFFIC_RADIUS,
    road_grade: str = DEFAULT_ROAD_GRADE,
    coord_type_input: str = "bd09ll",
    coord_type_output: str = "bd09ll",
) -> dict[str, Any]:
    api_key = str(ak or "").strip()
    if not api_key:
        raise ValueError("missing BAIDU_AK")

    center_info = _normalize_center(center)
    radius_m = _normalize_radius(radius)
    grade = _normalize_road_grade(road_grade)
    input_coord = _normalize_coord_type(coord_type_input, SUPPORTED_INPUT_COORD_TYPES, "bd09ll")
    output_coord = _normalize_coord_type(coord_type_output, SUPPORTED_OUTPUT_COORD_TYPES, "bd09ll")

    payload = _request_json(
        "https://api.map.baidu.com/traffic/v1/around",
        params={
            "ak": api_key,
            "center": center_info["baidu_center"],
            "radius": radius_m,
            "road_grade": grade,
            "coord_type_input": input_coord,
            "coord_type_output": output_coord,
        },
    )
    status = int(payload.get("status", -1))
    if status != 0:
        message = str(payload.get("message") or payload.get("msg") or payload.get("status_desc") or f"status={status}")
        raise RuntimeError(f"around traffic api failed: {message}")

    evaluation = payload.get("evaluation") or {}
    road_traffic = payload.get("road_traffic") or []
    return {
        "ok": True,
        "action": "around-traffic",
        "input_params": {
            "center": center_info["baidu_center"],
            "lat": center_info["lat"],
            "lng": center_info["lng"],
            "radius": radius_m,
            "road_grade": grade,
            "coord_type_input": input_coord,
            "coord_type_output": output_coord,
        },
        "summary": _build_traffic_summary(payload),
        "evaluation": {
            "status": evaluation.get("status"),
            "status_desc": evaluation.get("status_desc") or "",
        },
        "road_traffic_preview": _traffic_preview(road_traffic),
    }


def build_capabilities_payload() -> dict[str, Any]:
    return {
        "ok": True,
        "action": "capabilities",
        "intent_dispatch": "llm_only",
        "interaction_rule": "llm_must_check_required_fields_before_api_call",
        "optional_parameter_rule": "llm_should_fill_optional_fields_with_defaults_unless_user_specifies_other_values",
        "composition_rule": "for around traffic requests with place or road text but no coordinate, call geocode first and then around-traffic with the returned location.center",
        "supported_functions": [
            {
                "intent": "route_plan",
                "action": "route",
                "required_fields": ["origin", "destination"],
                "optional_fields": ["mode", "city"],
                "defaults": {"mode": "driving"},
                "field_specs": {
                    "origin": "route start point, e.g. 西安钟楼",
                    "destination": "route end point, e.g. 西安北站",
                    "mode": "one of driving/walking/transit/riding",
                    "city": "city hint to improve geocoding precision, e.g. 西安市",
                },
                "ask_templates": {
                    "origin": "请提供起点（origin），例如“西安钟楼”。",
                    "destination": "请提供终点（destination），例如“西安北站”。",
                    "mode": "请在 driving/walking/transit/riding 中选择路线模式。",
                    "city": "可选：请提供城市（city）以提高定位准确性，例如“西安市”。",
                },
            },
            {
                "intent": "weather_query",
                "action": "weather",
                "required_fields": ["location"],
                "optional_fields": ["city"],
                "notes": [
                    "script resolves Baidu district_id internally",
                    "if geocoding has no adcode, script uses reverse geocoding from resolved coordinates",
                ],
                "field_specs": {
                    "location": "weather target location, e.g. 西安市碑林区",
                    "city": "optional city hint, e.g. 西安市",
                },
                "ask_templates": {
                    "location": "请提供天气查询位置（location），例如“西安市碑林区”。",
                    "city": "可选：请提供城市（city），例如“西安市”。",
                },
            },
            {
                "intent": "single_road_traffic_query",
                "action": "road-traffic",
                "required_fields": ["road_name", "city"],
                "optional_fields": ["horizon_minutes"],
                "defaults": {"horizon_minutes": 30},
                "field_specs": {
                    "road_name": "target road name, e.g. 太乙路",
                    "city": "city name, e.g. 西安市",
                    "horizon_minutes": "trend horizon in minutes, default 30",
                },
                "ask_templates": {
                    "road_name": "请提供道路名（road_name），例如“太乙路”。",
                    "city": "请提供城市（city），例如“西安市”。",
                    "horizon_minutes": "可选：请提供趋势预测窗口（horizon_minutes，单位分钟），默认 30。",
                },
            },
            {
                "intent": "geocode_query",
                "action": "geocode",
                "required_fields": ["address"],
                "optional_fields": ["city", "ret_coordtype"],
                "defaults": {"ret_coordtype": "bd09ll"},
                "field_specs": {
                    "address": "address, POI, road name, or coordinate text to geocode, e.g. 西安钟楼",
                    "city": "optional city hint, required by the LLM when road/place text is ambiguous, e.g. 西安市",
                    "ret_coordtype": "optional Baidu geocoding output coordinate type: bd09ll default, or gcj02ll/bd09mc",
                },
                "ask_templates": {
                    "address": "请提供要地理编码的位置（address），例如“西安钟楼”。",
                    "city": "请提供城市（city）以避免道路或地点重名，例如“西安市”。",
                },
            },
            {
                "intent": "around_traffic_query",
                "action": "around-traffic",
                "required_fields": ["center"],
                "optional_fields": ["radius", "road_grade", "coord_type_input", "coord_type_output"],
                "defaults": {
                    "radius": DEFAULT_AROUND_TRAFFIC_RADIUS,
                    "road_grade": DEFAULT_ROAD_GRADE,
                    "coord_type_input": "bd09ll",
                    "coord_type_output": "bd09ll",
                },
                "field_specs": {
                    "center": "center coordinate as lat,lng or lng,lat; if user gives a road/place, LLM should call geocode first",
                    "radius": "query radius in meters, range [1,1000], default 500",
                    "road_grade": "road grades separated by commas; 0 all, 1 highway, 2 ring/expressway, 3 arterial, 4 secondary, 5 branch",
                    "coord_type_input": "bd09ll/gcj02/wgs84, default bd09ll",
                    "coord_type_output": "bd09ll/gcj02, default bd09ll",
                },
                "ask_templates": {
                    "center": "请提供中心点坐标（center），或提供明确的地点/道路名和城市以便先做地理编码。",
                },
            },
        ],
        "supported_actions": SUPPORTED_ACTIONS,
        "route_modes": sorted(SUPPORTED_ROUTE_MODES),
        "around_traffic_defaults": {
            "radius": DEFAULT_AROUND_TRAFFIC_RADIUS,
            "road_grade": DEFAULT_ROAD_GRADE,
            "coord_type_input": "bd09ll",
            "coord_type_output": "bd09ll",
        },
        "required_env": ["BAIDU_AK"],
    }


def run_simple_baidu_action(
    *,
    action: str,
    origin: str = "",
    destination: str = "",
    city: str = "",
    mode: str = "",
    road_name: str = "",
    location: str = "",
    address: str = "",
    center: str = "",
    radius: int = DEFAULT_AROUND_TRAFFIC_RADIUS,
    road_grade: str = DEFAULT_ROAD_GRADE,
    coord_type_input: str = "bd09ll",
    coord_type_output: str = "bd09ll",
    ret_coordtype: str = "",
    horizon_minutes: int = 30,
    ak: str = "",
) -> dict[str, Any]:
    action_name = str(action or "").strip().lower()

    if action_name == "capabilities":
        return build_capabilities_payload()

    if action_name == "route":
        missing_fields = [
            field
            for field in ["origin", "destination"]
            if not str({"origin": origin, "destination": destination}[field] or "").strip()
        ]
        if missing_fields:
            return _missing_payload(
                "route",
                missing_fields=missing_fields,
                reason="route planning needs both origin and destination",
                ask_user="请补充起点(origin)和终点(destination)，例如：从西安钟楼到西安北站。",
                options={"route_modes": sorted(SUPPORTED_ROUTE_MODES)},
            )

        mode_text = str(mode or "").strip().lower() or "driving"
        if mode_text not in SUPPORTED_ROUTE_MODES:
            return _missing_payload(
                "route",
                missing_fields=["mode"],
                reason=f"unsupported mode: {mode_text}",
                ask_user="请在 driving/walking/transit/riding 中选择路径规划模式。",
                options={"route_modes": sorted(SUPPORTED_ROUTE_MODES)},
            )

        api_key = _resolve_ak(ak)
        if not api_key:
            return _missing_payload(
                "route",
                missing_fields=["baidu_ak"],
                reason="BAIDU_AK is required for Baidu route planning API",
                ask_user="请提供 BAIDU_AK，或在环境变量中设置 BAIDU_AK 后重试。",
            )

        return plan_route_by_baidu(
            origin=origin,
            destination=destination,
            city_hint=city,
            mode=mode_text,
            ak=api_key,
        )

    if action_name == "weather":
        location_text = str(location or "").strip()
        if not location_text:
            return _missing_payload(
                "weather",
                missing_fields=["location"],
                reason="weather query needs location",
                ask_user="请提供天气查询位置，例如：西安市碑林区。",
            )

        api_key = _resolve_ak(ak)
        if not api_key:
            return _missing_payload(
                "weather",
                missing_fields=["baidu_ak"],
                reason="BAIDU_AK is required for Baidu weather API",
                ask_user="请提供 BAIDU_AK，或在环境变量中设置 BAIDU_AK 后重试。",
            )

        return query_weather_by_baidu(location=location_text, city_hint=city, ak=api_key)

    if action_name == "road-traffic":
        missing_fields = []
        if not str(road_name or "").strip():
            missing_fields.append("road_name")
        if not str(city or "").strip():
            missing_fields.append("city")

        if missing_fields:
            return _missing_payload(
                "road-traffic",
                missing_fields=missing_fields,
                reason="single road traffic query needs both road_name and city",
                ask_user="请补充道路名(road_name)和城市(city)，例如：road_name=太乙路, city=西安市。",
            )

        api_key = _resolve_ak(ak)
        if not api_key:
            return _missing_payload(
                "road-traffic",
                missing_fields=["baidu_ak"],
                reason="BAIDU_AK is required for Baidu traffic API",
                ask_user="请提供 BAIDU_AK，或在环境变量中设置 BAIDU_AK 后重试。",
            )

        return query_single_road_traffic_by_baidu(
            road_name=road_name,
            city=city,
            ak=api_key,
            horizon_minutes=horizon_minutes,
        )

    if action_name == "geocode":
        address_text = str(address or location or "").strip()
        if not address_text:
            return _missing_payload(
                "geocode",
                missing_fields=["address"],
                reason="geocode needs address",
                ask_user="请提供要地理编码的位置(address)，例如：西安钟楼。",
            )

        api_key = _resolve_ak(ak)
        if not api_key:
            return _missing_payload(
                "geocode",
                missing_fields=["baidu_ak"],
                reason="BAIDU_AK is required for Baidu geocoding API",
                ask_user="请在环境变量中设置 BAIDU_AK 后重试。",
            )

        return geocode_by_baidu(address=address_text, city_hint=city, ak=api_key, ret_coordtype=ret_coordtype)

    if action_name == "around-traffic":
        center_text = str(center or "").strip()
        if not center_text:
            return _missing_payload(
                "around-traffic",
                missing_fields=["center"],
                reason="around traffic query needs center coordinate",
                ask_user="请提供中心点坐标(center)，或提供明确的地点/道路名和城市以便先做地理编码。",
                options={"default_radius": DEFAULT_AROUND_TRAFFIC_RADIUS},
            )

        api_key = _resolve_ak(ak)
        if not api_key:
            return _missing_payload(
                "around-traffic",
                missing_fields=["baidu_ak"],
                reason="BAIDU_AK is required for Baidu around traffic API",
                ask_user="请在环境变量中设置 BAIDU_AK 后重试。",
            )

        return query_around_traffic_by_baidu(
            center=center_text,
            ak=api_key,
            radius=radius,
            road_grade=road_grade,
            coord_type_input=coord_type_input,
            coord_type_output=coord_type_output,
        )

    return {
        "ok": False,
        "action": action_name,
        "error": f"Unsupported action: {action_name}",
        "supported_actions": SUPPORTED_ACTIONS,
    }


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Simple Baidu traffic services. Intent decision must be made by LLM, not by script rules.")
    p.add_argument("--action", required=True, choices=SUPPORTED_ACTIONS, help="Action name")
    p.add_argument("--origin", default="", help="Route origin")
    p.add_argument("--destination", default="", help="Route destination")
    p.add_argument("--city", default="", help="City hint")
    p.add_argument("--mode", default="", help="Route mode: driving/walking/riding/transit")
    p.add_argument("--road-name", default="", help="Road name for single-road traffic query")
    p.add_argument("--location", default="", help="Location text for weather query")
    p.add_argument("--address", default="", help="Address, POI, or road text for geocode action")
    p.add_argument("--center", default="", help="Center coordinate for around-traffic action, lat,lng or lng,lat")
    p.add_argument("--radius", type=int, default=DEFAULT_AROUND_TRAFFIC_RADIUS, help="Radius in meters for around-traffic action, range [1,1000]")
    p.add_argument("--road-grade", default=DEFAULT_ROAD_GRADE, help="Road grade filter for around-traffic action, e.g. 0 or 1,2,3")
    p.add_argument("--coord-type-input", default="bd09ll", help="Input coordinate type for around-traffic: bd09ll/gcj02/wgs84")
    p.add_argument("--coord-type-output", default="bd09ll", help="Output coordinate type for around-traffic: bd09ll/gcj02")
    p.add_argument("--ret-coordtype", default="", help="Optional geocode ret_coordtype: gcj02ll or bd09mc")
    p.add_argument("--horizon-minutes", type=int, default=30, help="Trend horizon minutes for road-traffic action")
    p.add_argument("--ak", default="", help="Optional Baidu AK (fallback to env BAIDU_AK)")
    p.add_argument("--format", choices=["text", "json"], default="text", help="Output format")
    return p


def main() -> int:
    args = parser().parse_args()
    try:
        payload = run_simple_baidu_action(
            action=args.action,
            origin=args.origin,
            destination=args.destination,
            city=args.city,
            mode=args.mode,
            road_name=args.road_name,
            location=args.location,
            address=args.address,
            center=args.center,
            radius=args.radius,
            road_grade=args.road_grade,
            coord_type_input=args.coord_type_input,
            coord_type_output=args.coord_type_output,
            ret_coordtype=args.ret_coordtype,
            horizon_minutes=args.horizon_minutes,
            ak=args.ak,
        )
        print(_to_text_payload(payload) if args.format == "text" else json.dumps(payload, ensure_ascii=False))
        return 0 if payload.get("ok") else 1
    except Exception as exc:
        payload = {
            "ok": False,
            "action": args.action,
            "error": str(exc),
        }
        print(_to_text_payload(payload) if args.format == "text" else json.dumps(payload, ensure_ascii=False), file=os.sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
