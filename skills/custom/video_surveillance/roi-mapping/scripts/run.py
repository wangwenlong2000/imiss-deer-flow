#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import shutil
import subprocess
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from hashlib import sha1, sha256
from pathlib import Path
from typing import Any
from uuid import uuid4

try:
    import yaml
except Exception:
    yaml = None

VERSION = "1.0.0"

def load_structured(path: str | None) -> Any:
    if not path:
        return {}
    source = Path(path)
    text = source.read_text(encoding="utf-8")
    if source.suffix.lower() in {".yaml", ".yml"}:
        if yaml is None:
            raise RuntimeError("PyYAML is required to read YAML files")
        return yaml.safe_load(text) or {}
    return json.loads(text) if text.strip() else {}

def load_config(path: str | None) -> dict[str, Any]:
    data = load_structured(path)
    return data if isinstance(data, dict) else {}

def load_list(path: str | None, key: str) -> list[dict[str, Any]]:
    data = load_structured(path)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        value = data.get(key) or data.get("data", {}).get(key, [])
        return value if isinstance(value, list) else []
    return []

def load_dict(path: str | None, key: str | None = None) -> dict[str, Any]:
    data = load_structured(path)
    if not isinstance(data, dict):
        return {}
    if key and key in data and isinstance(data[key], dict):
        return data[key]
    if key and isinstance(data.get("data"), dict) and isinstance(data["data"].get(key), dict):
        return data["data"][key]
    return data

def load_input(path: str | None) -> dict[str, Any]:
    data = load_structured(path)
    return data if isinstance(data, dict) else {}

def input_value(input_data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    nested = input_data.get("data") if isinstance(input_data.get("data"), dict) else {}
    for key in keys:
        if input_data.get(key) is not None:
            return input_data[key]
        if nested.get(key) is not None:
            return nested[key]
    return default

def input_list(input_data: dict[str, Any], key: str) -> list[dict[str, Any]]:
    value = input_value(input_data, key, default=[])
    return value if isinstance(value, list) else []

def input_dict(input_data: dict[str, Any], key: str) -> dict[str, Any]:
    value = input_value(input_data, key, default={})
    return value if isinstance(value, dict) else {}

def parse_json_arg(value: str | None, default: Any) -> Any:
    return json.loads(value) if value else default

def success(skill: str, data: dict[str, Any], confidence: float = 1.0) -> dict[str, Any]:
    return {"skill": skill, "version": VERSION, "status": "success", "confidence": round(float(confidence), 4), "data": data}

def failed(skill: str, code: str, message: str, retryable: bool = False, detail: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"skill": skill, "version": VERSION, "status": "failed", "error_code": code, "message": message, "retryable": retryable, "detail": detail or {}}

def emit(result: dict[str, Any], output: str | None) -> int:
    text = json.dumps(result, ensure_ascii=False, indent=2)
    if output:
        path = Path(output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0 if result.get("status") == "success" else 1

def parse_time(value: str | None, fallback: datetime | None = None) -> datetime:
    if not value:
        return fallback or datetime.now().astimezone()
    return datetime.fromisoformat(value.replace("Z", "+00:00"))

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def iso_add_seconds(value: str, seconds: int | float) -> str:
    return (parse_time(value) + timedelta(seconds=float(seconds))).isoformat()

def bbox_center(bbox: list[float]) -> tuple[float, float]:
    x1, y1, x2, y2 = bbox
    return ((x1 + x2) / 2, (y1 + y2) / 2)

def bbox_bottom_center(bbox: list[float]) -> tuple[float, float]:
    x1, _, x2, y2 = bbox
    return ((x1 + x2) / 2, y2)

def bbox_area(bbox: list[float]) -> float:
    x1, y1, x2, y2 = bbox
    return max(0.0, x2 - x1) * max(0.0, y2 - y1)

def bbox_overlap(a: list[float], b: list[float]) -> float:
    ax1, ay1, ax2, ay2 = a
    bx1, by1, bx2, by2 = b
    inter = bbox_area([max(ax1, bx1), max(ay1, by1), min(ax2, bx2), min(ay2, by2)])
    base = bbox_area(a)
    return 0.0 if base <= 0 else inter / base

def polygon_bounds(polygon: list[list[float]]) -> list[float]:
    xs = [p[0] for p in polygon]
    ys = [p[1] for p in polygon]
    return [min(xs), min(ys), max(xs), max(ys)] if xs and ys else [0, 0, 0, 0]

def polygon_area(polygon: list[list[float]]) -> float:
    if len(polygon) < 3:
        return 0.0
    total = 0.0
    for i, (x1, y1) in enumerate(polygon):
        x2, y2 = polygon[(i + 1) % len(polygon)]
        total += x1 * y2 - x2 * y1
    return abs(total) / 2

def point_in_polygon(point: tuple[float, float], polygon: list[list[float]]) -> bool:
    x, y = point
    inside = False
    j = len(polygon) - 1
    for i, (xi, yi) in enumerate(polygon):
        xj, yj = polygon[j]
        if (yi > y) != (yj > y):
            x_at_y = (xj - xi) * (y - yi) / ((yj - yi) or 1e-9) + xi
            if x < x_at_y:
                inside = not inside
        j = i
    return inside

def distance(a: tuple[float, float], b: tuple[float, float]) -> float:
    return math.hypot(a[0] - b[0], a[1] - b[1])

def subject_id(item: dict[str, Any]) -> str:
    return item.get("track_id") or item.get("object_id") or item.get("id") or "unknown"

def iter_detection_objects(detections: list[dict[str, Any]]):
    for detection in detections:
        for obj in detection.get("objects", []):
            enriched = dict(obj)
            enriched.setdefault("frame_id", detection.get("frame_id"))
            enriched.setdefault("timestamp", detection.get("timestamp"))
            yield enriched

SKILL = "roi-mapping"

def main() -> int:
    p = argparse.ArgumentParser(description="Match tracks or objects to ROIs.")
    p.add_argument("--input", help="Optional JSON payload; CLI flags override matching fields.")
    p.add_argument("--tracks-json")
    p.add_argument("--objects-json")
    p.add_argument("--rois-json")
    p.add_argument("--camera-id")
    p.add_argument("--roi-types")
    p.add_argument("--position-strategy", choices=["bottom_center", "center_point", "bbox_overlap"])
    p.add_argument("--config")
    p.add_argument("--output")
    args = p.parse_args()
    input_data = load_input(args.input)
    config = load_config(args.config)
    camera_id = args.camera_id or input_value(input_data, "camera_id", default="CAM_DEERFLOW_001")
    rois = load_list(args.rois_json, "rois") if args.rois_json else input_list(input_data, "rois") or config.get("cameras", {}).get(camera_id, {}).get("rois", [])
    objects = load_list(args.objects_json, "objects") if args.objects_json else input_list(input_data, "objects") or (load_list(args.tracks_json, "tracks") if args.tracks_json else input_list(input_data, "tracks"))
    roi_value = args.roi_types if args.roi_types is not None else input_value(input_data, "roi_types")
    roi_types = set(x.strip() for x in roi_value.split(",")) if isinstance(roi_value, str) else set(roi_value or [roi.get("type") for roi in rois])
    position_strategy = args.position_strategy or input_value(input_data, "position_strategy", default="bottom_center")
    matches = []
    for obj in objects:
        bbox = obj.get("last_bbox") or obj.get("bbox")
        if not bbox:
            continue
        for roi in rois:
            if roi_types and roi.get("type") not in roi_types:
                continue
            polygon = roi.get("polygon", [])
            if position_strategy == "bbox_overlap":
                overlap = bbox_overlap(bbox, polygon_bounds(polygon))
                matched = overlap > 0
            else:
                point = bbox_center(bbox) if position_strategy == "center_point" else bbox_bottom_center(bbox)
                matched = point_in_polygon(point, polygon)
                overlap = 1.0 if matched else 0.0
            matches.append({"object_id": subject_id(obj), "roi_id": roi.get("id"), "roi_type": roi.get("type"), "overlap_ratio": round(overlap,4), "matched": matched, "confidence": obj.get("confidence", 1.0)})
    return emit(success(SKILL, {"matches": matches}), args.output)

if __name__ == "__main__":
    raise SystemExit(main())
