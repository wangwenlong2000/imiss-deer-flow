
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

SKILL = "object-composition-event"

def main() -> int:
    p = argparse.ArgumentParser(description="Detect configured multi-object compositions.")
    p.add_argument("--detections-json")
    p.add_argument("--tracks-json")
    p.add_argument("--roi-matches-json")
    p.add_argument("--required-json", help='JSON list like [{"labels":["person"],"min_count":2}]')
    p.add_argument("--group-by", choices=["frame", "roi", "all"], default="frame")
    p.add_argument("--spatial-relation", choices=["near", "any"], default="any")
    p.add_argument("--max-distance-pixels", type=float, default=150)
    p.add_argument("--method-config")
    p.add_argument("--output")
    args = p.parse_args()
    cfg = load_dict(args.method_config)
    required = cfg.get("required") or parse_json_arg(args.required_json, [])
    group_by = cfg.get("group_by", args.group_by)
    max_distance = float(cfg.get("max_distance_pixels", args.max_distance_pixels))
    spatial = cfg.get("spatial_relation", args.spatial_relation)
    objects = list(iter_detection_objects(load_list(args.detections_json, "detections"))) + load_list(args.tracks_json, "tracks")
    roi_by_object = {m.get("object_id"): m.get("roi_id") for m in load_list(args.roi_matches_json, "matches") if m.get("matched")}
    grouped = defaultdict(list)
    for obj in objects:
        key = roi_by_object.get(subject_id(obj), "all") if group_by == "roi" else obj.get("frame_id", "all") if group_by == "frame" else "all"
        grouped[key].append(obj)
    groups = []
    for key, items in grouped.items():
        counts = Counter(i.get("label") for i in items)
        if any(sum(counts[label] for label in rule.get("labels", [])) < int(rule.get("min_count", 1)) for rule in required):
            continue
        if spatial == "near":
            centers = [bbox_center(i.get("last_bbox") or i.get("bbox")) for i in items if i.get("last_bbox") or i.get("bbox")]
            if len(centers) >= 2 and not any(distance(a,b) <= max_distance for idx,a in enumerate(centers) for b in centers[idx+1:]):
                continue
        confs = [float(i.get("confidence", 1.0)) for i in items]
        groups.append({"group_id": f"grp_{len(groups)+1:03d}", "object_ids": [subject_id(i) for i in items], "labels": sorted({i.get("label") for i in items if i.get("label")}), "roi_id": key if group_by == "roi" else None, "confidence": round(sum(confs)/max(len(confs),1), 4)})
    conf = sum(g["confidence"] for g in groups) / len(groups) if groups else 0
    return emit(success(SKILL, {"method": "object_composition", "matched": bool(groups), "groups": groups}, conf), args.output)

if __name__ == "__main__":
    raise SystemExit(main())
