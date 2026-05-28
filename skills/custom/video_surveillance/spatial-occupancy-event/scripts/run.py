
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

SKILL = "spatial-occupancy-event"

def main() -> int:
    p = argparse.ArgumentParser(description="Detect spatial occupancy from ROI matches and tracks.")
    p.add_argument("--roi-matches-json", required=True)
    p.add_argument("--tracks-json")
    p.add_argument("--target-labels", default="")
    p.add_argument("--roi-types", default="")
    p.add_argument("--excluded-roi-types", default="")
    p.add_argument("--min-overlap-ratio", type=float, default=0.0)
    p.add_argument("--min-confidence", type=float, default=0.0)
    p.add_argument("--method-config")
    p.add_argument("--output")
    args = p.parse_args()
    cfg = load_dict(args.method_config)
    labels = set(cfg.get("target_labels") or [x.strip() for x in args.target_labels.split(",") if x.strip()])
    roi_types = set(cfg.get("roi_types") or [x.strip() for x in args.roi_types.split(",") if x.strip()])
    excluded = set(cfg.get("excluded_roi_types") or [x.strip() for x in args.excluded_roi_types.split(",") if x.strip()])
    min_overlap = float(cfg.get("min_overlap_ratio", args.min_overlap_ratio))
    min_confidence = float(cfg.get("min_confidence", args.min_confidence))
    tracks = {t.get("track_id"): t for t in load_list(args.tracks_json, "tracks")}
    matches = []
    for match in load_list(args.roi_matches_json, "matches"):
        if not match.get("matched"):
            continue
        track = tracks.get(match.get("object_id"), {})
        label = track.get("label") or match.get("label")
        if labels and label not in labels:
            continue
        if roi_types and match.get("roi_type") not in roi_types:
            continue
        if match.get("roi_type") in excluded:
            continue
        if float(match.get("overlap_ratio", 0)) < min_overlap:
            continue
        confidence = min(float(track.get("confidence", match.get("confidence", 1.0))), float(match.get("confidence", 1.0)))
        if confidence < min_confidence:
            continue
        matches.append({"subject_id": match.get("object_id"), "subject_type": "track" if match.get("object_id") in tracks else "object", "label": label, "roi_id": match.get("roi_id"), "roi_type": match.get("roi_type"), "overlap_ratio": match.get("overlap_ratio", 0), "confidence": round(confidence, 4), "evidence_frame_ids": track.get("evidence_frame_ids", [])})
    conf = sum(float(m["confidence"]) for m in matches) / len(matches) if matches else 0
    return emit(success(SKILL, {"method": "spatial_occupancy", "matched": bool(matches), "matches": matches}, conf), args.output)

if __name__ == "__main__":
    raise SystemExit(main())
