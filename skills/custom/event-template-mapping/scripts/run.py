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

def method_confidence(result: dict[str, Any]) -> float:
    items = result.get("matches") or result.get("clusters") or result.get("groups") or result.get("anomalies") or []
    return sum(float(i.get("confidence", 1.0)) for i in items) / len(items) if items else 0.0

def subject_ids(result: dict[str, Any]) -> list[str]:
    ids = []
    for key in ("matches", "clusters", "groups"):
        for item in result.get(key, []):
            ids.extend(item.get("object_ids", []))
            if item.get("subject_id"):
                ids.append(item["subject_id"])
    return ids

def evidence_ids(result: dict[str, Any]) -> list[str]:
    ids = []
    for item in result.get("matches", []):
        ids.extend(item.get("evidence_frame_ids", []))
    return ids

def build_event(template_name: str, template: dict[str, Any], method_results: list[dict[str, Any]], camera_id: str | None) -> dict[str, Any] | None:
    if not method_results:
        return None
    hits = [r for r in method_results if r.get("matched")]
    join_policy = template.get("join_policy", "all_required")
    if join_policy == "all_required" and len(hits) != len(method_results):
        return None
    if join_policy == "any_required" and not hits:
        return None
    scores = [method_confidence(r) for r in method_results]
    score = sum(scores) / len(scores) if scores else 0.0
    if join_policy == "weighted_score":
        weights = template.get("score_policy", {}).get("weights", {})
        total_weight = 0.0
        total = 0.0
        for result in method_results:
            weight = float(weights.get(result.get("method"), 1.0))
            total_weight += weight
            total += method_confidence(result) * weight
        score = total / total_weight if total_weight else 0.0
    if score < float(template.get("score_policy", {}).get("min_score", 0)):
        return None
    related = sorted({sid for r in method_results for sid in subject_ids(r) if sid})
    starts = [i.get("start_time") for r in method_results for i in r.get("matches", []) if i.get("start_time")]
    ends = [i.get("end_time") for r in method_results for i in r.get("matches", []) if i.get("end_time")]
    roi_ids = [i.get("roi_id") for r in method_results for i in r.get("matches", []) + r.get("clusters", []) + r.get("groups", []) if i.get("roi_id")]
    stable_key = "|".join([str(camera_id), template_name, ",".join(related), ",".join(roi_ids)])
    suffix = sha1(stable_key.encode("utf-8")).hexdigest()[:10]
    rules = []
    for method in template.get("methods", []):
        if method.get("target_labels"):
            rules.append("label:" + ",".join(method["target_labels"]))
        if method.get("roi_types"):
            rules.append("roi:" + ",".join(method["roi_types"]))
        if method.get("min_duration_seconds") is not None:
            rules.append(f"duration>={method['min_duration_seconds']}")
    rules.extend(f"method:{hit.get('method')}" for hit in hits)
    return {"event_id": f"EVT_{template_name}_{suffix}", "event_type": template.get("event_type", template_name), "camera_id": camera_id, "roi_id": roi_ids[0] if roi_ids else None, "start_time": min(starts) if starts else None, "end_time": max(ends) if ends else None, "confidence": round(score, 4), "severity": template.get("severity", "medium"), "related_tracks": related, "reason": f"{template_name} matched by " + ", ".join(h.get("method", "unknown") for h in hits), "method_hits": [h.get("method") for h in hits], "rule_hits": rules, "evidence_frame_ids": sorted({fid for r in method_results for fid in evidence_ids(r)}), "status": "candidate", "created_at": utc_now_iso()}

SKILL = "event-template-mapping"

def main() -> int:
    p = argparse.ArgumentParser(description="Map method-level event results into event candidates.")
    p.add_argument("--input", help="Optional JSON payload; CLI flags override matching fields.")
    p.add_argument("--method-results-json")
    p.add_argument("--template-json")
    p.add_argument("--template-name")
    p.add_argument("--camera-id")
    p.add_argument("--config")
    p.add_argument("--output")
    args = p.parse_args()
    input_data = load_input(args.input)
    config = load_config(args.config)
    template_name = args.template_name or input_value(input_data, "template_name") or input_value(input_data, "event_type", default="event")
    camera_id = args.camera_id or input_value(input_data, "camera_id")
    template = load_dict(args.template_json) if args.template_json else input_dict(input_data, "template") or config.get("event_templates", {}).get(template_name, {})
    method_results = load_list(args.method_results_json, "method_results") if args.method_results_json else input_list(input_data, "method_results")
    event = build_event(template_name, template, method_results, camera_id)
    data = {"matched": False} if not event else {"matched": True, **event}
    return emit(success(SKILL, data, event.get("confidence", 0) if event else 1.0), args.output)

if __name__ == "__main__":
    raise SystemExit(main())
