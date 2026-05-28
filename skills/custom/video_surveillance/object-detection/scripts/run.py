
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

SKILL = "object-detection"

def run_ultralytics(frames, labels, thresholds, model_config):
    try:
        from ultralytics import YOLO
    except ModuleNotFoundError:
        return failed(SKILL, "YOLO_DEPENDENCY_MISSING", "ultralytics is not installed")
    model_path = model_config.get("model_path") or model_config.get("name", "yolov8n.pt")
    try:
        model = YOLO(model_path)
    except Exception as exc:
        return failed(SKILL, "YOLO_MODEL_LOAD_FAILED", f"Failed to load YOLO model: {exc}")
    label_set = set(labels)
    detections = []
    for frame_index, frame in enumerate(frames):
        image_path = Path(str(frame.get("image_uri", "")).removeprefix("file://"))
        if not image_path.exists():
            return failed(SKILL, "FRAME_NOT_FOUND", f"Frame image not found: {image_path}")
        try:
            results = model.predict(source=str(image_path), conf=float(model_config.get("default_threshold", 0.25)), device=model_config.get("device"), verbose=False)
        except Exception as exc:
            return failed(SKILL, "YOLO_INFERENCE_FAILED", f"YOLO inference failed: {exc}", True)
        objects = []
        for result in results:
            for box_index, box in enumerate(result.boxes):
                cls_id = int(box.cls[0].item())
                label = result.names.get(cls_id, str(cls_id))
                mapped = model_config.get("label_map", {}).get(label, label)
                conf = float(box.conf[0].item())
                if label_set and mapped not in label_set:
                    continue
                if conf < float(thresholds.get(mapped, 0.0)):
                    continue
                objects.append({"object_id": f"det_{frame_index + 1:04d}_{box_index + 1:03d}", "label": mapped, "confidence": round(conf, 4), "bbox": [round(float(v), 2) for v in box.xyxy[0].tolist()], "model_label": label})
        detections.append({"frame_id": frame.get("frame_id"), "timestamp": frame.get("timestamp"), "objects": objects})
    vals = [o["confidence"] for d in detections for o in d["objects"]]
    return success(SKILL, {"detections": detections, "model": str(model_path)}, sum(vals) / len(vals) if vals else 0)

def main() -> int:
    p = argparse.ArgumentParser(description="Detect objects in sampled frames.")
    p.add_argument("--frames-json", required=True)
    p.add_argument("--labels", default="person")
    p.add_argument("--provider", choices=["ultralytics"])
    p.add_argument("--model-path")
    p.add_argument("--config")
    p.add_argument("--output")
    args = p.parse_args()
    config = load_config(args.config)
    frames = load_list(args.frames_json, "frames")
    labels = [x.strip() for x in args.labels.split(",") if x.strip()]
    model_config = dict(config.get("models", {}).get("object_detection", {}))
    if args.provider:
        model_config["provider"] = args.provider
    if args.model_path:
        model_config["model_path"] = args.model_path
    provider = model_config.get("provider", "ultralytics")
    if provider != "ultralytics":
        return emit(failed(SKILL, "UNSUPPORTED_PROVIDER", f"Unsupported object detection provider: {provider}"), args.output)
    result = run_ultralytics(frames, labels, model_config.get("thresholds", {}), model_config)
    return emit(result, args.output)

if __name__ == "__main__":
    raise SystemExit(main())
