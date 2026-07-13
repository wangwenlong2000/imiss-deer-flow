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

SKILL = "frame-sampling"

def main() -> int:
    p = argparse.ArgumentParser(description="Sample image frames from a local video.")
    p.add_argument("--input", help="Optional JSON payload; CLI flags override matching fields.")
    p.add_argument("--video", required=False)
    p.add_argument("--frames-json")
    p.add_argument("--camera-id")
    p.add_argument("--capture-seconds", type=float)
    p.add_argument("--interval-seconds", type=float)
    p.add_argument("--fps", type=float)
    p.add_argument("--started-at")
    p.add_argument("--output-dir")
    p.add_argument("--config")
    p.add_argument("--output")
    args = p.parse_args()
    input_data = load_input(args.input)
    config = load_config(args.config)
    camera_id = args.camera_id or input_value(input_data, "camera_id", default="CAM_DEERFLOW_001")
    frames = load_list(args.frames_json, "frames") if args.frames_json else input_list(input_data, "frames")
    if frames:
        return emit(success(SKILL, {"camera_id": camera_id, "frames": frames}), args.output)
    video = args.video or input_value(input_data, "video", "video_path", "raw_segment_uri", "file_path")
    if not video:
        return emit(failed(SKILL, "MISSING_VIDEO", "--video or --frames-json is required"), args.output)
    sampling = input_dict(input_data, "sampling_strategy")
    interval_seconds = args.interval_seconds if args.interval_seconds is not None else float(sampling.get("interval_seconds", 1.0))
    fps = args.fps if args.fps is not None else sampling.get("fps")
    capture_seconds = args.capture_seconds if args.capture_seconds is not None else input_value(input_data, "capture_seconds")
    started_at = args.started_at or input_value(input_data, "started_at")
    output_dir = args.output_dir or input_value(input_data, "output_dir")
    try:
        import cv2
    except ModuleNotFoundError:
        return emit(failed(SKILL, "OPENCV_MISSING", "opencv-python-headless is required"), args.output)
    path = Path(str(video).removeprefix("file://"))
    cap = cv2.VideoCapture(str(path))
    if not cap.isOpened():
        return emit(failed(SKILL, "VIDEO_OPEN_FAILED", f"Could not open video: {path}", True), args.output)
    video_fps = cap.get(cv2.CAP_PROP_FPS) or 25
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH) or 0)
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT) or 0)
    total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
    duration = capture_seconds if capture_seconds is not None else (total / video_fps if total else config.get("default_capture_seconds", 10))
    step = 1 / max(float(fps), 0.001) if fps else interval_seconds
    start = started_at or config.get("now") or "2026-05-20T10:00:00+08:00"
    out_dir = Path(output_dir or config.get("output_dir", "outputs")) / "frames" / camera_id
    
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        return emit(failed(SKILL, "OUTPUT_DIR_UNAVAILABLE", f"Could not create output directory: {out_dir}: {exc}", True), args.output)
    frames = []
    elapsed = 0.0
    seq = 1
    while elapsed <= float(duration) + 1e-9:
        cap.set(cv2.CAP_PROP_POS_MSEC, elapsed * 1000)
        ok, frame = cap.read()
        if not ok:
            break
        ts = iso_add_seconds(start, elapsed)
        frame_id = f"{camera_id}_{ts.replace('-', '').replace(':', '').replace('+', '').replace('T', '')}_{seq:04d}"
        image_path = out_dir / f"{frame_id}.jpg"
        cv2.imwrite(str(image_path), frame)
        frames.append({"frame_id": frame_id, "camera_id": camera_id, "timestamp": ts, "image_uri": str(image_path), "width": width, "height": height, "sequence": seq, "source_elapsed_seconds": round(elapsed, 3)})
        seq += 1
        elapsed += step
    cap.release()
    return emit(success(SKILL, {"camera_id": camera_id, "frames": frames, "source_video_uri": str(path), "fps": video_fps}), args.output)

if __name__ == "__main__":
    raise SystemExit(main())
