#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import shutil
import subprocess
import sys
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

class SkillInputError(Exception):
    """Input file could not be loaded; reported through the standard failure contract."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def argv_output() -> str | None:
    """Best-effort --output path so early input failures still honour it."""
    argv = sys.argv
    if "--output" in argv:
        index = argv.index("--output")
        if index + 1 < len(argv):
            return argv[index + 1]
    return None


def load_structured(path: str | None) -> Any:
    if not path:
        return {}
    source = Path(path)
    if not source.exists():
        raise SkillInputError("INPUT_NOT_FOUND", f"Input file not found: {path}")
    if source.is_dir():
        raise SkillInputError("INPUT_NOT_FOUND", f"Input path is a directory, not a file: {path}")
    try:
        text = source.read_text(encoding="utf-8")
    except OSError as exc:
        raise SkillInputError("INPUT_UNREADABLE", f"Could not read input file {path}: {exc}") from exc
    if source.suffix.lower() in {".yaml", ".yml"}:
        if yaml is None:
            raise SkillInputError("INPUT_INVALID_YAML", "PyYAML is required to read YAML files")
        try:
            return yaml.safe_load(text) or {}
        except Exception as exc:  # noqa: BLE001 - yaml raises library specific errors
            raise SkillInputError("INPUT_INVALID_YAML", f"Invalid YAML in {path}: {exc}") from exc
    if not text.strip():
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise SkillInputError("INPUT_INVALID_JSON", f"Invalid JSON in {path}: {exc}") from exc

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
    # Compatibility aliases for callers that map input field names directly
    # to CLI flags. The structured video_object_analytics tool does not use
    # these aliases, but standalone skill calls remain safe and deterministic.
    p.add_argument("--video-path", dest="video_path_alias")
    p.add_argument("--frames-json")
    p.add_argument("--camera-id")
    p.add_argument("--capture-seconds", type=float)
    p.add_argument("--start-time", type=float)
    p.add_argument("--end-time", type=float)
    p.add_argument("--interval-seconds", type=float)
    p.add_argument("--fps", type=float)
    p.add_argument("--every-frame", action="store_true", help="Decode every native video frame in the requested half-open time range")
    p.add_argument("--started-at")
    p.add_argument("--output-dir")
    p.add_argument("--config")
    p.add_argument("--output")
    p.add_argument("--output-json", dest="output_json_alias")
    args = p.parse_args()
    output_path = args.output or args.output_json_alias
    input_data = load_input(args.input)
    config = load_config(args.config)
    camera_id = args.camera_id or input_value(input_data, "camera_id")
    frames = load_list(args.frames_json, "frames") if args.frames_json else input_list(input_data, "frames")
    if frames:
        return emit(success(SKILL, {"camera_id": camera_id, "frames": frames}), output_path)
    video = args.video or args.video_path_alias or input_value(input_data, "video", "video_path", "raw_segment_uri", "file_path")
    if not video:
        return emit(failed(SKILL, "MISSING_VIDEO", "--video or --frames-json is required"), output_path)
    sampling = input_dict(input_data, "sampling_strategy")
    interval_seconds = args.interval_seconds if args.interval_seconds is not None else float(sampling.get("interval_seconds", 1.0))
    fps = args.fps if args.fps is not None else sampling.get("fps")
    capture_seconds = args.capture_seconds if args.capture_seconds is not None else input_value(input_data, "capture_seconds")
    start_offset = args.start_time if args.start_time is not None else float(input_value(input_data, "start_time", default=0.0) or 0.0)
    end_offset = args.end_time if args.end_time is not None else input_value(input_data, "end_time")
    if end_offset is not None:
        end_offset = float(end_offset)
        if end_offset <= start_offset:
            return emit(failed(SKILL, "INVALID_TIME_RANGE", "end-time must be greater than start-time"), output_path)
        capture_seconds = end_offset - start_offset
    started_at = args.started_at or input_value(input_data, "started_at")
    output_dir = args.output_dir or input_value(input_data, "output_dir")
    try:
        import cv2
    except ModuleNotFoundError:
        return emit(failed(SKILL, "OPENCV_MISSING", "opencv-python-headless is required"), output_path)
    path = Path(str(video).removeprefix("file://"))
    cap = cv2.VideoCapture(str(path))
    if not cap.isOpened():
        return emit(failed(SKILL, "VIDEO_OPEN_FAILED", f"Could not open video: {path}", True), output_path)
    video_fps = cap.get(cv2.CAP_PROP_FPS)
    if not video_fps or not math.isfinite(video_fps) or video_fps <= 0:
        cap.release()
        return emit(failed(SKILL, "INVALID_VIDEO_FPS", "Video FPS is unknown; cannot assign frame times"), output_path)
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH) or 0)
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT) or 0)
    total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
    duration = capture_seconds if capture_seconds is not None else (total / video_fps if total else config.get("default_capture_seconds", 10))
    end_elapsed = float(start_offset) + float(duration)
    every_frame = args.every_frame or sampling.get("mode") == "every_frame"
    step = 1 / float(fps) if fps else float(interval_seconds)
    if not all(math.isfinite(value) for value in [float(start_offset), float(duration), step]) or start_offset < 0 or duration <= 0 or step <= 0:
        cap.release()
        return emit(failed(SKILL, "INVALID_TIME_RANGE", "Offsets, duration and sampling interval must be finite and positive (start >= 0)"), output_path)
    start = started_at
    if start:
        try:
            parse_time(start)
        except (ValueError, TypeError):
            cap.release()
            return emit(failed(SKILL, "INVALID_STARTED_AT", "started_at must be an ISO 8601 timestamp"), output_path)
    source_key = camera_id or "local_" + sha1(str(path.resolve()).encode()).hexdigest()[:10]
    out_dir = Path(output_dir or config.get("output_dir", "outputs")) / "frames" / source_key
    
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        return emit(failed(SKILL, "OUTPUT_DIR_UNAVAILABLE", f"Could not create output directory: {out_dir}: {exc}", True), output_path)
    # Some AVI streams expose a non-zero first decoded PTS (e.g. 80 ms).
    # Video-relative time starts at the first decoded frame, not that raw PTS.
    first_ok, _ = cap.read()
    pts_origin = cap.get(cv2.CAP_PROP_POS_MSEC) / 1000 if first_ok else 0.0
    cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
    frames = []
    elapsed = float(start_offset)
    seq = 1
    if every_frame:
        cap.set(cv2.CAP_PROP_POS_FRAMES, math.ceil(start_offset * video_fps - 1e-9))
    while elapsed < end_elapsed - 1e-9:
        if not every_frame:
            cap.set(cv2.CAP_PROP_POS_MSEC, elapsed * 1000)
        ok, frame = cap.read()
        if not ok:
            break
        frame_index = max(0, int(cap.get(cv2.CAP_PROP_POS_FRAMES)) - 1)
        raw_pts = cap.get(cv2.CAP_PROP_POS_MSEC) / 1000
        actual_elapsed = max(0.0, raw_pts - pts_origin)
        if not raw_pts and frame_index:
            actual_elapsed = frame_index / video_fps
        if actual_elapsed >= end_elapsed - 1e-9:
            break
        ts = iso_add_seconds(start, actual_elapsed) if start else None
        milliseconds = round(actual_elapsed * 1000)
        hours, remainder = divmod(milliseconds, 3600000)
        minutes, remainder = divmod(remainder, 60000)
        seconds, millis = divmod(remainder, 1000)
        display = f"{hours:02d}:{minutes:02d}:{seconds:02d}.{millis:03d}"
        frame_id = f"{source_key}_{milliseconds:012d}ms_{seq:06d}"
        image_path = out_dir / f"{frame_id}.jpg"
        if not cv2.imwrite(str(image_path), frame):
            cap.release()
            return emit(failed(SKILL, "FRAME_WRITE_FAILED", f"Could not save frame: {image_path}"), output_path)
        frames.append({"frame_id": frame_id, "camera_id": camera_id, "timestamp": ts,
                       "time_reference": "absolute" if start else "relative", "timestamp_display": display,
                       "image_uri": str(image_path), "width": width, "height": height,
                       "sequence": seq, "source_frame_index": frame_index, "source_pts_seconds": round(raw_pts, 6),
                       "source_elapsed_seconds": round(actual_elapsed, 6)})
        seq += 1
        elapsed = (frame_index + 1) / video_fps if every_frame else start_offset + (seq - 1) * step
    cap.release()
    return emit(success(SKILL, {"camera_id": camera_id, "frames": frames, "source_video_uri": str(path),
                "fps": video_fps, "sampling_mode": "every_frame" if every_frame else "interval",
                "time_reference": "absolute" if start else "relative", "started_at": start,
                "pts_origin_seconds": pts_origin,
                "requested_start_seconds": start_offset, "requested_end_seconds": end_elapsed}), output_path)

if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SkillInputError as exc:
        raise SystemExit(
            emit(
                {
                    "skill": SKILL,
                    "version": VERSION,
                    "status": "failed",
                    "error_code": exc.code,
                    "message": exc.message,
                    "retryable": False,
                    "detail": {},
                },
                argv_output(),
            )
        )
