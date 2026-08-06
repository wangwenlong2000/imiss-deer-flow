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

SKILL = "video-segment-extraction"

def main() -> int:
    p = argparse.ArgumentParser(description="Extract an MP4 clip around an event time.")
    p.add_argument("--input", help="Optional JSON payload; CLI flags override matching fields.")
    p.add_argument("--event-id")
    p.add_argument("--raw-segment-uri")
    p.add_argument("--event-time")
    p.add_argument("--start-time")
    p.add_argument("--event-elapsed-seconds", type=float)
    p.add_argument("--pre-seconds", type=int)
    p.add_argument("--post-seconds", type=int)
    p.add_argument("--output-dir")
    p.add_argument("--config")
    p.add_argument("--output")
    args = p.parse_args()
    input_data = load_input(args.input)
    config = load_config(args.config)
    event = input_dict(input_data, "event")
    event_id = args.event_id or input_value(input_data, "event_id") or event.get("event_id")
    raw_segment_uri = args.raw_segment_uri or input_value(input_data, "raw_segment_uri") or event.get("raw_segment_uri")
    event_time = args.event_time or args.start_time or input_value(input_data, "event_time", "start_time") or event.get("event_time") or event.get("start_time")
    if not event_time:
        return emit(failed(SKILL, "MISSING_EVENT", "--event-time or --start-time is required"), args.output)
    if not event_id:
        return emit(failed(SKILL, "MISSING_EVENT_ID", "--event-id or input.event_id is required"), args.output)
    if not raw_segment_uri:
        return emit(failed(SKILL, "MISSING_RAW_SEGMENT", "--raw-segment-uri or input.raw_segment_uri is required"), args.output)
    event_elapsed = args.event_elapsed_seconds if args.event_elapsed_seconds is not None else float(input_value(input_data, "event_elapsed_seconds", default=event.get("event_elapsed_seconds", 0.0)))
    pre_seconds = args.pre_seconds if args.pre_seconds is not None else int(input_value(input_data, "pre_seconds", default=10))
    post_seconds = args.post_seconds if args.post_seconds is not None else int(input_value(input_data, "post_seconds", default=10))
    source = Path(str(raw_segment_uri).removeprefix("file://"))
    out_dir = Path(args.output_dir or input_value(input_data, "output_dir") or config.get("output_dir", "outputs")) / "evidence"
    
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        return emit(failed(SKILL, "OUTPUT_DIR_UNAVAILABLE", f"Could not create output directory: {out_dir}: {exc}", True), args.output)
    out = out_dir / f"{event_id}_clip.mp4"
    actual_pre = pre_seconds
    if source.exists():
        if not shutil.which("ffmpeg"):
            return emit(failed(SKILL, "FFMPEG_MISSING", "ffmpeg is required but was not found in PATH", False), args.output)
        start_offset = max(0.0, event_elapsed - pre_seconds)
        actual_pre = event_elapsed - start_offset
        cmd = ["ffmpeg", "-y", "-ss", str(start_offset), "-i", str(source), "-t", str(pre_seconds + post_seconds), "-c:v", "libx264", "-c:a", "aac", "-movflags", "+faststart", str(out)]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            return emit(failed(SKILL, "FFMPEG_CLIP_FAILED", "FFmpeg failed to extract evidence clip", True, {"stderr": result.stderr[-2000:]}), args.output)
        uri = str(out)
    else:
        out.write_text(f"clip:{event_id}:{raw_segment_uri}\n", encoding="utf-8")
        uri = str(out)
    data = {"event_id": event_id, "clip_uri": uri, "start_time": iso_add_seconds(event_time, -actual_pre), "end_time": iso_add_seconds(event_time, post_seconds)}
    return emit(success(SKILL, data), args.output)

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
