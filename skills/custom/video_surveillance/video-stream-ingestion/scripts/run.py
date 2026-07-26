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

SKILL = "video-stream-ingestion"

def parse_frame_rate(value: str | None) -> float | None:
    """ffprobe returns rates as "12/1"; normalize to float fps."""
    if not value or value in {"0/0", "N/A"}:
        return None
    try:
        if "/" in value:
            numerator, denominator = value.split("/", 1)
            denominator_value = float(denominator)
            if denominator_value == 0:
                return None
            return round(float(numerator) / denominator_value, 3)
        return round(float(value), 3)
    except ValueError:
        return None


def probe_video(path: Path) -> dict[str, Any]:
    if not shutil.which("ffprobe"):
        raise FileNotFoundError("ffprobe")
    cmd = [
        "ffprobe", "-v", "error", "-select_streams", "v:0",
        "-show_entries",
        "stream=width,height,r_frame_rate,avg_frame_rate,codec_name,codec_long_name,pix_fmt,nb_frames"
        ":format=duration,size,bit_rate,format_name",
        "-of", "default=noprint_wrappers=1:nokey=0", str(path),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    metadata: dict[str, Any] = {}
    if result.returncode == 0:
        for line in result.stdout.splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                metadata[k] = v
    for key in ("width", "height", "nb_frames", "size", "bit_rate"):
        if key in metadata and metadata[key] not in {"N/A", ""}:
            try:
                metadata[key] = int(float(metadata[key]))
            except ValueError:
                metadata.pop(key, None)
    return metadata

def main() -> int:
    p = argparse.ArgumentParser(description="Normalize a local video file into raw segment metadata.")
    p.add_argument("--input", help="Optional JSON payload; CLI flags override matching fields.")
    p.add_argument("--video", "--file-path", dest="file_path")
    p.add_argument("--raw-segment-uri")
    p.add_argument("--camera-id")
    p.add_argument("--source-type", choices=["local_file"])
    p.add_argument("--capture-seconds", type=int)
    p.add_argument("--started-at")
    p.add_argument("--config")
    p.add_argument("--output")
    args = p.parse_args()
    input_data = load_input(args.input)
    config = load_config(args.config)
    camera_id = args.camera_id or input_value(input_data, "camera_id", default="CAM_DEERFLOW_001")
    file_path = args.file_path or input_value(input_data, "video", "video_path", "file_path")
    raw_uri = args.raw_segment_uri or input_value(input_data, "raw_segment_uri")
    source_type = args.source_type or input_value(input_data, "source_type") or "local_file"
    if source_type != "local_file":
        return emit(failed(SKILL, "UNSUPPORTED_SOURCE", f"Only local_file video input is supported: {source_type}"), args.output)
    duration = args.capture_seconds if args.capture_seconds is not None else int(input_value(input_data, "capture_seconds", default=10))
    started_at = args.started_at or input_value(input_data, "started_at") or config.get("now") or "2026-05-20T10:00:00+08:00"
    session_id = f"VS_{uuid4().hex[:8]}"
    width = height = None
    path = Path(file_path or raw_uri or "")
    if not path.exists():
        return emit(failed(SKILL, "SOURCE_NOT_FOUND", f"Local video not found: {path}"), args.output)
    raw_uri = str(path)
    try:
        meta = probe_video(path)
    except FileNotFoundError:
        return emit(failed(SKILL, "FFPROBE_MISSING", "ffprobe is required but was not found in PATH", False), args.output)
    video_duration = round(float(meta["duration"]), 3) if meta.get("duration") else None
    if video_duration:
        duration = min(duration, int(video_duration))
    width, height = meta.get("width"), meta.get("height")
    if not raw_uri:
        return emit(failed(SKILL, "SOURCE_NOT_FOUND", "A local video path is required"), args.output)
    fps = parse_frame_rate(meta.get("avg_frame_rate")) or parse_frame_rate(meta.get("r_frame_rate"))
    data = {
        "camera_id": camera_id,
        "source_type": source_type,
        "file_status": "ok",
        "video_session_id": session_id,
        "started_at": started_at,
        "ended_at": iso_add_seconds(started_at, duration),
        "raw_segment_uri": raw_uri,
        # duration_seconds 是本次接入的采集窗口，video_duration_seconds 是文件的完整时长
        "duration_seconds": duration,
        "video_duration_seconds": video_duration,
        "width": width,
        "height": height,
        "resolution": f"{width}x{height}" if width and height else None,
        "fps": fps,
        "codec": meta.get("codec_name"),
        "codec_long_name": meta.get("codec_long_name"),
        "pixel_format": meta.get("pix_fmt"),
        "container_format": meta.get("format_name"),
        "bit_rate": meta.get("bit_rate"),
        "frame_count": meta.get("nb_frames"),
        "file_size_bytes": meta.get("size") or (path.stat().st_size if path.exists() else None),
        "filename": path.name,
    }
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
