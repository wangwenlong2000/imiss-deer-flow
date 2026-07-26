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

SKILL = "privacy-masking"

def mask_image(uri: str, regions: list[dict[str, Any] | list[float]], method: str):
    try:
        import cv2
    except ModuleNotFoundError:
        return failed(SKILL, "OPENCV_MISSING", "opencv-python-headless is required")
    path = Path(uri.removeprefix("file://"))
    image = cv2.imread(str(path))
    if image is None:
        return failed(SKILL, "IMAGE_READ_FAILED", f"Could not read image: {path}")
    for region in regions:
        bbox = region.get("bbox", region) if isinstance(region, dict) else region
        x1, y1, x2, y2 = [max(0, int(v)) for v in bbox]
        roi = image[y1:y2, x1:x2]
        if roi.size:
            image[y1:y2, x1:x2] = cv2.GaussianBlur(roi, (31, 31), 0)
    out = path.with_name(f"{path.stem}_masked{path.suffix}")
    cv2.imwrite(str(out), image)
    return success(SKILL, {"uri": str(out), "privacy_masked": True, "masked_regions": regions, "method": method})

def valid_regions(regions: list[dict[str, Any] | list[float]]) -> tuple[bool, str]:
    if not regions:
        return False, "sensitive_regions with explicit bbox coordinates is required when privacy masking is enabled"
    for index, region in enumerate(regions):
        bbox = region.get("bbox", region) if isinstance(region, dict) else region
        if not isinstance(bbox, list) or len(bbox) != 4:
            return False, f"sensitive_regions[{index}] must provide bbox as [x1, y1, x2, y2]"
        try:
            x1, y1, x2, y2 = [float(v) for v in bbox]
        except (TypeError, ValueError):
            return False, f"sensitive_regions[{index}].bbox values must be numeric"
        if x2 <= x1 or y2 <= y1:
            return False, f"sensitive_regions[{index}].bbox must satisfy x2 > x1 and y2 > y1"
    return True, ""

def main() -> int:
    p = argparse.ArgumentParser(description="Mask sensitive regions in an evidence image.")
    p.add_argument("--input", help="Optional JSON payload; CLI flags override matching fields.")
    p.add_argument("--image-uri", "--uri", dest="uri")
    p.add_argument("--sensitive-regions-json")
    p.add_argument("--method")
    p.add_argument("--disabled", action="store_true")
    p.add_argument("--config")
    p.add_argument("--output")
    args = p.parse_args()
    input_data = load_input(args.input)
    config = load_config(args.config)
    uri = args.uri or input_value(input_data, "image_uri", "uri")
    if not uri:
        return emit(failed(SKILL, "MISSING_URI", "--image-uri or input.uri is required"), args.output)
    enabled = not args.disabled and not bool(input_value(input_data, "disabled", default=False)) and config.get("privacy_masking", {}).get("enabled", True)
    regions = load_list(args.sensitive_regions_json, "sensitive_regions") if args.sensitive_regions_json else input_list(input_data, "sensitive_regions")
    method = args.method or input_value(input_data, "method", default="gaussian_blur")
    if not enabled:
        return emit(success(SKILL, {"uri": uri, "privacy_masked": False, "masked_regions": []}), args.output)
    ok, message = valid_regions(regions)
    if not ok:
        return emit(failed(SKILL, "MISSING_SENSITIVE_REGIONS", message), args.output)
    path = Path(str(uri).removeprefix("file://"))
    if not path.exists():
        return emit(success(SKILL, {"uri": uri, "privacy_masked": True, "masked_regions": regions, "method": method}), args.output)
    return emit(mask_image(uri, regions, config.get("privacy_masking", {}).get("method", method)), args.output)

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
