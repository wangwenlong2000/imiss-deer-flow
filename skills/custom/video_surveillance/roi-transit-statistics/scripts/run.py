#!/usr/bin/env python3
"""Count ROI entries, exits, and dwell time from tracks.

This skill is a geometric + temporal aggregator. It answers "how many objects
entered / left / stayed in this area", and nothing else. It must not label the
result as an event (congestion, intrusion, loitering...). Event semantics stay
in `single-video-event-analysis`.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    import yaml
except Exception:
    yaml = None

VERSION = "1.0.0"
SKILL = "roi-transit-statistics"
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "configs" / "deerflow_config.json"


class SkillInputError(Exception):
    """Input file could not be loaded; reported through the standard failure contract."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def argv_output() -> str | None:
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
        value = data.get(key)
        if value is None and isinstance(data.get("data"), dict):
            value = data["data"].get(key)
        return value if isinstance(value, list) else []
    return []


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


def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


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


def resolve_rois(args, input_data: dict[str, Any], config: dict[str, Any], camera_id: str) -> list[dict[str, Any]]:
    """ROIs may come from --rois-json, the input payload, or the camera config."""
    if args.rois_json:
        rois = load_list(args.rois_json, "rois")
        if rois:
            return rois
    rois = input_list(input_data, "rois")
    if rois:
        return rois
    camera = (config.get("cameras") or {}).get(camera_id) or {}
    rois = camera.get("rois")
    return rois if isinstance(rois, list) else []


def track_samples(track: dict[str, Any]) -> list[tuple[float, tuple[float, float]]]:
    """Return [(elapsed_seconds, (x, y))] for a track's trajectory.

    `object-tracking` emits `trajectory` as a bare [[x, y], ...] list with no
    per-point timestamps, so elapsed time is interpolated evenly between
    start_time and end_time. Dwell numbers are therefore an approximation whose
    resolution is the frame-sampling interval; this is stated in the output as
    `dwell_seconds_is_approximate`.
    """
    trajectory = track.get("trajectory")
    if not isinstance(trajectory, list) or not trajectory:
        bbox = track.get("last_bbox")
        if isinstance(bbox, list) and len(bbox) == 4:
            x1, y1, x2, y2 = [float(v) for v in bbox]
            return [(0.0, ((x1 + x2) / 2, y2))]
        return []
    points: list[tuple[float, float]] = []
    for item in trajectory:
        if isinstance(item, list) and len(item) >= 2:
            try:
                points.append((float(item[0]), float(item[1])))
            except (TypeError, ValueError):
                continue
        elif isinstance(item, dict) and item.get("x") is not None and item.get("y") is not None:
            try:
                points.append((float(item["x"]), float(item["y"])))
            except (TypeError, ValueError):
                continue
    if not points:
        return []
    start = parse_time(track.get("start_time"))
    end = parse_time(track.get("end_time"))
    duration = float(track.get("duration_seconds") or 0)
    if start and end:
        duration = max(duration, (end - start).total_seconds())
    if len(points) == 1:
        return [(0.0, points[0])]
    step = duration / (len(points) - 1) if duration > 0 else 0.0
    return [(index * step, point) for index, point in enumerate(points)]


def analyse_track_in_roi(track: dict[str, Any], polygon: list[list[float]], dwell_threshold: float) -> dict[str, Any] | None:
    samples = track_samples(track)
    if not samples:
        return None
    flags = [(elapsed, point_in_polygon(point, polygon)) for elapsed, point in samples]
    entered = 0
    left = 0
    inside_total = 0.0
    longest_inside = 0.0
    current_inside = 0.0
    first_enter: float | None = None
    last_leave: float | None = None
    previous_inside = flags[0][1]
    if previous_inside:
        first_enter = flags[0][0]
    for index in range(1, len(flags)):
        elapsed, inside = flags[index]
        span = elapsed - flags[index - 1][0]
        if previous_inside:
            inside_total += span
            current_inside += span
        if inside and not previous_inside:
            entered += 1
            if first_enter is None:
                first_enter = elapsed
            current_inside = 0.0
        elif not inside and previous_inside:
            left += 1
            last_leave = elapsed
            longest_inside = max(longest_inside, current_inside)
            current_inside = 0.0
        previous_inside = inside
    longest_inside = max(longest_inside, current_inside)
    if not entered and not left and not inside_total and not flags[0][1]:
        return None
    start = parse_time(track.get("start_time"))

    def absolute(offset: float | None) -> str | None:
        if offset is None or start is None:
            return None
        return (start + timedelta(seconds=offset)).isoformat()

    return {
        "track_id": track.get("track_id") or track.get("object_id") or "unknown",
        "label": track.get("label"),
        "entered": entered,
        "left": left,
        "dwell_seconds": round(inside_total, 2),
        "longest_continuous_dwell_seconds": round(longest_inside, 2),
        "dwelled": longest_inside >= dwell_threshold,
        "first_enter_time": absolute(first_enter),
        "last_leave_time": absolute(last_leave),
        "started_inside": flags[0][1],
        "ended_inside": flags[-1][1],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Count ROI entries, exits, and dwell time from tracks.")
    parser.add_argument("--input", help="Optional JSON payload; CLI flags override matching fields.")
    parser.add_argument("--tracks-json", help="Tracks JSON produced by object-tracking.")
    parser.add_argument("--rois-json", help="ROI polygons JSON. Falls back to input.rois, then config cameras.<id>.rois.")
    parser.add_argument("--camera-id")
    parser.add_argument("--dwell-seconds", type=float, help="Continuous seconds inside an ROI required to count as dwelling (default 3).")
    parser.add_argument("--config")
    parser.add_argument("--output")
    args = parser.parse_args()

    input_data = load_input(args.input)
    config_path = args.config or (str(DEFAULT_CONFIG_PATH) if DEFAULT_CONFIG_PATH.is_file() else None)
    config = load_config(config_path)
    camera_id = args.camera_id or input_value(input_data, "camera_id", default="CAM_DEERFLOW_001")

    tracks = load_list(args.tracks_json, "tracks") if args.tracks_json else input_list(input_data, "tracks")
    if not tracks:
        return emit(
            failed(SKILL, "MISSING_TRACKS", "--tracks-json or input.tracks is required; run object-tracking first"),
            args.output,
        )

    rois = resolve_rois(args, input_data, config, camera_id)
    if not rois:
        return emit(
            failed(
                SKILL,
                "MISSING_ROIS",
                f"No ROI polygons for camera {camera_id}; provide --rois-json, input.rois, or config cameras.{camera_id}.rois",
            ),
            args.output,
        )

    dwell_threshold = args.dwell_seconds
    if dwell_threshold is None:
        dwell_threshold = float(input_value(input_data, "dwell_seconds", default=0) or 0) or 3.0

    roi_results: list[dict[str, Any]] = []
    for index, roi in enumerate(rois):
        polygon = roi.get("polygon") if isinstance(roi, dict) else None
        if not isinstance(polygon, list) or len(polygon) < 3:
            return emit(
                failed(SKILL, "INVALID_ROI_POLYGON", f"rois[{index}].polygon must be a list of at least 3 [x, y] points"),
                args.output,
            )
        per_track = []
        for track in tracks:
            if not isinstance(track, dict):
                continue
            analysed = analyse_track_in_roi(track, polygon, dwell_threshold)
            if analysed:
                per_track.append(analysed)
        label_counts = Counter(item["label"] for item in per_track if item.get("label"))
        roi_results.append(
            {
                "roi_id": roi.get("id") or f"ROI_{index + 1:03d}",
                "roi_name": roi.get("name") or roi.get("id") or f"ROI_{index + 1:03d}",
                "roi_type": roi.get("type"),
                "entered": sum(item["entered"] for item in per_track),
                "left": sum(item["left"] for item in per_track),
                "dwelled": sum(1 for item in per_track if item["dwelled"]),
                "unique_objects": len({item["track_id"] for item in per_track}),
                "by_label": dict(label_counts),
                "tracks": per_track,
            }
        )

    data = {
        "camera_id": camera_id,
        "dwell_threshold_seconds": dwell_threshold,
        "tracks_analyzed": len(tracks),
        "rois": roi_results,
        "totals": {
            "entered": sum(item["entered"] for item in roi_results),
            "left": sum(item["left"] for item in roi_results),
            "dwelled": sum(item["dwelled"] for item in roi_results),
        },
        # 轨迹点没有逐点时间戳，停留时长按 start_time..end_time 均匀插值得到，
        # 精度受抽帧间隔限制。下游不得把它当作精确计时结果。
        "dwell_seconds_is_approximate": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
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
