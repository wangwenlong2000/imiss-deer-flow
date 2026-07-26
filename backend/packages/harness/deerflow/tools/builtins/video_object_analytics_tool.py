from __future__ import annotations

import json
import re
import shlex
from collections import Counter
from pathlib import Path
from typing import Literal
from uuid import uuid4

from langchain.tools import ToolRuntime, tool
from langgraph.typing import ContextT

from deerflow.agents.thread_state import ThreadState
from deerflow.sandbox.tools import (
    ensure_sandbox_initialized,
    ensure_thread_directories_exist,
    get_thread_data,
    is_local_sandbox,
    normalize_dataset_virtual_paths_in_command,
    replace_virtual_paths_in_command,
    validate_local_bash_command_paths,
)


# The GitHub branch keeps the video-surveillance bundle in one namespace.
# Keep these paths owned by the structured tool so the model never has to
# invent the bundle layout or underlying CLI arguments.
SKILLS_ROOT = "/mnt/skills/custom/video_surveillance"
CONFIG_PATH = f"{SKILLS_ROOT}/configs/deerflow_config.json"
MODEL_PATH = f"{SKILLS_ROOT}/models/yolov8n.pt"
DEFAULT_LABELS = ["person", "car", "bus", "truck", "motorcycle", "bicycle"]
CAMERA_ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,80}$")
VIDEO_ROOTS = ("/mnt/datasets/", "/mnt/user-data/uploads/", "/mnt/user-data/workspace/", "/data/deerflow/videos/")


def _quote_command(parts: list[str]) -> str:
    return shlex.join([str(part) for part in parts])


def _validate_video_path(video_path: str) -> str:
    if not isinstance(video_path, str) or not video_path.startswith("/"):
        raise ValueError("video_path must be an absolute path")
    if not any(video_path == root.rstrip("/") or video_path.startswith(root) for root in VIDEO_ROOTS):
        raise ValueError("video_path must be under /mnt/datasets, /mnt/user-data, or /data/deerflow/videos")
    return video_path


def _run_in_sandbox(runtime: ToolRuntime[ContextT, ThreadState], sandbox, command: str) -> str:
    thread_data = get_thread_data(runtime)
    if is_local_sandbox(runtime):
        command = normalize_dataset_virtual_paths_in_command(command)
        validate_local_bash_command_paths(command, thread_data)
        command = replace_virtual_paths_in_command(command, thread_data)
    return sandbox.execute_command(command)


def _summarize_detection(result: dict) -> dict:
    data = result.get("data") if isinstance(result.get("data"), dict) else {}
    detections = data.get("detections") if isinstance(data.get("detections"), list) else []
    per_frame = []
    aggregate = Counter()
    for detection in detections:
        objects = detection.get("objects") if isinstance(detection, dict) else []
        labels = Counter(obj.get("label") for obj in objects if isinstance(obj, dict) and obj.get("label"))
        aggregate.update(labels)
        per_frame.append(
            {
                "frame_id": detection.get("frame_id"),
                "timestamp": detection.get("timestamp"),
                "counts": dict(sorted(labels.items())),
                "object_count": sum(labels.values()),
            }
        )
    return {
        "frame_count": len(per_frame),
        "per_frame": per_frame,
        "frame_detection_totals": dict(sorted(aggregate.items())),
        "total_frame_detections": sum(aggregate.values()),
        "unique_object_count": None,
        "unique_object_count_note": "Detection frames are not deduplicated; use operation=track for cross-frame identities.",
        "model": data.get("model"),
    }


def _normalize_labels(labels: list[str] | str | None) -> list[str]:
    if labels is None:
        return list(DEFAULT_LABELS)
    if isinstance(labels, str):
        try:
            parsed = json.loads(labels)
        except json.JSONDecodeError:
            parsed = [part.strip() for part in labels.split(",") if part.strip()]
        labels = parsed
    if not isinstance(labels, list):
        raise ValueError("labels must be a list of label names")
    return labels


def _summarize_tracks(result: dict) -> dict:
    data = result.get("data") if isinstance(result.get("data"), dict) else {}
    tracks = data.get("tracks") if isinstance(data.get("tracks"), list) else []
    labels = Counter(track.get("label") for track in tracks if isinstance(track, dict) and track.get("label"))
    movement = Counter(track.get("movement_state") for track in tracks if isinstance(track, dict) and track.get("movement_state"))
    return {"track_count": len(tracks), "by_label": dict(sorted(labels.items())), "by_movement_state": dict(sorted(movement.items()))}


@tool("video_object_analytics", parse_docstring=True)
def video_object_analytics_tool(
    runtime: ToolRuntime[ContextT, ThreadState],
    video_path: str,
    operation: Literal["detect", "track"] = "detect",
    camera_id: str = "CAM_DEERFLOW_001",
    capture_seconds: float = 2.0,
    interval_seconds: float = 1.0,
    labels: list[str] | str | None = None,
) -> str:
    """Run deterministic video object detection or tracking.

    Use this tool for requests to count visible people, vehicles, or other
    objects, or to track those objects across frames. The tool owns the CLI
    parameters, model path, configuration path, and persistent workspace.
    Do not call bash or compose the underlying skill commands for this task.

    Args:
        video_path: Absolute path to the local video file.
        operation: Use "detect" for frame-level objects or "track" for cross-frame tracks.
        camera_id: Camera identifier used in frame and track records.
        capture_seconds: Number of seconds from the start of the video to process.
        interval_seconds: Sampling interval in seconds.
        labels: Optional COCO labels to keep, such as person, car, bus, and truck.
    """
    try:
        sandbox = ensure_sandbox_initialized(runtime)
        ensure_thread_directories_exist(runtime)
        video_path = _validate_video_path(video_path)
        if operation not in {"detect", "track"}:
            raise ValueError("operation must be detect or track")
        if not CAMERA_ID_RE.fullmatch(camera_id):
            raise ValueError("camera_id contains unsupported characters")
        if not 0 < float(capture_seconds) <= 3600:
            raise ValueError("capture_seconds must be between 0 and 3600")
        if not 0 < float(interval_seconds) <= 3600:
            raise ValueError("interval_seconds must be between 0 and 3600")
        selected_labels = _normalize_labels(labels)
        if not isinstance(selected_labels, list) or not selected_labels or any(
            not isinstance(label, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,40}", label) for label in selected_labels
        ):
            raise ValueError("labels must be a non-empty list of simple label names")

        run_id = uuid4().hex[:12]
        run_dir = f"/mnt/user-data/workspace/video-object-analytics/{run_id}"
        frames_dir = f"{run_dir}/frames"
        frames_json = f"{run_dir}/frames.json"
        detections_json = f"{run_dir}/detections.json"
        tracks_json = f"{run_dir}/tracks.json"
        log_path = f"{run_dir}/execution.log"

        frame_command = _quote_command(
            [
                "python",
                f"{SKILLS_ROOT}/frame-sampling/scripts/run.py",
                "--video",
                video_path,
                "--camera-id",
                camera_id,
                "--capture-seconds",
                str(float(capture_seconds)),
                "--interval-seconds",
                str(float(interval_seconds)),
                "--output-dir",
                frames_dir,
                "--config",
                CONFIG_PATH,
                "--output",
                frames_json,
            ]
        )
        detection_command = _quote_command(
            [
                "python",
                f"{SKILLS_ROOT}/object-detection/scripts/run.py",
                "--frames-json",
                frames_json,
                "--labels",
                ",".join(selected_labels),
                "--provider",
                "ultralytics",
                "--model-path",
                MODEL_PATH,
                "--config",
                CONFIG_PATH,
                "--output",
                detections_json,
            ]
        )
        commands = [
            f"mkdir -p {shlex.quote(run_dir)} && {frame_command} > {shlex.quote(run_dir + '/frames.log')} 2>&1",
            f"{detection_command} > {shlex.quote(run_dir + '/detections.log')} 2>&1",
        ]
        if operation == "track":
            tracking_command = _quote_command(
                [
                    "python",
                    f"{SKILLS_ROOT}/object-tracking/scripts/run.py",
                    "--detections-json",
                    detections_json,
                    "--camera-id",
                    camera_id,
                    "--config",
                    CONFIG_PATH,
                    "--output",
                    tracks_json,
                ]
            )
            commands.append(f"{tracking_command} > {shlex.quote(run_dir + '/tracks.log')} 2>&1")

        # Keep execution and result retrieval in one sandbox request. AIO
        # sandbox implementations may rotate the backing container between
        # API calls, while files inside a single shell invocation are stable.
        result_code = (
            "import json; "
            f"result={{'detection': json.load(open({detections_json!r}))}}; "
            + (f"result['tracking'] = json.load(open({tracks_json!r})); " if operation == "track" else "")
            + "print(json.dumps(result, ensure_ascii=False, separators=(',', ':')));"
        )
        commands.append(_quote_command(["python", "-c", result_code]))
        # Keep this a single POSIX command chain. The AIO shell endpoint has
        # returned opaque ErrorObservation objects for some multiline shell
        # payloads, while the same commands are reliable with `&&` chaining.
        command = " && ".join(commands)
        execution_output = _run_in_sandbox(runtime, sandbox, command)
        if execution_output.startswith("Error:"):
            raise RuntimeError(execution_output)

        output_lines = [line.strip() for line in execution_output.splitlines() if line.strip() and line.strip() != "(no output)"]
        if not output_lines:
            raise RuntimeError("video analytics command returned no result")
        try:
            command_result = json.loads(output_lines[-1])
            detection_result = command_result["detection"]
        except (json.JSONDecodeError, KeyError, TypeError) as exc:
            raise RuntimeError(f"video analytics command returned invalid result: {output_lines[-1][:500]}") from exc
        if detection_result.get("status") != "success":
            return json.dumps(
                {
                    "skill": "video-object-analytics",
                    "status": "failed",
                    "executed_skills": ["video-object-analytics", "frame-sampling", "object-detection"],
                    "detail": detection_result,
                },
                ensure_ascii=False,
            )
        result: dict = {
            "skill": "video-object-analytics",
            "status": "success",
            "operation": operation,
            "executed_skills": ["video-object-analytics", "frame-sampling", "object-detection"],
            "model": MODEL_PATH,
            "artifacts": {"frames_json": frames_json, "detections_json": detections_json},
            "summary": _summarize_detection(detection_result),
        }
        if operation == "track":
            tracking_result = command_result.get("tracking")
            if not isinstance(tracking_result, dict):
                raise RuntimeError("video analytics command did not return tracking result")
            if tracking_result.get("status") != "success":
                result["status"] = "failed"
                result["executed_skills"].append("object-tracking")
                result["detail"] = tracking_result
                return json.dumps(result, ensure_ascii=False)
            result["executed_skills"].append("object-tracking")
            result["artifacts"]["tracks_json"] = tracks_json
            result["track_summary"] = _summarize_tracks(tracking_result)
        return json.dumps(result, ensure_ascii=False)
    except Exception as exc:
        return json.dumps(
            {
                "skill": "video-object-analytics",
                "status": "failed",
                "error_code": type(exc).__name__,
                "message": str(exc),
            },
            ensure_ascii=False,
        )
