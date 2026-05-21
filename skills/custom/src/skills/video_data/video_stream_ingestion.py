from __future__ import annotations

import subprocess
from pathlib import Path
from uuid import uuid4

from src.skills.base import BaseSkill, SkillContext
from src.skills.utils import iso_add_seconds


class VideoStreamIngestionSkill(BaseSkill):
    name = "video-stream-ingestion"

    def run(self, input_data: dict, context: SkillContext) -> dict:
        camera_id = input_data.get("camera_id")
        source_type = input_data.get("source_type", "mock")
        if not camera_id:
            return self.failed("MISSING_CAMERA_ID", "camera_id is required")

        if source_type not in {"mock", "local_file", "rtsp", "gb28181", "api"}:
            return self.failed("UNSUPPORTED_SOURCE", f"Unsupported source_type: {source_type}")

        started_at = input_data.get("started_at") or context.config.get("now") or "2026-05-20T10:00:00+08:00"
        capture_seconds = int(input_data.get("capture_seconds", 10))
        session_id = input_data.get("video_session_id") or f"VS_{uuid4().hex[:8]}"
        raw_uri = input_data.get("raw_segment_uri")
        duration_seconds = capture_seconds
        width = height = None

        if source_type == "local_file":
            source_path = Path(input_data.get("stream_url") or input_data.get("file_path") or "")
            if not source_path.exists():
                return self.failed("SOURCE_NOT_FOUND", f"Local video not found: {source_path}")
            raw_uri = str(source_path)
            metadata = _probe_video(source_path)
            duration_seconds = min(capture_seconds, int(float(metadata.get("duration", capture_seconds))))
            width = metadata.get("width")
            height = metadata.get("height")

        if not raw_uri:
            raw_uri = context.storage.upload_bytes(
                f"{source_type}:{camera_id}:{session_id}".encode("utf-8"),
                f"raw/{camera_id}/{session_id}.mp4",
            )

        return self.success(
            {
                "camera_id": camera_id,
                "source_type": source_type,
                "stream_status": "ok",
                "video_session_id": session_id,
                "started_at": started_at,
                "ended_at": iso_add_seconds(started_at, duration_seconds),
                "raw_segment_uri": raw_uri,
                "duration_seconds": duration_seconds,
                "width": width,
                "height": height,
            }
        )


def _probe_video(path: Path) -> dict:
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=width,height:format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=0",
        str(path),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    metadata = {}
    for line in result.stdout.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        metadata[key] = value
    if "width" in metadata:
        metadata["width"] = int(metadata["width"])
    if "height" in metadata:
        metadata["height"] = int(metadata["height"])
    return metadata
