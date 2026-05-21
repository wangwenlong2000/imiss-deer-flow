from __future__ import annotations

import subprocess
from pathlib import Path

from src.skills.base import BaseSkill, SkillContext
from src.skills.utils import iso_add_seconds


class VideoSegmentExtractionSkill(BaseSkill):
    name = "video-segment-extraction"

    def run(self, input_data: dict, context: SkillContext) -> dict:
        event_id = input_data.get("event_id")
        event_time = input_data.get("event_time") or input_data.get("start_time")
        if not event_id or not event_time:
            return self.failed("MISSING_EVENT", "event_id and event_time/start_time are required")

        pre = int(input_data.get("pre_seconds", 10))
        post = int(input_data.get("post_seconds", 10))
        source_uri = input_data.get("raw_segment_uri", "")
        source_path = Path(source_uri.removeprefix("file://"))
        output_dir = Path(context.config.get("output_dir", "outputs")) / "evidence"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{event_id}_clip.mp4"
        actual_pre = pre
        if source_path.exists():
            event_elapsed = float(input_data.get("event_elapsed_seconds", 0))
            start_offset = max(0.0, event_elapsed - pre)
            actual_pre = event_elapsed - start_offset
            duration = pre + post
            cmd = [
                "ffmpeg",
                "-y",
                "-ss",
                str(start_offset),
                "-i",
                str(source_path),
                "-t",
                str(duration),
                "-c:v",
                "libx264",
                "-c:a",
                "aac",
                "-movflags",
                "+faststart",
                str(output_path),
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                return self.failed("FFMPEG_CLIP_FAILED", "FFmpeg failed to extract evidence clip", True, {"stderr": result.stderr[-2000:]})
            uri = str(output_path)
        else:
            uri = context.storage.upload_bytes(
                f"clip:{event_id}:{source_uri}".encode("utf-8"),
                f"evidence/{event_id}_clip.mp4",
            )
        return self.success(
            {
                "event_id": event_id,
                "clip_uri": uri,
                "start_time": iso_add_seconds(event_time, -actual_pre),
                "end_time": iso_add_seconds(event_time, post),
            }
        )
