from __future__ import annotations

from pathlib import Path

import cv2

from src.skills.base import BaseSkill, SkillContext
from src.skills.utils import iso_add_seconds


class FrameSamplingSkill(BaseSkill):
    name = "frame-sampling"

    def run(self, input_data: dict, context: SkillContext) -> dict:
        camera_id = input_data.get("camera_id")
        if not camera_id:
            return self.failed("MISSING_CAMERA_ID", "camera_id is required")

        provided = input_data.get("frames")
        if provided:
            return self.success({"camera_id": camera_id, "frames": provided})

        raw_segment_uri = input_data.get("raw_segment_uri", "")
        raw_path = Path(raw_segment_uri.removeprefix("file://"))
        if raw_path.exists() and input_data.get("source_type") == "local_file":
            return self._sample_local_video(raw_path, input_data, context)

        strategy = input_data.get("sampling_strategy", {})
        mode = strategy.get("mode", "interval")
        if mode == "fps":
            step = 1 / max(float(strategy.get("fps", 1)), 0.001)
        else:
            step = float(strategy.get("interval_seconds", 1))

        start = input_data.get("started_at") or context.config.get("now") or "2026-05-20T10:00:00+08:00"
        capture_seconds = int(input_data.get("capture_seconds") or context.config.get("default_capture_seconds", 10))
        width, height = input_data.get("resolution") or context.config.get("default_resolution", [1920, 1080])

        frames = []
        sequence = 1
        elapsed = 0.0
        while elapsed <= capture_seconds + 1e-9:
            timestamp = iso_add_seconds(start, elapsed)
            frame_id = f"{camera_id}_{timestamp.replace('-', '').replace(':', '').replace('+', '').replace('T', '')}_{sequence:04d}"
            image_uri = context.storage.upload_bytes(
                f"frame:{frame_id}".encode("utf-8"),
                f"frames/{camera_id}/{sequence:04d}.jpg",
            )
            frames.append(
                {
                    "frame_id": frame_id,
                    "camera_id": camera_id,
                    "timestamp": timestamp,
                    "image_uri": image_uri,
                    "width": width,
                    "height": height,
                    "sequence": sequence,
                }
            )
            sequence += 1
            elapsed += step

        return self.success({"camera_id": camera_id, "frames": frames})

    def _sample_local_video(self, path: Path, input_data: dict, context: SkillContext) -> dict:
        camera_id = input_data["camera_id"]
        strategy = input_data.get("sampling_strategy", {})
        mode = strategy.get("mode", "interval")
        start = input_data.get("started_at") or context.config.get("now") or "2026-05-20T10:00:00+08:00"
        capture_seconds = float(input_data.get("capture_seconds") or input_data.get("duration_seconds") or context.config.get("default_capture_seconds", 10))
        output_dir = Path(context.config.get("output_dir", "outputs")) / "frames" / camera_id
        output_dir.mkdir(parents=True, exist_ok=True)

        capture = cv2.VideoCapture(str(path))
        if not capture.isOpened():
            return self.failed("VIDEO_OPEN_FAILED", f"Could not open video: {path}", True)
        fps = capture.get(cv2.CAP_PROP_FPS) or 25
        width = int(capture.get(cv2.CAP_PROP_FRAME_WIDTH) or input_data.get("width") or 0)
        height = int(capture.get(cv2.CAP_PROP_FRAME_HEIGHT) or input_data.get("height") or 0)
        if mode == "fps":
            step_seconds = 1 / max(float(strategy.get("fps", 1)), 0.001)
        else:
            step_seconds = float(strategy.get("interval_seconds", 1))

        frames = []
        sequence = 1
        elapsed = 0.0
        while elapsed <= capture_seconds + 1e-9:
            capture.set(cv2.CAP_PROP_POS_MSEC, elapsed * 1000)
            ok, frame = capture.read()
            if not ok:
                break
            timestamp = iso_add_seconds(start, elapsed)
            frame_id = f"{camera_id}_{timestamp.replace('-', '').replace(':', '').replace('+', '').replace('T', '')}_{sequence:04d}"
            image_path = output_dir / f"{frame_id}.jpg"
            cv2.imwrite(str(image_path), frame)
            frames.append(
                {
                    "frame_id": frame_id,
                    "camera_id": camera_id,
                    "timestamp": timestamp,
                    "image_uri": str(image_path),
                    "width": width,
                    "height": height,
                    "sequence": sequence,
                    "source_elapsed_seconds": round(elapsed, 3),
                }
            )
            sequence += 1
            elapsed += step_seconds
        capture.release()
        return self.success({"camera_id": camera_id, "frames": frames, "source_video_uri": str(path), "fps": fps})
