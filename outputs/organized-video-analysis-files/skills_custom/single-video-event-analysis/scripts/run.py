#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

VERSION = "1.0.0"
SKILL = "single-video-event-analysis"


def load_json(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    source = Path(path)
    text = source.read_text(encoding="utf-8")
    return json.loads(text) if text.strip() else {}


def input_value(data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    nested = data.get("data") if isinstance(data.get("data"), dict) else {}
    for key in keys:
        if data.get(key) is not None:
            return data[key]
        if nested.get(key) is not None:
            return nested[key]
    return default


def success(data: dict[str, Any], confidence: float = 0.0) -> dict[str, Any]:
    return {
        "skill": SKILL,
        "version": VERSION,
        "status": "success",
        "confidence": round(float(confidence), 4),
        "data": data,
    }


def failed(code: str, message: str, retryable: bool = False, detail: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "skill": SKILL,
        "version": VERSION,
        "status": "failed",
        "error_code": code,
        "message": message,
        "retryable": retryable,
        "detail": detail or {},
    }


def emit(result: dict[str, Any], output: str | None) -> int:
    text = json.dumps(result, ensure_ascii=False, indent=2)
    if output:
        path = Path(output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0 if result.get("status") == "success" else 1


def frame_id_from_path(path: str) -> str:
    return Path(path).stem


def collect_frames(output_dir: Path, metadata: dict[str, Any]) -> list[dict[str, Any]]:
    frames: list[dict[str, Any]] = []
    for chapter in metadata.get("chapters", []):
        for item in chapter.get("frames", []):
            rel = item.get("path")
            if not rel:
                continue
            abs_path = output_dir / rel
            frames.append(
                {
                    "frame_id": frame_id_from_path(rel),
                    "image_uri": str(abs_path),
                    "timestamp_seconds": item.get("timestamp_seconds"),
                    "timestamp_display": item.get("timestamp_display"),
                    "pass": item.get("pass"),
                    "chapter": chapter.get("index"),
                }
            )
    frames.sort(key=lambda item: float(item.get("timestamp_seconds") or 0))
    return frames


def build_manifest(
    payload: dict[str, Any],
    review_config: dict[str, Any],
    output_dir: Path,
    metadata_path: Path,
    metadata: dict[str, Any],
    frames: list[dict[str, Any]],
) -> dict[str, Any]:
    target_events = input_value(payload, "target_events", default=review_config.get("default_target_events", []))
    if not isinstance(target_events, list):
        target_events = [str(target_events)]
    return {
        "schema": "single_video_event_review_manifest",
        "version": VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "camera_id": input_value(payload, "camera_id", default="CAM_DEERFLOW_001"),
        "video": metadata.get("video", {}),
        "target_events": target_events,
        "metadata_uri": str(metadata_path),
        "output_dir": str(output_dir),
        "frames": frames,
        "review_instructions": [
            "Inspect frames in chronological order.",
            "Use coarse frames to find candidate moments.",
            "Use dense frames around candidate moments before deciding event type.",
            "Base every event on visible evidence only.",
            "Mark requires_review=true for serious or ambiguous events.",
        ],
        "output_schema": {
            "visual_timeline": "list of timestamped observations",
            "events": "list of event candidates with event_type, time range, confidence, reason, evidence_frame_ids, requires_review",
            "summary": "short summary",
            "warnings": "quality or uncertainty notes",
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Prepare one monitoring video for LLM event review.")
    parser.add_argument("--input")
    parser.add_argument("--video")
    parser.add_argument("--camera-id")
    parser.add_argument("--output-dir")
    parser.add_argument("--coarse-fps", type=float)
    parser.add_argument("--dense-fps", type=float)
    parser.add_argument("--scene-threshold", type=float)
    parser.add_argument("--dense-window", type=float)
    parser.add_argument("--max-frames", type=int)
    parser.add_argument("--config")
    parser.add_argument("--output")
    args = parser.parse_args()

    payload = load_json(args.input)
    config = load_json(args.config)
    review_config = config.get("video_event_review", {}) if isinstance(config.get("video_event_review"), dict) else {}
    video = args.video or input_value(payload, "video_path", "file_path", "raw_segment_uri", "video")
    if not video:
        return emit(failed("MISSING_VIDEO", "video_path, file_path, raw_segment_uri, or --video is required"), args.output)

    video_path = Path(str(video).removeprefix("file://"))
    if not video_path.exists():
        return emit(failed("VIDEO_NOT_FOUND", f"Video not found: {video_path}"), args.output)

    output_dir = Path(args.output_dir or input_value(payload, "output_dir", default="outputs/single-video-event-analysis"))
    output_dir.mkdir(parents=True, exist_ok=True)

    extractor = Path(__file__).resolve().parents[2] / "analyze-video" / "scripts" / "extract_frames.py"
    if not extractor.exists():
        return emit(failed("EXTRACTOR_NOT_FOUND", f"Frame extractor not found: {extractor}"), args.output)

    cmd = [sys.executable, str(extractor), str(video_path), "--output-dir", str(output_dir)]
    for flag, value in (
        ("--coarse-fps", args.coarse_fps if args.coarse_fps is not None else input_value(payload, "coarse_fps", default=review_config.get("coarse_fps"))),
        ("--dense-fps", args.dense_fps if args.dense_fps is not None else input_value(payload, "dense_fps", default=review_config.get("dense_fps"))),
        ("--scene-threshold", args.scene_threshold if args.scene_threshold is not None else input_value(payload, "scene_threshold", default=review_config.get("scene_threshold"))),
        ("--dense-window", args.dense_window if args.dense_window is not None else input_value(payload, "dense_window", default=review_config.get("dense_window"))),
        ("--max-frames", args.max_frames if args.max_frames is not None else input_value(payload, "max_frames", default=review_config.get("max_frames"))),
    ):
        if value is not None:
            cmd.extend([flag, str(value)])

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return emit(
            failed(
                "FRAME_EXTRACTION_FAILED",
                "Frame extraction failed",
                True,
                {"stderr": result.stderr[-4000:], "stdout": result.stdout[-1000:]},
            ),
            args.output,
        )

    metadata_path = output_dir / "metadata.json"
    if not metadata_path.exists():
        return emit(failed("MISSING_METADATA", f"metadata.json was not created: {metadata_path}", True), args.output)

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    frames = collect_frames(output_dir, metadata)
    manifest = build_manifest(payload, review_config, output_dir, metadata_path, metadata, frames)
    manifest_path = output_dir / "review_manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    data = {
        "review_manifest_uri": str(manifest_path),
        "metadata_uri": str(metadata_path),
        "output_dir": str(output_dir),
        "frame_count": len(frames),
        "frames": frames,
        "requires_llm_visual_review": True,
        "next_step": "Inspect frames and produce visual_timeline plus event candidates from visible evidence.",
    }
    return emit(success(data, 0.0), args.output)


if __name__ == "__main__":
    raise SystemExit(main())
