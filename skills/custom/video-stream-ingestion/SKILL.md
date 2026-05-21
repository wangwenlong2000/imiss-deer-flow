---
name: video-stream-ingestion
description: Ingest video monitoring sources and return raw segment/session metadata. Use when Codex needs to connect or normalize RTSP, GB28181, platform API, local MP4, or mock video sources before frame sampling; especially when input includes camera_id, source_type, stream_url, file_path, capture_seconds, or raw_segment_uri. This skill exposes the production Python implementation and FFprobe-backed local-file probing, but must not run vision inference or event logic.
---

# Video Stream Ingestion

## Execution Priority

Prioritize calling this skill's `scripts/run.py` entrypoint first. Only write custom code after the script result is unsuitable or the script cannot cover the requested task.

## Input Resolution

If the user provides a video path, local file, stream URL, or uploaded video, build the input JSON for this skill instead of writing ingestion code. Use a stable default `camera_id` when the user does not provide one, infer `source_type=local_file` for file paths, and pass the path as `file_path`, `stream_url`, or `raw_segment_uri` as appropriate. Only ask the user for clarification when no usable video source can be inferred.

## Workflow

1. Read camera id, source type, stream URL or local file, and capture duration.
2. Select the source adapter for RTSP, GB28181, API, local file, or mock input.
3. Capture a short segment with timeout and retry-aware failure handling.
4. Store the raw segment and return session metadata.

## Available Implementation

- Registry name: `video-stream-ingestion`
- Python class: `src.skills.video_data.video_stream_ingestion.VideoStreamIngestionSkill`
- Pipeline caller: `src.orchestrator.pipeline.run_camera_pipeline`
- CLI entrypoint: `python video-stream-ingestion/scripts/run.py --input <input.json> --config <config.json> --output <result.json>`

## Standalone CLI

This Skill can be executed independently through the shared runner:

```bash
python video-stream-ingestion/scripts/run.py \
  --input <input.json> \
  --config <config.json> \
  --output <result.json>
```

This directory owns registry skill `video-stream-ingestion`, so the agent should call this script directly for this skill.

## Tool Invocation

- Local MP4 probing uses `ffprobe` through the Python implementation.
- Mock streams use `InMemoryStorage` from `src.skills.base`.
- For direct code use, call:

```python
registry.get("video-stream-ingestion").run(input_data, context)
```

## Inputs

Required:
- `camera_id`
- `source_type`: `local_file`, `mock`, `rtsp`, `gb28181`, or `api`

Common fields:
- `stream_url` or `file_path`
- `capture_seconds`
- `started_at`
- `raw_segment_uri`

## Outputs

Returns `raw_segment_uri`, `video_session_id`, `started_at`, `ended_at`, `duration_seconds`, and optional video width/height.

## Failure Modes

- `MISSING_CAMERA_ID`
- `UNSUPPORTED_SOURCE`
- `SOURCE_NOT_FOUND`

## Constraints

Do not detect objects, sample frames, create events, or generate evidence.
