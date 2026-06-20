---
name: video-stream-ingestion
description: Normalize an existing local video file after city-video-intelligence has routed a video-library or analysis request, returning raw segment/session metadata. For direct user ingestion, Elasticsearch, StreetModel, search, statistics, evidence, or camera-health requests, load city-video-intelligence first. Use this atomic skill when Codex already has a clear plan and a local MP4/MOV/AVI/MKV file before frame sampling.
---

# Video File Normalization

## Execution Priority

For any direct user business request that combines video ingestion with search, embedding, statistics, evidence, or other city-video operations, load `city-video-intelligence` first and run its router script. Use this atomic skill only after the business route is clear.

Prioritize calling this skill's `scripts/run.py` entrypoint first. Only write custom code after the script result is unsuitable or the script cannot cover the requested task.

## Input Resolution

If the user provides a local video path or uploaded video file, build the input JSON for this skill instead of writing normalization code. Use a stable default `camera_id` when the user does not provide one, infer `source_type=local_file`, and pass the path as `file_path`, `video_path`, or `raw_segment_uri`. Only ask the user for clarification when no usable video file can be inferred.


## Atomic CLI

Run this skill directly with its own script. The script does not call other skill scripts and does not depend on shared `src`, `tools`, or registry modules.

```bash
python video-stream-ingestion/scripts/run.py --video <video.mp4> --camera-id <camera_id> --source-type local_file --capture-seconds 10 --config <config.json> --output <result.json>
```

Parameters: `--video`, `--raw-segment-uri`, `--camera-id`, `--source-type`, `--capture-seconds`, `--started-at`, `--config`, `--output`.

## Workflow

1. Read camera id, local video file path, and optional capture duration.
2. Validate that the file exists and probe width, height, and duration with ffprobe.
3. Return normalized `raw_segment_uri` and session metadata for downstream frame sampling.

## Available Implementation

This skill is implemented as an atomic standalone script in its own `scripts/run.py`. The script contains the executable logic for this skill and must not import shared `src`, `tools`, or registry modules.

## Inputs

Required:
- `camera_id`
- `source_type`: `local_file`

Common fields:
- `file_path`, `video_path`, or `raw_segment_uri`
- `capture_seconds`
- `started_at`

## Outputs

Returns `raw_segment_uri`, `video_session_id`, `started_at`, `ended_at`, `duration_seconds`, video width/height when available, and `file_status`.

## Failure Modes

- `MISSING_CAMERA_ID`
- `UNSUPPORTED_SOURCE`
- `SOURCE_NOT_FOUND`

## Constraints

Do not detect objects, sample frames, create events, or generate evidence.
