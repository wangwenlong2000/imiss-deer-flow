---
name: frame-sampling
description: Sample structured frames from video monitoring segments or streams. Use when Codex has raw_segment_uri, camera_id, source_type=local_file, sampling_strategy, capture_seconds, or provided frame records and needs stable Frame schema outputs. This skill exposes OpenCV-backed real video frame extraction and mock frame generation for tests.
---

# Frame Sampling

## Execution Priority

Prioritize calling this skill's `scripts/run.py` entrypoint first. Only write custom code after the script result is unsuitable or the script cannot cover the requested task.

## Input Resolution

If the user provides a video file instead of `frames`, do not ask the user to provide frames and do not write custom OpenCV extraction code. First call `video-stream-ingestion/scripts/run.py` when the source needs normalization, then call this skill with `camera_id`, `source_type=local_file`, `raw_segment_uri`, `capture_seconds`, and `sampling_strategy`. Use `sampling_strategy.mode=interval` and `interval_seconds=1` as the default unless the user requests a different cadence.

## Workflow

1. Read camera id, raw segment URI, and sampling strategy.
2. Support interval, fps, keyframe, and adaptive strategies when available.
3. Generate stable frame ids from camera id, timestamp, and sequence.
4. Store sampled images and return Frame schema records.

## Available Implementation

- Registry name: `frame-sampling`
- Python class: `src.skills.video_data.frame_sampling.FrameSamplingSkill`
- Real video backend: OpenCV `cv2.VideoCapture`
- Output directory: `context.config["output_dir"]/frames/<camera_id>` for local videos
- CLI entrypoint: `python frame-sampling/scripts/run.py --input <input.json> --config <config.json> --output <result.json>`

## Standalone CLI

This Skill can be executed independently through the shared runner:

```bash
python frame-sampling/scripts/run.py \
  --input <input.json> \
  --config <config.json> \
  --output <result.json>
```

This directory owns registry skill `frame-sampling`, so the agent should call this script directly for this skill.

## Tool Invocation

Call through the registry:

```python
registry.get("frame-sampling").run(input_data, context)
```

For a local video run, create an input JSON with `raw_segment_uri`, `source_type=local_file`, `camera_id`, `capture_seconds`, and `sampling_strategy`, then call:

```bash
python frame-sampling/scripts/run.py \
  --input <input.json> \
  --config <config.json> \
  --output <result.json>
```

## Inputs

Required:
- `camera_id`

Common fields:
- `raw_segment_uri`
- `source_type`
- `started_at`
- `capture_seconds`
- `sampling_strategy.mode`: `interval` or `fps`
- `sampling_strategy.interval_seconds`
- `sampling_strategy.fps`

## Outputs

Returns `frames`, each with `frame_id`, `camera_id`, `timestamp`, `image_uri`, `width`, `height`, `sequence`, and for real video `source_elapsed_seconds`.

## Failure Modes

- `MISSING_CAMERA_ID`
- `VIDEO_OPEN_FAILED`

## Constraints

Do not run detection, tracking, event recognition, or evidence annotation.
