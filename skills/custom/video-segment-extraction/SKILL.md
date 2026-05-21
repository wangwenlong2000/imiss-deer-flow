---
name: video-segment-extraction
description: Extract playable short video evidence clips around event times. Use when Codex has event_id, camera_id, raw_segment_uri, event_time, event_elapsed_seconds, pre_seconds, or post_seconds and needs an MP4 clip URI plus clip time range. This skill exposes FFmpeg-backed clipping for real local videos and memory-backed synthetic clips for tests.
---

# Video Segment Extraction

## Execution Priority

Prioritize calling this skill's `scripts/run.py` entrypoint first. Only write custom code after the script result is unsuitable or the script cannot cover the requested task.

## Input Resolution

If event timing is missing but the user provided a video analysis request, do not write custom clipping code. First call upstream event skills, especially `event-rule-engine/scripts/run.py`, to produce an event with `event_time` or `event_elapsed_seconds`; then call this skill with `raw_segment_uri`, `event_id`, and the pre/post window.

## Workflow

1. Read event id, source segment, event time, and pre/post window.
2. Locate the source media range.
3. Clip or synthesize a short segment in a playable format.
4. Store and return the clip URI and time range.

## Available Implementation

- Registry name: `video-segment-extraction`
- Python class: `src.skills.video_data.video_segment_extraction.VideoSegmentExtractionSkill`
- Real video backend: `ffmpeg`
- Pipeline caller: `src.orchestrator.pipeline.run_camera_pipeline`
- Output directory: `context.config["output_dir"]/evidence`

## Standalone CLI

This Skill can be executed independently through the shared runner:

```bash
python video-segment-extraction/scripts/run.py \
  --input <input.json> \
  --config <config.json> \
  --output <result.json>
```

This directory owns registry skill `video-segment-extraction`, so the agent should call this script directly for this skill.

## Tool Invocation

Direct registry call:

```python
registry.get("video-segment-extraction").run(input_data, context)
```

CLI command:

```bash
python video-segment-extraction/scripts/run.py \
  --input <input.json> \
  --config <config.json> \
  --output <result.json>
```

The pipeline passes `event_elapsed_seconds` so FFmpeg clips the right offset in the source video.

## Inputs

Required:
- `event_id`
- `event_time` or `start_time`

Common fields:
- `camera_id`
- `raw_segment_uri`
- `event_elapsed_seconds`
- `pre_seconds`
- `post_seconds`

## Outputs

Returns `event_id`, `clip_uri`, `start_time`, and `end_time`.

## Failure Modes

- `MISSING_EVENT`
- `FFMPEG_CLIP_FAILED`

## Constraints

Do not decide whether an event occurred. Do not modify event confidence.
