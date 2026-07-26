---
name: video-object-analytics
description: Deterministic structured entrypoint for local-video object detection and cross-frame tracking. Use for direct requests to count visible people, vehicles, or other objects, or to track them across frames. The dedicated video_object_analytics tool owns all CLI parameters, model configuration, persistent paths, and downstream skill order.
---

# Video Object Analytics

This skill is the structured entrypoint for direct object detection and tracking requests.

## Mandatory execution rule

Call the `video_object_analytics` tool. Do not call `bash`, `frame-sampling`, `object-detection`, or `object-tracking` scripts directly for this workflow. Do not invent CLI flags.

The tool accepts structured fields only:

- `video_path`
- `operation`: `detect` or `track`
- `camera_id`
- `capture_seconds`
- `interval_seconds`
- `labels`

The tool internally runs the fixed chain:

```text
detect: frame-sampling -> object-detection
track:  frame-sampling -> object-detection -> object-tracking
```

All intermediate files are placed under the thread's `/mnt/user-data/workspace` directory and the local YOLO model is selected by the tool. Do not use `/tmp` for artifacts that another tool must read.

## Output contract

Return the tool's structured result. Use its `summary`, `track_summary`, `artifacts`, and `executed_skills` fields. Do not manually recalculate counts from raw JSON or claim a confidence threshold that is not present in the tool result.

## Constraints

- YOLO detects visible objects only; it does not decide whether an event occurred.
- Tracking output is object-level trajectory analytics only; it does not infer events or identities.
- If the tool returns `status=failed`, report its concrete error and stop instead of trying alternative shell commands.
