---
name: object-tracking
description: Track detected objects across video frames and return Track schema. Use when Codex has frame-level detections and needs stable track ids, trajectories, duration, last bbox, movement state, confidence, and evidence frame ids before ROI mapping and temporal event rules.
---

# Object Tracking

## Execution Priority

Prioritize calling this skill's `scripts/run.py` entrypoint first. Only write custom code after the script result is unsuitable or the script cannot cover the requested task.

## Input Resolution

If `detections` are missing but the user provided frames or a video, do not ask the user to provide detections and do not write tracking code. Call upstream skills in order: `frame-sampling/scripts/run.py` when frames are missing, then `object-detection/scripts/run.py`, then this skill. Use the detection labels implied by the task, such as `person` for people, fight, crowd, or behavior analysis.

## Workflow

1. Sort detections by timestamp.
2. Associate same-label detections across frames.
3. Build track trajectory, start/end times, duration, last bbox, and confidence.
4. Classify movement state as stationary or moving.

## Available Implementation

- Registry name: `object-tracking`
- Python class: `src.skills.vision_processing.object_tracking.ObjectTrackingSkill`
- Current backend: lightweight same-label nearest-center association
- Configuration keys: `tracking.association_distance_pixels`, `tracking.stationary_distance_pixels`

## Standalone CLI

This Skill can be executed independently through the shared runner:

```bash
python object-tracking/scripts/run.py \
  --input <input.json> \
  --config <config.json> \
  --output <result.json>
```

This directory owns registry skill `object-tracking`, so the agent should call this script directly for this skill.

## Tool Invocation

```python
registry.get("object-tracking").run({"camera_id": camera_id, "detections": detections}, context)
```

## Inputs

- `camera_id`
- `detections`
- Optional `tracks` to pass through precomputed tracks

## Outputs

Returns `tracks`, each with `track_id`, `label`, `start_time`, `end_time`, `duration_seconds`, `trajectory`, `movement_state`, `confidence`, `last_bbox`, and `evidence_frame_ids`.

## Failure Modes

The current implementation tolerates empty detections and returns an empty track list.

## Constraints

Do not apply ROI rules or emit event candidates.
