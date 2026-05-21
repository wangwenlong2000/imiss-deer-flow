---
name: roi-mapping
description: Match detections or tracks against configured camera ROI polygons. Use when Codex has camera_id plus tracks or objects and needs point containment or bbox overlap matches for no-parking, sidewalk, road, public-area, or other ROI types before spatial event detection.
---

# ROI Mapping

## Execution Priority

Prioritize calling this skill's `scripts/run.py` entrypoint first. Only write custom code after the script result is unsuitable or the script cannot cover the requested task.

## Input Resolution

If tracks or detections are missing but the user provided a video, do not ask the user to provide geometric inputs and do not write ROI matching code. Call `frame-sampling/scripts/run.py`, `object-detection/scripts/run.py`, and `object-tracking/scripts/run.py` as needed before this skill. If ROI polygons are missing, first use camera ROI definitions from the config; only ask the user when neither config nor task context provides usable ROIs.

## Workflow

1. Read camera ROI configuration and subjects.
2. Choose center point, bottom center, bbox overlap, or mask overlap strategy.
3. Compute containment or overlap for each subject and ROI.
4. Return matched ROI ids, types, overlap ratios, and confidence.

## Available Implementation

- Registry name: `roi-mapping`
- Python class: `src.skills.vision_processing.roi_mapping.ROIMappingSkill`
- Geometry helpers: `src.skills.utils.point_in_polygon`, `bbox_iou_like_overlap`, `polygon_bounds`
- Camera ROI source: `context.config["cameras"][camera_id]["rois"]`

## Standalone CLI

This Skill can be executed independently through the shared runner:

```bash
python roi-mapping/scripts/run.py \
  --input <input.json> \
  --config <config.json> \
  --output <result.json>
```

This directory owns registry skill `roi-mapping`, so the agent should call this script directly for this skill.

## Tool Invocation

```python
registry.get("roi-mapping").run({"camera_id": camera_id, "tracks": tracks}, context)
```

## Inputs

- `camera_id`
- `tracks` or `objects`
- Optional `rois`
- Optional `roi_types`
- Optional `position_strategy`: `bottom_center`, `center_point`, or `bbox_overlap`

## Outputs

Returns `matches`, each with `object_id`, `roi_id`, `roi_type`, `overlap_ratio`, `matched`, and `confidence`.

## Failure Modes

The current implementation returns empty matches when ROI configuration or bboxes are missing.

## Constraints

Do not create final event types. Keep geometric matching explainable.
