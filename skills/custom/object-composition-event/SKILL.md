---
name: object-composition-event
description: Detect configured multi-object compositions in video frames or tracks. Use when Codex has detections, tracks, roi_matches, and method_config with required label groups, min_count, group_by, spatial_relation, or max_distance_pixels and needs method-level object group matches.
---

# Object Composition Event

## Execution Priority

Prioritize calling this skill's `scripts/run.py` entrypoint first. Only write custom code after the script result is unsuitable or the script cannot cover the requested task.

## Input Resolution

If `detections`, `tracks`, or `roi_matches` are missing but the user provided a video, do not ask the user to provide these intermediate artifacts and do not write custom composition logic. Call `frame-sampling/scripts/run.py`, `object-detection/scripts/run.py`, `object-tracking/scripts/run.py`, and `roi-mapping/scripts/run.py` as needed before this skill.

## Workflow

1. Read required label groups, minimum counts, spatial relation, and grouping mode.
2. Group objects by frame, ROI, or whole scene.
3. Check required counts and spatial relation.
4. Return groups with object ids, labels, ROI id, and confidence.

## Available Implementation

- Registry name: `object-composition-event`
- Python class: `src.skills.event_detection.object_composition_event.ObjectCompositionEventSkill`
- Usually called by: `event-rule-engine`
- Geometry helper: `src.skills.utils.distance`

## Standalone CLI

This Skill can be executed independently through the shared runner:

```bash
python object-composition-event/scripts/run.py \
  --input <input.json> \
  --config <config.json> \
  --output <result.json>
```

This directory owns registry skill `object-composition-event`, so the agent should call this script directly for this skill.

## Tool Invocation

```python
registry.get("object-composition-event").run({
    "method_config": method_config,
    "detections": detections,
    "tracks": tracks,
    "roi_matches": roi_matches,
}, context)
```

## Inputs

- `method_config.required`
- `method_config.spatial_relation`
- `method_config.max_distance_pixels`
- `method_config.group_by`
- `detections`
- `tracks`
- `roi_matches`

## Outputs

Returns `method="object_composition"`, `matched`, and `groups`.

## Failure Modes

Returns `matched=false` when required label counts or spatial relation rules are not satisfied.

## Constraints

Do not decide final business semantics without template mapping.
