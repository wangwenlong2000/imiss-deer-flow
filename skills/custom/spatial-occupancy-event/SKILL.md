---
name: spatial-occupancy-event
description: Determine whether objects, tracks, or masks occupy configured video monitoring ROI regions. Use when Codex has roi_matches, tracks, and method_config with target_labels, roi_types, excluded_roi_types, min_overlap_ratio, or min_confidence and needs method-level spatial matches for event templates.
---

# Spatial Occupancy Event

## Execution Priority

Prioritize calling this skill's `scripts/run.py` entrypoint first. Only write custom code after the script result is unsuitable or the script cannot cover the requested task.

## Input Resolution

If `roi_matches`, `tracks`, or objects are missing but the user provided a video, do not ask the user to provide them and do not write custom spatial logic. Call upstream skills in order: `frame-sampling/scripts/run.py`, `object-detection/scripts/run.py`, `object-tracking/scripts/run.py`, and `roi-mapping/scripts/run.py`, then call this skill.

## Workflow

1. Read target labels, ROI types, excluded ROI types, position strategy, and thresholds.
2. Filter tracks or objects by label and ROI match.
3. Apply overlap and confidence thresholds.
4. Return method-level matches with subject ids, ROI ids, overlap ratio, confidence, and evidence frame ids.

## Available Implementation

- Registry name: `spatial-occupancy-event`
- Python class: `src.skills.event_detection.spatial_occupancy_event.SpatialOccupancyEventSkill`
- Usually called by: `event-rule-engine`

## Standalone CLI

This Skill can be executed independently through the shared runner:

```bash
python spatial-occupancy-event/scripts/run.py \
  --input <input.json> \
  --config <config.json> \
  --output <result.json>
```

This directory owns registry skill `spatial-occupancy-event`, so the agent should call this script directly for this skill.

## Tool Invocation

```python
registry.get("spatial-occupancy-event").run({
    "method_config": method_config,
    "tracks": tracks,
    "roi_matches": roi_matches,
}, context)
```

## Inputs

- `method_config.target_labels`
- `method_config.roi_types`
- `method_config.excluded_roi_types`
- `method_config.min_overlap_ratio`
- `method_config.min_confidence`
- `tracks`
- `roi_matches`

## Outputs

Returns `method="spatial_occupancy"`, `matched`, and `matches`.

## Failure Modes

Returns `matched=false` when no subject passes label, ROI, overlap, and confidence filters.

## Constraints

Do not run detection, generate evidence, route work orders, or emit final business event types directly.
