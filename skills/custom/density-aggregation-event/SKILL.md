---
name: density-aggregation-event
description: Detect excessive counts, density, or clustering for target labels in configured ROIs. Use when Codex has tracks, roi_matches, camera_id, ROI polygons, and method_config with target_labels, roi_types, min_count, min_density, or min_duration_seconds and needs method-level aggregation clusters.
---

# Density Aggregation Event

## Execution Priority

Prioritize calling this skill's `scripts/run.py` entrypoint first. Only write custom code after the script result is unsuitable or the script cannot cover the requested task.

## Input Resolution

If `tracks` or `roi_matches` are missing but the user provided a video, do not ask the user to provide them and do not write custom density code. Call upstream skills in order: `frame-sampling/scripts/run.py`, `object-detection/scripts/run.py`, `object-tracking/scripts/run.py`, and `roi-mapping/scripts/run.py`, then call this skill. Use task-implied labels such as `person` for crowd, density, fight, or people aggregation analysis.

## Workflow

1. Filter tracks by target label, ROI type, and duration.
2. Count subjects per ROI.
3. Estimate density from ROI or cluster area.
4. Return clusters that exceed count and density thresholds.

## Available Implementation

- Registry name: `density-aggregation-event`
- Python class: `src.skills.event_detection.density_aggregation_event.DensityAggregationEventSkill`
- ROI area helper: `src.skills.utils.polygon_area`
- Usually called by: `event-rule-engine`

## Standalone CLI

This Skill can be executed independently through the shared runner:

```bash
python density-aggregation-event/scripts/run.py \
  --input <input.json> \
  --config <config.json> \
  --output <result.json>
```

This directory owns registry skill `density-aggregation-event`, so the agent should call this script directly for this skill.

## Tool Invocation

```python
registry.get("density-aggregation-event").run({
    "camera_id": camera_id,
    "method_config": method_config,
    "tracks": tracks,
    "roi_matches": roi_matches,
}, context)
```

## Inputs

- `method_config.target_labels`
- `method_config.roi_types`
- `method_config.min_count`
- `method_config.min_density`
- `method_config.min_duration_seconds`
- `tracks`
- `roi_matches`
- Optional `rois`

## Outputs

Returns `method="density_aggregation"`, `matched`, and `clusters` with `cluster_id`, `count`, `density`, `roi_id`, `duration_seconds`, and `confidence`.

## Failure Modes

Returns `matched=false` when no ROI exceeds configured count and density thresholds.

## Constraints

Do not create evidence or final business events directly.
