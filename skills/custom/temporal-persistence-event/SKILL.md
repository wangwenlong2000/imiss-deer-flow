---
name: temporal-persistence-event
description: Determine whether a tracked object or visual state persists long enough to count as sustained occupancy or sustained anomaly. Use when Codex has tracks and method_config with target_labels, movement_states, min_duration_seconds, or max_gap_seconds and needs method-level persistence matches for event templates.
---

# Temporal Persistence Event

## Execution Priority

Prioritize calling this skill's `scripts/run.py` entrypoint first. Only write custom code after the script result is unsuitable or the script cannot cover the requested task.

## Input Resolution

If `tracks` are missing but the user provided frames or a video, do not ask the user to provide tracks and do not write custom temporal analysis. Call `frame-sampling/scripts/run.py` when frames are missing, then `object-detection/scripts/run.py`, then `object-tracking/scripts/run.py`, and finally this skill.

## Workflow

1. Read target labels, movement states, and minimum duration.
2. Filter tracks by label and movement state.
3. Compare duration against the configured threshold.
4. Return method-level matches with subject id, duration, movement state, confidence, and time range.

## Available Implementation

- Registry name: `temporal-persistence-event`
- Python class: `src.skills.event_detection.temporal_persistence_event.TemporalPersistenceEventSkill`
- Usually called by: `event-rule-engine`

## Standalone CLI

This Skill can be executed independently through the shared runner:

```bash
python temporal-persistence-event/scripts/run.py \
  --input <input.json> \
  --config <config.json> \
  --output <result.json>
```

This directory owns registry skill `temporal-persistence-event`, so the agent should call this script directly for this skill.

## Tool Invocation

```python
registry.get("temporal-persistence-event").run({
    "method_config": method_config,
    "tracks": tracks,
}, context)
```

## Inputs

- `method_config.target_labels`
- `method_config.movement_states`
- `method_config.min_duration_seconds`
- `tracks`

## Outputs

Returns `method="temporal_persistence"`, `matched`, and `matches` with `subject_id`, `duration_seconds`, `movement_state`, `confidence`, `start_time`, and `end_time`.

## Failure Modes

Returns `matched=false` when no track reaches the configured duration and movement filters.

## Constraints

Do not perform ROI matching or final event mapping.
