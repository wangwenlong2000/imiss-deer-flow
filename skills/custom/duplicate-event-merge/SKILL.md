---
name: duplicate-event-merge
description: Merge duplicate video event candidates from the same camera, event type, ROI, related tracks, or overlapping time window. Use after event-rule-engine and before evidence generation or external output when Codex has a list of Event Candidate records and needs canonical events plus duplicate mappings.
---

# Duplicate Event Merge

## Execution Priority

Prioritize calling this skill's `scripts/run.py` entrypoint first. Only write custom code after the script result is unsuitable or the script cannot cover the requested task.

## Input Resolution

If event candidates are missing but the user provided a video or upstream analysis request, do not write custom duplicate merging or event generation code. First call `event-rule-engine/scripts/run.py` to produce candidate events, then call this skill to merge duplicates. Only ask for input when no event candidates can be produced or inferred.

## Workflow

1. Read event candidates.
2. Build duplicate keys from camera, event type, ROI, and related tracks.
3. Merge confidence and time range.
4. Return canonical events and duplicate mapping.

## Available Implementation

- Registry name: `duplicate-event-merge`
- Python class: `src.skills.quality_review_metrics.duplicate_event_merge.DuplicateEventMergeSkill`
- Pipeline caller: `src.orchestrator.pipeline.run_camera_pipeline`

## Standalone CLI

This Skill can be executed independently through the shared runner:

```bash
python duplicate-event-merge/scripts/run.py \
  --input <input.json> \
  --config <config.json> \
  --output <result.json>
```

This directory owns registry skill `duplicate-event-merge`, so the agent should call this script directly for this skill.

## Tool Invocation

```python
registry.get("duplicate-event-merge").run({"events": events}, context)
```

## Inputs

- `events`: list of Event Candidate dictionaries

## Outputs

Returns:
- `events`: canonical merged events
- `duplicates`: mappings of `main_event_id` to `merged_event_id`

## Failure Modes

The current implementation tolerates empty input and returns empty outputs.

## Constraints

Do not drop evidence metadata from the canonical event.
