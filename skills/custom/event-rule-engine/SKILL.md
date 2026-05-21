---
name: event-rule-engine
description: Orchestrate configurable video event templates by calling method-level event skills and mapping their results into Event Candidate schema. Use when Codex has camera_id, tracks, detections, roi_matches, frames, segments, camera_health, enabled templates, or event_templates and needs candidate events before duplicate merge and evidence generation.
---

# Event Rule Engine

## Execution Priority

Prioritize calling this skill's `scripts/run.py` entrypoint first. Only write custom code after the script result is unsuitable or the script cannot cover the requested task.

## Input Resolution

If `frames`, `detections`, `tracks`, or `roi_matches` are missing but the user provided a video, do not ask the user to provide these intermediate artifacts and do not write custom event detection code. Build the inputs by calling upstream skills in order: `video-stream-ingestion/scripts/run.py`, `frame-sampling/scripts/run.py`, `camera-health-check/scripts/run.py`, `object-detection/scripts/run.py`, `object-tracking/scripts/run.py`, and `roi-mapping/scripts/run.py` as needed. Then call this skill to execute configured event templates.

## Workflow

1. Read enabled event templates from input or config.
2. Validate required inputs for each method.
3. Call method skills such as spatial occupancy, temporal persistence, composition, and density aggregation.
4. Apply template join and score policies through template mapping.
5. Return event candidates with method hits and rule hits.

## Available Implementation

- Registry name: `event-rule-engine`
- Python class: `src.skills.event_detection.event_rule_engine.EventRuleEngineSkill`
- Depends on `context.registry`
- Calls method skills by names declared in `event_templates.*.methods[].skill`
- Calls `event-template-mapping` for final event creation

## Standalone CLI

This Skill can be executed independently through the shared runner:

```bash
python event-rule-engine/scripts/run.py \
  --input <input.json> \
  --config <config.json> \
  --output <result.json>
```

This directory owns registry skill `event-rule-engine`, so the agent should call this script directly for this skill.

## Tool Invocation

```python
registry.get("event-rule-engine").run(event_inputs, context)
```

Typical input is produced by `src.orchestrator.pipeline.run_camera_pipeline`.

## Inputs

- `camera_id`
- `frames`
- `detections`
- `tracks`
- `roi_matches`
- `segments`
- `camera_health`
- `templates`

Config required:
- `context.config["event_templates"]`
- Optional `context.config["enabled_event_templates"]`

## Outputs

Returns `events` and `method_outputs`. Each event includes `event_id`, `event_type`, `camera_id`, `confidence`, `reason`, `method_hits`, `rule_hits`, `related_tracks`, and `evidence_frame_ids`.

## Failure Modes

- `MISSING_REGISTRY`
- Propagates failures from called method skills.

## Constraints

Do not hard-code local policies or create evidence files.
