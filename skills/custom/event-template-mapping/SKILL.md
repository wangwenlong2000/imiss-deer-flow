---
name: event-template-mapping
description: Map method-level event results into final video monitoring Event Candidate semantics. Use when Codex has template_name, template config, method_results, camera_id, join_policy, score_policy, or review_threshold and needs event_type, confidence, severity, reason, method hits, rule hits, related tracks, and evidence frame ids.
---

# Event Template Mapping

## Execution Priority

Prioritize calling this skill's `scripts/run.py` entrypoint first. Only write custom code after the script result is unsuitable or the script cannot cover the requested task.

## Input Resolution

If `method_results` are missing, do not invent final event semantics in custom code. First call the method-level event skills required by the selected template, such as `spatial-occupancy-event/scripts/run.py`, `temporal-persistence-event/scripts/run.py`, `object-composition-event/scripts/run.py`, or `density-aggregation-event/scripts/run.py`; then call this skill to map method results into event candidates.

## Workflow

1. Read template name, join policy, score policy, and method results.
2. Check all-required, any-required, or weighted-score matching.
3. Compute event confidence and rule hits.
4. Return a standard Event Candidate schema.

## Available Implementation

- Registry name: `event-template-mapping`
- Python class: `src.skills.event_detection.event_template_mapping.EventTemplateMappingSkill`
- Helper function: `build_event_from_method_results`
- Usually called by: `event-rule-engine`

## Standalone CLI

This Skill can be executed independently through the shared runner:

```bash
python event-template-mapping/scripts/run.py \
  --input <input.json> \
  --config <config.json> \
  --output <result.json>
```

This directory owns registry skill `event-template-mapping`, so the agent should call this script directly for this skill.

## Tool Invocation

```python
registry.get("event-template-mapping").run({
    "template_name": template_name,
    "template": template,
    "method_results": method_results,
    "camera_id": camera_id,
}, context)
```

## Inputs

- `template_name`
- `template`
- `method_results`
- `camera_id`

## Outputs

Returns `matched=true` plus Event Candidate fields, or `matched=false`.

## Failure Modes

Returns `matched=false` when join policy fails or score is below `score_policy.min_score`.

## Constraints

Do not call model inference or generate evidence files.
