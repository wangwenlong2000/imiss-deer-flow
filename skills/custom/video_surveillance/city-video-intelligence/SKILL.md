---
name: city-video-intelligence
description: Business orchestration for city video intelligence requests. Use when users ask for monitoring video search, semantic video retrieval, event review, object statistics, evidence packages, video ingestion, camera health operations, cross-camera analysis, or role-based scenarios such as traffic police, property security, urban management, emergency management, or legal evidence preparation. This skill maps business intent to existing custom video skills and prevents ad hoc code generation unless the user explicitly asks for development or testing.
---

# City Video Intelligence Orchestration

## Overview

Use this skill as the business layer above the existing video-monitoring atomic skills. It classifies a user request, selects the right capability and scenario, returns the recommended skill chain, and states the expected business deliverable. Use it for direct user tasks as well as planning questions.

This skill is not a replacement for `video-search`, `single-video-event-analysis`, `object-statistics`, `evidence-package-generation`, `batch-video-ingestion`, `camera-health-check`, or related atomic skills. It tells the agent which of those existing skills to use and when to stop for human confirmation.

## Execution Priority

1. Run `scripts/run.py` with the user's raw request to produce a structured orchestration plan. This is mandatory before any clarification question, except destructive operations that require confirmation.
2. Read `references/capability-map.md` only when the request is ambiguous, role-based, multi-step, or asks about available capabilities.
3. Follow the returned `recommended_skill_chain` by loading each downstream skill's `SKILL.md` before running its script.
4. Return business-facing results: matched capability, scenario, execution plan, deliverables, risks, and required confirmations.
5. If the plan contains a non-empty `follow_up_question`, stop immediately and make the whole visible response exactly that question as plain assistant text. Do not inspect datasets, probe services, load downstream skills, or call `ask_clarification`.

## Atomic CLI

```bash
python city-video-intelligence/scripts/run.py \
  --request "我是交警队的，需要调取大成路路口今天上午 8-10 点的监控" \
  --output <plan.json>
```

Parameters: `--request`, `--role`, `--output`, `--format`.

## Business Mode Rules

- Do not write ad hoc Python, JavaScript, SQL, or shell code for business requests.
- Use existing skill scripts and documented workflows. If existing skills cannot satisfy the request, return a `capability_gap` and ask for authorization before implementation work.
- Treat destructive operations such as deleting indexed videos, rewriting shared indices, or purging test data as requiring explicit confirmation.
- Treat event conclusions as evidence-bound. If video evidence is missing, blurry, occluded, sparse, or ambiguous, return `requires_human_review=true`.
- Do not let object detection alone decide event semantics. Use it only as supporting context.
- For evidence package requests, do not run event analysis merely to invent an event. If the user provides an event time or time range, package that range. If the event time is missing, ask for it or return a plan with the missing input.
- For development or benchmark requests, use provided test videos, indexes, or query terms. If no test data is available, return `capability_gap` or ask for the missing dataset before running lower-level tools.
- When asking for missing information, ask only one concrete question. Do not add preamble text, explanations, examples, option lists, `context`, alternatives such as "if you have...", or extra prompt text unless the user asks for guidance. Apply the same rule to final prose questions.
- If a downstream video skill returns `status: failed`, summarize that failure and stop. Do not debug services or probe ports unless the user explicitly asks for debugging.
- Once the orchestration plan or requested downstream result is available, stop calling tools and summarize the result.

## Capability Families

Use these families to classify requests before selecting downstream skills:

- `video_asset_retrieval`: locate known videos or indexed clips by keyword, camera, place, time, object, or count condition.
- `semantic_video_retrieval`: retrieve videos by natural-language visual meaning through StreetModel/vector search.
- `single_video_event_understanding`: inspect one video and produce a timeline, event candidates, confidence, and review status.
- `object_statistics`: count objects, group by time/camera/type, and summarize trends.
- `evidence_preservation`: generate snapshots, clips, privacy-masked artifacts, hashes, and package manifests.
- `video_library_governance`: ingest, index, embed, check storage, or manage video assets.
- `camera_health_operations`: diagnose online status, black screen, blur, occlusion, freeze, corruption, or view shift.
- `cross_video_investigation`: compare, correlate, or track across cameras and time windows.
- `business_scenario_orchestration`: translate role-based requests into one of the above chains.
- `development_test`: benchmark or compare retrieval/embedding behavior only when the user explicitly asks for testing or development.

## Output Contract

Return or preserve JSON-compatible fields:

```json
{
  "status": "success",
  "request": "",
  "mode": "business_operation | development_test",
  "capability": "",
  "business_scenario": "",
  "recommended_skill_chain": [],
  "execution_plan": [],
  "expected_deliverables": [],
  "missing_fields": [],
  "follow_up_question": null,
  "guardrails": [],
  "requires_confirmation": false,
  "requires_human_review": false,
  "capability_gap": null
}
```

## Constraints

- Do not execute downstream atomic skills until the plan is clear.
- Do not create new code to bypass an unsupported operation.
- Do not perform destructive video-library management without explicit confirmation.
- Do not produce legal, enforcement, or emergency conclusions without evidence references and review status.
- If the plan includes `follow_up_question`, the whole visible response must be exactly that question. Do not append preamble, markdown, examples, alternatives, or extra guidance, and do not run downstream tools first.
- If a plan has `capability_gap`, do not implement new functionality. State the nearest supported workflow or ask the single `follow_up_question` when present.
