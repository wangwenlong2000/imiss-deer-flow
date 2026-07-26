---
name: city-video-intelligence
description: Business orchestration for city video intelligence requests. Use when users ask for monitoring video search, semantic video retrieval, event review, object statistics, evidence packages, video ingestion, camera health operations, cross-camera analysis, or role-based scenarios such as traffic police, property security, urban management, emergency management, or legal evidence preparation. Also use it when the user asks what the current data and skills can or cannot support (capability-boundary questions) — it answers with an explicit capability_gap instead of guessing. This skill maps business intent to existing custom video skills and prevents ad hoc code generation unless the user explicitly asks for development or testing.
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
5. If the plan contains a non-empty `follow_up_question`, stop immediately and make the whole visible response exactly that question as plain assistant text. For ingestion requests, preserve the returned JSON template verbatim. Do not inspect datasets, probe services, load downstream skills, or call `ask_clarification`.

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
- When asking for missing information, ask only one concrete question. The ingestion-format template returned by the router is the sole exception: show it verbatim. Do not add any text before or after the returned question.
- If a downstream video skill returns `status: failed`, summarize that failure and stop. Do not debug services or probe ports unless the user explicitly asks for debugging.
- Attempt StreetModel sampled-frame image embedding for every ingestion request. Always run `video-stream-ingestion`, then `batch-video-ingestion` in metadata-only mode, then `video-embedding-index` with `embedding_provider=streetmodel`, `video_preprocess=image_frames`, and in-place storage. Do not require a StreetModel-visible video mount for the default ingestion path.
- Store source metadata and StreetModel vector fields in the same `citybrain-video-library` document. Use partial ES updates for embedding enrichment; never replace the full source document with a compact vector copy.
- Allow video ingestion, search, statistics, evidence lookup, and embedding tools to access only `citybrain-video-library`. Reject every other video index before making an Elasticsearch request.
- For video search, route image input to direct image-vector retrieval. Route text input to keyword retrieval plus an LLM-optimized text-to-video vector query, then use the search skill's fused ranking.
- If source ingestion succeeds and vector generation fails, keep the source-index document and return partial success. Report `overall_status=partial_success`, `source_ingestion_status=success`, `vector_status=failed`, and the concrete embedding error. Never roll back or delete the source document.
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
  "embedding_attempt_required": false,
  "embedding_failure_policy": "keep_source_and_report_partial_success | null",
  "source_index": "citybrain-video-library | null",
  "embedding_target_index": "citybrain-video-library | null",
  "embedding_storage_mode": "in_place | null",
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
- For ingestion aggregation, use `success` when source and vector writes succeed, `partial_success` when the source write succeeds but vector generation fails, and `failed` when source ingestion fails.
- If the plan includes `follow_up_question`, the whole visible response must be exactly that question. Preserve any ingestion JSON template already included in it, but do not append additional preamble, alternatives, or guidance, and do not run downstream tools first.
- If a plan has `capability_gap`, do not implement new functionality. State the nearest supported workflow or ask the single `follow_up_question` when present.
