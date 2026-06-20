---
name: video-search
description: Search indexed monitoring videos in Elasticsearch by keyword, camera, time range, object labels, event type, location, and optional precomputed query vectors, including StreetModel video-vector fields. Use after videos have been ingested into the video library.
---

# Video Search

## Execution Priority

Prioritize calling this skill's `scripts/run.py` entrypoint first. Only write custom code after the script result is unsuitable or the script cannot cover the requested task.

## Business Orchestration Gate

For monitoring-video business requests, StreetModel testing, retrieval benchmark requests, or requests comparing keyword search with vector search, load `city-video-intelligence` first and run its router script with the raw user question. If that plan returns `follow_up_question`, stop immediately and make the whole visible response exactly that question. Use this skill only after the business route is clear.

## Input Resolution

Use structured filters when the user names a camera, time window, object class, event type, or video id. Use `--query` for natural-language or keyword search. Use `--embedding-provider streetmodel` when the query should be embedded by `Qwen3-VL-Embedding-2B` directly. Use `--query-vector-json` only when the user provides a precomputed vector. Use `--vector-field` for a non-default dense vector field such as `video_vector-Qwen3-VL-Embedding-2B_urban_governance`.

## Atomic CLI

```bash
python video-search/scripts/run.py --query "person near gate" --camera-id CAM_DEERFLOW_001 --top-k 10 --index huangxiao-video-library-vector-v1 --query-vector-json <query-vector.json> --config <config.json> --output <result.json>
```

StreetModel text-to-video semantic retrieval:

```bash
python video-search/scripts/run.py --query "夜晚路口有很多车辆经过" --index huangxiao-video-library-vector-v1 --embedding-provider streetmodel --vector-query-mode semantic --config <config.json> --output <result.json>
```

Parameters: `--query`, `--index`, `--camera-id`, `--start-time`, `--end-time`, `--labels`, `--event-type`, `--query-vector-json`, `--embedding-provider`, `--embedding-model`, `--base-url`, `--dimensions`, `--timeout-seconds`, `--vector-field`, `--vector-query-mode`, `--top-k`, `--config`, `--output`.

## Workflow

1. Connect to Elasticsearch using `ES_URL`, `ES_USERNAME`, and `ES_PASSWORD` or explicit config fallback.
2. Build bool filters for camera, time, labels, and event type.
3. Use keyword search over `content_text`, `filename`, location fields, and metadata.
4. If `--embedding-provider streetmodel` is set and no `--query-vector-json` is provided, call StreetModel `/embed` with `items[].type=text` to create the query vector.
5. If a query vector is available and the index has the configured vector field, use vector scoring. If that field is unavailable, fall back to the legacy `vector` field when present.
6. Use `--vector-query-mode semantic` for pure vector retrieval over filters only, or `hybrid` to combine keyword matching and vector scoring.
5. Return compact hits with scores and source metadata.

## Outputs

Returns `hits`, `total`, `query_mode`, and `index`.

## Failure Modes

- `ES_CONNECTION_FAILED`
- `ES_REQUEST_FAILED`
- `INDEX_NOT_FOUND`

## Constraints

This skill retrieves indexed video records. It does not ingest videos, run object detection, infer events, or generate evidence packages.
