---
name: video-search
description: Search indexed monitoring videos in Elasticsearch by keyword, camera, time range, object labels, event type, location, and optional precomputed query vectors. Use after videos have been ingested into the video library.
---

# Video Search

## Execution Priority

Prioritize calling this skill's `scripts/run.py` entrypoint first. Only write custom code after the script result is unsuitable or the script cannot cover the requested task.

## Input Resolution

Use structured filters when the user names a camera, time window, object class, event type, or video id. Use `--query` for natural-language or keyword search. Use `--query-vector-json` only when the user provides a precomputed vector.

## Atomic CLI

```bash
python video-search/scripts/run.py --query "person near gate" --camera-id CAM_DEERFLOW_001 --top-k 10 --index citybrain-video-library --config <config.json> --output <result.json>
```

Parameters: `--query`, `--index`, `--camera-id`, `--start-time`, `--end-time`, `--labels`, `--event-type`, `--query-vector-json`, `--top-k`, `--config`, `--output`.

## Workflow

1. Connect to Elasticsearch using `ES_URL`, `ES_USERNAME`, and `ES_PASSWORD` or explicit config fallback.
2. Build bool filters for camera, time, labels, and event type.
3. Use keyword search over `content_text`, `filename`, location fields, and metadata.
4. If a query vector is provided and the index has a `vector` field, use vector scoring; otherwise fall back to keyword and filters.
5. Return compact hits with scores and source metadata.

## Outputs

Returns `hits`, `total`, `query_mode`, and `index`.

## Failure Modes

- `ES_CONNECTION_FAILED`
- `ES_REQUEST_FAILED`
- `INDEX_NOT_FOUND`

## Constraints

This skill retrieves indexed video records. It does not ingest videos, run object detection, infer events, or generate evidence packages.
