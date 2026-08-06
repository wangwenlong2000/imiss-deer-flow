---
name: object-statistics
description: Compute object statistics for monitoring videos from Elasticsearch video-library records or local detections/tracks JSON. Use when Codex needs counts by label, camera, time bucket, or movement state.
---

# Object Statistics

## Execution Priority

Prioritize calling this skill's `scripts/run.py` entrypoint first. Only write custom code after the script result is unsuitable or the script cannot cover the requested task.

## Input Resolution

If the user provides detections or tracks JSON, pass them directly. If the user asks for statistics over indexed videos, query Elasticsearch with the provided `video_id`, `camera_id`, and time filters.

## Atomic CLI

```bash
python object-statistics/scripts/run.py --index citybrain-video-library --camera-id CAM_DEERFLOW_001 --group-by label,camera,time,movement --config <config.json> --output <stats.json>
```

Parameters: `--input`, `--index`, `--video-id`, `--camera-id`, `--start-time`, `--end-time`, `--detections-json`, `--tracks-json`, `--group-by`, `--config`, `--output`.

Only `citybrain-video-library` is accepted for Elasticsearch-backed statistics. Other index values return `UNSUPPORTED_VIDEO_INDEX`.

## Workflow

1. Load local detections/tracks if provided.
2. Otherwise retrieve matching video documents from Elasticsearch.
3. Aggregate object counts by label, camera, time bucket, and movement state.
4. Return normalized statistics suitable for reporting.

## Outputs

Returns `total_objects`, `by_label`, `by_camera`, `by_time_bucket`, and `movement_states`.

## Failure Modes

- `MISSING_INPUT`
- `ES_CONNECTION_FAILED`
- `ES_REQUEST_FAILED`

## Constraints

Do not infer business events or abnormal behavior. This skill only counts and groups detected/tracked objects.
