---
name: batch-video-ingestion
description: Batch ingest local monitoring videos into the Elasticsearch video library with metadata, sampled-frame object summaries, and searchable documents. Use when Codex has a video directory, video manifest, camera_id, or multiple video files to index for later retrieval and statistics.
---

# Batch Video Ingestion

## Execution Priority

Prioritize calling this skill's `scripts/run.py` entrypoint first. Only write custom code after the script result is unsuitable or the script cannot cover the requested task.

## Input Resolution

If the user provides a directory, pass it as `--video-dir`. If the user provides a manifest, pass it as `--manifest-json`. A manifest may be a list of video records or an object with `videos`. Each record may include `video_id`, `camera_id`, `file_path`, `video_path`, `path`, `virtual_path`, `source_type`, `started_at`, `capture_seconds`, `metadata`, `location`, `description`, `tags`, `analysis_mode`, and optional `vector`.

## Ingestion Modes

Always tell the user there are two ingestion modes and mention the time tradeoff:

- Fast metadata-only ingestion: use `--analysis-mode metadata_only` or `--skip-content-detection`. This validates/probes the video, enriches metadata, and writes searchable fields to Elasticsearch. It does not sample frames or run YOLO. Use this for large batches, quick cataloging, or when the user only needs search by known metadata such as city, road, camera, filename, tags, description, and path.
- Content-detection ingestion: use `--analysis-mode object_detection` (default). This probes the video, samples frames, runs YOLO object detection, tracks objects, and writes labels/counts/tracks to Elasticsearch. Warn that it is slower because model loading, frame extraction, and per-frame inference can take tens of seconds per video depending on sampling settings.

When metadata is available, include as much relevant information as possible in the manifest: `location.city`, `location.district`, address, road, camera name, scene, tags, description, shooting time, owner scope, and any business labels. The script writes these into `metadata`, `location`, `search_terms`, `path_tokens`, and `content_text` so later retrieval can match them.

## Atomic CLI

```bash
python batch-video-ingestion/scripts/run.py --manifest-json <videos.json> --index citybrain-video-library --analysis-mode object_detection --config <config.json> --refresh --output <result.json>
```

Parameters: `--input`, `--manifest-json`, `--video-dir`, `--index`, `--analysis-mode`, `--skip-content-detection`, `--config`, `--output`, `--refresh`.

## Workflow

1. Resolve video records from manifest, input JSON, or directory scan.
2. Ensure the Elasticsearch index and mapping exist.
3. Normalize each local video with `video-stream-ingestion`.
4. Enrich the document with filename/path tokens, metadata, location, tags, description, and search terms.
5. In `object_detection` mode, sample frames, run object detection, and run object tracking when available.
6. Build a searchable video document and upsert it into Elasticsearch.

## Available Implementation

This skill is implemented as an atomic standalone script in its own `scripts/run.py`. It calls neighboring skill scripts by path and does not import shared registry modules.

## Outputs

Returns `ingested_count`, `failed_count`, `index`, `documents`, and `failures`. Each document summary includes `ingestion_mode` and `content_detection_enabled`.

## Failure Modes

- `MISSING_INPUT`
- `ES_CONNECTION_FAILED`
- `ES_REQUEST_FAILED`
- Per-video failures are returned in `failures` while successful videos continue.

## Constraints

Do not run LLM event analysis during default ingestion. YOLO output is used only for object labels, counts, tracks, and retrieval metadata. This skill does not perform OCR yet; visible text extraction should be handled by a future OCR skill and added to `metadata` or `content_text`.
