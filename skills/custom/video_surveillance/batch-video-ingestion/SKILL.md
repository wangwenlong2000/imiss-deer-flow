---
name: batch-video-ingestion
description: Batch ingest local monitoring videos into the Elasticsearch video library after city-video-intelligence has routed the request, with metadata, sampled-frame object summaries, and searchable documents. For direct user video ingestion, ingest-and-embed, Elasticsearch, StreetModel, or business video-library requests, load city-video-intelligence first. Use this atomic skill when Codex already has a clear ingestion plan, video directory, video manifest, camera_id, or multiple video files to index for later retrieval and statistics.
---

# Batch Video Ingestion

## Execution Priority

For any direct user business request that mentions ingestion, import, Elasticsearch, semantic embedding, StreetModel, search, statistics, evidence, or camera operations, load `city-video-intelligence` first and run its router script. Use this atomic skill only after the business route is clear and recommends ingestion.

Do not edit manifests or skill scripts to recover from avoidable CLI mistakes. If a manifest is needed, create it once with `write_file` before running this script.

For every ingestion request, run metadata-only source ingestion first. Do not run object detection, frame sampling, YOLO, or tracking unless the user explicitly asks for object counts, labels, vehicles/person detection, statistics, or event analysis. Initialize the source document with `embedding_status=pending`, then attempt `video-embedding-index` with StreetModel against the same index and `video_id`. If that later step fails, keep this source-index result and report the overall workflow as partial success.

Prioritize calling this skill's `scripts/run.py` entrypoint first. Only write custom code after the script result is unsuitable or the script cannot cover the requested task.

## User Input Prompt

When the video source is missing, use the exact input template returned by `city-video-intelligence`.

- Request `index`, `target_index`, `embedding_storage_mode`, and `videos`. For the unified workflow, both index fields are `citybrain-video-library` and the storage mode is `in_place`.
- State that every ingestion extracts representative images and attempts a StreetModel frame embedding, vector failure does not remove the source document, and the user must not provide a `vector` array.
- Accept a plain video file path or directory as the minimal reply.
- Treat every field except a video path as optional. Preserve any supplied `video_id`, `camera_id`, time, location, description, and tags.
- Do not ask the user to choose an embedding model unless they explicitly request a non-default model.

## Input Resolution

If the user provides a directory, pass it as `--video-dir`. If the user provides a manifest, pass it as `--manifest-json`. A manifest may be a list of video records or an object with `videos`. Each record may include `video_id`, `camera_id`, `file_path`, `video_path`, `path`, `virtual_path`, `source_type`, `started_at`, `capture_seconds`, `metadata`, `location`, `description`, `tags`, `analysis_mode`, and optional `vector`.

Preserve user-provided business metadata in the manifest: camera id, camera name, location, road/intersection, owner department, capture time, description, tags, scene type, and any business labels. Put these fields under `metadata`, `location`, `tags`, and top-level IDs so later search and embedding can use them.

## Ingestion Modes

There are two ingestion modes:

- Default metadata-only ingestion: use `--analysis-mode metadata_only` or `--skip-content-detection`. This validates/probes the video, enriches metadata, and writes searchable fields to Elasticsearch. It does not sample frames or run YOLO. Use this for normal入库, ingest-and-embed, large batches, quick cataloging, and search by known metadata such as city, road, camera, filename, tags, description, and path.
- Explicit content-detection ingestion: use `--analysis-mode object_detection` only when the user asks for object labels/counts, detection, statistics, or event-analysis prerequisites. This probes the video, samples frames, runs YOLO object detection, tracks objects, and writes labels/counts/tracks to Elasticsearch. Warn that it is slower because model loading, frame extraction, and per-frame inference can take tens of seconds per video depending on sampling settings.

When metadata is available, include as much relevant information as possible in the manifest: `location.city`, `location.district`, address, road, camera name, scene, tags, description, shooting time, owner scope, and any business labels. The script writes these into `metadata`, `location`, `search_terms`, `path_tokens`, and `content_text` so later retrieval can match them.

## Atomic CLI

```bash
python batch-video-ingestion/scripts/run.py --manifest-json <videos.json> --index citybrain-video-library --analysis-mode metadata_only --config <config.json> --refresh --output <result.json>
```

Parameters: `--input`, `--manifest-json`, `--video-dir`, `--index`, `--analysis-mode`, `--skip-content-detection`, `--config`, `--output`, `--refresh`.

## Workflow

1. Resolve video records from manifest, input JSON, or directory scan.
2. Ensure the Elasticsearch index and mapping exist.
3. Normalize each local video with `video-stream-ingestion`.
4. Enrich the document with filename/path tokens, metadata, location, tags, description, and search terms.
5. In explicit `object_detection` mode only, sample frames, run object detection, and run object tracking when available.
6. Build a searchable video document with pending embedding fields and upsert it into `citybrain-video-library`.
7. Return the source-ingestion result so the orchestrator can extract representative frames and enrich the same document with a 2048-dimensional StreetModel image vector.

## Available Implementation

This skill is implemented as an atomic standalone script in its own `scripts/run.py`. It calls neighboring skill scripts by path and does not import shared registry modules.

## Outputs

Returns `ingested_count`, `failed_count`, `index`, `documents`, and `failures`. Each document summary includes `ingestion_mode` and `content_detection_enabled`. After success, continue to `video-embedding-index` without rerunning this script. Preserve this successful result if embedding later fails.

## Failure Modes

- `MISSING_INPUT`
- `ES_CONNECTION_FAILED`
- `ES_REQUEST_FAILED`
- Per-video failures are returned in `failures` while successful videos continue.

## Constraints

Only `citybrain-video-library` is accepted. Any other CLI, input JSON, or configured index returns `UNSUPPORTED_VIDEO_INDEX` before an ES request. Do not run LLM event analysis during default ingestion. YOLO output is used only for object labels, counts, tracks, and retrieval metadata.
