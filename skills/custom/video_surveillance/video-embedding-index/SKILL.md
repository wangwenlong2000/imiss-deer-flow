---
name: video-embedding-index
description: Generate text embeddings or StreetModel sampled-frame image embeddings for video-library documents after city-video-intelligence has routed the request. Only enrich citybrain-video-library in place. For direct user StreetModel, ingest-and-embed, semantic retrieval, benchmark, or keyword-vs-vector requests, load city-video-intelligence first.
---

# Video Embedding Index

## Execution Priority

For direct user business requests that ingest videos, test StreetModel, compare keyword and vector search, or perform semantic monitoring-video retrieval, load `city-video-intelligence` first and run its router script. Use this atomic skill only after the business route is clear.

For every video ingestion workflow, attempt this skill after `batch-video-ingestion`. Use StreetModel sampled-frame image embeddings, not the generic text-embedding provider. The default `image_frames` mode extracts local JPEG frames, sends them inline as Base64, averages their vectors, normalizes the result, and never requires StreetModel to read a mounted video path. Use `--storage-mode in_place`; the skill reads or creates the `citybrain-video-library` document and performs a partial ES update. If embedding fails after source ingestion succeeded, preserve the source-index document and report partial success with the concrete vector failure.

For business execution, do not run proactive StreetModel health probes or `curl` checks. Run the selected embedding action once; if it returns a concrete StreetModel failure, report that failure and stop. Use `streetmodel_health` only when the user explicitly asks to debug StreetModel service health.

Prioritize `streetmodel_embed_query` for query vectors and `streetmodel_embed_video` for direct-video writes when MCP is enabled. The MCP video tool uses the same sampled-frame image flow by default. Both CLI and MCP are restricted to `citybrain-video-library`. For indexed-library enrichment, prefer the CLI because it reads the existing source document and applies a partial update.

## Safety Boundary

Only `citybrain-video-library` is allowed as the source and target index. Only `embedding_storage_mode=in_place` is accepted. Update embedding fields through ES `_update`; never write a separate vector index or replace the complete source document.

## Input Resolution

Use MCP `streetmodel_embed_video` when the caller has one locally readable video path. It extracts images locally and writes only to `citybrain-video-library`; the video path does not need to be visible to StreetModel. Use MCP `streetmodel_embed_query` when the user only needs a text query vector. Use `--source-index citybrain-video-library`, `--target-index citybrain-video-library`, and `--storage-mode in_place` to enrich indexed documents. Any other index is rejected before an ES request.

## MCP Tools

When the `streetmodel-video-embedding` MCP server is enabled, prefer these tools:

- `streetmodel_health`: Check StreetModel `/health`, `/models`, configured dimensions, and vector field before long-running embedding jobs.
- `streetmodel_embed_video`: Extract representative JPEG frames, submit `image_base64` items, aggregate the returned vectors, and write one video-document vector to Elasticsearch. It defaults to `video_preprocess=image_frames`, `image_frame_count=8`, `frame_sample_fps=1.0`, and `proxy_max_width=768`. Keep `return_vector=false` unless the caller explicitly needs the raw 2048-dim vector in the response.
- `streetmodel_embed_query`: Generate a text query vector for semantic retrieval. Use `query_vector_output` when a downstream script needs a JSON file.

If the MCP tools are missing, fail, or are not exposed to the current agent, use the atomic CLI fallback below.

## Atomic CLI

```bash
python video-embedding-index/scripts/run.py --source-index citybrain-video-library --target-index citybrain-video-library --storage-mode in_place --owner huangxiao --embedding-provider streetmodel --config <config.json> --output <result.json>
```

Parameters: `--source-index`, `--target-index`, `--storage-mode`, `--owner`, `--video-id`, `--video-uri`, `--video-vector-output`, `--query`, `--query-vector-output`, `--limit`, `--config`, `--output`, `--refresh`, `--embedding-provider`, `--embedding-model`, `--dimensions`, `--vector-field`, `--base-url`, `--timeout-seconds`, `--video-preprocess`, `--frame-sample-fps`, `--image-frame-count`, `--proxy-max-width`. Legacy mounted-video mode also accepts `--deerflow-path-prefix`, `--streetmodel-path-prefix`, `--copy-video-to-shared`, `--max-sampled-frames`, and `--proxy-output-fps`.

Single local video using the default mount-free image-frame flow:

```bash
python video-embedding-index/scripts/run.py --embedding-provider streetmodel --video-id camera01_0001 --video-uri /mnt/user-data/uploads/0001.mp4 --video-preprocess image_frames --image-frame-count 8 --target-index citybrain-video-library --storage-mode in_place --config <config.json> --output <result.json>
```

Use direct video URI input only as an explicit legacy compatibility mode when StreetModel can read the path:

```bash
python video-embedding-index/scripts/run.py --embedding-provider streetmodel --video-id camera01_0001 --video-uri /nfsdat2/home/xhuangslm/shared_videos/camera01/0001.mp4 --video-preprocess none --target-index citybrain-video-library --storage-mode in_place --config <config.json> --output <result.json>
```

## Workflow

1. Load embedding and `streetmodel_embedding` config from `config.yaml`.
2. For `provider=streetmodel`, validate `/health` and `/models`, probe the local source, and uniformly extract up to `image_frame_count` JPEG frames.
3. Call `/embed` once with `items[].type=image_base64`, average all returned frame vectors, and L2-normalize the result.
4. In `in_place` mode, partially update the original source document with `embedding_status`, `embedding_model`, `embedding_provider`, `video_embedding_preprocess_mode=image_frames`, frame-count/aggregation metadata, and the configured 2048-dimensional vector field.
5. When `--video-uri` is provided, skip source-index reads and upsert the direct video into `citybrain-video-library`.
6. For legacy text providers, generate vectors from each document's `embedding_text` or `content_text` into `vector` in the same document.
7. Refresh `citybrain-video-library` and return counts.

## Available Implementation

This skill is implemented as an atomic standalone script in its own `scripts/run.py`. It uses standard-library HTTP for Elasticsearch and StreetModel. For legacy local semantic embeddings it requires `sentence-transformers` when `provider=sentence-transformers`.

## Outputs

Returns `embedded_count`, `failed_count`, `source_index`, `target_index`, `storage_mode`, `owner`, `embedding_model`, `vector_field`, and `documents`.

## Failure Modes

- `UNSUPPORTED_VIDEO_INDEX`
- `UNSUPPORTED_EMBEDDING_STORAGE_MODE`
- `EMBEDDING_DEPENDENCY_MISSING`
- `EMBEDDING_MODEL_LOAD_FAILED`
- `STREETMODEL_CONNECTION_FAILED`
- `STREETMODEL_MODEL_NOT_FOUND`
- `STREETMODEL_INVALID_RESPONSE`
- `FFPROBE_MISSING`
- `FFPROBE_FAILED`
- `FFMPEG_MISSING`
- `VIDEO_FRAME_EXTRACTION_FAILED`
- `VIDEO_FRAME_EXTRACTION_EMPTY`
- `VIDEO_PREPROCESS_SOURCE_NOT_READABLE`
- `VIDEO_PROXY_GENERATION_FAILED`
- `VIDEO_SOURCE_NOT_FOUND`
- `VIDEO_COPY_TO_SHARED_FAILED`
- `VIDEO_URI_NOT_MAPPABLE`
- `VECTOR_FIELD_DIMENSION_MISMATCH`
- `ES_CONNECTION_FAILED`
- `ES_REQUEST_FAILED`

When the script returns `status="failed"` for `STREETMODEL_CONNECTION_FAILED`, `STREETMODEL_REQUEST_FAILED`, `STREETMODEL_MODEL_NOT_FOUND`, `VECTOR_FIELD_DIMENSION_MISMATCH`, or any other embedding failure, report that concrete failure and stop. Do not probe ports, search for config files, or try alternate service URLs unless the user explicitly asks for debugging.

In an ingestion chain, an embedding failure must not delete or roll back the successfully written source document. Aggregate it as `overall_status=partial_success`, `source_ingestion_status=success`, and `vector_status=failed`.

## Constraints

Do not delete source documents. Change only embedding lifecycle, preparation, and vector fields through partial updates. Reject every source or target index other than `citybrain-video-library`.
