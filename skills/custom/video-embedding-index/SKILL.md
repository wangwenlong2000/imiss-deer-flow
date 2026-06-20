---
name: video-embedding-index
description: Generate text or StreetModel video-level embeddings for video-library documents after city-video-intelligence has routed the request, and write vectors into a protected personal Elasticsearch index. For direct user StreetModel, ingest-and-embed, semantic retrieval, benchmark, or keyword-vs-vector requests, load city-video-intelligence first. Use when Codex already has a clear embedding plan for indexed monitoring videos, especially for huangxiao-video-library-vector-v1.
---

# Video Embedding Index

## Execution Priority

For direct user business requests that ask to test StreetModel, compare keyword and vector search, ingest-and-embed videos, or perform semantic monitoring-video retrieval, load `city-video-intelligence` first and run its router script. Use this atomic skill only after the business route is clear.

For business execution, do not run proactive StreetModel health probes or `curl` checks. Run the selected embedding action once; if it returns a concrete StreetModel failure, report that failure and stop. Use `streetmodel_health` only when the user explicitly asks to debug StreetModel service health.

Prioritize the `streetmodel-video-embedding` MCP embedding tools when they are enabled and the request is already routed. Use `streetmodel_embed_video` or `streetmodel_embed_query` before falling back to this skill's `scripts/run.py` entrypoint. Only write custom code after the MCP tools and script result are unsuitable or cannot cover the requested task.

## Safety Boundary

By default this skill only writes to indices whose name starts with `huangxiao-`. The default target index is `huangxiao-video-library-vector-v1`. Do not write to shared indices such as `citybrain-video-library` unless the user explicitly asks and passes `--allow-shared-index`.

## Input Resolution

Use MCP `streetmodel_embed_video` when the caller already has one GPU-accessible or mappable video path and wants to embed it. The default video path preprocesses the source into a short frame-proxy MP4 before calling StreetModel, so long videos do not exceed model limits. Use MCP `streetmodel_embed_query` when the user only needs a text query vector for semantic video retrieval. If MCP tools are unavailable, use `--source-index` to read existing video documents and `--target-index` to write embedded copies. Use `--video-uri` for a direct video path. Use `--copy-video-to-shared` only when the input is a local/uploaded video and the DeerFlow `deerflow_path_prefix` is mounted to the StreetModel `streetmodel_path_prefix`. Use `--embedding-provider streetmodel` when the vector must come from `Qwen3-VL-Embedding-2B` via StreetModel. Use `--video-preprocess none` only for short videos that are known to be safe for direct model ingestion. Use `--query` and `--query-vector-output` when the user only needs a text query vector JSON file for `video-search`.

## MCP Tools

When the `streetmodel-video-embedding` MCP server is enabled, prefer these tools:

- `streetmodel_health`: Check StreetModel `/health`, `/models`, configured dimensions, and vector field before long-running embedding jobs.
- `streetmodel_embed_video`: Generate one video embedding and write it to the protected target Elasticsearch index. It defaults to `video_preprocess=frame_proxy`, `frame_sample_fps=1.0`, `max_sampled_frames=300`, `proxy_output_fps=4.0`, and `proxy_max_width=768`. Keep `return_vector=false` unless the caller explicitly needs the raw 2048-dim vector in the response.
- `streetmodel_embed_query`: Generate a text query vector for semantic retrieval. Use `query_vector_output` when a downstream script needs a JSON file.

If the MCP tools are missing, fail, or are not exposed to the current agent, use the atomic CLI fallback below.

## Atomic CLI

```bash
python video-embedding-index/scripts/run.py --source-index citybrain-video-library --target-index huangxiao-video-library-vector-v1 --owner huangxiao --embedding-provider streetmodel --config <config.json> --output <result.json>
```

Parameters: `--source-index`, `--target-index`, `--owner`, `--video-id`, `--video-uri`, `--video-vector-output`, `--query`, `--query-vector-output`, `--limit`, `--config`, `--output`, `--allow-shared-index`, `--refresh`, `--embedding-provider`, `--embedding-model`, `--dimensions`, `--vector-field`, `--base-url`, `--timeout-seconds`, `--deerflow-path-prefix`, `--streetmodel-path-prefix`, `--video-preprocess`, `--frame-sample-fps`, `--max-sampled-frames`, `--proxy-output-fps`, `--proxy-max-width`.

Single uploaded/local video copied into the configured shared prefix:

```bash
python video-embedding-index/scripts/run.py --embedding-provider streetmodel --video-id camera01_0001 --video-uri /mnt/user-data/uploads/0001.mp4 --copy-video-to-shared --target-index huangxiao-video-library-vector-v1 --config <config.json> --output <result.json>
```

Parameters also include `--copy-video-to-shared`.

Disable preprocessing only for short videos:

```bash
python video-embedding-index/scripts/run.py --embedding-provider streetmodel --video-id camera01_0001 --video-uri /nfsdat2/home/xhuangslm/shared_videos/camera01/0001.mp4 --video-preprocess none --target-index huangxiao-video-library-vector-v1 --config <config.json> --output <result.json>
```

## Workflow

1. Load embedding and `streetmodel_embedding` config from `config.yaml`.
2. For `provider=streetmodel`, validate `/health` and `/models`, map each video path to the GPU-accessible prefix, and by default create a frame-proxy MP4 in `deerflow_path_prefix/embedding_proxies/`.
3. Call `/embed` with `items[].type=video` and the proxy URI. The original source remains the document identity; the proxy is only the model input.
4. Write one copied document per original video into the protected target index with `owner`, `embedding_model`, `embedding_provider`, `video_embedding_uri`, `video_embedding_source_uri`, `video_embedding_proxy_uri`, `video_embedding_preprocess_mode`, `video_embedding_dimensions`, and the configured 2048-dim vector field.
5. When `--video-uri` is provided, skip source-index reads and upsert a single minimal video document to the protected target index. If `--copy-video-to-shared` is set for a local path, copy the file under `deerflow_path_prefix/direct_uploads/` before mapping it to `streetmodel_path_prefix`.
6. For legacy text providers, keep generating vectors from each document's `embedding_text` or `content_text` into `vector`.
7. Refresh the target index and return counts.

## Available Implementation

This skill is implemented as an atomic standalone script in its own `scripts/run.py`. It uses standard-library HTTP for Elasticsearch and StreetModel. For legacy local semantic embeddings it requires `sentence-transformers` when `provider=sentence-transformers`.

## Outputs

Returns `embedded_count`, `failed_count`, `source_index`, `target_index`, `owner`, `embedding_model`, `vector_field`, and `documents`.

## Failure Modes

- `UNSAFE_TARGET_INDEX`
- `EMBEDDING_DEPENDENCY_MISSING`
- `EMBEDDING_MODEL_LOAD_FAILED`
- `STREETMODEL_CONNECTION_FAILED`
- `STREETMODEL_MODEL_NOT_FOUND`
- `STREETMODEL_INVALID_RESPONSE`
- `FFPROBE_MISSING`
- `FFPROBE_FAILED`
- `FFMPEG_MISSING`
- `VIDEO_PREPROCESS_SOURCE_NOT_READABLE`
- `VIDEO_PROXY_GENERATION_FAILED`
- `VIDEO_SOURCE_NOT_FOUND`
- `VIDEO_COPY_TO_SHARED_FAILED`
- `VIDEO_URI_NOT_MAPPABLE`
- `VECTOR_FIELD_DIMENSION_MISMATCH`
- `ES_CONNECTION_FAILED`
- `ES_REQUEST_FAILED`

When the script returns `status="failed"` for `STREETMODEL_CONNECTION_FAILED`, `STREETMODEL_REQUEST_FAILED`, `STREETMODEL_MODEL_NOT_FOUND`, `VECTOR_FIELD_DIMENSION_MISMATCH`, or any other embedding failure, report that concrete failure and stop. Do not probe ports, search for config files, or try alternate service URLs unless the user explicitly asks for debugging.

## Constraints

Do not change or delete source documents. Do not write to non-personal/shared indices unless explicitly allowed.
