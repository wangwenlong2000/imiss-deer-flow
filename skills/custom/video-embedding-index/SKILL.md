---
name: video-embedding-index
description: Generate text or StreetModel video-level embeddings for video-library documents and write vectors into a protected personal Elasticsearch index. Use when Codex needs semantic vector search for indexed monitoring videos, especially for huangxiao-video-library-vector-v1.
---

# Video Embedding Index

## Execution Priority

Prioritize calling this skill's `scripts/run.py` entrypoint first. Only write custom code after the script result is unsuitable or the script cannot cover the requested task.

## Safety Boundary

By default this skill only writes to indices whose name starts with `huangxiao-`. The default target index is `huangxiao-video-library-vector-v1`. Do not write to shared indices such as `citybrain-video-library` unless the user explicitly asks and passes `--allow-shared-index`.

## Input Resolution

Use `--source-index` to read existing video documents and `--target-index` to write embedded copies. Use `--video-uri` when the caller already has one GPU-accessible or mappable video path and wants to embed it directly. Use `--copy-video-to-shared` only when the input is a local/uploaded video and the DeerFlow `deerflow_path_prefix` is mounted to the StreetModel `streetmodel_path_prefix`. Use `--embedding-provider streetmodel` when the vector must come from `Qwen3-VL-Embedding-2B` via StreetModel. Use `--query` and `--query-vector-output` when the user only needs a text query vector JSON file for `video-search`.

## Atomic CLI

```bash
python video-embedding-index/scripts/run.py --source-index citybrain-video-library --target-index huangxiao-video-library-vector-v1 --owner huangxiao --embedding-provider streetmodel --config <config.json> --output <result.json>
```

Parameters: `--source-index`, `--target-index`, `--owner`, `--video-id`, `--video-uri`, `--video-vector-output`, `--query`, `--query-vector-output`, `--limit`, `--config`, `--output`, `--allow-shared-index`, `--embedding-provider`, `--embedding-model`, `--dimensions`, `--vector-field`, `--base-url`, `--timeout-seconds`, `--deerflow-path-prefix`, `--streetmodel-path-prefix`.

Single uploaded/local video copied into the configured shared prefix:

```bash
python video-embedding-index/scripts/run.py --embedding-provider streetmodel --video-id camera01_0001 --video-uri /mnt/user-data/uploads/0001.mp4 --copy-video-to-shared --target-index huangxiao-video-library-vector-v1 --config <config.json> --output <result.json>
```

Parameters also include `--copy-video-to-shared`.

## Workflow

1. Load embedding and `streetmodel_embedding` config from `config.yaml`.
2. For `provider=streetmodel`, validate `/health` and `/models`, map each video path to the GPU-accessible prefix, and call `/embed` with `items[].type=video`.
3. Write copied documents into the protected target index with `owner`, `embedding_model`, `embedding_provider`, `video_embedding_uri`, `video_embedding_dimensions`, and the configured 2048-dim vector field.
4. When `--video-uri` is provided, skip source-index reads and upsert a single minimal video document to the protected target index. If `--copy-video-to-shared` is set for a local path, copy the file under `deerflow_path_prefix/direct_uploads/` before mapping it to `streetmodel_path_prefix`.
5. For legacy text providers, keep generating vectors from each document's `embedding_text` or `content_text` into `vector`.
6. Refresh the target index and return counts.

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
- `VIDEO_SOURCE_NOT_FOUND`
- `VIDEO_COPY_TO_SHARED_FAILED`
- `VIDEO_URI_NOT_MAPPABLE`
- `VECTOR_FIELD_DIMENSION_MISMATCH`
- `ES_CONNECTION_FAILED`
- `ES_REQUEST_FAILED`

## Constraints

Do not change or delete source documents. Do not write to non-personal/shared indices unless explicitly allowed.
