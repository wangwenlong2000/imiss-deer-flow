---
name: video-embedding-index
description: Generate embeddings for video-library documents and write vectors into a protected personal Elasticsearch index. Use when Codex needs semantic vector search for indexed monitoring videos, especially for huangxiao-video-library-vector-v1.
---

# Video Embedding Index

## Execution Priority

Prioritize calling this skill's `scripts/run.py` entrypoint first. Only write custom code after the script result is unsuitable or the script cannot cover the requested task.

## Safety Boundary

By default this skill only writes to indices whose name starts with `huangxiao-`. The default target index is `huangxiao-video-library-vector-v1`. Do not write to shared indices such as `citybrain-video-library` unless the user explicitly asks and passes `--allow-shared-index`.

## Input Resolution

Use `--source-index` to read existing video documents and `--target-index` to write embedded copies. Use `--query` and `--query-vector-output` when the user only needs a query vector JSON file for `video-search`.

## Atomic CLI

```bash
python video-embedding-index/scripts/run.py --source-index citybrain-video-library --target-index huangxiao-video-library-vector-v1 --owner huangxiao --config <config.json> --output <result.json>
```

Parameters: `--source-index`, `--target-index`, `--owner`, `--video-id`, `--query`, `--query-vector-output`, `--limit`, `--config`, `--output`, `--allow-shared-index`, `--embedding-provider`, `--embedding-model`, `--dimensions`.

## Workflow

1. Load embedding config from `config.yaml`.
2. Generate vectors from each document's `embedding_text` or `content_text`.
3. Upsert copied documents into the protected target index with `owner`, `embedding_model`, `embedding_provider`, `embedding_text`, and `vector`.
4. Refresh the target index and return counts.

## Available Implementation

This skill is implemented as an atomic standalone script in its own `scripts/run.py`. It uses standard-library HTTP for Elasticsearch. For real semantic embeddings it requires `sentence-transformers` when `provider=sentence-transformers`.

## Outputs

Returns `embedded_count`, `failed_count`, `source_index`, `target_index`, `owner`, `embedding_model`, and `documents`.

## Failure Modes

- `UNSAFE_TARGET_INDEX`
- `EMBEDDING_DEPENDENCY_MISSING`
- `EMBEDDING_MODEL_LOAD_FAILED`
- `ES_CONNECTION_FAILED`
- `ES_REQUEST_FAILED`

## Constraints

Do not change or delete source documents. Do not write to non-personal/shared indices unless explicitly allowed.
