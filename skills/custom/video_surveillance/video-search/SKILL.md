---
name: video-search
description: Search indexed monitoring videos from text, images, structured filters, or precomputed vectors. Use for keyword retrieval, LLM-optimized text-to-video semantic retrieval, combined keyword/vector search, or image-to-video similarity search against StreetModel/Qwen3-VL video vectors.
---

# Video Search

## Execution Priority

Prioritize calling this skill's `scripts/run.py` entrypoint first. Only write custom code after the script result is unsuitable or the script cannot cover the requested task.

## Business Orchestration Gate

For monitoring-video business requests, StreetModel testing, retrieval benchmark requests, or requests comparing keyword search with vector search, load `city-video-intelligence` first and run its router script with the raw user question. If that plan returns `follow_up_question`, stop immediately and make the whole visible response exactly that question. Use this skill only after the business route is clear.

## Input Resolution

Use `--image` when the user provides an image. Convert it directly to a Qwen3-VL image embedding and compare it with stored video vectors. Accept an HTTP(S) image URL or a StreetModel-visible path; use `--copy-image-to-shared` for a local/uploaded image outside the configured shared path.

Use `--query` for text. The default `dual` mode:

1. Remove command wording and extract the visual description.
2. Ask the configured LLM for a concise `keyword_query` and a complete `semantic_query` without inventing facts.
3. Run Elasticsearch keyword matching with `keyword_query`.
4. Convert `semantic_query` to a StreetModel text vector and run video-vector similarity search.
5. Merge both ranked lists with reciprocal rank fusion.

Use `--search-mode keyword` for lexical matching only, `semantic` for pure vector retrieval, `dual` for separate keyword/vector retrieval plus fusion, or `hybrid` for vector scoring constrained by keyword matching. Use structured filters for camera, time, object class, or event type. Use `--query-vector-json` only for a precomputed vector.

## Atomic CLI

```bash
python video-search/scripts/run.py --query "请查找夜晚路口有很多车辆经过的视频" --search-mode dual --index citybrain-video-library --config <config.yaml> --output <result.json>
```

Image-to-video similarity retrieval:

```bash
python video-search/scripts/run.py --image <query.jpg> --copy-image-to-shared --index citybrain-video-library --config <config.yaml> --output <result.json>
```

Parameters include `--query`, `--image`, `--search-mode`, `--index`, structured filters, `--query-vector-json`, StreetModel settings, LLM settings, shared-image path settings, `--top-k`, `--config`, and `--output`.

When `--config` is omitted, the script checks `DEER_FLOW_CONFIG_PATH` and then `./config.yaml`. In an AIO sandbox, prefer injected `ES_URL`, `ES_USERNAME`, and `ES_PASSWORD`; `localhost` refers to the sandbox itself, not the Elasticsearch host.

## Workflow

1. Connect to Elasticsearch using `ES_URL`, `ES_USERNAME`, and `ES_PASSWORD` or explicit config fallback.
2. Build bool filters for camera, time, labels, and event type.
3. For images, call StreetModel `/embed` with `items[].type=image` and search the stored video vector field.
4. For text, extract the description and select keyword, semantic, dual, or hybrid mode.
5. In dual mode, use the configured chat LLM to produce keyword and semantic queries, run both retrieval branches, and fuse results with RRF.
6. Return compact hits, the query plan, branch-specific results, embedding metadata, and any partial-success reason.

## Outputs

Returns `hits`, `total`, `query_mode`, `search_mode`, `input_type`, and `index`. Dual mode also returns `query_plan` and `retrievals.keyword`/`retrievals.vector`.

## Failure Modes

- `ES_CONNECTION_FAILED`
- `ES_NOT_CONFIGURED`
- `ES_REQUEST_FAILED`
- `INDEX_NOT_FOUND`
- `LLM_QUERY_OPTIMIZER_NOT_CONFIGURED`
- `LLM_QUERY_OPTIMIZATION_FAILED`
- `IMAGE_SOURCE_NOT_FOUND`
- `IMAGE_URI_NOT_MAPPABLE`
- `IMAGE_COPY_TO_SHARED_FAILED`
- `VECTOR_FIELD_UNAVAILABLE`

## Constraints

Only `citybrain-video-library` is accepted. Do not combine `--image` with `--query` or `--query-vector-json`. If the LLM or vector branch fails in dual mode, preserve successful keyword results and return `overall_status=partial_success`.

On Elasticsearch failure, preserve and report the script's `error_code`, `message`, and `detail` fields. Do not switch to `localhost:9200` or different credentials unless the configured target explicitly points there.
