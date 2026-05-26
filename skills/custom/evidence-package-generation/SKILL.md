---
name: evidence-package-generation
description: Generate an evidence package manifest for indexed monitoring videos or event/search results, including snapshot evidence and optional short video clips. Use when Codex needs a package_id, manifest URI, evidence items, hashes, and reusable evidence artifacts.
---

# Evidence Package Generation

## Execution Priority

Prioritize calling this skill's `scripts/run.py` entrypoint first. Only write custom code after the script result is unsuitable or the script cannot cover the requested task.

## Input Resolution

Use `--event-json` when the user provides an event. Use `--search-result-json` when the user wants evidence for search hits. Use `--video-id` to retrieve a source video document from Elasticsearch.

## Atomic CLI

```bash
python evidence-package-generation/scripts/run.py --video-id <video_id> --event-json <event.json> --output-dir <package_dir> --index citybrain-video-library --config <config.json> --output <result.json>
```

Parameters: `--input`, `--index`, `--video-id`, `--event-json`, `--search-result-json`, `--output-dir`, `--pre-seconds`, `--post-seconds`, `--config`, `--output`.

## Workflow

1. Resolve package items from event JSON, search results, or Elasticsearch video id lookup.
2. Generate snapshot evidence when event frames are available.
3. Generate a short clip when source video and event timing are available.
4. Write a package manifest JSON containing evidence artifacts and integrity hashes.

## Outputs

Returns `package_id`, `manifest_uri`, `items`, and `evidence`.

## Failure Modes

- `MISSING_INPUT`
- `ES_CONNECTION_FAILED`
- `ES_REQUEST_FAILED`
- Nested evidence generation failures are included per item.

## Constraints

Do not decide whether an event occurred and do not modify event confidence. This skill packages already available video/event evidence.
