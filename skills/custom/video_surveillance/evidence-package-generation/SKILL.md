---
name: evidence-package-generation
description: Generate an evidence package manifest for indexed monitoring videos or event/search results, including snapshot evidence and optional short video clips. Use when Codex needs a package_id, manifest URI, evidence items, hashes, and reusable evidence artifacts.
---

# Evidence Package Generation

## Execution Priority

Prioritize calling this skill's `scripts/run.py` entrypoint first. For local video path plus timestamp/range evidence requests, run it once, then stop. Only write custom code after the script result is unsuitable or the script cannot cover the requested task.

## Input Resolution

Use `--event-json` when the user provides an event. Use `--search-result-json` when the user wants evidence for search hits. Use `--video-id` to retrieve a source video document from Elasticsearch.

If the user provides a local video path plus an event timestamp or time range, do not ask which evidence package variant is needed and do not call `video-segment-extraction` separately first. Run this skill once with direct CLI fields such as `--raw-segment-uri`, `--event-elapsed-seconds`, `--pre-seconds`, and `--post-seconds`. Convert `HH:MM:SS` or `MM:SS` video offsets to elapsed seconds. This skill packages clip/snapshot artifacts when available and SHA-256 integrity hashes. After this skill returns success, summarize `package_id`, `manifest_uri`, `manifest_hash`, and `artifact_summary`, then stop.

## Atomic CLI

```bash
python evidence-package-generation/scripts/run.py --video-id <video_id> --event-json <event.json> --output-dir <package_dir> --index citybrain-video-library --config <config.json> --output <result.json>
```

Local video evidence package:

```bash
python evidence-package-generation/scripts/run.py \
  --raw-segment-uri /mnt/datasets/Vedio-demo/Trafic.mp4 \
  --event-elapsed-seconds 1 \
  --event-type manual_evidence \
  --pre-seconds 2 \
  --post-seconds 2 \
  --output-dir /mnt/user-data/outputs/evidence-package \
  --output /mnt/user-data/outputs/evidence-package/result.json
```

Parameters: `--input`, `--index`, `--video-id`, `--event-json`, `--search-result-json`, `--raw-segment-uri`, `--event-id`, `--event-type`, `--event-time`, `--event-elapsed-seconds`, `--camera-id`, `--output-dir`, `--pre-seconds`, `--post-seconds`, `--config`, `--output`.

Only `citybrain-video-library` is accepted for Elasticsearch video lookup. Other index values return `UNSUPPORTED_VIDEO_INDEX`.

## Workflow

1. Resolve package items from event JSON, search results, or Elasticsearch video id lookup.
2. Generate snapshot evidence when event frames are available.
3. Generate a short clip when source video and event timing are available.
4. Write a package manifest JSON containing evidence artifacts and integrity hashes.

## Outputs

Returns `package_id`, `manifest_uri`, `manifest_hash`, `items`, `evidence`, and `artifact_summary`. `artifact_summary` contains key artifact paths, statuses, time ranges, and hashes for final reporting.

## Failure Modes

- `MISSING_INPUT`
- `ES_CONNECTION_FAILED`
- `ES_REQUEST_FAILED`
- Nested evidence generation failures are included per item.

## Constraints

Do not decide whether an event occurred and do not modify event confidence. This skill packages already available video/event evidence.

Every field in the package must come from the actual inputs. Camera ID, location, and shooting time are external business records and cannot be recovered from a video file — when they are absent, record them as missing rather than supplying a plausible value. Never copy values from `skills/custom/video_surveillance/examples/` or from any file marked `"_example": true`; those are invented placeholders, and a package carrying them is a fabricated evidence record.

Do not write custom FFmpeg, hashing, or JSON-packaging code for local video evidence requests unless this script returns a concrete unsupported failure. Do not run `ls`, `find`, `sha256sum`, `cat`, `read_file` on generated artifacts, or `present_files` after a successful result. Report the manifest path and key artifact paths from the script output in the final answer.
