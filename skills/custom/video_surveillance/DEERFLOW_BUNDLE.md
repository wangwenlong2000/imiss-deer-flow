# DeerFlow Video Monitoring Bundle

This folder is designed to be used as the DeerFlow custom skills root.

## What This Folder Contains

- `single-video-event-analysis`: the unified LLM frame-review entrypoint for incident analysis
- `object-detection`: YOLO object detection only
- Video utility skills for ingestion, frame sampling, FFmpeg extraction, evidence snapshots, masking, review routing, and duplicate merging
- Video library skills for Elasticsearch-backed batch ingestion, search, object statistics, and evidence package generation
- Personal video-vector indexing with `video-embedding-index`
- YOLO model weights in `models/yolov8n.pt`
- DeerFlow-oriented config in `configs/deerflow_config.json`
- Python dependency list in `requirements.txt`

## Recommended DeerFlow Layout

The repository `skills/` directory is mounted at `/mnt/skills`, so this bundle is discovered as a nested directory at:

```text
/mnt/skills/custom/video_surveillance
```

Every skill in this bundle therefore lives at `/mnt/skills/custom/video_surveillance/<skill-id>/`. Use that full prefix in all commands; paths of the form `/mnt/skills/custom/<skill-id>/` do not exist.

The one exception is the YOLO weight file: `configs/deerflow_config.json` points at `/mnt/skills/custom/models/yolov8n.pt` (a copy also exists inside this bundle at `models/yolov8n.pt`).

## Runtime Dependencies

Install Python dependencies in the DeerFlow execution environment:

```bash
pip install -r /mnt/skills/custom/video_surveillance/requirements.txt
```

System tools required for video extraction and clipping:

```bash
ffmpeg
ffprobe
```

## Model Weight Configuration

The config uses:

```json
"model_path": "/mnt/skills/custom/models/yolov8n.pt"
```

YOLO is used only by `object-detection`; it must not be treated as an event detector.

## Main Event Analysis Call

```bash
python /mnt/skills/custom/video_surveillance/single-video-event-analysis/scripts/run.py \
  --input /mnt/data/video-monitoring-runs/run_001/single_video_analysis_input.json \
  --config /mnt/skills/custom/video_surveillance/configs/deerflow_config.json \
  --output /mnt/data/video-monitoring-runs/run_001/prepared_manifest.json
```

The script extracts frames and creates `review_manifest.json`. The Agent must then inspect the listed frames and produce the final event timeline and event candidates from visible evidence.

## Object Detection Call

```bash
python /mnt/skills/custom/video_surveillance/object-detection/scripts/run.py \
  --input /mnt/data/video-monitoring-runs/run_001/object_detection_input.json \
  --config /mnt/skills/custom/video_surveillance/configs/deerflow_config.json \
  --output /mnt/data/video-monitoring-runs/run_001/object_detection_result.json
```

## Current Composition Pattern

```text
single-video-event-analysis
  -> evidence-snapshot
  -> video-segment-extraction
  -> privacy-masking
  -> human-review-routing
  -> duplicate-event-merge
```

Optional object analytics:

```text
frame-sampling
  -> object-detection
  -> object-tracking
  -> roi-mapping
```

Video library workflow:

```text
batch-video-ingestion
  -> video-search
  -> object-statistics
  -> evidence-package-generation
```

The video library skills use `ES_URL`, `ES_USERNAME`, and `ES_PASSWORD` from the sandbox environment. The unified index is `citybrain-video-library`; source metadata, embedding lifecycle fields, and the StreetModel video vector are stored in the same document.

Use `video-embedding-index --storage-mode in_place` to enrich indexed source documents through partial ES updates. All video Elasticsearch tools reject index names other than `citybrain-video-library`.
