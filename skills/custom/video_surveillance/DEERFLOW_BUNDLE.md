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

Mount or copy this folder so that its contents are available at:

```text
/mnt/skills/custom
```

Do not place the whole folder under a nested bundle directory unless DeerFlow is configured to discover nested skills.

## Runtime Dependencies

Install Python dependencies in the DeerFlow execution environment:

```bash
pip install -r /mnt/skills/custom/requirements.txt
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
python /mnt/skills/custom/single-video-event-analysis/scripts/run.py \
  --input /mnt/data/video-monitoring-runs/run_001/single_video_analysis_input.json \
  --config /mnt/skills/custom/configs/deerflow_config.json \
  --output /mnt/data/video-monitoring-runs/run_001/prepared_manifest.json
```

The script extracts frames and creates `review_manifest.json`. The Agent must then inspect the listed frames and produce the final event timeline and event candidates from visible evidence.

## Object Detection Call

```bash
python /mnt/skills/custom/object-detection/scripts/run.py \
  --input /mnt/data/video-monitoring-runs/run_001/object_detection_input.json \
  --config /mnt/skills/custom/configs/deerflow_config.json \
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

The video library skills use `ES_URL`, `ES_USERNAME`, and `ES_PASSWORD` from the sandbox environment. The default index is `citybrain-video-library`; precomputed vectors may be stored in `vector`, but this bundle does not generate embeddings automatically.

For personal semantic search, use `video-embedding-index` to write vectors into `huangxiao-video-library-vector-v1`. This keeps shared ES service access separate from per-user index ownership.
