# DeerFlow Video Monitoring Bundle

## What This Folder Contains

This folder is designed to be used as the DeerFlow custom skills root.

It contains:

- 17 independent Skill directories, each with its own `SKILL.md` and `scripts/run.py`
- Shared implementation code in `src/`
- Shared runner helpers in `tools/`
- YOLO model weights in `models/yolov8n.pt`
- DeerFlow-oriented config in `configs/deerflow_config.json`
- Python dependency list in `requirements.txt`

## Recommended DeerFlow Layout

Mount or copy this folder so that its contents are available at:

```text
/mnt/skills/custom
```

The directory should look like:

```text
/mnt/skills/custom/
  object-detection/
    SKILL.md
    scripts/run.py
  frame-sampling/
    SKILL.md
    scripts/run.py
  ...
  src/
  tools/
  models/yolov8n.pt
  configs/deerflow_config.json
```

Do not place the whole folder under `/mnt/skills/custom/deerflow_video_monitoring_bundle` unless DeerFlow is configured to discover nested skills. The Skill directories should be directly under the custom skills root.

## Runtime Dependencies

Install Python dependencies in the DeerFlow execution environment:

```bash
pip install -r /mnt/skills/custom/requirements.txt
```

System tools required:

```bash
ffmpeg
ffprobe
```

## Model Weight Configuration

The config uses:

```json
"model_path": "/mnt/skills/custom/models/yolov8n.pt"
```

If you mount the bundle elsewhere, update `configs/deerflow_config.json`.

## Video Input Configuration

The default config expects videos at:

```text
/mnt/data/videos/
```

Example:

```text
/mnt/data/videos/Trafic.mp4
```

Each Skill input JSON should pass the concrete video path or frame paths it needs.

## Calling Individual Skills

Each Skill has its own standalone entrypoint:

```bash
python /mnt/skills/custom/<skill-name>/scripts/run.py \
  --input <input.json> \
  --config /mnt/skills/custom/configs/deerflow_config.json \
  --output <result.json>
```

Examples:

```bash
python /mnt/skills/custom/video-stream-ingestion/scripts/run.py \
  --input /mnt/data/video-monitoring-runs/run_001/video_ingestion_input.json \
  --config /mnt/skills/custom/configs/deerflow_config.json \
  --output /mnt/data/video-monitoring-runs/run_001/video_ingestion_result.json
```

```bash
python /mnt/skills/custom/object-detection/scripts/run.py \
  --input /mnt/data/video-monitoring-runs/run_001/object_detection_input.json \
  --config /mnt/skills/custom/configs/deerflow_config.json \
  --output /mnt/data/video-monitoring-runs/run_001/object_detection_result.json
```

## Composition Pattern

Agent can compose Skills in this order:

```text
video-stream-ingestion
  -> frame-sampling
  -> camera-health-check
  -> object-detection
  -> object-tracking
  -> roi-mapping
  -> event-rule-engine
  -> duplicate-event-merge
  -> evidence-snapshot
  -> video-segment-extraction
  -> human-review-routing
```

The output JSON of one Skill should be transformed into the input JSON of the next Skill by the agent or by a future workflow adapter.

