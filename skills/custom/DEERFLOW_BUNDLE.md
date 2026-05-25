# DeerFlow Video Monitoring Bundle

## What This Folder Contains

This folder is designed to be used as the DeerFlow custom skills root.

It contains:

- Independent Skill directories, each with its own `SKILL.md` and standalone script entrypoint
- No shared `src/`, `tools/`, or registry module is required for the atomic scripts
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

`analyze-video` uses `scripts/extract_frames.py` as its entrypoint and supports
the same `--input` JSON pattern.

Examples:

```bash
python /mnt/skills/custom/object-detection/scripts/run.py \
  --input /mnt/data/video-monitoring-runs/run_001/object_detection_input.json \
  --config /mnt/skills/custom/configs/deerflow_config.json \
  --output /mnt/data/video-monitoring-runs/run_001/object_detection_result.json
```

## Composition Pattern

Agent can compose Skills in this order:

```text
frame-sampling
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
