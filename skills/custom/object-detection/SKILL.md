---
name: object-detection
description: Detect objects in sampled video frames and return standard Detection schema. Use when Codex has frames and labels and needs real YOLO or mock detections with labels, confidences, and bounding boxes. This skill exposes ultralytics YOLO, OpenCV DNN ONNX YOLO, and deterministic mock backends; use it before tracking, ROI mapping, and event rules.
---

# Object Detection

## Execution Priority

Prioritize calling this skill's `scripts/run.py` entrypoint first. Only write custom code after the script result is unsuitable or the script cannot cover the requested task.

## Input Resolution

If `frames` are missing but the user provided a video path or upload, do not ask the user to provide frames and do not write detection or frame extraction code. First call `video-stream-ingestion/scripts/run.py` if needed, then `frame-sampling/scripts/run.py` to generate `frames`, then call this skill. If `labels` are missing, infer common labels from the task when safe, such as `person` for crowd, fight, behavior, or pedestrian analysis; otherwise ask only for target labels.

## Workflow

1. Read frames and target labels.
2. Load the configured lightweight detector or accept deterministic mock detections.
3. Map model labels to internal labels.
4. Apply per-label confidence thresholds.
5. Return frame-level Detection records.

## Available Implementation

- Registry name: `object-detection`
- Python class: `src.skills.vision_processing.object_detection.ObjectDetectionSkill`
- Production backend 1: `ultralytics.YOLO`
- Production backend 2: OpenCV DNN with YOLOv8 ONNX
- Mock backend: configured `context.config["mock_detections"]`

## Standalone CLI

This Skill can be executed independently through the shared runner:

```bash
python object-detection/scripts/run.py \
  --input <input.json> \
  --config <config.json> \
  --output <result.json>
```

This directory owns registry skill `object-detection`, so the agent should call this script directly for this skill.

## Tool Invocation

Registry call:

```python
registry.get("object-detection").run({"frames": frames, "labels": labels}, context)
```

Production command:

```bash
python object-detection/scripts/run.py \
  --input <input.json> \
  --config <config.json> \
  --output <result.json>
```

Configuration:

```yaml
models:
  object_detection:
    provider: ultralytics
    name: yolov8n.pt
    default_threshold: 0.25
    thresholds:
      person: 0.35
      car: 0.35
```

For ONNX:

```yaml
models:
  object_detection:
    provider: opencv_dnn
    model_path: /path/to/yolov8n.onnx
```

## Inputs

- `frames`
- `labels`
- Optional `detections` to pass through precomputed results

## Outputs

Returns `detections`, each with `frame_id`, `timestamp`, and `objects` containing `object_id`, `label`, `confidence`, `bbox`, and optional `model_label`.

## Failure Modes

- `YOLO_DEPENDENCY_MISSING`
- `YOLO_MODEL_LOAD_FAILED`
- `YOLO_MODEL_NOT_FOUND`
- `YOLO_INFERENCE_FAILED`
- `FRAME_NOT_FOUND`

## Constraints

Do not infer business events, track identities, or create evidence.
