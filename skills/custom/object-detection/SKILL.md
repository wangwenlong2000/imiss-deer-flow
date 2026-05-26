---
name: object-detection
description: Detect visible objects in sampled video frames and return standard Detection schema only. Use when Codex needs YOLO labels, confidences, and bounding boxes for people, vehicles, or COCO objects. Do not use this skill to infer events, abnormal behavior, accidents, fights, falls, congestion, intrusion, smoke/fire, or business semantics.
---

# Object Detection

## Execution Priority

Prioritize calling this skill's `scripts/run.py` entrypoint first. Only write custom code after the script result is unsuitable or the script cannot cover the requested task.

## Input Resolution

If `frames` are missing but the user provided a video path or upload, call `frame-sampling/scripts/run.py` to generate `frames`, then call this skill. If `labels` are missing, use explicit object labels from the user when available; otherwise default to common monitoring labels such as `person,car,bus,truck,motorcycle,bicycle`.


## Atomic CLI

Run this skill directly with its own script. The script does not call other skill scripts and does not depend on shared `src`, `tools`, or registry modules.

```bash
python object-detection/scripts/run.py --frames-json <frames.json> --labels person,car --provider ultralytics --config <config.json> --output <detections.json>
```

Parameters: `--input`, `--frames-json`, `--labels`, `--provider`, `--model-path`, `--config`, `--output`.

## Workflow

1. Read frames and target labels.
2. Load the configured ultralytics YOLO detector for real inference. The mock provider is disabled and must not be used for chain tests.
3. Map model labels to internal labels.
4. Apply per-label confidence thresholds.
5. Return frame-level Detection records.

YOLO is only an object detector in this project. It must not decide whether a fight, fall, accident, congestion, intrusion, illegal occupation, smoke/fire, or any other event occurred.

## Available Implementation

This skill is implemented as an atomic standalone script in its own `scripts/run.py`. The script contains the executable logic for this skill and must not import shared `src`, `tools`, or registry modules.

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
- `MOCK_PROVIDER_DISABLED`

## Constraints

Do not infer business events, abnormal behavior, track identities, ROI occupancy, review routing, or evidence.
