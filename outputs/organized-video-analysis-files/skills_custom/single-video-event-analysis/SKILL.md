---
name: single-video-event-analysis
description: Analyze one local monitoring video for incidents by extracting timestamped frames and having the LLM review visible evidence. Use for fight, fall, traffic accident, congestion, crowd gathering, smoke/fire, intrusion, illegal occupation, or other abnormal events. This skill is the only custom event-detection entrypoint; YOLO/object detection may be used only as optional object context and must not decide event semantics.
---

# Single Video Event Analysis

## Purpose

Use this skill when the user wants to know what happened in a monitoring video.
Event detection is based on extracted frames and LLM visual review, not YOLO labels,
tracking duration, ROI geometry, or rule templates alone.

## Execution Priority

1. Run `scripts/run.py` to extract frames and build a review manifest.
2. Read the returned `review_manifest_uri` and frame list.
3. Inspect representative frame images in chronological order.
4. Inspect denser frames around suspected moments.
5. Produce the final event analysis JSON from visible evidence.

## Atomic CLI

```bash
python single-video-event-analysis/scripts/run.py \
  --input <input.json> \
  --config <config.json> \
  --output <prepared_manifest.json>
```

Useful fields in `input.json`:

- `video_path`, `file_path`, `raw_segment_uri`, or `video`
- `camera_id`
- `target_events`
- `output_dir`
- `coarse_fps`, `dense_fps`, `scene_threshold`, `dense_window`, `max_frames`

## Review Rules

- Only report events that are visible in the sampled frames.
- Use `unknown` or `requires_review=true` when evidence is blurry, occluded, sparse, or ambiguous.
- Use at least two timestamps for temporal claims unless a single frame clearly shows aftermath, smoke, fire, a crash scene, or a fallen person.
- Do not let YOLO output decide events. YOLO can only provide optional object context such as "person", "car", or "truck".
- Include negative evidence when useful, such as "no visible collision before 00:12".

## Output Schema

Return a JSON-compatible result with:

```json
{
  "video": {},
  "visual_timeline": [
    {
      "time": "00:00:12.000",
      "frame_id": "frame_000012000ms",
      "observation": "Visible subjects, actions, spatial relations, and scene state.",
      "uncertainty": "Optional ambiguity note."
    }
  ],
  "events": [
    {
      "event_id": "EVT_...",
      "event_type": "fight | fall_down | traffic_accident | traffic_congestion | crowd_gathering | smoke_or_fire | intrusion | illegal_occupation | other_abnormal_event | unknown",
      "start_time": "00:00:10.000",
      "end_time": "00:00:18.000",
      "confidence": 0.0,
      "severity": "low | medium | high",
      "evidence_frame_ids": [],
      "reason": "Concrete visible evidence and uncertainty.",
      "requires_review": true
    }
  ],
  "summary": "Short human-readable summary.",
  "warnings": []
}
```

## Constraints

- Do not call legacy rule-based event skills. They have been removed from this bundle.
- Do not infer off-camera causes.
- Do not fabricate exact counts when the frame only supports a range.
