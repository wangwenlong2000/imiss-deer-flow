---
name: single-video-event-analysis
description: Analyze one local monitoring video for incidents after city-video-intelligence has routed the request, by extracting timestamped frames and having the LLM review visible evidence. Use for fight, fall, traffic accident, congestion, crowd gathering, smoke/fire, intrusion, illegal occupation, or other abnormal events. For direct user monitoring-video event questions, load city-video-intelligence first. This skill is the only custom event-detection entrypoint; YOLO/object detection may be used only as optional object context and must not decide event semantics.
---

# Single Video Event Analysis

## Purpose

Use this skill when the user wants to know what happened in a monitoring video.
Event detection is based on extracted frames and LLM visual review, not YOLO labels,
tracking duration, ROI geometry, or rule templates alone.

## Business Orchestration Gate

If this skill is loaded for a direct user monitoring-video request and no `city-video-intelligence` plan has already been executed in the current turn, load `city-video-intelligence` first and run its router script with the raw user request. Continue here only when that plan recommends `single-video-event-analysis`.

Do not edit this skill's files or scripts during business execution. If the script fails, report the concrete failure and stop unless the user explicitly asked for debugging or implementation.

## Execution Priority

1. Use `write_file` to create a small input JSON file, then run `scripts/run.py` with `--input <file>`. Do not use shell heredocs or pass an inline JSON string to `--input`.
2. Read the returned `review_manifest_uri` and use the listed `frames` as the review set.
3. Inspect only the listed review frames in chronological order. Default maximum is 8 images.
4. Read `all_frames_uri` or inspect denser frames only if a reviewed frame visibly suggests a possible event or is too ambiguous to classify.
5. Produce the final event analysis from visible evidence and stop. Do not keep browsing frames after the conclusion is clear.

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

For ordinary business questions, omit `max_frames` so the script default samples across the whole video and exposes only 8 review frames. Do not use `max_frames` as the review-image limit. Do not rerun the script after a successful result.

## Review Rules

- Only report events that are visible in the sampled frames.
- Use `unknown` or `requires_review=true` when evidence is blurry, occluded, sparse, or ambiguous.
- Use at least two timestamps for temporal claims unless a single frame clearly shows aftermath, smoke, fire, a crash scene, or a fallen person.
- Do not let YOLO output decide events. YOLO can only provide optional object context such as "person", "car", or "truck".
- Include negative evidence when useful, such as "no visible collision before 00:12".
- Never include internal thinking markers such as `<think>` or `</think>` in the final user-visible response.
- If a draft answer contains hidden reasoning before a `</think>` token, discard that hidden text and answer only with the event result and visible evidence summary.
- For negative findings, phrase the conclusion as evidence-bound, such as "未在抽样审阅帧中发现交通事故迹象", unless the reviewed evidence covers the full incident window.

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
