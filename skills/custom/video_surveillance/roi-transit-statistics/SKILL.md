---
name: roi-transit-statistics
description: Count how many objects entered, left, and dwelled in each camera ROI, based on tracks and ROI polygons. Use when the user asks to mark areas and count traffic through them — for example "标出门口和停车区，分别统计进入、离开和停留的目标数量" or "统计这个区域有多少人停留超过 30 秒". Works on a single local video's tracks and does not need Elasticsearch, unlike object-statistics. This skill only counts geometry over time; it never decides whether an event such as intrusion, loitering, or congestion occurred.
---

# ROI Transit Statistics

## Purpose

Turn tracks plus ROI polygons into per-area counts: how many objects entered, how
many left, and how many stayed longer than a dwell threshold.

`roi-mapping` answers "is this object inside the polygon" for a single moment.
This skill aggregates that over time into enter/leave/dwell counts.

## Boundary

This skill is object analytics, not event detection. It reports counts and dwell
seconds. It must not label a result as intrusion, loitering, congestion, illegal
occupation, or any other business event — that judgment belongs to
`single-video-event-analysis`, which reviews visible frame evidence.

## Choosing between this and object-statistics

| Request | Skill |
| --- | --- |
| Single local video, count by area | `roi-transit-statistics` |
| Video library / cross-camera / time-range trend | `object-statistics` (needs Elasticsearch) |

## Execution Priority

Run this skill's `scripts/run.py` first. Only write custom code if the script
cannot cover the request.

Do not edit this skill's files during business execution. If the script fails,
report the concrete failure and stop unless the user explicitly asked for debugging.

## Input Resolution

This skill consumes `tracks`, not raw video. If tracks are not available yet:

1. Run `video-object-analytics` (or `frame-sampling` → `object-detection` →
   `object-tracking`) to produce tracks.
2. Then run this skill on the resulting tracks JSON.

ROI polygons are resolved in this order: `--rois-json`, then `input.rois`, then
`config.cameras.<camera_id>.rois`.

## Atomic CLI

```bash
python roi-transit-statistics/scripts/run.py \
  --tracks-json <tracks.json> \
  --rois-json <rois.json> \
  --camera-id CAM_001 \
  --dwell-seconds 3 \
  --output <roi_stats.json>
```

`<rois.json>`:

```json
{
  "rois": [
    {"id": "ROI_GATE", "name": "门口", "type": "gate", "polygon": [[0, 0], [200, 0], [200, 200], [0, 200]]},
    {"id": "ROI_PARK", "name": "停车区", "type": "parking", "polygon": [[400, 400], [900, 400], [900, 900], [400, 900]]}
  ]
}
```

Parameters: `--tracks-json`, `--rois-json`, `--camera-id`, `--dwell-seconds`,
`--input`, `--config`, `--output`.

## Workflow

1. Load tracks and ROI polygons.
2. For every track, walk its trajectory and test each point against each polygon.
3. Count outside→inside transitions as `entered` and inside→outside as `left`.
4. Accumulate time inside; a track counts as `dwelled` when its longest continuous
   time inside reaches the dwell threshold.
5. Return per-ROI counts, per-label breakdown, and per-track detail.

## Outputs

Per ROI: `roi_id`, `roi_name`, `roi_type`, `entered`, `left`, `dwelled`,
`unique_objects`, `by_label`, and a `tracks` list with `track_id`, `label`,
`entered`, `left`, `dwell_seconds`, `longest_continuous_dwell_seconds`,
`first_enter_time`, `last_leave_time`, `started_inside`, `ended_inside`.

Top level also returns `camera_id`, `dwell_threshold_seconds`, `tracks_analyzed`,
`totals`, and `dwell_seconds_is_approximate`.

## Accuracy Note

`object-tracking` emits `trajectory` as bare `[[x, y], ...]` points with no
per-point timestamps, so elapsed time is interpolated evenly between `start_time`
and `end_time`. Dwell seconds are therefore approximate at the resolution of the
frame-sampling interval, and the output always carries
`dwell_seconds_is_approximate: true`. Report dwell numbers as approximate; do not
present them as exact timing.

## Failure Modes

- `MISSING_TRACKS` — no tracks supplied; run object tracking first
- `MISSING_ROIS` — no ROI polygons for the camera
- `INVALID_ROI_POLYGON` — a polygon has fewer than 3 points
- `INPUT_NOT_FOUND` / `INPUT_UNREADABLE` / `INPUT_INVALID_JSON` / `INPUT_INVALID_YAML`

## Constraints

Keep the output explainable: counts and geometry only, no event conclusions.
