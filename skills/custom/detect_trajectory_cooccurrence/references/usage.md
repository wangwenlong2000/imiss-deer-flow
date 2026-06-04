# detect_trajectory_cooccurrence Usage

多目标时空伴行检测 — 用户两两在同 geohash 网格 + 时间窗重叠的伴行检测。

## Inputs

staypoints.jsonl

## Outputs

- `cooccurrence_pairs.jsonl`
- `cooccurrence_events.jsonl`
- `summary.json`

## Pipeline Position

This skill is part of the Spatiotemporal 时空轨迹分析流水线.
Upstream: extract_stay_points / clean_trajectory / _trajectory_common_v2
Downstream: analyze_event_impact
