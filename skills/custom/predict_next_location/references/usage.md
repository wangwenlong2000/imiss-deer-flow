# predict_next_location Usage

下一目的地预测 — 马尔可夫链下一位置预测（可选时段条件）。

## Inputs

staypoints.jsonl

## Outputs

- `transition_matrix.jsonl`
- `predictions.jsonl`
- `summary.json`

## Pipeline Position

This skill is part of the Spatiotemporal 时空轨迹分析流水线.
Upstream: extract_stay_points / clean_trajectory / _trajectory_common_v2
Downstream: analyze_spatiotemporal_accessibility
