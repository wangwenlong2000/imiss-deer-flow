# classify_trajectory_state Usage

运动状态分类 — 把每个用户分类为：长时静止/定向迁徙/高频巡游/随机游走。

## Inputs

cleaned_points.jsonl 或 staypoints.jsonl

## Outputs

- `state_classification.jsonl`
- `summary.json`

## Pipeline Position

This skill is part of the Spatiotemporal 时空轨迹分析流水线.
Upstream: extract_stay_points / clean_trajectory / _trajectory_common_v2
Downstream: mine_trajectory_patterns, profile_urban_region
