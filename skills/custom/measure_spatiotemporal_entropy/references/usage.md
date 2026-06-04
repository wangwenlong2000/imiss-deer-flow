# measure_spatiotemporal_entropy Usage

时空分布熵 / 活力指数 — 香农熵（来源/去向/时段三维）+ 综合活力指数。

## Inputs

od_matrix.jsonl + evidence.jsonl（任一或两者）

## Outputs

- `entropy_by_region.jsonl`
- `summary.json`

## Pipeline Position

This skill is part of the Spatiotemporal 时空轨迹分析流水线.
Upstream: extract_stay_points / clean_trajectory / _trajectory_common_v2
Downstream: profile_urban_region, fuse_spatial_evidence
