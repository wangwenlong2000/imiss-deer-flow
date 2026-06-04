# analyze_spatiotemporal_accessibility Usage

经验等时圈空间可达性 — 基于历史 OD 耗时矩阵跑 Dijkstra，多 budget 切等时圈。

## Inputs

od_matrix.jsonl (来自 analyze_od_flow)

## Outputs

- `isochrone.json`
- `reachable_cells.jsonl`
- `summary.json`

## Pipeline Position

This skill is part of the Spatiotemporal 时空轨迹分析流水线.
Upstream: extract_stay_points / clean_trajectory / _trajectory_common_v2
Downstream: fuse_spatial_evidence
