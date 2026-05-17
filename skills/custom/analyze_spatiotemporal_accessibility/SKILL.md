---
name: analyze_spatiotemporal_accessibility
description: Use this skill when the user asks to compute trajectory-based isochrones, reachable areas, travel-time accessibility, radiation range, or real mobility coverage from a starting geohash.
metadata:
  short-description: 基于历史 OD 耗时矩阵计算轨迹等时圈与空间可达范围。
---

# analyze_spatiotemporal_accessibility

Use this Skill after `$analyze_od_flow` to compute empirical isochrones from an OD travel-time graph.

## Command

```bash
cd /mnt/skills/custom/analyze_spatiotemporal_accessibility
python3 scripts/analyze_spatiotemporal_accessibility.py --od-matrix /path/to/od_matrix.jsonl --output-dir /path/to/output --origin-geohash wtw3s --budgets-min 15,30,60
```

## Outputs

- `isochrone.json`
- `reachable_cells.jsonl`
- `viz_input.jsonl`
- `summary.json`

## Algorithm

- Directed graph construction from OD matrix
- Minimum-flow filtering
- Dijkstra shortest travel-time search
- Reachable geohash cells by time budget
- Landmark-enriched isochrone evidence

## Next Skills

Use `viz_input.jsonl` with heatmap rendering or `$fuse_spatial_evidence`.
