---
name: analyze_spatiotemporal_accessibility
description: >
  Use this skill when the user asks to compute isochrones (reachable areas within X minutes), travel-time accessibility, or radiation range from a starting region. Builds a directed graph from historical OD travel times and runs Dijkstra to find cells reachable within budget minutes.
metadata:
  short-description: 基于历史 OD 耗时矩阵跑 Dijkstra，多 budget 切等时圈。
---

# analyze_spatiotemporal_accessibility

经验等时圈空间可达性。用户提到 等时圈/可达范围/isochrone/辐射范围 时调用。

## Command

```bash
cd /mnt/skills/custom/analyze_spatiotemporal_accessibility
python3 scripts/analyze_spatiotemporal_accessibility.py --od-matrix od_matrix.jsonl --output-dir /path/to/output --budgets-min 15,30,60
```

## Input

od_matrix.jsonl (来自 analyze_od_flow)

## Optional Flags

```bash
--origin-geohash str  # 起点 geohash（必填）
--budgets-min str  # 时间预算列表（分钟），逗号分隔
--min-flow int  # OD 边的最小流量阈值
```

## Outputs

- `isochrone.json`
- `reachable_cells.jsonl`
- `summary.json`

## Algorithm Note

详见 `references/usage.md` 与 `_trajectory_common_v2/trajectory_tasks.py::analyze_spatiotemporal_accessibility`。

## Next Skills

- `$fuse_spatial_evidence`

## 输出已包含业务地名（自包含地名映射，无外部依赖）

每条输出记录都自动注入：
- `landmark`（如 "陆家嘴金融区"）
- `district`（如 "浦东新区"）
- `lat` / `lon`（中心点经纬度）

走的是同一份 `_trajectory_common_v2/geohash_landmarks.json` 映射表（4 城共 14 个 geohash）。
报告写作时**优先用 landmark 而不是 geohash**。

## 串调地图热力图（可选）

输出目录里的 `viz_input.jsonl` 是 search_results 兼容格式，可以直接喂给
`_trajectory_common_v2/render_heatmap.py` 出 PNG：

```bash
python3 ../_trajectory_common_v2/render_heatmap.py \
  --input <输出目录>/viz_input.jsonl \
  --output <输出目录>/heatmap.png \
  --title "<场景描述>"
```

