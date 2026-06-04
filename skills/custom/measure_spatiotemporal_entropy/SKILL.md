---
name: measure_spatiotemporal_entropy
description: >
  Use this skill when the user asks to quantify region diversity, vitality, or spatiotemporal complexity using Shannon entropy of inflow/outflow/time-slot distributions. Outputs vitality_index = H_avg * log(visit_count) per region.
metadata:
  short-description: 香农熵（来源/去向/时段三维）+ 综合活力指数。
---

# measure_spatiotemporal_entropy

时空分布熵 / 活力指数。用户提到 熵/活力/多样性/复杂度/混合度 时调用。

## Command

```bash
cd /mnt/skills/custom/measure_spatiotemporal_entropy
python3 scripts/measure_spatiotemporal_entropy.py  --output-dir /path/to/output
```

## Input

od_matrix.jsonl + evidence.jsonl（任一或两者）

## Optional Flags

```bash
--od-matrix str  # od_matrix.jsonl
--evidence str  # evidence.jsonl
```

## Outputs

- `entropy_by_region.jsonl`
- `summary.json`

## Algorithm Note

详见 `references/usage.md` 与 `_trajectory_common_v2/trajectory_tasks.py::measure_spatiotemporal_entropy`。

## Next Skills

- `$profile_urban_region`
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

