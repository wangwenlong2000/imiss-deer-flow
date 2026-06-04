---
name: classify_trajectory_state
description: >
  Use this skill when the user wants to classify each user's trajectory into motion states such as stationary, directed migration, frequent circulation, or random walk. Computes features like avg speed, directionality index, spatial span, geohash diversity.
metadata:
  short-description: 把每个用户分类为：长时静止/定向迁徙/高频巡游/随机游走。
---

# classify_trajectory_state

运动状态分类。用户提到 运动状态/出行行为分类/语义识别 时调用。

## Command

```bash
cd /mnt/skills/custom/classify_trajectory_state
python3 scripts/classify_trajectory_state.py --input <file> --output-dir /path/to/output --geohash-precision 6
```

## Input

cleaned_points.jsonl 或 staypoints.jsonl

## Optional Flags

```bash
--geohash-precision int  # see references/usage.md
```

## Outputs

- `state_classification.jsonl`
- `summary.json`

## Algorithm Note

详见 `references/usage.md` 与 `_trajectory_common_v2/trajectory_tasks.py::classify_trajectory_state`。

## Next Skills

- `$mine_trajectory_patterns`
- `$profile_urban_region`

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

