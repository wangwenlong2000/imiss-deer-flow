---
name: predict_next_location
description: >
  Use this skill when the user asks to predict where a user will go next, given their current location and time. Trains a first-order Markov chain (optionally conditioned on time slot) from historical staypoints.
metadata:
  short-description: 马尔可夫链下一位置预测（可选时段条件）。
---

# predict_next_location

下一目的地预测。用户提到 下一站预测/目的地预测/Markov/next location 时调用。

## Command

```bash
cd /mnt/skills/custom/predict_next_location
python3 scripts/predict_next_location.py --input <file> --output-dir /path/to/output
```

## Input

staypoints.jsonl

## Optional Flags

```bash
--current-geohash str  # 当前位置（不填则只训练矩阵）
--current-time str  # 当前时间 ISO 字符串
--top-k int  # see references/usage.md
--geohash-precision int  # see references/usage.md
--train-only bool  # 只训练矩阵不做预测
```

## Outputs

- `transition_matrix.jsonl`
- `predictions.jsonl`
- `summary.json`

## Algorithm Note

详见 `references/usage.md` 与 `_trajectory_common_v2/trajectory_tasks.py::predict_next_location`。

## Next Skills

- `$analyze_spatiotemporal_accessibility`

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

