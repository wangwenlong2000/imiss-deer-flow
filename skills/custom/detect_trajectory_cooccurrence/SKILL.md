---
name: detect_trajectory_cooccurrence
description: >
  Use this skill when the user asks to detect groups of users co-occurring in same time-space windows, identify suspicious gathering events, or perform contact tracing / trajectory companion mining. Two users are flagged as co-occurring if they overlap in the same geohash cell with overlap >= min_overlap_min.
metadata:
  short-description: 用户两两在同 geohash 网格 + 时间窗重叠的伴行检测。
---

# detect_trajectory_cooccurrence

多目标时空伴行检测。用户提到 伴行/同行/聚集/接触追踪/co-occurrence 时调用。

## Command

```bash
cd /mnt/skills/custom/detect_trajectory_cooccurrence
python3 scripts/detect_trajectory_cooccurrence.py --input <file> --output-dir /path/to/output --geohash-precision 6 --min-overlap-min 15.0
```

## Input

staypoints.jsonl

## Optional Flags

```bash
--geohash-precision int  # see references/usage.md
--min-overlap-min float  # 最小时间重叠（分钟）
--target-users str  # 只查这些用户（逗号分隔），空则全两两
--max-pairs int  # 最大对数限制
```

## Outputs

- `cooccurrence_pairs.jsonl`
- `cooccurrence_events.jsonl`
- `summary.json`

## Algorithm Note

详见 `references/usage.md` 与 `_trajectory_common_v2/trajectory_tasks.py::detect_trajectory_cooccurrence`。

## Next Skills

- `$analyze_event_impact`

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

