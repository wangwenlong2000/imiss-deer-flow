---
name: classify_trajectory_state
description: Use this skill when the user wants to classify each user's trajectory into motion states such as stationary, directed migration, frequent circulation, random walk, cruising, long-stay, or semantic mobility behavior.
metadata:
  short-description: 识别用户轨迹状态：长时静止、定向迁徙、高频巡游或随机游走。
---

# classify_trajectory_state

Use this Skill on cleaned points or staypoints to infer entity movement state.

## Command

```bash
cd /mnt/skills/custom/classify_trajectory_state
python3 scripts/classify_trajectory_state.py --input /path/to/staypoints.jsonl --output-dir /path/to/output --geohash-precision 5
```

## Outputs

- `state_classification.jsonl`
- `summary.json`

## Algorithm

- Per-user chronological trajectory construction
- Average speed, spatial span, directionality, and geohash diversity features
- Rule-based state classification
- Landmark enrichment when geohash can be mapped

## Next Skills

Use results with `$mine_trajectory_patterns`, `$profile_urban_region`, or `$fuse_spatial_evidence`.
