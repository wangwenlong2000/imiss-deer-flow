---
name: detect_trajectory_cooccurrence
description: Use this skill when the user asks to detect co-occurrence, companion trajectories, contact tracing, crowd gathering, group convergence, or multiple users appearing in the same space-time window.
metadata:
  short-description: 检测多目标轨迹在同一时空窗口内的伴行、交汇或聚集。
---

# detect_trajectory_cooccurrence

Use this Skill to find user pairs or groups sharing the same geohash cell with overlapping dwell time.

## Command

```bash
cd /mnt/skills/custom/detect_trajectory_cooccurrence
python3 scripts/detect_trajectory_cooccurrence.py --input /path/to/staypoints.jsonl --output-dir /path/to/output --geohash-precision 5 --min-overlap-min 15
```

Useful flags:

```bash
--target-users u1,u2 --max-pairs 50000
```

## Outputs

- `cooccurrence_pairs.jsonl`
- `cooccurrence_events.jsonl`
- `summary.json`

## Next Skills

Use co-occurrence events with `$analyze_event_impact` or `$fuse_spatial_evidence`.
