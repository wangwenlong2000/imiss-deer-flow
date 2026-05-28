---
name: mine_trajectory_patterns
description: Use this skill when the user asks to discover frequent travel patterns, recurring commuting sequences, repeated geohash chains, or multi-stop trip patterns from staypoints.
metadata:
  short-description: Mine frequent trajectory grid sequences and transitions.
---

# mine_trajectory_patterns

Use this Skill to extract repeated grid sequences from `staypoints.jsonl`.

## Command

```bash
cd /mnt/skills/custom/mine_trajectory_patterns
python3 scripts/mine_trajectory_patterns.py \
  --input /path/to/staypoints.jsonl \
  --output-dir /path/to/output \
  --max-length 4 \
  --min-support 0.05
```

## Outputs

- `top_patterns.jsonl`
- `patterns_L2.jsonl`
- `patterns_L3.jsonl`
- `patterns_L4.jsonl`
- `summary.json`

## Algorithm

- Per-user staypoint sequence construction
- Consecutive duplicate compression
- PrefixSpan-style contiguous sequence mining
- Support-ratio ranking
- Business landmark labels for each pattern

## Next Skills

Use frequent patterns with `$predict_next_location`, `$profile_urban_region`, or report generation.
