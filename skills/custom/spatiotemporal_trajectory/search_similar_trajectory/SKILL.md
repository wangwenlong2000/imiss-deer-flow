---
name: search_similar_trajectory
description: Use this skill when the user wants to find historical trips or trajectory records similar to a target trip based on start/end position, travel distance, and duration.
metadata:
  short-description: Search similar trips from trajectory trip records.
---

# search_similar_trajectory

Use this Skill after trip segmentation to retrieve trips similar to a target trip.

## Command

```bash
cd /mnt/skills/custom/search_similar_trajectory
python3 scripts/search_similar_trajectory.py \
  --input /path/to/trips.jsonl \
  --output-dir /path/to/output \
  --target-trip-id trip_000001 \
  --top-k 10
```

If `--target-trip-id` is omitted, the first valid trip is used as the target.

## Outputs

- `similar_trajectories.jsonl`
- `summary.json`

## Algorithm

- Trip feature vectorization
- Start/end coordinate comparison
- Distance and duration comparison
- Standardized Euclidean nearest-neighbor ranking

## Next Skills

Use similar trips for route anomaly explanation, commute group comparison, or historical case retrieval.
