---
name: extract_stay_points
description: Use this skill when the user wants to extract stay points, dwell events, OD endpoints, or meaningful stops from GPS, check-in, signaling, taxi, or cleaned trajectory data.
metadata:
  short-description: Extract stay points from trajectory records.
---

# extract_stay_points

Use this Skill to detect places where a user stayed within a spatial radius for a minimum dwell time.

## Command

```bash
cd /mnt/skills/custom/spatiotemporal_trajectory/extract_stay_points
python3 scripts/extract_stay_points.py --input /path/to/raw_or_cleaned.csv --output-dir /mnt/user-data/outputs/staypoints
```

Useful flags:

```bash
--stay-radius-m 200 --stay-min-minutes 20 --max-speed-kmh 200 --geohash-precision 6
```

## Outputs

- `staypoints.jsonl`
- `staypoints_detail.csv` — the same stay records as CSV, including a header when no stay qualifies
- `summary.json`

## Algorithm

- Reuses trajectory cleaning
- Sliding distance-radius stay detection
- Dwell-time threshold filtering
- The minimum duration is inclusive: exactly 20 minutes meets a 20-minute threshold.
- A qualifying stay at the end of a trajectory is included without requiring a departure point.
- Stay centroid calculation
- Geohash encoding

Read `summary.json` and `staypoints.jsonl` to verify counts before reporting. Present
the generated files with `present_files`. Skill files are read-only; do not edit
them during a business request. If execution fails, report the specific error
rather than repeatedly debugging or installing packages.

When the user requests CSV detail, present `summary.outputs.staypoints_csv`.
A per-user count CSV is a separate summary and does not replace stay-event detail.
The runner already writes this CSV; do not add a second conversion step.

When asked for counts per user, report `summary.json.staypoints_by_user`, which
includes users with zero stays. Do not group only the positive stay records and
silently omit users who did not meet the duration threshold.

## Next Skills

Use `staypoints.jsonl` for OD endpoint reasoning, stay-duration anomaly checks, or urban region profiling.
