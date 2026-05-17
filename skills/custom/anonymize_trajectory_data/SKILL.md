---
name: anonymize_trajectory_data
description: Use this skill when the user asks to anonymize, desensitize, privacy-protect, differentially perturb, k-anonymize, hash user IDs, grid-generalize, or prepare sensitive GPS/check-in trajectory data before RAG or LLM analysis.
metadata:
  short-description: 对敏感时空轨迹做用户哈希、坐标加噪、时间桶化、网格泛化与 K 匿名过滤。
---

# anonymize_trajectory_data

Use this Skill before exposing sensitive trajectory evidence to retrieval or report generation.

## Command

```bash
cd /mnt/skills/custom/anonymize_trajectory_data
python3 scripts/anonymize_trajectory_data.py --input /path/to/raw_trajectory.csv --output-dir /path/to/output --epsilon 1.0 --geohash-precision 5 --min-users 5
```

## Input

CSV with user, latitude, longitude, timestamp, and optional category columns.

Useful flags:

```bash
--user-col user_id --lat-col latitude --lon-col longitude --time-col timestamp --cat-col category
--city-label Shanghai --epsilon 1.0 --geohash-precision 5 --min-users 5 --salt citybench_v1 --seed 42
```

## Outputs

- `evidence.jsonl`
- `privacy_report.json`

## Algorithm

- Irreversible user-id hashing
- Laplace coordinate noise for differential privacy
- Time bucketing and geohash grid generalization
- K-anonymity filtering for sparse cells
- Privacy provenance fields for RAG evidence

## Next Skills

Use `evidence.jsonl` with `$citybench-rag-search`, `$search_spatiotemporal`, `$analyze_region_heat`, or `$fuse_spatial_evidence`.
