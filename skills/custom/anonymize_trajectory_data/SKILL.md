---
name: anonymize_trajectory_data
description: >
  Use this skill when the user wants to anonymize / de-identify raw trajectory data before sharing or analysis: pseudonymize user ids, generalize fine-grained locations to coarser geohash cells, and bucket timestamps. Produces a privacy-preserving JSONL plus a summary with k-anonymity style coverage stats.
metadata:
  short-description: 轨迹脱敏：用户ID哈希化、地理粗化（geohash截断）、时间分桶。
---

# anonymize_trajectory_data

轨迹数据脱敏。用户提到 脱敏/匿名化/去标识/隐私保护/k-匿名 时调用。

## Command

```bash
cd /mnt/skills/custom/anonymize_trajectory_data
python3 scripts/anonymize_trajectory_data.py --input <file> --output-dir /path/to/output
```

## Input

cleaned_points.jsonl 或 staypoints.jsonl（任意含 user/经纬度或geohash/时间 的轨迹文件）

## Optional Flags

```bash
--coarse-precision int  # 脱敏后保留的 geohash 位数，默认 5（越小越粗，隐私越强）
--time-bucket str       # 时间分桶：hour | day，默认 hour
--salt str              # 用户ID哈希盐值，默认 spatiotemporal_v1
--drop-latlon           # 丢弃精确经纬度，只保留粗化 geohash（默认开启）
```

## Outputs

- `anonymized.jsonl`
- `summary.json`

## Algorithm Note

- 用户ID：`hash_uid(salt + user_id)` → 12 位 sha256 摘要（不可逆伪名）。
- 空间泛化：把 geohash 截断到 `--coarse-precision` 位；默认丢弃精确 lat/lon。
- 时间泛化：时间戳按 hour/day 分桶。
- summary 给出脱敏后每个粗化网格的记录数与最小桶大小（k 值），便于评估再识别风险。

详见 `references/usage.md` 与 `_trajectory_common_v2/trajectory_tasks.py`（复用 `hash_uid`/`read_records`/`normalize_record`）。

## Next Skills

- `$map_spatial_grid`
- `$analyze_od_flow`
