---
name: measure_spatiotemporal_entropy
description: Use this skill when the user asks to measure spatiotemporal entropy, mobility diversity, regional vitality, flow complexity, inflow-outflow variety, or urban activity index.
metadata:
  short-description: 计算时空分布熵、流动多样性与区域活力指数。
---

# measure_spatiotemporal_entropy

Use this Skill with OD matrix and/or evidence records to quantify regional mobility complexity.

## Command

```bash
cd /mnt/skills/custom/measure_spatiotemporal_entropy
python3 scripts/measure_spatiotemporal_entropy.py --od-matrix /path/to/od_matrix.jsonl --evidence /path/to/evidence.jsonl --output-dir /path/to/output
```

## Outputs

- `entropy_by_region.jsonl`
- `viz_input.jsonl`
- `summary.json`

## Algorithm

- Shannon entropy of inflow origins
- Shannon entropy of outflow destinations
- Shannon entropy of active time slots
- Composite vitality index
- Landmark-enriched region evidence

## Next Skills

Use entropy outputs with `$profile_urban_region` or `$fuse_spatial_evidence`.
