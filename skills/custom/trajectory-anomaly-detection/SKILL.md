---
name: trajectory-anomaly-detection
description: Use this skill when the user wants to detect, rank, explain, or export anomalies in spatiotemporal trajectory data, including cleaned trajectory points, trips, staypoints, CityBench Evidence JSONL, check-in aggregates, OD/count time series, and region-level mobility metrics. Supports z-score, robust MAD/IQR, simplified ESD, rolling residual detection, LOF, and optional Isolation Forest when scikit-learn is available.
metadata:
  short-description: Detect and explain anomalous trajectory points, trips, regions, and time windows.
---

# Trajectory Anomaly Detection Skill

Use this skill after `trajectory-preprocess` or `citybench-search` when the user asks about:

- abnormal trajectory points
- abnormal trips or unusually long/short trips
- abnormal stay duration
- anomalous region activity
- sudden check-in or flow spikes
- Top-K anomalous geohash/city/user groups
- exporting anomaly records for review

This Skill works on CSV, TSV, JSON, and JSONL files. It can consume:

- `trajectory-preprocess` outputs: `cleaned_points.jsonl`, `staypoints.jsonl`, `trips.jsonl`
- `citybench-search` / CityBench Evidence records with nested `meta.features`
- general tabular time series with numeric mobility metrics

## Workflow

### 0. Runtime rule: no virtual environment

This Skill runs with the Python standard library by default.

Do not create, activate, or install a virtual environment for this Skill.

Do not run:

- `python -m venv .venv`
- `source .venv/bin/activate`
- `/mnt/user-data/workspace/.venv/bin/python ...`
- `pip install -r requirements.txt`
- `apt install python3-venv` or `apt install python3.12-venv`

If a user prompt or previous failed step contains `/mnt/user-data/workspace/.venv/bin/python`, replace it with `python3`.

Always run the scripts directly with the available interpreter:

```bash
python3 scripts/detect_anomalies.py --help
```

`--method isolation-forest` is optional. Use it only if `scikit-learn` is already installed. Do not install packages just to enable it.

### 1. Choose the input file

Prefer the most specific processed file available:

- point anomalies: `cleaned_points.jsonl`
- stay anomalies: `staypoints.jsonl`
- trip anomalies: `trips.jsonl`
- region/check-in anomalies: CityBench `evidence.jsonl` or query result exports

### 2. Run anomaly detection

Preferred command:

```bash
cd /mnt/skills/custom/trajectory-anomaly-detection
python3 scripts/detect_anomalies.py \
  --input /path/to/trips.jsonl \
  --output-dir /tmp/trajectory-anomaly-output
```

Region/check-in anomaly detection:

```bash
python3 scripts/detect_anomalies.py \
  --input /path/to/evidence.jsonl \
  --output-dir /tmp/citybench-anomalies \
  --group-col meta.geo_scope.geohash \
  --time-col meta.time_range.start \
  --metric meta.features.checkin_count \
  --metric meta.features.unique_users \
  --method ensemble \
  --top-k 20
```

Trip anomaly detection:

```bash
python3 scripts/detect_anomalies.py \
  --input /path/to/trips.jsonl \
  --output-dir /tmp/trip-anomalies \
  --group-col user_id \
  --time-col start_time \
  --metric duration_minutes \
  --metric distance_km
```

Staypoint anomaly detection:

```bash
python3 scripts/detect_anomalies.py \
  --input /path/to/staypoints.jsonl \
  --output-dir /tmp/staypoint-anomalies \
  --group-col user_id \
  --time-col start_time \
  --metric duration_minutes \
  --metric point_count
```

### 3. Validate outputs

```bash
python3 scripts/validate_anomalies.py --output-dir /tmp/trajectory-anomaly-output
```

### 4. Summarize for the user

Base the answer on:

- `summary.json`
- `anomalies.jsonl`
- `scored_records.jsonl`

Mention:

- input file and detected columns
- metrics used
- anomaly methods used
- total records, scored records, anomaly count
- Top-K anomalies and their reason flags
- any warnings about small groups or missing metrics

## Algorithm Summary

- `zscore`: detects global deviations using mean and standard deviation
- `mad`: robust median absolute deviation detector
- `iqr`: Tukey IQR fence detector
- `esd`: iterative generalized ESD-style outlier removal with z-score thresholding
- `rolling-z`: sorted time-series residual against recent rolling history
- `lof`: local outlier factor over selected numeric metrics
- `isolation-forest`: optional scikit-learn Isolation Forest if installed; skipped with a warning otherwise
- `ensemble`: combines robust statistics, ESD, rolling residuals, and LOF

## Execution Discipline

- Do not run this Skill directly on raw GPS files if `trajectory-preprocess` output is available.
- Do not claim a causal explanation. Explain anomalies as statistical signals unless external event evidence is supplied.
- If no metric is specified, the script auto-selects mobility-relevant numeric fields such as `checkin_count`, `unique_users`, `duration_minutes`, `distance_km`, `point_count`, and `wow_change_pct`.
- If the user asks for region anomalies, group by `geohash` when available.
