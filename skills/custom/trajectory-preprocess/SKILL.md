---
name: trajectory-preprocess
description: Use this skill when the user wants to clean, standardize, segment, or inspect raw spatiotemporal trajectory data such as GPS traces, check-in logs, taxi traces, or user mobility CSV/JSONL files. It supports coordinate validation, timestamp parsing, speed-threshold denoising, staypoint extraction, trip segmentation, geohash encoding, and output validation.
metadata:
  short-description: Clean and standardize raw trajectory data into reusable points, staypoints, and trips.
---

# Trajectory Preprocess Skill

Use this skill before downstream spatiotemporal analysis when the user provides or references raw trajectory data and wants:

- GPS/check-in trajectory cleaning
- abnormal point denoising
- staypoint extraction
- trip segmentation
- user trajectory standardization
- geohash/grid mapping
- quality inspection before OD, anomaly, forecasting, or route analysis

This Skill is the planned data foundation for future trajectory analysis Skills such as `trajectory-od-analysis`, `trajectory-pattern-mining`, `trajectory-anomaly-detection`, and `trajectory-forecasting`.

## Directory Layout

- `agents/`: invocation hints for DeerFlow models
- `references/`: field schema, preprocessing rules, and quality checks
- `scripts/`: executable preprocessing tools
- `SKILL.md`: usage contract for the agent

## Workflow

### 0. Runtime rule: no virtual environment

This Skill uses only the Python standard library.

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
python scripts/preprocess_trajectory.py --help
```

or:

```bash
python3 scripts/preprocess_trajectory.py --help
```

### 1. Identify the input file

Accept local trajectory files in CSV, TSV, JSON, or JSONL format. Prefer an explicit file path if the user provides one.

The main script can infer common column names, including:

- user: `user_id`, `uid`, `User ID`
- trajectory: `trajectory_id`, `traj_id`, `trip_id`
- time: `timestamp`, `time`, `datetime`, `UTC Time`
- latitude: `lat`, `latitude`
- longitude: `lon`, `lng`, `longitude`
- POI/category fields are preserved as attributes when present

If automatic detection fails, rerun with explicit `--user-col`, `--time-col`, `--lat-col`, and `--lon-col`.

### Demo sample fallback

If the user explicitly asks to test with `/mnt/user-data/workspace/trajectory_sample.csv` and that file is missing, create a deterministic demo sample first. Clearly mention in the final answer that demo data was synthesized because the requested file did not exist.

Do not synthesize data for real analysis requests or for any non-demo filename.

```bash
cd /mnt/skills/custom/trajectory-preprocess
python3 scripts/create_demo_trajectory_sample.py \
  --output /mnt/user-data/workspace/trajectory_sample.csv
```

### 2. Run the full preprocessing pipeline

Preferred command:

```bash
cd /mnt/skills/custom/trajectory-preprocess
python3 scripts/preprocess_trajectory.py \
  --input /path/to/trajectory.csv \
  --output-dir /tmp/trajectory-preprocess-output
```

Useful options:

```bash
python3 scripts/preprocess_trajectory.py \
  --input /path/to/trajectory.csv \
  --output-dir /tmp/trajectory-preprocess-output \
  --max-speed-kmh 180 \
  --stay-radius-m 200 \
  --stay-min-minutes 20 \
  --trip-gap-minutes 60 \
  --geohash-precision 6
```

The script writes:

- `cleaned_points.jsonl`
- `staypoints.jsonl`
- `trips.jsonl`
- `summary.json`

### 3. Run a focused step when needed

Staypoint extraction only:

```bash
cd /mnt/skills/custom/trajectory-preprocess
python3 scripts/extract_staypoints.py \
  --input /path/to/trajectory.csv \
  --output-dir /tmp/trajectory-staypoints
```

Trip segmentation only:

```bash
cd /mnt/skills/custom/trajectory-preprocess
python3 scripts/segment_trips.py \
  --input /path/to/trajectory.csv \
  --output-dir /tmp/trajectory-trips
```

Output validation:

```bash
cd /mnt/skills/custom/trajectory-preprocess
python3 scripts/validate_outputs.py --output-dir /tmp/trajectory-preprocess-output
```

### 4. Summarize for the user

Base the answer on `summary.json` and mention:

- number of raw points, cleaned points, dropped points
- invalid coordinate/time counts
- speed-filtered points
- number of users, staypoints, and trips
- output paths
- any quality warnings

Do not claim OD, route recommendation, or forecasting results from this Skill alone. This Skill prepares standardized trajectory evidence for those downstream Skills.

## Algorithm Summary

- Coordinate validation: latitude and longitude range checks
- Timestamp normalization: ISO, epoch seconds, and common check-in timestamp parsing
- Denoising: duplicate removal and speed-threshold filtering with Haversine distance
- Spatial mapping: pure-Python geohash encoding
- Staypoint detection: distance-radius and dwell-time threshold
- Trip segmentation: per-user/per-trajectory temporal gap splitting
- Quality checks: schema validation, count consistency, and warning generation
