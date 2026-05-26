# Traffic Anomaly Detection

Use `anomaly_runner.py` for traffic-flow anomaly detection only. It supports uploaded CSV, local CSV database files, and inline numeric history.

## Detectors

- `seasonal_ad`: periodic outlier detection.
- `persist_ad`: sudden jump compared with recent window.
- `level_shift_ad`: baseline or regime shift.
- `volatility_shift_ad`: variance or spread shift.
- `threshold_ad`: business threshold anomalies. Requires `threshold_low` and/or `threshold_high`.
- `pca_ad`: multivariate anomalies on aligned multi-series data. Requires at least two features.
- `auto`: default detector set; can include threshold/PCA when parameters request them.

## Mandatory Workflow

1. Read the detector catalog:

```bash
python /mnt/skills/custom/road-traffic-analysis/scripts/anomaly_runner.py --action detectors
```

2. Inspect CSV data before detector selection:

```bash
python /mnt/skills/custom/road-traffic-analysis/scripts/anomaly_runner.py \
  --action inspect \
  --file /mnt/user-data/uploads/traffic.csv \
  --head 100
```

If the user wants built-in/local/database CSV data, first choose a file from `/mnt/skills/custom/road-traffic-analysis/data/csv`, then inspect that full path:

```bash
python /mnt/skills/custom/road-traffic-analysis/scripts/anomaly_runner.py \
  --action inspect \
  --file /mnt/skills/custom/road-traffic-analysis/data/csv/<selected>.csv \
  --head 100
```

3. Decide semantic column mapping from actual inspected columns:
   - required: `timestamp_col`, `value_col`
   - optional: `series_id_col`

4. Ask user when any of these is missing or unclear:
   - timestamp/value mapping
   - frequency such as `h`, `15min`, `D`
   - detector preference
   - threshold values for `threshold_ad`
   - multivariate/PCA intent for `pca_ad`

5. If the user did not specify a detector, ask whether they want manual choice. Use `auto` only if the user confirms no preference or asks the agent to decide.

## Commands

CSV anomaly detection:

```bash
python /mnt/skills/custom/road-traffic-analysis/scripts/anomaly_runner.py \
  --action detect \
  --file /mnt/user-data/uploads/traffic.csv \
  --timestamp-col <timestamp_col> \
  --value-col <value_col> \
  --series-id-col <series_id_col_or_empty> \
  --detector <auto|seasonal_ad|persist_ad|level_shift_ad|volatility_shift_ad|threshold_ad|pca_ad> \
  --freq <freq>
```

Threshold example:

```bash
python /mnt/skills/custom/road-traffic-analysis/scripts/anomaly_runner.py \
  --action detect \
  --file /mnt/user-data/uploads/traffic.csv \
  --timestamp-col 采样时间 \
  --value-col 车流量 \
  --series-id-col 路段ID \
  --detector threshold_ad \
  --threshold-high 800 \
  --freq h
```

Inline history example:

```bash
python /mnt/skills/custom/road-traffic-analysis/scripts/anomaly_runner.py \
  --action detect \
  --history "120,122,121,600,123,122,121,119,118,117" \
  --detector threshold_ad \
  --threshold-high 500 \
  --freq h
```

## Output

Include:

- input source, CSV path or inline history
- mapping used
- data sufficiency and assumptions
- detector(s) used and key parameters
- anomaly count/rate
- output file path
