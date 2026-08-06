# Traffic Forecasting

Use `forecast_runner.py` for traffic-flow prediction only. It supports uploaded CSV, local CSV database files, and inline numeric history.

## Models

- `seasonal_naive`: stable periodic baseline, fastest baseline.
- `autoets`: smooth trend + seasonality.
- `autoarima`: autocorrelation-driven regular series.
- `mstl`: complex or multi-seasonal patterns.
- `auto`: evaluate supported models and select one automatically.

## Mandatory Workflow

1. Read the model catalog:

```bash
python /mnt/skills/custom/road-traffic-analysis/scripts/forecast_runner.py --action models
```

2. Inspect CSV data before model discussion:

```bash
python /mnt/skills/custom/road-traffic-analysis/scripts/forecast_runner.py \
  --action inspect \
  --file /mnt/user-data/uploads/traffic.csv \
  --head 100
```

If the user wants built-in/local/database CSV data, first choose a file from `/mnt/skills/custom/road-traffic-analysis/data/csv`, then inspect that full path:

```bash
python /mnt/skills/custom/road-traffic-analysis/scripts/forecast_runner.py \
  --action inspect \
  --file /mnt/skills/custom/road-traffic-analysis/data/csv/<selected>.csv \
  --head 100
```

3. Decide semantic column mapping from actual inspected columns:
   - required: `timestamp_col`, `value_col`
   - optional: `series_id_col`

4. Ask user when any of these is missing or unclear:
   - timestamp/value mapping
   - forecast horizon
   - frequency such as `h`, `15min`, `D`
   - model preference

5. If the user did not specify a model, ask whether they want manual choice. Use `auto` only if the user confirms no preference or asks the agent to decide.

## Commands

CSV forecast:

```bash
python /mnt/skills/custom/road-traffic-analysis/scripts/forecast_runner.py \
  --action forecast \
  --file /mnt/user-data/uploads/traffic.csv \
  --timestamp-col <timestamp_col> \
  --value-col <value_col> \
  --series-id-col <series_id_col_or_empty> \
  --horizon <horizon> \
  --freq <freq> \
  --model <seasonal_naive|autoets|autoarima|mstl|auto>
```

Inline history forecast:

```bash
python /mnt/skills/custom/road-traffic-analysis/scripts/forecast_runner.py \
  --action forecast \
  --history "120,130,128,140,150,158,162,167" \
  --horizon 6 \
  --freq h \
  --model auto
```

## Output

Include:

- input source, CSV path or inline history
- mapping used
- data sufficiency and assumptions
- model used or auto-selected model
- key forecast findings
- output file path
