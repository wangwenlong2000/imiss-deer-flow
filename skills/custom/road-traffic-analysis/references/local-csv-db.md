# Local CSV Database

Use this when the user asks to analyze, forecast, or detect anomalies from the skill's built-in road-traffic CSV data but does not upload a file.

## Location

CSV files live under:

```bash
/mnt/skills/custom/road-traffic-analysis/data/csv
```

Repo-local path:

```bash
skills/custom/road-traffic-analysis/data/csv
```

## Prompt Routing

The LLM chooses the CSV by inspecting directory and file names. Do not build a script-level classifier for dataset selection.

Workflow:

1. List available CSV files:

```bash
find /mnt/skills/custom/road-traffic-analysis/data/csv \
  -maxdepth 2 \
  -type f \
  -name "*.csv" \
  -printf "%p\n"
```

2. Select the best matching CSV from the user's wording and filename/path.
3. If there is exactly one clear match, pass its full absolute path to the existing runner.
4. If no CSV or multiple equally plausible CSV files match, ask the user to choose from the candidate filenames.
5. After a CSV is selected, handle it exactly like an uploaded CSV:
   - run `inspect`
   - infer semantic columns from the inspected schema
   - for descriptive data analysis, let the LLM generate task-specific pandas analysis
   - for forecast/anomaly workflows, infer `timestamp_col`, `value_col`, and optional `series_id_col`
   - ask only for missing required mapping, `freq`, `horizon`, threshold, model, or detector when unclear

## Important

- Always pass the full CSV path from this directory to `--file`.
- Do not read the whole CSV into model context.
- Uploaded files take priority when the user explicitly says they uploaded a file.
- Use the local CSV database when the user says built-in data, sample data, local data, database, existing data, or names a CSV that appears in this directory.

## Examples

Data analysis:

```bash
python /mnt/skills/custom/road-traffic-analysis/scripts/forecast_runner.py \
  --action inspect \
  --file /mnt/skills/custom/road-traffic-analysis/data/csv/<selected>.csv \
  --head 100
```

Forecast:

```bash
python /mnt/skills/custom/road-traffic-analysis/scripts/forecast_runner.py \
  --action inspect \
  --file /mnt/skills/custom/road-traffic-analysis/data/csv/<selected>.csv \
  --head 100
```

Anomaly detection:

```bash
python /mnt/skills/custom/road-traffic-analysis/scripts/anomaly_runner.py \
  --action inspect \
  --file /mnt/skills/custom/road-traffic-analysis/data/csv/<selected>.csv \
  --head 100
```
