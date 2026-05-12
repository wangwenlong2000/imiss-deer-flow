# CSV Data Analysis

Use this for descriptive analysis of uploaded or local road-traffic CSV data. The LLM owns the analysis plan after schema inspection; do not use a script-level classifier or a fixed summary runner.

Suitable requests include:

- `分析过去三日小寨东路的交通情况`
- `总结这个交通流量 CSV`
- `比较各传感器/路段的车流量和车速`
- `看早晚高峰和非高峰有什么差异`
- `按天/小时统计拥堵水平`

## Workflow

1. Resolve the CSV.
   - If the user uploaded a CSV, use the uploaded path.
   - If the user refers to local/built-in/database data, list `/mnt/skills/custom/road-traffic-analysis/data/csv` and choose the best matching CSV by filename/path.
   - If exactly one local CSV clearly matches, proceed with it.
   - If there are no matches or multiple equally plausible matches, ask the user to choose.

2. Inspect the selected CSV before analysis:

```bash
python /mnt/skills/custom/road-traffic-analysis/scripts/forecast_runner.py \
  --action inspect \
  --file /mnt/skills/custom/road-traffic-analysis/data/csv/<selected>.csv \
  --head 100
```

3. Infer semantic columns from the inspected schema:
   - timestamp/time column, if time-window/trend analysis is needed
   - traffic count/volume column
   - speed column
   - congestion level/status column
   - road/sensor/location column
   - peak/off-peak column

4. Generate a small pandas analysis tailored to the user question. Typical operations:
   - filter the requested time window
   - aggregate by day, hour, road, sensor, or peak/off-peak
   - compute count, sum, mean, median, min, max
   - identify highest/lowest periods or locations
   - compare speed, flow, and congestion patterns

5. Answer with measured results and the selected CSV path.

## Analysis Constraints

- Do not read the whole CSV into model context.
- Do not invent unavailable metrics.
- Do not claim incident causes from CSV statistics alone.
- If a requested metric has no matching column, say which column is missing and continue with available metrics.
- For visualization requests, refer to the chart-visualization skill after producing the data summary needed for plotting.

## Example Pandas Shape

```bash
python - <<'PY'
import pandas as pd

path = "/mnt/skills/custom/road-traffic-analysis/data/csv/<selected>.csv"
df = pd.read_csv(path)
df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
df = df.dropna(subset=["Timestamp"]).sort_values("Timestamp")

end = df["Timestamp"].max()
window = df[df["Timestamp"] >= end - pd.Timedelta(days=3)]

daily = window.groupby(window["Timestamp"].dt.date).agg(
    records=("Timestamp", "size"),
    vehicle_count_mean=("Vehicle_Count", "mean"),
    vehicle_count_sum=("Vehicle_Count", "sum"),
    speed_mean=("Vehicle_Speed", "mean"),
    congestion_mean=("Congestion_Level", "mean"),
    congestion_max=("Congestion_Level", "max"),
).reset_index()

print(daily.to_json(orient="records", force_ascii=False))
PY
```

This example is a shape, not a fixed rule. Adapt column names and grouping to the inspected CSV and the user's actual question.
