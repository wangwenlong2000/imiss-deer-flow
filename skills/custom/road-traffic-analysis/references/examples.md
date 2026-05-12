# Road Traffic Examples

Use these as canonical task shapes.

## Realtime

User: `西安市今天的天气怎么样`

- Family: realtime
- Action: `weather`
- Params: `location="西安市"`

User: `我想查一下西安市朱雀大街这条路的交通拥堵情况`

- Family: realtime
- Action: `road-traffic`
- Params: `road_name="朱雀大街"`, `city="西安市"`
- Do not use `around-traffic`, web search, forecast, or anomaly detection.

User: `查一下西安市太乙路周边实时路况`

- Family: realtime
- Step 1: `geocode(address="太乙路", city="西安市")`
- Step 2: `around-traffic(center="<geocode.location.center>", radius=500, road_grade=0)`

User: `查一下太乙路周边实时路况`

- Missing: `city`
- Ask: `请提供城市（city），例如“西安市”。`

## CSV Data Analysis

User: `分析一下过去三日小寨东路的交通情况`

- Family: CSV data analysis
- Data source: local CSV database
- Step 1: list `/mnt/skills/custom/road-traffic-analysis/data/csv/**/*.csv`
- Step 2: choose `小寨东路交通流量.csv` because it clearly matches the road name
- Step 3: inspect with `forecast_runner.py --action inspect --file /mnt/skills/custom/road-traffic-analysis/data/csv/小寨东路交通流量.csv --head 100`
- Step 4: infer columns from the inspected schema, for example timestamp, vehicle count, speed, congestion, sensor/location, and peak/off-peak fields when present
- Step 5: write task-specific pandas code to filter the last/requested 3-day window and aggregate by day, hour, sensor/location, and peak/off-peak as relevant
- Reply: report measured traffic volume, speed, congestion level, time-window coverage, and notable patterns. Include selected CSV path.

User: `用本地CSV库比较几个路段的平均车速`

- Family: CSV data analysis
- Data source: local CSV database
- Step 1: list local CSV files
- Step 2: select matching CSV(s), or ask the user to choose if multiple files could match
- Step 3: inspect selected CSV(s)
- Step 4: generate pandas comparison based on actual column names
- Reply: compare measured averages and caveat missing columns.

## Forecasting

User: `我上传了 traffic.csv，预测未来12小时车流量`

- Family: forecasting
- Step 1: `forecast_runner.py --action models`
- Step 2: `forecast_runner.py --action inspect --file /mnt/user-data/uploads/traffic.csv --head 100`
- Step 3: infer `timestamp_col` and `value_col` from inspected columns
- Step 4: ask for `freq` or model if unclear
- Step 5: run `--action forecast`

User: `根据 120,130,128,140,150 预测后6小时`

- Family: forecasting
- Mode: inline history
- Required if unclear: `freq`, model preference

User: `用本地数据库里的太乙路车流数据预测未来12小时`

- Family: forecasting
- Data source: local CSV database
- Step 1: list `/mnt/skills/custom/road-traffic-analysis/data/csv/**/*.csv`
- Step 2: choose the CSV whose filename/path best matches `太乙路` and traffic flow
- Step 3: `forecast_runner.py --action inspect --file <selected_full_csv_path> --head 100`
- Step 4: infer `timestamp_col`, `value_col`, optional `series_id_col`; ask only if unclear
- Step 5: run `--action forecast`

## Anomaly Detection

User: `我上传了 traffic.csv，帮我找车流量异常点`

- Family: anomaly detection
- Step 1: `anomaly_runner.py --action detectors`
- Step 2: `anomaly_runner.py --action inspect --file /mnt/user-data/uploads/traffic.csv --head 100`
- Step 3: infer `timestamp_col` and `value_col` from inspected columns
- Step 4: ask for `freq` or detector preference if unclear
- Step 5: run `--action detect`

User: `这些小时车流量 120,122,121,600,123 是否异常，超过500算异常`

- Family: anomaly detection
- Mode: inline history
- Detector: `threshold_ad`
- Params: `threshold_high=500`

User: `从内置CSV库里找朱雀大街数据，检查有没有异常车流`

- Family: anomaly detection
- Data source: local CSV database
- Step 1: list `/mnt/skills/custom/road-traffic-analysis/data/csv/**/*.csv`
- Step 2: choose the CSV whose filename/path best matches `朱雀大街`
- Step 3: `anomaly_runner.py --action inspect --file <selected_full_csv_path> --head 100`
- Step 4: infer `timestamp_col`, `value_col`, optional `series_id_col`; ask only if unclear
- Step 5: run `--action detect`

## RAG

User: `2024年西安市中心城区主干路高峰期平均速度是多少？请给出处。`

- Family: RAG knowledge retrieval
- Prompt routing: annual-report fact query
- Command: `rag_xian2024_min.py query "2024年西安市中心城区主干路高峰期平均速度" --top-k 5`
- Reply: answer with source `section_path` and page range.

User: `今天早高峰西安市朱雀大街是否需要临时疏导？请结合实时路况和西安年度交通指标给出研判。`

- Family: mixed realtime + RAG
- Step 1: `weather(location="西安市xx区")`
- Step 2: `road-traffic(road_name="朱雀大街", city="西安市")`
- Step 3: `geocode(address="朱雀大街", city="西安市")`
- Step 4: `around-traffic(center="<geocode.location.center>", radius=500)`
- Step 5: `rag_xian2024_min.py query "西安市中心城区主干路高峰期平均速度 交通体检指标 2024" --top-k 5`
- Reply: keep realtime measurements and annual-report citations separate.

User: `查一下西安市朱雀大街现在堵不堵`

- Family: realtime
- Action: `road-traffic`
- Params: `road_name="朱雀大街"`, `city="西安市"`
- Do not call RAG because the user only asked for current status.
