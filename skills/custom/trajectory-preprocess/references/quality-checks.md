# 质量检查说明

`validate_outputs.py` 会检查预处理结果是否满足下列条件：

- `summary.json` 存在且可以解析。
- `cleaned_points.jsonl`、`staypoints.jsonl`、`trips.jsonl` 至少存在。
- `cleaned_points.jsonl` 中每条记录包含 `user_id`、`timestamp`、`lat`、`lon`、`geohash`。
- `staypoints.jsonl` 中每条记录包含起止时间、质心坐标和停留时长。
- `trips.jsonl` 中每条记录包含起止时间、点数和距离。

常见质量警告：

- 清洗后点数为 0：通常是字段识别失败、坐标无效或时间解析失败。
- 停留点数为 0：可能是数据采样过稀，或停留半径/时间阈值过严格。
- trip 数为 0：可能是每个用户点数太少，或 `--min-trip-points` 过高。
- speed-filtered 点数过高：可能存在坐标系错误、时间排序问题或阈值过低。

