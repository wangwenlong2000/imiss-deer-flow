# Trajectory Anomaly Detection 数据结构

## 支持输入

支持 CSV、TSV、JSON、JSONL。

常见输入包括：

- `cleaned_points.jsonl`
- `staypoints.jsonl`
- `trips.jsonl`
- CityBench `evidence.jsonl`
- 聚合后的 OD/count/time-series 表

## 字段识别

脚本会扁平化嵌套 JSON 字段。例如：

- `meta.geo_scope.geohash`
- `meta.time_range.start`
- `meta.features.checkin_count`
- `meta.features.unique_users`
- `meta.features.wow_change_pct`

常见自动识别字段：

- 时间字段：`timestamp`、`start_time`、`time_range.start`、`meta.time_range.start`
- 分组字段：`geohash`、`meta.geo_scope.geohash`、`user_id`、`trajectory_id`
- 指标字段：`checkin_count`、`unique_users`、`duration_minutes`、`distance_km`、`point_count`、`wow_change_pct`

## 输出 1：anomalies.jsonl

每行是一条异常记录：

```json
{
  "rank": 1,
  "record_index": 12,
  "group": "wx4g0",
  "timestamp": "2012-06-04T07:00:00",
  "anomaly_score": 4.52,
  "flags": [
    {
      "metric": "meta.features.checkin_count",
      "method": "zscore",
      "score": 4.52,
      "direction": "high",
      "value": 381
    }
  ],
  "record": {}
}
```

## 输出 2：scored_records.jsonl

保留所有被评分记录，包括正常记录和异常记录。

## 输出 3：summary.json

包含：

- 输入路径
- 输出路径
- 使用算法
- 使用指标
- 总记录数
- 异常记录数
- 分组数
- Top-K 异常摘要
- 警告信息

