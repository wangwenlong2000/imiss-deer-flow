# Trajectory Preprocess 数据结构

## 输入字段

脚本会自动识别常见轨迹字段。

必需字段：

- `user_id`: 用户或设备标识
- `timestamp`: 时间戳
- `lat`: 纬度
- `lon`: 经度

可选字段：

- `trajectory_id`: 原始轨迹或行程编号
- `venue_id`: POI 或签到点编号
- `category`: POI 类别
- `city`: 城市
- 其他字段会保留在 `attributes`

## 输出 1：cleaned_points.jsonl

每行是一条清洗后的轨迹点：

```json
{
  "point_id": "pt_000001",
  "user_id": "u1",
  "trajectory_id": "u1",
  "timestamp": "2012-06-01T07:20:00",
  "lat": 39.9042,
  "lon": 116.4074,
  "geohash": "wx4g09",
  "sequence": 1,
  "attributes": {}
}
```

## 输出 2：staypoints.jsonl

每行是一个停留点：

```json
{
  "staypoint_id": "sp_000001",
  "user_id": "u1",
  "trajectory_id": "u1",
  "start_time": "2012-06-01T07:20:00",
  "end_time": "2012-06-01T07:50:00",
  "duration_minutes": 30.0,
  "centroid_lat": 39.9043,
  "centroid_lon": 116.4075,
  "geohash": "wx4g09",
  "point_count": 4
}
```

## 输出 3：trips.jsonl

每行是一段出行轨迹：

```json
{
  "trip_id": "trip_000001",
  "user_id": "u1",
  "trajectory_id": "u1",
  "start_time": "2012-06-01T07:00:00",
  "end_time": "2012-06-01T08:10:00",
  "duration_minutes": 70.0,
  "point_count": 12,
  "start_geohash": "wx4g0",
  "end_geohash": "wx4g2",
  "distance_km": 8.4
}
```

## 输出 4：summary.json

汇总清洗质量、过滤数量、用户数、停留点数、行程数、输出路径和质量警告。

