# 查询模式

## 区域异常

用户说：

- “找出签到异常波动显著的区域”
- “哪些 geohash 在早高峰异常”
- “Top 5 异常商圈”

建议：

```bash
python scripts/detect_anomalies.py \
  --input evidence.jsonl \
  --output-dir /tmp/anomaly-output \
  --group-col meta.geo_scope.geohash \
  --time-col meta.time_range.start \
  --metric meta.features.checkin_count \
  --metric meta.features.unique_users
```

## Trip 异常

用户说：

- “找出异常长距离出行”
- “哪些用户的通勤 trip 异常”
- “轨迹段是否存在异常”

建议：

```bash
python scripts/detect_anomalies.py \
  --input trips.jsonl \
  --output-dir /tmp/trip-anomaly-output \
  --group-col user_id \
  --time-col start_time \
  --metric duration_minutes \
  --metric distance_km
```

## 停留异常

用户说：

- “识别异常停留”
- “哪些用户停留时间异常”

建议：

```bash
python scripts/detect_anomalies.py \
  --input staypoints.jsonl \
  --output-dir /tmp/stay-anomaly-output \
  --group-col user_id \
  --time-col start_time \
  --metric duration_minutes \
  --metric point_count
```

