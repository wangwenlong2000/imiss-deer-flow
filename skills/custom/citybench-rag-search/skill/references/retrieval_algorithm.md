# Retrieval Algorithm & Evidence Schema Reference

## RRF Fusion Formula

```
score(d) = Σ  1 / (k + rank_i)
           i∈{BM25, kNN}

k = 60  (default constant, tunable via RRF_RANK_CONSTANT env var)
rank_i  = 1-based rank of document d in channel i's result list
```

When a document appears in both BM25 and kNN results, its scores from both
channels are added. Documents appearing in only one channel receive only
that channel's contribution.

## Three-Channel Architecture

### Channel A — Metadata Filter (Pre-filter)

Structured field filtering applied **before** BM25 and kNN scoring.
Narrows the candidate set, does not contribute to ranking.

| Filter         | ES Field                     | Type    |
|----------------|------------------------------|---------|
| city           | `meta.geo_scope.city`        | term    |
| time start     | `meta.time_range.start`      | range   |
| time end       | `meta.time_range.end`        | range   |
| geohash prefix | `meta.geo_scope.geohash`     | prefix  |
| anomaly only   | `meta.features.anomaly_flag` | term    |
| data type      | `data_type`                  | term    |

### Channel B — BM25 Full-Text

- Index analyzer: `ik_max_word` (maximum segmentation for recall)
- Search analyzer: `ik_smart` (intelligent segmentation for precision)
- Fallback: ES `standard` analyzer when IK plugin is not installed

### Channel C — kNN Vector Search

- Model: DashScope `text-embedding-v3`
- Dimensions: 1024
- Similarity: cosine
- Implementation: `script_score` with `cosineSimilarity` (compatible with
  all ES 8.x versions)

## Evidence JSONL Schema

Each evidence record has this structure:

```json
{
  "evidence_id": "traj_ev_20120615_0700_beijing_wx4g0",
  "data_type": "spatiotemporal_trajectory",
  "text": "2012年6月15日早高峰(7:00-9:00)，Beijing(geohash:wx4g0)区域，签到活动量387次，活跃用户约152人，热门类别为咖啡店(32%)、写字楼(28%)、地铁站(22%)，较前一周同时段上升12.3%，属于正常波动。",
  "meta": {
    "source_id": "citybench_checkins_beijing",
    "source_path": "data_lake/.../Beijing_filtered_checkins.csv",
    "time_range": {
      "start": "2012-06-15T07:00:00",
      "end": "2012-06-15T09:00:00"
    },
    "geo_scope": {
      "city": "Beijing",
      "geohash": "wx4g0"
    },
    "granularity": "hourly_district",
    "sensitivity_level": "aggregated_safe",
    "access_policy": "open",
    "features": {
      "checkin_count": 387,
      "unique_users": 152,
      "top_categories": ["咖啡店", "写字楼", "地铁站"],
      "wow_change_pct": 12.3,
      "anomaly_flag": false
    }
  }
}
```

## ES Index Mapping

```json
{
  "evidence_id": "keyword",
  "data_type":   "keyword",
  "text":        "text (ik_max_word / ik_smart)",
  "text_vector": "dense_vector (1024d, cosine)",
  "meta": {
    "source_id":        "keyword",
    "time_range.start": "date",
    "time_range.end":   "date",
    "geo_scope.city":   "keyword",
    "geo_scope.geohash":"keyword",
    "features.checkin_count":  "integer",
    "features.unique_users":   "integer",
    "features.top_categories": "keyword",
    "features.wow_change_pct": "float",
    "features.anomaly_flag":   "boolean"
  }
}
```

## Privacy Protection

1. **Spatial blurring**: Individual GPS → Geohash grid (~5km²)
2. **Temporal aggregation**: Exact timestamps → 2-hour time slots
3. **User threshold**: Groups with < 5 unique users are discarded
4. **Immutability**: Raw data is archived read-only; only aggregated
   data enters the search index

## Candidate Expansion

Each channel retrieves `top_k × 5` candidates. After RRF fusion, the
final `top_k` results are returned. This over-fetch ensures adequate
recall for fusion quality.
