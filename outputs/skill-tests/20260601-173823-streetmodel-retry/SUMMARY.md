# StreetModel Retry Summary

- started_at: `2026-06-01T17:39:14+08:00`
- total: `2`
- passed: `1`
- failed: `1`

| Case | Pass | Status | Error | Dimensions | Embedded | Failed | Message |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `streetmodel-query-embedding` | `True` | `success` | `None` | `2048` | `None` | `None` |  |
| `streetmodel-video-embedding` | `False` | `failed` | `STREETMODEL_REQUEST_FAILED` | `None` | `None` | `None` | StreetModel request failed: HTTP 400 |
