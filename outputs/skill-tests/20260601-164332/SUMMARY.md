# Video Monitoring Skills Agent Chain Test Summary

- started_at: `2026-06-01T16:43:32+0800`
- finished_at: `2026-06-01T16:45:35+0800`
- total: `4`
- passed: `4`
- failed: `0`

## Docker Services

```text
NAMES                           IMAGE                                                 STATUS       PORTS
huangxiao-deer-flow-nginx       nginx:alpine                                          Up 5 days    80/tcp, 0.0.0.0:3230->3230/tcp, [::]:3230->3230/tcp
huangxiao-deer-flow-gateway     huangxiao-deer-flow-dev-gateway                       Up 5 days    2024/tcp, 8001/tcp
huangxiao-deer-flow-langgraph   huangxiao-deer-flow-dev-langgraph                     Up 5 days    2024/tcp, 8001/tcp
huangxiao-deer-flow-frontend    huangxiao-deer-flow-dev-frontend                      Up 5 days    3000/tcp

```

## Results

| Skill | Pass | Thread | Run | Notes |
| --- | --- | --- | --- | --- |
| `batch-video-ingestion` | `True` | `00c4f2ac-6e43-49bb-a25d-6214e999173a` | `019e825a-3c16-7612-8877-6d8fee4552e7` | ok |
| `video-search` | `True` | `e0266560-58d2-4ab4-8507-25cd64cad5a7` | `019e825a-bf09-7c51-8a11-c3f6f3ecec67` | ok |
| `video-embedding-index` | `True` | `b8ba44cd-4571-4390-b08e-bfc4b9c48942` | `019e825b-2ee1-7bf3-b184-875f5f4fff07` | ok |
| `object-statistics` | `True` | `fece3965-dff0-4cf7-9502-c2a325d7d8e4` | `019e825b-9df3-7973-b11c-2b09e941e94b` | ok |
