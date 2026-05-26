# Video Monitoring Skills Agent Chain Test Summary

- started_at: `2026-05-26T16:58:26+0800`
- finished_at: `2026-05-26T17:00:36+0800`
- total: `4`
- passed: `4`
- failed: `0`

## Docker Services

```text
NAMES                            IMAGE                                                 STATUS          PORTS
huangxiao-deer-flow-nginx        nginx:alpine                                          Up 8 minutes    80/tcp, 0.0.0.0:3230->3230/tcp, [::]:3230->3230/tcp
huangxiao-deer-flow-gateway      huangxiao-deer-flow-dev-gateway                       Up 16 seconds   2024/tcp, 8001/tcp
huangxiao-deer-flow-langgraph    huangxiao-deer-flow-dev-langgraph                     Up 27 seconds   2024/tcp, 8001/tcp
huangxiao-deer-flow-frontend     huangxiao-deer-flow-dev-frontend                      Up 8 minutes    3000/tcp

```

## Results

| Skill | Pass | Thread | Run | Notes |
| --- | --- | --- | --- | --- |
| `batch-video-ingestion` | `True` | `87d31825-f323-4ee0-8bef-8a98178c9bcc` | `019e6381-b905-7e92-8826-16448716abc3` | ok |
| `video-search` | `True` | `b72427ca-ddc6-43d8-869d-6f777852125b` | `019e6382-233a-7213-a218-8c83af8d2da7` | ok |
| `object-statistics` | `True` | `43020d02-3a95-43b4-9272-8fe3409e9e93` | `019e6382-86d1-71b0-9936-a6becebff965` | ok |
| `evidence-package-generation` | `True` | `e54c3b4b-fa37-4f0f-8719-de3e72a186de` | `019e6383-03b0-7fe0-b1ac-dd9bd2bac434` | ok |
