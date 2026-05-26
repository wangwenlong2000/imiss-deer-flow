# Video Monitoring Skills Agent Chain Test Summary

- started_at: `2026-05-26T16:54:42+0800`
- finished_at: `2026-05-26T16:56:56+0800`
- total: `4`
- passed: `1`
- failed: `3`

## Docker Services

```text
NAMES                            IMAGE                                                 STATUS         PORTS
huangxiao-deer-flow-nginx        nginx:alpine                                          Up 4 minutes   80/tcp, 0.0.0.0:3230->3230/tcp, [::]:3230->3230/tcp
huangxiao-deer-flow-gateway      huangxiao-deer-flow-dev-gateway                       Up 4 minutes   2024/tcp, 8001/tcp
huangxiao-deer-flow-langgraph    huangxiao-deer-flow-dev-langgraph                     Up 4 minutes   2024/tcp, 8001/tcp
huangxiao-deer-flow-frontend     huangxiao-deer-flow-dev-frontend                      Up 4 minutes   3000/tcp

```

## Results

| Skill | Pass | Thread | Run | Notes |
| --- | --- | --- | --- | --- |
| `batch-video-ingestion` | `False` | `b6b6b51d-341e-4339-8838-f9bf908c3d51` | `019e637e-4dbb-7743-b37b-4e4439965e36` | ES_CONNECTION_FAILED |
| `video-search` | `False` | `579d9d64-79d9-4e6f-9655-3f621e3b3170` | `019e637e-b9ab-74a2-bc84-cfb7895ce5b2` | ES_CONNECTION_FAILED |
| `object-statistics` | `False` | `ddb44396-830c-44d1-94fa-fa4682cd13ed` | `019e637f-4666-7810-9e92-8c6dedfca73c` | ES_CONNECTION_FAILED |
| `evidence-package-generation` | `True` | `cb3e3f60-f358-4242-b125-3d8a6a3951bb` | `019e637f-acb2-74b1-ba21-b263df4be9cc` | ok |
