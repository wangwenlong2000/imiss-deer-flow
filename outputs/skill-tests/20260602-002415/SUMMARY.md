# Video Monitoring Skills Agent Chain Test Summary

- started_at: `2026-06-02T00:24:15+0800`
- finished_at: `2026-06-02T00:25:49+0800`
- total: `3`
- passed: `3`
- failed: `0`

## Docker Services

```text
NAMES                           IMAGE                                                 STATUS                   PORTS
deer-flow-sandbox-d70c93b3      huangxiao-deerflow-sandbox:network-tools              Up 6 minutes (healthy)   0.0.0.0:8182->8080/tcp, [::]:8182->8080/tcp
huangxiao-deer-flow-nginx       nginx:alpine                                          Up 6 days                80/tcp, 0.0.0.0:3230->3230/tcp, [::]:3230->3230/tcp
huangxiao-deer-flow-gateway     huangxiao-deer-flow-dev-gateway                       Up 6 days                2024/tcp, 8001/tcp
huangxiao-deer-flow-langgraph   huangxiao-deer-flow-dev-langgraph                     Up 6 days                2024/tcp, 8001/tcp
huangxiao-deer-flow-frontend    huangxiao-deer-flow-dev-frontend                      Up 6 days                3000/tcp

```

## Results

| Skill | Pass | Thread | Run | Notes |
| --- | --- | --- | --- | --- |
| `batch-video-ingestion` | `True` | `b5255ef8-57a1-4bf4-a00a-c4a60a1f4ad0` | `019e8400-094c-71a1-9fbe-39bf215b4dc1` | ok |
| `video-search` | `True` | `cb00ab93-4d1c-4e0a-a63e-2ed3a402a389` | `019e8400-aa8c-7bd1-afa8-125d7ee2ebcf` | ok |
| `video-embedding-index` | `True` | `48a69439-0b34-4017-9562-329d1e009b8a` | `019e8401-07b3-7db2-9687-ffdf8823e778` | ok |
