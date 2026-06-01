# Video Monitoring Skills Agent Chain Test Summary

- started_at: `2026-06-01T16:53:33+0800`
- finished_at: `2026-06-01T16:55:51+0800`
- total: `4`
- passed: `4`
- failed: `0`

## Docker Services

```text
NAMES                           IMAGE                                                 STATUS                   PORTS
deer-flow-sandbox-6ca36382      huangxiao-deerflow-sandbox:network-tools              Up 8 minutes (healthy)   0.0.0.0:8182->8080/tcp, [::]:8182->8080/tcp
deer-flow-sandbox-30ea8bdb      huangxiao-deerflow-sandbox:network-tools              Up 8 minutes (healthy)   0.0.0.0:8184->8080/tcp, [::]:8184->8080/tcp
deer-flow-sandbox-1e7328ad      huangxiao-deerflow-sandbox:network-tools              Up 9 minutes (healthy)   0.0.0.0:8183->8080/tcp, [::]:8183->8080/tcp
huangxiao-deer-flow-nginx       nginx:alpine                                          Up 5 days                80/tcp, 0.0.0.0:3230->3230/tcp, [::]:3230->3230/tcp
huangxiao-deer-flow-gateway     huangxiao-deer-flow-dev-gateway                       Up 5 days                2024/tcp, 8001/tcp
huangxiao-deer-flow-langgraph   huangxiao-deer-flow-dev-langgraph                     Up 5 days                2024/tcp, 8001/tcp
huangxiao-deer-flow-frontend    huangxiao-deer-flow-dev-frontend                      Up 5 days                3000/tcp

```

## Results

| Skill | Pass | Thread | Run | Notes |
| --- | --- | --- | --- | --- |
| `batch-video-ingestion` | `True` | `6b3361b6-dd4e-420b-8335-e9f62b025e8c` | `019e8263-6958-7362-8499-8e693b841bdd` | ok |
| `video-search` | `True` | `a5ed59e3-584f-4da2-95fe-eead83528ff7` | `019e8263-fea0-74d3-9e4c-bee5ee50fa2e` | ok |
| `video-embedding-index` | `True` | `8171a60b-9a38-4f6f-bfe3-5c174a9fa182` | `019e8264-8b62-74f3-a886-c0d5b458d31c` | ok |
| `object-statistics` | `True` | `1823af36-dc52-40c3-8044-651961d0b31c` | `019e8265-0d0a-7391-af5a-1ba7b7e69c64` | ok |
