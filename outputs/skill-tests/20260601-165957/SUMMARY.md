# Video Monitoring Skills Agent Chain Test Summary

- started_at: `2026-06-01T16:59:57+0800`
- finished_at: `2026-06-01T17:01:03+0800`
- total: `2`
- passed: `2`
- failed: `0`

## Docker Services

```text
NAMES                           IMAGE                                                 STATUS                   PORTS
deer-flow-sandbox-cf3f9550      huangxiao-deerflow-sandbox:network-tools              Up 4 minutes (healthy)   0.0.0.0:8183->8080/tcp, [::]:8183->8080/tcp
deer-flow-sandbox-df9ec218      huangxiao-deerflow-sandbox:network-tools              Up 5 minutes (healthy)   0.0.0.0:8182->8080/tcp, [::]:8182->8080/tcp
deer-flow-sandbox-994cb0e2      huangxiao-deerflow-sandbox:network-tools              Up 5 minutes (healthy)   0.0.0.0:8184->8080/tcp, [::]:8184->8080/tcp
huangxiao-deer-flow-nginx       nginx:alpine                                          Up 5 days                80/tcp, 0.0.0.0:3230->3230/tcp, [::]:3230->3230/tcp
huangxiao-deer-flow-gateway     huangxiao-deer-flow-dev-gateway                       Up 5 days                2024/tcp, 8001/tcp
huangxiao-deer-flow-langgraph   huangxiao-deer-flow-dev-langgraph                     Up 5 days                2024/tcp, 8001/tcp
huangxiao-deer-flow-frontend    huangxiao-deer-flow-dev-frontend                      Up 5 days                3000/tcp

```

## Results

| Skill | Pass | Thread | Run | Notes |
| --- | --- | --- | --- | --- |
| `video-search` | `True` | `1e97f94f-0217-4bf2-86b7-e957bcd52cfd` | `019e8269-465b-74c1-8bd3-8669d10ca4d5` | ok |
| `video-embedding-index` | `True` | `4c58a1b6-1238-40a6-9cb0-f6cf316d670b` | `019e8269-cb72-74a2-aefa-7a5dffaf207a` | ok |
