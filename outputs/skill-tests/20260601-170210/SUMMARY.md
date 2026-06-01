# Video Monitoring Skills Agent Chain Test Summary

- started_at: `2026-06-01T17:02:10+0800`
- finished_at: `2026-06-01T17:03:33+0800`
- total: `3`
- passed: `3`
- failed: `0`

## Docker Services

```text
NAMES                           IMAGE                                                 STATUS                        PORTS
deer-flow-sandbox-7b8dc5b5      huangxiao-deerflow-sandbox:network-tools              Up About a minute (healthy)   0.0.0.0:8186->8080/tcp, [::]:8186->8080/tcp
deer-flow-sandbox-415c20c8      huangxiao-deerflow-sandbox:network-tools              Up 2 minutes (healthy)        0.0.0.0:8185->8080/tcp, [::]:8185->8080/tcp
deer-flow-sandbox-cf3f9550      huangxiao-deerflow-sandbox:network-tools              Up 6 minutes (healthy)        0.0.0.0:8183->8080/tcp, [::]:8183->8080/tcp
deer-flow-sandbox-df9ec218      huangxiao-deerflow-sandbox:network-tools              Up 7 minutes (healthy)        0.0.0.0:8182->8080/tcp, [::]:8182->8080/tcp
deer-flow-sandbox-994cb0e2      huangxiao-deerflow-sandbox:network-tools              Up 7 minutes (healthy)        0.0.0.0:8184->8080/tcp, [::]:8184->8080/tcp
huangxiao-deer-flow-nginx       nginx:alpine                                          Up 5 days                     80/tcp, 0.0.0.0:3230->3230/tcp, [::]:3230->3230/tcp
huangxiao-deer-flow-gateway     huangxiao-deer-flow-dev-gateway                       Up 5 days                     2024/tcp, 8001/tcp
huangxiao-deer-flow-langgraph   huangxiao-deer-flow-dev-langgraph                     Up 5 days                     2024/tcp, 8001/tcp
huangxiao-deer-flow-frontend    huangxiao-deer-flow-dev-frontend                      Up 5 days                     3000/tcp

```

## Results

| Skill | Pass | Thread | Run | Notes |
| --- | --- | --- | --- | --- |
| `video-search` | `True` | `a7ed3194-4e10-4a18-aa02-d573fda074ab` | `019e826b-4ce3-7032-bb19-b13077e164ef` | ok |
| `video-embedding-index` | `True` | `42ffb668-b855-4d3b-87ea-b626ec0cf4d7` | `019e826b-b464-7991-b306-855218c0d484` | ok |
| `object-statistics` | `True` | `0cd6e325-164a-4773-b7f1-5e1ec6fa03f9` | `019e826c-239b-7de0-a77a-9b07fce32cb9` | ok |
