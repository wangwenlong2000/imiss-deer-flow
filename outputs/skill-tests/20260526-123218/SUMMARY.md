# Video Monitoring Skills Agent Chain Test Summary

- started_at: `2026-05-26T12:32:18+0800`
- finished_at: `2026-05-26T12:41:59+0800`
- total: `14`
- passed: `14`
- failed: `0`

## Docker Services

```text
NAMES                            IMAGE                                                 STATUS                        PORTS
deer-flow-sandbox-350754c3       huangxiao-deerflow-sandbox:network-tools              Up About a minute (healthy)   0.0.0.0:8183->8080/tcp, [::]:8183->8080/tcp
deer-flow-sandbox-6ccaa818       huangxiao-deerflow-sandbox:network-tools              Up 2 minutes (healthy)        0.0.0.0:8182->8080/tcp, [::]:8182->8080/tcp
huangxiao-deer-flow-nginx        nginx:alpine                                          Up 17 hours                   80/tcp, 0.0.0.0:3230->3230/tcp, [::]:3230->3230/tcp
huangxiao-deer-flow-langgraph    huangxiao-deer-flow-dev-langgraph                     Up 17 hours                   2024/tcp, 8001/tcp
huangxiao-deer-flow-gateway      huangxiao-deer-flow-dev-gateway                       Up 17 hours                   2024/tcp, 8001/tcp
huangxiao-deer-flow-frontend     huangxiao-deer-flow-dev-frontend                      Up 17 hours                   3000/tcp

```

## Results

| Skill | Pass | Thread | Run | Notes |
| --- | --- | --- | --- | --- |
| `analyze-video` | `True` | `a4f8d126-c42d-4f2f-871f-032723e4ca2c` | `019e6290-9f3f-7db0-8c26-46f56e2e7d47` | ok |
| `camera-health-check` | `True` | `46b77f15-80dd-4357-9551-e866831d9bbc` | `019e628e-e50e-7421-97f0-a4ff8d67e8f3` | ok |
| `duplicate-event-merge` | `True` | `2159cf9f-735c-40d9-ad8b-4e595151c0ac` | `019e6294-fa63-75a2-bd01-cc23829d1a57` | ok |
| `evidence-snapshot` | `True` | `66d1ff14-03a6-4672-b4bd-2e74a562d608` | `019e6292-e5ad-7810-91cf-d526b902098f` | ok |
| `ffmpeg-utils` | `True` | `151667d4-ce28-4072-94c9-6b6992215d0b` | `019e6295-856e-7b12-90c1-18c8fc7cb398` | ok |
| `frame-sampling` | `True` | `c90abf29-f642-44d9-acdc-d6557d9924db` | `019e628e-6e51-76d3-9388-553c2882e1c7` | ok |
| `human-review-routing` | `True` | `05ed696a-0ce3-4529-a56f-a6f7e12eb12a` | `019e6294-8ed9-7e91-84c2-a3cabfc7a3cc` | ok |
| `object-detection` | `True` | `72180333-05ee-48e8-92d2-bf03ce6f7fc6` | `019e628f-4f91-7481-939f-bc18462207b2` | ok |
| `object-tracking` | `True` | `898496a3-3eb6-4350-82fb-a8153f234942` | `019e628f-c07c-7501-9fe6-06d430c0fbc1` | ok |
| `privacy-masking` | `True` | `83787a4d-eb1e-4d02-bb92-a0867fb7dc25` | `019e6293-fd48-7b51-a944-e3c888d9f396` | ok |
| `roi-mapping` | `True` | `75adb0d4-da1a-4c71-9d66-7942ef8180db` | `019e6290-2f58-7581-9970-f5100854a45b` | ok |
| `single-video-event-analysis` | `True` | `48fc0233-afda-45bc-8339-ce8da79a54e9` | `019e6291-2646-7fa1-adec-7a68440bb678` | ok |
| `video-segment-extraction` | `True` | `8d1db24a-4441-4cb0-b3d2-033977b8cf5d` | `019e6293-660f-7a81-8109-7282e4f79c28` | ok |
| `video-stream-ingestion` | `True` | `0f35a3ac-6437-4bbd-82f8-4b3844965641` | `019e628e-13ec-7bf1-b9c0-adddf183edb6` | ok |
