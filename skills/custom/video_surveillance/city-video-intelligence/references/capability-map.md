# City Video Intelligence Capability Map

Use this reference when classifying video monitoring requests into business capabilities and downstream custom skills.

## Capability Table

| Capability | Solves | Typical Requests | Inputs | Outputs | Downstream Skills | Code Policy |
| --- | --- | --- | --- | --- | --- | --- |
| `video_asset_retrieval` | Find indexed videos or clips | Search bus videos; find Dacheng Road intersection footage; check `CAM_DEERFLOW_001` yesterday morning; find clips with more than 10 cars | keyword, place, camera, time, object label, count condition, video name | video hits, clips, matched metadata, match reason | `video-search`, `object-statistics` | Do not write code |
| `semantic_video_retrieval` | Find visually similar content from natural language | Find traffic congestion; pedestrians crossing; evening street footage; rainy road footage; lane-changing clips | natural-language query, optional time/place/camera | ranked similar clips, scores, semantic reason | `video-search`, `video-embedding-index` | Do not write code |
| `single_video_event_understanding` | Understand what happened in one video | Analyze fighting video; detect accident; check fall; intrusion; smoke/fire; crowd gathering | video path, camera, target events | visual timeline, event candidates, evidence frames, confidence, review flag | `single-video-event-analysis`, `frame-sampling`, optional `object-detection` | Do not write code |
| `object_statistics` | Count and summarize objects | Vehicle count today; pedestrians in last hour; vehicle type distribution; traffic flow by time; bus/car ratio; average dwell time | camera, time range, object types, grouping dimension | counts, distributions, trends, report summary | `object-statistics`, `object-tracking`, optional `roi-mapping` | Do not write code |
| `evidence_preservation` | Turn results into deliverable evidence | Clip 30 seconds around event; privacy mask faces/plates; export timestamped snapshots; package with hash | event, video, time range, privacy requirement | snapshots, clips, masked copies, hashes, manifest/package | `evidence-package-generation`, `evidence-snapshot`, `video-segment-extraction`, `privacy-masking`, `ffmpeg-utils` | Do not write code |
| `video_library_governance` | Make videos searchable and manageable | Ingest upload to Elasticsearch; batch import directory; generate StreetModel vectors; check storage; delete test data | files, directory, index, owner, ingestion mode | video ids, ingestion status, embedding status, failures | `video-stream-ingestion`, `batch-video-ingestion`, `video-embedding-index` | Confirm destructive actions |
| `camera_health_operations` | Check camera availability and quality | Is camera online; blurry or occluded; black screen or frozen; health report | camera id, video/frames, time range | health status, issues, severity, operations advice | `camera-health-check`, `frame-sampling` | Do not write code |
| `cross_video_investigation` | Correlate across cameras/time | Compare two intersections; track a vehicle; heatmap people flow; same person across cameras; suspicious behavior search | cameras, time range, target object/behavior | comparison, correlation chain, risk leads | `video-search`, `object-tracking`, `object-statistics`, `roi-mapping`, semantic search | Mark partial support when identity matching or heatmaps are unavailable |
| `business_scenario_orchestration` | Complete role-based workflows | Traffic police retrieval; property security review; urban management crowd statistics; emergency fire-risk search; legal evidence package | role, place, time, goal, deliverable | business report, evidence, review items, next action | Select one or more capabilities above | Do not write code |
| `development_test` | Evaluate system quality | Test StreetModel embedding; compare keyword and vector search; benchmark ingestion speed | test set, queries, metrics | test report, comparison, benchmark | related skills and test drivers | Code allowed only for explicit testing/development requests |

## Scenario Playbooks

| Scenario | Trigger Examples | Recommended Chain | Deliverable |
| --- | --- | --- | --- |
| `traffic_police_video_review` | traffic police, road, intersection, accident, congestion, evidence | `video-search` -> optional `single-video-event-analysis` -> optional `evidence-package-generation` | relevant footage, event summary, evidence clips |
| `property_security_review` | property, community gate, stranger, intrusion, night footage | `video-search` -> `single-video-event-analysis` -> optional `human-review-routing` | suspicious entry timeline, evidence frames, review recommendation |
| `urban_management_flow_statistics` | urban management, business district, pedestrian flow, peak time | `object-statistics` -> optional `video-search` | peak period, count/trend table, report summary |
| `emergency_fire_risk_search` | emergency management, fire risk, smoke, flame, fire | `video-search` semantic mode -> `single-video-event-analysis` -> `evidence-package-generation` | risk candidates, evidence frames, human-review list |
| `legal_evidence_package` | court, legal evidence, hash, integrity, submit evidence | `evidence-package-generation` -> optional `privacy-masking` | manifest, clips, snapshots, hashes, privacy-safe copy |

## Capability Gaps

Return `capability_gap` instead of writing code when the request requires:

- real-time RTSP/GB28181 camera connection not already represented as local video or indexed data;
- reliable identity recognition across cameras without an existing identity-matching skill;
- heatmap visualization when only raw object counts are available;
- OCR/text extraction from video frames;
- deleting or rewriting shared indices without explicit confirmation.
