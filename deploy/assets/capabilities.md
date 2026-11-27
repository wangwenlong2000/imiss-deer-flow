# Skill 能力与运行档目录

此目录由 `scripts/build_skill_runtime_manifest.py` 从 Skill、Router Card 和 2026-11-26 依赖盘点生成。运行档分配为部署规划建议；它不是自动执行授权或业务验收结果。

盘点包含 119 个 Skill、338 条依赖声明。依赖镜像元数据只表示包/命令存在记录，不证明导入、服务连接或任务成功。

## 运行档（拟议）

| 运行档 | 初始用途 | 边界 |
|---|---|---|
| `base` | 提示、通用基础处理、法规/业务文本流程 | 外部模型与工具仍按各 Skill 声明配置 |
| `code` | 代码解析、切分、DAG及静态扫描 | Java/C++解析器、切分器和OpenGrep资产须按Skill核实 |
| `network` | 网络流量和PCAP处理 | 依赖tshark、数据格式及RAG服务的能力需分别验收 |
| `geo` | 遥感、轨迹及地理处理 | 可能需要GDAL原生库、大型数据/模型；不可仅凭包清单判定 |
| `video` | 视频处理、索引和视觉服务调用 | ffmpeg、GPU模型、ES和原视频资产按场景配置 |
| `browser` | 网页/微信公众号抓取 | 需要Node、Playwright及浏览器资产；会访问外部站点 |
| `compatibility` | 旧脚本兼容运行档 | 作为过渡选项，后续按实测拆分 |

## Skill 按能力分组

### `basic`

`bootstrap`, `chinese-policy-analysis`, `code2patent`, `consulting-analysis`, `frontend-design`, `legal-document-assistant`, `legal-proposal-generator`, `legal-qa-extractor`, `legal-text-format`, `litigation-analysis`, `patent-analysis`, `surprise-me`

### `code`

`code-business-dag-analysis-pipeline`, `code-semantic-labeler`, `code-splitter-adapter`, `code-to-ast-new`, `dataflow-extractor`, `opengrep-compliance`, `pipeline-graph-builder`, `pipeline-reasoner`

### `legal`

`china-contract-review`, `china-legal-consultation`, `china-legal-issue-analysis`, `chinese-policy-analysis`, `code2patent`, `law-local-retrieval`, `legal-briefing`, `legal-document-assistant`, `legal-proposal-generator`, `legal-qa-extractor`, `legal-text-format`, `litigation-analysis`, `official-document-writer`, `patent-analysis`, `policy-rag`, `trademark-assistant`, `wechat-article-fetch`, `wechat-article-search`

### `network`

`network-traffic-analysis`

### `phone`

`association-path-analysis`, `condition-based-screening`, `dataset-onboarding-graph-preprocess`, `dataset-overview-analysis`, `dataset-quality-and-linkability-diagnostic`, `gang-cluster-analysis`, `graph-operator`, `group-risk-analysis`, `overlap-analysis`, `risk-evidence-pack`, `shared-device-analysis`, `sichuan-shaanxi-comparison`, `single-number-analysis`, `subgraph-extraction-analysis`, `time-series-anomaly-analysis`, `topn-high-risk-discovery`

### `public`

`bootstrap`, `chart-visualization`, `claude-to-deerflow`, `consulting-analysis`, `data-analysis`, `deep-research`, `find-skills`, `frontend-design`, `github-deep-research`, `image-generation`, `podcast-generation`, `ppt-generation`, `skill-creator`, `surprise-me`, `vercel-deploy-claimable`, `video-generation`, `web-design-guidelines`

### `remote_sensing`

`V1+urban-greenspace-assessment`, `building-footprint-extraction`, `disaster-risk-assessment`, `flood-risk-mapping`, `green-space-monitoring`, `illegal-dumping-detection`, `impervious-surface-mapping`, `road-damage-assessment`, `space-data-processing`, `urban-change-detection`, `urban-expansion-analysis`, `urban-heat-assessment`, `water-quality-monitoring`, `write-proposal`

### `street`

`location-matcher`

### `traffic`

`road-traffic-analysis`

### `trajectory`

`analyze_event_impact`, `analyze_od_flow`, `analyze_region_heat`, `analyze_spatiotemporal_accessibility`, `anonymize_trajectory_data`, `classify_trajectory_state`, `clean_trajectory`, `detect_flow_anomaly`, `detect_route_anomaly`, `detect_trajectory_cooccurrence`, `extract_stay_points`, `forecast_region_flow`, `fuse_spatial_evidence`, `map_spatial_grid`, `measure_spatiotemporal_entropy`, `mine_trajectory_patterns`, `predict_next_location`, `profile_urban_region`, `search_similar_trajectory`, `search_spatiotemporal`

### `video`

`analyze-video`, `batch-video-ingestion`, `camera-health-check`, `city-video-intelligence`, `duplicate-event-merge`, `evidence-package-generation`, `evidence-snapshot`, `ffmpeg-utils`, `frame-sampling`, `human-review-routing`, `object-detection`, `object-statistics`, `object-tracking`, `privacy-masking`, `roi-mapping`, `roi-transit-statistics`, `single-video-event-analysis`, `video-embedding-index`, `video-object-analytics`, `video-privacy-masking`, `video-search`, `video-segment-extraction`, `video-stream-ingestion`

## 判定口径

- 清单中的源代码入口仅表示静态发现的调用候选，不代表获准自动执行。
- `required`、`conditional`、`optional`沿用依赖盘点和Router Card条件；条件分支的实际触发由对应动作决定。
- 模型版本、包锁定版本、外部服务在线状态、索引完整性、数据质量和业务准确率需分别验收。
- 本清单不内嵌凭据、生产端点或用户数据；外部服务探测需要由部署环境显式执行。
- 未在源码或盘点中确认的降级行为标记为未知，不推断静默回退。
