---
name: dataset-overview-analysis
description: 电话网络数据集总体概览分析 skill。用于统计对象规模、风险对象分布、通话关系规模、共享设备规模、时间覆盖、数据质量和当前可分析能力范围，回答“数据里有什么、能做什么”，并生成技术报告和演示开场报告。
allowed-tools: Bash
---

# dataset-overview-analysis

## 一、这个 skill 是做什么的

`dataset-overview-analysis` 是电话网络数据分析链路里的**数据集总览入口 skill**。

它不负责深挖某一个号码，也不直接判断某个团伙是否可疑，而是先回答：

- 当前电话网络数据里有多少号码？
- 风险标签对象有多少，分布如何？
- 通话关系规模多大？
- 是否有时间字段，能不能做时间序列异常分析？
- 是否有设备关系，能不能做共享设备分析？
- 当前数据支持哪些后续分析 skill？
- 数据质量是否存在明显缺失、重复或字段识别问题？
- 如果用于甲方演示，应该怎样概括“数据里有什么、能做什么”？

它适合作为甲方演示或完整分析流程的第一步。

---

## 二、默认数据集规则

前端和命令行都按同一套规则执行：

1. 如果用户明确指定 `dataset-root` 或数据集路径，优先使用用户指定的数据。
2. 如果用户只说“使用 unified 数据集”或“使用已预处理好的电话网络数据”，默认使用：

```text
/workspace/imiss-deer-flow-main/datasets/phone-network
```

并默认分析：

```text
processed/unified/
```

3. 如果用户没有指定数据集，也没有额外上传新数据，默认使用已预处理好的 `unified` 电话网络数据。
4. 该 skill 当前面向**已处理好的图结构数据**，包括：
   - `user_nodes.csv`
   - `call_edges.csv`
   - `edges_phone_imei.parquet` 或 `edges_phone_imei.csv`
5. 如果前端上传的是原始 CSV/Excel 明细表，而不是上述图结构数据，需要先经过数据接入/建图流程，不能直接用本 skill 当作清洗建图工具。

---

## 三、它输出什么

脚本会生成一组完整证据文件：

- `dataset_overview_<dataset>.md`：技术版总体概览 markdown 报告
- `dataset_overview_<dataset>_presentation.md`：演示开场版 markdown 报告
- `dataset_overview_<dataset>_summary.json`：结构化摘要
- `dataset_overview_<dataset>_evidence.xlsx`：Excel 证据工作簿
- `dataset_overview_<dataset>_overview_counts.csv`：核心规模统计
- `dataset_overview_<dataset>_label_distribution.csv`：标签分布
- `dataset_overview_<dataset>_sub_label_distribution.csv`：子标签分布
- `dataset_overview_<dataset>_province_distribution.csv`：省份分布
- `dataset_overview_<dataset>_daily_overview.csv`：日级通话概览
- `dataset_overview_<dataset>_hourly_top.csv`：小时分布 Top
- `dataset_overview_<dataset>_top_callers.csv`：高活跃源号码
- `dataset_overview_<dataset>_top_counterparties.csv`：公共对端
- `dataset_overview_<dataset>_top_shared_devices.csv`：共享设备样例
- `dataset_overview_<dataset>_top_phone_device_counts.csv`：号码设备数 Top
- `dataset_overview_<dataset>_data_quality.csv`：数据质量检查
- `dataset_overview_<dataset>_available_capabilities.csv`：可分析能力清单

前端展示时，必须展示 markdown 报告，并提供 csv/xlsx/json 附件下载入口。若用户要求“甲方演示开场版”“汇报版”“更通俗的总览”，优先读取并展示 `_presentation.md`。

---

## 四、适合处理的问题

当用户问下面这些问题时，优先使用本 skill：

- “这个电话网络数据集里有什么？”
- “帮我看一下数据规模和标签分布。”
- “这个数据能做哪些分析？”
- “当前数据有没有共享设备关系？”
- “当前数据有没有时间字段，能不能做时间序列分析？”
- “先给我一个电话网络数据总体概览。”
- “甲方演示前，先生成一个数据概览报告。”
- “生成一份适合演示开场的数据总览。”

---

## 五、不适合处理的问题

下面这些问题不要优先用本 skill：

- “完整分析某个号码” → 用 `single-number-analysis` 或 `risk-evidence-pack`
- “找 TopN 高风险号码” → 用 `topn-high-risk-discovery`
- “筛出夜间异常/共享设备明显的号码” → 用 `condition-based-screening`
- “两个号码怎么连起来” → 用 `association-path-analysis`
- “两个号码有没有共同对端/共享设备” → 用 `overlap-analysis`
- “抽取某个号码的一跳/两跳局部图” → 用 `subgraph-extraction-analysis`
- “分析一组号码像什么群体” → 用 `group-risk-analysis`
- “识别疑似团伙簇” → 用 `gang-cluster-analysis`
- “分析某号码最近是否异常活跃” → 用 `time-series-anomaly-analysis`

---

## 六、命令行调用方式

### 1. 标准 unified 数据概览

```bash
cd /workspace/imiss-deer-flow-main/skills/custom/phone-network-analysis/dataset-overview-analysis/scripts
python3 dataset_overview_wrapper.py \
  --dataset-root /workspace/imiss-deer-flow-main/datasets/phone-network \
  --dataset unified \
  --top-k 20
```

### 2. 一键回归测试

```bash
cd /workspace/imiss-deer-flow-main/skills/custom/phone-network-analysis/dataset-overview-analysis/scripts
bash test_dataset_overview_analysis.sh
```

---

## 七、前端推荐提示词

### Q1：总体概览

请使用 `dataset-overview-analysis` skill，对已预处理好的 unified 电话网络数据进行总体概览分析，输出对象规模、风险标签分布、通话关系规模、共享设备规模、时间覆盖、数据质量和可分析能力范围，并生成 markdown 报告和 csv/xlsx/json 附件。

### Q2：甲方演示入口

请使用 `dataset-overview-analysis` skill，生成一份适合甲方演示开场使用的电话网络数据总览报告，重点回答“数据里有什么、能做什么、后续可以调用哪些分析 skill”。优先展示 `dataset_overview_unified_presentation.md`，同时保留技术版报告和证据附件。

### Q3：数据质量与能力检查

请使用 `dataset-overview-analysis` skill，检查当前 unified 电话网络数据是否具备节点、通话关系、共享设备、时间序列和标签分布分析能力，并输出数据质量检查表和可用能力清单。

---

## 八、分析逻辑和基础算子对齐

本 skill 是通过基础图分析算子组合实现的：

- 对象规模与标签分布 = `node_lookup + aggregation_query`
- 通话关系规模与公共对端 = `relationship_filter + aggregation_query`
- 共享设备概览 = `query_shared_device + aggregation_query`
- 时间覆盖与小时分布 = `relationship_filter(time window/hour) + aggregation_query`
- 可分析能力范围 = 基于基础算子可用性和 skill 路由映射生成

也就是说，它不是孤立脚本，而是整个电话网络 skill 体系的“数据入口总览层”。

---

## 九、前端展示要求

脚本返回 JSON 后，前端/调用链路必须：

1. 读取 `report_path` 对应的技术版 markdown 报告。
2. 如果用户要求演示版、汇报版、开场版，读取 `files.presentation_md` 对应的演示报告。
3. 展示 markdown 报告正文摘要。
4. 将 `artifacts` 中的 markdown、csv、xlsx、json 文件作为附件展示或提供下载入口。
5. 不要只把路径字符串展示给用户。
6. 如果某些附件文件存在但没有展示，属于前端展示链路问题，不属于脚本执行失败。

---

## 十、后续推荐分析链路

本 skill 通常作为第一步。完成数据概览后，推荐按下面链路继续：

1. 如果要找重点对象：`topn-high-risk-discovery` 或 `condition-based-screening`
2. 如果要解释单个号码：`risk-evidence-pack` 或 `single-number-analysis`
3. 如果要分析设备池：`shared-device-analysis`
4. 如果要分析两个号码关系：`association-path-analysis` 或 `overlap-analysis`
5. 如果只想看局部邻居结构：`subgraph-extraction-analysis`
6. 如果要分析号码集合：`group-risk-analysis` 或 `gang-cluster-analysis`
7. 如果要分析阶段性变化：`time-series-anomaly-analysis`

---

## 十一、输出字段说明

JSON 顶层字段包括：

- `ok`：脚本是否正常执行
- `status`：`ok` 或 `partial`
- `dataset`：当前分析的数据集名称
- `input_summary`：输入路径和参数
- `result.node_summary`：节点和风险分布概览
- `result.call_summary`：通话关系和时间覆盖概览
- `result.device_summary`：共享设备概览
- `result.quality_summary`：数据质量检查摘要
- `result.capability_summary`：可分析能力摘要
- `artifacts`：所有生成文件
- `files.presentation_md`：演示开场版报告路径
- `report_path`：技术版 markdown 报告路径

---

## 十二、注意事项

- 本 skill 的目标是“总览”，不是对单个号码或群体做最终研判。
- 如果某个数据表缺失，脚本会尽量输出 partial 结果，而不是直接失败。
- 如果数据没有时间字段，时间序列分析能力会标记为受限。
- 如果数据没有设备关系表，共享设备分析能力会标记为受限。
- 如果数据没有标签字段，风险对象分布会标记为未知。
- “有记录日期”不等于连续日历跨度。技术版和演示版报告都会说明该差异，避免把跨年数据误读成连续 289 天数据。
- 演示报告不输出主观“数据质量评分”，只展示脚本实际检查得到的缺失项、警告项和可用能力。
