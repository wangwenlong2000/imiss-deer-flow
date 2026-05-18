---
name: phone-network-analysis
description: 电话网络数据分析总入口。当前体系包含 23 项能力：8 个基础图分析算子、3 个 YiGraph 风格高级图分析 skill、12 个电话网络业务/数据处理/数据诊断 skill。用于完成电话网络数据接入建图、数据质量与可联动性诊断、数据概览、单号画像、TopN 风险发现、共享设备分析、群体/团伙识别、条件筛选、风险证据包、时间异常分析、地域对比和图关系分析。调用时必须根据用户自然语言问题自动路由到最合适的子 skill；如果用户信息不足，也要优先根据任务意图选择 skill，而不是要求用户明确说出 skill 名称。
allowed-tools: Bash
---

# phone-network-analysis 总 SKILL.md（23 项能力最终版）

## 0. 总原则

本文件是 `skills/custom/phone-network-analysis/SKILL.md` 的总入口说明。

它不是某一个单独脚本的说明，而是整个电话网络数据分析体系的路由与能力总目录。

前端或智能体在收到用户请求时，必须先阅读本文件，再决定使用哪个子 skill、哪个 wrapper 脚本、哪个数据集和哪种附件展示模式。

本目录的核心目标是：

1. 让用户即使不明确说 skill 名称，也能根据问题意图自动路由到正确能力。
2. 让上传原始数据、已建图数据、已有 unified 数据都能进入正确流程。
3. 避免把不支持的数据能力说成支持，例如当前 unified 不支持可靠跨省同实体联动追踪。
4. 保证报告和附件来自真实脚本输出，不编造、不口头代替真实文件。
5. 保证所有最终用户可见解释尽量使用中文，命令、参数、文件名可保留英文。

---

## 1. 当前能力总数与分类

当前电话网络数据分析体系按能力口径共 **23 项**。

分为三大类：

1. **基础图分析算子能力：8 项**
2. **YiGraph 风格高级图分析 skill：3 项**
3. **电话网络业务 / 数据处理 / 数据诊断 skill：12 项**

> 注意：这里的“23 项”是能力口径，不完全等同于 23 个目录。基础图分析算子通常由 `graph-operator` 统一提供，但在总能力中按 8 个基础算子单独计数。

---

## 2. 23 项能力总览表

| 序号 | 分类 | 能力 / skill | 主要作用 |
|---:|---|---|---|
| 1 | 基础图算子 | `node_lookup` / `query_phone_node` | 查询号码节点画像、标签、省份、来源等基础信息 |
| 2 | 基础图算子 | `relationship_filter` | 按条件过滤通话边、设备边、时间边、方向边等关系 |
| 3 | 基础图算子 | `aggregation_query` | 聚合统计、排序、TopN、分组计数 |
| 4 | 基础图算子 | `neighbor_query` / `expand_neighbors` | 一跳 / 多跳邻居展开 |
| 5 | 基础图算子 | `path_query` | 查询两个节点之间的路径和桥接关系 |
| 6 | 基础图算子 | `common_neighbor` | 查询共同邻居、共同对端、共同设备等重叠对象 |
| 7 | 基础图算子 | `subgraph` | 围绕中心节点抽取局部子图 |
| 8 | 基础图算子 | `subgraph_by_nodes` | 围绕一组节点抽取局部子图 |
| 9 | 高级图分析 | `association-path-analysis` | 两号码路径、桥接节点、复合关联链路分析 |
| 10 | 高级图分析 | `overlap-analysis` | 两号码共同对端、共享设备、联系圈重叠分析 |
| 11 | 高级图分析 | `subgraph-extraction-analysis` | 围绕单号抽取局部关系图和关键节点 |
| 12 | 电话网络业务 | `single-number-analysis` | 单号码综合画像、局部关系、共享设备和下钻建议 |
| 13 | 电话网络业务 | `topn-high-risk-discovery` | 动态发现 TopN 高风险号码并排序 |
| 14 | 电话网络业务 | `shared-device-analysis` | 共享设备、设备池、同设备挂载号码分析 |
| 15 | 电话网络业务 | `group-risk-analysis` | 面向号码集合的群体风险模式分析 |
| 16 | 电话网络业务 | `gang-cluster-analysis` | 疑似团伙簇、核心节点、桥接点和证据链分析 |
| 17 | 电话网络业务 | `condition-based-screening` | 按夜间行为、联系人广度、共享设备、标签、省份等条件筛选目标 |
| 18 | 电话网络业务 | `risk-evidence-pack` | 单号码结构化风险证据包 |
| 19 | 电话网络业务 | `time-series-anomaly-analysis` | 号码或群体时间趋势、夜间行为变化、异常突变分析 |
| 20 | 数据概览 | `dataset-overview-analysis` | 总体概览，回答“数据里有什么、能做什么” |
| 21 | 地域对比 | `sichuan-shaanxi-comparison` | 四川与陕西地域风险特征、行为模式、群体结构对比 |
| 22 | 数据接入 | `dataset-onboarding-graph-preprocess` | 原始 CSV/Excel/JSON/Parquet 接入、字段识别、清洗和建图 |
| 23 | 数据诊断 | `dataset-quality-and-linkability-diagnostic` | 检查数据质量、标准图结构、后续 skill 可用性和跨省可联动性 |

---

## 3. 绝对禁止的行为

### 3.1 禁止伪造运行结果

不得在没有运行脚本、没有读取真实输出文件的情况下编造：

- 号码风险分；
- 共享设备数量；
- TopN 排名；
- 团伙簇数量；
- 证据包内容；
- 数据质量结论；
- 可联动性结论。

如果脚本失败，必须说明失败原因，不能假装成功。

### 3.2 禁止把路径当成报告

如果脚本生成 markdown 报告，必须读取报告内容并总结核心结果。

只输出类似下面内容是不合格的：

```text
报告已生成：/mnt/user-data/outputs/xxx.md
```

正确做法：

1. 确认文件存在；
2. 读取 markdown 报告；
3. 展示核心结论；
4. 同时提供下载附件入口。

### 3.3 禁止把不支持的跨省联动说成支持

当前 `unified` 数据经诊断更接近：

```text
四川数据 + 陕西数据按统一 schema 合并
```

它不是可靠的跨省实体统一索引。

因此不得声称当前 unified 可以可靠识别：

- 真实跨省共享设备；
- 真实跨省共同对端；
- 真实跨省强关联号码对；
- 真实跨省关系链路。

如果用户问跨省联动，必须先做或引用 `dataset-quality-and-linkability-diagnostic` 的结论。

### 3.4 禁止上传原始数据后直接调用业务分析

如果用户上传的是原始 CSV / Excel / JSON / Parquet，而不是标准图结构，不能直接调用：

- `single-number-analysis`
- `topn-high-risk-discovery`
- `shared-device-analysis`
- `group-risk-analysis`
- `gang-cluster-analysis`
- `risk-evidence-pack`

必须先调用：

```text
dataset-onboarding-graph-preprocess
```

将原始数据转换为标准三件套。

### 3.5 禁止错误数据不诊断

如果上传数据缺字段、没有号码列、没有设备列、没有时间列，不能简单回复“无法处理”。

应调用：

```text
dataset-onboarding-graph-preprocess
```

或：

```text
dataset-quality-and-linkability-diagnostic
```

输出质量诊断报告，说明为什么不能继续分析。

---

## 4. 标准数据结构

后续大多数电话网络分析 skill 依赖标准图结构。

标准图结构至少包含：

```text
processed/<dataset>/user_nodes.csv
processed/<dataset>/call_edges.csv
processed/graph_views/<dataset>/edges_phone_imei.parquet
```

其中：

### 4.1 `user_nodes.csv`

号码节点表。

典型字段：

```text
user_id
province
dataset_name
label
sub_label
source_table
age
open_card_time
access_mode
monthly_fee
monthly_flow_mb
monthly_call_duration
caller_ratio_3m
caller_dispersion_3m
cross_province_ratio_3m
broadband_flag
```

### 4.2 `call_edges.csv`

通话关系边表。

典型字段：

```text
src_user_id
dst_counterparty_id
event_time
event_date
event_hour
duration
call_type
imei
province
city
county
station
cell
roaming_place
counterparty_belong
source_table
```

### 4.3 `edges_phone_imei.parquet`

号码—设备二部图边表。

典型字段：

```text
user_id
imei
edge_count
src_id
dst_id
src_type
dst_type
edge_type
dataset
```

### 4.4 标准数据结构是否存在的判断

若三件套均存在，通常说明数据具备基本图分析条件。

但是否能做某个具体分析，还要看字段是否齐全：

- 没有设备边：不能做共享设备分析；
- 没有时间字段：不能做时间序列异常分析；
- 没有 label/sub_label：风险标签类分析会降级；
- 只有单一省份：不能做四川-陕西地域对比；
- 两省 ID 命名空间不一致：不能做可靠跨省同实体联动追踪。

---

## 5. 数据集选择规则

### 5.1 默认数据集

如果用户没有指定数据集，默认使用：

```text
dataset=unified
```

默认根目录：

```text
/workspace/imiss-deer-flow-main/datasets/phone-network
```

对应路径：

```text
/workspace/imiss-deer-flow-main/datasets/phone-network/processed/unified/user_nodes.csv
/workspace/imiss-deer-flow-main/datasets/phone-network/processed/unified/call_edges.csv
/workspace/imiss-deer-flow-main/datasets/phone-network/processed/graph_views/unified/edges_phone_imei.parquet
```

### 5.2 用户指定数据集

如果用户明确提到：

- `dataset=xxx`
- “分析 onboarded_loop_demo”
- “使用我刚建图生成的数据集”
- “使用上传数据生成的新数据集”

必须优先使用用户指定的数据集。

### 5.3 前端上传新数据

如果用户上传的是原始表格文件，第一步必须是：

```text
dataset-onboarding-graph-preprocess
```

建图完成后生成新的 dataset 名称，例如：

```text
onboarded_clean_demo
onboarded_messy_demo
onboarded_loop_demo
```

后续分析应使用该 dataset。

### 5.4 已经是标准图结构的数据

如果用户上传或指定的目录已经包含：

```text
user_nodes.csv
call_edges.csv
edges_phone_imei.parquet
```

可以先调用：

```text
dataset-quality-and-linkability-diagnostic
```

确认 `graph_ready=true`，然后再进入业务分析。

---

## 6. 参数调用规则

### 6.1 推荐优先使用 dataset 模式

当前多数新版本 wrapper 支持：

```bash
--dataset-root /workspace/imiss-deer-flow-main/datasets/phone-network
--dataset <dataset_name>
```

例如：

```bash
python3 xxx_wrapper.py \
  --dataset-root /workspace/imiss-deer-flow-main/datasets/phone-network \
  --dataset onboarded_loop_demo
```

### 6.2 显式路径模式作为兼容方式

如果某个旧 wrapper 暂不支持 dataset 模式，或用户明确指定文件路径，应使用：

```bash
--user-node-path /path/to/user_nodes.csv
--call-graph-path /path/to/call_edges.csv
--device-graph-path /path/to/edges_phone_imei.parquet
```

### 6.3 显式路径优先

如果同时提供 dataset 参数和显式路径，显式路径优先。

原因：用户显式传路径通常意味着要覆盖默认推导。

### 6.4 `artifact-mode` 规则

如果 wrapper 支持 `--artifact-mode`，按以下规则使用：

| 模式 | 适用场景 | 前端展示 |
|---|---|---|
| `markdown_only` | 用户只要报告，不要附件冗余 | 只展示 markdown 报告 |
| `essential` | 常规前端使用 | 展示核心报告 + 核心证据附件 |
| `full` | 命令行验收、详细复查 | 展示所有 csv/json/xlsx/parquet 证据附件 |

如果用户明确说“只要 markdown 报告”，必须使用：

```bash
--artifact-mode markdown_only
```

如果用户要求“完整证据附件”，使用：

```bash
--artifact-mode full
```

默认前端推荐使用：

```bash
--artifact-mode essential
```

---

## 7. 输出与附件展示规则

### 7.1 必须展示真实文件

如果脚本输出 JSON 中包含：

- `report_path`
- `artifacts`
- `output_paths`
- `files`
- `summary_json`
- `evidence_xlsx`
- `csv_path`
- `csv_paths`

必须检查文件是否真实存在。

### 7.2 Markdown 报告处理

如果有 markdown 报告：

1. 读取报告正文；
2. 摘要展示核心结论；
3. 保留下载入口；
4. 不要连续重复粘贴同一份报告两遍。

### 7.3 CSV / JSON / XLSX / Parquet 处理

如果用户要求证据附件，应展示真实下载入口。

如果用户没有要求完整附件，前端默认不应展示过多文件，应优先使用 `artifact_mode=essential`。

### 7.4 输出语言

用户可见说明必须以中文为主。

允许保留英文的内容：

- skill 名称；
- 参数名；
- 文件名；
- 字段名；
- 命令行；
- JSON key。

解释性文字不要无故切换成英文。

### 7.5 失败输出

如果脚本失败，必须说明：

1. 哪个脚本失败；
2. 失败命令；
3. 关键错误信息；
4. 可能原因；
5. 下一步修复方法。

不得只说“失败了”。

---

## 8. 自然语言路由总表

| 用户自然语言意图 | 优先 skill |
|---|---|
| 上传原始 CSV/Excel，需要处理成图 | `dataset-onboarding-graph-preprocess` |
| 上传数据无法建图、缺字段、错误数据 | `dataset-onboarding-graph-preprocess` |
| 判断这个数据能不能分析、支持哪些 skill | `dataset-quality-and-linkability-diagnostic` |
| 判断 unified 是否支持跨省实体联动 | `dataset-quality-and-linkability-diagnostic` |
| 数据里有什么、规模多大、能做什么 | `dataset-overview-analysis` |
| 找 TopN 高风险号码、风险名单 | `topn-high-risk-discovery` |
| 完整分析某一个号码 | `single-number-analysis` |
| 解释某个号码为什么高风险、给证据包 | `risk-evidence-pack` |
| 分析某号码共享设备、设备池 | `shared-device-analysis` |
| 两个号码有没有共同设备/共同对端/重叠 | `overlap-analysis` |
| 两个号码之间有没有路径/桥接链路 | `association-path-analysis` |
| 抽一个号码的 1 跳/2 跳局部图 | `subgraph-extraction-analysis` |
| 分析一组号码的群体风险特征 | `group-risk-analysis` |
| 从一组号码里识别疑似团伙簇 | `gang-cluster-analysis` |
| 按夜间、共享设备、省份、标签筛目标 | `condition-based-screening` |
| 分析近期趋势、异常日期、活跃突变 | `time-series-anomaly-analysis` |
| 四川和陕西风险特征对比 | `sichuan-shaanxi-comparison` |
| 跨省共享设备/跨省共同对端 | 先用 `dataset-quality-and-linkability-diagnostic`，当前 unified 不支持可靠判定 |

---

## 9. 基础图分析算子能力（8 项）

基础图算子通常由 `phone-network-analysis/graph-operator` 提供。

它们是上层业务 skill 的底层积木，通常不直接作为最终用户报告入口，除非用户明确要求执行底层图操作。

### 9.1 `node_lookup` / `query_phone_node`

定位：节点画像查询。

适合处理：

- “查这个号码是否存在。”
- “这个号码的标签、省份、来源是什么？”
- “给我这个节点的基础画像。”

输入通常包括：

- `user_id`
- `user_nodes.csv`

输出通常包括：

- 号码是否存在；
- 省份；
- 标签；
- 来源表；
- 基础属性。

常作为以下 skill 的第一步：

- `single-number-analysis`
- `risk-evidence-pack`
- `subgraph-extraction-analysis`

### 9.2 `relationship_filter`

定位：关系边过滤。

适合处理：

- “筛选夜间通话。”
- “筛选某个时间段的边。”
- “筛选设备关系。”
- “筛选某号码的通话边。”

可作用于：

- `call_edges.csv`
- `edges_phone_imei.parquet`

常用于：

- 条件筛选；
- 时间序列分析；
- 单号局部关系；
- 共享设备分析。

### 9.3 `aggregation_query`

定位：聚合统计与排序。

适合处理：

- “统计每个号码通话次数。”
- “统计每个设备挂载多少号码。”
- “统计 TopN 联系人广度。”
- “按省份/标签分组统计。”

常用于：

- `topn-high-risk-discovery`
- `dataset-overview-analysis`
- `condition-based-screening`
- `group-risk-analysis`

### 9.4 `neighbor_query` / `expand_neighbors`

定位：一跳 / 多跳邻居展开。

适合处理：

- “展开这个号码的一跳邻居。”
- “找两跳关系圈。”
- “查看某设备关联哪些号码。”

常用于：

- `single-number-analysis`
- `subgraph-extraction-analysis`
- `gang-cluster-analysis`

### 9.5 `path_query`

定位：路径查询。

适合处理：

- “A 和 B 之间是否有路径？”
- “它们通过哪个中间节点连接？”
- “找到号码之间的桥接链路。”

常用于：

- `association-path-analysis`
- `gang-cluster-analysis`

### 9.6 `common_neighbor`

定位：共同邻居 / 共同对端查询。

适合处理：

- “两个号码有没有共同联系人？”
- “共同对端是谁？”
- “是否处于同一联系圈？”

常用于：

- `overlap-analysis`
- `association-path-analysis`
- `group-risk-analysis`

### 9.7 `subgraph`

定位：围绕一个中心节点抽取局部子图。

适合处理：

- “抽取某号码 2 跳图。”
- “看该号码局部关系圈。”
- “保留关键节点，截断过大子图。”

常用于：

- `subgraph-extraction-analysis`
- `single-number-analysis`

### 9.8 `subgraph_by_nodes`

定位：围绕一组节点抽取子图。

适合处理：

- “分析这组号码内部关系。”
- “抽取候选团伙子图。”
- “查看群体内部设备/对端连接。”

常用于：

- `group-risk-analysis`
- `gang-cluster-analysis`

---

## 10. YiGraph 风格高级图分析 skill（3 项）

### 10.1 `association-path-analysis`

定位：两号码之间的路径型联合分析。

典型用户问法：

- “这两个号码之间有没有路径？”
- “A 和 B 是通过什么连起来的？”
- “有没有中间桥接节点？”
- “输出两个号码之间的关系链路。”

适合输入：

- 两个号码 ID；
- 可选最大跳数；
- 可选关系类型限制。

输出重点：

- 是否存在直接通话；
- 是否存在共享设备路径；
- 是否存在共同对端路径；
- 是否存在桥接节点；
- 路径证据报告。

不要用于：

- 单号码完整风险画像；
- 大规模 TopN 排序；
- 群体/团伙整体识别。

如果用户只给一个号码，应优先考虑 `single-number-analysis` 或 `subgraph-extraction-analysis`。

### 10.2 `overlap-analysis`

定位：两号码之间的重叠关系分析。

典型用户问法：

- “这两个号码有没有共同对端？”
- “它们有没有共享设备？”
- “它们联系圈重叠多少？”
- “两个号码是否属于同一圈层？”

输出重点：

- 共同对端数量；
- 共同对端样例；
- 共享设备数量；
- 重叠比例；
- 重叠强度解释。

不要用于：

- 多号码群体分析，群体应使用 `group-risk-analysis`；
- 团伙簇识别，团伙应使用 `gang-cluster-analysis`。

### 10.3 `subgraph-extraction-analysis`

定位：围绕单号码抽取局部关系图。

典型用户问法：

- “抽取这个号码的一跳子图。”
- “抽取这个号码的两跳关系图。”
- “看看这个号码周围有哪些关键节点。”
- “输出局部关系规模。”

输出重点：

- 截断前/截断后节点数；
- 截断前/截断后边数；
- 局部邻居；
- 推荐下钻节点；
- 共享设备线索。

不要用于：

- 完整单号码风险解释；完整解释使用 `single-number-analysis` 或 `risk-evidence-pack`。

---

## 11. 电话网络业务 / 数据处理 / 数据诊断 skill（12 项）

### 11.1 `single-number-analysis`

定位：单号码综合分析入口。

典型用户问法：

- “完整分析这个号码。”
- “这个号码有什么风险？”
- “输出号码画像、关系规模、可疑节点和下钻建议。”
- “围绕这个号码做综合分析。”

适合输入：

- 单个 `phone_id` / `user_id`；
- `hops`；
- `max_nodes`；
- `top_k`；
- `analysis_mode`；
- `dataset`。

主要输出：

- 号码画像；
- 通话关系规模；
- 共享设备线索；
- Top 可疑节点；
- 桥接点 / 枢纽点；
- 推荐二次下钻对象；
- Markdown 报告。

优先调用场景：

- 用户给了一个具体号码，并要求完整分析。

不要误用：

- 如果用户只是要局部子图，用 `subgraph-extraction-analysis`；
- 如果用户要结构化证据包，用 `risk-evidence-pack`。

### 11.2 `topn-high-risk-discovery`

定位：动态发现 TopN 高风险号码。

典型用户问法：

- “找 Top20 高风险号码。”
- “输出重点关注对象名单。”
- “给我一批最可疑的号码。”
- “按风险分排序。”

适合输入：

- `top_n`；
- `candidate_scope`；
- `analysis_mode`；
- 可选省份、标签过滤；
- `dataset`。

主要输出：

- 候选池规模；
- TopN 风险名单；
- 风险分；
- 风险等级；
- 入榜原因；
- CSV 名单；
- Markdown 报告。

路由规则：

- 只要用户说 TopN、排行榜、风险名单、重点对象，应优先用本 skill。

### 11.3 `shared-device-analysis`

定位：共享设备关系分析。

典型用户问法：

- “这个号码共用了哪些设备？”
- “这个设备下面有哪些号码？”
- “两个号码是否共用设备？”
- “有没有设备池型风险？”

适合模式：

- 单号码模式；
- 设备模式；
- 两号码比较模式；
- 设备池扩散模式。

主要输出：

- 设备数；
- 同设备关联号码；
- 高挂载设备；
- 共享设备证据；
- 可疑扩散对象。

不要误用：

- 如果用户要求完整团伙识别，用 `gang-cluster-analysis`；
- 如果用户只是按条件筛共享设备对象，用 `condition-based-screening`。

### 11.4 `group-risk-analysis`

定位：面向号码集合的群体风险分析。

典型用户问法：

- “分析这一组号码的群体风险。”
- “这组号码是什么类型的风险群体？”
- “识别高通话量、夜间异常、联系人广度异常、共享设备型群体。”
- “输出核心成员和关键证据。”

适合输入：

- 多个号码；
- `phone-id-file`；
- `risk_only`；
- `top_k`；
- `dataset`。

主要输出：

- 群体规模；
- 过滤链路；
- 模式触发情况；
- 核心成员排序；
- 共享设备证据；
- 号码对证据；
- 共同对端证据。

和 `gang-cluster-analysis` 的区别：

- `group-risk-analysis` 更偏群体画像与模式归纳；
- `gang-cluster-analysis` 更偏簇结构、核心节点、桥接点和团伙识别。

### 11.5 `gang-cluster-analysis`

定位：疑似团伙簇识别。

典型用户问法：

- “这一组号码里有没有疑似团伙？”
- “识别紧密关联的小群体。”
- “找核心节点和桥接点。”
- “输出共享设备证据和共同对端证据。”

适合输入：

- 号码集合；
- `phone-id-file`；
- 候选扩展参数；
- `candidate_scope`；
- `dataset`。

主要输出：

- 候选扩展规模；
- 团伙簇列表；
- 重点簇；
- 核心节点；
- 桥接点；
- 共享设备证据；
- 共同对端证据；
- 号码对证据；
- 多 CSV / XLSX 证据文件。

使用注意：

- 如果输入数据很小，可能只能输出轻量簇或无明显团伙，这是正常现象。
- 不能把“疑似团伙簇”写成“已确认团伙”。

### 11.6 `condition-based-screening`

定位：按规则条件筛选目标号码。

典型用户问法：

- “筛出夜间行为明显异常的号码。”
- “筛出联系人广度高的号码。”
- “筛出共享设备数量高的对象。”
- “按省份、标签、夜间占比、共享设备数进行筛选。”

常见筛选条件：

- 夜间通话占比；
- 夜间通话次数；
- 联系人数；
- 通话次数；
- 共享设备数量；
- 标签；
- 省份；
- 风险分。

主要输出：

- 筛选链路；
- 每一步剩余对象数；
- 命中对象列表；
- 共享设备证据；
- 共同对端证据；
- Excel 证据工作簿。

不要误用：

- 不要用它代替四川-陕西地域对比；地域对比应使用 `sichuan-shaanxi-comparison`。

### 11.7 `risk-evidence-pack`

定位：单号码结构化风险证据包。

典型用户问法：

- “给这个号码生成证据包。”
- “为什么这个号码高风险？”
- “把它的风险原因、证据和建议整理出来。”
- “输出可交付的风险证据材料。”

主要输出：

- 号码画像；
- 风险结论；
- 风险原因；
- 通话证据；
- 共享设备证据；
- 共同对端证据；
- 重叠关系证据；
- 后续建议；
- Markdown / CSV / JSON / XLSX 证据包。

和 `single-number-analysis` 的区别：

- `single-number-analysis` 更偏综合分析和下钻发现；
- `risk-evidence-pack` 更偏可交付证据整理和解释。

### 11.8 `time-series-anomaly-analysis`

定位：号码级或群体级时间序列异常分析。

典型用户问法：

- “这个号码最近活跃是否上升？”
- “这组号码是否出现阶段性风险上升？”
- “分析 recent 与 baseline 的变化。”
- “找异常日期和关键贡献对象。”

主要输出：

- 时间覆盖；
- recent 窗口指标；
- baseline 窗口指标；
- 通话量变化；
- 联系人变化；
- 夜间占比变化；
- 异常日期；
- 小时分布；
- 贡献成员 / 对端。

适用前提：

- `call_edges.csv` 中必须存在可用时间字段，如 `event_time` 或 `event_date`。

### 11.9 `dataset-overview-analysis`

定位：数据集总体概览入口。

典型用户问法：

- “这个数据集里有什么？”
- “整体规模多大？”
- “风险标签分布如何？”
- “能做哪些分析？”
- “生成甲方汇报开场版数据总览。”

主要输出：

- 号码规模；
- 风险对象分布；
- 标签分布；
- 省份分布；
- 通话关系规模；
- 共享设备规模；
- 时间覆盖；
- 数据质量；
- 可分析能力清单；
- 技术版报告；
- 演示版报告。

使用建议：

- 对一个新 dataset，完成建图后应优先跑本 skill。

### 11.10 `sichuan-shaanxi-comparison`

定位：四川与陕西地域对比分析。

典型用户问法：

- “比较四川和陕西的风险特征。”
- “两地行为模式有什么不同？”
- “比较群体结构和对象分布。”
- “生成甲方汇报版地域对比报告。”

主要输出：

- 两省号码规模；
- 标签结构；
- 风险占比；
- 通话行为；
- 归一化指标；
- 时间覆盖口径提醒；
- 共享设备规模；
- 群体结构差异；
- 代表性对象。

重要边界：

- 它是地域对比，不是跨省同实体联动追踪。
- 不要用它证明“跨省共享设备”或“跨省团伙”。

### 11.11 `dataset-onboarding-graph-preprocess`

定位：上传原始数据后的接入、字段映射、清洗和建图。

典型用户问法：

- “我上传了新的 CSV，请帮我建图。”
- “把这些原始电话记录转换成标准图结构。”
- “自动识别号码、对端、设备、时间、标签、省份字段。”
- “数据不规整，帮我检查并输出质量报告。”
- “为什么这份数据不能建图？”

支持输入：

- CSV；
- Excel；
- JSON；
- Parquet；
- 多文件混合；
- 中文字段；
- 部分缺失字段；
- call-only / device-only / label-only 边界表。

主要输出：

- `user_nodes.csv`；
- `call_edges.csv`；
- `edges_phone_imei.parquet`；
- `schema_mapping.json`；
- `data_quality_report.csv`；
- `preprocess_summary.json`；
- Markdown 建图报告；
- `graph_ready` 标识。

路由强制规则：

- 只要用户上传原始数据或提到建图、字段映射、清洗，优先用本 skill。
- 错误数据也要调用本 skill 输出诊断，不要直接拒绝。
- `graph_ready=false` 时，后续业务分析不能继续，需要修复字段或数据。

### 11.12 `dataset-quality-and-linkability-diagnostic`

定位：数据质量、标准图结构、后续 skill 可用性和跨省可联动性诊断。

典型用户问法：

- “这个数据集能不能分析？”
- “它支持哪些 skill？”
- “为什么某个 skill 不能用？”
- “这个数据是不是标准图结构？”
- “unified 是否支持四川和陕西跨省联动？”
- “给出后续可调用 skill 的命令模板。”

主要输出：

- 数据文件存在性检查；
- schema 检查；
- 行数概览；
- 图结构是否 ready；
- 时间字段是否可用；
- 设备边是否可用；
- 标签字段是否可用；
- 能力矩阵；
- 下游命令模板；
- 跨省 ID 命名空间诊断；
- linkability score / level；
- Markdown / JSON / CSV 报告。

必须优先使用的场景：

- 用户问“这个数据能不能做某类分析”；
- 用户问“为什么 cross-province-linkage 不能做”；
- 用户问“新建图数据是否能接入后续 skill”；
- 用户不确定应该调用哪个分析 skill。

与 `dataset-overview-analysis` 的区别：

- `dataset-overview-analysis` 回答“数据里有什么、规模多大、能做什么”；
- `dataset-quality-and-linkability-diagnostic` 回答“数据结构是否合格、哪些 skill 技术上可用、哪些能力受数据前提限制”。

---

## 12. 新数据接入闭环流程

如果用户上传原始数据，标准流程如下：

```text
dataset-onboarding-graph-preprocess
→ dataset-quality-and-linkability-diagnostic
→ dataset-overview-analysis
→ topn-high-risk-discovery
→ single-number-analysis / risk-evidence-pack
→ shared-device-analysis / group-risk-analysis / gang-cluster-analysis / time-series-anomaly-analysis
```

### 12.1 第一步：建图

使用：

```text
dataset-onboarding-graph-preprocess
```

输出标准三件套。

### 12.2 第二步：数据体检

使用：

```text
dataset-quality-and-linkability-diagnostic
```

确认：

- `graph_ready=true`；
- 后续 skill 是否 supported；
- 是否缺少时间、设备、标签等字段。

### 12.3 第三步：数据概览

使用：

```text
dataset-overview-analysis
```

生成“数据里有什么、能做什么”的报告。

### 12.4 第四步：风险发现

使用：

```text
topn-high-risk-discovery
```

输出重点关注号码名单。

### 12.5 第五步：重点号码下钻

按需求选择：

- 完整画像：`single-number-analysis`
- 证据包：`risk-evidence-pack`
- 共享设备：`shared-device-analysis`
- 时间趋势：`time-series-anomaly-analysis`

### 12.6 第六步：群体 / 团伙分析

如果用户提供一组号码，选择：

- 群体特征：`group-risk-analysis`
- 团伙簇：`gang-cluster-analysis`

---

## 13. 已有 processed 数据分析流程

如果用户分析已存在的 `unified` 或其它 processed dataset：

```text
dataset-quality-and-linkability-diagnostic
→ dataset-overview-analysis
→ topn-high-risk-discovery
→ single-number-analysis / risk-evidence-pack
→ shared-device-analysis / association-path-analysis / overlap-analysis
→ group-risk-analysis / gang-cluster-analysis
→ time-series-anomaly-analysis
```

说明：

- 如果用户只是想先看数据，直接用 `dataset-overview-analysis`；
- 如果用户担心数据是否能分析，先用 `dataset-quality-and-linkability-diagnostic`；
- 如果用户已经指定号码，直接进入对应号码分析 skill。

---

## 14. 四川-陕西相关流程

### 14.1 地域对比

如果用户说：

- “比较四川和陕西”；
- “两地风险特征差异”；
- “地域对比”；
- “四川陕西数据分析”；

优先使用：

```text
sichuan-shaanxi-comparison
```

### 14.2 跨省联动

如果用户说：

- “跨省共享设备”；
- “跨省共同对端”；
- “跨省强关联对象”；
- “跨省链路”；

必须先使用或引用：

```text
dataset-quality-and-linkability-diagnostic
```

当前 unified 的诊断结论是：

```text
not_linkable_due_to_visible_namespace_difference
```

因此当前不能可靠做真实跨省同实体联动追踪。

### 14.3 不要误导

可以说：

```text
当前数据支持四川-陕西地域对比。
```

不要说：

```text
当前数据已经支持可靠跨省共享设备识别。
```

---

## 15. 常见问题路由示例

### 15.1 用户问：“我上传了几个 Excel，帮我分析。”

应先判断上传文件是否为原始数据。

如果是原始表格：

```text
dataset-onboarding-graph-preprocess
```

不要直接跑 TopN。

### 15.2 用户问：“这个数据集能做哪些分析？”

优先：

```text
dataset-quality-and-linkability-diagnostic
```

如果还要总体规模，再调用：

```text
dataset-overview-analysis
```

### 15.3 用户问：“给我找几个重点号码。”

优先：

```text
topn-high-risk-discovery
```

### 15.4 用户问：“这个号码为什么高风险？”

如果要综合画像：

```text
single-number-analysis
```

如果要可交付证据包：

```text
risk-evidence-pack
```

### 15.5 用户问：“这个号码周围关系图是什么？”

优先：

```text
subgraph-extraction-analysis
```

### 15.6 用户问：“两个号码有没有关系？”

如果问路径：

```text
association-path-analysis
```

如果问共同对端/共享设备/重叠：

```text
overlap-analysis
```

### 15.7 用户问：“这组号码是不是团伙？”

优先：

```text
gang-cluster-analysis
```

如果更偏群体画像：

```text
group-risk-analysis
```

### 15.8 用户问：“筛出夜间异常号码。”

优先：

```text
condition-based-screening
```

### 15.9 用户问：“最近有没有异常突增？”

优先：

```text
time-series-anomaly-analysis
```

### 15.10 用户问：“四川和陕西有什么不同？”

优先：

```text
sichuan-shaanxi-comparison
```

---

## 16. 常见误用纠正

### 16.1 把局部子图当完整单号分析

错误：

```text
用户要求完整分析号码，却只调用 subgraph-extraction-analysis。
```

正确：

```text
使用 single-number-analysis；必要时再调用 subgraph-extraction-analysis 辅助解释。
```

### 16.2 把条件筛选当地域对比

错误：

```text
用户要求四川-陕西对比，却分别用 condition-based-screening 筛四川和陕西再拼报告。
```

正确：

```text
使用 sichuan-shaanxi-comparison。
```

### 16.3 把数据质量支持误解成 wrapper 参数支持

`dataset-quality-and-linkability-diagnostic` 输出某 skill 为 supported，表示数据层具备分析条件。

但早期 wrapper 是否支持 `--dataset-root --dataset` 取决于脚本版本。

如已统一参数，优先用 dataset 模式；未统一时使用诊断报告中的显式路径命令模板。

### 16.4 把跨省 ID 无交集解释为真实无联动

错误。

当前 unified 中四川和陕西 ID 命名空间不同，不能这么解释。

正确说法：

```text
当前哈希后的数据表中没有可直接相等匹配的跨省实体，且存在可见命名空间差异，因此不能可靠判断真实跨省联动。
```

### 16.5 错误数据不调用建图 skill

错误。

错误数据也要调用 `dataset-onboarding-graph-preprocess` 输出质量诊断。

---

## 17. 每个业务 skill 的最低输入条件

| skill | 最低输入条件 |
|---|---|
| `dataset-onboarding-graph-preprocess` | 原始表格目录，至少应有号码列；缺号码列也可输出诊断 |
| `dataset-quality-and-linkability-diagnostic` | dataset-root + dataset，或标准三件套路径 |
| `dataset-overview-analysis` | `user_nodes.csv`，最好有 `call_edges.csv` 和设备边 |
| `topn-high-risk-discovery` | `user_nodes.csv` + 至少一种关系边 |
| `single-number-analysis` | 指定 phone_id，且节点表存在 |
| `risk-evidence-pack` | 指定 phone_id，且节点表存在 |
| `shared-device-analysis` | 设备边存在；单号/设备/两号模式至少满足对应参数 |
| `group-risk-analysis` | 多个号码或 `phone-id-file` |
| `gang-cluster-analysis` | 多个号码或 `phone-id-file`，最好有通话边和设备边 |
| `condition-based-screening` | 节点表 + 条件所需字段；夜间筛选需要时间字段 |
| `time-series-anomaly-analysis` | 通话边中存在时间字段 |
| `sichuan-shaanxi-comparison` | 至少包含四川和陕西两个省份 |
| `association-path-analysis` | 两个号码，最好有通话边/设备边/共同对端 |
| `overlap-analysis` | 两个号码，最好有通话边/设备边 |
| `subgraph-extraction-analysis` | 单个中心号码，至少有一种关系边 |

---

## 18. 文件命名与目录约定

### 18.1 输出目录

用户可见报告和附件通常输出到：

```text
/mnt/user-data/outputs/
```

### 18.2 项目数据目录

标准数据目录：

```text
/workspace/imiss-deer-flow-main/datasets/phone-network/
```

### 18.3 processed 数据目录

```text
/workspace/imiss-deer-flow-main/datasets/phone-network/processed/<dataset>/
```

### 18.4 graph views 目录

```text
/workspace/imiss-deer-flow-main/datasets/phone-network/processed/graph_views/<dataset>/
```

### 18.5 日志目录

如测试脚本支持日志，应输出到：

```text
/workspace/imiss-deer-flow-main/logs/
```

---

## 19. 推荐前端回答格式

当执行某个 skill 后，前端最终回答建议包含：

```text
分析完成。

本次使用 skill：xxx
数据集：xxx
处理状态：ok / graph_ready / not_graph_ready / partial

核心结论：
1. ...
2. ...
3. ...

生成文件：
- Markdown 报告
- CSV / JSON / XLSX / Parquet 附件（按 artifact_mode 控制）

后续建议：
- 可以继续调用 xxx
- 如果要下钻，可提供号码 ID 或号码集合
```

不要在用户没有要求时输出过长命令过程。

---

## 20. 典型端到端示例

### 20.1 上传新数据后完整分析

用户：

```text
我上传了一批电话网络 CSV，帮我分析有没有高风险对象。
```

正确流程：

```text
1. dataset-onboarding-graph-preprocess
2. dataset-quality-and-linkability-diagnostic
3. dataset-overview-analysis
4. topn-high-risk-discovery
```

不要直接从上传 CSV 调 TopN。

### 20.2 新建图数据单号下钻

用户：

```text
对刚才 Top1 号码做完整分析。
```

正确流程：

```text
single-number-analysis
```

如果用户要证据包：

```text
risk-evidence-pack
```

### 20.3 数据是否支持跨省联动

用户：

```text
这个 unified 能不能找跨省共享设备？
```

正确流程：

```text
dataset-quality-and-linkability-diagnostic
```

结论要说明当前 unified 的命名空间限制。

---

## 21. 当前不作为正式完成项的能力

### 21.1 `cross-province-linkage-analysis`

该方向曾尝试实现，但当前 unified 数据不满足可靠跨省实体统一索引条件。

因此暂不作为正式完成业务 skill。

可保留为未来方向：

- 如果提供统一哈希规则；
- 如果提供实体映射表；
- 如果提供原始 ID；
- 如果能重建统一脱敏流程。

### 21.2 `compliance-and-desensitization-control`

这是后续可做的交付合规检查 skill。

它与课题二合规评测有关，但当前合规评测工作应单独作为标注指南和评测任务推进，不混入本阶段 23 项已完成能力。

---

## 22. 维护规则

每新增或修改一个子 skill，必须同步更新本文件以下部分：

1. YAML `description`；
2. 当前能力总数；
3. 23 项能力总览表；
4. 对应 skill 的详细说明；
5. 自然语言路由总表；
6. 最低输入条件表；
7. 推荐流程；
8. 常见误用纠正；
9. 数据限制说明。

不得只改子目录 `SKILL.md` 而不更新总入口。

---

## 23. 新增 skill 描述模板

新增 skill 时，按下面模板写入本文件：

```md
### `skill-name`

定位：一句话说明它解决什么问题。

典型用户问法：
- 问法 1
- 问法 2
- 问法 3

适合输入：
- 输入 1
- 输入 2

主要输出：
- 输出 1
- 输出 2
- 输出 3

优先调用场景：
- 场景 1
- 场景 2

不要误用：
- 不适合场景 1
- 不适合场景 2
```

---

## 24. 当前最终状态

截至当前版本，`phone-network-analysis` 正式纳入说明的能力为：

- 8 项基础图分析算子；
- 3 项 YiGraph 风格高级图分析 skill；
- 12 项电话网络业务 / 数据处理 / 数据诊断 skill；
- 合计 23 项能力。

当前体系已经支持：

```text
原始数据上传
→ 建图预处理
→ 数据质量与可联动性诊断
→ 数据概览
→ TopN 风险发现
→ 单号分析 / 风险证据包
→ 共享设备 / 群体 / 团伙 / 时间异常 / 地域对比
```

当前体系不应声称支持：

```text
在没有统一哈希规则、实体映射表或原始 ID 的情况下，可靠识别四川和陕西真实跨省同实体联动关系。
```

本文件是前端自动路由和项目交付说明的总依据。
