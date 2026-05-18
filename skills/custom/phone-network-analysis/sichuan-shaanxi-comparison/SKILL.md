# sichuan-shaanxi-comparison

## 作用定位

`sichuan-shaanxi-comparison` 是电话网络数据分析体系中的**地域对比分析 skill**，面向已经预处理成统一图结构的电话网络数据，专门比较四川与陕西两个地区在对象规模、风险标签、通话行为、共享设备、群体结构和代表性对象上的差异。

它用于回答：

- 四川和陕西的数据规模有什么不同？
- 两地风险对象分布有什么不同？
- 两地通话活跃度、联系人广度、夜间行为有什么不同？
- 两地共享设备和设备池风险是否不同？
- 两地群体结构和代表性对象有什么差异？
- 后续应调用哪些 skill 继续下钻？

本 skill 不输出最终定性结论，只输出基于当前数据的地域差异证据和后续研判入口。

---

## 必须注意：不要用其他 skill 替代本 skill

当前任务只要明确要求“四川-陕西对比”“四川与陕西地域对比”“两省风险特征/行为模式/群体结构差异”，必须优先运行本目录下的：

```bash
sichuan_shaanxi_comparison_wrapper.py
```

不要用 `condition-based-screening` 的两个省份筛选结果手工拼接来替代本 skill。原因是：

- `condition-based-screening` 是条件筛选工具，适合回答“满足某些条件的对象有哪些”；
- `sichuan-shaanxi-comparison` 是全量地域对比工具，适合回答“两地整体差异是什么”；
- 两者可以互补，但不能互相替代。

本 skill 生成的正式文件名必须以：

```text
sichuan_shaanxi_comparison_
```

开头。如果输出文件全是 `condition_screening_...`，说明前端没有正确执行本 skill。

---

## 适用数据

默认使用电话网络统一数据集：

```text
/mnt/datasets/phone-network/processed/unified/user_nodes.csv
/mnt/datasets/phone-network/processed/unified/call_edges.csv
/mnt/datasets/phone-network/processed/graph_views/unified/edges_phone_imei.parquet
```

在本地项目 Docker 容器中测试时，通常路径为：

```text
/workspace/imiss-deer-flow-main/datasets/phone-network/processed/unified/user_nodes.csv
/workspace/imiss-deer-flow-main/datasets/phone-network/processed/unified/call_edges.csv
/workspace/imiss-deer-flow-main/datasets/phone-network/processed/graph_views/unified/edges_phone_imei.parquet
```

如果用户没有指定数据集，优先使用：

```text
dataset=unified
```

---

## 与基础算子的关系

本 skill 是由电话网络基础图分析算子组合实现的，不是单独脱离现有体系的新逻辑。

| 分析内容 | 对应基础算子风格 |
|---|---|
| 对象规模、标签分布、省份分布 | `node_lookup + aggregation_query` |
| 通话记录规模、联系人广度、夜间占比 | `relationship_filter + aggregation_query` |
| 共享设备、设备池、跨省设备样例 | `query_shared_device + aggregation_query` |
| 代表性对象排序 | `aggregation_query + scoring_layer` |
| 群体结构指标 | `subgraph_by_nodes / aggregation_query + scoring_layer` |
| 地域差异解释 | `province filter + metric contrast + evidence ranking` |

---

## 分析口径

本 skill 默认做的是**全量地域对比**，不是条件筛选。

它会比较：

1. 对象规模和风险比例；
2. `label / sub_label` 分布；
3. 通话记录、联系人广度、夜间占比；
4. 时间覆盖与归一化行为强度；
5. 共享设备、设备池和高挂载设备；
6. 群体结构指标；
7. 两省代表性对象和共享设备样例。

由于四川和陕西数据的时间覆盖可能不同，报告中会明确给出“时间可比性提醒”。跨省比较时，应优先参考人均、日均和比例类指标，不要只看原始通话总量。

---

## 运行方式

### 前端/平台默认运行

```bash
cd /mnt/skills/custom/phone-network-analysis/sichuan-shaanxi-comparison/scripts && python3 sichuan_shaanxi_comparison_wrapper.py \
  --dataset unified \
  --province-a sichuan \
  --province-b shaanxi \
  --top-k 10
```

### 本地项目容器运行

```bash
cd /workspace/imiss-deer-flow-main/skills/custom/phone-network-analysis/sichuan-shaanxi-comparison/scripts
python3 sichuan_shaanxi_comparison_wrapper.py \
  --dataset-root /workspace/imiss-deer-flow-main/datasets/phone-network \
  --dataset unified \
  --province-a sichuan \
  --province-b shaanxi \
  --top-k 10
```

### 一键测试

```bash
cd /workspace/imiss-deer-flow-main/skills/custom/phone-network-analysis/sichuan-shaanxi-comparison/scripts
bash test_sichuan_shaanxi_comparison.sh
```

---

## 主要参数

| 参数 | 说明 | 默认值 |
|---|---|---|
| `--dataset-root` | 电话网络数据根目录 | 自动识别 `/workspace/...` 或 `/mnt/datasets/...` |
| `--dataset` | 数据集名称 | `unified` |
| `--province-a` | 第一个对比省份 | `sichuan` |
| `--province-b` | 第二个对比省份 | `shaanxi` |
| `--top-k` | 每个省份输出的 Top 样例数量 | `10` |
| `--output-dir` | 输出目录 | `/mnt/user-data/outputs` |

---

## 输出内容

本 skill 会生成一组结构化结果。

### 1. Markdown 报告

- `sichuan_shaanxi_comparison_unified.md`：技术版完整报告
- `sichuan_shaanxi_comparison_unified_presentation.md`：甲方汇报版报告

### 2. CSV 证据表

- `node_overview.csv`：两省对象规模、风险数、风险占比
- `label_distribution.csv`：两省 label 分布
- `sub_label_distribution.csv`：两省 sub_label 分布
- `call_behavior.csv`：通话行为和时间归一化指标
- `hourly_distribution.csv`：小时分布对比
- `device_summary.csv`：共享设备对比
- `top_shared_devices.csv`：Top 共享设备样例
- `cross_device_examples.csv`：跨省共享设备样例，如果存在则输出
- `structure_metrics.csv`：群体结构指标
- `metric_contrast.csv`：关键差异指标
- `top_objects.csv`：两省代表性对象样例
- `top_callers.csv`：两省高活跃号码样例

### 3. 结构化附件

- `summary.json`：结构化摘要
- `evidence.xlsx`：多 sheet 证据工作簿

---

## 前端使用提示词

### Q1：完整地域对比分析

```text
请使用 sichuan-shaanxi-comparison skill，对 unified 电话网络数据中的四川和陕西开展全量地域对比分析。要求必须运行 sichuan_shaanxi_comparison_wrapper.py，不要用 condition-based-screening 代替。请输出对象规模、风险标签分布、通话行为模式、时间覆盖与归一化指标、共享设备信号、群体结构差异、代表性对象样例，并生成 markdown 报告和 csv/xlsx/json 附件。
```

### Q2：甲方汇报版

```text
请使用 sichuan-shaanxi-comparison skill，生成一份适合甲方汇报的四川-陕西电话网络地域对比报告。要求必须展示 sichuan_shaanxi_comparison_unified_presentation.md，并同时提供技术版 markdown、summary.json、evidence.xlsx 和 csv 证据附件下载入口。重点说明两地风险特征、行为模式、群体结构、时间覆盖口径和后续分析建议。
```

### Q3：设备与团伙线索重点版

```text
请使用 sichuan-shaanxi-comparison skill，对四川和陕西进行地域对比，重点关注共享设备、设备池风险、群体结构差异和代表性风险对象。必须运行本 skill 的 wrapper 脚本，并输出后续可联动 shared-device-analysis、gang-cluster-analysis、cross-province-linkage-analysis 的建议。
```

---

## 结果解释原则

1. 地域差异不等于风险定性，只说明当前数据中的统计差异。
2. 如果两省时间覆盖不同，原始通话总量只能作为规模背景，应优先参考人均、日均和比例类指标。
3. 共享设备、共同对端、夜间行为等都属于证据信号，需要结合后续 skill 下钻确认。
4. Top 对象样例用于后续分析入口，不代表最终名单。
5. 如果出现跨省共享设备样例，应交给 `cross-province-linkage-analysis` 进一步分析。
6. 如果需要单个号码解释，应调用 `risk-evidence-pack` 或 `single-number-analysis`。
7. 如果需要“按联系人广度、共享设备、省份等条件筛选”，应另行调用 `condition-based-screening`，不要把它的筛选结果当作本 skill 的地域总览报告。

---

## 推荐后续链路

完成本 skill 后，建议按以下路径继续：

1. `cross-province-linkage-analysis`：核查跨省共享设备、共同对端和跨省强关联对象。
2. `risk-evidence-pack`：对代表性号码生成完整证据包。
3. `shared-device-analysis`：对 Top 共享设备做设备池深挖。
4. `gang-cluster-analysis`：对疑似设备池或群体结构做团伙簇识别。
5. `time-series-anomaly-analysis`：对两省重点对象进行阶段性行为变化分析。


## 前端调用强约束

当用户要求“四川-陕西对比”“地域对比”“两省风险特征差异”时，必须调用本目录下的 `scripts/sichuan_shaanxi_comparison_wrapper.py`。不要用 `condition-based-screening` 分别筛选两省后手工拼接报告，因为那是条件切片分析，不是全量地域对比。

输出报告必须明确展示：

1. 这是全量地域对比，不是条件切片；
2. 两省时间覆盖不同，原始总量只作规模背景；
3. 跨省行为对比优先看比例、人均、日均、每活跃号码日均指标；
4. 如用户需要特定条件对象对比，才另行调用 `condition-based-screening`。


## 附件展示模式

本技能支持 `--artifact-mode` 控制前端展示的下载附件数量：

- `full`：默认模式，展示技术报告、演示报告、全部 CSV、summary JSON 和 evidence XLSX，适合命令行验收和证据链检查。
- `essential`：只展示技术报告、演示报告、summary JSON 和 evidence XLSX，适合常规前端分析。
- `markdown_only`：只展示两个 Markdown 报告，适合用户明确说“只要 markdown 报告”或希望前端输出简洁的场景。

如果用户明确要求“只要 markdown 报告”，前端必须在执行命令时加上：

```bash
--artifact-mode markdown_only
```

注意：即使使用 `markdown_only`，脚本仍会在输出目录生成完整证据文件，只是不把 CSV/XLSX/JSON 放入 `artifacts` 展示列表，避免前端下载卡片过多。
