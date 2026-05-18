---
name: time-series-anomaly-analysis
description: 围绕号码级或群体级时间变化，分析活跃趋势、夜间行为变化和异常突变情况，用于识别阶段性风险上升对象及异常群体。
allowed-tools: Bash, Read, Write
---

# time-series-anomaly-analysis

## 这个 skill 是干什么的

这个 skill 用于对电话网络数据做时间维度分析，回答：

- “这个号码最近是不是突然活跃起来了？”
- “这组号码最近有没有阶段性爆发、夜间行为上升或异常突变？”
- “哪些日期、小时段、对端或成员推动了这次时间异常？”

支持两种模式：

- `phone`：单号码时间序列异常分析。
- `group`：号码集合 / 群体时间序列异常分析。

## 数据集使用规则

- 如果用户明确指定 `dataset-root` 或具体数据集，优先使用用户指定的数据。
- 如果用户没有明确指定，优先使用已经预处理好的真实 `unified` 电话网络图数据。
- 如果用户说“用 unified 数据集”或“用已预处理好的电话网络数据”，应理解为使用真实 `unified` 图数据，而不是测试样例。
- 本 skill 不能直接处理未预处理的原始 Excel/CSV；新数据必须先整理成当前统一图结构。
- 如果当前数据没有可解析时间列，返回 `time_evidence_unavailable`，不要假装分析成功。

## 核心能力

### 号码级

- 比较 recent window 与 baseline window 的活跃度变化。
- 比较 recent window 与 baseline window 的夜间行为变化。
- 识别重点异常日期，优先展示真实突增日和夜间行为异常日。
- 输出小时分布。
- 输出关键贡献对端。
- 给出阶段判断：`spike_rising`、`night_shift_rising`、`cooling_down`、`volatile`、`stable_or_mild`。

### 群体级

- 比较群体 recent window 与 baseline window 的日均通话变化。
- 比较群体活跃成员数变化。
- 比较群体夜间行为变化。
- 识别群体异常日期。
- 输出小时分布。
- 输出推动异常上升的关键成员。
- 用于判断是否存在阶段性风险上升群体或异常突变群体。

## v1.4 重要说明

v1.4 修复和优化了 v1.3 的几个问题：

1. 统计窗口改为“日历日口径”，缺失日期会补 0，避免把“近 7 天”误算成“有记录的 6 天”。
2. 异常日期展示不再把低通话量、负 z-score 的普通日期当成“重点异常日期”。
3. 报告中增加活跃日覆盖说明，避免 recent/baseline 的数据覆盖被误解。
4. `target_not_found` 或缺少时间列时，只输出 markdown 与 summary.json，不再展示空的 csv/xlsx 证据包。
5. 阶段判断增加中文解释，便于前端和人工阅读。

## 它是怎么实现的

这个 skill 通过基础图分析算子组合实现：

- 时间窗口切片 = `relationship_filter(time window)`
- 日级 / 小时级聚合 = `aggregation_query`
- 单号或群体范围限定 = `node_lookup / subgraph_by_nodes`
- 异常日期排序 = `aggregation_query + scoring_layer`

## 输入参数

### 通用参数

- `--mode`：`phone` 或 `group`
- `--dataset-root`：可选，明确指定数据根目录
- `--dataset`：默认 `unified`
- `--recent-days`：默认 `7`
- `--baseline-days`：默认 `30`
- `--top-k`：默认 `10`
- `--evidence-limit`：默认 `100`
- `--night-start-hour`：默认 `22`
- `--night-end-hour`：默认 `6`

### phone 模式

- `--phone-id`：目标号码 ID。

### group 模式

- `--phone-ids`：逗号分隔的号码列表。
- `--phone-id-file`：号码文件，每行一个号码。

## 标准命令

### 号码级

```bash
cd /mnt/skills/custom/phone-network-analysis/time-series-anomaly-analysis/scripts && python3 time_series_anomaly_analysis_wrapper.py \
  --mode phone \
  --phone-id "<PHONE_ID>" \
  --dataset unified \
  --recent-days 7 \
  --baseline-days 30 \
  --top-k 10
```

### 群体级

`sample_group_ids.txt` 是 skill 自带内置示例文件，应优先使用绝对路径：

```bash
cd /mnt/skills/custom/phone-network-analysis/time-series-anomaly-analysis/scripts && python3 time_series_anomaly_analysis_wrapper.py \
  --mode group \
  --phone-id-file /mnt/skills/custom/phone-network-analysis/time-series-anomaly-analysis/scripts/sample_group_ids.txt \
  --dataset unified \
  --recent-days 7 \
  --baseline-days 30 \
  --top-k 10
```

### 明确指定真实数据根目录

```bash
cd /mnt/skills/custom/phone-network-analysis/time-series-anomaly-analysis/scripts && python3 time_series_anomaly_analysis_wrapper.py \
  --mode phone \
  --phone-id "<PHONE_ID>" \
  --dataset-root "/workspace/imiss-deer-flow-main/datasets/phone-network" \
  --dataset unified \
  --recent-days 7 \
  --baseline-days 30 \
  --top-k 10
```

## 前端提问模板

### 模板 1：号码级时间异常

请使用 `time-series-anomaly-analysis` skill，分析这个号码最近的活跃趋势、夜间行为变化和异常突变情况，并输出：

1. recent 与 baseline 的窗口对比
2. 活跃日覆盖情况
3. 重点异常日期
4. 小时分布
5. 关键贡献对端
6. markdown 报告与 csv/xlsx/json 附件

号码ID：`<PHONE_ID>`
参数：`dataset=unified, recent_days=7, baseline_days=30, top_k=10`

### 模板 2：群体级时间异常

请使用 `time-series-anomaly-analysis` skill，围绕这组号码分析阶段性活跃上升、夜间行为变化和异常突变，并输出：

1. 群体窗口对比
2. 活跃成员变化
3. 重点异常日期
4. 小时分布
5. 推动异常上升的关键成员
6. markdown 报告与 csv/xlsx/json 附件

号码文件：`sample_group_ids.txt`
参数：`dataset=unified, recent_days=7, baseline_days=30, top_k=10`

## 输出文件

正常情况下会生成：

1. `markdown` 完整报告
2. `daily.csv`：日级指标，v1.4 起按日历日补齐缺失日期
3. `anomaly_days.csv`：重点异常日期
4. `hourly.csv`：小时分布
5. `contributors.csv`：关键贡献对象
6. `summary.json`：结构化摘要
7. `evidence.xlsx`：证据工作簿

如果目标号码不存在或没有时间列，只生成 markdown 和 summary.json，避免输出空证据表误导使用者。

## 特别说明

### 如果目标号码不存在

- 返回 `status=target_not_found`
- 明确说明“当前数据集中未找到该号码”
- 只输出基本说明文档和 summary.json

### 如果当前数据没有可解析时间列

- 返回 `status=time_evidence_unavailable`
- 明确说明“当前缺少可解析时间列，无法形成时间趋势证据”
- 不要把这种情况误当成“没有异常”

## 测试方法

```bash
cd /mnt/skills/custom/phone-network-analysis/time-series-anomaly-analysis/scripts && bash test_time_series_anomaly_analysis.sh
```

测试覆盖：

- 号码级经典样例
- 群体级样例
- 故意不存在的号码
- 报告存在性检查

## 适用边界

适合：

- 时间维度的活跃变化研判
- 夜间行为变化研判
- 异常日期识别
- 阶段性风险上升分析
- 群体级阶段性异常识别

不适合：

- 直接替代单号画像，请用 `single-number-analysis`
- 直接替代群体结构分析，请用 `group-risk-analysis` / `gang-cluster-analysis`
- 在没有任何时间列的图数据上硬做趋势判断
