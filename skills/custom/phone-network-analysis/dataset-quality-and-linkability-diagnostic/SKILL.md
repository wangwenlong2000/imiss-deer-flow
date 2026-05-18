# dataset-quality-and-linkability-diagnostic

## 一、定位

`dataset-quality-and-linkability-diagnostic` 是电话网络数据分析体系中的**数据质量与可分析能力诊断 skill**。

它不负责发现风险对象，也不负责生成业务研判结论。它的作用是先判断一个已经处理好的电话网络图数据集是否具备后续分析条件，尤其用于回答：

- 这个数据集是否已经形成标准图结构？
- 是否存在 `user_nodes.csv`、`call_edges.csv`、`edges_phone_imei.parquet/csv`？
- 号码节点、通话边、设备边是否可用？
- 是否支持时间序列分析、共享设备分析、单号分析、群体分析、团伙分析等下游 skill？
- 如果是多省份/多来源数据，是否能做跨省同实体联动分析？
- 哪些 skill 可以用，哪些不能用，原因是什么？

该 skill 适合放在两个位置：

1. `dataset-onboarding-graph-preprocess` 建图完成之后，用于验证新数据是否可被后续 skill 使用。
2. 对现有 `unified` 或其他 processed 数据集做体检，防止前端误调用不适合的数据分析能力。

---

## 二、输入数据要求

本 skill 面向**已经预处理成标准图结构的数据集**，默认读取：

```text
<dataset_root>/processed/<dataset>/user_nodes.csv
<dataset_root>/processed/<dataset>/call_edges.csv
<dataset_root>/processed/graph_views/<dataset>/edges_phone_imei.parquet
```

如果设备边不是 parquet，也支持：

```text
<dataset_root>/processed/graph_views/<dataset>/edges_phone_imei.csv
```

如果用户上传的是原始 CSV / Excel / JSON 明细表，不应直接使用本 skill，应先调用：

```text
dataset-onboarding-graph-preprocess
```

将其转换成标准图结构。

---

## 三、必须调用的脚本

前端或命令行必须运行：

```bash
python3 dataset_quality_linkability_diagnostic_wrapper.py
```

不要临时重写脚本。不要用 `dataset-overview-analysis`、`condition-based-screening` 或其他 skill 代替本 skill。

---

## 四、核心参数

| 参数 | 含义 | 默认值 |
|---|---|---|
| `--dataset-root` | 电话网络数据根目录 | `/workspace/imiss-deer-flow-main/datasets/phone-network` |
| `--dataset` | 要诊断的数据集名称 | `unified` |
| `--province-a` | 跨省可联动性诊断中的省份 A | 自动选择或用户指定 |
| `--province-b` | 跨省可联动性诊断中的省份 B | 自动选择或用户指定 |
| `--top-k` | 样例输出数量 | `10` |
| `--artifact-mode` | 附件展示模式：`full` / `essential` / `markdown_only` | `essential` |

---

## 五、输出内容

运行后会输出：

1. Markdown 诊断报告
2. summary JSON
3. capability matrix CSV
4. quality checks CSV
5. linkability diagnostics CSV

核心报告包括：

- 数据构成：节点、通话边、设备边、省份、时间字段等；
- 标准图结构是否可用；
- 数据质量检查；
- 下游 skill 能力矩阵；
- 跨省/跨来源实体可联动性诊断；
- 不支持分析时的原因和修复建议。

---

## 六、能力矩阵说明

本 skill 会给下游 skill 输出 `supported / partial / not_supported / not_applicable` 判断。

常见判断逻辑：

- 有用户节点 + 通话边/设备边：支持单号、TopN、群体、证据包等分析；
- 有设备边：支持共享设备分析；
- 有通话时间字段：支持时间序列异常分析；
- 有省份字段且至少两个省份：支持地域对比；
- 存在跨省 ID 相等证据：才支持跨省联动分析；
- 如果跨省 ID 格式明显不同且没有交集，不应做真实跨省同实体链路结论。

---

## 七、跨省可联动性诊断规则

如果数据中存在两个及以上省份，本 skill 会检查：

- `user_id` 跨省交集；
- `imei` 跨省交集；
- `dst_counterparty_id` 跨省交集；
- 可由 ID 相等识别出的跨省直接通话；
- 各省 `user_id / imei / dst_counterparty_id` 的长度和格式是否一致。

输出等级包括：

| 等级 | 含义 |
|---|---|
| `linkable_with_multiple_cross_province_signals` | 多类跨省 ID 相等证据，支持跨省联动线索分析 |
| `partially_linkable_with_one_cross_province_signal` | 只有一类跨省证据，只能做有限分析 |
| `no_cross_province_overlap_found_namespace_unknown` | 未检出跨省交集，但无法确认命名空间是否一致 |
| `not_linkable_due_to_visible_namespace_difference` | ID 格式明显不同，不支持可靠跨省同实体分析 |
| `not_applicable_single_or_unknown_province` | 单省或缺少省份字段，不适用跨省诊断 |

---

## 八、前端调用规则

当前端用户提出以下需求时，应调用本 skill：

- “检查这个数据集能不能分析”
- “判断这个数据支持哪些 skill”
- “为什么这个数据不能做共享设备/时间序列/跨省联动”
- “建图后帮我检查一下能不能继续分析”
- “unified 是不是实体统一索引”
- “两个省份能不能做跨省联动”

前端回答要求：

- 最终解释必须使用中文；
- 不要把 `not_supported` 解释为业务事实不存在；
- 如果跨省可联动性不支持，应明确说明是“数据前提不满足”，不是“真实没有跨省联动”；
- 不要把诊断结论夸大为案件事实；
- 如用户只要报告，使用 `artifact_mode=markdown_only`；
- 如用户要完整证据，使用 `artifact_mode=full`。

---

## 九、命令行示例

### 1. 诊断 unified 数据集

```bash
cd /workspace/imiss-deer-flow-main/skills/custom/phone-network-analysis/dataset-quality-and-linkability-diagnostic/scripts

python3 dataset_quality_linkability_diagnostic_wrapper.py \
  --dataset-root /workspace/imiss-deer-flow-main/datasets/phone-network \
  --dataset unified \
  --province-a sichuan \
  --province-b shaanxi \
  --top-k 10 \
  --artifact-mode essential
```

### 2. 诊断新建图的数据集

```bash
python3 dataset_quality_linkability_diagnostic_wrapper.py \
  --dataset-root /workspace/imiss-deer-flow-main/datasets/phone-network \
  --dataset onboarded_clean_demo \
  --top-k 10 \
  --artifact-mode essential
```

### 3. 只输出 markdown 附件

```bash
python3 dataset_quality_linkability_diagnostic_wrapper.py \
  --dataset-root /workspace/imiss-deer-flow-main/datasets/phone-network \
  --dataset onboarded_clean_demo \
  --artifact-mode markdown_only
```

---

## 十、一键测试

```bash
cd /workspace/imiss-deer-flow-main/skills/custom/phone-network-analysis/dataset-quality-and-linkability-diagnostic/scripts
chmod +x *.py *.sh
bash test_dataset_quality_linkability_diagnostic.sh
```

正常结尾应看到：

```text
[OK] dataset-quality-and-linkability-diagnostic tests finished
```

---

## 十一、边界说明

本 skill 只做数据可用性诊断，不做业务风险结论。

它不能在没有原始 ID、统一哈希规则或实体映射表的情况下，自动解决跨来源实体对齐问题。若诊断结果显示跨省 ID 命名空间不一致，应暂停跨省同实体联动分析，改做地域对比或补充映射数据。

---

## 十二、V1.2 新增：下游命令模板与调用方式说明

本版本会在 summary JSON、Markdown 报告和 `*_downstream_command_templates.csv` 中输出下游命令模板。

需要特别注意：能力矩阵里的 `supported` 表示**数据层面支持该分析**，不代表所有旧版 wrapper 都支持 `--dataset-root` / `--dataset` 参数。部分早期 skill 仍需要显式路径调用。

因此报告会把下游 skill 分成两种调用方式：

| 调用方式 | 含义 | 示例 |
|---|---|---|
| `dataset 模式` | wrapper 支持 `--dataset-root` 和 `--dataset` | `dataset-overview-analysis`、`time-series-anomaly-analysis` |
| `显式路径模式` | wrapper 需要直接传入 `user_nodes.csv`、`call_edges.csv`、`edges_phone_imei.parquet` | `single-number-analysis`、`topn-high-risk-discovery` 等早期 skill |

如果用户问“下一步怎么跑下游分析”，优先读取本 skill 生成的 `downstream_command_templates`，不要凭经验自行拼旧参数。

### 示例：TopN 对新建图数据的正确调用方式

```bash
cd /workspace/imiss-deer-flow-main/skills/custom/phone-network-analysis/topn-high-risk-discovery/scripts
python3 topn_high_risk_discovery_wrapper.py \
  --top-n 10 \
  --analysis-mode mixed \
  --candidate-scope all \
  --user-node-path /workspace/imiss-deer-flow-main/datasets/phone-network/processed/<dataset>/user_nodes.csv \
  --call-graph-path /workspace/imiss-deer-flow-main/datasets/phone-network/processed/<dataset>/call_edges.csv \
  --device-graph-path /workspace/imiss-deer-flow-main/datasets/phone-network/processed/graph_views/<dataset>/edges_phone_imei.parquet
```

不要给旧版 `topn-high-risk-discovery` wrapper 传 `--dataset-root`、`--dataset` 或 `--artifact-mode`，否则会出现 `unrecognized arguments`。
