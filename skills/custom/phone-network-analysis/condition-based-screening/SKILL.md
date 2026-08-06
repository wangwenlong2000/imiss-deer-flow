---
name: condition-based-screening
description: 根据用户已经明确给出的筛选条件，在电话网络号码集合中执行规则过滤，找出“满足这些条件”的对象，并输出筛选链路、命中号码、命中原因和证据附件。核心任务是条件命中，不是风险排行榜；适用于用户明确提出“筛选/过滤/满足条件/按省份/按标签/按阈值/同时满足”等规则约束的场景，例如筛选夜间通话占比超过阈值、联系人广度高于阈值、共享设备数量达到阈值的号码。不用于系统自动发现最可疑号码、不生成全量 TopN 风险榜、不做综合风险排名、不承担单号码证据包、原始数据建图或地域对比。
---

# condition-based-screening

## 这个 skill 做什么

`condition-based-screening` 是电话网络数据中的**规则筛选型** skill。

它的核心不是“自动找最可疑对象”，而是：

> 用户已经给出明确筛选条件，系统按照这些条件过滤号码集合，找出满足条件的对象，并解释每个对象命中了哪些规则。

它回答的是：

1. 给定条件能筛出哪些号码；
2. 每个号码具体命中了哪些条件；
3. 筛选过程是否真的缩小了样本范围；
4. 命中对象是否存在共享设备、共同对端等补充证据；
5. 哪些命中对象适合继续做单号码画像、证据包或群体分析。

## 与 topn-high-risk-discovery 的强边界

必须严格区分本 skill 和 `topn-high-risk-discovery`。

| 维度 | condition-based-screening | topn-high-risk-discovery |
|---|---|---|
| 用户意图 | 用户已经给出明确规则 | 用户没有给硬性筛选规则 |
| 核心动作 | 筛选、过滤、命中解释 | 自动打分、排序、发现 |
| 典型词 | 筛选、过滤、满足条件、阈值、同时满足、按省份、按标签 | TopN、Top10、排行榜、最可疑、自动发现、优先核查 |
| 输出重点 | 命中对象、筛选链路、命中原因 | 风险排名、风险分、驱动因素 |
| 是否做综合榜单 | 不做全量风险排行榜 | 做全量或候选集风险排行榜 |

如果用户说：

- “筛选夜间通话明显异常的号码”；
- “过滤出四川省风险标签对象”；
- “找同时满足联系人广度高、共享设备多的号码”；
- “按阈值找夜间占比超过 0.35 的对象”；

应使用本 skill。

如果用户说：

- “自动找 Top10 高风险号码”；
- “生成高风险排行榜”；
- “哪些号码最可疑”；
- “综合评分排序所有号码”；

应使用 `topn-high-risk-discovery`，不是本 skill。

## 适合使用的场景

本 skill 适合规则筛选、条件命中、候选集合收缩类问题，例如：

- 筛选夜间通话占比超过阈值的号码；
- 筛选联系人广度高于阈值的号码；
- 筛选共享设备数量达到阈值的号码；
- 筛选四川省或陕西省中的特定标签对象；
- 筛选风险标签对象或未标注但满足可疑条件的对象；
- 筛选同时满足多个条件的号码；
- 验证某类目标画像在数据中是否存在；
- 输出筛选链路、命中号码、证据表和工作簿附件。

## 不适合使用的场景

以下情况不要使用本 skill：

- 用户没有给明确条件，只想让系统自动发现最可疑号码；
- 用户要求生成 TopN 高风险排行榜；
- 用户要求对全部号码做综合风险评分排序；
- 用户要求解释某一个号码为什么可疑；
- 用户要求生成单号码风险证据包；
- 用户上传原始 CSV，要求先建图；
- 用户要求四川和陕西做地域对比；
- 用户要求跨省同实体联动识别。

对应应转向：

- 自动高风险排行榜：`topn-high-risk-discovery`；
- 单号码画像：`single-number-analysis`；
- 单号码风险证据包：`risk-evidence-pack`；
- 原始数据建图：`dataset-onboarding-graph-preprocess`；
- 四川陕西地域对比：`sichuan-shaanxi-comparison`；
- 跨省可联动性判断：`dataset-quality-and-linkability-diagnostic`。

---

## 一、数据集使用规则

### 规则 1：用户明确指定数据集时，优先使用用户指定的数据集

如果用户问题或命令中明确给出：

- `--dataset-root /workspace/imiss-deer-flow-main/datasets/phone-network`
- `--dataset-root /mnt/datasets/phone-network`
- `--dataset unified`
- “使用 unified / sichuan / shaanxi 数据”

则直接按用户指定执行。

### 规则 2：用户没有指定数据集根目录时，优先使用真实 unified 预处理数据

脚本优先自动查找以下真实数据位置：

1. `PHONE_NETWORK_DATASETS_ROOT`
2. `<repo_root>/datasets/phone-network`
3. `/mnt/datasets/phone-network`
4. `/workspace/imiss-deer-flow-main/datasets/phone-network`
5. `~/imiss-deer-flow-main/datasets/phone-network`

### 规则 3：用户没有指定 dataset 时，默认使用 `unified`

前端问题中出现：

- “在现有电话网络数据里筛选”；
- “在 unified 电话网络数据里筛选”；
- “用已预处理好的数据筛选”；

都应默认理解为：

```text
dataset-root = /mnt/datasets/phone-network
dataset = unified
```

### 规则 4：只有真实数据根目录找不到时，才允许退回测试样例

测试样例只用于回归测试，不是正式前端分析的默认数据源。

---

## 二、输入数据

本 skill 默认依赖标准图结构三件套：

```text
datasets/phone-network/processed/unified/user_nodes.csv
datasets/phone-network/processed/unified/call_edges.csv
datasets/phone-network/processed/unified/edges_phone_imei.parquet
```

各文件作用：

| 文件 | 用途 |
|---|---|
| `user_nodes.csv` | 号码节点属性、标签、省份、sub_label |
| `call_edges.csv` | 通话次数、对端数量、夜间行为、通话时间和时长 |
| `edges_phone_imei.parquet` | 号码与设备关系、共享设备、一机多号线索 |

---

## 三、支持的筛选模式

### 1. 夜间行为筛选

```bash
--mode night_abnormal
```

常用条件：

- `--min-night-ratio`
- `--min-night-count`

适合问题：

```text
筛选夜间通话占比高的号码。
筛选夜间通话次数超过 8 次的号码。
```

### 2. 联系人广度筛选

```bash
--mode broad_contacts
```

常用条件：

- `--min-counterparties`

适合问题：

```text
筛选联系人数量超过 50 的号码。
筛选对端广度异常的对象。
```

### 3. 共享设备筛选

```bash
--mode shared_device
```

常用条件：

- `--min-shared-device-count`
- `--min-shared-peer-total`

适合问题：

```text
筛选共享设备数量达到阈值的号码。
筛选疑似一机多号或设备池相关号码。
```

### 4. 高通话量筛选

```bash
--mode high_call_volume
```

常用条件：

- `--min-call-records`

适合问题：

```text
筛选通话记录数超过阈值的号码。
```

### 5. 多条件联合筛选

```bash
--mode mixed
```

适合把多个规则组合起来筛选，例如：

```text
筛选四川省、风险标签、联系人广度高、共享设备明显的号码。
```

重要约束：

> `--mode mixed` 只会自动补联系人广度和共享设备两个默认阈值；它不会自动启用夜间通话条件。  
> 如果用户同时提到“夜间通话异常 + 联系人广度高 + 共享设备多”，必须显式传入 `--min-night-ratio` 和 `--min-night-count`。

---

## 四、常用条件参数

- `--risk-only`：只保留风险标签对象；
- `--unlabeled-only`：只保留未显式标注风险的对象；
- `--labels`：按 label 筛选；
- `--sub-labels`：按 sub_label 筛选；
- `--province`：按省份筛选；
- `--min-risk-score`：按风险分阈值筛选；
- `--min-call-records`：按通话记录数阈值筛选；
- `--min-counterparties`：按联系人广度阈值筛选；
- `--min-shared-device-count`：按共享设备数量阈值筛选；
- `--min-shared-peer-total`：按共享设备牵出号码数量阈值筛选；
- `--min-night-ratio`：按夜间通话占比阈值筛选；
- `--min-night-count`：按夜间通话次数阈值筛选；
- `--match-mode all|any`：控制条件组合方式；
- `--min-match-count`：至少命中几条条件；
- `--top-k`：只控制输出展示条数，不表示自动风险排行榜。

重要说明：

> 本 skill 的 `--top-k` 只是“命中结果太多时展示前 K 条”，不是自动发现 TopN 高风险对象，也不是综合风险排名。

---

## 五、前端推荐提问模板

### 模板 1：夜间行为筛选

```text
请在 unified 电话网络数据中筛选夜间通话占比超过 0.35 且夜间通话次数不少于 8 次的号码，并输出筛选链路、命中号码和证据附件。
```

### 模板 2：多条件联合筛选

```text
请筛选四川省中同时满足风险标签、联系人广度较高、共享设备明显的号码，并说明每个号码命中了哪些条件。
```

### 模板 3：未标注对象筛选

```text
请筛选未显式标注风险但联系人广度高且共享设备数量达到阈值的号码，输出命中原因和后续下钻建议。
```

### 模板 4：阈值过滤

```text
请筛选通话记录数超过 100、对端数量超过 50、共享设备数量不少于 1 的号码。
```

---

## 六、命令行正式分析

正式分析优先使用容器内路径：

```bash
cd /mnt/skills/custom/phone-network-analysis/condition-based-screening/scripts
```

如果当前运行环境不是容器，才使用仓库路径：

```bash
cd /workspace/imiss-deer-flow-main/skills/custom/phone-network-analysis/condition-based-screening/scripts
```

正式分析命令必须显式写出：

```bash
--dataset-root /mnt/datasets/phone-network
--dataset unified
--output-root /mnt/user-data/outputs
```

### 场景 1：夜间行为筛选

```bash
python3 condition_based_screening_wrapper.py \
  --dataset-root /mnt/datasets/phone-network \
  --dataset unified \
  --group-name night_abnormal_targets \
  --mode night_abnormal \
  --min-night-ratio 0.35 \
  --min-night-count 8 \
  --top-k 20 \
  --output-root /mnt/user-data/outputs
```

### 场景 2：联系人广度筛选

```bash
python3 condition_based_screening_wrapper.py \
  --dataset-root /mnt/datasets/phone-network \
  --dataset unified \
  --group-name broad_contact_targets \
  --mode broad_contacts \
  --min-counterparties 50 \
  --top-k 20 \
  --output-root /mnt/user-data/outputs
```

### 场景 3：共享设备筛选

```bash
python3 condition_based_screening_wrapper.py \
  --dataset-root /mnt/datasets/phone-network \
  --dataset unified \
  --group-name device_targets \
  --mode shared_device \
  --min-shared-device-count 1 \
  --min-shared-peer-total 5 \
  --top-k 20 \
  --output-root /mnt/user-data/outputs
```

### 场景 4：夜间异常 + 联系人广度高 + 共享设备多

```bash
python3 condition_based_screening_wrapper.py \
  --dataset-root /mnt/datasets/phone-network \
  --dataset unified \
  --group-name night_broad_device_targets \
  --mode mixed \
  --min-night-ratio 0.30 \
  --min-night-count 10 \
  --min-counterparties 50 \
  --min-shared-device-count 1 \
  --match-mode all \
  --top-k 30 \
  --output-root /mnt/user-data/outputs
```

说明：

- `--min-night-ratio 0.30` 和 `--min-night-count 10` 是夜间异常默认业务阈值；
- `--min-counterparties 50` 是联系人广度固定阈值，如用户要求自动阈值，可去掉该参数，由脚本按 P80 自动计算；
- `--min-shared-device-count 1` 表示至少存在共享设备；
- `--match-mode all` 表示三个维度必须同时满足。

### 场景 5：省份/风险标签/联系人/共享设备联合筛选

```bash
python3 condition_based_screening_wrapper.py \
  --dataset-root /mnt/datasets/phone-network \
  --dataset unified \
  --group-name mixed_targets \
  --mode mixed \
  --province sichuan \
  --risk-only \
  --min-counterparties 50 \
  --min-shared-device-count 1 \
  --match-mode all \
  --top-k 20 \
  --output-root /mnt/user-data/outputs
```

### 场景 6：未标注但满足条件对象筛选

```bash
python3 condition_based_screening_wrapper.py \
  --dataset-root /mnt/datasets/phone-network \
  --dataset unified \
  --group-name unlabeled_condition_hits \
  --mode mixed \
  --unlabeled-only \
  --min-counterparties 80 \
  --min-shared-device-count 1 \
  --match-mode all \
  --top-k 20 \
  --output-root /mnt/user-data/outputs
```

---

## 七、命令行回归测试

```bash
cd /mnt/skills/custom/phone-network-analysis/condition-based-screening/scripts
bash test_condition_based_screening.sh
```

该测试用于验证脚本功能，必要时会使用脚本自带的小型样例数据。

---

## 八、输出文件

默认输出到：

- `/mnt/user-data/outputs`

必须通过 `--output-root /mnt/user-data/outputs` 显式指定输出目录，便于前端展示和下载。当前脚本不会自动 fallback 到项目 `outputs/`；如果该目录不可写，应先修复挂载或权限问题，再重新执行。

标准输出包括：

1. `report_md`：Markdown 筛选报告；
2. `targets_csv`：命中对象表；
3. `devices_csv`：共享设备证据表；
4. `counterparts_csv`：共同对端证据表；
5. `summary_json`：结构化摘要；
6. `evidence_xlsx`：证据工作簿。

---

## 九、结果解读原则

本 skill 的结果表示“满足用户指定规则的候选对象”，不是最终风险定性，也不是系统自动计算出的高风险排行榜。

解读时应说明：

- 筛选条件是什么；
- 原始候选规模是多少；
- 筛选后命中规模是多少；
- 每个命中对象命中了哪些条件；
- 是否存在共享设备、共同对端等补充证据；
- 是否建议继续调用其他 skill 下钻。

推荐后续链路：

- 单个命中号码解释：`single-number-analysis`；
- 单个命中号码证据包：`risk-evidence-pack`；
- 共享设备下钻：`shared-device-analysis`；
- 两号码关系重叠：`overlap-analysis`；
- 两号码路径分析：`association-path-analysis`；
- 群体或团伙扩展：`group-risk-analysis` / `gang-cluster-analysis`。

---

## 十、基础算子对齐

本 skill 属于基础算子组合得到的电话网络规则筛选 skill：

- 条件对象筛选 = `node_lookup + aggregation_query + relationship_filter`；
- 夜间行为筛选 = `time-window relationship_filter + aggregation_query`；
- 联系人广度筛选 = `neighbor_query + aggregation_query`；
- 共享设备筛选 = `query_shared_device + aggregation_query`；
- 条件命中解释 = `aggregation_query + evidence_summary`。
