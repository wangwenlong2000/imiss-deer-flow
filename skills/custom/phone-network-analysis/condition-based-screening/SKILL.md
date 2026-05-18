---
name: condition-based-screening
description: 在电话网络数据中按夜间行为、联系人广度、共享设备、标签、省份、风险分等条件进行目标筛选，输出筛选链路、命中对象与证据附件。若用户明确指定数据集，则优先使用指定数据集；若未指定，则默认优先使用真实 unified 预处理数据集。
allowed-tools: Bash, Read, Write, Glob, Grep
---

# condition-based-screening

## 这个 skill 做什么

用于在电话网络数据中做“规则筛选型”分析，不是随便排个 TopN，而是基于明确条件把一批对象筛出来，并回答三件事：

1. 当前条件到底筛到了哪些号码
2. 筛选过程是否真的缩小了样本范围
3. 命中对象有没有共享设备、共同对端等补充证据

它特别适合这类问题：

- 找夜间行为明显异常的号码
- 找联系人广度异常大的号码
- 找共享设备明显的号码
- 只筛某省、某标签、某 sub_label 的对象
- 把多个条件联合起来，筛特定类型的疑似目标
- 产出 markdown 报告、csv 证据表和 xlsx 工作簿

所以它和 `topn-high-risk-discovery` 是并列关系：

- `topn-high-risk-discovery`：按综合风险排序，找“最值得优先看”的对象
- `condition-based-screening`：按指定规则筛，找“符合某类目标画像”的对象

---

## 一、数据集使用规则（前端和命令行都按这一套来）

### 规则 1：用户明确指定了数据集，就优先用用户指定的

如果问题里或命令里明确给了：

- `--dataset-root /workspace/imiss-deer-flow-main/datasets/phone-network`
- `--dataset unified`
- 或明确说“用 unified / sichuan / shaanxi”

那就直接按用户指定执行，不再猜测。

### 规则 2：用户没有明确指定数据集根目录时，优先使用真实 unified 预处理数据

脚本会优先自动寻找这些真实数据位置：

1. `PHONE_NETWORK_DATASETS_ROOT`
2. `<repo_root>/datasets/phone-network`
3. `/workspace/imiss-deer-flow-main/datasets/phone-network`
4. `/mnt/datasets/phone-network`
5. `~/imiss-deer-flow-main/datasets/phone-network`

### 规则 3：如果没有明确写 `--dataset`，默认按 `unified` 处理

所以在前端提问时：

- 用户说“在已预处理好的 unified 电话网络数据集中筛选”
- 用户说“直接用现有统一数据”

都应理解为：

- 默认真实数据集
- 默认 `unified`
- 不要先去找测试样例目录

### 规则 4：只有真实数据根目录确实找不到时，才允许退回测试样例

测试样例只是回归测试用，不是前端正式分析默认优先级。

---

## 二、支持的筛选模式

### 1）夜间异常

```bash
--mode night_abnormal
```

常配条件：

- `--min-night-ratio`
- `--min-night-count`

### 2）联系人广度异常

```bash
--mode broad_contacts
```

常配条件：

- `--min-counterparties`

### 3）共享设备显著

```bash
--mode shared_device
```

常配条件：

- `--min-shared-device-count`
- `--min-shared-peer-total`

### 4）高通话量

```bash
--mode high_call_volume
```

常配条件：

- `--min-call-records`

### 5）混合模式

```bash
--mode mixed
```

适合把多个条件一起组合起来做联合筛选。

---

## 三、支持的常用条件

- `--risk-only`：只保留风险标签对象
- `--unlabeled-only`：只保留未显式标注风险的对象
- `--labels`
- `--sub-labels`
- `--province`
- `--min-risk-score`
- `--min-call-records`
- `--min-counterparties`
- `--min-shared-device-count`
- `--min-shared-peer-total`
- `--min-night-ratio`
- `--min-night-count`
- `--match-mode all|any`
- `--min-match-count`
- `--top-k`

说明：

- `match-mode=all`：所有启用条件都要命中
- `match-mode=any`：命中任意条件即可
- `min-match-count`：至少命中几条条件，适合 mixed 场景进一步收紧

---

## 四、前端推荐提问模板

### 模板 1：夜间异常筛选

请使用 `condition-based-screening` skill，在已预处理好的 unified 电话网络数据集中筛出夜间行为明显异常的号码，并输出：
1. 候选规模和筛选后命中规模
2. 筛选链路
3. 命中对象表
4. 共享设备证据和共同对端证据
5. markdown 报告与 csv / xlsx 附件

### 模板 2：多条件联合筛选

请使用 `condition-based-screening` skill，在真实 unified 电话网络数据中筛出同时满足以下条件的对象：
- 四川省
- 风险标签对象
- 联系人广度较高
- 共享设备明显
并说明这些条件是否真的缩小了样本范围。

### 模板 3：未标注对象筛查

请使用 `condition-based-screening` skill，在 unified 数据中筛出“未显式标注风险，但共享设备明显且联系人广度偏高”的对象，并输出筛选链路与证据附件。

---

## 五、命令行正式分析（推荐写法）

先进入：

```bash
cd /workspace/imiss-deer-flow-main/skills/custom/phone-network-analysis/condition-based-screening/scripts
```

### 场景 1：夜间异常

```bash
python3 condition_based_screening_wrapper.py \
  --dataset-root /workspace/imiss-deer-flow-main/datasets/phone-network \
  --group-name night_abnormal_targets \
  --dataset unified \
  --mode night_abnormal \
  --min-night-ratio 0.35 \
  --min-night-count 8 \
  --top-k 20
```

### 场景 2：联系人广度异常

```bash
python3 condition_based_screening_wrapper.py \
  --dataset-root /workspace/imiss-deer-flow-main/datasets/phone-network \
  --group-name broad_contact_targets \
  --dataset unified \
  --mode broad_contacts \
  --min-counterparties 50 \
  --top-k 20
```

### 场景 3：共享设备显著

```bash
python3 condition_based_screening_wrapper.py \
  --dataset-root /workspace/imiss-deer-flow-main/datasets/phone-network \
  --group-name device_targets \
  --dataset unified \
  --mode shared_device \
  --min-shared-device-count 1 \
  --min-shared-peer-total 5 \
  --top-k 20
```

### 场景 4：混合联合筛选

```bash
python3 condition_based_screening_wrapper.py \
  --dataset-root /workspace/imiss-deer-flow-main/datasets/phone-network \
  --group-name mixed_targets \
  --dataset unified \
  --mode mixed \
  --province sichuan \
  --risk-only \
  --min-counterparties 50 \
  --min-shared-device-count 1 \
  --match-mode all \
  --top-k 20
```

### 场景 5：未标注但可疑对象筛选

```bash
python3 condition_based_screening_wrapper.py \
  --dataset-root /workspace/imiss-deer-flow-main/datasets/phone-network \
  --group-name unlabeled_suspicious_targets \
  --dataset unified \
  --mode mixed \
  --unlabeled-only \
  --min-counterparties 80 \
  --min-shared-device-count 1 \
  --match-mode all \
  --top-k 20
```

---

## 六、命令行回归测试

```bash
cd /workspace/imiss-deer-flow-main/skills/custom/phone-network-analysis/condition-based-screening/scripts
bash test_condition_based_screening.sh
```

这个回归测试主要用于验证脚本功能本身，必要时会使用脚本自带的小型样例数据。

---

## 七、输出文件

通常会输出到：

- `/mnt/user-data/outputs`
- 若该目录不可写，则应改到项目 `outputs/`

标准输出包括：

1. `report_md`：完整 markdown 报告
2. `targets_csv`：命中对象表
3. `devices_csv`：共享设备证据表
4. `counterparts_csv`：共同对端证据表
5. `summary_json`：结构化摘要
6. `evidence_xlsx`：单文件工作簿

---

## 八、结果应该怎么解读

这个 skill 输出的不只是“筛到了哪些号码”，还会回答：

- 样本范围是否真的被缩小了
- 每个命中对象命中了哪些条件
- 有没有共享设备证据
- 有没有共同对端证据
- 哪些对象适合继续联动：
  - `single-number-analysis`
  - `shared-device-analysis`
  - `overlap-analysis`
  - `association-path-analysis`

---

## 九、基础算子对齐

本 skill 属于“基础算子组合得到的电话网络高级筛选 skill”，其实现对齐关系如下：

- 条件对象筛选 = `node_lookup + aggregation_query + relationship_filter`
- 夜间行为筛选 = `time-window relationship_filter + aggregation_query`
- 联系人广度筛选 = `neighbor_query + aggregation_query`
- 共享设备筛选 = `query_shared_device + aggregation_query`
- 条件命中排序 = `aggregation_query + scoring_layer`
