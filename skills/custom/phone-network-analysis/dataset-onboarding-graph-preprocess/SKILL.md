# dataset-onboarding-graph-preprocess

## 1. 技能定位

`dataset-onboarding-graph-preprocess` 是电话网络数据分析体系中的“数据接入 / 预处理 / 建图”技能。

它负责把前端上传的原始 CSV、Excel、Parquet 或 JSON 表格转换为后续电话网络分析技能统一使用的图结构数据：

```text
processed/<dataset>/user_nodes.csv
processed/<dataset>/call_edges.csv
processed/graph_views/<dataset>/edges_phone_imei.parquet
```

只要新上传的数据被转换成上述结构，后续就可以继续调用 `dataset-overview-analysis`、`single-number-analysis`、`topn-high-risk-discovery`、`condition-based-screening`、`shared-device-analysis`、`group-risk-analysis`、`gang-cluster-analysis`、`risk-evidence-pack`、`time-series-anomaly-analysis` 等技能。

## 2. 必须使用本技能的场景

只要用户提出下面任意需求，就必须优先使用本技能：

- 上传了新的电话网络原始 CSV / Excel / JSON / Parquet，需要转换成项目统一图结构；
- 希望把原始通话记录、用户标签表、设备绑定表接入现有电话网络分析链路；
- 希望生成 `user_nodes.csv`、`call_edges.csv`、`edges_phone_imei.parquet`；
- 希望检查上传数据字段能否被识别；
- 希望处理“不规整数据”“中文字段”“坏时间”“重复行”“缺失值”；
- 希望处理错误数据，并说明为什么不能建图；
- 用户提到“建图、预处理、数据接入、上传数据、原始数据、转成标准图结构、无法建图、质量检查、字段映射”等关键词。

如果用户只是想分析已经预处理好的 `unified` 数据，不需要调用本技能，应直接调用对应分析技能。

## 3. 前端调用原则

前端使用时必须优先运行正式 wrapper，**不要临时重写脚本，不要手写替代脚本**。

标准脚本路径：

```text
/mnt/skills/custom/phone-network-analysis/dataset-onboarding-graph-preprocess/scripts/dataset_onboarding_graph_preprocess_wrapper.py
```

如果运行环境是项目 Docker，路径通常是：

```text
/workspace/imiss-deer-flow-main/skills/custom/phone-network-analysis/dataset-onboarding-graph-preprocess/scripts/dataset_onboarding_graph_preprocess_wrapper.py
```

如果前端上传文件后不知道路径，先在常见上传目录中查找：

```bash
find /mnt/user-data -maxdepth 6 \( -name "*.csv" -o -name "*.xlsx" -o -name "*.xls" -o -name "*.json" -o -name "*.parquet" \) 2>/dev/null | head -80
```

然后把**真实包含上传文件的目录**作为 `--input-dir`。常见前端上传目录可能是：

```text
/mnt/user-data/uploads/
/mnt/user-data/workspace/
/mnt/user-data/workspace/onboarding_input/
```

不要直接使用不存在的占位路径，例如 `/workspace/imiss-deer-flow-main/uploads/new_phone_dataset`，除非你已经手动把文件放到了这个目录。

## 4. 用户可见输出规范

- 面向用户的最终回答必须使用中文。
- 不要输出英文说明句，例如 “Run the script”、“Read report”、“Generated files”。
- 可以保留命令、文件名、参数名本身的英文，例如 `user_nodes.csv`、`artifact_mode=essential`。
- 不要在最终回答正文里连续粘贴同一份 Markdown 报告两遍。
- 下载入口由前端附件卡片展示即可，正文只需要简要说明“报告和标准图结构文件已生成”。
- 如果已经展示了完整 Markdown 报告正文，就不要再把同一份报告全文复制一次。
- 如果输入文件缺少号码字段，也要运行本技能生成质量诊断报告，不要回复“找不到 skill”。

## 5. 输入数据要求

本技能支持一个目录或多个文件。支持格式：

- `.csv`
- `.xlsx` / `.xls`
- `.parquet`
- `.json`

可以接受以下几类表：

1. 用户标签表：包含号码、标签、省份等字段。
2. 通话明细表：包含主叫、被叫、时间、时长、设备等字段。
3. 设备绑定表：包含号码和设备 ID。
4. 混合明细表：一张表里同时包含号码、对端、设备、时间、标签。
5. 错误或不完整表：脚本会尽量读取并输出质量诊断，不会直接让前端无结果。

注意：设备绑定表没有“对端列”、用户标签表没有“对端列/设备列”是正常情况。脚本会把这类情况记录为“角色说明”，不当作质量错误。

## 6. 字段自动识别

脚本会自动识别常见中英文字段名：

| 标准含义 | 常见字段名 |
|---|---|
| 号码 | phone、phone_number、mobile、msisdn、caller、src_user_id、subscriber、main_number、主叫、主叫号码、手机号、手机号码、号码、用户号码 |
| 对端 | callee、called、counterparty、dst_counterparty_id、peer、target、peer_number、被叫、被叫号码、对端、对端号码、联系人号码 |
| 设备 | imei、device_id、terminal、terminal_id、设备号、终端、终端号、meid、imsi |
| 时间 | event_time、call_time、timestamp、start_datetime、通话时间、呼叫时间、开始时间 |
| 日期 | event_date、date、call_date、通话日期 |
| 小时 | event_hour、hour、call_hour、小时、时段 |
| 时长 | duration、duration_sec、call_duration、seconds、通话时长、时长、秒数 |
| 标签 | label、is_risk、risk_label、标签、风险标签、是否风险 |
| 子标签 | sub_label、risk_type、子标签、风险类型、标签类型 |
| 省份 | province、省份、归属省、归属地省、归属地、area_province |

如果自动识别不准，必须通过命令行显式指定字段，例如：

```bash
--source-col caller --target-col callee --device-col imei --time-col call_time
```

## 7. 输出 schema

### user_nodes.csv

```text
province,dataset_name,user_id,label,sub_label,age,open_card_time,access_mode,monthly_fee,monthly_flow_mb,monthly_call_duration,caller_ratio_3m,caller_dispersion_3m,cross_province_ratio_3m,broadband_flag,source_table
```

### call_edges.csv

```text
province,dataset_name,src_user_id,dst_counterparty_id,event_time,event_date,event_hour,duration,call_type,imei,city,county,station,cell,roaming_place,counterparty_belong,source_table
```

### edges_phone_imei.parquet

```text
src_id,dst_id,src_type,dst_type,edge_type,dataset,user_id,imei,edge_count
```

## 8. 脱敏与 ID 规则

默认情况下，本技能会对原始号码、对端和设备 ID 重新哈希，避免把原始敏感 ID 写入图结构或报告。

默认哈希策略：

- 号码和对端使用同一类 `phone` 哈希空间；
- 设备使用 `device` 哈希空间；
- 同一原始号码在不同文件中会得到相同哈希 ID；
- 同一原始设备在不同文件中会得到相同哈希 ID；
- 可以通过 `--hash-salt` 固定盐值，保证同一批数据可复现。

如果输入本身已经是哈希 ID，并且确实希望保留，可使用：

```bash
--preserve-existing-ids
```

注意：如果多个来源的数据想做跨来源同实体联动，必须使用同一哈希盐值、统一脱敏规则或提供实体映射表。

## 9. artifact_mode 规则

- `markdown_only`：只展示 Markdown 报告下载入口。
- `essential`：展示 Markdown 报告 + 标准三件套：`user_nodes.csv`、`call_edges.csv`、`edges_phone_imei.parquet`。推荐前端默认使用。
- `full`：展示 Markdown 报告 + 标准三件套 + summary / mapping / quality 明细文件。适合命令行验收或调试。

无论 `artifact_mode` 怎么设置，脚本都会在输出目录生成完整文件。这个参数只控制前端展示哪些下载卡片，避免附件过多或重复。

## 10. 标准运行命令

前端常用命令：

```bash
cd /mnt/skills/custom/phone-network-analysis/dataset-onboarding-graph-preprocess/scripts

python3 dataset_onboarding_graph_preprocess_wrapper.py \
  --input-dir /mnt/user-data/uploads \
  --dataset-root /mnt/user-data/workspace/phone-network \
  --dataset onboarded_demo \
  --dataset-name phone-network-onboarded-demo \
  --province unknown \
  --hash-salt project_fixed_salt \
  --hash-length 64 \
  --overwrite \
  --artifact-mode essential
```

本地 Docker 项目环境常用命令。注意：使用前必须先把待处理文件放进该目录：

```bash
mkdir -p /workspace/imiss-deer-flow-main/uploads/new_phone_dataset
# cp /path/to/your/*.csv /workspace/imiss-deer-flow-main/uploads/new_phone_dataset/

cd /workspace/imiss-deer-flow-main/skills/custom/phone-network-analysis/dataset-onboarding-graph-preprocess/scripts

python3 dataset_onboarding_graph_preprocess_wrapper.py \
  --input-dir /workspace/imiss-deer-flow-main/uploads/new_phone_dataset \
  --dataset-root /workspace/imiss-deer-flow-main/datasets/phone-network \
  --dataset onboarded_demo \
  --province unknown \
  --hash-salt project_fixed_salt \
  --hash-length 64 \
  --overwrite \
  --artifact-mode essential
```

如果路径下没有可读取文件，脚本会输出质量诊断报告，不会直接崩溃。

## 11. 前端推荐提示词

### 规整数据接入

```text
请使用 dataset-onboarding-graph-preprocess skill，将我上传的原始电话网络 CSV/Excel 文件转换为电话网络分析所需的标准图结构。请自动识别号码、对端、设备、时间、标签和省份字段，生成 user_nodes.csv、call_edges.csv、edges_phone_imei.parquet，并输出字段映射、质量检查和 markdown 报告。参数：dataset=onboarded_demo，artifact_mode=essential，hash_length=64。请只展示一次报告，不要连续重复粘贴同一份 Markdown。
```

### 不规整数据接入

```text
请使用 dataset-onboarding-graph-preprocess skill，处理我上传的不规整电话网络数据。数据中可能包含中文字段、缺失值、坏时间、重复行和 JSON 用户标签。请先查找上传文件所在目录，再运行 dataset_onboarding_graph_preprocess_wrapper.py，不要临时重写脚本。请尽量自动识别字段并生成标准图结构，同时输出字段映射、质量检查、警告说明和 markdown 报告。参数：dataset=onboarded_messy_demo，artifact_mode=essential，hash_length=64。请只展示一次报告，不要连续重复粘贴同一份 Markdown。
```

### 错误数据诊断

```text
请使用 dataset-onboarding-graph-preprocess skill，处理我上传的错误测试数据。即使数据缺少号码字段，也请运行正式 wrapper 输出质量检查报告，说明为什么不能正常建图。不要回复找不到 skill，不要临时重写脚本。参数：dataset=onboarded_bad_demo，artifact_mode=markdown_only，hash_length=64。
```

## 12. 一键测试

```bash
cd /workspace/imiss-deer-flow-main/skills/custom/phone-network-analysis/dataset-onboarding-graph-preprocess/scripts
chmod +x *.py *.sh
bash test_dataset_onboarding_graph_preprocess.sh
```

测试脚本会：

1. 生成规整原始通话数据、用户标签表和设备绑定表；
2. 检查规整数据能否生成标准三件套；
3. 检查显式字段映射和 `markdown_only` 模式；
4. 检查不规整中文字段、缺失值、坏时间场景；
5. 检查缺少号码字段的错误数据是否能生成质量诊断报告；
6. 检查不存在输入目录时能否输出诊断 JSON / Markdown；
7. 检查使用真实存在输入目录的正式处理命令；
8. 确认最终输出 `[OK] dataset-onboarding-graph-preprocess tests finished`。

## 13. 重要边界

- 本技能只负责把原始表格转换为当前项目需要的图结构，不负责判断业务风险事实。
- 如果原始文件没有号码字段，不能建用户节点，但会生成质量诊断报告。
- 如果原始文件没有对端字段，不能建通话边。
- 如果原始文件没有设备字段，不能建共享设备边。
- 如果不同来源数据要做同实体联动，必须使用统一哈希盐值、统一脱敏规则或实体映射表。
- 本技能生成的是表格式图结构，不是必须写入图数据库。后续技能通过 CSV/Parquet 表执行图查询和聚合分析。
