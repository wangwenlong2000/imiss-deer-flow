# 条件筛选分析报告：unlabeled_case

## 一、总体结论

- 数据集：`unified`
- 候选对象数：`5`
- 筛选命中数：`4`
- 模式：`mixed`
- 匹配模式：`any`
- 收缩比例：`20.00%`
- 总体结论：筛选后命中 4 个对象。
- 过滤影响：当前筛选条件共缩小 1 个对象，收缩比例 20.00%，筛选对样本范围产生了真实约束；匹配模式为 any。
- 关键发现：Top1 对象 p5：风险分 44.75，命中 3 条条件。
- 关键发现：最显著共享设备 imei_a：群体内命中成员 3 个，共挂载 3 个号码。
- 关键发现：最显著共同对端 c1：被 4 个命中对象共同联系，累计通话 4 次。

## 二、启用的筛选条件

- `unlabeled_only`：仅保留未显式标注为风险的对象
- `broad_contacts`：联系人广度 >= 3
- `shared_device_count`：共享设备数 >= 1

## 三、筛选链路

- initial_scope：5
- hit_unlabeled_only：2
- hit_broad_contacts：4
- hit_shared_device_count：3
- after_any_union：4

### 条件命中统计
- unlabeled_only：命中 2 个对象
- broad_contacts：命中 4 个对象
- shared_device_count：命中 3 个对象

## 四、命中对象 Top 列表

- Rank 1: `p5` | score=44.75 | matched=3 | evidence=未标注风险对象；联系人广度=4；共享设备数=1 | 推荐下钻=shared-device-analysis, single-number-analysis, time-series-anomaly-analysis
- Rank 2: `p1` | score=41.25 | matched=2 | evidence=联系人广度=12；共享设备数=1 | 推荐下钻=shared-device-analysis, single-number-analysis, time-series-anomaly-analysis
- Rank 3: `p2` | score=37.0 | matched=2 | evidence=联系人广度=6；共享设备数=1 | 推荐下钻=shared-device-analysis, single-number-analysis
- Rank 4: `p3` | score=26.0 | matched=2 | evidence=未标注风险对象；联系人广度=3 | 推荐下钻=single-number-analysis

## 五、命中对象画像概览

### 省份分布
- sichuan：4 个

### sub_label 分布
- normal：2 个
- purefraud：1 个
- risk：1 个

## 六、共享设备证据

- 设备 `imei_a` | 命中成员数=3 | 总挂载号码数=3 | 风险号码数=2 | 成员预览=p1, p2, p5
- 设备 `imei_b` | 命中成员数=1 | 总挂载号码数=1 | 风险号码数=0 | 成员预览=p3
- 设备 `imei_d` | 命中成员数=1 | 总挂载号码数=1 | 风险号码数=0 | 成员预览=p5

## 七、共同对端证据

- 对端 `c1` | 共接触成员数=4 | 总通话量=4 | 成员预览=p1, p2, p3, p5
- 对端 `c2` | 共接触成员数=4 | 总通话量=4 | 成员预览=p1, p2, p3, p5
- 对端 `c3` | 共接触成员数=4 | 总通话量=4 | 成员预览=p1, p2, p3, p5
- 对端 `c4` | 共接触成员数=3 | 总通话量=3 | 成员预览=p1, p2, p5
- 对端 `c5` | 共接触成员数=2 | 总通话量=2 | 成员预览=p1, p2
- 对端 `c6` | 共接触成员数=2 | 总通话量=2 | 成员预览=p1, p2

## 八、后续建议

- 优先对 Top1 `p5` 调用 single-number-analysis 做单号深挖。
- 命中对象存在共享设备证据，建议继续调用 shared-device-analysis。
- 命中对象存在共同对端重叠，建议结合 overlap-analysis 或 association-path-analysis 做关系复核。
- 可对 Top1 和 Top2 做 association-path-analysis / overlap-analysis 复核路径与同圈关系。

## 九、基础算子对齐

- 条件对象筛选 = node_lookup + aggregation_query + relationship_filter
- 夜间行为筛选 = time-window relationship_filter + aggregation_query
- 联系人广度筛选 = neighbor_query + aggregation_query
- 共享设备筛选 = query_shared_device + aggregation_query
- 条件命中排序 = aggregation_query + scoring_layer

## 十、生成文件

- `report_md`：condition_screening_report_unlabeled_case_unified.md
- `targets_csv`：condition_screening_targets_unlabeled_case_unified_4targets.csv
- `devices_csv`：condition_screening_devices_unlabeled_case_unified_3rows.csv
- `counterparts_csv`：condition_screening_counterparts_unlabeled_case_unified_6rows.csv
- `summary_json`：condition_screening_summary_unlabeled_case_unified.json
- `evidence_xlsx`：condition_screening_evidence_unlabeled_case_unified.xlsx
