# 条件筛选分析报告：night_case

## 一、总体结论

- 数据集：`unified`
- 候选对象数：`5`
- 筛选命中数：`2`
- 模式：`night_abnormal`
- 匹配模式：`all`
- 收缩比例：`60.00%`
- 总体结论：筛选后命中 2 个对象。
- 过滤影响：当前筛选条件共缩小 3 个对象，收缩比例 60.00%，筛选对样本范围产生了真实约束；匹配模式为 all。
- 关键发现：Top1 对象 p1：风险分 40.25，命中 2 条条件。
- 关键发现：最显著共享设备 imei_a：群体内命中成员 2 个，共挂载 3 个号码。
- 关键发现：最显著共同对端 c1：被 2 个命中对象共同联系，累计通话 2 次。

## 二、启用的筛选条件

- `night_ratio`：夜间通话占比 >= 0.50
- `night_count`：夜间通话次数 >= 4

## 三、筛选链路

- initial_scope：5
- after_night_ratio：2
- after_night_count：2

### 条件命中统计
- night_ratio：命中 2 个对象
- night_count：命中 2 个对象

## 四、命中对象 Top 列表

- Rank 1: `p1` | score=40.25 | matched=2 | evidence=夜间占比=0.92；夜间次数=11 | 推荐下钻=shared-device-analysis, single-number-analysis, time-series-anomaly-analysis
- Rank 2: `p5` | score=34.75 | matched=2 | evidence=夜间占比=1.00；夜间次数=4 | 推荐下钻=shared-device-analysis, single-number-analysis, time-series-anomaly-analysis

## 五、命中对象画像概览

### 省份分布
- sichuan：2 个

### sub_label 分布
- purefraud：1 个
- normal：1 个

## 六、共享设备证据

- 设备 `imei_a` | 命中成员数=2 | 总挂载号码数=3 | 风险号码数=1 | 成员预览=p1, p5
- 设备 `imei_d` | 命中成员数=1 | 总挂载号码数=1 | 风险号码数=0 | 成员预览=p5

## 七、共同对端证据

- 对端 `c1` | 共接触成员数=2 | 总通话量=2 | 成员预览=p1, p5
- 对端 `c2` | 共接触成员数=2 | 总通话量=2 | 成员预览=p1, p5
- 对端 `c3` | 共接触成员数=2 | 总通话量=2 | 成员预览=p1, p5
- 对端 `c4` | 共接触成员数=2 | 总通话量=2 | 成员预览=p1, p5

## 八、后续建议

- 优先对 Top1 `p1` 调用 single-number-analysis 做单号深挖。
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

- `report_md`：condition_screening_report_night_case_unified.md
- `targets_csv`：condition_screening_targets_night_case_unified_2targets.csv
- `devices_csv`：condition_screening_devices_night_case_unified_2rows.csv
- `counterparts_csv`：condition_screening_counterparts_night_case_unified_4rows.csv
- `summary_json`：condition_screening_summary_night_case_unified.json
- `evidence_xlsx`：condition_screening_evidence_night_case_unified.xlsx
