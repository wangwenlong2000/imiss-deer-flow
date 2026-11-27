# 合规语料与整改评测索引

本目录保存合规评测语料、标注说明，以及类型 3、6、7 的整改数据、预测、指标和报告。项目文档入口见 [文档索引](../docs/README.md)，规则接入、策略合同及其他参考材料见 [合规文档索引](../docs/compliance/README.md)，运行评测结果见 [outputs 合规评测入口](../outputs/README.md#合规评测)。

检测器运行代码位于 [后端合规模块](../backend/packages/harness/deerflow/compliance/)，运行配置位于 [config/compliance](../config/compliance/)。本目录的数据与报告按各自版本留存；原始语料、开发回归、历史固定分区和独立冻结测试分别使用，不能混称为统一正式验收成绩。

## 方法与标注说明

- [模型方法与数据包说明](violation_detection_model_normalized_723_README.md)：历史模型包的方法、输入输出及校验说明。
- [合规评测标注指南](合规评测标注指南7.23.docx)：原始标注参考材料。
- [提交材料](提交.docx)：保留的原始 Office 材料，具体要求结合版本与正文核对。
- [三个检测器规则修改汇总报告](三个检测器规则修改汇总报告.md)：类型 3、6、7 的修改、结果及已知限制；分项入口见下文。

仓库其他原始 DOCX/PDF 参考资料通过 [合规文档索引](../docs/compliance/README.md) 查找。

## 十场景原始评测语料

[合规评测全部十类违规类型数据](合规评测全部十类违规类型数据/) 保存按业务来源组织的 JSONL 标注。目录中的十个文件表示语料来源，不表示十种检测类型与文件逐一对应。

| 来源 | 文件 |
| --- | --- |
| 政策法规 | [policy_regulation.jsonl](合规评测全部十类违规类型数据/policy_regulation.jsonl) |
| 程序代码 | [program_code.jsonl](合规评测全部十类违规类型数据/program_code.jsonl) |
| 网络流量 | [network_flow_compliance_annotations_150.jsonl](合规评测全部十类违规类型数据/network_flow_compliance_annotations_150.jsonl) |
| 视频监控 | [video_monitoring_200.jsonl](合规评测全部十类违规类型数据/video_monitoring_200.jsonl) |
| 交通流量 | [repair_traffic_flow_samples_150.jsonl](合规评测全部十类违规类型数据/repair_traffic_flow_samples_150.jsonl) |
| 遥感 | [remote_sensing_all_120.jsonl](合规评测全部十类违规类型数据/remote_sensing_all_120.jsonl) |
| 统计年鉴 | [compliance_annotations_250.jsonl](合规评测全部十类违规类型数据/compliance_annotations_250.jsonl) |
| 通信网络 | [phone_network.jsonl](合规评测全部十类违规类型数据/phone_network.jsonl) |
| 街景 | [street_view_200.jsonl](合规评测全部十类违规类型数据/street_view_200.jsonl) |
| 时空轨迹 | [spatiotemporal_compliance_all_225.jsonl](合规评测全部十类违规类型数据/spatiotemporal_compliance_all_225.jsonl) |

跨来源、统一十类及 Gate/场景实验的结果保存在 [outputs/compliance-eval](../outputs/compliance-eval/)，应按报告中的数据范围、规则/模型版本和评测口径阅读。

## 类型 3：硬编码凭证整改

- [修改报告](类型3-hardcoded_cred整改/修改报告.md)。
- [开发回归数据](类型3-hardcoded_cred整改/program_code_type3_regression.jsonl)、[指标](类型3-hardcoded_cred整改/latest_metrics.json)。
- [数据构建与评测脚本](../scripts/build_eval_hardcoded_cred_program_code.py)。

此组包含原始代码样本及新增合成边界样本，是规则调整后的开发回归，尚不构成独立冻结集。纯规则检测结果与生产 Gate 适用范围、完整引擎处置分别核验。

## 类型 6：文本身份标识整改

[修改报告](类型6-text_id整改/修改报告.md) 同时解释以下两组数据的历史与边界。

| 分组 | 数据与证据 | 使用边界 |
| --- | --- | --- |
| 历史回归及事后划分 | [全量回归](类型6-text_id整改/text_id_type6_regression.jsonl)、[划分清单](类型6-text_id整改/split_manifest.json)、[指标](类型6-text_id整改/latest_metrics.json) | 样本曾被查看和用于调规则；事后划分不能作为独立冻结测试 |
| 新批次独立评测 | [数据](类型6-text_id整改/新批次独立评测/fresh_text_id_100.jsonl)、[划分清单](类型6-text_id整改/新批次独立评测/split_manifest.json)、[冻结规则记录](类型6-text_id整改/新批次独立评测/frozen_rule.json) | 按准备、开发、冻结、测试的顺序留样 |
| 新批次开发分区 | [开发集](类型6-text_id整改/新批次独立评测/development_20.jsonl)、[开发指标](类型6-text_id整改/新批次独立评测/development_metrics.json) | 用于本批次规则开发，不与冻结测试成绩混算 |
| 新批次冻结测试分区 | [测试集](类型6-text_id整改/新批次独立评测/test_80.jsonl)、[测试指标](类型6-text_id整改/新批次独立评测/test_metrics.json)、[逐条预测](类型6-text_id整改/新批次独立评测/test_predictions.jsonl) | 保留误报；与开发集同属合成模板家族，仍需外部来源复核 |

复现入口：[历史数据构建脚本](../scripts/build_eval_text_id.py)、[新批次冻结评测脚本](../scripts/text_id_fresh_holdout.py)。纯规则指标不包含在线 LLM 边界复核或完整 Gate/策略链路验收。

## 类型 7：机密信息整改

- [修改报告](类型7-confidential整改/修改报告.md)。
- [回归数据](类型7-confidential整改/confidential_type7_regression.jsonl)、[固定测试分区](类型7-confidential整改/confidential_type7_test_split.jsonl)。
- [开发指标](类型7-confidential整改/dev_metrics.json)、[测试指标](类型7-confidential整改/latest_metrics.json)、[测试逐条预测](类型7-confidential整改/latest_predictions.jsonl)。
- [数据构建与评测脚本](../scripts/build_eval_confidential.py)。

该测试分区曾被探索性运行和查看，属于固定分区回归，不能称为盲测或独立冻结集。纯规则指标不包含在线 LLM 复核；最终外发处置还依赖具体制度和人工核实。

## 阅读与复现约定

- 结合报告中的数据来源、划分历史、规则哈希和错误样本阅读指标；`latest_*` 只表示对应脚本输出的命名，不能自动推断为当前版本正式成绩。
- 纯规则、LLM 复核、完整引擎 Gate/策略与实际业务验收分别报告；不同数据集的指标不直接横向排序。
- 复现脚本可能更新数据、划分或 `latest_*` 产物。先保留既有版本和证据，再在明确的新批次执行，避免覆盖冻结结果。
- 本次目录分类只新增索引，所有既有语料、报告、指标和冻结记录保持原位。
