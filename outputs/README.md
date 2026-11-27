# 运行产物与证据索引

本目录保存按实验、日期和版本留存的报告、结构化结果、执行日志及复现脚本。项目文档入口见 [文档索引](../docs/README.md)。文件名中的 `final`、`latest` 或日志存在本身不表示当前验收通过；应结合对应报告、失败记录和适用范围阅读。

Git 仅收录本索引、目录整理记录及经过检查的 rc4 公开摘要和白名单证据。其余原始日志、历史运行结果和测试账号材料保留在本机，以下对应链接在仅克隆 GitHub 仓库时可能不存在。安装镜像包另存于服务器的 `/mnt/data/`，不随 Git 上传。

现有证据保留原目录和文件名。以下分类只提供入口，不移动或重写原始结果；`__pycache__/` 为自动缓存，不属于报告材料。

## 当前工作区整理记录

[2026-11-27 目录整理](../docs/history/目录整理记录_20261127.md) 保存本次分类说明；逐文件[旧新路径对照](workspace-organization-20261127/moves.csv)和[校验结果](workspace-organization-20261127/verification.json)位于独立目录，不混入交付验收证据。

## 交付验收

当前基础单机版的交付状态以 [rc4 正式验收记录](../docs/acceptance/20261127-rc4.md) 为准，原始执行材料见 [rc4 证据说明](delivery-20261127-rc4/README.md)。同机独立目录恢复、镜像完整性和业务验收分别记录，不能据此扩大为跨机器迁移或全部 Skill 场景通过。

| 入口 | 用途 |
| --- | --- |
| [最终交付摘要](delivery-20261127-rc4/release-delivery-summary.json) | 发布材料、包后检查和已知边界的结构化汇总 |
| [步骤日志索引](delivery-20261127-rc4/commands.jsonl) | 追加式命令、退出码和对应日志；保留失败及后续重试 |
| [源码回归摘要](delivery-20261127-rc4/regression-summary.json) | 部署/Skill、OneCity、合规回归；不替代真实业务验收 |
| [镜像构建摘要](delivery-20261127-rc4/image-build-summary.json) | 精确镜像身份及构建来源核验 |
| [Gateway 尝试历史](delivery-20261127-rc4/gateway-attempt-history.json) | 首次失败与一次重试通过分别保留，不能只取最新成功 |
| [流式镜像归档核验](delivery-20261127-rc4/image-archive-verification-summary.json) | 归档哈希、镜像身份及原始缺失 descriptor 数组 |
| [目标平台核验](delivery-20261127-rc4/image-archive-target-platform-summary.json) | Linux amd64 payload 完整；MySQL 非目标 ARM64 及其 attestation 仍缺失，未实测 `docker load` |
| [白名单证据说明](delivery-20261127-rc4/delivery-evidence-public/README.md) | 脱敏站点快照、持久留样和恢复证据入口 |

历史过程材料保留在 [rc1](delivery-20261126-rc1/)、[base](delivery-20261127-base/)、[final](delivery-20261127-final/) 和 [continuation](delivery-20261127-continuation/README.md)。这些目录反映各自版本和执行阶段，不代表 rc4 的最新通过结论。发布包生成后的校验写在包外证据目录，不回写冻结的发布包。

## 合规评测

合规语料、整改数据和评测边界见 [合规数据索引](../合规检测/README.md)，接入与规则说明见 [合规文档索引](../docs/compliance/README.md)。以下文件属于不同评测口径，不能直接合并指标。

| 入口 | 用途 |
| --- | --- |
| [十类统一评测](compliance-eval/unified-ten-type.json) / [逐条预测](compliance-eval/unified-ten-type-predictions.jsonl) | 十类检测结果及样本级记录 |
| [人工复核记录](compliance-eval/unified-ten-type-manual-review.jsonl) | 与自动预测分开保存的人工复核 |
| [Gate 与场景评测](compliance-eval/gate-scene-eval.json) / [逐条预测](compliance-eval/gate-scene-eval-predictions.jsonl) | Gate、场景与策略链路的评测口径 |
| [模型跨来源评测](compliance-eval/model-cross-source.json) | 跨来源评测结果 |
| [修复后报告](compliance-eval/after-compliance-repair/report.json) / [预测](compliance-eval/after-compliance-repair/predictions.jsonl) | 对应修复阶段的评测快照 |
| [工作区报告](compliance-eval/current-workspace/report.json) / [预测](compliance-eval/current-workspace/predictions.jsonl) | 运行当时的工作区快照，不能自动视为当前源码结果 |
| [复现记录](compliance-eval/reproduction-records/reproduction.json) | 复现过程入口；日期批次见 [完整目录](compliance-eval/) |

开发回归、历史固定分区和独立冻结测试应分别报告；纯规则成绩也不等于在线 LLM 复核或完整业务链路通过。

## 多用户联调

[2026-11-12 联调目录](onecity-multiuser-integration-20261112/) 保存该批次的部署和真实 HTTP 结果，不替代当前版本验收。

- [部署摘要](onecity-multiuser-integration-20261112/deployment-summary.json)、[真实 HTTP 摘要](onecity-multiuser-integration-20261112/real-http-summary.json)。
- [生产冒烟结果](onecity-multiuser-integration-20261112/production-smoke.json)、[准备记录 v2](onecity-multiuser-integration-20261112/production-smoke-prepare-v2.json)。
- [清理记录](onecity-multiuser-integration-20261112/production-smoke-cleanup.json)、[清理记录 v2](onecity-multiuser-integration-20261112/production-smoke-cleanup-v2.json)；清理记录与业务成功记录分别阅读。

## 并发

| 报告与结果 | 复现脚本 |
| --- | --- |
| [并发测试报告](concurrency-test-report-2026-10-15.md) / [JSON 结果](concurrency-results.json) | [concurrency_probe.py](concurrency_probe.py) |
| [真实请求并发报告](real-request-concurrency-report-2026-10-15.md) / [JSON 结果](real-request-concurrency-results.json) | [real_request_concurrency_probe.py](real_request_concurrency_probe.py) |

结果对应报告中的请求、模型和资源配置；真实请求、模拟请求及不同并发控制设置分别比较。

## 性能

先阅读 [耗时拆解报告](latency-breakdown-report-2026-10-15.md) 和 [思考模式质量报告](latency-thinking-quality-report-2026-10-15.md)。`fake` 为模拟模型对照，不能当作真实模型时延或业务质量成绩；`real` 为对应真实请求实验。单请求、并发、预热及 thinking 控制条件不能混算。

| 实验 | 分析与原始记录 |
| --- | --- |
| 模拟模型 | [分析](latency-fake-analysis.json)、[runs](latency-fake-runs.json)、[调用记录](latency-fake-model-calls.jsonl) |
| 模拟模型预热 | [分析](latency-fake-warm-analysis.json)、[runs](latency-fake-warm-runs.json) |
| 真实模型单请求 | [分析](latency-real-single-analysis.json)、[runs](latency-real-single-runs.json) |
| 真实模型并发 | [分析](latency-real-concurrent-analysis.json)、[runs](latency-real-concurrent-runs.json)、[模型调用记录](latency-real-model-calls.jsonl) |
| 直接调用与并行对照 | [控制结果](latency-direct-parallel-control.json)、[调用时间线](latency-model-call-timeline.csv) |
| thinking 控制 | [控制设置/结果](latency-thinking-control.json)、[street-alert 关闭思考](latency-think-off-street-alert-result.json)、[开启思考](latency-think-on-street-alert-result.json)、[wide-scene 开启思考](latency-think-on-wide-scene-result.json) |
| thinking 调用明细 | [street-alert 关闭](latency-think-off-street-alert-calls.jsonl)、[street-alert 开启](latency-think-on-street-alert-calls.jsonl)、[wide-scene 开启](latency-think-on-wide-scene-calls.jsonl) |

复现与分析入口：[运行驱动](latency_run_driver.py)、[模型代理](latency_model_proxy.py)、[耗时分析](latency_analyze.py)、[直接模型探针](latency_direct_model_probe.py)、[思考质量探针](latency_thinking_quality_probe.py)。

## 汇报与录制

- [近期研发成果语料](ppt-materials-20261112/近期研发成果_PPT汇报语料.md) / [Word 版](ppt-materials-20261112/近期研发成果_PPT汇报语料.docx)。
- [近两周研发进展](ppt-materials-20261112/近两周研发进展_功能汇报版.md) / [Word 版](ppt-materials-20261112/近两周研发进展_功能汇报版.docx)。
- [两分钟录制脚本](recording-handoff-20261112/两分钟录制脚本.md)、[录制提问清单](recording-handoff-20261112/录制提问清单.md)、[桌面操作流程](recording-handoff-20261112/桌面Codex录制操作流程.md)。
- [预期结果](recording-handoff-20261112/expected-results.json)、[录制准备状态](recording-handoff-20261112/recording-readiness.json)、[准备记录](recording-handoff-20261112/setup.json)、[浏览器权限核验脚本](recording-handoff-20261112/verify-permission-in-browser.js)、[演示数据](recording-handoff-20261112/assets/)。
- [录制测试资料包](recording-handoff-20261112/录制测试资料.zip)、[桌面录制交接包](recording-handoff-20261112/桌面Codex录制交接包.zip)。

汇报文案、演示预期和打包交接材料供说明与复现使用；验收结论仍以对应版本的正式记录和实际执行证据为准。
