# 重启后真实服务验证（2026-10-22）

> 新增接近用户使用方式的 90 个会话、120 回合真实服务测试，首次严格通过 109/120。新增历史附件、澄清续接、引用校验和开发服务器热重载问题，见 [扩展测试报告](intent-realistic-service-check-20261022.md)。

本轮通过当前项目 nginx 的 **3328** 入口调用实际 LangGraph 服务，不是单独导入分类函数。四个项目容器均处于刚重启后的运行状态；`/api/langgraph/ok` 返回 `ok:true`，`/api/models` 只列出 `qwen3.6-35b-a3b`，显示名为 Qwen3.6-35B-A3B。旧手册的 3538 端口已不适用于当前容器。

真实图中包含 IntentRecognitionMiddleware、ComplianceInputGateMiddleware、SceneSkillFilterMiddleware、RoutingHiddenStepMiddleware。全部测试设置 `interrupt_before=["model"]`，在业务模型执行前暂停，以验证中间件识别与规划；本轮未执行具体业务任务或向 OneCityAgent 平台发消息，也未修改、重启后端代码。

## 测试范围与结果

共 35 条：同学原始问题 28 条（纯文本、不传附件或文件元数据）；政策法规、电话网络、街景图像 3 条；模糊/多意图 3 条；移除的消防场景负例 1 条。每题创建独立测试会话，并发 2。所有原题文字与预期标签保持不变。

| 测试组 | 场景集合严格匹配 | 识别阶段观测均值 | 请求均值 | 请求 P95 |
| --- | --- | --- | --- | --- |
| 同学原题纯文本 | **24/28，85.71%** | **1.82 秒** | **4.17 秒** | 5.93 秒 |
| 补充的政策法规/电话网络/街景 | 3/3 | 1.74 秒 | 3.98 秒 | 4.57 秒 |
| 模糊问题与视频+网络多意图 | 3/3 | 1.26 秒 | 2.94 秒 | 3.06 秒 |
| 消防负例 | 0/1 | 1.59 秒 | 3.49 秒 | 3.49 秒 |
| 全部 | **30/35，85.71%** | 1.76 秒 | 4.03 秒 | 5.93 秒 |

识别阶段观测时间是流式事件中“前一中间件更新至意图中间件更新”的间隔，包含调度/传输观察开销，不是供应商纯推理时间。请求时间还包含创建会话、排队、构图、中间件与状态读取。以上均不等于完整业务任务完成时间。

34 条运行暂停在 model 前；1 条网络安全原题被 InputGate 提前终止。35 条均生成初步规划；34 条未被闸门终止的运行实际写入隐藏规划消息，且其消息通过移动端纯适配器 **34/34** 保留为 `task_planning` / `规划智能体` 片段。运输层无错误，未出现 structured_fallback 降级。

## 调用次数与额外耗时

34 条可完整采集模型流式轨迹的运行中，**意图识别节点各调用 Qwen 一次**，未观察到识别修复；另有 **4 次属于合规输入闸门的模型调用**。所有观测到的模型名均为 `qwen3.6-35b-a3b`。被提前终止的那条网络题没有完整保存模型流式轨迹，次数记作未知，未按零次计算。

流式接口还会发出场景技能说明与规划的 HumanMessage；这些消息不是模型调用，统计已按 `ls_model_name` 非空的唯一消息 ID 过滤。原始采集与纠正后的分类/调用统计均保留在结果目录，避免把指导消息误算为 LLM 调用。

因此，“意图识别一次调用”已经在运行服务中得到验证，但部分请求在合规边界复核时仍会增加模型调用；不能把整条前处理链一律称为一次。

## 发现的问题

1. **缺附件信息时误拒识。** `traffic-04` 纯文本“请帮我检查一下交通流量是否出现异常”返回空场景并要求数据来源/时间。这类缺执行数据的请求仍应能确定道路交通，当前处理偏保守。
2. **依赖背景的两条轨迹题仍返回澄清。** `trajectory-doc-09`、`trajectory-doc-10` 未匹配原材料预期轨迹场景。
3. **视频 embedding 技术任务仍误分代码。** `video-04` 被识别为 program_snippet，仍需改善业务执行与源码分析的边界。
4. **移除类别的请求被硬凑到近似类别。** “发生火灾告警，请登记告警”没有视频输入或视频分析目标，却被分到 video_surveillance。消防类本身未重新启用，但仍存在支持范围外请求误路由问题。
5. **合规输入疑似误拦截。** `network-01` 正确识别为 network_traffic 并生成规划，随后被 InputGate 以 `political`、`warn/refuse` 终止。其内容是政务网络异常通信排查，应单独复核该检测结果；本轮未绕过或修改合规策略。按识别层指标计对，按链路是否放行单独记录为拦截。

纯文本原题比此前独立评测的 25/28 少一题；两次输入条件不同，原评测给 traffic-04 提供了 anno.csv 的文件名元数据。新增独立对照保持同一问句：不传元数据再次返回空场景；通过接口 input.uploaded_files 传入原有 `{"filename":"anno.csv"}` 元数据后，正确识别 road_traffic。本对照仅检验状态元数据对识别的作用，未真实上传/读取 CSV，也未混入 28 题主统计。

## 可核对材料

- [35 条用例](intent-live-service-eval-20261022/cases.json)、[实际识别与会话 ID](intent-live-service-eval-20261022/results.jsonl)、[汇总](intent-live-service-eval-20261022/summary.json)
- [运行图](intent-live-service-eval-20261022/graph.json)、[附件元数据独立对照](intent-live-service-eval-20261022/metadata-comparison.json)
- [被拦截会话的状态与原因](intent-live-service-eval-20261022/network-blocked-state.json)、[规划适配器验证](intent-live-service-eval-20261022/planner-adapter-verification.json)
- [采集脚本](intent-live-service-eval-20261022/probe.py)、[源码和结果哈希](intent-live-service-eval-20261022/manifest.json)

采集脚本中 BASE/OUT 是本次固定入口与目录，复现时使用新的输出目录并复制 cases.json；它拒绝覆盖已有 results.jsonl。测试会话已标记 purpose，保留作核对，均未运行业务模型。
