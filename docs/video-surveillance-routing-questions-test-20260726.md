# 视频监控 Skill 路由问题清单测试报告（2026-07-26，StreetModel 旁路）

本报告测试的是 [`video-surveillance-skill-routing-questions.md`](video-surveillance-skill-routing-questions.md)
中的自然语言题干，方法参照 [`deerflow-agent-skill-test.md`](deerflow-agent-skill-test.md) 的三层结构。
与之前的 `C-0xx` 批次不同，本次发送的是问题清单里的原始题干，不指定 skill。

## 1. 测试条件

| 项目 | 实际值 |
| --- | --- |
| Agent | `lead_agent` |
| 模型 | `qwen3.6-35b-a3b`（SSE 中出现的 model 字段唯一值） |
| API | `http://127.0.0.1:3538/api/langgraph` → HTTP 200 |
| 服务栈 | `qwen36test-deer-flow-{nginx,gateway,langgraph,frontend}` 运行中，沙箱容器 healthy |
| 测试视频 | `Trafic-30s.mp4`（ffprobe 实测 30.0s / 1280x674 / 12fps / h264 / 1533354 字节） |
| StreetModel | `219.245.185.245:3130` 不可达，**本次旁路** |
| Elasticsearch | `172.17.0.1:3128` HTTP 502，**本次旁路** |
| 线程 | 每题独立 thread，第二轮真实上传视频附件 |

判定口径：runner 自带的 `actual_skills` 会把场景过滤器注入的 `<available_skills>` 候选集计入
（一次调用就"命中"23 个 skill），本报告一律改用
[`scripts/analyze_video_agent_sse.py`](../scripts/analyze_video_agent_sse.py)，只从原始 SSE 的真实
`tool_calls` 提取结论。

复现命令：

```bash
cd /home/huangxiao/City_brain/imiss-deer-flow-qwen36-35b-a3b-test

# 第一轮：不挂附件，测纯意图路由
python3 scripts/run_video_surveillance_qwen35b_test.py \
  --timeout 420 \
  --cases-file scripts/video_routing_questions_cases.json \
  --output-root outputs/skill-tests/video-routing-questions

# 第二轮：挂真实视频，测路由与执行
python3 scripts/run_video_surveillance_qwen35b_test.py \
  --timeout 420 \
  --cases-file scripts/video_routing_questions_cases_round2.json \
  --dataset-root /home/huangxiao/City_brain/imiss-deer-flow-qwen36-35b-a3b-test/datasets \
  --output-root outputs/skill-tests/video-routing-questions-r2

# 判定
python3 scripts/analyze_video_agent_sse.py outputs/skill-tests/video-routing-questions-r2/<批次>
```

产物：`outputs/skill-tests/video-routing-questions/20260726-184743/`（第一轮）与
`outputs/skill-tests/video-routing-questions-r2/20260726-194754/`（第二轮），
每题含 `turn-1.sse` 原始流、`agent_final.md`、`validation.json`。

## 2. 问题清单的预期路由已有 9 条过期

测试前清单为 20 题。其中 6 题的预期 skill 已从可执行 bundle 移除，**本次已从清单中删除**：

| 原题号 | 预期 skill | bundle 现状 |
| --- | --- | --- |
| 3 | `density-aggregation-event` | 已移除 |
| 5 | `event-rule-engine` | 已移除 |
| 6 | `event-template-mapping` | 已移除 |
| 11 | `object-composition-event` | 已移除 |
| 17 | `spatial-occupancy-event` | 已移除 |
| 18 | `temporal-persistence-event` | 已移除 |

这 6 个正是 `deerflow-agent-skill-test.md` 1.2 节所说"按密度、ROI、目标组合和时间规则直接判定
业务事件"的旧 skill，新架构下事件语义统一由 `city-video-intelligence` -> `single-video-event-analysis`
承担。删除后清单为 14 题，重排编号 Q01–Q14。

**测试中又发现 3 条预期与新架构冲突**（这 3 条题目本身有效，只是清单标注的预期 skill 过期）：

| 题 | 清单预期 | 模型实际行为 | 谁对 |
| --- | --- | --- | --- |
| Q01 | `analyze-video` | `city-video-intelligence` -> `single-video-event-analysis` | 模型对。题干含"异常事件、时间线、复核报告"，属事件语义，按 1.1 节应走统一入口 |
| Q09 | `object-detection` | `video-object-analytics`(detect) | 模型对。1.1 节要求对象分析走结构化工具，明确禁止 Agent 自行拼接底层检测命令 |
| Q10 | `object-tracking` | `video-object-analytics`(track) | 模型对。同上 |

建议后续把 Q01/Q09/Q10 的预期改为新架构口径，否则会把正确行为判成失败。

## 3. 第一轮：不挂附件的意图路由（14 题）

| 题 | 清单预期 | 真实工具调用 | 结果 |
| --- | --- | --- | --- |
| Q01 | `analyze-video` | `ask_clarification` | 追问视频路径，路由未验证 |
| Q02 | `camera-health-check` | `invoke_skill`×4、`read_file`×4、`bash`×9 | 触达 `camera-health-check`，但递归上限，回答 21 字符 |
| Q03 | `duplicate-event-merge` | `invoke_skill`×2、`read_file`、`bash`×3 | **通过**，正确列出所需告警字段 |
| Q04 | `evidence-snapshot` | `ls`×2、`ask_clarification` | 追问，并主动扫 `/mnt/datasets` 列出 3 个候选视频 |
| Q05 | `ffmpeg-utils` | `ask_clarification` | 追问，路由未验证 |
| Q06 | `frame-sampling` | `bash`×2、`ask_clarification` | 追问，路由未验证 |
| Q07 | `human-review-routing` | `ask_clarification` | 追问，路由未验证 |
| Q08 | `video-object-analytics` | 无工具调用 | 直接作答，未进 skill |
| Q09 | `object-detection` | `ask_clarification` | 追问，路由未验证 |
| Q10 | `object-tracking` | `ask_clarification` | 追问，路由未验证 |
| Q11 | `privacy-masking` | `ls` | 未追问也未进 skill |
| Q12 | `roi-mapping` | `ask_clarification` | 追问，路由未验证 |
| Q13 | `video-segment-extraction` | `ask_clarification` | 追问，路由未验证 |
| Q14 | `video-stream-ingestion` | `ask_clarification` | 追问 RTSP 地址，路由未验证 |

这一轮只验证到两件事：

1. **不伪造行为达标**。14 题里 10 题停在追问或列目录，没有任何一题在缺视频的情况下编造检测结果、
   事件时间线或 artifact 路径，符合 5.4 节的失败判定要求。
2. **纯意图轮无法验证路由**。11/14 因缺附件停在追问，拿不到 skill 落点。清单"测试建议"里
   "先用没有上传文件的题目测试意图路由"这条在当前 Agent 行为下收益很低，实际的路由测试必须挂附件。

## 4. 第二轮：挂真实视频（14 题）

| 题 | 清单预期 | 真实触达 skill | 调用数 | 递归上限 | 判定 |
| --- | --- | --- | --- | --- | --- |
| Q01 | `analyze-video` | `city-video-intelligence`、`single-video-event-analysis` | 9 | 否 | 新架构正确；但 `BadRequestError` 中断，回答 38 字符 |
| Q02 | `camera-health-check` | `camera-health-check`、`frame-sampling` | 13 | **是** | 路由通过、执行被截断（108 字符） |
| Q03 | `duplicate-event-merge` | `duplicate-event-merge` | 3 | 否 | **通过** |
| Q04 | `evidence-snapshot` | `city-video-intelligence`、`video-object-analytics` | 16 | **是** | **失败**，未触达目标 skill，且手写 bash 视频命令 |
| Q05 | `ffmpeg-utils` | 无 | 12 | **是** | **失败**，`bash`×9 全程手写 ffmpeg |
| Q06 | `frame-sampling` | `frame-sampling` | 4 | 否 | **通过**，1569 字符完整回答 |
| Q07 | `human-review-routing` | 无 | 7 | 否 | 未触达，`bash`×4 后追问事件数据 |
| Q08 | `video-object-analytics` | `video-object-analytics`(detect+track) | 7 | 否 | **通过**，752 字符 |
| Q09 | `object-detection` | `video-object-analytics`(detect) | 3 | 否 | 新架构正确，2522 字符完整回答 |
| Q10 | `object-tracking` | `video-object-analytics`(track) | 4 | 否 | 新架构正确，4246 字符完整回答 |
| Q11 | `privacy-masking` | 无 | 12 | **是** | **失败**，`bash`×10 手写，回答 40 字符 |
| Q12 | `roi-mapping` | `roi-mapping`、`video-object-analytics` | 13 | **是** | 路由通过、执行被截断（77 字符） |
| Q13 | `video-segment-extraction` | `city-video-intelligence`、`single-video-event-analysis` | 16 | **是** | **失败**，未落到片段提取 |
| Q14 | `video-stream-ingestion` | 无 | 1 | 否 | 未触达，追问 RTSP 地址（题干确实没给地址） |

汇总：

| 分类 | 题 | 数量 |
| --- | --- | --- |
| 命中清单预期且正常收尾 | Q03、Q06、Q08 | 3 |
| 命中清单预期但被递归上限截断 | Q02、Q12 | 2 |
| 新架构正确（清单预期过期） | Q01、Q09、Q10 | 3 |
| 合理追问但未进 skill | Q07、Q14 | 2 |
| 路由失败 | Q04、Q05、Q11、Q13 | 4 |

## 5. 主要发现

### 5.1 递归上限与工具调用次数强相关，阈值在 9–12 之间

第二轮 14 题按调用次数排列，分界线异常干净：

| 调用次数 | 题 | 是否撞递归上限 |
| --- | --- | --- |
| 1、3、3、4、4、7、7、9 | Q14、Q03、Q09、Q06、Q10、Q07、Q08、Q01 | 全部**否** |
| 12、12、13、13、16、16 | Q05、Q11、Q02、Q12、Q04、Q13 | 全部**是** |

没有例外：**≤9 次调用无一触发，≥12 次调用无一幸免**。`recursion_limit` 默认 100，
意味着每次工具调用平均消耗约 8 个 graph step，12 次调用即耗尽预算。
这不是"任务太重"，而是**递归预算相对于单次工具调用的 step 开销设得过低**。

触发后的共同表现是最终回答被截断到 16–108 字符（Q13 例外，1017 字符），任务无法收尾。

### 5.2 更正一个此前的判断：递归上限与路由失败无因果关系

测试中途曾观察到 Q05（`bash`×9）、Q11（`bash`×10）都是"放弃 skill 改用 bash 硬做"后撞上递归上限，
一度判断为"路由失败导致模型 bash 死磕，进而耗尽递归预算"。**完整数据不支持这个因果链**：

- Q12 正确触达 `roi-mapping`、Q02 正确触达 `camera-health-check`、Q13 正确走完
  `city-video-intelligence` -> `single-video-event-analysis`，三题路由都对，一样撞上递归上限；
- Q07 路由失败（未触达 `human-review-routing`）且用了 `bash`×4，反而没有触发。

二者只是都与"调用次数多"相关，彼此没有因果。递归上限是独立问题，需要单独在
lead_agent 侧调整 `recursion_limit` 或增加终止条件。

### 5.3 手写 bash 绕过 skill 仍然存在，且集中在媒体处理类

Q05（`ffmpeg-utils`）、Q11（`privacy-masking`）两题模型完全没有尝试 skill，直接用
`bash` 拼 ffmpeg/OpenCV 命令，各用掉 9 次和 10 次 bash。Q04 也被检出手写视频命令。
这与之前 C-002 反复绕过 `video-stream-ingestion` 是同一类问题：
**有对应 skill 时模型仍可能手写等价命令**，且这类题的 skill 声明并不缺失。

### 5.4 Q13 的路由值得单独讨论

Q13 题干是"从 8 小时监控录像中提取包含异常行为的多个短视频片段"，清单预期
`video-segment-extraction`。模型走的是 `city-video-intelligence` -> `single-video-event-analysis`，
并输出了带时间线的审阅报告，但始终没有落到片段提取。

严格说这不算完全跑偏——题干确实先要"找异常行为"再"提取片段"，事件语义部分走统一入口是对的，
问题是**链路缺了后半段**。按 1.1 节的推荐链路，`single-video-event-analysis` 之后应接
`video-segment-extraction`。这条链路在编排计划里存在（Layer A 已验证），但 Agent 实际执行时没有走完，
且该题同时撞上递归上限，无法区分是路由问题还是被截断所致。

## 6. 结论

- 14 题中，清单预期命中 5 题（其中 2 题被递归上限截断），新架构正确但清单过期 3 题，
  合理追问 2 题，明确路由失败 4 题。
- 问题清单累计发现 9 条过期预期：已删除 6 条，另有 Q01/Q09/Q10 三条需要改口径。
- 第二轮 6/14 触发 `GraphRecursionError`，且与调用次数呈现无例外的阈值关系（≤9 不触发，≥12 必触发），
  是本次最突出的问题，影响面超过路由本身。
- 媒体处理类 skill（`ffmpeg-utils`、`privacy-masking`）被 bash 绕过的情况稳定复现。
- 以上问题都落在 lead_agent 的执行策略层（递归预算、终止条件、"有 skill 不得手写等价命令"的约束），
  不在视频 skill bundle 内，本次未做修改。
- StreetModel 与 Elasticsearch 恢复后，需补测 `video-search`、`video-embedding-index`、
  `object-statistics` 的在线检索路径。
