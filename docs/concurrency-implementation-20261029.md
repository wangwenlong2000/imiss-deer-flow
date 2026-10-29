# 轻量多会话并发实现记录

执行日期：2026-10-29（北京时间）。按用户确认的轻量演示范围实现，不新增账号系统、数据库服务或分布式任务系统。已有街景、遥感和视频等工作区改动保留，未作为本轮并发修改重写。

## 实现效果

- 启动入口统一显式设置图任务执行名额，`DEER_FLOW_GRAPH_JOBS`默认2；其余任务由LangGraph原有队列调度。
- 新建图任务入口限制“运行中＋等待中”总数，`DEER_FLOW_MAX_INFLIGHT_RUNS`默认10。当同时运行2个时，最多另有8个未完成任务等待；超过总量的提交返回HTTP 429和`Retry-After: 3`，不会新建对应图任务。
- 网页通过本次提交的准确`thread_id`与`run_id`查询任务状态，显示“排队中”或“正在处理”。尚无任务元数据时显示中性的等待提示，不以流连接已打开推断开始执行。
- 状态查询失败不会中断原有SSE；结束后停止状态轮询。提交/执行错误通过提示展示，队列满时可稍后重试。
- 使用原有会话文件目录与上传、下载接口，不增加用户身份判断。这是会话间文件分开保存，不是账号权限隔离。

## 改动位置与兼容方式

| 位置 | 改动 |
| --- | --- |
| [图服务HTTP入口](../backend/app/graph_http.py)、[图配置](../backend/langgraph.json) | 通过自定义Starlette中间件查询当前运行时的队列计数，控制新任务受理 |
| [网页会话hook](../frontend/src/core/threads/hooks.ts)、[消息列表](../frontend/src/components/workspace/messages/message-list.tsx)及中英文词条 | 每秒查询本次run状态，显示排队/处理状态及错误提示 |
| Docker Compose和本地启动脚本 | 补齐遗漏的显式并发参数；Compose可传入未完成任务总量限制 |
| [端口调整脚本](../scripts/update-docker-ports-cname.sh) | 调整端口时保留并发参数，兼容原来与新增命令格式 |
| [回归测试](../backend/tests/test_graph_admission.py)、[隔离运行时验证脚本](../scripts/verify_concurrency_runtime.py) | 验证竞争提交、异常释放、流连接、排队、取消与文件分离 |

受理限制覆盖有会话/无会话的新建、流式、等待结果及批量创建接口；批量按实际任务数量计数。读取状态、GET流重连、取消、删除和会话查询不占新增受理名额。请求内容原样交给原生接口验证，未改动意图识别、规划、合规检查、技能、工具或最终答案生成逻辑。

创建阶段的短锁在原生API存储任务并发出响应头后释放；不会等到SSE结束才允许另一个任务。客户端断线但后台继续执行的任务仍由原生队列统计，不能因HTTP断线而提前释放容量。队列状态读取失败时返回503，避免失去计数后无限放行。

这采用官方支持的自定义HTTP中间件扩展方式，并在当前安装版本验证；实际队列计数通过当前运行时的`Runs.stats`取得，升级LangGraph时需要复跑兼容验证。[官方扩展说明](https://docs.langchain.com/langsmith/custom-middleware)

## 验证结果

**后端256项回归通过**，覆盖新增受理限制以及已有上传、下载、手机端、完整流程、回合契约、交付质量、场景修复、checkpoint、沙箱及部署检查。已有Pydantic序列化警告9条，没有测试失败。

执行集合：`test_graph_admission`、`test_uploads_router`、`test_workspace_files_router`、`test_mobile_compliance_forwarding`、`test_harness_boundary`、`test_full_flow_regressions`、`test_turn_contract_regressions`、`test_remaining_quality_regressions`、`test_scene_service_fixes`、`test_checkpointer`、`test_checkpointer_none_fix`、`test_sandbox_tools_security`、`test_aio_sandbox_local_backend`、`test_docker_sandbox_mode_detection`。

**前端TypeScript检查通过，改动文件ESLint通过，共30项测试通过，包含6项重复正文渲染回归。** 本次没有进行浏览器自动化视觉验收。

**真实LangGraph运行时隔离验证通过**。运行记录保留本地，不随公开版本发布。使用临时配置、临时端口、临时checkpoint与文件目录，实际启动原生图服务；测试图执行固定CSV求和，不调用模型或GPU，也没有修改生产业务图。

| 验证项 | 实际结果 |
| --- | --- |
| 两个任务并行 | 两个节点各持续约4秒，起始时间相差不足1毫秒，执行区间重叠 |
| 第三个等待 | 前两个运行时查到第三个为pending；第三个在前两个完成后开始 |
| 超量拒绝 | 隔离验证将总量设为3，第4个提交返回429 |
| 同名文件分离 | 三个会话都上传`same.csv`、生成`report.csv`，实际下载总和分别11、22、33，标记分别匹配各自会话 |
| 流式及续接 | 队列满时仍能GET订阅排队任务的流；原生wait、POST流式接口、同会话续接均通过 |
| 取消与恢复容量 | 任务取消达到interrupted后，新任务可继续处理 |

## 业务验证与合成样例

通过原业务图、模型、工具、沙箱及下载接口完成三个独立CSV会话验证：A、B重叠执行，C先排队再执行；三个任务完成后，最终表格和下载CSV均符合各自输入。三个输入都可使用同名文件`same.csv`，输出均为`report.csv`，文件内容保持各自独立。同会话解释追问通过，原CSV保持不变。

| 样例 | 道路总流量（各） | 流量均值（各） | 两道路平均速度 | 合成输入 | 下载结果样例 |
| --- | ---: | ---: | --- | --- | --- |
| A | 400 | 200 | 20/30 | [A.input.csv](concurrency-implementation-eval-20261029/real-business/A.input.csv) | [A.report.csv](concurrency-implementation-eval-20261029/real-business/A.report.csv) |
| B | 800 | 400 | 21/31 | [B.input.csv](concurrency-implementation-eval-20261029/real-business/B.input.csv) | [B.report.csv](concurrency-implementation-eval-20261029/real-business/B.report.csv) |
| C | 1200 | 600 | 22/32 | [C.input.csv](concurrency-implementation-eval-20261029/real-business/C.input.csv) | [C.report.csv](concurrency-implementation-eval-20261029/real-business/C.report.csv) |

公开版本仅包含合成CSV与验证结论；实际会话/运行标识、状态快照、部署及模型调用元数据、原始流和内部记录保留本地。上述结果仅覆盖小型道路CSV工作负载，不能外推为视频、遥感或百人并发容量。测试由同一机器发出并发HTTP请求完成，未进行两台物理电脑人工操作或完整浏览器视觉验收。

## 两台电脑的演示步骤

1. 两台电脑访问同一个已部署网址，各自点击新建会话，确认地址中的会话编号不同。
2. 分别上传内容不同的CSV，可以使用相同文件名；同时提交各自的分析问题。
3. 查看每个会话的处理状态、答案和下载文件，确认对应各自数据。
4. 在两个任务尚未结束时，用第三个独立会话提交问题，观察“排队中”随后变为“正在处理”。任务过短可能来不及观察排队，应使用正常但较耗时的样例；不要仅根据开了三个浏览器判断并发。
5. 记录样例、完成状态及用时。图任务并行不保证模型服务同时受理全部内部请求；实际速度取决于模型配额、工具与沙箱资源。

## 配置及后续维护

- 调整并发：启动前设置`DEER_FLOW_GRAPH_JOBS`，默认2。调整受理总量：设置`DEER_FLOW_MAX_INFLIGHT_RUNS`，默认10，要求正整数。
- Docker的执行数在Compose命令中展开，总量作为图容器环境变量传入；更改后需更新对应图服务。仅修改变量文件而不更新已有容器，不会自动生效。
- 不依赖模型的复验命令：在后端Python环境运行`python ../scripts/verify_concurrency_runtime.py`。脚本启动的临时服务和临时数据会清理，不重启真实服务。
- 需要真实模型和沙箱的复验可运行`python ../scripts/verify_concurrency_business.py --base-url http://localhost:2026 --output /tmp/concurrency-business-new-run`。该脚本会调用模型并创建三个测试会话，核对最终表格、CSV和运行状态；应使用新的私有输出目录。原始SSE/状态不应作为公开交付物。
- 当前只支持单个图服务进程的受理总量，不是多副本集群配额。定时任务入口、图内部子智能体/工具扇出不在本次HTTP受理限额内。
- 本轮未设置独立视频/GPU调度、未迁移手机会话字典或数据中心JSON登记表；避免把这个小规模并发演示版本当作完整多用户生产系统。
- 本轮没有更改已有全局长期记忆开关；部署时应按会话隔离需求检查配置。没有增加账号级私有历史列表。
