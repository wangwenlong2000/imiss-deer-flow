# OneCityAgent 同门交接与联调手册

## 1. 项目目标与边界

本仓库交接的是 OneCityAgent（城市超脑）前端/后端及其对 DeerFlow 的调用适配。用户从 OneCityAgent 城市超脑页面提交文字或附件，OneCityAgent 将身份、会话和附件信息交给 DeerFlow；DeerFlow 执行意图识别、合规输入闸、场景分析/工具调用、模型生成、合规输出闸，再把阶段事件和最终结果以流式消息返回；OneCityAgent 将各段消息、合规卡片和最终产物展示给用户。

OneCityAgent 在此链路中不只是 iframe 或单纯转发器：它负责自己的登录/会话，接收身份和上传文件，维护 DeerFlow thread 映射，适配 SSE 事件并呈现前端。合规判定和业务分析的权威逻辑仍由 DeerFlow 侧提供，前端的合规卡片是结构化审计结果的展示。

## 2. 代码结构

| 路径 | 用途 |
|---|---|
| `web/src/pages/city-brain/` | 城市超脑聊天页面、身份/使用场景选择、附件展示、SSE 消息卡片和文件预览 |
| `web/src/utils/`、`web/src/services/` | 前端请求、会话与流式状态管理 |
| `onecityagent/service/a2a_service/coordinator_sse_acting.py` | DeerFlow 适配核心：ensure thread、上传、调用流式分析、转发事件、预览文件和改写产物链接 |
| `onecityagent/controller/` | Flask API 路由和会话/配置等控制器 |
| `app.py` | Python Web API 入口 |
| `python_a2a/` | 项目内使用的 A2A 客户端/协议实现 |
| `config/*.example.*` | 脱敏配置模板；真实配置必须保存在本地忽略文件中 |
| `start.sh` | 启动本地协作智能体与应用进程 |
| `nginx.conf.template` | 可选反向代理模板；由部署方注入内部监听值后再使用 |

## 3. 消息与文件链路

```text
浏览器城市超脑页面
  ├─ /api、/v1 ───────────────> OneCityAgent Flask API
  └─ /query/deerflow ─────────> A2A SSE Coordinator
                                  ├─ 创建/确认 DeerFlow thread
                                  ├─ 上传附件到 DeerFlow thread
                                  ├─ POST DeerFlow mobile run API
                                  └─ 将 SSE 事件适配并逐段返回浏览器
                                         ├─ 系统阶段提示
                                         ├─ 合规检测结构化事件
                                         ├─ 意图/规划/执行/反馈消息
                                         └─ 最终答复、文件与图表链接
```

### 正常联调顺序

1. 前端发起请求时携带用户输入、OneCityAgent 会话标识、当前合规身份/场景，以及可选附件。
2. Coordinator 根据会话维护或获取 DeerFlow thread ID；有附件时先将文件上传到该 thread。
3. Coordinator 调用 DeerFlow mobile run 流式接口。具体 API URL 仅通过本地环境变量提供，不在代码内写死。
4. DeerFlow 发出的 SSE 事件由 Coordinator 适配成 OneCityAgent 前端当前支持的事件/消息结构。
5. DeerFlow 返回的文件相对链接由后端按 `DEERFLOW_PUBLIC_BASE_URL` 改写。这个地址必须能被用户浏览器访问；若 DeerFlow 的服务端内部地址与浏览器地址不同，应分别配置，不要在前端拼接固定主机和端口。
6. 结束事件到达后，确认最终答复和附件/报告卡片正常展示，并确认合规卡片包含检测闸门、身份场景、风险类型、处置动作、审计编号和依据（若 DeerFlow 返回这些字段）。

### 身份传递

当前前端和 Coordinator 的白名单键值是：`ordinary_user`（个人自用，`self_use`）、`internal_operator`（组织内部，`internal_org`）、`cross_org_operator`（跨组织，`cross_org`）、`publisher`（公开发布，`public_release`）、`researcher`（科研匿名化，`research_anon`）。Coordinator 根据键值自行构造 `user_context` 和 `compliance_context`；浏览器不能自行指定角色或组织字段。若 DeerFlow 端枚举调整，前后端映射和测试必须一起更新。

联调至少检查一个个人自用样本和一个公开发布样本的请求体及返回审计卡片，确认选择值、DeerFlow 实际身份、闸门和动作一致。不能只改中文标签，也不能让前端默认身份覆盖用户选择。

### 合规事件字段约定

当前前端单独识别 `type: "compliance"` 事件，并从 `compliance_result`（或兼容的 `data`）读取如下字段：

```json
{
  "type": "compliance",
  "compliance_result": {
    "gate": "InputGate|ContextGate|OutputGate",
    "scene": "self_use|public_release|...",
    "violation_types": ["..."],
    "actions": ["..."],
    "audit_ref": "...",
    "basis": ["..."],
    "final_disposition": "allow|warn|refuse|..."
  }
}
```

一般消息由 OneCityAgent Coordinator 规范为 `{type: "agent"|"系统", role, content, conversation_id, status?, metadata?}`；流结束事件为 `{type: "[DONE]", conversation_id, finished}`。前端监听 SSE 的 `data:` 行，按 `type` 分流处理。合规结果可以单独作为 `type: "compliance"` 事件返回，也可以在 `[DONE]` 的 `compliance_results` 数组中补发。字段命名以上述当前前端解析代码为准；变更要同时更新适配器和组件，并用真实事件 JSON 做回归。不要从最终自然语言答复里猜测合规状态，也不要伪造审计数据。

### 当前 OneCityAgent 侧接口

- `GET /query/deerflow`：SSE 问答入口。主要 query 参数为 `query`、`conversation_id`、`user_id`、`compliance_identity`，可带真实 `thread_id` 和附件元数据。身份值必须在白名单中。
- `POST /query/deerflow/upload`：multipart 文件上传，文件字段名为 `files`，需传会话/线程信息；Coordinator 先确保 DeerFlow thread，再把文件转交给 DeerFlow。
- `GET /query/deerflow/file/preview?url=...&filename=...`：请求 Markdown/文本等产物的预览；其他格式会返回下载提示/链接。
- DeerFlow thread ensure：`/api/deerflow/mobile/threads/ensure`。
- DeerFlow thread 文件上传：`/api/threads/{thread_id}/uploads`。
- DeerFlow 流式运行路径：由 `DEERFLOW_API_URL` 提供完整地址；不要假定公开域名或固定端口。

## 4. 本地配置

### 环境文件

复制模板后在本地编辑，绝不提交真实配置：

```bash
cp .env.example .env
cp config/config.example.toml config/config.toml
```

`.env` 中需要由 DeerFlow 部署负责人提供：

- `DEERFLOW_API_URL`：后端可访问的 mobile run SSE API 完整地址。
- `DEERFLOW_PUBLIC_BASE_URL`：浏览器可访问的 DeerFlow 文件服务基地址，用于报告、图表和附件下载；若后端返回的文件链接已是浏览器可访问绝对地址，可按实际实现配置。
- `SECRET_KEY`：本项目独立随机值，不能复用模型 API key。
- `DEERFLOW_*_TIMEOUT`：按任务最长执行时间调整。

本地 `config/config.toml` 负责模型、RAG、数据库、A2A 智能体等配置。API key、DB 凭据和私钥只写到被 `.gitignore` 排除的文件，不要复制到 README、Issue、截图或聊天记录。A2A/MCP 服务地址应由本地部署拓扑决定。

### 端口与地址原则

本交接副本不包含原服务器的公网 IP、对外访问端口、域名或 DeerFlow 实例地址。`.env.example` 中的内部监听默认值只用于本机进程间通信；部署时可以通过环境变量调整。反向代理模板需由部署环境渲染，不应把公网 `host:port` 反写进源码、前端构建产物或 Git 历史。

如果前后端分开部署，设置 `ONECITY_BACKEND_URL` 和 `ONECITY_COORDINATOR_URL` 后再启动前端开发服务器；二者应指向同一套 OneCityAgent 环境。浏览器生产访问一般使用同源反向代理，不应把 DeerFlow 私网地址暴露给前端。

## 5. 启动与验证

以下是通用开发流程；本项目元数据要求 Python 3.13 或更新版本，前端使用 Node/npm。首次启动前先完成上一节配置，确认 DeerFlow 地址和数据库可达。

### 后端

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
bash start.sh
```

`start.sh` 会启动配置中的协作/A2A 服务与 Flask API。日志和 PID 只写入本地 `logs/`、`pids/`，这些目录不会进入仓库。确认端口无冲突、数据库已初始化、A2A URL 与本地监听配置一致。

### 前端

```bash
set -a
source .env
set +a
cd web
npm ci
npm run dev
```

如果 Coordinator 与 Flask API 分开部署，再设置 `ONECITY_COORDINATOR_URL`。开发代理配置在 `web/.umirc.ts`，默认不包含团队现网地址。生产构建可按项目既有发布环境执行 `npm run build`，再由反向代理托管构建产物。

### 最小验收清单

1. 打开城市超脑页面，创建新会话，确认历史会话仅属于当前开发数据库/账号，不是另一套实例的数据。
2. 选择个人自用身份提交一个合成、低风险问题；确认请求确实抵达指定 DeerFlow，最终答复完整。
3. 选择公开发布身份提交同一条带合成个人信息的测试内容；确认输入闸/输出闸行为符合 DeerFlow 测试期望，OneCityAgent 展示真实返回的处置结果，而不是 mock 或旧会话数据。
4. 上传一个无敏感信息的小型测试文件；确认上传绑定正确 DeerFlow thread，DeerFlow 返回处理结果后卡片可预览/下载。
5. 使用会生成 Markdown/图表的安全合成任务；确认 SSE 最终事件结束、产物链接在浏览器可访问。
6. 对比前端选择身份、后端请求中的身份字段、DeerFlow 审计事件中的身份值；三者完全一致。
7. 查看 OneCityAgent 与 DeerFlow 日志中的错误和请求关联 ID；日志不得打印 API key、完整个人数据或认证头。

## 6. 常见故障定位

| 表现 | 优先检查 |
|---|---|
| 页面打开但问答仍是模拟内容 | `DEERFLOW_API_URL` 是否缺失/错误；是否误启 mock fallback；看 Coordinator 日志确认请求目标和响应码 |
| 请求超时或 SSE 不结束 | DeerFlow run 地址和访问权限、代理 buffering、长连接 read timeout、结束事件是否到达；确认反向代理关闭响应缓冲 |
| 上传后 DeerFlow 找不到文件 | thread ensure 是否成功；上传是否使用同一真实 thread ID；上传的字段名/接口与 DeerFlow 契约是否匹配 |
| 回答完成但报告链接打不开 | `DEERFLOW_PUBLIC_BASE_URL` 是否为浏览器可访问地址；查看后端改写后的 artifact URL 和浏览器网络请求 |
| 合规卡片不显示/身份错误 | 核实 SSE 合规事件字段、身份枚举映射、事件被适配器过滤/改名的路径；不要只看最终回答文本 |
| 历史记录像另一套环境 | OneCityAgent API / 数据库连接是否指向预期实例；清空测试会话前先确认数据归属，勿直接删库 |
| 前端开发代理失败 | `ONECITY_BACKEND_URL`、`ONECITY_COORDINATOR_URL` 与当前开发服务监听地址是否一致 |

## 7. 与 DeerFlow 同事的接口协作清单

提交联调前互相确认：

- mobile run、thread ensure、thread upload、文件预览/下载接口的完整路径、认证方式和网络可达性；具体地址通过私下渠道交付，不写入此仓库。
- 请求字段：query、用户/会话 ID、thread ID、身份/合规上下文、附件引用；确认敏感字段最小化。
- SSE 事件清单、事件顺序、结束标记、错误事件、合规结果的结构化 JSON Schema、文件 artifact 元数据。
- 同一请求在 OneCityAgent 和 DeerFlow 日志中可用 request/thread/audit ID 关联，但日志不要记录密钥或不必要的个人信息。
- 双方各自提供一条 self-use 放行、public-release 拦截、附件上传、可下载报告的合成验收用例。

## 8. 交接协作方式

1. 同门 fork 私有仓库或由仓库管理员邀请为最小权限协作者；不要通过压缩包传 `.env`、数据库导出或私钥。
2. 每人从独立分支开发，通过 Pull Request 合并；PR 描述记录改动文件、接口影响、测试结果和回滚方式。
3. 前端、OneCityAgent 代理层、DeerFlow 端接口/合规引擎分开归属，接口变更先更新 Schema/样例，再更新适配器。
4. 合并前至少跑 Python 编译检查、前端 lint/build、上述最小验收，并扫描秘密和硬编码网络地址。
5. 现有工作区没有自动生成的、可保证与每个成员机器完全一致的依赖锁定环境；安装失败时先对齐 Python/Node 版本和 lockfile，不要提交虚拟环境。

## 9. 当前交接包的限制

- 代码来自本地 OneCityAgent 工作区的现有实现；该目录本身没有 Git 历史，因此仓库初始提交只记录整理后的快照，不能代表完整开发历史。
- 没有复制运行中的 `.env`、真实配置、私钥、数据库数据、日志、上传数据、旧版备份或编译目录。
- 源码中的 DeerFlow 固定主机/外部端口兜底已移除；运行时必须通过本地部署配置提供连接地址。
- 仍需项目负责人核实源项目授权/许可证、依赖来源、数据库初始化脚本及生产部署安全策略后，才能用于团队外部分发或正式部署。
