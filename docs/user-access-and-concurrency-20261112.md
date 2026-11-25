# 用户权限与并发改进记录

## 本轮实现

IAM 启用后，Gateway 所有 `/api/*` 请求统一认证。线程相关入口检查服务端写入的 `org_id + user_id + conversation_id`，并向 IAM 复核当前操作；错误凭证返回 401，未授权返回 403，其他用户的资源返回 404，依赖不可用返回 503。认证结果仅在当前 ASGI 请求内复用，不跨请求缓存，撤权后的下一次访问会重新校验。

原生 LangGraph 接口增加统一访问中间件：允许用户读取自己的会话、历史、checkpoint、运行状态及流重连，取消自己的任务和删除自己的会话/运行。搜索强制附加组织与用户过滤，并逐条复核 IAM，移除已撤权对话。分页仍按原生 offset/limit，撤权过滤后页面可能不满；不会返回全局计数。创建会话/任务、修改状态及其他未开放操作需经过已认证 Gateway；普通 Token 不能直接调用原生运行接口。

Gateway 到图服务使用由共享签名密钥派生的内部凭证。两份 Nginx 配置均剥除外部提供的内部凭证头，图中间件消费后也立即移除，LangGraph 配置另行排除该头持久化。IAM 部署会自动要求任务签名，内部凭证不能代替用户调度声明。

共享 Agent/用户画像、MCP、IM 通道和技能路由管理接口需显式 `platform:admin` 权限；技能配置写操作也需此权限。该权限来自服务端 IAM，不依据客户端角色名自动授予。

## 数据隔离与持久化

- 移动端会话映射迁到 `${DEER_FLOW_HOME}/gateway.sqlite3` 的 `mobile_conversations` 表，组织、用户、业务会话组成唯一键。短事务预留确定性 thread UUID，网络调用在事务外执行，竞争创建自动恢复；Gateway 重启后读取同一映射。
- 数据源登记使用同一 SQLite 文件的独立表，逐条事务写入、注册去重，避免 JSON 读改写造成丢记录。原 `data-center/sources.json` 仅导入一次，旧记录不自动取得用户或组织归属。
- 数据源上传由服务端填写归属，同名文件使用独立存储目录；登记失败清理本次文件，删除事务失败恢复暂存文件。挂载需要源资源和目标会话双重权限；同名目标采用递增文件名，保留已有内容。
- 上传和挂载对同一会话使用跨进程异步文件锁，将保存、合规扫描及失败回滚串行化；不同会话仍可并发。文件和目录符号链接不得将上传、转换、回滚或附件扫描指向其他会话。
- 记忆存储、缓存、后台更新及模型调用时注入按组织、用户和 Agent 隔离。运行上下文使用绑定 thread 的签名记忆声明；IAM 模式缺少有效声明时不注入或写入全局记忆。认证记忆固定在 `users/<组织与用户摘要>/`，不会继承旧全局记忆，原 `memory.storage_path` 仅用于无 IAM 的开发模式。
- `deerflow.security.access` 输出结构化访问审计，记录服务端生成的请求编号、用户、组织、方法、路径、状态与决策原因，不记录凭证、查询参数、正文或模型输出。

## 接入与兼容

沿用 [OneCity IAM 接入约定](compliance/onecity_deerflow_compliance_contract.md)，Gateway 和 LangGraph 必须配置相同 IAM 提供方和至少 32 字节的 `DEER_FLOW_ADMISSION_SIGNING_KEY`，并连接正确的内网图服务地址。生产使用 `MOBILE_IAM_REQUIRED=true`。

```dotenv
MOBILE_IAM_PROVIDER=app.gateway.onecity_iam:OneCityIAMProvider
MOBILE_IAM_ENDPOINT=http://<内网Coordinator地址>/internal/deerflow/iam
MOBILE_IAM_REQUIRED=true
DEER_FLOW_ADMISSION_SIGNING_KEY=<随机生成且两个服务一致的密钥>
MOBILE_ADAPTER_LANGGRAPH_URL=http://<内网图服务地址>:3329
```

新建任务通过 `/api/deerflow/mobile/run` 或 `/api/mobile/chat`，先由 IAM 验证操作/资源，再由 Gateway 签名受理。独立 DeerFlow 网页目前仍直接创建原生任务，尚未接通这一认证提交链路：无 IAM 的开发模式保持兼容，启用 IAM 后需使用已集成的 OneCity 入口。IM 通道也需先映射到可信 IAM 身份，不能凭内部通行凭证绕过用户声明。不要为了兼容未改造客户端关闭生产认证。

数据中心操作需由 IAM 在 `principal.permissions` 返回下列明确权限；通配权限不扩大资源所有权范围。

| 权限 | 操作 |
| --- | --- |
| `data_center:read` | 列表和详情 |
| `data_center:upload` | 上传和注册当前用户会话文件 |
| `data_center:export` | 下载 |
| `data_center:delete` | 删除自己拥有的数据源 |
| `data_center:attach` | 挂载数据源；另行校验目标会话 upload、源会话 export |
| `data_center:es_read` | 读取授权 ES 索引 |
| `data_center:*` | 以上操作，仍校验所有权和共享配置 |

组织共享仅读取部署方配置 `DATA_CENTER_ORG_SHARED_SOURCES='{"org-a":["source-id"]}'`，且资源必须已归属同一组织。共享不授予非所有者删除权；会话文件仍需会话 IAM 授权。ES 另需 `DATA_CENTER_ORG_ES_INDICES='{"org-a":["allowed-index"]}'`，未配置时 IAM 用户不能读取 ES；请求正文不能修改这两项授权。

原有缺少组织归属的会话/数据源不会自动归当前登录用户所有。启用前需由管理员核实归属后迁移；旧全局记忆也需独立核实，不能批量分给第一个访问者。备份至少包含 SQLite、会话文件和用户记忆；不要让旧版进程继续写 `sources.json`。SQLite 和文件锁适用于同机共享数据卷，不能直接用作跨主机配额方案。

## 验证方式与范围

本轮定向及相关业务回归共 **499 项通过**，相关新改模块 Ruff 检查通过，`git diff --check` 通过。测试在只读挂载工作区、无网络的独立容器内运行；已有 CSV 检查依赖的 DuckDB spatial 扩展由官方 HTTPS 来源预先缓存，测试期间未放开外网。现有 datetime/Pydantic 警告未导致失败。

定向测试覆盖跨组织同名用户、跨用户读改删、无归属旧数据、凭证失效与权限撤销、上游故障、签名篡改、并发受理、数据库线程/进程争用、同名文件、上传回滚、记忆队列及真实 LangChain 同步/异步调用的隔离注入。

新增隔离验证脚本，在临时目录与端口启动真实 LangGraph，使用确定性 CSV 图和测试 IAM，不调用模型/GPU、线上 IAM 或生产数据：

```sh
python ../scripts/verify_user_access_runtime.py
```

14 组运行时断言覆盖认证与撤权、跨用户/组织资源访问、搜索、双任务实际并行、第三任务排队、用户/全局 429、流重连、取消恢复、同名文件结果、记忆及并发数据源登记。测试认证提供方只用于临时进程，不能替代真实 OneCity 联调。

当前仍采用单图进程配额，默认执行 2、全局运行与排队 10、已认证单用户 2。后续多副本需共享计数和原子预留；未进行真实模型/GPU 容量验收或浏览器视觉验收。SQLite 与文件操作之间尚无崩溃恢复日志，异常回滚测试不能等同断电恢复保证。LocalSandbox 不提供操作系统级用户隔离，生产工具执行需使用隔离沙箱；应用层路径检查也不替代操作系统访问控制。

## OneCity 双向 HTTP 联调

`python scripts/verify_onecity_multiuser.py --onecity-root <OneCity源码路径>` 启动独立 SQLite 账号目录、真实 OneCity Coordinator/IAM bridge、DeerFlow Gateway 与原生 LangGraph，三个合成账号分别上传同名 CSV，验证真实双向 HTTP callback。10 组检查全部通过，结果见 `outputs/onecity-multiuser-integration-20261112/real-http-summary.json`。

测试中两个 24 秒任务的执行区间重叠约 23 秒，第三个任务收到 pending 后在前一任务结束后转为 running；三份报告总和分别为 11、22、33。测试还覆盖本人会话重复创建幂等、跨组织与跨用户拒绝、同用户限额 429、同属一个用户的错误会话不能下载另一 thread，以及撤销令牌后两端 401。该测试配置为执行 2、总量 4、单用户 1；未调用模型，不能作为生产模型容量指标。

真实 OneCity 入口的生产验收由 `scripts/verify_onecity_production.py` 执行，使用随机普通测试账号，清理时撤销令牌并停用测试账号；不借用现有用户凭据。生产专用服务和回退见[部署说明](onecity-private-multiuser-deployment.md)。历史无可靠归属的数据保留在原实例，不自动分配给当前用户。
