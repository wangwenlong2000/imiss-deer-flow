# 按用户与任务类别控制并发

## 调度规则

- `DEER_FLOW_GRAPH_JOBS` 决定实际同时执行的任务数；默认 2。不同会话、不同用户和不同任务类型仍共享这组 worker。
- 全局 `DEER_FLOW_MAX_INFLIGHT_RUNS` 限制运行中与排队中任务总数；默认 10。
- `DEER_FLOW_MAX_INFLIGHT_PER_USER` 限制单个已认证用户的运行中与排队中任务数；默认 2。超限返回 HTTP 429，其他用户仍可使用全局剩余名额。
- `DEER_FLOW_IDENTITY_CLASS_LIMITS` 和 `DEER_FLOW_TASK_TYPE_LIMITS` 可为合规身份类别和授权操作类别分别设置全局上限。未配置某项时，该项只受全局上限约束。

LangGraph 在队列名额不足时返回 HTTP 429 和 `Retry-After`。移动端 JSON 接口将其保留为 `code: 429` 与 `retry_after`；SSE 接口通过 `error` 事件传递同样的状态和重试秒数，避免把正常排队拒绝显示成系统故障。

身份类别取自合规检测的固定 profile：`ordinary_user`、`internal_operator`、`cross_org_operator`、`publisher`、`researcher`。任务类别取自已授权 operation：`analysis`、`upload`、`export`、`share`、`public_release`、`research`。这两个类别默认不产生角色优先级；具体限额由部署方按资源容量配置。

## 身份可信边界

Gateway 仅在 `MOBILE_IAM_PROVIDER` 成功认证用户并获得授权决策后，才会创建并签署调度声明。声明绑定组织、用户、合规身份类别、授权操作和 thread，并使用 HMAC 对用户标识做不可逆的稳定摘要。LangGraph 验签后才按用户和类别计数；客户端直接填写 `user_id`、身份类别或任务类别不会取得对应配额。

Gateway 与 LangGraph 必须配置相同的 `DEER_FLOW_ADMISSION_SIGNING_KEY`，至少 32 字节。Compose 两个服务都会从根目录 `.env` 读取变量。例如先生成密钥：

```sh
openssl rand -hex 32
```

将输出值配置到 `.env`。同时启用 IAM：

```dotenv
MOBILE_IAM_PROVIDER=app.gateway.onecity_iam:OneCityIAMProvider
MOBILE_IAM_ENDPOINT=http://<onecity-internal-host>:<A2A_COORDINATOR_PORT>/internal/deerflow/iam
MOBILE_IAM_REQUIRED=true
DEER_FLOW_ADMISSION_SIGNING_KEY=<生成的随机密钥>
DEER_FLOW_MAX_INFLIGHT_PER_USER=2
```

`MOBILE_IAM_ENDPOINT` 必须直连 OneCityAgent Coordinator 的内网监听端口；当前公开 Nginx 配置不转发 IAM bridge。

OneCityAgent 的 `/query/deerflow`、上传、预览和下载请求必须带登录响应返回的 `Authorization` 请求头。前端 SSE 使用 fetch 流式读取以携带该请求头。OneCityAgent 的 IAM bridge 默认只允许用户访问自己创建的对话；额外操作按服务端角色映射授予。

无 IAM 的开发模式默认 `DEER_FLOW_REQUIRE_SIGNED_ADMISSION=false`，兼容浏览器直接请求，这类请求只计入全局队列。配置 IAM 提供方或强制 IAM 后，图服务自动要求签名受理，公网原生任务创建也必须改走已认证 Gateway。内部凭证不能替代任务的用户签名。独立网页和 IM 入口尚需接入可信身份提交链路；最新部署及迁移要求见[用户权限与并发改进记录](user-access-and-concurrency-20261112.md)。

可选的分类上限使用 JSON 对象，键必须来自上述合规类别：

```dotenv
DEER_FLOW_IDENTITY_CLASS_LIMITS='{"ordinary_user":4,"internal_operator":6,"cross_org_operator":3,"publisher":2,"researcher":4}'
DEER_FLOW_TASK_TYPE_LIMITS='{"analysis":8,"upload":4,"export":3,"share":3,"public_release":2,"research":4}'
```

示例数字只是配置格式示例，不代表合规授权或容量建议。修改变量后需重启或重建 Gateway 和 LangGraph 服务。签名密钥轮换前应等待当前任务结束，否则旧密钥签发的运行记录不会计入新密钥下的用户计数。未配置 IAM 的请求仍受全局队列保护，但没有可信用户身份，不能获得逐用户限额。

## 当前边界

按用户和类别查询活跃运行记录依赖当前 `langgraph dev` 的单进程内存运行时；启用了可信声明但运行时不能枚举任务元数据时，接口会返回 503 并拒绝绕过用户限额。切换到 Postgres 或多副本前，需要把计数和原子预留迁到共享存储。此功能控制受理上限，不改变 LangGraph worker 的实际执行并行度；超过用户/类别名额的提交返回 429，不会在应用层另建一条私有等待队列。

## 验证记录

- 并发受理、签名身份声明及移动端错误回传定向测试：43 项通过。
- 隔离 LangGraph 运行时验证：两个不同用户的任务同时执行；同一用户和同一任务类别的超限提交均返回 429；第三个任务进入 pending，第四个任务被拒绝；队列流重连、取消后恢复、同名上传/下载隔离及原生 wait/stream 均通过。验证图只执行确定性的 CSV 求和，不调用模型或 GPU。
