# OneCity / DeerFlow 交付验证契约 v1

这些契约固定现有 HTTP/SSE 行为与交付验收入口，不改变线上路由或授权规则。

## 验证层级

| Suite | 实际请求 | 通过条件 |
| --- | --- | --- |
| `basic` | 公开 `/healthz`、两个匿名身份入口、被隐藏的内部 IAM 路径、本站动态公钥指纹 | 就绪 JSON 正确、未登录返回 401 JSON、公开 IAM 返回 404；公开公钥与本地站点私钥派生指纹一致；只代表入口与站点身份检查 |
| `interfaces` | 动态公钥、登录、两普通账号身份、会话及消息历史读写、跨账号访问拒绝、登出及撤权 | 每个断言通过；默认临时账户必须清理成功 |
| `scenes` / `full` | 完整接口验证、真实模型问答、文件上传下载及隔离、真实工具计算及产物下载、精确数值、原生 run 终态、所选场景样本 | 所有选中能力有通过样例，SSE 明确完成且有真实 thread/run ID，原生 run 成功；不能用健康接口或 fixture 代替 |

`full` 当前表示 `interfaces + 所选场景`，不代表备份恢复或第二台机器验收；它们需独立运行并留证。

验证命令只能显式运行。`interfaces/scenes/full` 默认使用部署配置的 bootstrap 管理员密码检查初始账号，再注册两个带不可预测运行标识的普通账号。注册不指定角色；角色必须由服务端规则授予。也可使用权限为 0600 的凭据文件，格式为 `{"accounts":[{"email":"...","password":"..."},{"email":"...","password":"..."}]}`，两个账号应属于不同工作区，且为普通角色。使用已有测试账号会在结束时登出并撤销它们的当前令牌。

默认临时账号通过 `deploy.cli.verification_fixture` 的受限清理入口撤权、禁用；只有邮箱、昵称、用户 ID、普通角色和随机运行 ID 均匹配时才允许处理。测试上传、产物和审计记录保留作追溯，不删除业务数据库记录。清理失败会使整个验收失败，保留权限为 0600 的 `cleanup-state.json`；该文件可能含临时密码/令牌，不能随公开证据发送。清理成功后自动替换为无凭据的摘要。

## 动态登录公钥

`GET /v1/user/public-key` 返回：

```json
{"code":0,"data":{"public_key":"-----BEGIN PUBLIC KEY-----\n...","key_id":"...","algorithm":"RSA-OAEP-256"}}
```

登录密码通过该站点返回的 RSA 公钥加密，OAEP 和 MGF1 均使用 SHA-256。验证器不使用历史硬编码公钥。`POST /v1/user/login` 返回 200、`code:0`，令牌读取 `Authorization` 响应头（兼容 `data.access_token`）。验证报告不保存令牌、密码、响应正文或模型回答原文。

所有 suite（包含 `basic`）先在本地读取部署根目录的 `secrets/private.pem` 和 `secrets/rsa.passphrase`，导出 RSA 公钥的 DER SubjectPublicKeyInfo SHA-256 指纹，严格比对公开接口公钥及 `key_id`。私钥、口令不离开本机，也不进入报告。缺少或权限过宽的私钥、错误口令、公钥不匹配或伪造 key_id 均失败；身份比对失败时不会向目标发送任何登录凭据。这样 `public_url` 或 `--base-url` 误指另一站点时，不能仅靠健康接口通过验收。

## HTTP / 授权边界

- `/v1/user/info` 与 `/query/deerflow/identity` 无令牌应返回 401 JSON，不能重定向为登录网页。
- `/internal/deerflow/iam` 仅供内部服务使用，不能通过公开 edge 暴露。
- `POST /query/deerflow/dialog` 接受 `conversation_id`、`name`，会话 ID 格式为 `citybrain-` 后接最多 22 个字母数字或连字符。现有同名会话只有当前所有者可幂等访问。
- 当前会话的 creator、工作区 membership、thread/run 和文件归属全部由服务端验证；前端传入 user_id、角色或工作区不能授权。
- 跨账号访问会话、历史、文件或运行状态返回 404；普通身份不能通过自行传入角色提升权限。
- 401/403/404/409/429/503 保留 HTTP 语义；429 的 `Retry-After` 应保留，不能包装为成功。
- `resume` 仅加入原任务，必须传入现有 `conversation_id/thread_id/run_id`；不能创建新任务。取消与状态查询同样重新验证所有权。

## SSE 结束判据

OneCity 公开 SSE 每帧使用 `data: <JSON>\n\n`。心跳注释不属于结果。主要事件是：

```json
{"type":"系统","status":"running","metadata":{"source_event":"status","thread_id":"<UUID>","run_id":"<UUID>"}}
{"type":"agent","content":"实际回答","metadata":{"source_event":"final_answer"}}
{"type":"[DONE]","finished":true,"thread_id":"<UUID>"}
```

验收要求 `final_answer` 非空、至少获得有效 UUID 的 thread/run、明确 `finished:true`，且随后状态接口返回 `success` 或 `completed`。错误事件、失败/取消状态、断流、JSON 损坏、无结束标记、只输出 mock 或缺少 run 身份均不能算成功。因澄清而结束也不能替代一个预期有确定数值与产物的测试样例。验证失败后只取消本次已收到 ID 的确切任务。

## 按场景补充样例

基础样例已内置：12 × 12 的真实模型问答，以及上传 CSV 后运行本地工具生成 `acceptance-result.csv`，精确验证 A=48、B=70，并验证跨账号文件访问拒绝。启用 `code/street/video/...` 等能力时，必须显式提供相应真实样例，不能把基础问答认作这些能力已验收。

样例文件格式见 `scene-manifest.schema.json`。路径相对于样例文件，查询可能调用模型/工具并生成真实文件；操作人员应提前确认数据授权和模型费用。示例：

```json
{
  "schema_version": 1,
  "cases": [
    {
      "id": "approved-code-sample",
      "capability": "code",
      "query": "请分析上传的已授权样本，按验收说明生成结果。",
      "upload": {"filename": "approved-sample.py", "path": "fixtures/approved-sample.py"},
      "expected_text": ["约定的结构化结论"],
      "expected_artifact": {"path": "result.json", "sha256": "<固定输入对应的实际验收摘要>"}
    }
  ]
}
```

示例中的路径、结论和摘要仅说明语法，不能作为已验证场景直接使用。对模型可能改变文本的产物优先用数值/结构判据；对确定性产物可以使用 SHA-256。CSV 判据支持明确 key/value 列及精确映射，重复 key、缺少或多余行均失败。报告保存样例 ID、耗时、运行 ID、断言结果、答案/产物哈希和覆盖范围，输出位于部署根目录的 `evidence/verification/<run-id>/report.json`，权限为 0600。
