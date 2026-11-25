# OneCityAgent × DeerFlow 集成工作区

本应用已合入 UrbanBrain 仓库的 `apps/onecityagent/`。从仓库根目录使用 `make onecity-api`、`make onecity-coordinator`、`make onecity-web` 启动对应进程，使用 `make onecity-build` 构建前端。依赖与本地配置仍位于本目录，完整步骤见 [统一项目说明](../../docs/integration-onecity-20261125.md)。

这是 OneCityAgent 城市超脑页面及其 DeerFlow 流式接入逻辑的独立交接副本。仓库仅面向已获授权的协作者；真实部署地址、对外端口、令牌、数据库凭据和私钥均不应提交。

## 先读什么

- [同门交接与联调手册](HANDOFF.md)：结构、消息链路、身份/文件传递、配置、启动、测试、排错和协作流程。
- [安全与配置说明](SECURITY.md)：哪些文件不能提交，如何本地配置和检查提交。
- `config/*.example.*` 与 `.env.example`：脱敏模板；复制后填写本地环境值。

## 仓库包含什么

- `web/`：城市超脑前端（会话、身份选择、附件和分段消息展示）。
- `onecityagent/`：Python 后端、A2A 协调器及 DeerFlow SSE/上传/文件预览代理。
- `python_a2a/`：项目使用的 A2A 协议库。
- `mock_mcp_tools/`：开发和演示用 mock 工具。
- `start.sh`：本地服务编排脚本；内部监听端口可由环境变量覆盖。
- `nginx.conf.template`：反向代理模板，不包含任何公网主机映射。

## 重要说明

此仓库是代码交接包，不包含可直接连接某个现网 DeerFlow 实例的凭据或地址。先按手册填好本地 `config/config.toml`、`config/a2a.json` 和未跟踪的 `.env`，再启动和联调。请勿将真实业务数据、账号令牌或用户会话导入仓库。

## 多用户入口（2026-11-12）

OneCity 登录凭据贯穿 DeerFlow 会话、流式问答、上传、预览和下载。后端按有效账号、工作区成员关系与本人会话归属授权；浏览器传入的身份和角色不授予权限。账号切换清理前端缓存并中止旧连接，本地历史按经服务端验证的组织与用户隔离。旧本地历史不会自动归属给新账号。

登录返回当前新令牌，退出立即撤销；注册默认普通角色，个人设置只接受资料偏好字段。通用历史 API 同样限制本人访问。排队和运行状态来自实际调度器；HTTP 或 SSE 内的 429 均展示重试提示。配额为服务端部署参数，不能用浏览器连接数代表执行容量。

使用 Node 22 构建 `web/`；`node web/scripts/verify-multi-user-client.cjs` 检查客户端认证边界。隔离 Python 回归位于 `tests/test_authentication.py`、`tests/test_deerflow_access.py` 与 `tests/test_history_access.py`。生产设置、部署回退与验证限制见 [交接手册](HANDOFF.md)。独立 DeerFlow 网页登录不在本次接入范围。

2026-11-12 已完成 OneCity 生产入口与专用 IAM 后端切换，44 项真实入口检查通过，测试账号已停用。单图服务当前配置为执行 3、运行加排队合计 10、单用户 2；未进行真实模型吞吐压测。
