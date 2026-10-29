# OneCityAgent × DeerFlow 集成工作区

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
