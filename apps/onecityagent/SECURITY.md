# 安全与配置约定

## 绝不提交

- `.env` 及其他本地环境变量文件。
- `config/config.toml`、`config/a2a.json`、`config/mcp.json`。
- `config/private.pem`、API key、数据库口令、JWT/会话密钥、证书和私钥。
- `.venv/`、`node_modules/`、构建目录、日志、PID、数据库、会话/上传文件和备份。
- 含真实用户、电话、位置、业务告警或访问令牌的测试数据。

模板文件只允许使用虚构占位符。私有仓库也不等于安全的密钥保管库。

## 本地初始化

1. `cp .env.example .env`，填写由 DeerFlow 部署负责人提供的 API 地址和浏览器可访问的文件基地址，并生成新的随机 `SECRET_KEY`。
2. `cp config/config.example.toml config/config.toml`，填写模型、RAG、数据库参数。
3. 只有在需要 A2A 外部智能体时才填写 `config/a2a.json`；MCP 配置按需从 `config/mcp.example.json` 复制。
4. 私钥如果确实被当前业务流程使用，必须在本机生成/安全注入；不要从其他人的工作区复制，也不要提交私钥。
5. 用 `git status --short` 和 `git check-ignore -v .env config/config.toml config/private.pem` 检查忽略规则生效。

## 变更前检查

- 查找硬编码公网 IP、个人主机名、访问令牌、私钥和本地端口映射。
- 确认浏览器可访问的 DeerFlow 文件地址与后端访问地址分别配置；不能假设两者总是相同。
- 新增配置只能放入 `*.example.*` 模板，并且值必须是占位符。
- 如果任何真实密钥曾经进入 Git 历史，应先在对应服务端轮换/撤销；仅从当前文件删除并不能清除历史提交。

## 多用户授权边界

- 显式 Authorization 优先于旧浏览器 Cookie，无效请求头不得回退其他账号的 Cookie。
- 账号必须有效且启用；工作区与成员关系也须有效。角色和额外权限仅由服务端数据及配置映射生成。
- 注册不接受浏览器指定管理角色；个人设置只更新 nickname、avatar、language、color_schema、timezone，改密须核验当前密码。
- 文件必须经同源认证代理，文件 thread 必须与当前用户获授权 dialog 的真实 DeerFlow 映射一致。不得向外部链接转发平台令牌。
- IAM callback 只在服务内网开放；访问日志仅记录请求方法、路径、状态与耗时，不能记录参数、认证头和正文。
- 部署备份、环境文件、账号验证材料和生产日志保存在仓库外受限目录，验收材料只保留合成数据与脱敏断言。
