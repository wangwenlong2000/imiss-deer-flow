# OneCityAgent 与 UrbanBrain 统一项目

本次在 `integrate/street-onecity-20261125` 分支整合 GitHub 街景修复、当前 DeerFlow 工作树和本地 OneCityAgent 工作树。

| 来源 | 整合版本 |
| --- | --- |
| 当前 DeerFlow `compliance-merge` | `2bc7227`，本地源码改动快照为 `495642e` |
| GitHub `fix/street-workflow-20261125` | `b7c46a526cfd4977f6c84d1010480f1b3b53c7a5` |
| 本地 `onecityagent-integration` | `13e4bf1`，本地源码改动快照为 `25c7d312f645e1520a21e3cf53e5c11dd8f5cbe8` |

OneCity 通过 Git subtree 导入，完整源码位于 `apps/onecityagent/`，其原始提交历史可通过当前分支追溯。DeerFlow 的街景检索、候选审阅、证据交付和街景模型服务，以及两端已有的 IAM、会话归属、文件访问控制和 SSE 排队状态均包含在该分支。

## 目录与环境

```text
backend/                         DeerFlow Gateway 与 LangGraph
frontend/                        DeerFlow Next.js 前端
apps/onecityagent/
  app.py                         OneCity Flask API
  onecityagent/                  OneCity 服务与 Coordinator/IAM bridge
  python_a2a/                    本地 A2A 协议实现
  web/                           OneCity React/Umi 前端
  config/                        配置模板与本地配置
  tests/                         OneCity 隔离回归
skills/                          场景技能
services/streetmodel/             街景模型 HTTP 服务
```

DeerFlow 使用 `backend/pyproject.toml` 的 Python 环境与 `frontend/package.json` 的 Node 依赖。OneCity 使用 Python 3.13，依赖清单为自身的 `requirements.txt`，前端使用自身的 `web/package-lock.json`。街景模型服务依赖见其 README。

首次安装 OneCity 依赖前，先按下节完成本地配置。然后在根目录执行以下命令，需要已安装 Python 3.13 和 Node 22/npm。前端安装会执行 Umi 配置生成，因此安装时也需要应用的环境变量。

```bash
python3.13 -m venv apps/onecityagent/.venv
apps/onecityagent/.venv/bin/python -m pip install -r apps/onecityagent/requirements.txt
(
  cd apps/onecityagent
  set -a
  source ./.env
  set +a
  HUSKY=0 npm --prefix web ci
)
```

OneCity 前端安装与构建使用 Node 22。当前 Umi 依赖在 Node 24 下会因已移除的 `http_parser` 接口而构建失败。根启动脚本默认使用 `apps/onecityagent/.venv/bin/python`，可通过 `ONECITY_PYTHON` 指定现有 OneCity 解释器。DeerFlow、OneCity 与模型服务分别安装各自依赖。

## 本地配置与启动

从 `apps/onecityagent/.env.example` 和 `config/*.example.*` 建立该应用的本地配置。已有本地配置在本次整合时复制至对应目录，仍由 `.gitignore` 排除。配置字段与数据库准备步骤见应用的 [交接手册](../apps/onecityagent/HANDOFF.md)。

`apps/onecityagent/.env` 中的 `DEERFLOW_API_URL` 指向所用 Gateway 的 `/api/deerflow/mobile/run`。同一部署若设置了 `DEERFLOW_THREAD_ENSURE_URL`、`DEERFLOW_THREAD_UPLOAD_URL`、`DEERFLOW_FILE_INTERNAL_BASE`，也应指向相同 Gateway。`DEERFLOW_PUBLIC_BASE_URL` 按浏览器文件访问方式配置，现有 OneCity 同源认证代理支持该值为空。`ONECITY_BACKEND_URL` 和 `ONECITY_COORDINATOR_URL` 配置 Umi 开发代理。

Coordinator 的实际监听端口取自 `config/config.toml` 的 `[a2a].router` URL。`.env` 中的 `A2A_COORDINATOR_PORT`、`ONECITY_COORDINATOR_URL` 及 DeerFlow 的 IAM endpoint 应与该端口对应。

OneCity 的服务端身份通过 IAM bridge 传入 DeerFlow。启用认证的部署须同时配置 Gateway 和 LangGraph 的 `MOBILE_IAM_PROVIDER`、`MOBILE_IAM_ENDPOINT`、`MOBILE_IAM_REQUIRED` 及共享的受理签名配置，具体设置见 [多用户部署说明](onecity-private-multiuser-deployment.md)。

在三个终端分别执行以下命令。各进程前台运行，根脚本会切换到应用目录、加载应用的 `.env` 并设置本地 `PYTHONPATH`。

```bash
make onecity-api
make onecity-coordinator
make onecity-web
```

DeerFlow 服务使用根目录现有的 `make dev` 或部署命令。OneCity 配置中引用的 MySQL、A2A 和 MCP 服务依照交接手册准备。生产前端构建入口为 `make onecity-build`。部署辅助脚本 `scripts/deploy_onecity_entry.py` 的默认 `--source` 已指向本仓库的 OneCity 目录。

## 验证

```bash
make onecity-test
make onecity-test-client
```

Python 回归需要在 OneCity 环境安装 pytest。客户端回归复用 OneCity 前端的 TypeScript 依赖。两个测试命令均直接执行隔离回归。

两端真实 HTTP 联调使用合成账号、临时 SQLite 和确定性的 CSV 图任务。在已有 DeerFlow 后端环境中补齐 `Flask`、`Flask-Login`、`Peewee`、`Loguru` 后运行以下命令。

```bash
make onecity-verify
# 或指定具有上述依赖的现有解释器
make onecity-verify ONECITY_VERIFY_PYTHON=/path/to/python
```

联调脚本默认加载 `apps/onecityagent/`，`--onecity-root` 仍可指定其他工作树。街景模型及定位流程的独立验证见 [街景修复记录](releases/20261125-street-workflow.md)。

## 本次整合验证结果

2026-11-25 使用合并后的源码完成以下检查。

| 检查 | 结果 |
| --- | --- |
| DeerFlow 后端、IAM、合规、并发、街景技能和街景服务接口定向回归 | 375 项通过 |
| OneCity 认证、历史访问和 DeerFlow 接入回归 | 77 项通过 |
| DeerFlow 前端现有 Node 回归 | 32 项通过 |
| DeerFlow TypeScript 检查 | 通过 |
| OneCity 客户端会话、流与下载认证回归 | 通过 |
| OneCity 前端生产构建 | Node 22.21.1 下通过 |
| 两端真实 HTTP 联调 | 10 组检查通过 |
| 根启动入口的工作目录、环境加载、参数传递与 Make 映射 | 通过 |

HTTP 联调使用三个合成账号、临时 SQLite 和确定性 CSV 任务，验证认证、本人会话、同名文件隔离、跨用户拒绝、两个任务实际并行、第三任务排队、429 透传和令牌撤销。该验证覆盖接口与调度，真实模型和 GPU 推理的业务验收仍需使用部署环境另行执行。

整合回归同时修复了本地 ContextGate 纯文本工具结果处理路径缺少 `apply_actions` 导入的问题，并补齐清洁文本、告警、脱敏和拒绝的回归覆盖。
