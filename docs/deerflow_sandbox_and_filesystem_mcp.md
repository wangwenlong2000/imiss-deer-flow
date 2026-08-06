# DeerFlow 沙箱运行环境说明（含 filesystem MCP Server）

本文从原理、功能、使用三方面说明 DeerFlow 的沙箱运行环境，并解释 filesystem MCP Server 是什么，以及它与沙箱的关系。

## 1. 原理

### 1.1 沙箱的定位

DeerFlow 中的沙箱（sandbox）是 Agent 执行文件和命令类工具时的运行边界层，核心目标是：

- 给模型提供统一的虚拟路径和执行接口。
- 控制工具访问范围，减少越权风险。
- 在不同执行后端之间保持一致的使用体验。

### 1.2 运行模式

根据 `config.yaml` 的 `sandbox.use`，常见有两种模式：

- 本地模式：`deerflow.sandbox.local:LocalSandboxProvider`
- 容器模式：`deerflow.community.aio_sandbox:AioSandboxProvider`

在本地模式下，执行发生在宿主机；在容器模式下，执行发生在容器（或 provisioner 管理的运行单元）中。

### 1.3 工作机制（关键流程）

1. 中间件阶段注入 sandbox 能力（默认懒加载）。
2. 当首次调用 `bash`、`ls`、`read_file` 等沙箱工具时，才真正 acquire 沙箱实例。
3. 工具调用使用虚拟路径（如 `/mnt/user-data/...`、`/mnt/skills`），本地模式会做“虚拟路径 -> 实际路径”映射。
4. 执行前进行路径安全校验；执行后返回结果，并尽量屏蔽宿主机真实路径细节。

这意味着“沙箱”本质上不是单个命令，而是一套运行时隔离 + 路径映射 + 权限约束机制。

## 2. 功能

### 2.1 统一执行环境

- 提供一致的工具入口：`bash`、`ls`、`read_file`、`write_file` 等。
- 让模型始终面向虚拟路径工作，减少环境差异带来的不确定性。

### 2.2 路径映射与安全控制

- 线程目录通常映射为 `/mnt/user-data/workspace`、`/mnt/user-data/uploads`、`/mnt/user-data/outputs`。
- skills 目录可映射到 `config.skills.container_path`（默认 `/mnt/skills`），一般作为只读路径暴露。
- 本地 bash/文件工具会校验绝对路径是否落在允许根路径内。

### 2.3 生命周期管理

- 支持懒初始化：只有真正需要工具执行时才创建/获取沙箱。
- 多轮对话可复用同一线程的沙箱，降低重复创建成本。

## 3. 使用

### 3.1 基础配置示例

本地模式：

```yaml
sandbox:
  use: deerflow.sandbox.local:LocalSandboxProvider
```

容器模式：

```yaml
sandbox:
  use: deerflow.community.aio_sandbox:AioSandboxProvider
```

### 3.2 使用建议

- 优先使用虚拟路径，不直接写宿主机绝对路径。
- 在本地模式下，若路径访问失败，优先检查：
  - 是否存在虚拟路径映射。
  - 是否在允许访问根路径内。
- 生产环境通常建议容器沙箱，以获得更强隔离。

### 3.3 沙箱底层代码流程（实际调用链）

下面是 DeerFlow 中“沙箱真正开始工作”的代码级流程（以 `bash/read_file/ls` 这类沙箱工具为例）：

1. Agent 启动时组装运行时中间件链。
  - `build_lead_runtime_middlewares()` 会把 `ThreadDataMiddleware`、`SandboxMiddleware` 放进链路。
  - 代码位置：`backend/packages/harness/deerflow/agents/middlewares/tool_error_handling_middleware.py`

2. `ThreadDataMiddleware.before_agent()` 先写入线程目录路径到状态。
  - 写入 `thread_data.workspace_path/uploads_path/outputs_path`。
  - 默认是 lazy 模式：先计算路径，不立即创建目录。
  - 代码位置：`backend/packages/harness/deerflow/agents/middlewares/thread_data_middleware.py`

3. `SandboxMiddleware` 负责沙箱生命周期入口。
  - 默认 `lazy_init=True`，`before_agent()` 不立即 acquire。
  - 代码位置：`backend/packages/harness/deerflow/sandbox/middleware.py`

4. 模型首次调用沙箱工具时，才真正触发懒初始化。
  - 例如 `bash_tool()`、`ls_tool()`、`read_file_tool()` 首先调用 `ensure_sandbox_initialized(runtime)`。
  - 若 runtime 中还没有 sandbox，则：
    - 从 `runtime.context["thread_id"]` 取线程 ID。
    - 调用 `get_sandbox_provider().acquire(thread_id)` 创建或复用沙箱。
    - 将 `runtime.state["sandbox"] = {"sandbox_id": ...}` 写回状态。
  - 代码位置：`backend/packages/harness/deerflow/sandbox/tools.py`

5. Provider 选择与实例化。
  - `get_sandbox_provider()` 读取 `config.sandbox.use`，通过反射加载 provider 类。
  - 常见是 `LocalSandboxProvider` 或 `AioSandboxProvider`。
  - 代码位置：`backend/packages/harness/deerflow/sandbox/sandbox_provider.py`

6. Local 模式执行路径（本机执行）。
  - `LocalSandboxProvider.acquire()` 返回单例 `LocalSandbox("local")`。
  - `LocalSandbox.execute_command()` 最终调用 `subprocess.run(...)` 在宿主机执行。
  - 执行前后会做路径映射与输出路径反向映射。
  - 代码位置：
    - `backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py`
    - `backend/packages/harness/deerflow/sandbox/local/local_sandbox.py`

7. AIO 模式执行路径（容器/API 执行）。
  - `AioSandboxProvider.acquire()` 会按 thread_id 复用或创建容器沙箱。
  - 通过 backend create/discover，等待 `wait_for_sandbox_ready()`。
  - 返回 `AioSandbox` 后，`execute_command/read_file/write_file` 走 sandbox HTTP API。
  - 代码位置：
    - `backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py`
    - `backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox.py`

8. 本地模式下的安全与路径控制。
  - 工具层在执行前会做：
    - 虚拟路径归一化（如 dataset alias）
    - 允许根路径校验（线程目录 + 只读映射）
    - 虚拟路径到真实路径替换
  - 关键函数：`validate_local_bash_command_paths()`、`_resolve_local_tool_path()`、`replace_virtual_path()`。
  - 代码位置：`backend/packages/harness/deerflow/sandbox/tools.py`

9. 工具调用结束后的释放行为。
  - `SandboxMiddleware.after_agent()` 会调用 `provider.release(sandbox_id)`。
  - Local provider 的 `release()` 是 no-op（单例复用）。
  - AIO provider 的 `release()` 会把实例放入 warm pool，容器可复用，后续由 idle/shutdown 统一清理。

可把这条链路简化理解为：

`中间件挂载 -> 首次工具调用触发 acquire -> provider 分发到 local/aio -> 执行命令或文件操作 -> release/复用`

### 3.4 sandbox 创建与执行工具的代码流程（可逐文件追踪）

#### A. 创建流程（Create Path）

1. Agent 构建时注入运行时中间件。
  - 入口：`build_lead_runtime_middlewares(lazy_init=True)`
  - 注入：`ThreadDataMiddleware` + `SandboxMiddleware`
  - 文件：`backend/packages/harness/deerflow/agents/middlewares/tool_error_handling_middleware.py`

2. 线程级目录信息写入状态。
  - `ThreadDataMiddleware.before_agent()` 计算并写入 `thread_data`。
  - 文件：`backend/packages/harness/deerflow/agents/middlewares/thread_data_middleware.py`

3. 首次工具调用触发 lazy acquire。
  - `ensure_sandbox_initialized(runtime)`：
    - 先检查 `runtime.state["sandbox"]`。
    - 若不存在，读取 `runtime.context["thread_id"]`。
    - 调用 `provider.acquire(thread_id)` 获取 `sandbox_id`。
    - 写回 `runtime.state["sandbox"] = {"sandbox_id": ...}`。
  - 文件：`backend/packages/harness/deerflow/sandbox/tools.py`

4. Provider 决定具体沙箱实现。
  - `get_sandbox_provider()` 从 `config.sandbox.use` 反射加载 provider。
  - 文件：`backend/packages/harness/deerflow/sandbox/sandbox_provider.py`

5. Provider 产出具体实例。
  - Local：`LocalSandboxProvider.acquire()` 返回单例 `LocalSandbox("local")`。
  - AIO：`AioSandboxProvider.acquire()` 进行 discover/create，并返回 `AioSandbox`。
  - 文件：
    - `backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py`
    - `backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py`

#### B. 执行流程（Execute Path）

1. 工具入口统一先拿 sandbox。
  - `bash_tool / ls_tool / read_file_tool / write_file_tool / str_replace_tool`
  - 首句都是 `sandbox = ensure_sandbox_initialized(runtime)`。
  - 文件：`backend/packages/harness/deerflow/sandbox/tools.py`

2. 本地模式执行前做三件事。
  - 路径归一化：`normalize_dataset_virtual_paths_in_command()`
  - 安全校验：`validate_local_bash_command_paths()` 或 `_resolve_local_tool_path()`
  - 虚拟路径替换：`replace_virtual_paths_in_command()` / `replace_virtual_path()`

3. 最终调用 Sandbox 抽象接口。
  - 命令：`sandbox.execute_command(...)`
  - 文件读：`sandbox.read_file(...)`
  - 文件写：`sandbox.write_file(...)`
  - 抽象定义文件：`backend/packages/harness/deerflow/sandbox/sandbox.py`

4. 由具体实现执行。
  - LocalSandbox：`subprocess.run(...)` 在宿主机执行；并做路径反向脱敏。
    - 文件：`backend/packages/harness/deerflow/sandbox/local/local_sandbox.py`
  - AioSandbox：通过 sandbox API client 执行 shell/file 操作。
    - 文件：`backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox.py`

5. 一轮 Agent 结束后释放。
  - `SandboxMiddleware.after_agent()` 调用 `provider.release(sandbox_id)`。
  - Local 通常 no-op；AIO 进入 warm pool，后续可复用。
  - 文件：`backend/packages/harness/deerflow/sandbox/middleware.py`

### 3.5 sandbox 内部组件清单

从代码结构看，sandbox 子系统由 6 类组件组成：

1. 抽象接口层
  - `Sandbox`：统一定义 `execute_command/read_file/write_file/list_dir/update_file`。
  - 文件：`backend/packages/harness/deerflow/sandbox/sandbox.py`

2. Provider 层（实例生命周期）
  - `SandboxProvider` 抽象 + `get_sandbox_provider()` 单例入口。
  - 文件：`backend/packages/harness/deerflow/sandbox/sandbox_provider.py`

3. 具体运行时实现层
  - `LocalSandboxProvider` + `LocalSandbox`
  - `AioSandboxProvider` + `AioSandbox`
  - 文件：
    - `backend/packages/harness/deerflow/sandbox/local/local_sandbox_provider.py`
    - `backend/packages/harness/deerflow/sandbox/local/local_sandbox.py`
    - `backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py`
    - `backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox.py`

4. 工具适配层
  - 把 LLM 工具调用适配到 sandbox 方法：`bash/ls/read_file/write_file/str_replace`。
  - 文件：`backend/packages/harness/deerflow/sandbox/tools.py`

5. 路径与安全控制层
  - 虚拟路径映射、只读映射、绝对路径白名单、路径穿越防护。
  - 关键函数集中在 `tools.py`。

6. 中间件编排层
  - `ThreadDataMiddleware` 负责线程目录上下文；
  - `SandboxMiddleware` 负责 acquire/release 生命周期。
  - 文件：
    - `backend/packages/harness/deerflow/agents/middlewares/thread_data_middleware.py`
    - `backend/packages/harness/deerflow/sandbox/middleware.py`

### 3.6 sandbox 如何支持各类工具执行

#### 1) 直接由 sandbox 执行的工具

- `bash`：命令执行，调用 `execute_command`。
- `ls`：目录枚举，调用 `list_dir`。
- `read_file`：文本读取，调用 `read_file`。
- `write_file`：文本写入，调用 `write_file`。
- `str_replace`：先 `read_file` 再 `write_file`。

这些工具全部在 `backend/packages/harness/deerflow/sandbox/tools.py` 中实现，并统一依赖 `ensure_sandbox_initialized()`。

#### 2) 不直接由 sandbox 执行的工具

- Built-in 工具（如 `ask_clarification`、`present_file`）
- MCP 工具（来自外部 MCP server）
- 社区工具（web search/fetch 等）

这些工具通过总装配函数 `get_available_tools()` 合并到 Agent，但它们本身不一定走 sandbox 抽象接口。
文件：`backend/packages/harness/deerflow/tools/tools.py`

#### 3) 为什么这样设计

- 把“高风险本地执行能力”集中在 sandbox 子系统，便于做权限和路径约束。
- 把“外部能力接入（MCP/社区）”与 sandbox 解耦，降低耦合度并方便扩展。
- 工具侧统一调用模式（先 ensure，再执行），让不同 provider（local/aio）可透明替换。

## 4. filesystem MCP Server 是什么

filesystem MCP Server 是一个通过 MCP（Model Context Protocol）协议向 Agent 暴露文件系统能力的外部工具服务。常见配置方式如下（来自 `extensions_config.json`）：

- `type: "stdio"`
- `command: "npx"`
- `args: ["-y", "@modelcontextprotocol/server-filesystem", "<allowed paths>"]`

它本质上是“一个 MCP 工具源”，启动后会向 DeerFlow 注册可调用的文件相关工具。

## 5. filesystem MCP Server 和沙箱的关系

两者关系可以概括为：

- 沙箱：DeerFlow 内建工具的执行边界与隔离层。
- filesystem MCP Server：通过 MCP 动态接入的外部文件工具提供者。

它们不是同一个层次的组件，但会在 Agent 侧同时表现为“可用工具”。

### 5.1 相同点

- 都能让 Agent 获得文件操作能力。
- 都需要通过配置控制访问范围。

### 5.2 不同点

- 沙箱工具由 DeerFlow 自身实现并受其沙箱策略直接约束。
- filesystem MCP Server 是独立进程（或独立服务）提供的 MCP 工具，遵循它自己的 allowed paths 配置与实现逻辑。

### 5.3 实践建议

- 把沙箱当作默认执行边界（尤其是 `bash` 等执行类操作）。
- 把 filesystem MCP Server 当作扩展能力，重点收敛它的 allowed paths。
- 二者并用时，分别审查两套边界：
  - 沙箱允许根路径/挂载路径。
  - filesystem MCP Server 的允许目录参数。

## 6. 一句话总结

DeerFlow 沙箱负责“在 DeerFlow 内如何安全地执行工具”，filesystem MCP Server 负责“通过 MCP 额外接入哪些文件工具”；二者互补，但安全边界需要分别配置和校验。