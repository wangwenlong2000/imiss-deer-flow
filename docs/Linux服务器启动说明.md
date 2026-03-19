# Linux 服务器启动说明

这份文档记录当前项目在 Linux 服务器上的可用启动方式。

适用场景：
- 无 sudo
- 无 Docker
- 手动启动 3 个服务
- 使用自定义端口，避开服务器上已被占用的默认端口

当前确认可用的端口：
- LangGraph: `3024`
- Gateway: `38001`
- Frontend: `33000`

## 1. 前置条件

服务器需要具备以下工具：

```bash
git --version
python3 --version
node --version
pnpm --version
uv --version
ss --version || lsof -v
```

建议版本：
- Python `3.12+`
- Node.js `22+`
- pnpm `10+`
- uv `0.7+`

## 2. 目录与虚拟环境

项目目录：

```bash
~/imiss-deer-flow-main
```

后端统一使用这个虚拟环境：

```bash
~/imiss-deer-flow-main/backend/.venv
```

不要混用项目根目录下的其它 `.venv`。

激活方式：

```bash
cd ~/imiss-deer-flow-main/backend
source .venv/bin/activate
```

确认解释器：

```bash
which python
python --version
```

## 3. 配置要求

项目根目录需要有：
- `config.yaml`
- `.env`

加载配置路径时统一使用：

```bash
export DEER_FLOW_CONFIG_PATH=~/imiss-deer-flow-main/config.yaml
```

`.env` 至少需要：

```bash
DASHSCOPE_API_KEY=你的真实Key
```

`config.yaml` 中 Qwen 模型的关键配置应类似：

```yaml
models:
  - name: qwen-plus
    display_name: Qwen Plus
    use: langchain_openai:ChatOpenAI
    model: qwen-plus
    api_key: $DASHSCOPE_API_KEY
    base_url: https://dashscope.aliyuncs.com/compatible-mode/v1
    max_tokens: 4096
    temperature: 0.7
    supports_vision: true
```

## 4. 启动方式

需要开 3 个终端，分别启动 LangGraph、Gateway、Frontend。

如果你想一键启动，优先使用：

```bash
cd ~/imiss-deer-flow-main
make linux-server-start
```

一键停止：

```bash
cd ~/imiss-deer-flow-main
make linux-server-stop
```

一键查看状态：

```bash
cd ~/imiss-deer-flow-main
make linux-server-status
```

这三个命令固定使用当前服务器上已经验证可用的端口：
- LangGraph: `3024`
- Gateway: `38001`
- Frontend: `33000`

### 4.1 启动 LangGraph

```bash
cd ~/imiss-deer-flow-main/backend
source .venv/bin/activate
export DEER_FLOW_CONFIG_PATH=~/imiss-deer-flow-main/config.yaml
uv run langgraph dev --host 127.0.0.1 --port 3024 --no-browser --allow-blocking
```

### 4.2 启动 Gateway

```bash
cd ~/imiss-deer-flow-main/backend
source .venv/bin/activate
export DEER_FLOW_CONFIG_PATH=~/imiss-deer-flow-main/config.yaml
PYTHONPATH=. uv run uvicorn app.gateway.app:app --host 0.0.0.0 --port 38001 --reload
```

### 4.3 启动 Frontend

```bash
cd ~/imiss-deer-flow-main/frontend
NEXT_PUBLIC_BACKEND_BASE_URL=http://localhost:38001 NEXT_PUBLIC_LANGGRAPH_BASE_URL=http://localhost:3024 pnpm exec next dev --turbo --hostname 127.0.0.1 --port 33000
```

## 5. 访问地址

- Frontend: `http://127.0.0.1:33000`
- LangGraph Docs: `http://127.0.0.1:3024/docs`
- Gateway Docs: `http://127.0.0.1:38001/docs`

## 6. 健康检查

启动后可在新终端执行：

```bash
curl -I http://127.0.0.1:3024/docs
curl -I http://127.0.0.1:38001/docs
curl -I http://127.0.0.1:33000
curl -s http://127.0.0.1:38001/api/models
```

预期：
- 前 3 个返回 `200`
- `/api/models` 返回配置好的模型，例如 `qwen-plus`

## 7. 后端提问测试

如果要验证 LangGraph 链路是否正常，可直接运行：

```bash
~/imiss-deer-flow-main/backend/.venv/bin/python - <<'PY'
import asyncio
from langgraph_sdk import get_client

async def main():
    client = get_client(url="http://127.0.0.1:3024")
    thread = await client.threads.create()
    result = await client.runs.wait(
        thread["thread_id"],
        assistant_id="lead_agent",
        input={"messages":[{"type":"human","content":[{"type":"text","text":"你好，你是谁？"}]}]},
        context={
            "thread_id": thread["thread_id"],
            "model_name": "qwen-plus",
            "thinking_enabled": False,
            "is_plan_mode": False,
            "subagent_enabled": False,
        },
        config={"recursion_limit": 1000},
    )
    print(result)

asyncio.run(main())
PY
```

## 8. 关闭服务

如果服务是在前台终端里启动的，分别在 3 个终端中按：

```bash
Ctrl + C
```

## 9. 常见问题

### 9.1 不要使用默认端口

这台服务器上默认端口已被占用：
- `2024`
- `8001`

因此当前部署固定改用：
- `3024`
- `38001`
- `33000`

### 9.2 `langgraph dev` 报找不到 `langgraph.json`

需要在 `backend/` 目录下启动：

```bash
cd ~/imiss-deer-flow-main/backend
```

### 9.3 `ChatOpenAI` 能调用，但前端不能提问

先不要看前端，优先检查：
- LangGraph 是否真的启动在 `3024`
- Gateway 是否真的启动在 `38001`
- 前端环境变量是否指向这两个端口

### 9.4 不要混用虚拟环境

后端只使用：

```bash
~/imiss-deer-flow-main/backend/.venv
```

否则容易出现：
- `openai` 找不到
- `langchain_openai` 找不到
- `uv sync` 装到了别的环境里
