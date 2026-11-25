#!/bin/bash
set -e

# Load deployment-specific values from an ignored local .env file.
if [ -f .env ]; then
    set -a
    # shellcheck disable=SC1091
    source ./.env
    set +a
fi

export DEERFLOW_STREAM="${DEERFLOW_STREAM:-true}"
export DEERFLOW_TIMEOUT="${DEERFLOW_TIMEOUT:-1800}"
export DEERFLOW_API_URL="${DEERFLOW_API_URL:-}"
export DEERFLOW_PUBLIC_BASE_URL="${DEERFLOW_PUBLIC_BASE_URL:-}"

if [ -z "$DEERFLOW_API_URL" ]; then
    echo "错误：请在本地 .env 中设置 DEERFLOW_API_URL（不要提交 .env）。" >&2
    exit 1
fi
if [[ "$DEERFLOW_API_URL" == *"YOUR-"* || "$DEERFLOW_API_URL" == *"REPLACE_"* ]]; then
    echo "错误：DEERFLOW_API_URL 仍是模板占位符，请先替换为部署方提供的真实地址。" >&2
    exit 1
fi
if [ -z "$DEERFLOW_PUBLIC_BASE_URL" ] || [[ "$DEERFLOW_PUBLIC_BASE_URL" == *"YOUR-"* || "$DEERFLOW_PUBLIC_BASE_URL" == *"REPLACE_"* ]]; then
    echo "错误：请设置可供浏览器访问的 DEERFLOW_PUBLIC_BASE_URL。" >&2
    exit 1
fi
if [ -z "${SECRET_KEY:-}" ] || [[ "$SECRET_KEY" == *"REPLACE_"* ]]; then
    echo "错误：请在本地 .env 中设置随机 SECRET_KEY。" >&2
    exit 1
fi
if [ ! -f config/config.toml ] || rg -q 'REPLACE_WITH|YOUR-' config/config.toml; then
    echo "错误：请从配置模板创建并填写 config/config.toml 后再启动。" >&2
    exit 1
fi

for port_var in ONECITY_APP_PORT A2A_LLM_PORT A2A_INTENT_PORT A2A_PLANNING_PORT A2A_ACTOR_PORT A2A_FEEDBACK_PORT A2A_COORDINATOR_PORT; do
    value="${!port_var:-}"
    if ! [[ "$value" =~ ^[0-9]+$ ]] || [ "$value" -lt 1 ] || [ "$value" -gt 65535 ]; then
        echo "错误：请在本地 .env 中为 $port_var 设置有效的内部监听值。" >&2
        exit 1
    fi
done

# OneCity Agent 综合启动脚本
# 功能：
#  - 激活虚拟环境
#  - 设置 PYTHONPATH
#  - 启动 mock_mcp_tools 下所有 python 服务（跳过 __init__.py）
#  - 启动 a2a 服务并为每个服务生成日志和 pid
#  - 更新 A2A 配置（scripts/update_a2a_config.py）
#  - 后台启动 app.py 并记录 PID/日志

# set -u

# 终止这个 OneCityAgent 实例上次记录的服务。
# 旧 PID 文件可能来自已销毁的容器，不能盲目将其中的数字交给 kill。
echo "正在终止本实例已运行的服务..."
if compgen -G "pids/*.pid" > /dev/null; then
    for pid_file in pids/*.pid; do
        pid=$(cat "$pid_file" 2>/dev/null || true)
        if [[ "$pid" =~ ^[0-9]+$ ]] && [ "$pid" -ne 1 ] && kill -0 "$pid" 2>/dev/null; then
            cmdline=$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null || true)
            if [[ "$cmdline" == *"/app"* || "$cmdline" == *"onecityagent"* || "$cmdline" == *"app.py"* ]]; then
                kill "$pid" 2>/dev/null || true
            fi
        fi
    done
fi

echo "开始启动 OneCity Agent 集成服务..."

# 激活虚拟环境
VENV_PATH="/app/.venv/bin/activate"
if [ -f "$VENV_PATH" ]; then
    echo "激活虚拟环境: $VENV_PATH"
    # shellcheck disable=SC1091
    source "$VENV_PATH"
else
    echo "警告: 虚拟环境不存在: $VENV_PATH 。继续但可能会失败。"
fi

# 切换到项目目录（优先 /app，否则脚本所在目录）
if [ -d "/app" ]; then
    cd /app || exit 1
else
    cd "$(dirname "$0")" || exit 1
fi
PROJECT_ROOT=$(pwd)
PROJECT_PYTHON_PATH="$PROJECT_ROOT/.."
export PYTHONPATH="$PROJECT_PYTHON_PATH:$PYTHONPATH"

echo "项目根目录: $PROJECT_ROOT"
echo "PYTHONPATH: $PYTHONPATH"

# 创建日志和 pid 目录
mkdir -p logs pids

#
# 启动 mock_mcp_tools 下的所有 python 服务
#
mock_tools_dir="$PROJECT_ROOT/mock_mcp_tools"
if [ -d "$mock_tools_dir" ]; then
    echo "启动 $mock_tools_dir 目录下的所有服务..."
    for py_file in "$mock_tools_dir"/*.py; do
        if [ -f "$py_file" ]; then
            script_name=$(basename "$py_file")
            if [ "$script_name" = "__init__.py" ]; then
                continue
            fi
            relative_path="mock_mcp_tools/$script_name"
            log_file="logs/${script_name%.py}.log"
            pid_file="pids/${script_name%.py}.pid"
            echo "启动服务: $relative_path"
            nohup python "$py_file" > "$log_file" 2>&1 &
            pid=$!
            echo "$pid" > "$pid_file"
            echo "服务 $relative_path 已启动，PID: $pid，日志: $log_file"
        fi
    done
else
    echo "未找到目录: $mock_tools_dir，跳过 mock 服务启动。"
fi

# 给 mock 服务一些启动时间
sleep 10

#
# 启动 A2A 服务（可按需修改端口）
#
declare -A services=(
    ["a2a_llm_aq.py"]="$A2A_LLM_PORT"
    ["a2a_intent.py"]="$A2A_INTENT_PORT"
    ["a2a_planning.py"]="$A2A_PLANNING_PORT"
    ["a2a_thinking_acting.py"]="$A2A_ACTOR_PORT"
    ["a2a_reflection.py"]="$A2A_FEEDBACK_PORT"
    ["coordinator_sse_acting.py"]="$A2A_COORDINATOR_PORT"
)

for script in "${!services[@]}"; do
    port=${services[$script]}
    log_file="logs/${script%.py}.log"
    pid_file="pids/${script%.py}.pid"
    echo "启动服务: $script 端口: $port"
    nohup python "onecityagent/service/a2a_service/$script" --port "$port" > "$log_file" 2>&1 &
    pid=$!
    echo "$pid" > "$pid_file"
    echo "服务 $script 已启动，端口 $port，PID: $pid，日志: $log_file"
    sleep 1 
done
#
# 启动主应用 app.py
#
if [ ! -f "app.py" ]; then
    echo "错误: app.py 文件不存在，无法启动主应用"
    exit 1
fi

echo "使用 nohup 启动主应用 app.py ..."
nohup python app.py > logs/app.log 2>&1 &
APP_PID=$!
echo "$APP_PID" > "pids/app.pid"
echo "主应用已启动，PID: $APP_PID，日志: logs/app.log"

# 检查 app 进程是否存活
sleep 3
if ps -p "$APP_PID" > /dev/null; then
    echo "✅ 主应用启动成功"
else
    echo "❌ 主应用启动失败，请检查 logs/app.log"
    exit 1
fi

echo "所有服务已启动。"
echo "所有 PID 文件已保存到 pids/ 目录。"
echo "终止所有服务，请执行命令: kill \$(cat pids/*.pid)"
