#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

LOG_DIR="$REPO_ROOT/logs"
if [ ! -d "$LOG_DIR" ] || [ ! -w "$LOG_DIR" ]; then
    LOG_DIR="${TMPDIR:-/tmp}/deer-flow-skillrouter-logs"
fi

mkdir -p "$LOG_DIR"

is_port_listening() {
    local port="$1"

    if command -v lsof >/dev/null 2>&1; then
        lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1 && return 0
    fi

    if command -v ss >/dev/null 2>&1; then
        ss -ltn "( sport = :$port )" 2>/dev/null | tail -n +2 | grep -q . && return 0
    fi

    if command -v timeout >/dev/null 2>&1; then
        timeout 1 bash -c "exec 3<>/dev/tcp/127.0.0.1/$port" >/dev/null 2>&1 && return 0
    fi

    return 1
}

start_service() {
    local port="$1"
    local script_path="$2"
    local log_path="$3"
    local name="$4"

    if is_port_listening "$port"; then
        echo "✓ $name already running on port $port"
        return 0
    fi

    echo "Starting $name on port $port..."
    nohup python3 "$script_path" > "$log_path" 2>&1 &
}

start_service 7799 "$REPO_ROOT/scripts/serve_bge_m3.py" "$LOG_DIR/bge-m3.log" "BGE-M3"
start_service 7800 "$REPO_ROOT/scripts/serve_skillrouter_embedding.py" "$LOG_DIR/skillrouter-embedding.log" "SkillRouter Embedding"
start_service 7801 "$REPO_ROOT/scripts/serve_skillrouter_reranker.py" "$LOG_DIR/skillrouter-reranker.log" "SkillRouter Reranker"

echo ""
echo "Model services are starting. Logs:"
echo "  $LOG_DIR/bge-m3.log"
echo "  $LOG_DIR/skillrouter-embedding.log"
echo "  $LOG_DIR/skillrouter-reranker.log"