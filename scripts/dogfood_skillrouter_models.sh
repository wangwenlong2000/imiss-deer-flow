#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

BGE_PORT="${BGE_M3_PORT:-7799}"
EMBED_PORT="${SKILLROUTER_EMBEDDING_PORT:-7800}"
RERANK_PORT="${SKILLROUTER_RERANKER_PORT:-7801}"

LOG_DIR="$REPO_ROOT/logs"
if [ ! -d "$LOG_DIR" ] || [ ! -w "$LOG_DIR" ]; then
    LOG_DIR="${TMPDIR:-/tmp}/deer-flow-skillrouter-logs"
fi

BGE_LOG="$LOG_DIR/bge-m3.log"
EMBED_LOG="$LOG_DIR/skillrouter-embedding.log"
RERANK_LOG="$LOG_DIR/skillrouter-reranker.log"

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

wait_for_port() {
    local port="$1"
    local timeout_seconds="${2:-120}"
    local name="$3"

    "$REPO_ROOT/scripts/wait-for-port.sh" "$port" "$timeout_seconds" "$name"
}

json_request() {
    local url="$1"
    local payload="$2"

    python3 - "$url" "$payload" <<'PY'
import json
import sys
import urllib.request

url = sys.argv[1]
payload = json.loads(sys.argv[2])
data = json.dumps(payload).encode("utf-8")
request = urllib.request.Request(
    url,
    data=data,
    headers={"Content-Type": "application/json"},
    method="POST",
)
with urllib.request.urlopen(request, timeout=60) as response:
    print(response.read().decode("utf-8"))
PY
}

echo "=========================================="
echo "  SkillRouter Model Dogfood"
echo "=========================================="
echo ""
echo "Logs directory: $LOG_DIR"
echo ""

start_service "$BGE_PORT" "$REPO_ROOT/scripts/serve_bge_m3.py" "$BGE_LOG" "BGE-M3"
start_service "$EMBED_PORT" "$REPO_ROOT/scripts/serve_skillrouter_embedding.py" "$EMBED_LOG" "SkillRouter Embedding"
start_service "$RERANK_PORT" "$REPO_ROOT/scripts/serve_skillrouter_reranker.py" "$RERANK_LOG" "SkillRouter Reranker"

wait_for_port "$BGE_PORT" 120 "BGE-M3"
wait_for_port "$EMBED_PORT" 120 "SkillRouter Embedding"
wait_for_port "$RERANK_PORT" 120 "SkillRouter Reranker"

echo ""
echo "=== Health Checks ==="
curl -s "http://127.0.0.1:${BGE_PORT}/health" | python3 -m json.tool
curl -s "http://127.0.0.1:${EMBED_PORT}/health" | python3 -m json.tool
curl -s "http://127.0.0.1:${RERANK_PORT}/health" | python3 -m json.tool

echo ""
echo "=== Embedding Smoke Test: BGE-M3 ==="
curl -s "http://127.0.0.1:${BGE_PORT}/v1/embeddings" \
    -H "Content-Type: application/json" \
    -d '{"model":"BAAI/bge-m3","input":["hello world"]}' \
    | python3 -m json.tool | sed -n '1,60p'

echo ""
echo "=== Embedding Smoke Test: SkillRouter Embedding ==="
json_request \
    "http://127.0.0.1:${EMBED_PORT}/v1/embeddings" \
    '{"model":"pipizhao/SkillRouter-Embedding-0.6B","mode":"query","input":["Implement a feature branch workflow with PR checks."]}' \
    | python3 -m json.tool | sed -n '1,60p'

echo ""
echo "=== Rerank Smoke Test: SkillRouter Reranker ==="
json_request \
    "http://127.0.0.1:${RERANK_PORT}/v1/rerank" \
    '{"model":"pipizhao/SkillRouter-Reranker-0.6B","query":"Implement a feature branch workflow with PR checks.","documents":[{"name":"moai-foundation-git","desc":"Git workflow conventions","body":"# Git Workflow ..."},{"name":"concurrency-control","desc":"Mutex patterns for CI","body":"# Concurrency Control ..."}],"top_n":1}' \
    | python3 -m json.tool | sed -n '1,80p'

echo ""
echo "✓ Dogfood complete"
echo "Logs:"
echo "  $BGE_LOG"
echo "  $EMBED_LOG"
echo "  $RERANK_LOG"