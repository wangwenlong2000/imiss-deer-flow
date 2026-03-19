#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$REPO_ROOT/logs"
PID_DIR="$REPO_ROOT/.deerflow-no-nginx"

LANGGRAPH_PORT="${LANGGRAPH_PORT:-2024}"
GATEWAY_PORT="${GATEWAY_PORT:-8001}"
FRONTEND_PORT="${FRONTEND_PORT:-3000}"

usage() {
    cat <<EOF
Usage: $0 [start|stop|restart|status|logs]

Commands:
  start    Start LangGraph, Gateway and Frontend in background (default)
  stop     Stop all three services
  restart  Restart all three services
  status   Show process and port status
  logs     Show latest logs from all services

Environment variables (optional):
  LANGGRAPH_PORT   Default: 2024
  GATEWAY_PORT     Default: 8001
  FRONTEND_PORT    Default: 3000 (auto-fallback to 3001 if occupied)
EOF
}

pick_uv_bin() {
    if [ -x "$REPO_ROOT/backend/.venv/bin/uv" ]; then
        echo "$REPO_ROOT/backend/.venv/bin/uv"
        return 0
    fi
    if [ -x "$REPO_ROOT/.venv/bin/uv" ]; then
        echo "$REPO_ROOT/.venv/bin/uv"
        return 0
    fi
    if command -v uv >/dev/null 2>&1; then
        command -v uv
        return 0
    fi
    echo ""
    return 1
}

load_proxy_if_available() {
    if [ -f "$HOME/.proxy/use-proxy.sh" ]; then
        # shellcheck disable=SC1090
        source "$HOME/.proxy/use-proxy.sh"
    fi
}

ensure_config_exists() {
    if [ -n "${DEER_FLOW_CONFIG_PATH:-}" ] && [ -f "$DEER_FLOW_CONFIG_PATH" ]; then
        return 0
    fi
    if [ -f "$REPO_ROOT/config.yaml" ]; then
        export DEER_FLOW_CONFIG_PATH="$REPO_ROOT/config.yaml"
        return 0
    fi
    if [ -f "$REPO_ROOT/backend/config.yaml" ]; then
        export DEER_FLOW_CONFIG_PATH="$REPO_ROOT/backend/config.yaml"
        return 0
    fi
    echo "✗ No config file found. Run: make config"
    return 1
}

is_listening() {
    local port="$1"

    if command -v lsof >/dev/null 2>&1; then
        lsof -nP -iTCP:"$port" -sTCP:LISTEN -t >/dev/null 2>&1 && return 0
    fi

    if command -v ss >/dev/null 2>&1; then
        ss -ltn "( sport = :$port )" 2>/dev/null | tail -n +2 | grep -q . && return 0
    fi

    return 1
}

wait_port() {
    local port="$1"
    local timeout="$2"
    local name="$3"

    "$REPO_ROOT/scripts/wait-for-port.sh" "$port" "$timeout" "$name"
}

stop_services() {
    echo "Stopping no-nginx local services..."
    pkill -f "langgraph dev" 2>/dev/null || true
    pkill -f "uvicorn app.gateway.app:app" 2>/dev/null || true
    pkill -f "next dev" 2>/dev/null || true
    rm -rf "$PID_DIR" 2>/dev/null || true
    echo "✓ Services stopped"
}

show_status() {
    echo ""
    echo "=== Process Status ==="
    ps -ef | grep -E "langgraph dev|uvicorn app.gateway.app:app|next dev" | grep -v grep || true

    echo ""
    echo "=== Port Status ==="
    for p in "$LANGGRAPH_PORT" "$GATEWAY_PORT" "$FRONTEND_PORT" 3001; do
        if is_listening "$p"; then
            echo "port $p: open"
        else
            echo "port $p: closed"
        fi
    done

    if [ -f "$PID_DIR/frontend.port" ]; then
        echo ""
        echo "Frontend active port: $(cat "$PID_DIR/frontend.port")"
    fi
}

show_logs() {
    echo "=== logs/langgraph.log ==="
    tail -n 40 "$LOG_DIR/langgraph.log" 2>/dev/null || echo "(not found)"
    echo ""
    echo "=== logs/gateway.log ==="
    tail -n 40 "$LOG_DIR/gateway.log" 2>/dev/null || echo "(not found)"
    echo ""
    echo "=== logs/frontend.log ==="
    tail -n 40 "$LOG_DIR/frontend.log" 2>/dev/null || echo "(not found)"
}

start_services() {
    local uv_bin
    uv_bin="$(pick_uv_bin)"
    if [ -z "$uv_bin" ]; then
        echo "✗ uv not found. Install uv first."
        exit 1
    fi

    ensure_config_exists
    load_proxy_if_available

    export PATH="$(dirname "$uv_bin"):$PATH"

    stop_services

    mkdir -p "$LOG_DIR" "$PID_DIR"

    # Keep config aligned with latest schema when possible.
    "$REPO_ROOT/scripts/config-upgrade.sh" || true

    echo "Starting LangGraph on port $LANGGRAPH_PORT..."
    (
        cd "$REPO_ROOT/backend"
        NO_COLOR=1 "$uv_bin" run langgraph dev --host 127.0.0.1 --port "$LANGGRAPH_PORT" --no-browser --allow-blocking > "$LOG_DIR/langgraph.log" 2>&1
    ) &
    echo $! > "$PID_DIR/langgraph.pid"

    wait_port "$LANGGRAPH_PORT" 60 "LangGraph"
    echo "✓ LangGraph is up"

    echo "Starting Gateway on port $GATEWAY_PORT..."
    (
        cd "$REPO_ROOT/backend"
        PYTHONPATH=. "$uv_bin" run uvicorn app.gateway.app:app --host 0.0.0.0 --port "$GATEWAY_PORT" --reload --reload-include='*.yaml' --reload-include='.env' > "$LOG_DIR/gateway.log" 2>&1
    ) &
    echo $! > "$PID_DIR/gateway.pid"

    wait_port "$GATEWAY_PORT" 40 "Gateway"
    echo "✓ Gateway is up"

    if is_listening "$FRONTEND_PORT"; then
        echo "⚠ Frontend port $FRONTEND_PORT is occupied, fallback to 3001"
        FRONTEND_PORT=3001
    fi

    echo "Starting Frontend on port $FRONTEND_PORT..."
    (
        cd "$REPO_ROOT/frontend"
        NEXT_PUBLIC_BACKEND_BASE_URL="http://localhost:$GATEWAY_PORT" \
        NEXT_PUBLIC_LANGGRAPH_BASE_URL="http://localhost:$LANGGRAPH_PORT" \
        pnpm exec next dev --turbo --port "$FRONTEND_PORT" > "$LOG_DIR/frontend.log" 2>&1
    ) &
    echo $! > "$PID_DIR/frontend.pid"
    echo "$FRONTEND_PORT" > "$PID_DIR/frontend.port"

    wait_port "$FRONTEND_PORT" 120 "Frontend"
    echo "✓ Frontend is up"

    echo ""
    echo "=========================================="
    echo " DeerFlow (No Nginx) is running"
    echo "=========================================="
    echo "Frontend : http://localhost:$FRONTEND_PORT"
    echo "Gateway  : http://localhost:$GATEWAY_PORT/docs"
    echo "LangGraph: http://localhost:$LANGGRAPH_PORT/docs"
    echo "Logs     : $LOG_DIR"
}

CMD="${1:-start}"

case "$CMD" in
    start)
        start_services
        ;;
    stop)
        stop_services
        ;;
    restart)
        stop_services
        start_services
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs
        ;;
    -h|--help|help)
        usage
        ;;
    *)
        echo "Unknown command: $CMD"
        usage
        exit 1
        ;;
esac
