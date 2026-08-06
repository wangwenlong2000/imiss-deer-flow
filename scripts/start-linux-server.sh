#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$REPO_ROOT"

if [ ! -x "$REPO_ROOT/backend/.venv/bin/python" ]; then
    echo "backend virtualenv not found: $REPO_ROOT/backend/.venv"
    echo "Run: cd $REPO_ROOT/backend && uv sync"
    exit 1
fi

export DEER_FLOW_CONFIG_PATH="${DEER_FLOW_CONFIG_PATH:-$REPO_ROOT/config.yaml}"
export LANGGRAPH_PORT="${LANGGRAPH_PORT:-3024}"
export GATEWAY_PORT="${GATEWAY_PORT:-38001}"
export FRONTEND_PORT="${FRONTEND_PORT:-33000}"

echo "Starting DeerFlow on server ports:"
echo "  LangGraph: $LANGGRAPH_PORT"
echo "  Gateway  : $GATEWAY_PORT"
echo "  Frontend : $FRONTEND_PORT"
echo "  Config   : $DEER_FLOW_CONFIG_PATH"

"$REPO_ROOT/scripts/dev-no-nginx.sh" start
