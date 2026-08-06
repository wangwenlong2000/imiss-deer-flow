#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$REPO_ROOT"

export LANGGRAPH_PORT="${LANGGRAPH_PORT:-3024}"
export GATEWAY_PORT="${GATEWAY_PORT:-38001}"
export FRONTEND_PORT="${FRONTEND_PORT:-33000}"

"$REPO_ROOT/scripts/dev-no-nginx.sh" stop
