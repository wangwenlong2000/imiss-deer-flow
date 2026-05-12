#!/usr/bin/env bash

set -euo pipefail

check_port() {
    local port="$1"
    local name="$2"

    if curl -sf "http://127.0.0.1:$port/health" >/dev/null; then
        echo "✓ $name is online on port $port"
    else
        echo "✗ $name is offline on port $port"
    fi
}

check_port 7799 "BGE-M3"
check_port 7800 "SkillRouter Embedding"
check_port 7801 "SkillRouter Reranker"