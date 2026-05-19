#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

PATTERNS=("serve_bge_m3.py" "serve_skillrouter_embedding.py" "serve_skillrouter_reranker.py")

for pat in "${PATTERNS[@]}"; do
    # list matching pids, ignore the current script and its parent
    for pid in $(pgrep -af "$pat" 2>/dev/null | awk '{print $1}' || true); do
        if [ -z "$pid" ]; then
            continue
        fi
        if [ "$pid" -eq "$$" ] || [ "$pid" -eq "$PPID" ]; then
            continue
        fi
        kill "$pid" 2>/dev/null || true
        echo "killed $pid for pattern $pat"
    done
done

echo "✓ Model services stop attempted"
