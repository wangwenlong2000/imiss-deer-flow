#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../../.." && pwd)"
LOG_DIR="$REPO_ROOT/logs/topn-high-risk-discovery"
mkdir -p "$LOG_DIR"

run_case() {
  local name="$1"
  shift
  local ts
  ts="$(date +%Y%m%d_%H%M%S)"
  local log_file="$LOG_DIR/${name}_${ts}.log"
  echo "[RUN] $name"
  echo "[LOG] $log_file"
  {
    echo "===== CASE: $name ====="
    echo "===== TIME: $(date '+%F %T') ====="
    echo "===== CMD ====="
    printf '%q ' "$@"
    echo
    echo "===== OUTPUT ====="
    "$@"
  } > "$log_file" 2>&1
  echo "[DONE] $name"
}

cd "$SCRIPT_DIR"

run_case q1_all_views \
  python3 topn_high_risk_discovery_wrapper.py \
    --top-n 20 \
    --discovery-top-n 10 \
    --analysis-mode mixed \
    --ranking-view all_views \
    --candidate-scope all

run_case q2_unlabeled \
  python3 topn_high_risk_discovery_wrapper.py \
    --top-n 10 \
    --discovery-top-n 10 \
    --analysis-mode mixed \
    --ranking-view unlabeled_only \
    --candidate-scope unlabeled_only

run_case q3_device_priority \
  python3 topn_high_risk_discovery_wrapper.py \
    --top-n 10 \
    --discovery-top-n 10 \
    --analysis-mode device_only \
    --ranking-view device_priority \
    --candidate-scope all \
    --min-shared-device-count 1

echo "===== RECENT LOG FILES ====="
ls -lt "$LOG_DIR" | head -20 || true

echo "===== GENERATED REPORT FILES ====="
ls -lt /mnt/user-data/outputs/topn_high_risk_*.md 2>/dev/null | head -20 || true
ls -lt /mnt/user-data/outputs/topn_high_risk_*.csv 2>/dev/null | head -20 || true
