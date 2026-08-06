#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

ROOT_DIR="$(cd "$SCRIPT_DIR/../../../../.." && pwd)"
LOG_DIR="$ROOT_DIR/logs/gang-cluster-analysis"
mkdir -p "$LOG_DIR"
TS="$(date +%Y%m%d_%H%M%S)"

PHONE_FILE="$SCRIPT_DIR/sample_group_ids.txt"

echo "===== TEST 1: mixed cluster discovery ====="
python3 gang_cluster_analysis_wrapper.py \
  --group-name sample_gang \
  --phone-id-file "$PHONE_FILE" \
  --candidate-scope mixed \
  --max-expand-nodes 120 \
  --top-k 10 \
  --min-shared-device-count 1 \
  --min-common-counterparty-count 2 | tee "$LOG_DIR/mixed_${TS}.log"

echo "===== TEST 2: shared-device focused ====="
python3 gang_cluster_analysis_wrapper.py \
  --group-name sample_gang_device \
  --phone-id-file "$PHONE_FILE" \
  --candidate-scope shared_device \
  --max-expand-nodes 100 \
  --top-k 10 \
  --min-shared-device-count 1 | tee "$LOG_DIR/device_${TS}.log"

echo "===== TEST 3: common-counterparty focused ====="
python3 gang_cluster_analysis_wrapper.py \
  --group-name sample_gang_cp \
  --phone-id-file "$PHONE_FILE" \
  --candidate-scope common_counterparty \
  --max-expand-nodes 100 \
  --top-k 10 \
  --min-common-counterparty-count 2 | tee "$LOG_DIR/common_counterparty_${TS}.log"

echo "===== TEST 4: output existence ====="
ls -1 /mnt/user-data/outputs/gang_cluster_report_*.md 2>/dev/null || true
ls -1 /mnt/user-data/outputs/gang_cluster_report_*.csv 2>/dev/null || true
ls -1 /mnt/user-data/outputs/gang_cluster_report_*_evidence.xlsx 2>/dev/null || true

echo "Logs saved to: $LOG_DIR"
