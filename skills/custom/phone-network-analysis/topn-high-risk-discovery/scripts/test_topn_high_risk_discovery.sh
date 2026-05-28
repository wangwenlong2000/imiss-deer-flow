#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo '===== TEST 1: all_views mixed ====='
python3 topn_high_risk_discovery_wrapper.py \
  --top-n 20 \
  --discovery-top-n 10 \
  --analysis-mode mixed \
  --ranking-view all_views \
  --candidate-scope all

echo '===== TEST 2: device_priority ====='
python3 topn_high_risk_discovery_wrapper.py \
  --top-n 15 \
  --discovery-top-n 15 \
  --analysis-mode device_only \
  --ranking-view device_priority \
  --candidate-scope all \
  --min-shared-device-count 1

echo '===== TEST 3: unlabeled_only ====='
python3 topn_high_risk_discovery_wrapper.py \
  --top-n 20 \
  --discovery-top-n 10 \
  --analysis-mode mixed \
  --ranking-view unlabeled_only \
  --candidate-scope unlabeled_only

echo '===== TEST 4: province=sichuan ====='
python3 topn_high_risk_discovery_wrapper.py \
  --top-n 30 \
  --discovery-top-n 10 \
  --analysis-mode mixed \
  --ranking-view all_views \
  --candidate-scope all \
  --province sichuan

echo '===== TEST 5: output files ====='
ls -1 /mnt/user-data/outputs/topn_high_risk_*.md 2>/dev/null || true
ls -1 /mnt/user-data/outputs/topn_high_risk_*.csv 2>/dev/null || true
