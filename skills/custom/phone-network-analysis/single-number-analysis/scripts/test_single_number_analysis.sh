#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PHONE_ID="${1:-141ab86b0a1277138c664368f30bfd93878754a968ca4f0f6f9f4d1b2279328985781d0740742e523f43e705753c5b9fd2bec9752624c1b79cf2b1132f1915be}"

printf '\n===== TEST 1: mixed =====\n'
python3 single_number_analysis_wrapper.py \
  --phone-id "$PHONE_ID" \
  --hops 2 \
  --max-nodes 200 \
  --top-k 10 \
  --analysis-mode mixed

printf '\n===== TEST 2: call_only + directed_call =====\n'
python3 single_number_analysis_wrapper.py \
  --phone-id "$PHONE_ID" \
  --hops 2 \
  --max-nodes 200 \
  --top-k 10 \
  --analysis-mode call_only \
  --directed-call

printf '\n===== TEST 3: device_only =====\n'
python3 single_number_analysis_wrapper.py \
  --phone-id "$PHONE_ID" \
  --hops 2 \
  --max-nodes 200 \
  --top-k 10 \
  --analysis-mode device_only

printf '\n===== TEST 4: report existence =====\n'
ls -l /mnt/user-data/outputs/single_number_report_*.md 2>/dev/null || true
