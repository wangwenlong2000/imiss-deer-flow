#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/workspace/imiss-deer-flow-main}"
SCRIPT_DIR="$PROJECT_ROOT/skills/custom/phone-network-analysis/shared-device-analysis/scripts"
LOG_DIR="$PROJECT_ROOT/logs/shared-device-analysis"
mkdir -p "$LOG_DIR"
cd "$SCRIPT_DIR"

TS="$(date +%Y%m%d_%H%M%S)"
PHONE_A='d1beac94365462bd76de70f17c025864646a237800c4ac8a47cc724d63f04353b2ef3e1ff90a9677422bdc30da2a788a7fc9fd571d9146c0a1a4c2de49bfb12b'
PHONE_B='bdece8ac3e7d68e8dd24c70cd87bdeddcc0be03123f001080219b52cd77ce33695964de4686d4b189d72b5c29d325b640f27a83131e9bfb599349f080d225dc8'
PHONE_ID='141ab86b0a1277138c664368f30bfd93878754a968ca4f0f6f9f4d1b2279328985781d0740742e523f43e705753c5b9fd2bec9752624c1b79cf2b1132f1915be'
DEVICE_ID='2f861efd76303aa726394003ef4064bf8edd7d3dd42a1c57c50b187f017ebe48765c912801c3b6eeb1df37b4dab0207c2c5d82bb027acc9b1a2f606dfa7ac303'

python3 shared_device_analysis_wrapper.py \
  --mode pair \
  --phone-a "$PHONE_A" \
  --phone-b "$PHONE_B" \
  --top-k 10 \
  --min-shared-phone 1 \
  --min-device-phone-count 2 \
  > "$LOG_DIR/pair_${TS}.log" 2>&1

echo "pair log => $LOG_DIR/pair_${TS}.log"

python3 shared_device_analysis_wrapper.py \
  --mode phone \
  --phone-id "$PHONE_ID" \
  --top-k 10 \
  --min-shared-phone 1 \
  --min-device-phone-count 2 \
  > "$LOG_DIR/phone_${TS}.log" 2>&1

echo "phone log => $LOG_DIR/phone_${TS}.log"

python3 shared_device_analysis_wrapper.py \
  --mode device \
  --device-id "$DEVICE_ID" \
  --top-k 20 \
  --min-device-phone-count 2 \
  > "$LOG_DIR/device_${TS}.log" 2>&1

echo "device log => $LOG_DIR/device_${TS}.log"

printf '\nDone. Use these commands to inspect logs:\n'
printf '  tail -n 80 %s/pair_%s.log\n' "$LOG_DIR" "$TS"
printf '  tail -n 80 %s/phone_%s.log\n' "$LOG_DIR" "$TS"
printf '  tail -n 80 %s/device_%s.log\n' "$LOG_DIR" "$TS"
