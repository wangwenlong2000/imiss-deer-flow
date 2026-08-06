#!/usr/bin/env bash
set -euo pipefail

if [ -f "./overlap_analysis_wrapper.py" ]; then
  cd .
elif [ -f "/mnt/skills/custom/phone-network-analysis/overlap-analysis/scripts/overlap_analysis_wrapper.py" ]; then
  cd /mnt/skills/custom/phone-network-analysis/overlap-analysis/scripts
elif [ -f "/workspace/imiss-deer-flow-main/skills/custom/phone-network-analysis/overlap-analysis/scripts/overlap_analysis_wrapper.py" ]; then
  cd /workspace/imiss-deer-flow-main/skills/custom/phone-network-analysis/overlap-analysis/scripts
else
  echo "ERROR: cannot locate overlap_analysis_wrapper.py"
  exit 1
fi

python3 overlap_analysis_wrapper.py \
  --phone-a 'd1beac94365462bd76de70f17c025864646a237800c4ac8a47cc724d63f04353b2ef3e1ff90a9677422bdc30da2a788a7fc9fd571d9146c0a1a4c2de49bfb12b' \
  --phone-b 'bdece8ac3e7d68e8dd24c70cd87bdeddcc0be03123f001080219b52cd77ce33695964de4686d4b189d72b5c29d325b640f27a83131e9bfb599349f080d225dc8' \
  --top-k 10 \
  --min-common-counterparty 1