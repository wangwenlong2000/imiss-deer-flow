#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="/workspace/imiss-deer-flow-main"
DATASET_ROOT="${REPO_ROOT}/datasets/phone-network"
OUTPUT_DIR="/mnt/user-data/outputs"
LOG_DIR="${REPO_ROOT}/logs"
mkdir -p "${OUTPUT_DIR}" "${LOG_DIR}"

TS="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="${LOG_DIR}/test_dataset_overview_analysis_${TS}.log"

cd "${SCRIPT_DIR}"

echo "===== TEST 1: default unified dataset overview =====" | tee -a "${LOG_FILE}"
python3 dataset_overview_wrapper.py \
  --dataset-root "${DATASET_ROOT}" \
  --dataset unified \
  --top-k 10 | tee -a "${LOG_FILE}"

echo "===== TEST 2: basic output checks =====" | tee -a "${LOG_FILE}"
python3 - <<'PY' | tee -a "${LOG_FILE}"
import json
from pathlib import Path
summary_path = Path('/mnt/user-data/outputs/dataset_overview_unified_summary.json')
if not summary_path.exists():
    raise SystemExit('summary json missing')
summary = json.loads(summary_path.read_text(encoding='utf-8'))
print('summary status=', summary.get('status'), 'script_version=', summary.get('script_version'))
if not summary.get('ok'):
    raise SystemExit('summary ok=false')
if summary.get('script_version') != 'dataset-overview-analysis-release-v1.1':
    raise SystemExit('unexpected script_version: ' + str(summary.get('script_version')))
cap = (summary.get('result') or {}).get('capability_summary') or {}
if int(cap.get('available_count') or 0) < 10:
    raise SystemExit('available capability count too low')
required = [
    '/mnt/user-data/outputs/dataset_overview_unified.md',
    '/mnt/user-data/outputs/dataset_overview_unified_presentation.md',
    '/mnt/user-data/outputs/dataset_overview_unified_overview_counts.csv',
    '/mnt/user-data/outputs/dataset_overview_unified_available_capabilities.csv',
    '/mnt/user-data/outputs/dataset_overview_unified_summary.json',
    '/mnt/user-data/outputs/dataset_overview_unified_evidence.xlsx',
]
for p in required:
    if not Path(p).exists():
        raise SystemExit(f'missing output: {p}')
text = Path('/mnt/user-data/outputs/dataset_overview_unified_presentation.md').read_text(encoding='utf-8')
if '数据质量评分' in text:
    raise SystemExit('presentation report still contains subjective quality score')
if '日历跨度' not in text:
    raise SystemExit('presentation report missing calendar span explanation')
print('[CHECK] required outputs exist and presentation wording is clean')
PY

echo "===== TEST 3: output file list =====" | tee -a "${LOG_FILE}"
ls -l /mnt/user-data/outputs/dataset_overview_unified* | tee -a "${LOG_FILE}"

echo "[OK] dataset-overview-analysis tests finished. log=${LOG_FILE}" | tee -a "${LOG_FILE}"
