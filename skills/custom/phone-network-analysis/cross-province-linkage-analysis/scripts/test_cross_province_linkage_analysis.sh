#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

ROOT="/workspace/imiss-deer-flow-main"
DATASET_ROOT="$ROOT/datasets/phone-network"
OUT_DIR="/mnt/user-data/outputs"
LOG_DIR="$ROOT/logs"
mkdir -p "$OUT_DIR" "$LOG_DIR"
TS="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$LOG_DIR/test_cross_province_linkage_analysis_${TS}.log"

run_and_log() {
  "$@" 2>&1 | tee -a "$LOG_FILE"
}

echo "===== TEST 1: full cross-province linkage analysis =====" | tee -a "$LOG_FILE"
run_and_log python3 cross_province_linkage_wrapper.py \
  --dataset-root "$DATASET_ROOT" \
  --dataset unified \
  --province-a sichuan \
  --province-b shaanxi \
  --top-k 10 \
  --artifact-mode full

echo "===== TEST 2: summary and report checks =====" | tee -a "$LOG_FILE"
python3 - <<'PY' | tee -a "$LOG_FILE"
import json
from pathlib import Path
out = Path('/mnt/user-data/outputs')
summary_path = out / 'cross_province_linkage_unified_summary.json'
report_path = out / 'cross_province_linkage_unified.md'
presentation_path = out / 'cross_province_linkage_unified_presentation.md'
assert summary_path.exists(), summary_path
assert report_path.exists(), report_path
assert presentation_path.exists(), presentation_path
summary = json.loads(summary_path.read_text(encoding='utf-8'))
print('summary status=', summary.get('status'), 'script_version=', summary.get('script_version'))
assert summary.get('ok') is True
assert summary.get('status') == 'ok'
assert summary.get('script_version') == 'cross-province-linkage-analysis-release-v1.0'
assert summary.get('analysis_scope') == 'cross_province_linkage_not_region_comparison'
result = summary.get('result', {})
assert 'linkage_summary' in result
assert 'top_cross_shared_devices' in result
assert 'top_cross_common_counterparties' in result
report = report_path.read_text(encoding='utf-8')
presentation = presentation_path.read_text(encoding='utf-8')
assert '跨省联动分析报告' in report
assert '跨省共享设备' in report
assert '跨省共同对端' in report
assert '跨省关联线索' in report
assert '不等于案件定性' in report or '不是案件定性' in report
assert '跨省联动研判摘要' in presentation
print('[CHECK] required outputs exist and v1.0 report guards are present')
PY

echo "===== TEST 3: artifact markdown_only mode =====" | tee -a "$LOG_FILE"
python3 cross_province_linkage_wrapper.py \
  --dataset-root "$DATASET_ROOT" \
  --dataset unified \
  --province-a sichuan \
  --province-b shaanxi \
  --top-k 5 \
  --artifact-mode markdown_only > /tmp/cross_province_markdown_only.json
python3 - <<'PY' | tee -a "$LOG_FILE"
import json
from pathlib import Path
payload = json.loads(Path('/tmp/cross_province_markdown_only.json').read_text(encoding='utf-8'))
arts = payload.get('artifacts', [])
print('markdown_only artifacts=', [a.get('title') for a in arts])
assert len(arts) == 2
assert all(str(a.get('title', '')).endswith('.md') for a in arts)
print('[CHECK] markdown_only exposes only Markdown artifacts')
PY

echo "===== TEST 4: output file list =====" | tee -a "$LOG_FILE"
ls -lh "$OUT_DIR"/cross_province_linkage_unified* | tee -a "$LOG_FILE"

echo "[OK] cross-province-linkage-analysis tests finished. log=$LOG_FILE" | tee -a "$LOG_FILE"
