#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PROJECT_ROOT="/workspace/imiss-deer-flow-main"
DATASET_ROOT="$PROJECT_ROOT/datasets/phone-network"
OUTPUT_DIR="/mnt/user-data/outputs"
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$OUTPUT_DIR" "$LOG_DIR"
LOG_FILE="$LOG_DIR/test_sichuan_shaanxi_comparison_$(date +%Y%m%d_%H%M%S).log"

exec > >(tee "$LOG_FILE") 2>&1

echo "===== TEST 1: default Sichuan vs Shaanxi comparison ====="
python3 sichuan_shaanxi_comparison_wrapper.py \
  --dataset-root "$DATASET_ROOT" \
  --dataset unified \
  --province-a sichuan \
  --province-b shaanxi \
  --top-k 10 \
  --output-dir "$OUTPUT_DIR"

echo "===== TEST 2: basic output checks ====="
python3 - <<'PY'
import json
from pathlib import Path
summary_path = Path('/mnt/user-data/outputs/sichuan_shaanxi_comparison_unified_summary.json')
assert summary_path.exists(), 'summary json not found'
data = json.loads(summary_path.read_text(encoding='utf-8'))
print('summary status=', data.get('status'), 'script_version=', data.get('script_version'))
assert data.get('ok') is True
assert data.get('status') == 'ok'
assert data.get('script_version') == 'sichuan-shaanxi-comparison-release-v1.3'
assert data.get('analysis_scope') == 'full_province_comparison_not_condition_screening'
assert 'time_comparability_note' in data and data['time_comparability_note']
for key in ['report_md', 'presentation_md', 'summary_json', 'evidence_xlsx']:
    path = Path(data['files'][key])
    assert path.exists(), f'{key} missing: {path}'
for key in ['metric_contrast_csv', 'top_objects_csv', 'device_summary_csv', 'call_behavior_csv']:
    path = Path(data['files'][key])
    assert path.exists(), f'{key} missing: {path}'
report = Path(data['files']['report_md']).read_text(encoding='utf-8')
presentation = Path(data['files']['presentation_md']).read_text(encoding='utf-8')
assert '全量地域对比' in report
assert '条件切片' in report
assert '时间可比性提醒' in report
assert '每活跃号码日均通话' in presentation
assert 'condition_screening' not in Path(data['files']['report_md']).name
print('[CHECK] required outputs exist and v1.3 report guards are present')
PY

echo "===== TEST 3: output file list ====="
ls -lh "$OUTPUT_DIR"/sichuan_shaanxi_comparison_unified* || true


echo "===== TEST 4: markdown-only artifact mode ====="
python3 sichuan_shaanxi_comparison_wrapper.py \
  --dataset-root /workspace/imiss-deer-flow-main/datasets/phone-network \
  --dataset unified \
  --province-a sichuan \
  --province-b shaanxi \
  --top-k 10 \
  --artifact-mode markdown_only > "$LOG_DIR/sichuan_shaanxi_comparison_markdown_only.json"
python3 - <<'PY'
import json
from pathlib import Path
p=Path('/workspace/imiss-deer-flow-main/logs/sichuan_shaanxi_comparison_markdown_only.json')
data=json.loads(p.read_text())
assert data.get('script_version') == 'sichuan-shaanxi-comparison-release-v1.3'
assert data.get('artifact_mode') == 'markdown_only'
arts=data.get('artifacts', [])
assert len(arts) == 2, arts
assert all(a.get('type') == 'markdown_report' for a in arts), arts
files=data.get('files', {})
assert set(files.keys()) == {'report_md','presentation_md'}, files
print('[CHECK] markdown_only exposes only Markdown artifacts')
PY

echo "[OK] sichuan-shaanxi-comparison tests finished. log=$LOG_FILE"
