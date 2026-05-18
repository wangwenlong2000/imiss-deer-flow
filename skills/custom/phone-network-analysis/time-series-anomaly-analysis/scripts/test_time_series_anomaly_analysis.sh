#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SKILL_ROOT="$(cd "$SCRIPT_DIR/.." 2>/dev/null && pwd || pwd)"
PROJECT_ROOT="$(cd "$SKILL_ROOT/../../../.." 2>/dev/null && pwd || pwd)"
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/test_time_series_anomaly_analysis_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee "$LOG_FILE") 2>&1

DATASET_ROOT="${PHONE_NETWORK_DATASET_ROOT:-/workspace/imiss-deer-flow-main/datasets/phone-network}"
DATASET="unified"
PHONE_ID='141ab86b0a1277138c664368f30bfd93878754a968ca4f0f6f9f4d1b2279328985781d0740742e523f43e705753c5b9fd2bec9752624c1b79cf2b1132f1915be'
NOT_FOUND_ID='48a530c83d8abca7c1f648c2c5f0c8eb4d0a85d11c5dc2e79ba5ae4d1aa53f50db5982ddff5df770b3874fda6daccf899dc5627c6d8221c5eb0a6e5e9a4c9b3d'

echo "===== TEST 1: phone mode ====="
python3 "$SCRIPT_DIR/time_series_anomaly_analysis_wrapper.py" \
  --mode phone \
  --phone-id "$PHONE_ID" \
  --dataset-root "$DATASET_ROOT" \
  --dataset "$DATASET" \
  --recent-days 7 \
  --baseline-days 30 \
  --top-k 10

echo "===== TEST 2: group mode ====="
python3 "$SCRIPT_DIR/time_series_anomaly_analysis_wrapper.py" \
  --mode group \
  --phone-id-file "$SCRIPT_DIR/sample_group_ids.txt" \
  --dataset-root "$DATASET_ROOT" \
  --dataset "$DATASET" \
  --recent-days 7 \
  --baseline-days 30 \
  --top-k 10

echo "===== TEST 3: deliberate not-found phone ====="
python3 "$SCRIPT_DIR/time_series_anomaly_analysis_wrapper.py" \
  --mode phone \
  --phone-id "$NOT_FOUND_ID" \
  --dataset-root "$DATASET_ROOT" \
  --dataset "$DATASET" \
  --top-k 10

echo "===== TEST 4: basic output checks ====="
python3 - <<'PY'
import json
from pathlib import Path
out = Path('/mnt/user-data/outputs')
phone_json = out / 'time_series_anomaly_141ab86b_phone_unified_summary.json'
group_json = out / 'time_series_anomaly_141ab86b_group_unified_summary.json'
missing_json = out / 'time_series_anomaly_48a530c8_phone_unified_summary.json'
for p in [phone_json, group_json, missing_json]:
    if not p.exists():
        raise SystemExit(f'missing expected summary: {p}')
    data = json.loads(p.read_text(encoding='utf-8'))
    print(p.name, 'status=', data.get('status'), 'script_version=', data.get('script_version'))
    if data.get('script_version') != 'time-series-anomaly-analysis-release-v1.4':
        raise SystemExit(f'wrong script version in {p}: {data.get("script_version")}')
if json.loads(phone_json.read_text(encoding='utf-8')).get('status') != 'ok':
    raise SystemExit('phone test did not return ok')
if json.loads(group_json.read_text(encoding='utf-8')).get('status') != 'ok':
    raise SystemExit('group test did not return ok')
missing = json.loads(missing_json.read_text(encoding='utf-8'))
if missing.get('status') != 'target_not_found':
    raise SystemExit('not-found test did not return target_not_found')
if len(missing.get('artifacts', [])) > 2:
    raise SystemExit('not-found test should only expose md/json artifacts')
print('[CHECK] summaries validated')
PY

ls -1 /mnt/user-data/outputs/time_series_anomaly_*.md 2>/dev/null || true
ls -1 /workspace/imiss-deer-flow-main/outputs/time_series_anomaly_*.md 2>/dev/null || true

echo "[OK] time-series-anomaly-analysis tests finished. log=$LOG_FILE"
