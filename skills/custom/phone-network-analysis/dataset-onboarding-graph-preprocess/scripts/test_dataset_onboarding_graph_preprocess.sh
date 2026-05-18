#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

TS="$(date +%Y%m%d_%H%M%S)"
LOG_DIR="/workspace/imiss-deer-flow-main/logs"
mkdir -p "$LOG_DIR" /mnt/user-data/outputs
LOG_PATH="$LOG_DIR/test_dataset_onboarding_graph_preprocess_${TS}.log"
exec > >(tee "$LOG_PATH") 2>&1

INPUT_DIR="/tmp/phone_network_onboarding_sample_input"
OUTPUT_ROOT="/tmp/phone_network_onboarding_output"
MESSY_DIR="/tmp/phone_network_onboarding_messy_input"
EDGE_DIR="/tmp/phone_network_onboarding_edge_input"
BAD_DIR="/tmp/phone_network_onboarding_bad_input"
rm -rf "$INPUT_DIR" "$OUTPUT_ROOT" "$MESSY_DIR" "$EDGE_DIR" "$BAD_DIR"
mkdir -p "$INPUT_DIR" "$MESSY_DIR" "$EDGE_DIR" "$BAD_DIR"

echo "===== TEST 1: generate clean synthetic raw data ====="
python3 generate_sample_phone_raw_data.py --output-dir "$INPUT_DIR"
ls -lh "$INPUT_DIR"

echo "===== TEST 2: run clean graph preprocess ====="
python3 dataset_onboarding_graph_preprocess_wrapper.py \
  --input-dir "$INPUT_DIR" \
  --dataset-root "$OUTPUT_ROOT" \
  --dataset onboarded_demo \
  --dataset-name phone-network-onboarded-demo \
  --province test \
  --hash-salt test_salt \
  --hash-length 64 \
  --overwrite \
  --artifact-mode essential

echo "===== TEST 3: verify clean standard outputs ====="
test -f "$OUTPUT_ROOT/processed/onboarded_demo/user_nodes.csv"
test -f "$OUTPUT_ROOT/processed/onboarded_demo/call_edges.csv"
test -f "$OUTPUT_ROOT/processed/graph_views/onboarded_demo/edges_phone_imei.parquet"
test -f "$OUTPUT_ROOT/processed/onboarded_demo/preprocess_summary.json"

python3 - <<'PY'
import json
from pathlib import Path
p = Path('/tmp/phone_network_onboarding_output/processed/onboarded_demo/preprocess_summary.json')
d = json.loads(p.read_text(encoding='utf-8'))
assert d['ok'] is True, d
assert d['graph_ready'] is True, d
assert d['script_version'].endswith('v1.3'), d['script_version']
assert d['summary']['user_nodes'] > 0
assert d['summary']['call_edges'] > 0
assert d['summary']['device_edges'] > 0
# essential should expose report + three standard graph files only.
assert len(d.get('artifacts', [])) == 4, d.get('artifacts')
print('clean summary ok:', d['summary'])
PY

echo "===== TEST 4: run with explicit columns and markdown_only artifact mode ====="
python3 dataset_onboarding_graph_preprocess_wrapper.py \
  --input-file "$INPUT_DIR/raw_call_records.csv" \
  --dataset-root "$OUTPUT_ROOT" \
  --dataset onboarded_demo_explicit \
  --source-col caller \
  --target-col callee \
  --device-col imei \
  --time-col call_time \
  --duration-col duration_sec \
  --province-col province \
  --hash-salt test_salt \
  --hash-length 64 \
  --overwrite \
  --artifact-mode markdown_only

python3 - <<'PY'
import json
from pathlib import Path
p = Path('/tmp/phone_network_onboarding_output/processed/onboarded_demo_explicit/preprocess_summary.json')
d = json.loads(p.read_text(encoding='utf-8'))
assert len(d.get('artifacts', [])) == 1, d.get('artifacts')
print('markdown_only artifact check ok')
PY

echo "===== TEST 5: generate messy multilingual raw data ====="
python3 - <<'PY'
import json
from pathlib import Path
import pandas as pd
out = Path('/tmp/phone_network_onboarding_messy_input')
out.mkdir(parents=True, exist_ok=True)
rows = [
    {'主叫':'13800000001','被叫':'13990000001','通话时间':'2024-01-01 08:12:00','通话时长':'60','设备号':'IMEI_A','省份':'sichuan','市':'chengdu','呼叫类型':'out','备注':''},
    {'主叫':'13800000002','被叫':'13990000001','通话时间':'2024-01-01 23:59:00','通话时长':'120','设备号':'IMEI_A','省份':'sichuan','市':'chengdu','呼叫类型':'in','备注':''},
    {'主叫':'13800000003','被叫':'13990000002','通话时间':'bad_time_value','通话时长':'bad_duration','设备号':'IMEI_B','省份':'shaanxi','市':'xian','呼叫类型':'out','备注':'bad time'},
    {'主叫':'','被叫':'13990000003','通话时间':'2024-01-02 10:00:00','通话时长':'30','设备号':'','省份':'sichuan','市':'chengdu','呼叫类型':'out','备注':'missing phone'},
    {'主叫':'13800000001','被叫':'13990000001','通话时间':'2024-01-01 08:12:00','通话时长':'60','设备号':'IMEI_A','省份':'sichuan','市':'chengdu','呼叫类型':'out','备注':'duplicate'},
]
pd.DataFrame(rows).to_csv(out/'通话明细_混合问题.csv', index=False, encoding='utf-8-sig')
pd.DataFrame([
    {'用户号码':'13800000001','终端':'IMEI_A','归属地省':'sichuan','绑定来源':'synthetic'},
    {'用户号码':'13800000002','终端':'IMEI_A','归属地省':'sichuan','绑定来源':'synthetic'},
    {'用户号码':'13800000003','终端':'IMEI_B','归属地省':'shaanxi','绑定来源':'synthetic'},
    {'用户号码':'','终端':'','归属地省':'sichuan','绑定来源':'bad'},
]).to_csv(out/'设备绑定_有缺失.csv', index=False, encoding='utf-8-sig')
labels = [
    {'手机号':'13800000001','归属省':'sichuan','风险标签':'风险','风险类型':'risk','年龄':31,'开卡时间':'2023-01-01'},
    {'手机号':'13800000002','归属省':'sichuan','风险标签':'正常','风险类型':'normal','年龄':22,'开卡时间':'2023-02-01'},
    {'手机号':'13800000003','归属省':'shaanxi','风险标签':'风险','风险类型':'risk','年龄':45,'开卡时间':'2023-03-01'},
]
(out/'用户标签_中文字段.json').write_text(json.dumps(labels, ensure_ascii=False, indent=2), encoding='utf-8')
print('messy data written:', out)
PY
ls -lh "$MESSY_DIR"

echo "===== TEST 6: run messy graph preprocess ====="
python3 dataset_onboarding_graph_preprocess_wrapper.py \
  --input-dir "$MESSY_DIR" \
  --dataset-root "$OUTPUT_ROOT" \
  --dataset onboarded_messy_demo \
  --dataset-name phone-network-onboarded-messy-demo \
  --province unknown \
  --hash-salt test_salt \
  --hash-length 64 \
  --overwrite \
  --artifact-mode essential

echo "===== TEST 7: verify messy output and warnings ====="
python3 - <<'PY'
import json
from pathlib import Path
p = Path('/tmp/phone_network_onboarding_output/processed/onboarded_messy_demo/preprocess_summary.json')
d = json.loads(p.read_text(encoding='utf-8'))
assert d['ok'] is True, d
assert d['graph_ready'] is True, d
assert d['summary']['user_nodes'] >= 3, d['summary']
assert d['summary']['call_edges'] >= 3, d['summary']
assert d['summary']['device_edges'] >= 2, d['summary']
assert d['summary']['warnings_count'] >= 1, d['summary']
print('messy summary ok:', d['summary'])
PY

echo "===== TEST 8: bad input without phone column should generate diagnostic report ====="
python3 - <<'PY'
from pathlib import Path
import pandas as pd
out = Path('/tmp/phone_network_onboarding_bad_input')
out.mkdir(parents=True, exist_ok=True)
pd.DataFrame([
    {'姓名':'张三','金额':'100','备注':'没有号码字段'},
    {'姓名':'李四','金额':'200','备注':'仍然没有号码字段'},
]).to_csv(out/'bad_missing_phone.csv', index=False, encoding='utf-8-sig')
print('bad input written:', out)
PY
python3 dataset_onboarding_graph_preprocess_wrapper.py \
  --input-dir "$BAD_DIR" \
  --dataset-root "$OUTPUT_ROOT" \
  --dataset onboarded_bad_demo \
  --dataset-name phone-network-onboarded-bad-demo \
  --province unknown \
  --hash-salt test_salt \
  --hash-length 64 \
  --overwrite \
  --artifact-mode markdown_only

python3 - <<'PY'
import json
from pathlib import Path
p = Path('/tmp/phone_network_onboarding_output/processed/onboarded_bad_demo/preprocess_summary.json')
d = json.loads(p.read_text(encoding='utf-8'))
assert d['ok'] is True, d
assert d['graph_ready'] is False, d
assert d['status'] == 'not_graph_ready_missing_phone_column', d['status']
assert d['summary']['user_nodes'] == 0, d['summary']
assert len(d.get('artifacts', [])) == 1, d.get('artifacts')
print('bad input diagnostic ok:', d['status_zh'])
PY

echo "===== TEST 9: no readable input path should generate diagnostic JSON instead of crashing ====="
python3 dataset_onboarding_graph_preprocess_wrapper.py \
  --input-dir /tmp/this_input_dir_does_not_exist_for_onboarding_test \
  --dataset-root "$OUTPUT_ROOT" \
  --dataset onboarded_no_input_demo \
  --dataset-name phone-network-onboarded-no-input-demo \
  --overwrite \
  --artifact-mode markdown_only

python3 - <<'PY'
import json
from pathlib import Path
p = Path('/tmp/phone_network_onboarding_output/processed/onboarded_no_input_demo/preprocess_summary.json')
d = json.loads(p.read_text(encoding='utf-8'))
assert d['ok'] is True, d
assert d['graph_ready'] is False, d
assert d['status'] == 'not_graph_ready_no_readable_input_files', d['status']
assert len(d.get('artifacts', [])) == 1, d.get('artifacts')
print('no input diagnostic ok:', d['status_zh'])
PY

echo "===== TEST 10: formal processing example with an existing input directory ====="
FORMAL_DIR="/tmp/phone_network_onboarding_formal_input"
rm -rf "$FORMAL_DIR"
mkdir -p "$FORMAL_DIR"
cp "$INPUT_DIR"/*.csv "$FORMAL_DIR"/
python3 dataset_onboarding_graph_preprocess_wrapper.py \
  --input-dir "$FORMAL_DIR" \
  --dataset-root "$OUTPUT_ROOT" \
  --dataset onboarded_formal_demo \
  --dataset-name phone-network-onboarded-formal-demo \
  --province test \
  --hash-salt test_salt \
  --hash-length 64 \
  --overwrite \
  --artifact-mode markdown_only

python3 - <<'PY'
import json
from pathlib import Path
p = Path('/tmp/phone_network_onboarding_output/processed/onboarded_formal_demo/preprocess_summary.json')
d = json.loads(p.read_text(encoding='utf-8'))
assert d['ok'] is True and d['graph_ready'] is True, d
assert d['summary']['user_nodes'] > 0 and d['summary']['call_edges'] > 0, d['summary']
print('formal processing example ok:', d['summary'])
PY

echo "[OK] dataset-onboarding-graph-preprocess tests finished"
echo "log saved to: $LOG_PATH"
