#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

TS="$(date +%Y%m%d_%H%M%S)"
LOG_DIR="/workspace/imiss-deer-flow-main/logs"
mkdir -p "$LOG_DIR" /mnt/user-data/outputs
LOG_PATH="$LOG_DIR/test_dataset_quality_linkability_diagnostic_${TS}.log"
exec > >(tee "$LOG_PATH") 2>&1

ROOT="/tmp/phone_network_dataset_quality_test_root"
DATASET="diagnostic_demo"
BAD_DATASET="diagnostic_partial"
rm -rf "$ROOT"
mkdir -p "$ROOT/processed/$DATASET" "$ROOT/processed/graph_views/$DATASET" "$ROOT/processed/$BAD_DATASET" "$ROOT/processed/graph_views/$BAD_DATASET"


echo "===== TEST 1: build synthetic processed graph dataset ====="
python3 - <<'PY'
from pathlib import Path
import pandas as pd
root = Path('/tmp/phone_network_dataset_quality_test_root')
ds = root / 'processed' / 'diagnostic_demo'
gv = root / 'processed' / 'graph_views' / 'diagnostic_demo'
ds.mkdir(parents=True, exist_ok=True)
gv.mkdir(parents=True, exist_ok=True)
users = []
for p, prov, label in [
    ('u_s_001','sichuan',1), ('u_s_002','sichuan',0), ('u_s_003','sichuan',1),
    ('u_x_001','shaanxi',1), ('u_x_002','shaanxi',0), ('u_x_003','shaanxi',1),
]:
    users.append({'user_id': p, 'province': prov, 'label': label, 'sub_label': 'risk' if label else 'normal', 'dataset_name': 'synthetic'})
pd.DataFrame(users).to_csv(ds/'user_nodes.csv', index=False, encoding='utf-8-sig')
calls = [
    {'province':'sichuan','src_user_id':'u_s_001','dst_counterparty_id':'cp_common_1','event_time':'2024-01-01 01:00:00','duration':50},
    {'province':'sichuan','src_user_id':'u_s_002','dst_counterparty_id':'cp_s_only','event_time':'2024-01-01 10:00:00','duration':30},
    {'province':'shaanxi','src_user_id':'u_x_001','dst_counterparty_id':'cp_common_1','event_time':'2024-01-02 02:00:00','duration':80},
    {'province':'shaanxi','src_user_id':'u_x_002','dst_counterparty_id':'cp_x_only','event_time':'2024-01-02 11:00:00','duration':20},
    {'province':'sichuan','src_user_id':'u_s_003','dst_counterparty_id':'u_x_003','event_time':'2024-01-03 13:00:00','duration':60},
]
pd.DataFrame(calls).to_csv(ds/'call_edges.csv', index=False, encoding='utf-8-sig')
dev = pd.DataFrame([
    {'user_id':'u_s_001','imei':'dev_common','edge_count':2},
    {'user_id':'u_s_002','imei':'dev_s_only','edge_count':1},
    {'user_id':'u_x_001','imei':'dev_common','edge_count':2},
    {'user_id':'u_x_002','imei':'dev_x_only','edge_count':1},
])
try:
    dev.to_parquet(gv/'edges_phone_imei.parquet', index=False)
except Exception:
    dev.to_csv(gv/'edges_phone_imei.csv', index=False, encoding='utf-8-sig')
# partial dataset: user nodes only
partial = root / 'processed' / 'diagnostic_partial'
pd.DataFrame([{'user_id':'p1','province':'sichuan','label':0}]).to_csv(partial/'user_nodes.csv', index=False, encoding='utf-8-sig')
print('synthetic processed datasets written:', root)
PY


echo "===== TEST 2: run diagnostic on linkable dataset ====="
python3 dataset_quality_linkability_diagnostic_wrapper.py \
  --dataset-root "$ROOT" \
  --dataset "$DATASET" \
  --province-a sichuan \
  --province-b shaanxi \
  --top-k 5 \
  --artifact-mode essential

python3 - <<'PY'
import json
from pathlib import Path
p = Path('/mnt/user-data/outputs/dataset_quality_linkability_diagnostic_demo_summary.json')
d = json.loads(p.read_text(encoding='utf-8'))
assert d['script_version'].endswith('v1.2'), d['script_version']
assert d['graph_ready'] is True, d
assert d['linkability']['linkability_score'] >= 2, d['linkability']
assert any(x['skill']=='cross-province-linkage-analysis' and x['support_status']=='supported' for x in d['capability_matrix'])
assert 'downstream_command_templates' in d and len(d['downstream_command_templates']) >= 4, d.get('downstream_command_templates')
assert any(x['skill']=='topn-high-risk-discovery' and '--user-node-path' in x['command'] for x in d['downstream_command_templates'])
print('linkable diagnostic ok:', d['linkability']['linkability_level'])
PY


echo "===== TEST 3: run diagnostic on partial graph dataset ====="
python3 dataset_quality_linkability_diagnostic_wrapper.py \
  --dataset-root "$ROOT" \
  --dataset "$BAD_DATASET" \
  --artifact-mode markdown_only

python3 - <<'PY'
import json
from pathlib import Path
p = Path('/mnt/user-data/outputs/dataset_quality_linkability_diagnostic_partial_summary.json')
d = json.loads(p.read_text(encoding='utf-8'))
assert d['ok'] is True, d
assert d['graph_ready'] is False, d
assert d['status'] == 'partial_graph_only_user_nodes', d['status']
assert len(d.get('artifacts', [])) == 1, d.get('artifacts')
print('partial diagnostic ok:', d['status'])
PY


echo "===== TEST 4: optional real unified dataset diagnostic if available ====="
REAL_ROOT="/workspace/imiss-deer-flow-main/datasets/phone-network"
if [ -f "$REAL_ROOT/processed/unified/user_nodes.csv" ]; then
  python3 dataset_quality_linkability_diagnostic_wrapper.py \
    --dataset-root "$REAL_ROOT" \
    --dataset unified \
    --province-a sichuan \
    --province-b shaanxi \
    --top-k 10 \
    --artifact-mode essential
else
  echo "real unified dataset not found, skip"
fi

echo "[OK] dataset-quality-and-linkability-diagnostic tests finished"
echo "log saved to: $LOG_PATH"
