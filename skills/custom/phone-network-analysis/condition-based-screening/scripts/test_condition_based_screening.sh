#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TMP_DIR="${SCRIPT_DIR}/condition_based_screening_testdata"
DATA_ROOT="${TMP_DIR}/datasets/phone-network"
OUT_ROOT="${TMP_DIR}/outputs"
LOG_ROOT="${TMP_DIR}/logs"
SCRIPT="${SCRIPT_DIR}/condition_based_screening_wrapper.py"

rm -rf "${TMP_DIR}"
mkdir -p "${DATA_ROOT}/processed/unified"
mkdir -p "${DATA_ROOT}/processed/graph_views/unified"
mkdir -p "${OUT_ROOT}" "${LOG_ROOT}"

cat > "${DATA_ROOT}/processed/unified/call_edges.csv" <<'CSV'
province,dataset_name,src_user_id,dst_counterparty_id,event_time,event_date,event_hour,duration,call_type,imei
sichuan,phone-network-unified,p1,c1,2025-01-01 23:10:00,2025-01-01,23,10,2,imei_a
sichuan,phone-network-unified,p1,c2,2025-01-02 00:15:00,2025-01-02,0,20,2,imei_a
sichuan,phone-network-unified,p1,c3,2025-01-02 21:00:00,2025-01-02,21,15,2,imei_a
sichuan,phone-network-unified,p1,c4,2025-01-03 23:20:00,2025-01-03,23,15,2,imei_a
sichuan,phone-network-unified,p1,c5,2025-01-03 22:40:00,2025-01-03,22,15,2,imei_a
sichuan,phone-network-unified,p1,c6,2025-01-04 23:55:00,2025-01-04,23,15,2,imei_a
sichuan,phone-network-unified,p1,c7,2025-01-04 00:05:00,2025-01-04,0,15,2,imei_a
sichuan,phone-network-unified,p1,c8,2025-01-05 01:10:00,2025-01-05,1,15,2,imei_a
sichuan,phone-network-unified,p1,c9,2025-01-05 02:10:00,2025-01-05,2,15,2,imei_a
sichuan,phone-network-unified,p1,c10,2025-01-05 03:10:00,2025-01-05,3,15,2,imei_a
sichuan,phone-network-unified,p1,c11,2025-01-05 04:10:00,2025-01-05,4,15,2,imei_a
sichuan,phone-network-unified,p1,c12,2025-01-05 05:10:00,2025-01-05,5,15,2,imei_a
sichuan,phone-network-unified,p2,c1,2025-01-01 12:10:00,2025-01-01,12,10,2,imei_a
sichuan,phone-network-unified,p2,c2,2025-01-01 13:10:00,2025-01-01,13,10,2,imei_a
sichuan,phone-network-unified,p2,c3,2025-01-01 14:10:00,2025-01-01,14,10,2,imei_a
sichuan,phone-network-unified,p2,c4,2025-01-01 15:10:00,2025-01-01,15,10,2,imei_a
sichuan,phone-network-unified,p2,c5,2025-01-01 16:10:00,2025-01-01,16,10,2,imei_a
sichuan,phone-network-unified,p2,c6,2025-01-01 17:10:00,2025-01-01,17,10,2,imei_a
sichuan,phone-network-unified,p3,c1,2025-01-02 10:00:00,2025-01-02,10,8,2,imei_b
sichuan,phone-network-unified,p3,c2,2025-01-02 10:10:00,2025-01-02,10,8,2,imei_b
sichuan,phone-network-unified,p3,c3,2025-01-02 10:20:00,2025-01-02,10,8,2,imei_b
shaanxi,phone-network-unified,p4,c20,2025-01-02 11:00:00,2025-01-02,11,8,2,imei_c
shaanxi,phone-network-unified,p4,c21,2025-01-02 12:00:00,2025-01-02,12,8,2,imei_c
sichuan,phone-network-unified,p5,c1,2025-01-06 23:00:00,2025-01-06,23,20,2,imei_d
sichuan,phone-network-unified,p5,c2,2025-01-06 23:30:00,2025-01-06,23,20,2,imei_d
sichuan,phone-network-unified,p5,c3,2025-01-07 00:10:00,2025-01-07,0,20,2,imei_d
sichuan,phone-network-unified,p5,c4,2025-01-07 01:10:00,2025-01-07,1,20,2,imei_d
CSV

cat > "${DATA_ROOT}/processed/unified/user_nodes.csv" <<'CSV'
user_id,province,label,sub_label,risk_score
p1,sichuan,1,purefraud,92
p2,sichuan,1,risk,70
p3,sichuan,0,normal,20
p4,shaanxi,1,risk,55
p5,sichuan,0,normal,30
CSV

cat > "${DATA_ROOT}/processed/graph_views/unified/edges_phone_imei.csv" <<'CSV'
user_id,imei
p1,imei_a
p2,imei_a
p3,imei_b
p4,imei_c
p5,imei_d
p5,imei_a
CSV

echo '===== TEST 1: night abnormal ====='
python3 "${SCRIPT}" \
  --dataset-root "${DATA_ROOT}" \
  --dataset unified \
  --group-name night_case \
  --mode night_abnormal \
  --min-night-ratio 0.5 \
  --min-night-count 4 \
  --top-k 10 \
  --output-root "${OUT_ROOT}" | tee "${LOG_ROOT}/night_case.log"
grep -q '"ok": true' "${LOG_ROOT}/night_case.log"
grep -q 'p1' "${OUT_ROOT}"/condition_screening_targets_night_case_unified_*targets.csv

echo '===== TEST 2: shared device ====='
python3 "${SCRIPT}" \
  --dataset-root "${DATA_ROOT}" \
  --dataset unified \
  --group-name device_case \
  --mode shared_device \
  --min-shared-device-count 1 \
  --min-shared-peer-total 1 \
  --top-k 10 \
  --output-root "${OUT_ROOT}" | tee "${LOG_ROOT}/device_case.log"
grep -q '"ok": true' "${LOG_ROOT}/device_case.log"
grep -q 'imei_a' "${OUT_ROOT}"/condition_screening_devices_device_case_unified_*rows.csv

echo '===== TEST 3: mixed all + province + risk ====='
python3 "${SCRIPT}" \
  --dataset-root "${DATA_ROOT}" \
  --dataset unified \
  --group-name mixed_case \
  --province sichuan \
  --risk-only \
  --min-counterparties 5 \
  --min-shared-device-count 1 \
  --match-mode all \
  --top-k 10 \
  --output-root "${OUT_ROOT}" | tee "${LOG_ROOT}/mixed_case.log"
grep -q '"ok": true' "${LOG_ROOT}/mixed_case.log"
grep -q '筛选链路' "${OUT_ROOT}"/condition_screening_report_mixed_case_unified.md

echo '===== TEST 4: unlabeled suspicious ====='
python3 "${SCRIPT}" \
  --dataset-root "${DATA_ROOT}" \
  --dataset unified \
  --group-name unlabeled_case \
  --mode mixed \
  --unlabeled-only \
  --min-counterparties 3 \
  --min-shared-device-count 1 \
  --match-mode any \
  --top-k 10 \
  --output-root "${OUT_ROOT}" | tee "${LOG_ROOT}/unlabeled_case.log"
grep -q '"ok": true' "${LOG_ROOT}/unlabeled_case.log"
grep -q 'p5' "${OUT_ROOT}"/condition_screening_targets_unlabeled_case_unified_*targets.csv

echo '===== TEST 5: xlsx + json evidence ====='
ls -1 "${OUT_ROOT}"/condition_screening_evidence_*_unified.xlsx >/dev/null
ls -1 "${OUT_ROOT}"/condition_screening_summary_*_unified.json >/dev/null

echo '[OK] condition-based-screening tests passed'
