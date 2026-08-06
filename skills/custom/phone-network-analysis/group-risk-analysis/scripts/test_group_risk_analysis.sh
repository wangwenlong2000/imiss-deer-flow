#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../../../.." && pwd)"
LOG_DIR="${PROJECT_ROOT}/logs/group-risk-analysis"
mkdir -p "${LOG_DIR}"

cd "${SCRIPT_DIR}"

GROUP_FILE="${SCRIPT_DIR}/sample_group_ids.txt"
if [[ ! -f "${GROUP_FILE}" ]]; then
  echo "sample_group_ids.txt not found: ${GROUP_FILE}" >&2
  exit 1
fi

TS="$(date +%Y%m%d_%H%M%S)"

run_case() {
  local name="$1"
  shift
  echo "[RUN] ${name}"
  python3 group_risk_analysis_wrapper.py "$@" | tee "${LOG_DIR}/${name}_${TS}.log"
  echo "[DONE] ${name}"
  echo
}

run_case default_group \
  --group-name classic_group \
  --phone-id-file "${GROUP_FILE}" \
  --top-k 10

run_case risk_only_group \
  --group-name classic_group_risk_only \
  --phone-id-file "${GROUP_FILE}" \
  --risk-only \
  --top-k 10

run_case shared_device_focused \
  --group-name classic_group_device_focus \
  --phone-id-file "${GROUP_FILE}" \
  --min-shared-device-count 5 \
  --min-shared-peer-total 50 \
  --min-device-pool-count 5 \
  --top-k 10

run_case province_filtered \
  --group-name classic_group_sichuan \
  --phone-id-file "${GROUP_FILE}" \
  --province sichuan \
  --top-k 10

echo "Logs written to: ${LOG_DIR}"
