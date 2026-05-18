#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SKILL_ROOT="$(cd "$SCRIPT_DIR/.." 2>/dev/null && pwd || pwd)"
PROJECT_ROOT="$(cd "$SKILL_ROOT/../../../.." 2>/dev/null && pwd || pwd)"
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/test_risk_evidence_pack_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee "$LOG_FILE") 2>&1

DATASET_ROOT="${PHONE_NETWORK_DATASET_ROOT:-/workspace/imiss-deer-flow-main/datasets/phone-network}"
DATASET="unified"
CLASSIC_PHONE='141ab86b0a1277138c664368f30bfd93878754a968ca4f0f6f9f4d1b2279328985781d0740742e523f43e705753c5b9fd2bec9752624c1b79cf2b1132f1915be'

find_existing_unlabeled_phone() {
python3 - <<PY
import duckdb
from pathlib import Path
root = Path("$DATASET_ROOT")
user_nodes = root / "processed" / "$DATASET" / "user_nodes.csv"
call_edges = root / "processed" / "$DATASET" / "call_edges.csv"
if not user_nodes.exists() or not call_edges.exists():
    print("")
    raise SystemExit(0)
con = duckdb.connect(database=':memory:')
con.execute(f"CREATE VIEW user_nodes AS SELECT * FROM read_csv_auto('{user_nodes}', HEADER=TRUE)")
con.execute(f"CREATE VIEW call_edges AS SELECT * FROM read_csv_auto('{call_edges}', HEADER=TRUE)")
query = '''
WITH call_users AS (
    SELECT CAST(src_user_id AS VARCHAR) AS user_id FROM call_edges
    UNION ALL
    SELECT CAST(dst_counterparty_id AS VARCHAR) AS user_id FROM call_edges
), user_activity AS (
    SELECT user_id, COUNT(*) AS activity_cnt
    FROM call_users
    GROUP BY 1
)
SELECT CAST(u.user_id AS VARCHAR) AS user_id
FROM user_nodes u
JOIN user_activity a ON CAST(u.user_id AS VARCHAR) = CAST(a.user_id AS VARCHAR)
WHERE COALESCE(CAST(u.label AS INTEGER), 0) <> 1
  AND LOWER(COALESCE(CAST(u.sub_label AS VARCHAR), '')) NOT IN ('risk','purefraud','mutation')
ORDER BY a.activity_cnt DESC, CAST(u.user_id AS VARCHAR)
LIMIT 1
'''
rows = con.execute(query).fetchall()
print(rows[0][0] if rows else "")
PY
}

UNLABELED_PHONE="$(find_existing_unlabeled_phone)"

echo "===== TEST 1: classic risk phone ====="
python3 "$SCRIPT_DIR/risk_evidence_pack_wrapper.py" \
  --phone-id "$CLASSIC_PHONE" \
  --dataset-root "$DATASET_ROOT" \
  --dataset "$DATASET" \
  --top-k 10

echo "===== TEST 2: existing unlabeled phone ====="
if [[ -z "$UNLABELED_PHONE" ]]; then
  echo "[WARN] No existing unlabeled phone discovered; skipping test 2"
else
  echo "Using unlabeled phone: $UNLABELED_PHONE"
  python3 "$SCRIPT_DIR/risk_evidence_pack_wrapper.py" \
    --phone-id "$UNLABELED_PHONE" \
    --dataset-root "$DATASET_ROOT" \
    --dataset "$DATASET" \
    --top-k 10
fi

echo "===== TEST 3: deliberate not-found target ====="
python3 "$SCRIPT_DIR/risk_evidence_pack_wrapper.py" \
  --phone-id '48a530c83d8abca7c1f648c2c5f0c8eb4d0a85d11c5dc2e79ba5ae4d1aa53f50db5982ddff5df770b3874fda6daccf899dc5627c6d8221c5eb0a6e5e9a4c9b3d' \
  --dataset-root "$DATASET_ROOT" \
  --dataset "$DATASET" \
  --top-k 10

echo "===== TEST 4: report existence ====="
ls -1 /mnt/user-data/outputs/risk_evidence_pack_*.md 2>/dev/null || true
ls -1 /workspace/imiss-deer-flow-main/outputs/risk_evidence_pack_*.md 2>/dev/null || true

echo "[OK] risk-evidence-pack tests finished. log=$LOG_FILE"
