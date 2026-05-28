#!/usr/bin/env bash
set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
WORK_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
DATA_DIR="$WORK_DIR/testdata"
mkdir -p "$DATA_DIR"

cat > "$DATA_DIR/toy_graph.csv" <<'CSV'
src,dst
A,B
B,C
A,D
D,C
C,E
CSV

echo "[1/5] load_graph"
python "$SCRIPT_DIR/graph_operator_wrapper.py" \
  --operator load_graph \
  --graph-path "$DATA_DIR/toy_graph.csv" \
  --graph-format csv

echo "[2/5] expand_neighbors"
python "$SCRIPT_DIR/graph_operator_wrapper.py" \
  --operator expand_neighbors \
  --graph-path "$DATA_DIR/toy_graph.csv" \
  --graph-format csv \
  --node A

echo "[3/5] shortest_path"
python "$SCRIPT_DIR/graph_operator_wrapper.py" \
  --operator shortest_path \
  --graph-path "$DATA_DIR/toy_graph.csv" \
  --graph-format csv \
  --source A \
  --target E

echo "[4/5] basic_graph_metrics"
python "$SCRIPT_DIR/graph_operator_wrapper.py" \
  --operator basic_graph_metrics \
  --graph-path "$DATA_DIR/toy_graph.csv" \
  --graph-format csv

echo "[5/5] extract_subgraph"
python "$SCRIPT_DIR/graph_operator_wrapper.py" \
  --operator extract_subgraph \
  --graph-path "$DATA_DIR/toy_graph.csv" \
  --graph-format csv \
  --nodes A,B,C
