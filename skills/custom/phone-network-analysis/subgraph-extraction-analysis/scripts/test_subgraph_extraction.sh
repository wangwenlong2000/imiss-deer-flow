#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

python3 subgraph_extraction_wrapper.py \
  --phone-id '141ab86b0a1277138c664368f30bfd93878754a968ca4f0f6f9f4d1b2279328985781d0740742e523f43e705753c5b9fd2bec9752624c1b79cf2b1132f1915be' \
  --hops 1 \
  --max-nodes 100 \
  --top-k 10
