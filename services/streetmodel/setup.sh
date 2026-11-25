#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"
RUNTIME_DIR="$REPO_ROOT/.runtime/streetmodel"
mkdir -p "$RUNTIME_DIR" "$RUNTIME_DIR/wheels"
if [ ! -x "$RUNTIME_DIR/venv/bin/python" ]; then
    "${STREET_PYTHON:-/usr/bin/python3.10}" -m venv "$RUNTIME_DIR/venv"
fi
export NO_PROXY="${NO_PROXY:+${NO_PROXY},}files.pythonhosted.org"
"$RUNTIME_DIR/venv/bin/python" -m pip install --no-cache-dir --find-links "$RUNTIME_DIR/wheels" -r services/streetmodel/requirements.txt
for entry in 'ImAge 5d5e2d8cad11fe7ba699fdbd1f6b0a3c69dade18 https://github.com/Lu-Feng/ImAge.git' 'Qwen3-VL-Embedding 393e2978d27852b0d0230d6994f37f9c15bed73c https://github.com/QwenLM/Qwen3-VL-Embedding.git'; do
    read -r repo revision url <<< "$entry"
    if [ ! -d "$RUNTIME_DIR/$repo/.git" ]; then
        git clone --no-checkout "$url" "$RUNTIME_DIR/$repo"
    fi
    if [ "$(git -C "$RUNTIME_DIR/$repo" rev-parse HEAD 2>/dev/null || true)" != "$revision" ]; then
        git -C "$RUNTIME_DIR/$repo" fetch --depth 1 origin "$revision"
        git -C "$RUNTIME_DIR/$repo" checkout --detach "$revision"
    fi
done
"$RUNTIME_DIR/venv/bin/python" services/streetmodel/download_models.py
