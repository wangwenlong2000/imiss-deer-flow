#!/usr/bin/env bash
set -e

OUT="env-audit"
BACKEND_DIR="./backend"
FRONTEND_DIR="./frontend"

rm -rf "$OUT"
mkdir -p "$OUT/system" "$OUT/backend" "$OUT/frontend" "$OUT/nginx" "$OUT/sandbox"

cat /etc/os-release > "$OUT/system/os-release.txt"
uname -a > "$OUT/system/kernel-version.txt"
lscpu > "$OUT/system/cpu-memory.txt" 2>&1 || true
free -h >> "$OUT/system/cpu-memory.txt" 2>&1 || true
ss -lntp > "$OUT/system/ports.txt" 2>&1 || netstat -lntp > "$OUT/system/ports.txt" 2>&1 || true

cd "$BACKEND_DIR"

if [ -d ".venv" ]; then
  PY=".venv/bin/python"
else
  PY="$(which python3 || which python)"
fi

$PY -V > "../$OUT/backend/python-runtime-version.txt"
$PY -m pip --version > "../$OUT/backend/pip-version.txt" 2>&1 || true
$PY -m pip freeze > "../$OUT/backend/pip-freeze.txt" 2>&1 || true

if command -v uv >/dev/null 2>&1; then
  uv --version > "../$OUT/backend/uv-version.txt"
  uv pip list --python "$PY" > "../$OUT/backend/uv-pip-list.txt" 2>&1 || true
else
  echo "uv not found in current server environment" > "../$OUT/backend/uv-version.txt"
fi

cp pyproject.toml "../$OUT/backend/pyproject.toml" 2>/dev/null || true
cp requirements.txt "../$OUT/backend/requirements.txt" 2>/dev/null || true
cp uv.lock "../$OUT/backend/uv.lock" 2>/dev/null || true

cd ../"$FRONTEND_DIR"

node -v > "../$OUT/frontend/node-version.txt" 2>&1 || true
npm -v > "../$OUT/frontend/npm-version.txt" 2>&1 || true

if command -v pnpm >/dev/null 2>&1; then
  pnpm -v > "../$OUT/frontend/pnpm-version.txt"
  pnpm list --depth 0 > "../$OUT/frontend/pnpm-list-depth0.txt" 2>&1 || true
else
  echo "pnpm not found in current server environment" > "../$OUT/frontend/pnpm-version.txt"
fi

cp package.json "../$OUT/frontend/package.json" 2>/dev/null || true
cp pnpm-lock.yaml "../$OUT/frontend/pnpm-lock.yaml" 2>/dev/null || true
cp package-lock.json "../$OUT/frontend/package-lock.json" 2>/dev/null || true
cp yarn.lock "../$OUT/frontend/yarn.lock" 2>/dev/null || true

cd ..

nginx -v > "$OUT/nginx/nginx-version.txt" 2>&1 || true
nginx -t > "$OUT/nginx/nginx-config-test.txt" 2>&1 || true
cat /etc/os-release > "$OUT/nginx/os-release.txt"

node -v > "$OUT/sandbox/runtime-version.txt" 2>&1 || true
npm -v >> "$OUT/sandbox/runtime-version.txt" 2>&1 || true
python3 -V >> "$OUT/sandbox/runtime-version.txt" 2>&1 || true
python3 -m pip --version >> "$OUT/sandbox/runtime-version.txt" 2>&1 || true

python3 -m pip show dashscope numpy pandas duckdb xlrd openpyxl pyyaml requests \
  > "$OUT/sandbox/python-selected-packages.txt" 2>&1 || true

python3 -m pip freeze > "$OUT/sandbox/python-freeze.txt" 2>&1 || true

if command -v dpkg >/dev/null 2>&1; then
  dpkg -l | grep -Ei 'nodejs|python3-pip|fontconfig|fonts-noto-cjk|chromium|playwright|nss|gtk|x11|xcomposite|xdamage|xrandr|asound|atk|cups|drm|gbm|pango|cairo' \
    > "$OUT/sandbox/system-selected-packages.txt" || true
elif command -v rpm >/dev/null 2>&1; then
  rpm -qa | grep -Ei 'nodejs|python3-pip|fontconfig|noto|chromium|playwright|nss|gtk|x11|alsa|cups|drm|gbm|pango|cairo' \
    > "$OUT/sandbox/system-selected-packages.txt" || true
else
  echo "Neither dpkg nor rpm found" > "$OUT/sandbox/system-selected-packages.txt"
fi

echo "Done. Direct deployment audit files saved to $OUT/"
find "$OUT" -type f | sort
