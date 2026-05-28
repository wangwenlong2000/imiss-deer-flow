#!/usr/bin/env bash
set -euo pipefail

OPENGREP_VERSION="${OPENGREP_VERSION:-v1.21.0}"
OPENGREP_ASSET="${OPENGREP_ASSET:-opengrep_manylinux_x86}"
RULES_REF="${OPENGREP_RULES_REF:-main}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SKILL_ROOT="${REPO_ROOT}/skills/public/opengrep-compliance"
BIN_DIR="${SKILL_ROOT}/vendor/opengrep/linux"
RULES_DIR="${SKILL_ROOT}/rules"
OFFICIAL_RULES_DIR="${RULES_DIR}/official/opengrep-rules"
DOWNLOAD_DIR="${REPO_ROOT}/.tmp/opengrep-downloads"

OPENGREP_BASE_URL="https://github.com/opengrep/opengrep/releases/download/${OPENGREP_VERSION}"
RULES_ZIP_URL="https://github.com/opengrep/opengrep-rules/archive/refs/heads/${RULES_REF}.zip"

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

download() {
  local url="$1"
  local output="$2"
  echo "Downloading ${url}"
  curl --fail --location --show-error --silent --output "${output}" "${url}"
}

need_cmd curl
need_cmd unzip

mkdir -p "${BIN_DIR}" "${RULES_DIR}" "${DOWNLOAD_DIR}"

echo "Preparing OpenGrep assets"
echo "  Version: ${OPENGREP_VERSION}"
echo "  Asset:   ${OPENGREP_ASSET}"
echo "  Rules:   opengrep-rules ${RULES_REF}"
echo

download "${OPENGREP_BASE_URL}/${OPENGREP_ASSET}" "${BIN_DIR}/opengrep"
download "${OPENGREP_BASE_URL}/${OPENGREP_ASSET}.cert" "${BIN_DIR}/opengrep.cert"
download "${OPENGREP_BASE_URL}/${OPENGREP_ASSET}.sig" "${BIN_DIR}/opengrep.sig"

chmod +x "${BIN_DIR}/opengrep"

RULES_ZIP="${DOWNLOAD_DIR}/opengrep-rules-${RULES_REF}.zip"
RULES_EXTRACT_DIR="${DOWNLOAD_DIR}/opengrep-rules-extracted"

download "${RULES_ZIP_URL}" "${RULES_ZIP}"

rm -rf "${RULES_EXTRACT_DIR}"
mkdir -p "${RULES_EXTRACT_DIR}"
unzip -q "${RULES_ZIP}" -d "${RULES_EXTRACT_DIR}"

RULES_SOURCE="$(find "${RULES_EXTRACT_DIR}" -maxdepth 1 -type d -name 'opengrep-rules-*' | head -n 1)"
if [[ -z "${RULES_SOURCE}" ]]; then
  echo "Could not find extracted opengrep-rules directory" >&2
  exit 1
fi

rm -rf "${OFFICIAL_RULES_DIR}"
mkdir -p "${OFFICIAL_RULES_DIR}"

while IFS= read -r -d '' rule_file; do
  relative_path="${rule_file#"${RULES_SOURCE}/"}"
  mkdir -p "${OFFICIAL_RULES_DIR}/$(dirname "${relative_path}")"
  cp "${rule_file}" "${OFFICIAL_RULES_DIR}/${relative_path}"
done < <(
  find "${RULES_SOURCE}" \
    \( -path "${RULES_SOURCE}/.github" -o -path "${RULES_SOURCE}/stats" -o -path "${RULES_SOURCE}/scripts" \) -prune \
    -o -type f \( -name '*.yaml' -o -name '*.yml' \) \
    ! -name '*.test.yaml' ! -name '*.test.yml' \
    -print0
)

for name in README.md LICENSE SECURITY.md; do
  if [[ -f "${RULES_SOURCE}/${name}" ]]; then
    cp "${RULES_SOURCE}/${name}" "${OFFICIAL_RULES_DIR}/${name}"
  fi
done

cat > "${SKILL_ROOT}/rules/README.md" <<EOF
# OpenGrep Local Rules

These rules were downloaded during build preparation.

- OpenGrep version: ${OPENGREP_VERSION}
- Rules source: opengrep/opengrep-rules ${RULES_REF}
- Downloaded asset: ${OPENGREP_ASSET}
- Official rules path: official/opengrep-rules
- Import strategy: all upstream runtime .yaml/.yml rule files are imported; upstream test fixtures and repository metadata are excluded.

Runtime scans must use this local directory and must not use remote configs or \`--config auto\`.
EOF

echo
echo "OpenGrep assets are ready:"
echo "  Binary: ${BIN_DIR}/opengrep"
echo "  Cert:   ${BIN_DIR}/opengrep.cert"
echo "  Sig:    ${BIN_DIR}/opengrep.sig"
echo "  Rules:  ${RULES_DIR}"
echo "  Official rules: ${OFFICIAL_RULES_DIR}"
echo
"${BIN_DIR}/opengrep" --version || true
