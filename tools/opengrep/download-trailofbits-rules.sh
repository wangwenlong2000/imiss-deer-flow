#!/usr/bin/env bash
set -euo pipefail

RULES_REF="${TRAILOFBITS_RULES_REF:-master}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SKILL_ROOT="${REPO_ROOT}/skills/public/opengrep-compliance"
RULES_DIR="${SKILL_ROOT}/rules"
COMMUNITY_RULES_DIR="${RULES_DIR}/community/trailofbits-semgrep-rules"
DOWNLOAD_DIR="${REPO_ROOT}/.tmp/opengrep-downloads"

RULES_ZIP_URL="https://github.com/trailofbits/semgrep-rules/archive/refs/heads/${RULES_REF}.zip"

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

mkdir -p "${RULES_DIR}" "${DOWNLOAD_DIR}"

RULES_ZIP="${DOWNLOAD_DIR}/trailofbits-semgrep-rules-${RULES_REF}.zip"
RULES_EXTRACT_DIR="${DOWNLOAD_DIR}/trailofbits-semgrep-rules-extracted"

download "${RULES_ZIP_URL}" "${RULES_ZIP}"

rm -rf "${RULES_EXTRACT_DIR}"
mkdir -p "${RULES_EXTRACT_DIR}"
unzip -q "${RULES_ZIP}" -d "${RULES_EXTRACT_DIR}"

RULES_SOURCE="$(find "${RULES_EXTRACT_DIR}" -maxdepth 1 -type d -name 'semgrep-rules-*' | head -n 1)"
if [[ -z "${RULES_SOURCE}" ]]; then
  echo "Could not find extracted Trail of Bits semgrep-rules directory" >&2
  exit 1
fi

rm -rf "${COMMUNITY_RULES_DIR}"
mkdir -p "${COMMUNITY_RULES_DIR}"

while IFS= read -r -d '' rule_file; do
  relative_path="${rule_file#"${RULES_SOURCE}/"}"
  mkdir -p "${COMMUNITY_RULES_DIR}/$(dirname "${relative_path}")"
  cp "${rule_file}" "${COMMUNITY_RULES_DIR}/${relative_path}"
done < <(
  find "${RULES_SOURCE}" \
    \( -path "${RULES_SOURCE}/.github" \) -prune \
    -o -type f \( -name '*.yaml' -o -name '*.yml' \) \
    ! -name '*.test.yaml' ! -name '*.test.yml' \
    -print0
)

for name in README.md LICENSE CONTRIBUTING.md; do
  if [[ -f "${RULES_SOURCE}/${name}" ]]; then
    cp "${RULES_SOURCE}/${name}" "${COMMUNITY_RULES_DIR}/${name}"
  fi
done

cat > "${COMMUNITY_RULES_DIR}/IMPORT_NOTES.md" <<EOF
# Trail of Bits Semgrep Rules Import Notes

This directory contains a local import of Trail of Bits Semgrep runtime YAML/YML rules.

- Source: trailofbits/semgrep-rules ${RULES_REF}
- Import strategy: runtime .yaml/.yml rule files only
- Excluded: .github metadata and *.test.yaml/*.test.yml files

The opengrep-compliance skill loads the parent rules directory, so these community rules are used together with local baseline rules and OpenGrep official rules.
EOF

echo "Trail of Bits rules are ready:"
echo "  Rules: ${COMMUNITY_RULES_DIR}"
find "${COMMUNITY_RULES_DIR}" -type f \( -name '*.yaml' -o -name '*.yml' \) | wc -l
