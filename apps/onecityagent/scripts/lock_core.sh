#!/bin/sh
# Run from any directory with uv 0.9.9 available. Review the diff before release.
set -eu
ONECITY_ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ONECITY_ROOT/../.."
if [ "$(uv --version)" != "uv 0.9.9" ]; then
  echo 'Use uv 0.9.9 to regenerate the reviewed OneCity runtime lock.' >&2
  exit 1
fi
exec uv pip compile apps/onecityagent/requirements-core.txt \
  --python-version 3.13 \
  --python-platform x86_64-unknown-linux-gnu \
  --generate-hashes \
  --no-managed-python \
  --no-python-downloads \
  --output-file apps/onecityagent/requirements-core.lock "$@"
