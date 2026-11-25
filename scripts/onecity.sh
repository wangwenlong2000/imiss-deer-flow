#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
onecity_root="$repository_root/apps/onecityagent"
action="${1:-help}"
if [ "$#" -gt 0 ]; then
    shift
fi

case "$action" in
    api|coordinator|web|build)
        cd "$onecity_root"
        if [ -f .env ]; then
            set -a
            # shellcheck disable=SC1091
            source ./.env
            set +a
        fi
        ;;
    test|test-client)
        cd "$onecity_root"
        ;;
    *)
        echo "Usage: bash scripts/onecity.sh {api|coordinator|web|build|test|test-client} [arguments]"
        if [ "$action" = help ]; then
            exit 0
        fi
        exit 2
        ;;
esac

onecity_python="${ONECITY_PYTHON:-$onecity_root/.venv/bin/python}"
export PYTHONPATH="$onecity_root${PYTHONPATH:+:$PYTHONPATH}"

case "$action" in
    api)
        exec "$onecity_python" app.py "$@"
        ;;
    coordinator)
        exec "$onecity_python" onecityagent/service/a2a_service/coordinator_sse_acting.py "$@"
        ;;
    web|build)
        cd web
        if [ "$action" = web ]; then
            exec npm run dev -- "$@"
        fi
        exec npm run build -- "$@"
        ;;
    test)
        exec "$onecity_python" -m pytest -q tests "$@"
        ;;
    test-client)
        exec node web/scripts/verify-multi-user-client.cjs "$@"
        ;;
esac
