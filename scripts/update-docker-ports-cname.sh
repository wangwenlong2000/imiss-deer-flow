#!/usr/bin/env bash
set -euo pipefail

# Central port configuration.
# Priority: environment variables > config.yaml:docker_ports > built-in defaults.
NGINX_PORT="${NGINX_PORT:-}"
LANGGRAPH_PORT="${LANGGRAPH_PORT:-}"
GATEWAY_PORT="${GATEWAY_PORT:-}"
FRONTEND_PORT="${FRONTEND_PORT:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
CONFIG_FILE="${DEER_FLOW_CONFIG_PATH:-$PROJECT_ROOT/config.yaml}"
CURRENT_USER="${SUDO_USER:-${USER:-$(id -un)}}"

export PROJECT_ROOT CONFIG_FILE CURRENT_USER NGINX_PORT LANGGRAPH_PORT GATEWAY_PORT FRONTEND_PORT

PYTHON_BIN="${PYTHON_BIN:-}"

if [ -z "$PYTHON_BIN" ]; then
    if command -v python3 >/dev/null 2>&1; then
        PYTHON_BIN="python3"
    elif command -v python >/dev/null 2>&1; then
        PYTHON_BIN="python"
    else
        echo "Python interpreter not found. Set PYTHON_BIN or install python3." >&2
        exit 1
    fi
fi

"$PYTHON_BIN" <<'PY'
from pathlib import Path
import os
import re

project_root = Path(os.environ["PROJECT_ROOT"])
config_file = Path(os.environ["CONFIG_FILE"])
current_user = os.environ.get("CURRENT_USER", "user")

default_ports = {
    "nginx": "3224",
    "langgraph": "3225",
    "gateway": "3226",
    "frontend": "3227",
}


def load_config_ports(file_path: Path) -> dict[str, str]:
    if not file_path.is_file():
        return {}

    text = file_path.read_text()
    match = re.search(r"^docker_ports:\s*\n(?P<body>(?:^[ \t]+.*(?:\n|$))*)", text, flags=re.MULTILINE)
    if not match:
        return {}

    ports: dict[str, str] = {}
    for name, value in re.findall(r"^[ \t]+(nginx|langgraph|gateway|frontend):[ \t]*(\d+)[ \t]*(?:#.*)?$", match.group("body"), flags=re.MULTILINE):
        ports[name] = value
    return ports


config_ports = load_config_ports(config_file)
nginx_port = os.environ.get("NGINX_PORT") or config_ports.get("nginx") or default_ports["nginx"]
langgraph_port = os.environ.get("LANGGRAPH_PORT") or config_ports.get("langgraph") or default_ports["langgraph"]
gateway_port = os.environ.get("GATEWAY_PORT") or config_ports.get("gateway") or default_ports["gateway"]
frontend_port = os.environ.get("FRONTEND_PORT") or config_ports.get("frontend") or default_ports["frontend"]
username_slug = re.sub(r"[^a-z0-9_.-]+", "-", current_user.lower()).strip("-.") or "user"


def load_config_subnet(file_path: Path) -> str:
    """Read subnet.dev from config.yaml, e.g. '192.168.200.0/24'."""
    if not file_path.is_file():
        return ""
    text = file_path.read_text()
    match = re.search(r"^subnet:\s*\n(?P<body>(?:^[ \t]+.*(?:\n|$))*)", text, flags=re.MULTILINE)
    if not match:
        return ""
    dev_match = re.search(r"^[ \t]+dev:[ \t]*(\S+)[ \t]*(?:#.*)?$", match.group("body"), flags=re.MULTILINE)
    return dev_match.group(1) if dev_match else ""


dev_subnet = os.environ.get("DEV_SUBNET") or load_config_subnet(config_file) or "192.168.200.0/24"


def replace_exact(text: str, old: str, new: str, file_path: Path) -> str:
    if old not in text:
        raise SystemExit(f"Expected text not found in {file_path}: {old}")
    return text.replace(old, new)


def replace_regex(text: str, pattern: str, repl: str, file_path: Path, min_count: int = 1, max_count: int | None = None) -> str:
    new_text, count = re.subn(pattern, repl, text, flags=re.MULTILINE)
    if count < min_count:
        raise SystemExit(f"Pattern not found enough times in {file_path}: {pattern}")
    if max_count is not None and count > max_count:
        raise SystemExit(f"Pattern matched too many times in {file_path}: {pattern} ({count})")
    return new_text


def write_file(relative_path: str, updater) -> None:
    file_path = project_root / relative_path
    original = file_path.read_text()
    updated = updater(original, file_path)
    if updated != original:
        file_path.write_text(updated)
        print(f"updated {relative_path}")
    else:
        print(f"no change {relative_path}")


def update_container_names(text: str, file_path: Path) -> str:
    def repl(match: re.Match[str]) -> str:
        service = match.group(1)
        return f"container_name: {username_slug}-deer-flow-{service}"

    return replace_regex(
        text,
        r"container_name:\s*(?:[a-z0-9_.-]+-)?deer-flow-(provisioner|nginx|frontend|gateway|langgraph)\b",
        repl,
        file_path,
        min_count=4,
    )


def update_sandbox_prefix(text: str, file_path: Path, min_count: int = 1) -> str:
    return replace_regex(
        text,
        r"(?:[a-z0-9_.-]+-)?deer-flow-sandbox\b",
        f"{username_slug}-deer-flow-sandbox",
        file_path,
        min_count=min_count,
    )


def update_dev_compose(text: str, file_path: Path) -> str:
    text = update_container_names(text, file_path)
    text = replace_regex(text, r"#   - nginx: Reverse proxy \(port \d+\)", f"#   - nginx: Reverse proxy (port {nginx_port})", file_path)
    text = replace_regex(text, r"#   - frontend: Frontend Next\.js dev server \(port \d+\)", f"#   - frontend: Frontend Next.js dev server (port {frontend_port})", file_path)
    text = replace_regex(text, r"#   - gateway: Backend Gateway API \(port \d+\)", f"#   - gateway: Backend Gateway API (port {gateway_port})", file_path)
    text = replace_regex(text, r"#   - langgraph: LangGraph server \(port \d+\)", f"#   - langgraph: LangGraph server (port {langgraph_port})", file_path)
    text = replace_regex(text, r"# Access: http://localhost:\d+", f"# Access: http://localhost:{nginx_port}", file_path)
    text = replace_regex(text, r'      - "\d+:\d+"', f'      - "{nginx_port}:{nginx_port}"', file_path, min_count=1, max_count=1)
    text = replace_regex(
        text,
        r'    command: sh -c "cd frontend && (?:pnpm run dev(?: -- --port \d+)?|PORT=\d+ pnpm run dev) > /app/logs/frontend\.log 2>&1"',
        f'    command: sh -c "cd frontend && PORT={frontend_port} pnpm run dev > /app/logs/frontend.log 2>&1"',
        file_path,
        min_count=1,
        max_count=1,
    )
    text = replace_regex(
        text,
        r'--port \d+ --reload --reload-include=\'\*\.yaml \.env\'',
        f"--port {gateway_port} --reload --reload-include='*.yaml .env'",
        file_path,
        min_count=1,
        max_count=1,
    )
    text = replace_regex(text, r'langgraph dev --no-browser --allow-blocking --host 0\.0\.0\.0 --port \d+', f'langgraph dev --no-browser --allow-blocking --host 0.0.0.0 --port {langgraph_port}', file_path, min_count=1, max_count=1)
    text = replace_regex(text, r"        - subnet: \S+", f"        - subnet: {dev_subnet}", file_path, min_count=1, max_count=1)
    return text


def update_prod_compose(text: str, file_path: Path) -> str:
    text = update_container_names(text, file_path)
    text = replace_regex(text, r"#   - nginx:       Reverse proxy \(port \d+, configurable via PORT env var\)", f"#   - nginx:       Reverse proxy (port {nginx_port}, configurable via PORT env var)", file_path)
    text = replace_regex(text, r"# Access: http://localhost:\$\{PORT:-\d+\}", f"# Access: http://localhost:${{PORT:-{nginx_port}}}", file_path)
    text = replace_regex(text, r'      - "\$\{PORT:-\d+\}:\d+"', f'      - "${{PORT:-{nginx_port}}}:{nginx_port}"', file_path, min_count=1, max_count=1)
    text = replace_regex(text, r'--port \d+ --workers 2', f'--port {gateway_port} --workers 2', file_path, min_count=1, max_count=1)
    text = replace_regex(text, r'langgraph dev --no-browser --allow-blocking --no-reload --host 0\.0\.0\.0 --port \d+', f'langgraph dev --no-browser --allow-blocking --no-reload --host 0.0.0.0 --port {langgraph_port}', file_path, min_count=1, max_count=1)

    port_line = f"      - PORT={frontend_port}\n"
    if re.search(r"^\s+- PORT=\d+\s*$", text, flags=re.MULTILINE):
        text = replace_regex(text, r'^\s+- PORT=\d+\s*$', f'      - PORT={frontend_port}', file_path, min_count=1, max_count=1)
    else:
        marker = "    environment:\n      - BETTER_AUTH_SECRET=${BETTER_AUTH_SECRET}\n"
        replacement = marker + port_line
        text = replace_exact(text, marker, replacement, file_path)
    return text


def update_nginx_conf(text: str, file_path: Path) -> str:
    text = replace_regex(text, r"(upstream gateway \{\n\s+server [^:]+:)\d+(;)", rf"\g<1>{gateway_port}\g<2>", file_path, min_count=1, max_count=1)
    text = replace_regex(text, r"(upstream langgraph \{\n\s+server [^:]+:)\d+(;)", rf"\g<1>{langgraph_port}\g<2>", file_path, min_count=1, max_count=1)
    text = replace_regex(text, r"(upstream frontend \{\n\s+server [^:]+:)\d+(;)", rf"\g<1>{frontend_port}\g<2>", file_path, min_count=1, max_count=1)
    text = replace_regex(text, r"listen (\[::\]:)?\d+( default_server)?;", lambda match: f"listen {(match.group(1) or '')}{nginx_port}{match.group(2) or ''};", file_path, min_count=2, max_count=2)
    return text


def update_docker_script(text: str, file_path: Path) -> str:
    # Ensure BuildKit exports exist before COMPOSE_CMD (idempotent)
    if "export DOCKER_BUILDKIT=1" not in text:
        text = re.sub(
            r'(COMPOSE_CMD="docker compose)',
            "export DOCKER_BUILDKIT=1\nexport COMPOSE_DOCKER_CLI_BUILD=1\n\\1",
            text,
            count=1,
        )
    text = replace_regex(
        text,
        r'COMPOSE_CMD="docker compose -p [a-z0-9_.-]+ -f docker-compose-dev.yaml"',
        f'COMPOSE_CMD="docker compose -p {username_slug}-deer-flow-dev -f docker-compose-dev.yaml"',
        file_path,
        min_count=1,
        max_count=1,
    )
    text = replace_regex(text, r'http://localhost:\d+/api/langgraph/\*', f'http://localhost:{nginx_port}/api/langgraph/*', file_path, min_count=1)
    text = replace_regex(text, r'http://localhost:\d+/api/\*', f'http://localhost:{nginx_port}/api/*', file_path, min_count=1)
    text = replace_regex(text, r'http://localhost:\d+', f'http://localhost:{nginx_port}', file_path, min_count=2)
    return text


def update_deploy_script(text: str, file_path: Path) -> str:
    text = replace_regex(
        text,
        r'COMPOSE_CMD=\(docker compose -p [a-z0-9_.-]+ -f "\$DOCKER_DIR/docker-compose\.yaml"\)',
        f'COMPOSE_CMD=(docker compose -p {username_slug}-deer-flow -f "$DOCKER_DIR/docker-compose.yaml")',
        file_path,
        min_count=1,
        max_count=1,
    )
    text = replace_regex(text, r'localhost:\$\{PORT:-\d+\}/api/langgraph/\*', f'localhost:${{PORT:-{nginx_port}}}/api/langgraph/*', file_path, min_count=1, max_count=1)
    text = replace_regex(text, r'localhost:\$\{PORT:-\d+\}/api/\*', f'localhost:${{PORT:-{nginx_port}}}/api/*', file_path, min_count=1, max_count=1)
    text = replace_regex(text, r'localhost:\$\{PORT:-\d+\}', f'localhost:${{PORT:-{nginx_port}}}', file_path, min_count=1)
    return text


def update_serve_script(text: str, file_path: Path) -> str:
    return update_sandbox_prefix(text, file_path, min_count=2)


def update_start_daemon_script(text: str, file_path: Path) -> str:
    return update_sandbox_prefix(text, file_path, min_count=1)


def update_cleanup_containers_script(text: str, file_path: Path) -> str:
    return update_sandbox_prefix(text, file_path, min_count=1)


def update_aio_sandbox_provider(text: str, file_path: Path) -> str:
    return replace_regex(
        text,
        r'^DEFAULT_CONTAINER_PREFIX\s*=\s*"(?:[a-z0-9_.-]+-)?deer-flow-sandbox"\s*$',
        f'DEFAULT_CONTAINER_PREFIX = "{username_slug}-deer-flow-sandbox"',
        file_path,
        min_count=1,
        max_count=1,
    )


write_file("docker/docker-compose-dev.yaml", update_dev_compose)
write_file("docker/docker-compose.yaml", update_prod_compose)
write_file("docker/nginx/nginx.conf", update_nginx_conf)
write_file("docker/nginx/nginx.local.conf", update_nginx_conf)
write_file("scripts/docker.sh", update_docker_script)
write_file("scripts/deploy.sh", update_deploy_script)
write_file("scripts/serve.sh", update_serve_script)
write_file("scripts/start-daemon.sh", update_start_daemon_script)
write_file("scripts/cleanup-containers.sh", update_cleanup_containers_script)
write_file("backend/packages/harness/deerflow/community/aio_sandbox/aio_sandbox_provider.py", update_aio_sandbox_provider)

print("done")
print(f"Configured ports: nginx={nginx_port} langgraph={langgraph_port} gateway={gateway_port} frontend={frontend_port}")
print(f"Container name prefix: {username_slug}-")
print(f"Compose project names: {username_slug}-deer-flow-dev / {username_slug}-deer-flow")
print(f"Sandbox prefix: {username_slug}-deer-flow-sandbox")
print(f"Dev network subnet: {dev_subnet}")
PY