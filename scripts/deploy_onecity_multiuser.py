#!/usr/bin/env python3
"""Prepare and explicitly start OneCity's private, IAM-enabled DeerFlow pair.

``prepare`` only reads Docker metadata/configuration and writes a private local
bundle. It never changes existing containers, networks, routes or source files.
``start`` is a separate operator action; it only creates the two named new
containers. OneCity backup, source deployment and traffic switching are separate.
No credentials or populated configuration are printed by this script.
"""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import shutil
import stat
import subprocess
import sys
import time
from urllib.parse import urlparse


LABEL = "io.onecity.deerflow.multiuser-bundle"
PYTHON = "/app/backend/.venv/bin/python"
DATA = "/data/deerflow"
NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.-]+$")
ROOT = Path(__file__).resolve().parents[1]
REQUIRED_FILES = (
    "app/gateway/access_middleware.py", "app/gateway/onecity_iam.py",
    "app/gateway/mobile_iam.py", "app/gateway/thread_access.py",
    "app/gateway/conversation_store.py", "app/admission_claims.py",
    "app/internal_auth.py", "app/graph_access.py", "langgraph.json",
)
SNAPSHOT_TREES = ("app", "packages", "config")
SNAPSHOT_FILES = ("langgraph.json", "pyproject.toml", "uv.lock")
EXCLUDED_SOURCE = (
    ".*", "__pycache__", "*.pyc", "*.pyo", "*.db", "*.db-*", "*.sqlite*",
    "logs", "outputs", "node_modules", "dist", "build", "*.egg-info",
)


class DeploymentError(RuntimeError):
    """An operator-visible error containing no captured configuration/output."""


def run(command: list[str], *, input_text: str | None = None, timeout: int = 60) -> str:
    try:
        result = subprocess.run(
            command, input=input_text, text=True, capture_output=True,
            timeout=timeout, check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise DeploymentError(f"{command[0]} operation failed ({type(exc).__name__})") from None
    if result.returncode:
        # Docker/Python diagnostics may include model URLs, configuration or env.
        raise DeploymentError(f"{command[0]} operation failed (exit {result.returncode}); captured output suppressed")
    return result.stdout


def inspect_container(name: str) -> dict:
    return json.loads(run(["docker", "inspect", name]))[0]


def secure_directory(path: Path) -> None:
    if path.is_symlink():
        raise DeploymentError("The bundle directory must not be a symbolic link")
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.stat().st_uid != os.getuid():
        raise DeploymentError("The bundle directory must belong to the current operator")
    path.chmod(0o700)


def secure_write(path: Path, content: str, mode: int = 0o600) -> None:
    # Refuse overwrite/symlinks; prepare never silently rotates deployed secrets.
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    with os.fdopen(descriptor, "w", encoding="utf-8") as output:
        output.write(content)


def environment(container: dict) -> dict[str, str]:
    return dict(entry.split("=", 1) for entry in container["Config"].get("Env", []) if "=" in entry)


def env_text(values: dict[str, str]) -> str:
    for key, value in values.items():
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key) or any(c in value for c in "\r\n\x00"):
            raise DeploymentError("An inherited environment entry cannot be represented safely in a Docker env file")
    return "".join(f"{key}={value}\n" for key, value in sorted(values.items()))


def source_mount(container: dict, destination: str) -> dict:
    candidates = [mount for mount in container["Mounts"] if mount["Destination"].rstrip("/") == destination]
    if len(candidates) != 1:
        raise DeploymentError(f"Expected one source mount at {destination}")
    original = candidates[0]
    if original["Type"] == "bind":
        return {"type": "bind", "source": original["Source"], "target": destination, "readonly": True}
    if original["Type"] == "volume":
        return {"type": "volume", "source": original["Name"], "target": destination, "readonly": True}
    raise DeploymentError(f"Unsupported source mount type at {destination}")


def snapshot_backend(backend: Path, destination: Path) -> dict[str, str]:
    """Copy only backend source trees; never follow links into runtime/secrets."""
    secure_directory(destination)
    for tree in SNAPSHOT_TREES:
        source = backend / tree
        if not source.exists():
            continue
        if source.is_symlink():
            raise DeploymentError("Source snapshot trees must not be symbolic links")
        for directory, directories, filenames in os.walk(source, followlinks=False):
            directories[:] = [name for name in directories if not any(fnmatch.fnmatch(name, pattern) for pattern in EXCLUDED_SOURCE)]
            for name in [*directories, *filenames]:
                candidate = Path(directory) / name
                if any(fnmatch.fnmatch(name, pattern) for pattern in EXCLUDED_SOURCE):
                    continue
                if candidate.is_symlink():
                    raise DeploymentError("Source snapshot cannot contain symbolic links")
                target = destination / candidate.relative_to(backend)
                if candidate.is_dir():
                    target.mkdir(parents=True, exist_ok=True)
                elif candidate.is_file():
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(candidate, target)
    for name in SNAPSHOT_FILES:
        source = backend / name
        if source.is_symlink():
            raise DeploymentError("Source snapshot cannot contain symbolic links")
        if source.is_file():
            shutil.copyfile(source, destination / name)
    # Nested read-only/volume mounts need existing mountpoints underneath the
    # read-only backend bind. These remain empty in the source snapshot.
    for name in (".venv", ".langgraph_api"):
        (destination / name).mkdir()
    return {str(path.relative_to(destination)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(destination.rglob("*")) if path.is_file()}


def bind(source: Path | str, target: str, *, readonly: bool = True) -> dict:
    return {"type": "bind", "source": str(source), "target": target, "readonly": readonly}


def build_config(original: dict, *, sandbox_prefix: str, sandbox_port: int) -> dict:
    config = json.loads(json.dumps(original))
    sandbox = config.get("sandbox", {})
    if sandbox.get("use") != "deerflow.community.aio_sandbox:AioSandboxProvider" or sandbox.get("provisioner_url"):
        raise DeploymentError("This deployment bundle requires the existing local AIO container sandbox")
    if sandbox_prefix == sandbox.get("container_prefix") or sandbox_port == sandbox.get("port"):
        raise DeploymentError("The new sandbox prefix and base port must differ from the source deployment")
    if any(not mount.get("read_only", False) for mount in sandbox.get("mounts", [])):
        raise DeploymentError("Shared sandbox asset mounts must be read-only")
    sandbox.update(container_prefix=sandbox_prefix, port=sandbox_port)
    # An independent Gateway must not establish duplicate IM subscriptions.
    config["channels"] = {}
    compliance = config.setdefault("compliance", {})
    compliance.update(scene_resolver_mode="trusted_upstream", strict_startup=True)
    compliance.setdefault("scene", {})["resolver"] = None
    compliance.setdefault("audit", {})["path"] = f"{DATA}/compliance/audit"
    checkpointer = config.get("checkpointer", {})
    if checkpointer.get("type") != "sqlite":
        raise DeploymentError("This bundle requires the source deployment's SQLite checkpointer")
    checkpointer["connection_string"] = f"{DATA}/checkpoints.db"
    return config


def service_environment(original: dict, *, data_dir: Path, key: str, graph_name: str, iam_endpoint: str,
                        jobs: int, max_inflight: int, per_user: int) -> dict[str, str]:
    values = environment(original)
    # Docker sets these at container creation; retaining them creates misleading
    # identities. All model/provider values stay private in the generated file.
    values.pop("HOSTNAME", None)
    values.update({
        "DEER_FLOW_CONFIG_PATH": "/app/config.onecity.json",
        "DEER_FLOW_EXTENSIONS_CONFIG_PATH": "/app/extensions_config.json",
        "DEER_FLOW_HOME": DATA, "DEER_FLOW_HOST_BASE_DIR": str(data_dir),
        "DEER_FLOW_SANDBOX_HOST": "host.docker.internal",
        "MOBILE_IAM_PROVIDER": "app.gateway.onecity_iam:OneCityIAMProvider",
        "MOBILE_IAM_ENDPOINT": iam_endpoint, "MOBILE_IAM_REQUIRED": "true",
        "DEER_FLOW_ADMISSION_SIGNING_KEY": key, "DEER_FLOW_REQUIRE_SIGNED_ADMISSION": "true",
        "MOBILE_ADAPTER_LANGGRAPH_URL": f"http://{graph_name}:3329",
        "LANGGRAPH_URL": f"http://{graph_name}:3329",
        "DEER_FLOW_GRAPH_JOBS": str(jobs), "DEER_FLOW_MAX_INFLIGHT_RUNS": str(max_inflight),
        "DEER_FLOW_MAX_INFLIGHT_PER_USER": str(per_user),
        "DEERFLOW_RUN_EVENT_LOG_ENABLED": "0", "DEERFLOW_RUN_EVENT_LOG_DIR": f"{DATA}/run_events",
        "PYTHONPATH": "/app/backend:/app/backend/packages/harness", "PYTHONDONTWRITEBYTECODE": "1",
        "GATEWAY_PORT": "3330",
    })
    values["NO_PROXY"] = ",".join(filter(None, (
        values.get("NO_PROXY") or values.get("no_proxy"),
        "localhost", "127.0.0.1", graph_name, urlparse(iam_endpoint).hostname,
    )))
    values["no_proxy"] = values["NO_PROXY"]
    return values


def mount_argument(mount: dict) -> str:
    if any("," in mount[key] or "\x00" in mount[key] for key in ("source", "target")):
        raise DeploymentError("Docker mount paths must not contain commas or NUL bytes")
    parts = [f"type={mount['type']}", f"src={mount['source']}", f"dst={mount['target']}"]
    if mount["readonly"]:
        parts.append("readonly")
    return ",".join(parts)


def command_for(plan: dict, service: dict) -> list[str]:
    command = [
        "docker", "run", "--detach", "--pull", "never", "--restart", "unless-stopped",
        "--name", service["name"], "--network", plan["network"],
        "--label", f"{LABEL}={plan['bundle_id']}",
        "--user", "0:0",
        "--add-host", "host.docker.internal:host-gateway", "--workdir", "/app/backend",
        "--env-file", service["env_file"],
    ]
    for mount in service["mounts"]:
        command.extend(["--mount", mount_argument(mount)])
    # No --publish or host networking: OneCity uses the private Docker name.
    command.extend([service["image_id"], *service["command"]])
    return command


def prepare(args) -> dict:
    backend = args.repo.resolve() / "backend"
    if not all((backend / name).is_file() for name in REQUIRED_FILES):
        raise DeploymentError("The selected backend is missing required multiuser modules")
    runtime = args.runtime_dir.absolute()
    if runtime == args.repo.resolve() or runtime.is_relative_to(args.repo.resolve()):
        raise DeploymentError("The private deployment bundle must be outside the source repository")
    if runtime.exists() and any(runtime.iterdir()):
        raise DeploymentError("The bundle directory is not empty; reuse its start/status command or choose a new directory")
    gateway = inspect_container(args.source_gateway)
    graph = inspect_container(args.source_graph)
    onecity = inspect_container(args.onecity)
    if args.network not in onecity["NetworkSettings"]["Networks"]:
        raise DeploymentError("The OneCity container is not attached to the selected private network")
    run(["docker", "network", "inspect", args.network])
    # Parse using the already installed live PyYAML, without importing the app,
    # models or production databases, and never print the captured config.
    raw_config = json.loads(run([
        "docker", "exec", args.source_graph, PYTHON, "-c",
        "import json,os,yaml; print(json.dumps(yaml.safe_load(open(os.environ.get('DEER_FLOW_CONFIG_PATH','/app/config.yaml')))))",
    ]))
    config = build_config(raw_config, sandbox_prefix=args.sandbox_prefix, sandbox_port=args.sandbox_port)
    secure_directory(runtime)
    for name in ("data", "langgraph-runtime", "logs"):
        secure_directory(runtime / name)
    snapshot = runtime / "release/backend"
    source_manifest = snapshot_backend(backend, snapshot)
    secure_write(runtime / "source-manifest.json", json.dumps(source_manifest, indent=2) + "\n")
    key = secrets.token_urlsafe(48)
    secure_write(runtime / "admission.key", key + "\n")
    # JSON is also valid YAML and can be consumed by AppConfig.safe_load.
    secure_write(runtime / "config.onecity.json", json.dumps(config, ensure_ascii=False, indent=2) + "\n")
    graph_settings = json.loads((snapshot / "langgraph.json").read_text())
    graph_settings.pop("env", None)  # All runtime settings come from the private env file.
    secure_write(runtime / "langgraph.json", json.dumps(graph_settings, indent=2) + "\n")
    iam_endpoint = f"http://{args.onecity}:{args.iam_port}/internal/deerflow/iam"
    plan = {
        "schema_version": 1, "bundle_id": secrets.token_hex(12), "runtime_dir": str(runtime),
        "repo": str(args.repo.resolve()), "network": args.network, "onecity_container": args.onecity,
        "backend_snapshot": str(snapshot), "source_file_count": len(source_manifest),
        "iam_endpoint": iam_endpoint, "graph_jobs": args.jobs, "max_inflight": args.max_inflight,
        "max_inflight_per_user": args.per_user, "sandbox_prefix": args.sandbox_prefix,
        "sandbox_base_port": args.sandbox_port, "services": [],
        "onecity_switch": {
            "DEERFLOW_API_URL": f"http://{args.gateway_name}:3330/api/deerflow/mobile/run",
            "DEERFLOW_FILE_INTERNAL_BASE": f"http://{args.gateway_name}:3330",
            "DEERFLOW_PUBLIC_BASE_URL": "",
            "DEERFLOW_THREAD_ENSURE_URL": f"http://{args.gateway_name}:3330/api/deerflow/mobile/threads/ensure",
            "DEERFLOW_THREAD_UPLOAD_URL": f"http://{args.gateway_name}:3330/api/threads/{{thread_id}}/uploads",
        },
    }
    # Preserve only the known business asset mounts, all read-only. Do not copy
    # the source .deer-flow, .langgraph_api, logs, uv cache or Docker config.
    shared = ("/app/models", "/app/skills", "/app/datasets", "/app/config", "/app/extensions_config.json")
    for role, original, name in (("langgraph", graph, args.graph_name), ("gateway", gateway, args.gateway_name)):
        values = service_environment(original, data_dir=runtime / "data", key=key, graph_name=args.graph_name,
                                     iam_endpoint=iam_endpoint, jobs=args.jobs, max_inflight=args.max_inflight, per_user=args.per_user)
        mounts = [bind(snapshot, "/app/backend"), source_mount(original, "/app/backend/.venv")]
        mounts += [source_mount(original, target) for target in shared]
        for original_mount in original["Mounts"]:
            if original_mount["Destination"] == "/mnt/nas/streetview_meta":
                mounts.append(source_mount(original, original_mount["Destination"]))
        mounts += [
            bind(runtime / "data", DATA, readonly=False),
            bind(runtime / "config.onecity.json", "/app/config.onecity.json"),
            bind(runtime / "langgraph.json", "/app/backend/langgraph.json"),
            bind(runtime / "langgraph-runtime", "/app/backend/.langgraph_api", readonly=False),
            bind(runtime / "logs", "/app/logs", readonly=False),
            bind("/var/run/docker.sock", "/var/run/docker.sock"),
        ]
        command = (["/app/backend/.venv/bin/langgraph", "dev", "--no-browser", "--allow-blocking", "--no-reload",
                    "--n-jobs-per-worker", str(args.jobs), "--host", "0.0.0.0", "--port", "3329"]
                   if role == "langgraph" else
                   [PYTHON, "-m", "uvicorn", "app.gateway.app:app", "--host", "0.0.0.0", "--port", "3330", "--no-access-log"])
        env_file = runtime / f"{role}.env"
        secure_write(env_file, env_text(values))
        service = {"role": role, "name": name, "image_id": original["Image"], "env_file": str(env_file),
                   "mounts": mounts, "command": command}
        command_for(plan, service)  # Validate mount serialization before saving.
        plan["services"].append(service)
    plan["file_sha256"] = {
        name: hashlib.sha256((runtime / name).read_bytes()).hexdigest()
        for name in ("config.onecity.json", "langgraph.json", "gateway.env", "langgraph.env", "admission.key", "source-manifest.json")
    }
    secure_write(runtime / "plan.json", json.dumps(plan, ensure_ascii=False, indent=2) + "\n")
    launcher = "#!/bin/sh\nset -eu\nexec " + shlex.join([
        sys.executable, str(Path(__file__).resolve()), "start", "--runtime-dir", str(runtime),
    ]) + "\n"
    secure_write(runtime / "launch.sh", launcher, 0o700)
    return plan


def load_plan(runtime: Path) -> dict:
    if runtime.is_symlink() or runtime.stat().st_uid != os.getuid() or stat.S_IMODE(runtime.stat().st_mode) & 0o077:
        raise DeploymentError("The private bundle directory has unsafe ownership or permissions")
    plan = json.loads((runtime / "plan.json").read_text())
    if plan.get("runtime_dir") != str(runtime.absolute()):
        raise DeploymentError("The prepared bundle must stay at its original absolute path")
    for name, digest in plan["file_sha256"].items():
        path = runtime / name
        if (path.is_symlink() or stat.S_IMODE(path.stat().st_mode) & 0o077
                or hashlib.sha256(path.read_bytes()).hexdigest() != digest):
            raise DeploymentError("A private bundle file changed or has unsafe permissions; prepare a new bundle")
    snapshot = Path(plan["backend_snapshot"])
    expected_sources = json.loads((runtime / "source-manifest.json").read_text())
    actual_sources = {str(path.relative_to(snapshot)): hashlib.sha256(path.read_bytes()).hexdigest()
                      for path in snapshot.rglob("*") if path.is_file() and not path.is_symlink()}
    if (actual_sources != expected_sources or any(path.is_symlink() for path in snapshot.rglob("*"))):
        raise DeploymentError("The prepared backend source snapshot changed; prepare a new bundle")
    return plan


def wait_healthy(service: dict, timeout: int = 120) -> None:
    port, path = (3329, "/ok") if service["role"] == "langgraph" else (3330, "/health")
    probe = f"import urllib.request; urllib.request.urlopen('http://127.0.0.1:{port}{path}',timeout=3).read()"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            run(["docker", "exec", service["name"], PYTHON, "-c", probe], timeout=5)
            return
        except DeploymentError:
            time.sleep(1)
    raise DeploymentError(f"New {service['role']} service did not become healthy; existing services were untouched")


def start(plan: dict) -> None:
    existing_names = set(run(["docker", "ps", "--all", "--format", "{{.Names}}"]).splitlines())
    if any(service["name"] in existing_names for service in plan["services"]):
        raise DeploymentError("A target container already exists; refusing to replace or restart it")
    # Nothing changes in the source stack or OneCity. A failed partial start is
    # left available for diagnosis; the operator may remove the new containers.
    for service in plan["services"]:
        run(command_for(plan, service), timeout=60)
        wait_healthy(service)
        print(f"Healthy private service: {service['name']}")


def summary(plan: dict) -> dict:
    return {key: plan[key] for key in (
        "runtime_dir", "backend_snapshot", "source_file_count", "network", "iam_endpoint", "graph_jobs", "max_inflight", "max_inflight_per_user",
        "sandbox_prefix", "sandbox_base_port", "onecity_switch",
    )} | {"containers": [service["name"] for service in plan["services"]], "published_host_ports": [],
         "production_traffic_switched": False, "source_stack_modified": False}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "start", "status"))
    parser.add_argument("--runtime-dir", type=Path, default=Path.home() / ".local/state/onecity-multiuser")
    parser.add_argument("--repo", type=Path, default=ROOT)
    parser.add_argument("--source-gateway", default="gaozhuohui-deer-flow-gateway")
    parser.add_argument("--source-graph", default="gaozhuohui-deer-flow-langgraph")
    parser.add_argument("--onecity", default="onecityagent-a-dev")
    parser.add_argument("--network", default="onecityagent-net")
    parser.add_argument("--gateway-name", default="onecity-deerflow-gateway")
    parser.add_argument("--graph-name", default="onecity-deerflow-langgraph")
    parser.add_argument("--iam-port", type=int, default=7010)
    parser.add_argument("--sandbox-prefix", default="onecity-multiuser-sandbox")
    parser.add_argument("--sandbox-port", type=int, default=18481)
    parser.add_argument("--jobs", type=int, default=3)
    parser.add_argument("--max-inflight", type=int, default=6)
    parser.add_argument("--per-user", type=int, default=2)
    args = parser.parse_args()
    if any(not NAME_RE.fullmatch(getattr(args, name)) for name in (
        "source_gateway", "source_graph", "onecity", "network", "gateway_name", "graph_name", "sandbox_prefix",
    )):
        parser.error("Docker names and aliases contain unsupported characters")
    if (not 1 <= args.iam_port <= 65535 or not 1 <= args.sandbox_port <= 65000
            or not 1 <= args.jobs <= args.max_inflight or not 1 <= args.per_user <= args.max_inflight
            or args.gateway_name == args.graph_name
            or {args.gateway_name, args.graph_name} & {args.source_gateway, args.source_graph, args.onecity}):
        parser.error("Invalid limits, ports or overlapping container names")
    try:
        plan = prepare(args) if args.action == "prepare" else load_plan(args.runtime_dir.absolute())
        if args.action == "start":
            start(plan)
        if args.action == "status":
            for service in plan["services"]:
                container = inspect_container(service["name"])
                if container["Config"].get("Labels", {}).get(LABEL) != plan["bundle_id"]:
                    raise DeploymentError("A target container belongs to a different bundle")
                print(json.dumps({"container": service["name"], "status": container["State"]["Status"]}))
        print(json.dumps(summary(plan), ensure_ascii=False, indent=2))
        return 0
    except (DeploymentError, OSError, ValueError, KeyError) as exc:
        message = str(exc) if isinstance(exc, DeploymentError) else f"Private deployment operation failed ({type(exc).__name__})"
        print(message, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
