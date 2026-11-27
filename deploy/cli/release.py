"""Content-addressed release directories and secret-free acceptance evidence.

Packaging reads the selected checkout, not just committed Git files. The content
inventory is authoritative for a dirty worktree. No site secrets, generated
configuration, mutable state or ambient environment variables enter a release.
"""
from __future__ import annotations

from datetime import datetime, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import tempfile

from deploy.cli.configuration import SOURCE, read_config
from deploy.cli.preflight import inspect_images

SCHEMA_VERSION = 1
SCOPE = "basic-online-api"
ROOT_FILES = {
    "citybrain", "README.md", "LICENSE", "SECURITY.md", "Makefile",
    ".dockerignore", ".gitignore", ".gitattributes", "config.example.yaml",
    "extensions_config.example.json",
}
TREES = (
    "backend", "deploy", "skills", "config/compliance", "models/compliance",
    "contracts", "docker/sandbox", "apps/onecityagent/onecityagent",
    "apps/onecityagent/web", "apps/onecityagent/tests", "apps/onecityagent/docs",
    "apps/onecityagent/scripts", "docs/installation", "docs/architecture",
    "docs/acceptance",
)
SINGLE_FILES = {
    "scripts/check_skill_runtime.py", "scripts/build_skill_runtime_manifest.py",
    "scripts/onecity.sh", "scripts/tests/test_skill_runtime.py",
    "apps/onecityagent/README.md", "apps/onecityagent/app.py",
    "apps/onecityagent/pyproject.toml", "apps/onecityagent/requirements-core.txt",
    "apps/onecityagent/requirements-core.lock",
    "docs/项目架构整理与易安装改造计划_20261126.md",
    "docs/跨机器迁移与Docker部署指南_20261126.md",
    "docs/Skill依赖版本盘点_20261126.md",
    "docs/compliance/onecity_deerflow_compliance_contract.md",
    "docs/compliance/new_detector_onboarding_guide.md",
    "services/streetmodel/models.json",
    "tools/opengrep/download-apiiro-rules.sh",
    "tools/opengrep/download-opengrep-assets.sh",
    "tools/opengrep/download-trailofbits-rules.sh",
}
# These checked-in catalogues are immutable Skill assets, not site databases.
# Keep the exception exact: other *.db files and all runtime state stay blocked.
STATIC_DATABASE_FILES = {
    "skills/custom/policies-regulations/china-legal-consultation/references/data/case_types.db",
    "skills/custom/policies-regulations/china-legal-issue-analysis/references/data/case_types.db",
}
PUBLIC_SECRET_RULE_ROOT = "skills/custom/program_snippet/opengrep-compliance/rules/official/opengrep-rules/generic/secrets"
PUBLIC_SECRET_RULE_DIRECTORIES = {
    PUBLIC_SECRET_RULE_ROOT,
    PUBLIC_SECRET_RULE_ROOT + "/gitleaks",
    PUBLIC_SECRET_RULE_ROOT + "/security",
}
BLOCKED_PARTS = {
    ".git", ".venv", "venv", "node_modules", "__pycache__", ".cache",
    ".pytest_cache", ".mypy_cache", ".ruff_cache", ".next", ".umi",
    ".umi-production", ".langgraph_api", ".deer-flow", ".deerflow-no-nginx",
    ".runtime", ".playwright-browsers", "outputs", "logs", "log", "secrets",
    "backups", "generated", "dist", "coverage", "htmlcov", ".idea", ".vscode",
}
BLOCKED_NAMES = {
    "config.yaml", "config.local.yaml", "config.toml", "extensions_config.json",
    "mcp_config.json", "credentials.json", "credentials.yaml", "credentials.yml",
    "id_rsa", "id_ed25519", ".coverage", ".npmrc", ".pypirc", ".netrc",
    "npm-debug.log", "tsconfig.tsbuildinfo",
}
BLOCKED_SUFFIXES = {
    ".pyc", ".pyo", ".log", ".db", ".db-wal", ".db-shm", ".sqlite",
    ".sqlite3", ".sqlite3-wal", ".sqlite3-shm", ".pem", ".key", ".p12",
    ".pfx", ".keystore", ".bak", ".swp", ".swo", ".tar", ".gz", ".zip",
}
REQUIRED_FILES = {
    "citybrain", "config.example.yaml", "config/compliance/detectors.yaml",
    "config/compliance/policy_matrix.yaml", "models/compliance/ml_detector_0624_fresh.json",
    "deploy/cli/citybrain.py", "deploy/cli/configuration.py", "deploy/cli/release.py",
    "deploy/config/deployment.schema.json", "deploy/config/deployment.example.yaml",
    "deploy/requirements-toolbox.lock", "deploy/requirements-toolbox.txt",
    "deploy/images/OneCity.Dockerfile", "deploy/images/DeerFlow.Dockerfile",
    "deploy/images/Edge.Dockerfile", "deploy/images/Toolbox.Dockerfile",
    "apps/onecityagent/requirements-core.lock", "apps/onecityagent/web/package.json",
    "apps/onecityagent/web/package-lock.json", "backend/pyproject.toml", "backend/uv.lock",
    "README.md", "backend/README.md", "apps/onecityagent/README.md",
    "apps/onecityagent/docs/production-runtime.md",
    "backend/packages/harness/pyproject.toml", "skills/runtime-manifest.json",
    "skills/skill_runtime.schema.json",
}
PRIVATE_KEY = re.compile(rb"-----BEGIN (?:RSA |EC |OPENSSH |ENCRYPTED )?PRIVATE KEY-----\r?\n[A-Za-z0-9+/=]{32,}")
LIVE_KEY = re.compile(rb"\bsk-[A-Za-z0-9_-]{32,}\b")
VERSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$")
HEX = re.compile(r"^[0-9a-f]{64}$")
IMAGE_ARCHIVE = "images/application-images.tar.gz"
EVIDENCE_SUMMARIES = {"preflight.json", "acceptance-summary.json", "recovery-summary.json",
                      "build-summary.json", "release-check.json"}


def _now():
    return datetime.now(timezone.utc).isoformat()


def _json_bytes(value):
    return (json.dumps(value, sort_keys=True, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


def _hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _relative(value):
    if not isinstance(value, str):
        raise ValueError("Unsafe release path")
    path = PurePosixPath(value)
    if "\\" in value or "\n" in value or "\r" in value or \
            path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("Unsafe release path")
    if path.as_posix() != value:
        raise ValueError("Release path is not canonical")
    return path


def _blocked(path):
    # The vendored scanner's public YAML rules happen to live in a directory
    # named "secrets". Only those rule paths may bypass that directory name.
    public_rule = path.as_posix() in PUBLIC_SECRET_RULE_DIRECTORIES or (
        path.parent.as_posix() in PUBLIC_SECRET_RULE_DIRECTORIES and path.suffix in {".yaml", ".yml"})
    blocked_parts = BLOCKED_PARTS - {"secrets"} if public_rule else BLOCKED_PARTS
    return any(part in blocked_parts or part.startswith(".env") or ".bak-" in part or ".bak." in part
               for part in path.parts) or path.name in BLOCKED_NAMES or \
        any(path.name.endswith(suffix) for suffix in BLOCKED_SUFFIXES
            if not (suffix == ".db" and path.as_posix() in STATIC_DATABASE_FILES))


def _allowed(path):
    name = path.as_posix()
    return not _blocked(path) and (name in ROOT_FILES or name in SINGLE_FILES or name in STATIC_DATABASE_FILES or
        any(name == tree or name.startswith(tree + "/") for tree in TREES))


def _source_files(source):
    selected = []
    for directory, directories, filenames in os.walk(source, followlinks=False):
        relative_dir = Path(directory).relative_to(source)
        keep = []
        for name in sorted(directories):
            relative = PurePosixPath((relative_dir / name).as_posix())
            if _blocked(relative):
                continue
            text = relative.as_posix()
            possible = _allowed(relative) or any(tree.startswith(text + "/") for tree in TREES) or \
                any(item.startswith(text + "/") for item in SINGLE_FILES)
            if not possible:
                continue
            if (Path(directory) / name).is_symlink():
                raise ValueError("Release source contains a redirected directory: " + text)
            keep.append(name)
        directories[:] = keep
        for name in sorted(filenames):
            path = Path(directory) / name
            relative = PurePosixPath(path.relative_to(source).as_posix())
            if not _allowed(relative):
                continue
            _relative(relative.as_posix())
            if path.is_symlink() or not stat.S_ISREG(path.lstat().st_mode):
                raise ValueError("Release source contains a non-regular file: " + relative.as_posix())
            selected.append(path)
    return sorted(selected)


def _inventory(directory, paths=None):
    paths = paths if paths is not None else sorted(p for p in directory.rglob("*") if p.is_file())
    result = []
    for path in paths:
        mode = stat.S_IMODE(path.stat().st_mode)
        result.append({"path": path.relative_to(directory).as_posix(), "size": path.stat().st_size,
                       "mode": 0o755 if mode & 0o111 else 0o644, "sha256": _hash(path)})
    return result


def _content_hash(files):
    return hashlib.sha256("".join(
        f"{item['sha256']} {item['size']} {item['mode']:o} {item['path']}\n"
        for item in sorted(files, key=lambda item: item["path"])
    ).encode("utf-8")).hexdigest()


def _git_info(source):
    unavailable = {"git_metadata_available": False, "commit": None, "dirty": None}
    try:
        commit = subprocess.run(["git", "-C", str(source), "rev-parse", "HEAD"],
                                text=True, capture_output=True)
        if commit.returncode or not commit.stdout.strip():
            return unavailable
        status = subprocess.run(["git", "-C", str(source), "status", "--porcelain"],
                                text=True, capture_output=True)
        if status.returncode:
            return unavailable
    except OSError:
        # The delivery toolbox deliberately does not require Git, and an
        # extracted release is not a checkout. Neither condition proves clean.
        return unavailable
    return {"git_metadata_available": True, "commit": commit.stdout.strip(),
            "dirty": bool(status.stdout.strip())}


def _copy_source(path, destination):
    before = path.stat()
    destination.parent.mkdir(parents=True, exist_ok=True)
    carry = b""
    with path.open("rb") as source, destination.open("xb") as target:
        while chunk := source.read(1024 * 1024):
            scan = carry + chunk
            if PRIVATE_KEY.search(scan) or LIVE_KEY.search(scan):
                raise ValueError("Potential private credential in release source: " + path.name)
            target.write(chunk)
            carry = scan[-256:]
    after = path.stat()
    if (before.st_size, before.st_mtime_ns, before.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ino):
        raise RuntimeError("Source changed while packaging; retry after editing has finished")
    destination.chmod(0o755 if before.st_mode & 0o111 else 0o644)


def _compatibility(source, files):
    by_name = {item["path"]: item["sha256"] for item in files}
    catalogue = [item for item in files if item["path"] == "deploy/migrations/onecity.py" or
                 item["path"].startswith("apps/onecityagent/onecityagent/models/")]
    return {
        "deployment_schema": 1, "data_profile": "mysql-elasticsearch-disabled",
        "onecity_migration": {"id": "onecity.initial.v1", "implementation_sha256": by_name.get("deploy/migrations/onecity.py"),
                              "catalogue_implementation_sha256": _content_hash(catalogue)},
        "gateway_migration": {"id": "gateway.initial.v1", "implementation_sha256": by_name.get("deploy/migrations/gateway.py")},
        "checkpoint": {"implementation": "single-process-langgraph-dev",
                       "backend_lock_sha256": by_name.get("backend/uv.lock")},
        "in_place_update_supported": False,
        "note": "Fresh install and separately verified restore only; no reviewed in-place data/checkpoint compatibility claim",
    }


def _image_inventory(config, docker):
    images, errors = inspect_images(config, docker)
    toolbox = os.environ.get("CITYBRAIN_TOOLBOX_IMAGE", "citybrain-toolbox:local")
    result = docker("image", "inspect", toolbox, check=False)
    if result.returncode:
        errors.append("missing image: toolbox")
    else:
        item = json.loads(result.stdout)[0]
        if (item.get("Os"), item.get("Architecture")) != ("linux", "amd64"):
            errors.append("unsupported image platform: toolbox")
        images["toolbox"] = {"reference": toolbox, "id": item["Id"], "repo_digests": item.get("RepoDigests", [])}
    if errors:
        raise RuntimeError("Cannot publish a release with unresolved images: " + "; ".join(errors))
    images["sandbox"]["provenance"] = {
        "kind": "external-compatibility-asset", "clean_source_build_verified": False,
        "note": "The inspected compatibility sandbox is an external pre-existing asset; a local alias is not a clean build from this release source",
    }
    return images


def _assert_image_inventory(images, docker):
    for item in images.values():
        inspected = docker("image", "inspect", item["reference"], check=False)
        if inspected.returncode or json.loads(inspected.stdout)[0].get("Id") != item["id"]:
            raise RuntimeError("Image identity changed while packaging; repeat after builds have finished")


def _save_images(archive, references, *, process_factory=None):
    """Stream docker save through gzip without a second uncompressed copy."""
    factory = process_factory or subprocess.Popen
    with tempfile.TemporaryFile() as diagnostics:
        process = factory(["docker", "save", *references], stdout=subprocess.PIPE,
                          stderr=diagnostics, stdin=subprocess.DEVNULL)
        try:
            if process.stdout is None:
                raise RuntimeError("Docker image export did not expose a byte stream")
            with archive.open("xb") as output:
                # mtime=0 and no original filename keep archive metadata stable.
                with gzip.GzipFile(filename="", fileobj=output, mode="wb", compresslevel=1, mtime=0) as compressed:
                    shutil.copyfileobj(process.stdout, compressed, length=1024 * 1024)
            process.stdout.close()
            if process.wait() != 0:
                raise RuntimeError("Docker image export failed; private diagnostics were not exported")
            if archive.stat().st_size <= 20:
                raise RuntimeError("Docker image export produced an empty archive")
        except BaseException:
            if process.stdout is not None:
                process.stdout.close()
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            archive.unlink(missing_ok=True)
            raise


def _validate_output(output, source=None, site=None):
    output = Path(output).absolute()
    if ".." in output.parts:
        raise ValueError("Output path must be canonical")
    if output.is_symlink() or output.exists():
        raise ValueError("Output already exists or is redirected; choose a new directory")
    if any(parent.is_symlink() for parent in output.parents):
        raise ValueError("Output parent must not be a symlink")
    if source and (output == source or (output.is_relative_to(source) and
            any(output.relative_to(source).as_posix() == tree or output.relative_to(source).as_posix().startswith(tree + "/") for tree in TREES))):
        raise ValueError("Release output must not be inside packaged source trees")
    if site and output.is_relative_to(site) and output.relative_to(site).parts[:1] and \
            output.relative_to(site).parts[0] in {"config", "secrets", "data", "assets", "generated", "evidence"}:
        raise ValueError("Output must not be inside live configuration, state, assets or input evidence")
    return output


def pack_release(root, output, version, *, docker, source=None, include_images=False):
    root = Path(root).resolve()
    source = Path(source or os.environ.get("CITYBRAIN_SOURCE", SOURCE)).resolve()
    if not isinstance(version, str) or not VERSION.fullmatch(version):
        raise ValueError("A safe explicit release version is required")
    output = _validate_output(output, source=source, site=root)
    config = read_config(root)
    if config["capabilities"] != ["basic"] or config["elasticsearch"]["mode"] != "disabled":
        raise RuntimeError("This release profile covers basic online API capability with Elasticsearch disabled")
    paths = _source_files(source)
    names = {path.relative_to(source).as_posix() for path in paths}
    if not REQUIRED_FILES.issubset(names):
        raise ValueError("Release source is incomplete: " + ", ".join(sorted(REQUIRED_FILES - names)))
    images = _image_inventory(config, docker)
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".citybrain-release-", dir=output.parent) as temporary:
        stage = Path(temporary) / "package"
        stage.mkdir()
        for path in paths:
            _copy_source(path, stage / path.relative_to(source))
        source_files = _inventory(stage)
        if _inventory(source, _source_files(source)) != source_files:
            raise RuntimeError("Source changed while packaging; retry after editing has finished")
        image_archive = None
        if include_images:
            archive = stage / IMAGE_ARCHIVE
            archive.parent.mkdir()
            references = sorted({item["reference"] for item in images.values()})
            _assert_image_inventory(images, docker)
            _save_images(archive, references)
            if archive.is_symlink() or not archive.is_file() or not archive.stat().st_size:
                raise RuntimeError("Docker image archive was not created")
            archive.chmod(0o644)
            _assert_image_inventory(images, docker)
            image_archive = {"path": IMAGE_ARCHIVE, "sha256": _hash(archive), "size": archive.stat().st_size,
                             "format": "docker-save", "compression": "gzip",
                             "load": "gzip -dc images/application-images.tar.gz | docker load"}
        files = _inventory(stage)
        locks = [item for item in source_files if item["path"].endswith((".lock", "package-lock.json", "requirements-core.txt", "requirements-toolbox.txt"))]
        assets = [item for item in source_files if item["path"].startswith(("skills/", "models/compliance/", "config/compliance/"))]
        manifest = {
            "schema_version": SCHEMA_VERSION, "version": version, "created_at": _now(),
            "scope": SCOPE, "platform": "linux/amd64", "external_chat_model_required": True,
            "source": {**_git_info(source), "content_sha256": _content_hash(source_files), "file_count": len(source_files)},
            "compatibility": _compatibility(source, source_files), "images": images,
            "images_archive": image_archive, "dependency_files": locks,
            "assets": {"file_count": len(assets), "content_sha256": _content_hash(assets), "business_datasets_included": False},
            "acceptance": {"delivery_accepted": False, "reason": "A package integrity check does not replace live business and separate-machine recovery acceptance"},
            "files": files,
        }
        (stage / "manifest.json").write_bytes(_json_bytes(manifest))
        entries = [*files, {"path": "manifest.json", "sha256": _hash(stage / "manifest.json")}]
        (stage / "checksums.sha256").write_text("".join(f"{item['sha256']}  {item['path']}\n" for item in sorted(entries, key=lambda item: item["path"])))
        for directory, _, _ in os.walk(stage):
            Path(directory).chmod(0o755)
        if output.exists():
            raise ValueError("Output was created concurrently")
        stage.rename(output)
    return {"passed": True, "release": version, "path": str(output), "scope": SCOPE,
            "source_content_sha256": manifest["source"]["content_sha256"], "file_count": len(files),
            "images_included": include_images, "delivery_accepted": False}


def check_release(directory):
    directory = Path(directory).absolute()
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError("Release must be a real directory")
    for parent in directory.parents:
        if parent.is_symlink():
            raise ValueError("Release parent must not be redirected")
    manifest_path = directory / "manifest.json"
    checksums = directory / "checksums.sha256"
    if not manifest_path.is_file() or not checksums.is_file() or manifest_path.is_symlink() or checksums.is_symlink():
        raise ValueError("Release manifest and checksums are required")
    if manifest_path.stat().st_size > 16 * 1024 * 1024 or checksums.stat().st_size > 8 * 1024 * 1024:
        raise ValueError("Release metadata is too large")
    manifest = json.loads(manifest_path.read_text())
    if not isinstance(manifest, dict) or manifest.get("schema_version") != SCHEMA_VERSION or manifest.get("scope") != SCOPE or not VERSION.fullmatch(str(manifest.get("version", ""))):
        raise ValueError("Unsupported release manifest")
    images = manifest.get("images")
    if not isinstance(images, dict) or not {"onecity", "deerflow", "edge", "sandbox", "mysql", "toolbox"}.issubset(images):
        raise ValueError("Release image identities are incomplete")
    for image in images.values():
        if not isinstance(image, dict) or not isinstance(image.get("reference"), str) or \
                not image["reference"] or any(c.isspace() for c in image["reference"]) or \
                not re.fullmatch(r"sha256:[0-9a-f]{64}", str(image.get("id", ""))) or \
                not isinstance(image.get("repo_digests"), list):
            raise ValueError("Invalid release image identity")
    listed = manifest.get("files")
    if not isinstance(listed, list) or not listed or len(listed) > 100000:
        raise ValueError("Invalid release file inventory")
    by_name = {}
    for item in listed:
        if not isinstance(item, dict):
            raise ValueError("Invalid release file record")
        name = _relative(item.get("path")).as_posix()
        image_payload = name == IMAGE_ARCHIVE and bool(manifest.get("images_archive"))
        if not (_allowed(PurePosixPath(name)) or image_payload) or name in by_name or not HEX.fullmatch(str(item.get("sha256", ""))) or \
                not isinstance(item.get("size"), int) or isinstance(item["size"], bool) or item["size"] < 0 or item.get("mode") not in {0o644, 0o755}:
            raise ValueError("Unsafe, duplicate or invalid release file record")
        by_name[name] = item
    if not REQUIRED_FILES.issubset(by_name):
        raise ValueError("Release omits required implementation files")
    checksum_entries = {}
    for line in checksums.read_text().splitlines():
        if "  " not in line:
            raise ValueError("Invalid checksum line")
        digest, name = line.split("  ", 1)
        _relative(name)
        if not HEX.fullmatch(digest) or name in checksum_entries:
            raise ValueError("Invalid or duplicate checksum entry")
        checksum_entries[name] = digest
    expected = {**{name: item["sha256"] for name, item in by_name.items()}, "manifest.json": _hash(manifest_path)}
    errors = []
    if checksum_entries != expected:
        errors.append("checksum_inventory_mismatch")
    actual = set()
    for path in directory.rglob("*"):
        if path.is_symlink() or not (path.is_file() or path.is_dir()):
            raise ValueError("Release contains a symlink or special file")
        if path.is_file():
            actual.add(path.relative_to(directory).as_posix())
    if actual != set(by_name) | {"manifest.json", "checksums.sha256"}:
        errors.append("unlisted_or_missing_files")
    for name, item in by_name.items():
        path = directory / name
        if not path.is_file():
            errors.append("missing:" + name)
        elif path.stat().st_size != item["size"] or _hash(path) != item["sha256"]:
            errors.append("content_changed:" + name)
        elif stat.S_IMODE(path.stat().st_mode) != item["mode"]:
            errors.append("mode_changed:" + name)
    source_files = [item for item in listed if item["path"] != IMAGE_ARCHIVE]
    if manifest.get("source", {}).get("content_sha256") != _content_hash(source_files):
        errors.append("source_inventory_hash_mismatch")
    if manifest.get("source", {}).get("file_count") != len(source_files):
        errors.append("source_inventory_count_mismatch")
    if any(item not in source_files for item in manifest.get("dependency_files", [])):
        errors.append("dependency_inventory_mismatch")
    assets = [item for item in source_files if item["path"].startswith(("skills/", "models/compliance/", "config/compliance/"))]
    if manifest.get("assets", {}).get("content_sha256") != _content_hash(assets) or manifest.get("assets", {}).get("file_count") != len(assets):
        errors.append("asset_inventory_mismatch")
    archive = manifest.get("images_archive")
    if archive and (not isinstance(archive, dict) or archive.get("path") != IMAGE_ARCHIVE or any(
            archive.get(key) != by_name.get(IMAGE_ARCHIVE, {}).get(key) for key in ("sha256", "size"))):
        errors.append("image_archive_inventory_mismatch")
    return {"passed": not errors, "release": manifest["version"], "path": str(directory),
            "scope": SCOPE, "file_count": len(listed), "errors": errors,
            "images_checked": False, "business_acceptance_checked": False}


_SENSITIVE_NAMES = {"password", "token", "access_token", "authorization", "cookie", "set-cookie",
    "api_key", "secret", "secret_key", "credentials", "private_key", "model_key", "email",
    "username", "user_name",
    "stdout", "stderr", "rawstdout", "rawstderr",
    "request_body", "response_body", "prompt", "query", "content", "raw", "body"}


def _redact(value, key=""):
    normalized = key.lower().replace("-", "_")
    if normalized in _SENSITIVE_NAMES or any(normalized.endswith(ending) for ending in
            ("_password", "_token", "_secret", "_api_key", "_credentials", "credentials_file", "_email")):
        return "[REDACTED]"
    if isinstance(value, dict):
        return {str(k): _redact(v, str(k)) for k, v in value.items()}
    if isinstance(value, list):
        return [_redact(item) for item in value]
    if isinstance(value, str):
        if PRIVATE_KEY.search(value.encode()) or LIVE_KEY.search(value.encode()):
            return "[REDACTED]"
        value = re.sub(r"(https?://)[^/@\s]+:[^/@\s]+@", r"\1[REDACTED]@", value)
        value = re.sub(r"(https?://[^\s?#]+)[?#][^\s]*", r"\1?[REDACTED]", value)
        return re.sub(r"(?i)(password|token|secret|authorization|api[_-]?key)\s*[:=]\s*[^\s,;]+", r"\1=[REDACTED]", value)
    return value


def export_evidence(root, output):
    root = Path(root).resolve()
    output = _validate_output(output, site=root)
    candidates = []
    evidence = root / "evidence"
    if evidence.is_symlink():
        raise ValueError("Evidence directory is redirected")
    if evidence.is_dir():
        for path in sorted(evidence.rglob("*")):
            if path.is_symlink() or not (path.is_file() or path.is_dir()):
                raise ValueError("Evidence contains a redirected or special file")
            relative = path.relative_to(evidence).as_posix()
            approved = relative in EVIDENCE_SUMMARIES or bool(re.fullmatch(
                r"verification/[A-Za-z0-9][A-Za-z0-9_.-]{0,127}/report\.json", relative))
            if path.is_file() and approved and not path.name.endswith(".private.json") and \
                    path.name != "cleanup-state.json" and not _blocked(PurePosixPath(relative)):
                candidates.append((path, "reports/" + relative))
    installation = root / "data/platform/installation.json"
    if installation.is_symlink():
        raise ValueError("Installation identity is redirected")
    if installation.is_file():
        candidates.append((installation, "installation.json"))
    if not candidates:
        raise ValueError("No structured installation or verification evidence exists")
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".citybrain-evidence-", dir=output.parent) as temporary:
        stage = Path(temporary) / "evidence"
        stage.mkdir()
        for path, name in candidates:
            if path.stat().st_size > 8 * 1024 * 1024:
                raise ValueError("Evidence JSON is too large; export a bounded summary")
            document = json.loads(path.read_text())
            target = stage / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(_json_bytes(_redact(document)))
        inventory = _inventory(stage)
        (stage / "manifest.json").write_bytes(_json_bytes({
            "schema_version": 1, "created_at": _now(), "scope": SCOPE,
            "redacted": True, "site_secrets_included": False, "files": inventory,
            "note": "Reports are preserved with their pass/fail/coverage flags; export does not grant acceptance",
        }))
        all_files = _inventory(stage)
        (stage / "checksums.sha256").write_text("".join(f"{item['sha256']}  {item['path']}\n" for item in all_files))
        for directory, _, files in os.walk(stage):
            Path(directory).chmod(0o755)
            for name in files:
                (Path(directory) / name).chmod(0o644)
        if output.exists():
            raise ValueError("Output was created concurrently")
        stage.rename(output)
    return {"passed": True, "path": str(output), "structured_reports": len(candidates),
            "redacted": True, "site_secrets_included": False}


def _update_plan(root, target, *, direction):
    checked = check_release(target)
    if not checked["passed"]:
        return {"passed": False, "apply_supported": False, "blockers": ["target_release_integrity_failed"], "check": checked}
    root, target = Path(root), Path(target)
    manifest = json.loads((target / "manifest.json").read_text())
    identity = root / "data/platform/installation.json"
    installed = json.loads(identity.read_text()) if identity.is_file() and not identity.is_symlink() else {}
    blockers = []
    if installed.get("status") != "ready":
        blockers.append("installed_site_is_not_recorded_ready")
    compatibility = installed.get("compatibility")
    if not compatibility:
        blockers.append("installed_migration_and_checkpoint_compatibility_not_recorded")
    elif compatibility != manifest.get("compatibility"):
        blockers.append("migration_or_checkpoint_compatibility_differs")
    # The current manifest deliberately makes no unproven in-place update claim.
    # Enabling this requires actual schema/checkpoint inspection, a consistent
    # backup, drained writers and a verified restoration/health rollback record.
    blockers.append("reviewed_consistent_backup_and_health_rollback_workflow_not_available")
    return {"passed": True, "direction": direction, "from": installed.get("release"),
            "to": manifest["version"], "target": str(target), "apply_supported": False,
            "blockers": blockers, "writes_performed": False,
            "alternative": "Stop writers, create a consistent backup and restore into a separate installation using the reviewed target release"}


def release_command(root, args, *, docker, compose, render, verify):
    if args.command == "release":
        if args.action == "pack":
            if not args.output or not args.version:
                raise ValueError("release pack requires --version and --output")
            return pack_release(root, args.output, args.version, docker=docker,
                                include_images=args.include_images)
        if args.action == "check":
            if not args.path:
                raise ValueError("release check requires --path")
            return check_release(args.path)
    if args.command == "export-evidence":
        return export_evidence(root, args.output)
    if args.command in {"upgrade", "rollback"}:
        plan = _update_plan(root, args.to, direction=args.command)
        if args.action == "apply":
            raise RuntimeError("In-place " + args.command + " is disabled: " + "; ".join(plan["blockers"]))
        return plan
    raise ValueError("Unsupported release operation")
