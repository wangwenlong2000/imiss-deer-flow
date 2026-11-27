"""Consistent, checksummed backups and restore into a separate empty site.

The first supported recovery profile is MySQL (local or external), stopped
application writers, and Elasticsearch disabled. ES-enabled sites are rejected
until a coordinated snapshot adapter exists; copying an ES data directory is
never presented as a verified backup. Large immutable assets/NAS are referenced
by digest and mapping, rather than silently included or silently discarded.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import sqlite3
import stat
import tarfile
import tempfile
import uuid

import yaml

SCHEMA_VERSION = 2
PENDING = Path("data/.restore-pending.json")
WRITERS = {"api", "coordinator", "gateway", "graph", "edge"}
ACTIVE_STATES = {"running", "restarting", "paused"}
_MYSQL_ENV = '''set -eu
export MYSQL_PWD="$(cat /run/secrets/mysql_password)"
mysql_client() {
  mysql --host="$MYSQL_HOST" --port="$MYSQL_PORT" --user="$MYSQL_USER" \
    --connect-timeout=15 --default-character-set=utf8mb4 "$MYSQL_DATABASE" "$@"
}
'''
_MIGRATION_QUERY = "SELECT checksum,status FROM platform_schema_migrations WHERE name='onecity.initial.v1'"


def database_tools_service(root: Path, config: dict) -> dict:
    """Compose one-shot client, used for local and external MySQL alike."""
    db = config["mysql"]
    return {
        "image": config["images"]["mysql"], "profiles": ["tools"],
        "networks": ["services"], "secrets": ["mysql_password"],
        "environment": {"MYSQL_HOST": db["host"], "MYSQL_PORT": str(db["port"]),
                        "MYSQL_DATABASE": db["database"], "MYSQL_USER": db["user"]},
        "entrypoint": ["sh"], "command": ["-c", "exit 0"],
    }


def _hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _private_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(path.name + ".tmp-" + uuid.uuid4().hex)
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, sort_keys=True, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _records(output: str) -> list[dict]:
    output = output.strip()
    if not output:
        return []
    if output.startswith("["):
        result = json.loads(output)
        if not isinstance(result, list) or not all(isinstance(x, dict) for x in result):
            raise ValueError("Invalid container status response")
        return result
    return [json.loads(line) for line in output.splitlines() if line.strip()]


def _supported(config: dict) -> None:
    if config["elasticsearch"]["mode"] != "disabled":
        raise RuntimeError("Automatic recovery currently requires Elasticsearch disabled; "
                           "an ES snapshot and coordinated recovery procedure are required for this site")
    if config["mysql"]["mode"] not in {"local", "external"}:
        raise RuntimeError("Unsupported recovery database mode")


def _stopped(root: Path, config: dict, *, compose, docker) -> None:
    result = compose(root, "ps", "--all", "--format", "json")
    active = [item.get("Service", "unknown") for item in _records(result.stdout)
              if item.get("Service") in WRITERS and str(item.get("State", "")).lower() in ACTIVE_STATES]
    prefix = config["project"] + "-sandbox"
    sandboxes = docker("ps", "--all", "--filter", "name=" + prefix,
                       "--format", "{{json .}}")
    active.extend(item.get("Names", "sandbox") for item in _records(sandboxes.stdout)
                  if str(item.get("Names", "")).startswith(prefix)
                  and str(item.get("State", "")).lower() in ACTIVE_STATES)
    if active:
        raise RuntimeError("Stop application writers and this site's dynamic sandboxes before backup: "
                           + ", ".join(active))


def _client(root: Path, *, compose, script: str, directory: Path | None = None,
            writable: bool = False):
    args = ["--profile", "tools", "run", "--rm", "--no-deps", "-T", "--entrypoint", "sh"]
    if directory is not None:
        args.extend(["--volume", str(directory) + ":/backup:" + ("rw" if writable else "ro")])
    args.extend(["database-tools", "-ec", _MYSQL_ENV + script])
    return compose(root, *args)


def _mysql_migration(root: Path, *, compose) -> dict:
    result = _client(root, compose=compose,
                     script='mysql_client --batch --skip-column-names -e "' + _MIGRATION_QUERY + '"')
    lines = result.stdout.strip().splitlines()
    if len(lines) != 1:
        raise RuntimeError("MySQL does not have exactly one supported OneCity migration record")
    fields = lines[0].split("\t")
    if len(fields) != 2 or len(fields[0]) != 64 or any(x not in "0123456789abcdef" for x in fields[0]) or fields[1] != "ready":
        raise RuntimeError("MySQL OneCity migration is not ready or its checksum is invalid")
    return {"name": "onecity.initial.v1", "checksum": fields[0], "status": fields[1]}


def _mysql_server_state(root: Path, *, compose) -> str:
    """A transactional dump is only sufficient with no independent SQL writers."""
    script = ('mysql_client --batch --skip-column-names -e "SELECT VERSION(); '
              "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() "
              "AND ENGINE <> 'InnoDB'; SELECT COUNT(*) FROM information_schema.EVENTS "
              "WHERE EVENT_SCHEMA=DATABASE() AND STATUS='ENABLED'\"")
    result = _client(root, compose=compose, script=script)
    fields = result.stdout.strip().splitlines()
    if len(fields) != 3 or fields[1:] != ["0", "0"]:
        raise RuntimeError("Recovery requires InnoDB tables and no enabled MySQL scheduled events")
    if not fields[0].startswith("8.0."):
        raise RuntimeError("This release supports recovery only for MySQL 8.0.x")
    return fields[0]


def asset_inventory(root: Path) -> list[dict]:
    """Hash immutable site assets without following symlinks out of the site."""
    base = root / "assets"
    if not base.is_dir() or base.is_symlink():
        raise ValueError("A real assets directory is required")
    inventory = []
    for path in sorted(base.rglob("*")):
        if path.is_symlink():
            raise ValueError("Recovery asset inventory refuses symlinks: " + str(path.relative_to(root)))
        if path.is_file():
            inventory.append({"path": path.relative_to(base).as_posix(),
                              "size": path.stat().st_size, "sha256": _hash(path)})
        elif not path.is_dir():
            raise ValueError("Recovery asset inventory refuses special files")
    return inventory


def _snapshot_file(source: Path, destination: Path) -> None:
    """Use SQLite's backup API, including committed WAL data, for SQLite files."""
    with source.open("rb") as stream:
        is_sqlite = stream.read(16) == b"SQLite format 3\x00"
    if not is_sqlite:
        shutil.copy2(source, destination)
        return
    with sqlite3.connect(source.resolve().as_uri() + "?mode=ro", uri=True, timeout=30) as reader:
        with sqlite3.connect(destination) as writer:
            reader.backup(writer)
            if writer.execute("PRAGMA quick_check").fetchone() != ("ok",):
                raise RuntimeError("SQLite backup integrity check failed")
    os.chmod(destination, stat.S_IMODE(source.stat().st_mode) & 0o777)


def _copy_state(root: Path, destination: Path) -> None:
    for name in ("config", "secrets", "data/platform", "data/deerflow", "data/langgraph"):
        source_root = root / name
        if not source_root.is_dir() or source_root.is_symlink():
            raise ValueError("Required state directory missing or symlinked: " + name)
        (destination / name).mkdir(parents=True, exist_ok=True, mode=0o700)
        for source in sorted(source_root.rglob("*")):
            relative = source.relative_to(root)
            if source.is_symlink():
                raise ValueError("Backup refuses symlinked state: " + relative.as_posix())
            target = destination / relative
            if source.is_dir():
                target.mkdir(parents=True, exist_ok=True, mode=stat.S_IMODE(source.stat().st_mode) & 0o777)
            elif source.is_file():
                # SQLite sidecars are included through the SQLite backup API.
                if source.name.endswith(("-wal", "-shm")):
                    primary = source.with_name(source.name.rsplit("-", 1)[0])
                    if primary.is_file():
                        with primary.open("rb") as stream:
                            if stream.read(16) == b"SQLite format 3\x00":
                                continue
                if relative.parts[0] == "secrets" and source.stat().st_mode & 0o077:
                    raise ValueError("Secret permissions are too broad: " + source.name)
                target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                _snapshot_file(source, target)
            else:
                raise ValueError("Backup refuses special state files")


def _file_inventory(directory: Path) -> list[dict]:
    return [{"path": path.relative_to(directory).as_posix(), "size": path.stat().st_size,
             "mode": stat.S_IMODE(path.stat().st_mode), "sha256": _hash(path)}
            for path in sorted(directory.rglob("*")) if path.is_file()]


def _archive(directory: Path, output: Path) -> None:
    with tarfile.open(output, "w:gz", format=tarfile.PAX_FORMAT) as archive:
        for path in sorted(directory.rglob("*")):
            info = archive.gettarinfo(str(path), arcname=path.relative_to(directory).as_posix())
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            if path.is_file():
                with path.open("rb") as stream:
                    archive.addfile(info, stream)
            else:
                archive.addfile(info)
    output.chmod(0o600)


@contextmanager
def _lock(root: Path):
    descriptor = os.open(root / ".recovery.lock", os.O_RDWR | os.O_CREAT, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("Another backup or restore operation is running") from error
        yield
    finally:
        os.close(descriptor)


def save_backup(root: Path, output: Path, *, compose, read_config, docker) -> dict:
    root, output = Path(root).resolve(), Path(output).resolve()
    config = read_config(root)
    _supported(config)
    if (root / PENDING).exists():
        raise RuntimeError("Finish database restoration before creating another backup")
    if output.exists():
        raise ValueError("Backup output already exists; use a new filename")
    if output.is_relative_to(root) and output.relative_to(root).parts[:1] and \
            output.relative_to(root).parts[0] in {"config", "secrets", "data", "assets", "generated"}:
        raise ValueError("Backup output must not be inside live state or assets")
    output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with _lock(root), tempfile.TemporaryDirectory(prefix=".backup-", dir=output.parent) as temporary:
        _stopped(root, config, compose=compose, docker=docker)
        stage = Path(temporary)
        payload = stage / "payload"
        payload.mkdir(mode=0o700)
        server_version = _mysql_server_state(root, compose=compose)
        migration = _mysql_migration(root, compose=compose)
        dump = '''umask 077
mysqldump --host="$MYSQL_HOST" --port="$MYSQL_PORT" --user="$MYSQL_USER" \
  --default-character-set=utf8mb4 --single-transaction \
  --quick --routines --triggers --events --hex-blob --set-gtid-purged=OFF \
  --column-statistics=0 --no-tablespaces --skip-comments "$MYSQL_DATABASE" > /backup/mysql.sql
'''
        _client(root, compose=compose, script=dump, directory=payload, writable=True)
        sql = payload / "mysql.sql"
        if not sql.is_file() or sql.stat().st_size == 0:
            raise RuntimeError("MySQL dump is missing or empty")
        sql.chmod(0o600)
        _copy_state(root, payload)
        assets = asset_inventory(root)
        manifest = {
            "schema_version": SCHEMA_VERSION, "backup_id": uuid.uuid4().hex,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "project": config["project"], "release": config["release"], "source_root": str(root),
            "images": dict(config["images"]), "mysql_mode": config["mysql"]["mode"],
            "mysql_migration": migration, "elasticsearch_mode": "disabled",
            "mysql_server_version": server_version,
            "writers_stopped": True, "mysql_backup": "logical-single-transaction",
            "sqlite_backup": "sqlite-backup-api", "assets_included": False,
            "assets": assets, "nas_included": False,
            "nas_path": config.get("services", {}).get("nas_path"),
            "files": _file_inventory(payload),
        }
        inner = stage / "state.tar.gz"
        _archive(payload, inner)
        manifest["payload_sha256"] = _hash(inner)
        manifest["payload_size"] = inner.stat().st_size
        (stage / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, sort_keys=True))
        temporary_output = output.with_name("." + output.name + ".tmp-" + uuid.uuid4().hex)
        try:
            with tarfile.open(temporary_output, "w:gz") as archive:
                archive.add(inner, arcname="state.tar.gz", recursive=False)
                archive.add(stage / "manifest.json", arcname="manifest.json", recursive=False)
            temporary_output.chmod(0o600)
            # Atomic no-clobber link: a concurrently created output is preserved.
            os.link(temporary_output, output)
        finally:
            temporary_output.unlink(missing_ok=True)
    return {"backup": str(output), "backup_id": manifest["backup_id"],
            "sha256": _hash(output), "files": len(manifest["files"]),
            "assets_included": False, "asset_files": len(assets),
            "nas_included": False, "profile": "mysql-and-local-application-state"}


def _extract_checked(source: Path, stage: Path) -> dict:
    with tarfile.open(source, "r:gz") as archive:
        members = archive.getmembers()
        if len(members) != 2 or {m.name for m in members} != {"state.tar.gz", "manifest.json"} or any(not m.isfile() for m in members):
            raise ValueError("Unsupported or unsafe backup archive")
        meta = archive.getmember("manifest.json")
        if meta.size > 20 * 1024 * 1024:
            raise ValueError("Backup manifest is too large")
        manifest = json.load(archive.extractfile(meta))
        if manifest.get("schema_version") != SCHEMA_VERSION:
            raise ValueError("Unsupported backup schema version")
        if manifest.get("elasticsearch_mode") != "disabled" or manifest.get("writers_stopped") is not True:
            raise ValueError("Backup was not made with a supported consistent recovery profile")
        backup_id = manifest.get("backup_id")
        if not isinstance(backup_id, str) or len(backup_id) != 32 or any(c not in "0123456789abcdef" for c in backup_id):
            raise ValueError("Invalid backup identity")
        inner = stage / "state.tar.gz"
        payload = archive.getmember("state.tar.gz")
        if payload.size != manifest.get("payload_size"):
            raise ValueError("Backup payload size mismatch")
        if payload.size > shutil.disk_usage(stage).free:
            raise ValueError("Insufficient space for backup payload")
        with archive.extractfile(payload) as reader, inner.open("wb") as writer:
            shutil.copyfileobj(reader, writer)
        if _hash(inner) != manifest.get("payload_sha256"):
            raise ValueError("Backup payload checksum mismatch")
    entries = manifest.get("files")
    if not isinstance(entries, list) or any(not isinstance(item, dict) for item in entries):
        raise ValueError("Invalid backup file inventory")
    expected = {item.get("path"): item for item in entries}
    if len(expected) != len(entries):
        raise ValueError("Duplicate file in backup manifest")
    if any(not isinstance(item.get("size"), int) or item["size"] < 0 for item in entries):
        raise ValueError("Invalid backup inventory sizes")
    if sum(item["size"] for item in entries) > shutil.disk_usage(stage).free:
        raise ValueError("Insufficient space for restored state")
    allowed_tops = {"config", "secrets", "data", "mysql.sql"}
    destination = stage / "payload"
    destination.mkdir(mode=0o700)
    seen = set()
    with tarfile.open(inner, "r:gz") as archive:
        for member in archive:
            path = PurePosixPath(member.name)
            if (path.is_absolute() or ".." in path.parts or not path.parts
                    or path.parts[0] not in allowed_tops or str(path) != member.name
                    or member.mode & 0o7000 or not (member.isfile() or member.isdir())):
                raise ValueError("Unsafe backup member")
            if member.name in seen:
                raise ValueError("Duplicate backup member")
            seen.add(member.name)
            if path.parts[0] == "data" and len(path.parts) > 1 and path.parts[1] not in {"platform", "deerflow", "langgraph"}:
                raise ValueError("Unsupported raw database files in backup")
            target = destination.joinpath(*path.parts)
            if member.isdir():
                if member.name in expected:
                    raise ValueError("Backup file was replaced by a directory")
                target.mkdir(parents=True, exist_ok=True, mode=member.mode & 0o777)
                continue
            record = expected.get(member.name)
            if not record or member.size != record.get("size") or member.mode != record.get("mode"):
                raise ValueError("Backup member metadata mismatch")
            if path.parts[0] == "secrets" and member.mode & 0o077:
                raise ValueError("Backup secret permissions are too broad")
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            with archive.extractfile(member) as reader, target.open("xb") as writer:
                shutil.copyfileobj(reader, writer)
            target.chmod(member.mode & 0o777)
            if _hash(target) != record.get("sha256"):
                raise ValueError("Backup member checksum mismatch")
    if {item for item in seen if item in expected} != set(expected):
        raise ValueError("Backup is missing inventory files")
    if any(not destination.joinpath(*PurePosixPath(name).parts).is_file() for name in expected):
        raise ValueError("Backup is missing inventory files")
    for required in ("config/deployment.yaml", "mysql.sql"):
        if required not in expected:
            raise ValueError("Backup is missing required recovery files")
    return manifest


def _empty_target(root: Path) -> None:
    if root.is_symlink():
        raise ValueError("Restore root must not be a symlink")
    if not root.exists():
        return
    for path in root.rglob("*"):
        if (path == root / ".citybrain.lock" and not path.is_symlink()
                and path.is_file() and stat.S_IMODE(path.stat().st_mode) == 0o600):
            continue
        if path.is_symlink() or not path.is_dir():
            raise RuntimeError("Restore target must contain only empty directories")


def restore_backup(root: Path, source: Path, *, read_config, render, project=None,
                   http_port=None, public_url=None) -> dict:
    root, source = Path(root).absolute(), Path(source).resolve()
    if not source.is_file():
        raise ValueError("Backup archive not found")
    _empty_target(root)
    root.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".restore-", dir=root.parent) as temporary:
        stage = Path(temporary)
        manifest = _extract_checked(source, stage)
        payload = stage / "payload"
        path = payload / "config/deployment.yaml"
        config = yaml.safe_load(path.read_text())
        if project is not None:
            config["project"] = project
        if http_port is not None:
            config["http_port"] = http_port
        if public_url is not None:
            config["public_url"] = public_url
        path.write_text(yaml.safe_dump(config, allow_unicode=True, sort_keys=False))
        path.chmod(0o600)
        _supported(config)
        # Validate configuration and secret permissions before populating root.
        read_config(payload)
        for secret in (payload / "secrets").rglob("*"):
            if secret.is_file() and secret.stat().st_mode & 0o077:
                raise ValueError("Restored secret file is not private")
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        root.chmod(0o700)
        for name in ("config", "secrets", "data"):
            shutil.copytree(payload / name, root / name, dirs_exist_ok=True, copy_function=shutil.copy2)
        # A restored site receives a fresh lifecycle identity. The original
        # deployment ID/root/project are retained only as provenance evidence.
        old_installation = root / "data/platform/installation.json"
        if old_installation.exists():
            old_installation.replace(root / "data/platform/restored-source-installation.json")
        for name in ("assets/skills", "assets/models", "assets/datasets", "assets/config",
                     "generated", "backups", "data/mysql", "data/elasticsearch"):
            (root / name).mkdir(parents=True, exist_ok=True, mode=0o700)
        restore_id = manifest["backup_id"]
        recovery_dir = root / "backups" / ("restore-" + restore_id)
        recovery_dir.mkdir(mode=0o700)
        shutil.copy2(payload / "mysql.sql", recovery_dir / "mysql.sql")
        _private_json(recovery_dir / "manifest.json", manifest)
        sql_hash = next(item["sha256"] for item in manifest["files"] if item["path"] == "mysql.sql")
        _private_json(root / PENDING, {"schema_version": 1, "status": "pending",
                                     "backup_id": restore_id, "sql_sha256": sql_hash,
                                     "manifest_path": str((recovery_dir / "manifest.json").relative_to(root))})
        generation = render(root)
    return {"restored_files": True, "database_restored": False, "backup_id": restore_id,
            "root": str(root), "project": config["project"], "generation": generation["generation"],
            "next": "Import the exact immutable assets, then run install to restore MySQL into an empty database",
            "nas_included": False}


def restore_database(root: Path, *, compose, read_config) -> dict:
    root = Path(root).resolve()
    marker = root / PENDING
    if not marker.exists():
        return {"status": "not_required"}
    with _lock(root):
        pending = json.loads(marker.read_text())
        if pending.get("schema_version") != 1 or pending.get("status") not in {"pending", "importing", "imported"}:
            raise ValueError("Unsupported database restore state")
        if pending["status"] == "importing":
            raise RuntimeError("A database import was interrupted; preserve diagnostics and restore into a fresh empty database")
        manifest_path = PurePosixPath(pending.get("manifest_path", ""))
        if manifest_path.is_absolute() or ".." in manifest_path.parts or manifest_path.parts[:1] != ("backups",):
            raise ValueError("Unsafe restore manifest reference")
        manifest_file = root.joinpath(*manifest_path.parts)
        if manifest_file.is_symlink() or not manifest_file.resolve().is_relative_to(root):
            raise ValueError("Restore manifest must be a regular file")
        manifest = json.loads(manifest_file.read_text())
        sql = manifest_file.parent / "mysql.sql"
        if not sql.is_file() or sql.is_symlink() or _hash(sql) != pending["sql_sha256"]:
            raise ValueError("Restore SQL checksum mismatch")
        config = read_config(root)
        _supported(config)
        if config["images"]["mysql"] != manifest["images"]["mysql"]:
            raise ValueError("Restore requires the backed-up MySQL image version")
        if _mysql_server_state(root, compose=compose) != manifest.get("mysql_server_version"):
            raise ValueError("Restore requires the backed-up MySQL server version")
        if asset_inventory(root) != manifest["assets"]:
            raise ValueError("Restore assets differ from backup; import the exact referenced release assets")
        if pending["status"] == "pending":
            check = _client(root, compose=compose, script='mysql_client --batch --skip-column-names -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE()"')
            if check.stdout.strip() != "0":
                raise RuntimeError("Restore refuses a non-empty MySQL database")
            pending["status"] = "importing"
            _private_json(marker, pending)
            _client(root, compose=compose, directory=sql.parent,
                    script="mysql_client < /backup/mysql.sql")
            pending["status"] = "imported"
            _private_json(marker, pending)
        if _mysql_migration(root, compose=compose) != manifest["mysql_migration"]:
            raise RuntimeError("Restored MySQL migration record differs from backup")
        # Full schema checks are also run by install's normal migration step.
        _private_json(root / "data/platform/last-restore.json", {
            "backup_id": pending["backup_id"], "database_restored": True,
            "completed_at": datetime.now(timezone.utc).isoformat(), "assets_verified": True,
        })
        marker.unlink()
    return {"status": "restored", "backup_id": pending["backup_id"], "assets_verified": True}
