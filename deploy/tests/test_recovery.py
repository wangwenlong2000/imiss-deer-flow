"""Recovery tests use real archives and SQLite, with no Docker/service writes."""
from __future__ import annotations

import io
import json
from pathlib import Path
import shutil
import sqlite3
import tarfile
import tempfile
from types import SimpleNamespace
import unittest

import yaml

from deploy.cli.configuration import initialize, read_config, render
from deploy.cli.recovery import (
    PENDING, _extract_checked, _hash, database_tools_service,
    restore_backup, restore_database, save_backup,
)
from deploy.migrations.gateway import migrate


class FakeDocker:
    def __init__(self):
        self.writers = []
        self.sandboxes = []
        self.imported = False
        self.interrupt_import = False
        self.table_count = "0"
        self.migration = "a" * 64 + "\tready\n"
        self.server_state = "8.0.39\n0\n0\n"

    def docker(self, *args, **kwargs):
        return SimpleNamespace(stdout=json.dumps(self.sandboxes), returncode=0)

    def compose(self, root, *args, **kwargs):
        if args[0] == "ps":
            return SimpleNamespace(stdout=json.dumps(self.writers), returncode=0)
        script = args[-1]
        if "SELECT VERSION()" in script:
            return SimpleNamespace(stdout=self.server_state, returncode=0)
        if "mysqldump " in script:
            directory = Path(args[args.index("--volume") + 1].rsplit(":/backup:", 1)[0])
            (directory / "mysql.sql").write_text("CREATE TABLE synthetic(id INT);\nINSERT INTO synthetic VALUES(42);\n")
            return SimpleNamespace(stdout="", returncode=0)
        if "SELECT COUNT(*)" in script:
            return SimpleNamespace(stdout=self.table_count + "\n", returncode=0)
        if "mysql_client <" in script:
            if self.interrupt_import:
                raise RuntimeError("simulated import interruption")
            self.imported = True
            return SimpleNamespace(stdout="", returncode=0)
        if "SELECT checksum,status" in script:
            return SimpleNamespace(stdout=self.migration, returncode=0)
        raise AssertionError("Unexpected command")


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        self.root = self.base / "source"
        initialize(self.root)
        path = self.root / "config/deployment.yaml"
        config = yaml.safe_load(path.read_text())
        config["bootstrap_admin"]["email"] = "admin@example.test"
        path.write_text(yaml.safe_dump(config, sort_keys=False))
        (self.root / "secrets/model.key").write_text("synthetic-test-key")
        (self.root / "assets/skills/demo").mkdir()
        (self.root / "assets/skills/demo/SKILL.md").write_text("# test asset\n")
        (self.root / "data/langgraph/checkpoint.bin").write_bytes(b"checkpoint-state")
        (self.root / "data/deerflow/thread-file.txt").write_text("owned-user-artifact")
        (self.root / "data/platform/installation.json").write_text(json.dumps({
            "installation_id": "original-install", "root": str(self.root), "project": "citybrain",
        }))
        migrate(home=self.root / "data/deerflow")
        render(self.root)
        self.fake = FakeDocker()
        self.archive = self.base / "site-backup.tar.gz"

    def backup(self):
        return save_backup(self.root, self.archive, compose=self.fake.compose,
                           docker=self.fake.docker, read_config=read_config)

    def test_mysqldump_does_not_receive_mysql_client_only_connect_timeout(self):
        commands = []

        def compose(root, *args, **kwargs):
            commands.append(args)
            return self.fake.compose(root, *args, **kwargs)

        save_backup(self.root, self.archive, compose=compose,
                    docker=self.fake.docker, read_config=read_config)
        script = next(args[-1] for args in commands if "mysqldump " in args[-1])
        client_environment, dump = script.split("mysqldump ", 1)
        self.assertIn("--connect-timeout=15", client_environment)
        self.assertNotIn("--connect-timeout", dump)
        for required in ("--single-transaction", "--quick", "--routines", "--triggers",
                         "--events", "--hex-blob", "--set-gtid-purged=OFF",
                         "--column-statistics=0", "--no-tablespaces", "--skip-comments"):
            self.assertIn(required, dump)
        self.assertNotIn("--password", dump)

    def restore(self):
        target = self.base / "restored"
        result = restore_backup(target, self.archive, read_config=read_config, render=render,
                                project="restored-site", http_port=18089,
                                public_url="http://localhost:18089")
        return target, result

    def test_backup_restore_keeps_sqlite_wal_and_rebuilds_paths(self):
        database = self.root / "data/deerflow/gateway.sqlite3"
        connection = sqlite3.connect(database)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("INSERT INTO data_sources VALUES('owned','{\"owner_user_id\":\"u1\"}',NULL)")
        connection.commit()
        self.addCleanup(connection.close)
        report = self.backup()
        self.assertEqual(self.archive.stat().st_mode & 0o777, 0o600)
        self.assertFalse(report["assets_included"])
        target, result = self.restore()
        self.assertFalse(result["database_restored"])
        with sqlite3.connect(target / "data/deerflow/gateway.sqlite3") as reader:
            self.assertIn('"u1"', reader.execute("SELECT record FROM data_sources").fetchone()[0])
        self.assertEqual((target / "data/langgraph/checkpoint.bin").read_bytes(), b"checkpoint-state")
        self.assertEqual((target / "secrets/flask.key").read_bytes(), (self.root / "secrets/flask.key").read_bytes())
        compose = (target / "generated/current/compose.yaml").read_text()
        self.assertIn(str(target), compose)
        self.assertNotIn(str(self.root), compose)
        self.assertEqual(read_config(target)["project"], "restored-site")
        self.assertFalse((target / "data/platform/installation.json").exists())
        source_identity = json.loads((target / "data/platform/restored-source-installation.json").read_text())
        self.assertEqual(source_identity["installation_id"], "original-install")
        with self.assertRaisesRegex(ValueError, "assets differ"):
            restore_database(target, compose=self.fake.compose, read_config=read_config)
        shutil.copytree(self.root / "assets", target / "assets", dirs_exist_ok=True)
        restored = restore_database(target, compose=self.fake.compose, read_config=read_config)
        self.assertEqual(restored["status"], "restored")
        self.assertTrue(self.fake.imported)
        self.assertFalse((target / PENDING).exists())
        self.assertEqual(restore_database(target, compose=self.fake.compose, read_config=read_config)["status"], "not_required")

    def test_backup_refuses_active_application_or_dynamic_sandbox(self):
        self.fake.writers = [{"Service": "graph", "State": "running"}]
        with self.assertRaisesRegex(RuntimeError, "writers"):
            self.backup()
        self.fake.writers = []
        self.fake.sandboxes = [{"Names": "citybrain-sandbox-123", "State": "running"}]
        with self.assertRaisesRegex(RuntimeError, "dynamic sandboxes"):
            self.backup()
        self.assertFalse(self.archive.exists())

    def test_enabled_elasticsearch_is_not_silently_omitted(self):
        path = self.root / "config/deployment.yaml"
        config = yaml.safe_load(path.read_text())
        for mode in ("local", "external"):
            config["elasticsearch"]["mode"] = mode
            path.write_text(yaml.safe_dump(config))
            with self.assertRaisesRegex(RuntimeError, "ES snapshot"):
                self.backup()

    def test_nonempty_target_and_output_are_preserved(self):
        self.backup()
        with self.assertRaisesRegex(ValueError, "already exists"):
            self.backup()
        target = self.base / "not-empty"
        target.mkdir()
        (target / "valuable").write_text("keep")
        with self.assertRaisesRegex(RuntimeError, "empty directories"):
            restore_backup(target, self.archive, read_config=read_config, render=render)
        self.assertEqual((target / "valuable").read_text(), "keep")

    def test_corrupt_payload_is_rejected_before_target_is_populated(self):
        self.backup()
        corrupted = self.base / "corrupt.tar.gz"
        with tarfile.open(self.archive) as reader, tarfile.open(corrupted, "w:gz") as writer:
            for member in reader.getmembers():
                data = reader.extractfile(member).read()
                if member.name == "manifest.json":
                    manifest = json.loads(data)
                    manifest["payload_sha256"] = "0" * 64
                    data = json.dumps(manifest).encode()
                    member.size = len(data)
                writer.addfile(member, io.BytesIO(data))
        target = self.base / "bad-target"
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            restore_backup(target, corrupted, read_config=read_config, render=render)
        self.assertFalse(target.exists())

    def test_import_refuses_nonempty_database_and_preserves_pending(self):
        self.backup()
        target, _ = self.restore()
        shutil.copytree(self.root / "assets", target / "assets", dirs_exist_ok=True)
        self.fake.table_count = "12"
        with self.assertRaisesRegex(RuntimeError, "non-empty MySQL"):
            restore_database(target, compose=self.fake.compose, read_config=read_config)
        self.assertEqual(json.loads((target / PENDING).read_text())["status"], "pending")
        self.assertFalse(self.fake.imported)

    def test_interrupted_import_is_never_replayed_into_partial_database(self):
        self.backup()
        target, _ = self.restore()
        shutil.copytree(self.root / "assets", target / "assets", dirs_exist_ok=True)
        self.fake.interrupt_import = True
        with self.assertRaisesRegex(RuntimeError, "interruption"):
            restore_database(target, compose=self.fake.compose, read_config=read_config)
        self.assertEqual(json.loads((target / PENDING).read_text())["status"], "importing")
        with self.assertRaisesRegex(RuntimeError, "fresh empty database"):
            restore_database(target, compose=self.fake.compose, read_config=read_config)

    def test_private_secret_permissions_are_required(self):
        (self.root / "secrets/model.key").chmod(0o644)
        with self.assertRaisesRegex(ValueError, "permissions are too broad"):
            self.backup()

    def test_asset_symlink_is_not_followed(self):
        (self.root / "assets/skills/escape").symlink_to(self.base)
        with self.assertRaisesRegex(ValueError, "refuses symlinks"):
            self.backup()

    def test_external_mysql_uses_explicit_client_address_without_password_arguments(self):
        config = read_config(self.root)
        config["mysql"].update(mode="external", host="database.internal", port=13306)
        service = database_tools_service(self.root, config)
        self.assertEqual(service["environment"]["MYSQL_HOST"], "database.internal")
        self.assertEqual(service["environment"]["MYSQL_PORT"], "13306")
        self.assertNotIn("MYSQL_PASSWORD", service["environment"])
        self.assertEqual(service["secrets"], ["mysql_password"])

    def test_nontransactional_tables_and_scheduled_writers_are_rejected(self):
        for state in ("8.0.39\n1\n0\n", "8.0.39\n0\n1\n"):
            self.fake.server_state = state
            with self.assertRaisesRegex(RuntimeError, "InnoDB tables"):
                self.backup()
        self.assertFalse(self.archive.exists())

    def test_mysql_version_change_prevents_import(self):
        self.backup()
        target, _ = self.restore()
        shutil.copytree(self.root / "assets", target / "assets", dirs_exist_ok=True)
        self.fake.server_state = "8.0.40\n0\n0\n"
        with self.assertRaisesRegex(ValueError, "server version"):
            restore_database(target, compose=self.fake.compose, read_config=read_config)
        self.assertFalse(self.fake.imported)

    def test_changed_sql_and_asset_content_are_rejected_before_import(self):
        self.backup()
        target, _ = self.restore()
        shutil.copytree(self.root / "assets", target / "assets", dirs_exist_ok=True)
        asset = target / "assets/skills/demo/SKILL.md"
        original = asset.read_text()
        asset.write_text("unreviewed replacement")
        with self.assertRaisesRegex(ValueError, "assets differ"):
            restore_database(target, compose=self.fake.compose, read_config=read_config)
        asset.write_text(original)
        sql = next((target / "backups").rglob("mysql.sql"))
        sql.write_text("unreviewed SQL")
        with self.assertRaisesRegex(ValueError, "SQL checksum mismatch"):
            restore_database(target, compose=self.fake.compose, read_config=read_config)
        self.assertFalse(self.fake.imported)

    def test_archive_traversal_and_symlinks_are_rejected_before_writing(self):
        self.backup()
        with tarfile.open(self.archive) as archive:
            original = json.load(archive.extractfile("manifest.json"))
        for index, (name, link) in enumerate((("../outside", False), ("data/deerflow/link", True))):
            inner = self.base / f"unsafe-inner-{index}.tar.gz"
            with tarfile.open(inner, "w:gz") as archive:
                member = tarfile.TarInfo(name)
                if link:
                    member.type = tarfile.SYMTYPE
                    member.linkname = "/etc"
                    archive.addfile(member)
                else:
                    member.size = 5
                    archive.addfile(member, io.BytesIO(b"owned"))
            manifest = dict(original, payload_sha256=_hash(inner), payload_size=inner.stat().st_size)
            outer = self.base / f"unsafe-{index}.tar.gz"
            metadata = json.dumps(manifest).encode()
            with tarfile.open(outer, "w:gz") as archive:
                archive.add(inner, arcname="state.tar.gz", recursive=False)
                info = tarfile.TarInfo("manifest.json")
                info.size = len(metadata)
                archive.addfile(info, io.BytesIO(metadata))
            target = self.base / f"unsafe-target-{index}"
            with self.assertRaisesRegex(ValueError, "Unsafe backup member"):
                restore_backup(target, outer, read_config=read_config, render=render)
            self.assertFalse(target.exists())
        self.assertFalse((self.base / "outside").exists())

    def test_restore_accepts_only_the_private_top_level_lifecycle_lock(self):
        self.backup()
        target = self.base / "locked-target"
        target.mkdir()
        lock = target / ".citybrain.lock"
        lock.touch(mode=0o600)
        restored = restore_backup(target, self.archive, read_config=read_config, render=render)
        self.assertTrue(restored["restored_files"])
        for mode in (0o644, 0o666):
            other = self.base / f"insecure-lock-{mode}"
            other.mkdir()
            (other / ".citybrain.lock").touch(mode=mode)
            (other / ".citybrain.lock").chmod(mode)
            with self.assertRaisesRegex(RuntimeError, "empty directories"):
                restore_backup(other, self.archive, read_config=read_config, render=render)
        linked = self.base / "linked-lock"
        linked.mkdir()
        (linked / ".citybrain.lock").symlink_to(lock)
        with self.assertRaisesRegex(RuntimeError, "empty directories"):
            restore_backup(linked, self.archive, read_config=read_config, render=render)


if __name__ == "__main__":
    unittest.main()
