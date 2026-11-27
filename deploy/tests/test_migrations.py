"""Local migration smoke tests that do not require running services."""

from __future__ import annotations

import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from deploy.migrations.gateway import CHECKSUM, STATEMENTS, migrate


class GatewayMigrationTests(unittest.TestCase):
    def test_gateway_migration_is_idempotent(self):
        with tempfile.TemporaryDirectory(prefix="citybrain-gateway-migration-") as temp:
            with patch.dict(os.environ, {"DEER_FLOW_HOME": temp}):
                first = migrate()
                second = migrate()

            self.assertEqual(first["migration"], "gateway.initial.v1")
            self.assertEqual(second["status"], "ready")
            database = Path(temp) / "gateway.sqlite3"
            with sqlite3.connect(database) as connection:
                rows = connection.execute(
                    "SELECT name FROM platform_schema_migrations"
                ).fetchall()
                self.assertEqual(rows, [("gateway.initial.v1",)])
                tables = {
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
                self.assertIn("mobile_conversations", tables)
                self.assertIn("data_sources", tables)

    def test_unversioned_database_requires_explicit_validated_baseline(self):
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            with sqlite3.connect(home / "gateway.sqlite3") as connection:
                for statement in STATEMENTS:
                    connection.execute(statement)
                connection.execute("INSERT INTO data_sources VALUES(?,?,?)", (
                    "legacy", '{"id":"legacy","owner_user_id":null,"owner_org_id":null}', None
                ))
            with self.assertRaisesRegex(RuntimeError, "baseline explicitly"):
                migrate(home=home)
            migrate(home=home, baseline=True)
            with sqlite3.connect(home / "gateway.sqlite3") as connection:
                record = connection.execute("SELECT record FROM data_sources").fetchone()[0]
                self.assertIn('"owner_user_id":null', record)

    def test_checksum_drift_is_not_silently_accepted(self):
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            migrate(home=home)
            with sqlite3.connect(home / "gateway.sqlite3") as connection:
                connection.execute("UPDATE platform_schema_migrations SET checksum='tampered'")
            for baseline in (False, True):
                with self.assertRaisesRegex(RuntimeError, "checksum mismatch"):
                    migrate(home=home, baseline=baseline)

    def test_baseline_refuses_lost_unique_constraint(self):
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            with sqlite3.connect(home / "gateway.sqlite3") as connection:
                for statement in STATEMENTS:
                    connection.execute(statement.replace("thread_id TEXT NOT NULL UNIQUE", "thread_id TEXT NOT NULL"))
            with self.assertRaisesRegex(RuntimeError, "uniqueness constraints"):
                migrate(home=home, baseline=True)
            with sqlite3.connect(home / "gateway.sqlite3") as connection:
                self.assertFalse(connection.execute("SELECT 1 FROM sqlite_master WHERE name='platform_schema_migrations'").fetchall())

    def test_ready_migration_still_checks_actual_schema(self):
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            migrate(home=home)
            with sqlite3.connect(home / "gateway.sqlite3") as connection:
                connection.execute("ALTER TABLE data_sources ADD COLUMN unsafe_owner TEXT")
            with self.assertRaisesRegex(RuntimeError, "schema differs"):
                migrate(home=home)

    def test_old_scaffold_marker_requires_explicit_baseline(self):
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            migrate(home=home)
            with sqlite3.connect(home / "gateway.sqlite3") as connection:
                connection.execute("UPDATE platform_schema_migrations SET checksum='builtin-v1'")
            with self.assertRaisesRegex(RuntimeError, "checksum mismatch"):
                migrate(home=home)
            migrate(home=home, baseline=True)
            with sqlite3.connect(home / "gateway.sqlite3") as connection:
                self.assertEqual(connection.execute("SELECT checksum FROM platform_schema_migrations").fetchone(), (CHECKSUM,))

    def test_unknown_newer_migration_refuses_downgrade(self):
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            migrate(home=home)
            with sqlite3.connect(home / "gateway.sqlite3") as connection:
                connection.execute("INSERT INTO platform_schema_migrations VALUES('future.v2','hash')")
            with self.assertRaisesRegex(RuntimeError, "newer than this release"):
                migrate(home=home)


if __name__ == "__main__":
    unittest.main()
