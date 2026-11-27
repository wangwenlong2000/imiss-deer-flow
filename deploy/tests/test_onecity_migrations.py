"""Real Peewee schema migration tests; run in the OneCity test image."""
from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from deploy.migrations.onecity import (
    LEDGER, MIGRATION, catalogue_models, migrate, migration_checksum, model_indexes, status,
)


@unittest.skipUnless(importlib.util.find_spec("peewee"), "OneCity runtime dependencies required")
class OneCityMigrationTests(unittest.TestCase):
    def setUp(self):
        from peewee import SqliteDatabase
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = SqliteDatabase(str(Path(self.temporary.name) / "onecity.sqlite"))
        self.addCleanup(self.database.close)
        self.environment = patch.dict(os.environ, {"ADMIN_EMAIL": ""})
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def test_fresh_schema_is_idempotent_and_checksum_is_verified(self):
        first = migrate(database=self.database)
        second = migrate(database=self.database)
        self.assertEqual(first["checksum"], second["checksum"])
        self.assertEqual(status(database=self.database)["status"], "ready")
        with self.database.connection_context():
            self.database.execute_sql(f"UPDATE {LEDGER} SET checksum='tampered'")
        with self.assertRaisesRegex(RuntimeError, "checksum mismatch"):
            migrate(database=self.database)

    def test_existing_unversioned_schema_is_not_adopted_implicitly(self):
        from onecityagent.application.database import DB
        DB.configure(self.database)
        with self.database.connection_context():
            self.database.create_tables(catalogue_models())
        with self.assertRaisesRegex(RuntimeError, "baseline explicitly"):
            migrate(database=self.database)
        result = migrate(database=self.database, baseline=True)
        self.assertTrue(result["baseline"])
        self.assertEqual(status(database=self.database)["status"], "ready")

    def test_schema_drift_fails_even_when_migration_was_recorded(self):
        migrate(database=self.database)
        with self.database.connection_context():
            self.database.execute_sql("ALTER TABLE user ADD COLUMN unexpected_owner TEXT")
        with self.assertRaisesRegex(RuntimeError, "columns differ"):
            migrate(database=self.database)

    def test_unknown_nonempty_database_is_preserved(self):
        with self.database.connection_context():
            self.database.execute_sql("CREATE TABLE unrelated(id INTEGER PRIMARY KEY)")
            self.database.execute_sql("INSERT INTO unrelated VALUES(42)")
        with self.assertRaisesRegex(RuntimeError, "unversioned"):
            migrate(database=self.database)
        with self.assertRaisesRegex(RuntimeError, "unsupported tables"):
            migrate(database=self.database, baseline=True)
        with self.database.connection_context():
            self.assertEqual(self.database.execute_sql("SELECT id FROM unrelated").fetchone(), (42,))
            self.assertNotIn(LEDGER, self.database.get_tables())

    def test_bootstrap_is_idempotent_and_preserves_password(self):
        password = Path(self.temporary.name) / "admin.password"
        password.write_text("synthetic-migration-password-123456")
        password.chmod(0o600)
        with patch.dict(os.environ, {"ADMIN_EMAIL": "admin@example.test", "ADMIN_PASSWORD_FILE": str(password)}):
            self.assertEqual(migrate(database=self.database)["bootstrap_admin"], "created")
            from onecityagent.models.db_models import User
            with self.database.connection_context():
                old_hash = User.get().password
            password.write_text("different-synthetic-password-123456")
            self.assertEqual(migrate(database=self.database)["bootstrap_admin"], "preserved")
            with self.database.connection_context():
                self.assertEqual(User.get().password, old_hash)
                self.assertEqual(User.select().count(), 1)

    def test_inherited_and_meta_index_declarations_are_deduplicated(self):
        from onecityagent.application.database import DB
        from onecityagent.models.db_models import Dialog
        DB.configure(self.database)
        declared = Dialog._meta.fields_to_index()
        self.assertGreater(len(declared), len(model_indexes(Dialog)))
        result = migrate(database=self.database)
        self.assertEqual(result['status'], 'ready')
        with self.database.connection_context():
            names = [index.name for index in self.database.get_indexes('dialog')]
            self.assertEqual(len(names), len(set(names)))

    def test_matching_interrupted_ddl_resumes_missing_indexes_and_tables(self):
        from onecityagent.application.database import DB
        from onecityagent.models.db_models import Dialog
        DB.configure(self.database)
        models = catalogue_models()
        with self.database.connection_context():
            self.database.execute_sql(f'CREATE TABLE {LEDGER} ('
                'name VARCHAR(191) NOT NULL PRIMARY KEY,checksum VARCHAR(64) NOT NULL,'
                'status VARCHAR(16) NOT NULL,applied_at VARCHAR(32))')
            self.database.execute_sql(f'INSERT INTO {LEDGER}(name,checksum,status) VALUES(?,?,?)',
                                      (MIGRATION, migration_checksum(models), 'applying'))
            Dialog._schema.create_table(safe=False)
            index = next(iter(model_indexes(Dialog).values()))[0]
            self.database.execute(Dialog._schema._create_index(index, safe=False))
        self.assertEqual(migrate(database=self.database)['status'], 'ready')
        self.assertEqual(migrate(database=self.database)['status'], 'ready')
        self.assertEqual(status(database=self.database)['status'], 'ready')

    def test_recorded_ready_schema_does_not_silently_recreate_missing_indexes(self):
        migrate(database=self.database)
        with self.database.connection_context():
            from onecityagent.models.db_models import Dialog
            name = next(iter(model_indexes(Dialog)))
            self.database.execute_sql(f'DROP INDEX "{name}"')
        with self.assertRaisesRegex(RuntimeError, 'index is missing'):
            migrate(database=self.database)


if __name__ == "__main__":
    unittest.main()
