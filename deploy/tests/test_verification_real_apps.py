"""Protocol integration against real OneCity factories and isolated SQLite.

Run in the OneCity Python image. No installed service, external model or business
database is used. These checks validate the verifier's API compatibility only.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import threading
import unittest


AVAILABLE = sys.version_info >= (3, 11) and all(importlib.util.find_spec(name) for name in ("peewee", "flask", "Cryptodome", "cryptography"))


@unittest.skipUnless(AVAILABLE, "requires the isolated OneCity Python runtime")
class RealApplicationProtocolTests(unittest.TestCase):
    def test_interfaces_use_real_registration_history_and_guarded_cleanup(self):
        from Cryptodome.PublicKey import RSA
        from peewee import SqliteDatabase
        from werkzeug.serving import make_server, WSGIRequestHandler
        import yaml

        from deploy.cli.verification import verify_deployment
        from deploy.cli.verification_fixture import cleanup
        from onecityagent.application.api import create_api_app
        from onecityagent.application.coordinator import create_coordinator_app
        from onecityagent.application.bootstrap import seed_initial_data
        from onecityagent.models import create_tables
        from onecityagent.models.db_models import User

        class QuietHandler(WSGIRequestHandler):
            def log_request(self, *args, **kwargs):
                pass

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            key = RSA.generate(2048)
            (root / "secrets").mkdir(mode=0o700)
            key_path = root / "secrets/private.pem"
            key_path.write_bytes(key.export_key())
            key_path.chmod(0o600)
            (root / "secrets/rsa.passphrase").write_text("")
            (root / "secrets/rsa.passphrase").chmod(0o600)
            database = SqliteDatabase(root / "onecity.sqlite", pragmas={"journal_mode": "wal", "busy_timeout": 5000})
            settings = {
                "SECRET_KEY": "isolated-verification-protocol-secret",
                "DATABASE": {"type": "sqlite", "name": str(root / "onecity.sqlite")},
                "ONECITY_SELF_REGISTER_ROLE_CODE": "common",
                "ONECITY_RSA_PRIVATE_KEY_PATH": str(key_path),
                "ONECITY_RSA_PRIVATE_KEY_PASSPHRASE": "",
            }
            api = create_api_app(settings, database=database)
            coordinator = create_coordinator_app(settings, database=database)
            with api.app_context():
                create_tables()
                seed_initial_data(database, admin_email="admin@example.test", admin_password="isolated-admin-password")

            def application(environ, start_response):
                path = environ.get("PATH_INFO", "")
                if path.startswith("/internal/"):
                    start_response("404 Not Found", [("Content-Type", "application/json")])
                    return [b'{"error":"hidden"}']
                return (coordinator if path.startswith("/query/") else api)(environ, start_response)

            server = make_server("127.0.0.1", 0, application, threaded=True, request_handler=QuietHandler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                (root / "config").mkdir()
                source = Path(__file__).resolve().parents[1] / "config/deployment.example.yaml"
                config = yaml.safe_load(source.read_text())
                config["bootstrap_admin"]["email"] = "admin@example.test"
                config["public_url"] = "http://127.0.0.1:" + str(server.server_port)
                (root / "config/deployment.yaml").write_text(yaml.safe_dump(config))
                (root / "secrets/admin.password").write_text("isolated-admin-password")

                report = verify_deployment(root, "interfaces", cleanup_runner=lambda state: cleanup(state, app=api))
                self.assertTrue(report["passed"], json.dumps(report, indent=2))
                self.assertFalse(report["business_scenarios_executed"])
                with database.connection_context():
                    synthetic = list(User.select().where(User.email.endswith("@acceptance.invalid")))
                    self.assertEqual(len(synthetic), 2)
                    self.assertTrue(all(row.status == "0" and row.is_active == "0" and row.access_token is None for row in synthetic))
                    admin = User.get(User.email == "admin@example.test")
                    self.assertEqual(admin.status, "1")
                    self.assertIsNone(admin.access_token)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)
                if not database.is_closed():
                    database.close()


if __name__ == "__main__":
    unittest.main()
