"""Deployment configuration and generated Compose smoke tests."""

from __future__ import annotations

import os
from pathlib import Path
import tempfile
import unittest

import yaml
from cryptography.hazmat.primitives.serialization import load_pem_private_key

from deploy.cli.citybrain import asset_report
from deploy.cli.configuration import compose_document, initialize, read_config, render


class DeploymentConfigurationTests(unittest.TestCase):
    def test_initialize_and_render_create_secret_safe_runtime_files(self):
        with tempfile.TemporaryDirectory(prefix="citybrain-config-test-") as temp:
            root = Path(temp) / "site"
            initialize(root)

            config_path = root / "config/deployment.yaml"
            config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
            config["bootstrap_admin"]["email"] = "admin@example.test"
            config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
            (root / "secrets/model.key").write_text("synthetic-model-key\n", encoding="utf-8")
            os.chmod(root / "secrets/model.key", 0o600)

            loaded = read_config(root)
            self.assertEqual(loaded["project"], "citybrain")
            rendered = render(root)

            current = root / "generated/current"
            self.assertTrue(current.is_symlink())
            generation = current.resolve()
            compose = yaml.safe_load((generation / "compose.yaml").read_text(encoding="utf-8"))
            self.assertEqual(rendered["generation"], generation.name)
            self.assertEqual(compose["services"]["api"]["command"][:2], [
                "gunicorn", "onecityagent.entrypoints.api:app"
            ])
            self.assertEqual(compose["services"]["coordinator"]["healthcheck"]["test"][3],
                "import urllib.request; urllib.request.urlopen(\"http://127.0.0.1:7010/healthz\",timeout=3)")
            graph = compose["services"]["graph"]
            config_option = graph["command"].index("--config")
            self.assertEqual(graph["command"][config_option + 1], "/app/backend/langgraph.json")
            for service in ("graph", "gateway", "migrate-gateway"):
                self.assertNotIn("LANGGRAPH_CONFIG", compose["services"][service]["environment"])

            onecity = (generation / "onecity.toml").read_text(encoding="utf-8")
            self.assertIn('DATABASE_PASSWORD = "$MYSQL_PASSWORD"', onecity)
            self.assertNotIn("synthetic-model-key", onecity)
            self.assertEqual((root / "secrets/flask.key").stat().st_mode & 0o777, 0o600)
            self.assertEqual((root / "secrets/private.pem").stat().st_mode & 0o777, 0o600)
            passphrase = (root / "secrets/rsa.passphrase").read_bytes()
            private_key = load_pem_private_key(
                (root / "secrets/private.pem").read_bytes(), password=passphrase or None
            )
            self.assertIsNotNone(private_key)

    def test_graph_file_config_is_cli_argument_not_reserved_runtime_json_environment(self):
        root = Path("/tmp/citybrain-config-contract")
        config = yaml.safe_load((Path(__file__).parents[1] / "config/deployment.example.yaml").read_text())
        config["capacity"]["jobs"] = 5
        compose = compose_document(root, root / "generated/generation", config)
        graph = compose["services"]["graph"]
        self.assertEqual(graph["command"][:6], [
            "python", "/opt/citybrain/deploy/entrypoints/run.py",
            "/app/backend/.venv/bin/langgraph", "dev", "--config", "/app/backend/langgraph.json",
        ])
        jobs_option = graph["command"].index("--n-jobs-per-worker")
        self.assertEqual(graph["command"][jobs_option + 1], "5")
        self.assertIn("--no-reload", graph["command"])
        for service in ("graph", "gateway", "migrate-gateway"):
            self.assertNotIn("LANGGRAPH_CONFIG", compose["services"][service]["environment"])

    def test_configuration_rejects_credentials_embedded_in_urls(self):
        with tempfile.TemporaryDirectory(prefix="citybrain-invalid-config-") as temp:
            root = Path(temp) / "site"
            initialize(root)
            path = root / "config/deployment.yaml"
            config = yaml.safe_load(path.read_text(encoding="utf-8"))
            config["bootstrap_admin"]["email"] = "admin@example.test"
            config["model"]["base_url"] = "https://user:password@example.test/v1"
            path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "Credentials belong in secret files"):
                read_config(root)

    def test_preflight_rejects_empty_skill_and_compliance_asset_mounts(self):
        with tempfile.TemporaryDirectory(prefix="citybrain-preflight-test-") as temp:
            root = Path(temp) / "site"
            initialize(root)
            path = root / "config/deployment.yaml"
            config = yaml.safe_load(path.read_text(encoding="utf-8"))
            config["bootstrap_admin"]["email"] = "admin@example.test"
            path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
            errors, _warnings = asset_report(root, read_config(root))
            self.assertIn(
                "no SKILL.md files found in assets/skills (run assets import)", errors
            )
            self.assertIn(
                "missing config/compliance/detectors.yaml (run assets import)", errors
            )
            self.assertIn(
                "missing config/compliance/policy_matrix.yaml (run assets import)", errors
            )


if __name__ == "__main__":
    unittest.main()
