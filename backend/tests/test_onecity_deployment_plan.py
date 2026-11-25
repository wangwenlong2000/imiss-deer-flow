"""Deployment bundle isolation and credential handling without a Docker daemon."""

import argparse
import copy
import importlib.util
import json
from pathlib import Path
import stat
import tempfile
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[2] / "scripts/deploy_onecity_multiuser.py"
SPEC = importlib.util.spec_from_file_location("deploy_onecity_multiuser", SCRIPT)
deploy = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(deploy)


class OneCityDeploymentPlanTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repo = self.root / "source"
        for name in deploy.REQUIRED_FILES:
            file = self.repo / "backend" / name
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_text('{}' if file.suffix == '.json' else '# isolated test source\n')
        self.args = argparse.Namespace(
            repo=self.repo, runtime_dir=self.root / "private", source_gateway="existing-gateway",
            source_graph="existing-graph", onecity="existing-onecity", network="existing-network",
            sandbox_prefix="new-sandbox", sandbox_port=18481, iam_port=7010,
            gateway_name="new-gateway", graph_name="new-graph", jobs=3, max_inflight=10, per_user=2,
        )
        mounts = [{"Type": "bind", "Source": f"/production/{target.rsplit('/', 1)[-1]}", "Destination": target}
                  for target in ("/app/models", "/app/skills", "/app/datasets", "/app/config", "/app/extensions_config.json")]
        self.source = {
            "Image": "sha256:synthetic-local-image", "Config": {"Env": [
                "MODEL_API_KEY=synthetic-private-provider-key", "DEER_FLOW_HOST_BASE_DIR=/production/old-data",
                "DEERFLOW_RUN_EVENT_LOG_ENABLED=1", "DEERFLOW_RUN_EVENT_LOG_DIR=/production/logs",
                "NO_PROXY=internal-model", "HOSTNAME=old-container",
            ]},
            "Mounts": mounts + [{"Type": "volume", "Name": "existing-venv", "Destination": "/app/backend/.venv"}],
        }
        self.original_config = {
            "sandbox": {"use": "deerflow.community.aio_sandbox:AioSandboxProvider", "container_prefix": "old-sandbox", "port": 8181,
                        "mounts": [{"host_path": "/production/datasets", "container_path": "/mnt/datasets", "read_only": True}]},
            "compliance": {"enabled": True, "scene_resolver_mode": "manual_ui", "audit": {"path": "old-audit"}},
            "checkpointer": {"type": "sqlite", "connection_string": "/production/old.db"},
            "channels": {"feishu": {"enabled": True}}, "models": [{"api_key": "$MODEL_API_KEY"}],
        }

    def prepare(self):
        onecity = {"NetworkSettings": {"Networks": {"existing-network": {}}}}
        self.commands = []

        def fake_run(command, **kwargs):
            self.commands.append(command)
            if command[:3] == ["docker", "network", "inspect"]:
                return "[]"
            if command[:2] == ["docker", "exec"]:
                compile(command[-1], '<embedded configuration reader>', 'exec')
                return json.dumps(self.original_config)
            raise AssertionError(f"Unexpected operation during prepare: {command[:2]}")

        with patch.object(deploy, "inspect_container", side_effect=[self.source, self.source, onecity]), patch.object(deploy, "run", fake_run):
            return deploy.prepare(self.args)

    def test_prepare_only_reads_docker_and_keeps_live_assets_and_credentials_private(self):
        original = copy.deepcopy(self.source)
        plan = self.prepare()
        self.assertEqual(self.source, original)
        self.assertEqual([command[1] for command in self.commands], ["network", "exec"])
        safe_summary = json.dumps(deploy.summary(plan))
        self.assertNotIn("synthetic-private-provider-key", safe_summary)
        self.assertFalse(deploy.summary(plan)["production_traffic_switched"])
        self.assertEqual(deploy.summary(plan)["published_host_ports"], [])
        for filename in ("gateway.env", "langgraph.env", "admission.key", "config.onecity.json", "plan.json"):
            self.assertEqual(stat.S_IMODE((self.args.runtime_dir / filename).stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(self.args.runtime_dir.stat().st_mode), 0o700)
        credentials = []
        for role in ("gateway", "langgraph"):
            values = dict(line.split("=", 1) for line in (self.args.runtime_dir / f"{role}.env").read_text().splitlines())
            self.assertEqual(values["MODEL_API_KEY"], "synthetic-private-provider-key")
            self.assertEqual(values["DEER_FLOW_HOME"], "/data/deerflow")
            self.assertEqual(values["DEER_FLOW_HOST_BASE_DIR"], str(self.args.runtime_dir / "data"))
            self.assertEqual(values["MOBILE_IAM_REQUIRED"], "true")
            self.assertEqual(values["DEERFLOW_RUN_EVENT_LOG_ENABLED"], "0")
            self.assertIn("existing-onecity", values["NO_PROXY"])
            credentials.append(values["DEER_FLOW_ADMISSION_SIGNING_KEY"])
        self.assertEqual(credentials[0], credentials[1])
        self.assertGreaterEqual(len(credentials[0]), 32)
        self.assertNotIn(credentials[0], safe_summary)

    def test_new_containers_share_only_new_writable_state_and_publish_no_ports(self):
        plan = self.prepare()
        for service in plan["services"]:
            writable = {mount["target"] for mount in service["mounts"] if not mount["readonly"]}
            self.assertEqual(writable, {"/data/deerflow", "/app/backend/.langgraph_api", "/app/logs"})
            self.assertTrue(next(m for m in service["mounts"] if m["target"] == "/app/backend/.venv")["readonly"])
            self.assertTrue(next(m for m in service["mounts"] if m["target"] == "/app/backend")["readonly"])
            self.assertEqual(next(m for m in service["mounts"] if m["target"] == "/app/backend")["source"], str(self.args.runtime_dir / "release/backend"))
            command = deploy.command_for(plan, service)
            self.assertNotIn("--publish", command)
            self.assertNotIn("-p", command)
            self.assertNotIn("host", command)
            self.assertNotIn("--reload", command)
            self.assertNotIn("synthetic-private-provider-key", " ".join(command))
        config = json.loads((self.args.runtime_dir / "config.onecity.json").read_text())
        self.assertEqual(config["checkpointer"]["connection_string"], "/data/deerflow/checkpoints.db")
        self.assertEqual(config["compliance"]["scene_resolver_mode"], "trusted_upstream")
        self.assertTrue(config["compliance"]["strict_startup"])
        self.assertEqual(config["channels"], {})
        self.assertEqual(config["models"], self.original_config["models"])

    def test_changed_private_configuration_and_reprepare_are_rejected_without_rotating_key(self):
        self.prepare()
        before = (self.args.runtime_dir / "admission.key").read_text()
        self.assertEqual(deploy.load_plan(self.args.runtime_dir)["network"], "existing-network")
        with self.assertRaises(deploy.DeploymentError):
            deploy.prepare(self.args)
        self.assertEqual((self.args.runtime_dir / "admission.key").read_text(), before)
        with (self.args.runtime_dir / "gateway.env").open("a") as output:
            output.write("MOBILE_IAM_REQUIRED=false\n")
        with self.assertRaises(deploy.DeploymentError):
            deploy.load_plan(self.args.runtime_dir)

    def test_existing_container_names_are_never_replaced(self):
        plan = self.prepare()
        with patch.object(deploy, "run", return_value="new-graph\n") as runner:
            with self.assertRaises(deploy.DeploymentError):
                deploy.start(plan)
        self.assertEqual(runner.call_count, 1)
        self.assertEqual(runner.call_args.args[0][:3], ["docker", "ps", "--all"])

    def test_env_newline_and_unsafe_shared_sandbox_mounts_fail_closed(self):
        with self.assertRaises(deploy.DeploymentError):
            deploy.env_text({"MODEL_API_KEY": "synthetic\nMOBILE_IAM_REQUIRED=false"})
        unsafe = copy.deepcopy(self.original_config)
        unsafe["sandbox"]["mounts"][0]["read_only"] = False
        with self.assertRaises(deploy.DeploymentError):
            deploy.build_config(unsafe, sandbox_prefix="new-sandbox", sandbox_port=18481)

    def test_source_snapshot_excludes_runtime_and_survives_development_edits(self):
        source_file = self.repo / "backend/app/graph_access.py"
        original_source = source_file.read_text()
        for name in (".venv/secret", ".deer-flow/secret", "app/__pycache__/secret", "app/private.db", "app/.env"):
            candidate = self.repo / "backend" / name
            candidate.parent.mkdir(parents=True, exist_ok=True)
            candidate.write_text('synthetic-runtime-secret')
        self.prepare()
        source_file.write_text('later independent development change')
        plan = deploy.load_plan(self.args.runtime_dir)
        snapshot = Path(plan["backend_snapshot"])
        self.assertEqual((snapshot / "app/graph_access.py").read_text(), original_source)
        self.assertFalse(any('synthetic-runtime-secret' in path.read_text() for path in snapshot.rglob('*') if path.is_file()))
        (snapshot / "app/graph_access.py").write_text('unexpected deployment modification')
        with self.assertRaises(deploy.DeploymentError):
            deploy.load_plan(self.args.runtime_dir)


if __name__ == "__main__":
    unittest.main()
