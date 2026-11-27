"""Release packaging checks use temporary source/sites and a fake image API."""
from __future__ import annotations

import json
import gzip
import io
import os
from pathlib import Path
import shutil
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from deploy.cli.release import (
    IMAGE_ARCHIVE, REQUIRED_FILES, _git_info, _hash, _save_images, check_release, export_evidence,
    pack_release, release_command,
)


class ImageAPI:
    def __init__(self):
        self.calls = []
        self.missing = None

    def __call__(self, *args, **kwargs):
        self.calls.append(tuple(map(str, args)))
        if args[:2] == ("image", "inspect"):
            reference = args[2]
            if reference == self.missing:
                return SimpleNamespace(returncode=1, stdout="")
            item = {"Id": "sha256:" + "a" * 64, "RepoDigests": [], "Os": "linux", "Architecture": "amd64"}
            return SimpleNamespace(returncode=0, stdout=json.dumps([item]))
        if args[0] == "save":
            Path(args[args.index("--output") + 1]).write_bytes(b"synthetic image archive")
            return SimpleNamespace(returncode=0, stdout="")
        raise AssertionError("Unexpected image operation")


class ImageProcess:
    def __init__(self, data=b"synthetic docker image tar stream", returncode=0):
        self.stdout = io.BytesIO(data)
        self.returncode = returncode
        self.completed = False

    def wait(self, timeout=None):
        self.completed = True
        return self.returncode

    def poll(self):
        return self.returncode if self.completed else None

    def terminate(self):
        self.completed = True

    def kill(self):
        self.completed = True


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        self.source = self.base / "source"
        self.source.mkdir()
        self.site = self.base / "site"
        self.site.mkdir()
        self.output = self.base / "release"
        for name in REQUIRED_FILES:
            path = self.source / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("synthetic source: " + name + "\n")
        (self.source / "citybrain").chmod(0o755)
        self.config = {
            "capabilities": ["basic"], "elasticsearch": {"mode": "disabled"},
            "mysql": {"mode": "local"},
            "images": {name: name + ":test" for name in ("onecity", "deerflow", "edge", "sandbox", "mysql")},
        }
        self.config_patch = patch("deploy.cli.release.read_config", return_value=self.config)
        self.config_patch.start()
        self.addCleanup(self.config_patch.stop)
        self.docker = ImageAPI()

    def pack(self, **options):
        return pack_release(self.site, self.output, "20261126-rc1", source=self.source,
                            docker=self.docker, **options)

    def write(self, name, value="private value"):
        path = self.source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value)
        return path

    def rewrite_manifest(self, change):
        path = self.output / "manifest.json"
        document = json.loads(path.read_text())
        change(document)
        path.write_text(json.dumps(document))
        checksums = self.output / "checksums.sha256"
        lines = [line for line in checksums.read_text().splitlines() if not line.endswith("  manifest.json")]
        lines.append(_hash(path) + "  manifest.json")
        checksums.write_text("\n".join(sorted(lines)) + "\n")

    def test_release_has_current_uncommitted_source_and_complete_hashes(self):
        self.write("backend/app/new_feature.py", "current uncommitted implementation\n")
        result = self.pack()
        self.assertTrue(result["passed"])
        self.assertEqual((self.output / "backend/app/new_feature.py").read_text(), "current uncommitted implementation\n")
        self.assertTrue(check_release(self.output)["passed"])
        self.assertEqual((self.output / "citybrain").stat().st_mode & 0o777, 0o755)
        manifest = json.loads((self.output / "manifest.json").read_text())
        self.assertFalse(manifest["acceptance"]["delivery_accepted"])
        self.assertFalse(manifest["compatibility"]["in_place_update_supported"])
        self.assertEqual(manifest["images"]["onecity"]["id"], "sha256:" + "a" * 64)

    def test_release_pack_works_without_git_and_does_not_claim_clean_checkout(self):
        with patch("deploy.cli.release.subprocess.run", side_effect=FileNotFoundError("git")):
            self.pack()
        manifest = json.loads((self.output / "manifest.json").read_text())
        self.assertFalse(manifest["source"]["git_metadata_available"])
        self.assertIsNone(manifest["source"]["commit"])
        self.assertIsNone(manifest["source"]["dirty"])
        self.assertEqual(len(manifest["source"]["content_sha256"]), 64)
        self.assertTrue(check_release(self.output)["passed"])

    def test_non_git_source_metadata_is_explicitly_unknown(self):
        failed = SimpleNamespace(returncode=128, stdout="", stderr="not a git repository")
        with patch("deploy.cli.release.subprocess.run", return_value=failed):
            self.assertEqual(_git_info(self.source),
                             {"git_metadata_available": False, "commit": None, "dirty": None})

    def test_available_git_metadata_keeps_commit_and_dirty_semantics(self):
        for status, dirty in (("", False), (" M deploy/cli/release.py\n", True)):
            results = [SimpleNamespace(returncode=0, stdout="a" * 40 + "\n"),
                       SimpleNamespace(returncode=0, stdout=status)]
            with self.subTest(dirty=dirty), patch("deploy.cli.release.subprocess.run", side_effect=results):
                self.assertEqual(_git_info(self.source),
                                 {"git_metadata_available": True, "commit": "a" * 40, "dirty": dirty})

    def test_private_configs_state_outputs_and_dependencies_are_never_packaged(self):
        forbidden = [
            ".env", ".env.example", "config.yaml", "extensions_config.json",
            "apps/onecityagent/.env.example", "apps/onecityagent/config/private.pem",
            "backend/.env", "backend/.venv/secret.py", "backend/.langgraph_api/state",
            "apps/onecityagent/web/node_modules/package/index.js", "apps/onecityagent/web/dist/index.html",
            "skills/custom/example/.env", "skills/custom/example/secret.key",
            "deploy/secrets/admin.password", "outputs/private.json", "data/database.db",
        ]
        for name in forbidden:
            self.write(name)
        self.pack()
        for name in forbidden:
            self.assertFalse((self.output / name).exists(), name)

    def test_public_skill_assets_do_not_allow_other_databases_or_secret_directories(self):
        rule_root = "skills/custom/program_snippet/opengrep-compliance/rules/official/opengrep-rules/generic/secrets"
        allowed = [
            "skills/custom/policies-regulations/china-legal-consultation/references/data/case_types.db",
            "skills/custom/policies-regulations/china-legal-issue-analysis/references/data/case_types.db",
            "services/streetmodel/models.json",
            "tools/opengrep/download-apiiro-rules.sh",
            "tools/opengrep/download-opengrep-assets.sh",
            "tools/opengrep/download-trailofbits-rules.sh",
            rule_root + "/gitleaks/public-rule.yaml",
            rule_root + "/security/public-rule.yaml",
        ]
        forbidden = [
            "skills/custom/policies-regulations/china-legal-consultation/references/data/private.db",
            "services/streetmodel/config.yaml",
            "tools/opengrep/secrets/private.yaml",
            rule_root + "/gitleaks/.env",
            rule_root + "/gitleaks/private.key",
            rule_root + "/gitleaks/credentials.yaml",
            rule_root + "/gitleaks/runtime.db",
            rule_root + "/unreviewed/private.yaml",
        ]
        for name in allowed + forbidden:
            self.write(name, "synthetic static asset\n")
        self.pack()
        for name in allowed:
            self.assertTrue((self.output / name).is_file(), name)
        for name in forbidden:
            self.assertFalse((self.output / name).exists(), name)
        self.assertTrue(check_release(self.output)["passed"])

    def test_public_rule_exception_does_not_bypass_credential_content_scan(self):
        path = "skills/custom/program_snippet/opengrep-compliance/rules/official/opengrep-rules/generic/secrets/gitleaks/bad.yaml"
        self.write(path, "api_key: sk-" + "x" * 40)
        with self.assertRaisesRegex(ValueError, "Potential private credential"):
            self.pack()
        self.assertFalse(self.output.exists())

    def test_image_archive_is_explicit_and_covered_by_checksums(self):
        def save(path, references):
            _save_images(path, references, process_factory=lambda *_a, **_kw: ImageProcess())
        with patch("deploy.cli.release._save_images", side_effect=save):
            self.pack(include_images=True)
        self.assertTrue((self.output / IMAGE_ARCHIVE).is_file())
        self.assertFalse((self.output / "images/application-images.tar").exists())
        with gzip.open(self.output / IMAGE_ARCHIVE, "rb") as archive:
            self.assertEqual(archive.read(), b"synthetic docker image tar stream")
        self.assertTrue(check_release(self.output)["passed"])
        manifest = json.loads((self.output / "manifest.json").read_text())
        self.assertEqual(manifest["images_archive"]["compression"], "gzip")
        self.assertEqual(manifest["images"]["sandbox"]["provenance"]["kind"], "external-compatibility-asset")
        self.assertFalse(manifest["images"]["sandbox"]["provenance"]["clean_source_build_verified"])

    def test_failed_streaming_image_export_removes_partial_gzip(self):
        archive = self.base / "images.tar.gz"
        process = ImageProcess(returncode=1)
        calls = []
        def factory(args, **kwargs):
            calls.append(args)
            return process
        with self.assertRaisesRegex(RuntimeError, "Docker image export failed"):
            _save_images(archive, ["onecity:test"], process_factory=factory)
        self.assertEqual(calls, [["docker", "save", "onecity:test"]])
        self.assertFalse(archive.exists())
        self.assertTrue(process.completed)

    def test_empty_streaming_image_export_is_not_accepted(self):
        archive = self.base / "empty.tar.gz"
        with self.assertRaisesRegex(RuntimeError, "empty archive"):
            _save_images(archive, ["onecity:test"], process_factory=lambda *_a, **_kw: ImageProcess(b""))
        self.assertFalse(archive.exists())

    def test_default_package_does_not_save_images(self):
        self.pack()
        self.assertFalse(any(call[0] == "save" for call in self.docker.calls))

    def test_missing_image_blocks_release(self):
        self.docker.missing = "onecity:test"
        with self.assertRaisesRegex(RuntimeError, "unresolved images"):
            self.pack()
        self.assertFalse(self.output.exists())

    def test_missing_required_implementation_blocks_release(self):
        (self.source / "backend/uv.lock").unlink()
        with self.assertRaisesRegex(ValueError, "incomplete"):
            self.pack()

    def test_wrong_profile_cannot_be_presented_as_basic_acceptance(self):
        self.config["capabilities"] = ["street"]
        with self.assertRaisesRegex(RuntimeError, "basic online"):
            self.pack()

    def test_symlink_source_is_rejected(self):
        outside = self.base / "private.txt"
        outside.write_text("private")
        (self.source / "backend/leak.py").symlink_to(outside)
        with self.assertRaisesRegex(ValueError, "non-regular"):
            self.pack()

    def test_credential_body_is_rejected_without_echoing_it(self):
        credential = "sk-" + "x" * 40
        self.write("backend/app/mistake.py", 'API_KEY="' + credential + '"')
        with self.assertRaisesRegex(ValueError, "Potential private credential") as error:
            self.pack()
        self.assertNotIn(credential, str(error.exception))

    def test_output_must_not_overwrite_existing_or_live_state(self):
        self.output.mkdir()
        with self.assertRaisesRegex(ValueError, "already exists"):
            self.pack()
        with self.assertRaisesRegex(ValueError, "live configuration"):
            pack_release(self.site, self.site / "secrets/release", "rc1", source=self.source, docker=self.docker)

    def test_source_change_during_copy_does_not_publish_partial_release(self):
        from deploy.cli import release
        real_copy = release._copy_source
        changed = False
        def changing(path, target):
            nonlocal changed
            real_copy(path, target)
            if not changed:
                path.write_text(path.read_text() + "changed")
                changed = True
        with patch("deploy.cli.release._copy_source", side_effect=changing):
            with self.assertRaisesRegex(RuntimeError, "Source changed"):
                self.pack()
        self.assertFalse(self.output.exists())

    def test_checker_detects_changed_deleted_extra_and_mode_modified_files(self):
        self.pack()
        target = self.output / "backend/uv.lock"
        original = target.read_bytes()
        target.write_text("tampered")
        self.assertFalse(check_release(self.output)["passed"])
        target.write_bytes(original)
        target.unlink()
        self.assertFalse(check_release(self.output)["passed"])
        target.write_bytes(original)
        extra = self.output / "unexpected.txt"
        extra.write_text("extra")
        self.assertFalse(check_release(self.output)["passed"])
        extra.unlink()
        target.chmod(0o755)
        self.assertFalse(check_release(self.output)["passed"])

    def test_checker_rejects_traversal_and_symlink_even_with_rewritten_manifest_checksum(self):
        self.pack()
        self.rewrite_manifest(lambda manifest: manifest["files"].append({"path": "../private", "sha256": "a" * 64, "size": 0, "mode": 0o644}))
        with self.assertRaisesRegex(ValueError, "Unsafe release path"):
            check_release(self.output)

    def test_checker_rejects_symlink_payload(self):
        self.pack()
        path = self.output / "backend/uv.lock"
        path.unlink()
        path.symlink_to(self.source / "backend/uv.lock")
        with self.assertRaisesRegex(ValueError, "symlink"):
            check_release(self.output)

    def test_checker_validates_summary_hashes(self):
        self.pack()
        self.rewrite_manifest(lambda document: document["assets"].update(content_sha256="0" * 64))
        report = check_release(self.output)
        self.assertFalse(report["passed"])
        self.assertIn("asset_inventory_mismatch", report["errors"])

    def test_evidence_exports_structured_records_redacts_secrets_and_retains_failures(self):
        evidence = self.site / "evidence/verification/run"
        evidence.mkdir(parents=True)
        (evidence / "report.json").write_text(json.dumps({
            "passed": False, "suite": "full", "password": "private-password",
            "nested": {"access_token": "private-token", "result_sha256": "a" * 64},
            "url": "https://user:password@example.test/v1?api_key=secret",
            "content": "private model content", "uncovered_capabilities": ["code"],
        }))
        (evidence / "private.log").write_text("not included")
        (evidence / "cleanup-state.json").write_text('{"token":"uncleaned-account-state"}')
        (self.site / "evidence/last-command-error.private.json").write_text('{"stdout":"raw-private-model-text"}')
        (self.site / "evidence/unknown.json").write_text('{"arbitrary":"unreviewed-model-text"}')
        output = self.base / "public-evidence"
        report = export_evidence(self.site, output)
        self.assertTrue(report["passed"])
        exported = json.loads((output / "reports/verification/run/report.json").read_text())
        self.assertFalse(exported["passed"])
        self.assertEqual(exported["nested"]["result_sha256"], "a" * 64)
        self.assertEqual(exported["uncovered_capabilities"], ["code"])
        text = (output / "reports/verification/run/report.json").read_text()
        self.assertNotIn("private-password", text)
        self.assertNotIn("private-token", text)
        self.assertNotIn("private model content", text)
        self.assertFalse((output / "reports/verification/run/private.log").exists())
        self.assertFalse((output / "reports/verification/run/cleanup-state.json").exists())
        self.assertFalse((output / "reports/last-command-error.private.json").exists())
        self.assertFalse((output / "reports/unknown.json").exists())

    def test_evidence_refuses_redirects_and_export_recursion(self):
        (self.site / "evidence").mkdir()
        (self.site / "evidence/leak.json").symlink_to(self.source / "backend/uv.lock")
        with self.assertRaisesRegex(ValueError, "redirected"):
            export_evidence(self.site, self.base / "evidence-export")
        with self.assertRaisesRegex(ValueError, "input evidence"):
            export_evidence(self.site, self.site / "evidence/export")

    def test_update_plan_and_apply_do_not_touch_containers_without_reviewed_compatibility(self):
        self.pack()
        identity = self.site / "data/platform/installation.json"
        identity.parent.mkdir(parents=True)
        identity.write_text(json.dumps({"status": "ready", "release": "old"}))
        def never(*_args, **_kwargs):
            raise AssertionError("Planning/rejected apply must not mutate a site")
        args = SimpleNamespace(command="upgrade", action="plan", to=self.output)
        plan = release_command(self.site, args, docker=never, compose=never, render=never, verify=never)
        self.assertFalse(plan["apply_supported"])
        self.assertFalse(plan["writes_performed"])
        args.action = "apply"
        with self.assertRaisesRegex(RuntimeError, "disabled"):
            release_command(self.site, args, docker=never, compose=never, render=never, verify=never)
        args.command = "rollback"
        with self.assertRaisesRegex(RuntimeError, "disabled"):
            release_command(self.site, args, docker=never, compose=never, render=never, verify=never)


if __name__ == "__main__":
    unittest.main()
