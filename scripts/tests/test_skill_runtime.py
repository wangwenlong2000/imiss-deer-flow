from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from scripts.check_skill_runtime import ROOT, check_runtime, validate_schema


class SkillRuntimeManifestTests(unittest.TestCase):
    def test_actual_source_release_preserves_all_static_skill_evidence_and_assets(self) -> None:
        from deploy.cli.release import check_release, pack_release
        from deploy.tests.test_release import ImageAPI

        config = {
            'capabilities': ['basic'], 'elasticsearch': {'mode': 'disabled'},
            'mysql': {'mode': 'local'},
            'images': {name: name + ':static-test' for name in
                       ('onecity', 'deerflow', 'edge', 'sandbox', 'mysql')},
        }
        with TemporaryDirectory(prefix='citybrain-skill-release-test-') as directory:
            base = Path(directory)
            site, output = base / 'site', base / 'release'
            site.mkdir()
            with patch('deploy.cli.release.read_config', return_value=config):
                pack_release(site, output, 'static-test', source=ROOT, docker=ImageAPI())
            self.assertFalse((output / 'config.yaml').exists())
            self.assertTrue(check_release(output)['passed'])
            report = check_runtime(output, metadata_path=None, verify_evidence=True)
            self.assertEqual(report['summary']['skill_count'], 119)
            self.assertEqual(report['summary']['error'], 0,
                             [check for check in report['checks'] if check['status'] == 'error'])
            self.assertFalse(report['skill_code_executed'])

    def test_manifest_matches_schema_and_119_audited_skills(self) -> None:
        report = check_runtime(ROOT, metadata_path=None, verify_evidence=True)
        self.assertEqual(report['summary']['skill_count'], 119)
        self.assertEqual(report['summary']['error'], 0, report['checks'])
        self.assertFalse(report['network_checks_performed'])
        self.assertFalse(report['skill_code_executed'])
        self.assertFalse(report['package_install_or_mutation_performed'])

    def test_capability_filter_is_explicit_and_no_business_execution(self) -> None:
        report = check_runtime(ROOT, capabilities=['code'], metadata_path=None, verify_evidence=False)
        self.assertEqual(report['summary']['skill_count'], 8)
        self.assertEqual(report['summary']['error'], 0)
        self.assertTrue(all('code' in next(s['capability_groups'] for s in json.loads(
            (ROOT / 'skills/runtime-manifest.json').read_text())['skills'] if s['skill_id'] == sid)
            for sid in report['selected_skills']))

    def test_opengrep_is_checked_as_local_asset_not_path_command(self) -> None:
        report = check_runtime(ROOT, skill_ids=['opengrep-compliance'], metadata_path=None,
                               verify_evidence=False)
        self.assertEqual(report['summary']['error'], 0, report['checks'])
        asset = next(check for check in report['checks'] if check.get('asset_id') == 'opengrep-linux')
        self.assertEqual(asset['code'], 'asset_valid')
        self.assertFalse(any(check['code'] == 'tool_missing_from_path' for check in report['checks']))

    def test_schema_rejects_automatic_execution_grant(self) -> None:
        schema = json.loads((ROOT / 'skills/skill_runtime.schema.json').read_text())
        manifest = json.loads((ROOT / 'skills/runtime-manifest.json').read_text())
        manifest['skills'][0]['actions'].append({'id': 'bad', 'kind': 'cli', 'path': 'x',
            'interpreter': 'python3', 'arguments': 'x', 'automatic_execution': True})
        self.assertTrue(validate_schema(manifest, schema))

    def test_missing_manifest_is_read_only_error(self) -> None:
        report = check_runtime(ROOT / 'does-not-exist', metadata_path=None)
        self.assertEqual(report['summary']['error'], 1)
        self.assertFalse(report['network_checks_performed'])


if __name__ == '__main__':
    unittest.main()
