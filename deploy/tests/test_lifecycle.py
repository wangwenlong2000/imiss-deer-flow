"""Regression checks for installer boundaries, not mocked business acceptance."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import yaml

from deploy.cli.citybrain import root_arg, install, start
from deploy.cli.configuration import SOURCE, initialize, read_config, render
from deploy.cli.preflight import assert_project_owner, inspect_images
from deploy.tests.test_preflight import DockerFixture


class LifecycleTests(unittest.TestCase):
    def configured(self, directory):
        root = Path(directory) / 'site'
        initialize(root)
        path = root / 'config/deployment.yaml'
        config = yaml.safe_load(path.read_text())
        config['bootstrap_admin']['email'] = 'admin@example.test'
        path.write_text(yaml.safe_dump(config))
        (root / 'secrets/model.key').write_text('synthetic-test-key')
        return root

    def test_schema_checks_nested_patterns_and_booleans(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self.configured(directory)
            path = root / 'config/deployment.yaml'
            config = yaml.safe_load(path.read_text())
            for key, bad in [('project', 'bad project; command'), ('http_port', True)]:
                altered = {**config, key: bad}
                path.write_text(yaml.safe_dump(altered))
                with self.assertRaises(ValueError): read_config(root)
            config['bootstrap_admin']['email'] = 'not-an-email'
            path.write_text(yaml.safe_dump(config))
            with self.assertRaises(ValueError): read_config(root)

    def test_init_preserves_secrets_and_key_pair(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self.configured(directory)
            secrets_before = {p.name:p.read_bytes() for p in (root/'secrets').iterdir()}
            initialize(root)
            self.assertEqual(secrets_before, {p.name:p.read_bytes() for p in (root/'secrets').iterdir()})
            self.assertEqual((root/'secrets').stat().st_mode & 0o777, 0o700)

    def test_render_is_path_specific_and_does_not_mutate_management_state(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self.configured(directory)
            state = root/'data/platform/extensions.json'
            state.write_text('{"skills":{"test":{"enabled":false}}}')
            first = render(root)
            second = render(root)
            self.assertEqual(first['generation'], second['generation'])
            self.assertEqual(json.loads(state.read_text())['skills']['test']['enabled'], False)
            other = self.configured(Path(directory)/'other')
            self.assertNotEqual(first['generation'], render(other)['generation'])
            self.assertIn(str(root), (root/'generated/current/compose.yaml').read_text())

    def test_matching_project_name_does_not_authorize_adoption(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self.configured(directory)
            def docker(*args):
                text = 'container-id' if args[0] == 'ps' else json.dumps([{'Config':{'Labels':{'io.citybrain.root':'/somewhere-else'}}}])
                return subprocess.CompletedProcess(args, 0, text, '')
            with self.assertRaisesRegex(RuntimeError, 'another installation'):
                assert_project_owner(root, read_config(root), docker)

    def test_installer_refuses_silent_image_replacement(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self.configured(directory)
            state = {'root':str(root),'project':'citybrain','release':'local','images':{'onecity':{'id':'sha256:old'}}}
            (root/'data/platform/installation.json').write_text(json.dumps(state))
            report = {'passed':True,'images':{'onecity':{'id':'sha256:new'}}}
            with patch('deploy.cli.citybrain.preflight', return_value=report):
                with self.assertRaisesRegex(RuntimeError, 'image identities changed'):
                    install(root)

    def test_host_wrapper_mounts_the_actual_root_and_source(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            fake = base/'docker'
            log = base/'arguments'
            fake.write_text('#!/bin/sh\nif [ "$1" = run ]; then printf "%s\\n" "$@" > "$TEST_DOCKER_ARGS"; fi\n')
            fake.chmod(0o755)
            root = base/'a site'
            env = {**os.environ,'PATH':str(base)+':'+os.environ['PATH'],'TEST_DOCKER_ARGS':str(log)}
            subprocess.run([str(SOURCE/'citybrain'),'--root',str(root),'init'],env=env,check=True)
            arguments = log.read_text().splitlines()
            self.assertIn(str(root)+':'+str(root), arguments)
            self.assertIn('CITYBRAIN_ROOT='+str(root), arguments)
            self.assertIn(str(SOURCE)+':'+str(SOURCE)+':ro', arguments)
            self.assertIn('host', arguments)
            self.assertNotIn('/site', arguments)

    def test_root_directory_cannot_be_filesystem_root_or_relative(self):
        for value in ('/', 'relative/site'):
            with self.assertRaises(ValueError): root_arg(value)


class StartGuardTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name) / 'site'
        initialize(self.root)
        config_path = self.root / 'config/deployment.yaml'
        self.config = yaml.safe_load(config_path.read_text())
        self.config['bootstrap_admin']['email'] = 'admin@example.test'
        config_path.write_text(yaml.safe_dump(self.config))
        (self.root / 'secrets/model.key').write_text('synthetic-test-key')
        render(self.root)
        self.edge = {
            'Id': 'container-1', 'State': {'Running': True},
            'Config': {'Labels': {'io.citybrain.root': str(self.root),
                                  'com.docker.compose.project': self.config['project'],
                                  'com.docker.compose.service': 'edge'}},
            'Mounts': [{'Type': 'bind',
                        'Source': str(self.root / 'generated/current/nginx.conf'),
                        'Destination': '/etc/citybrain/nginx.conf'}],
            'NetworkSettings': {'Ports': {'8080/tcp': [{'HostPort': str(self.config['http_port'])}]}},
        }
        self.docker = DockerFixture([self.edge])
        images, errors = inspect_images(self.config, self.docker)
        self.assertFalse(errors)
        self.state = {'root': str(self.root), 'project': self.config['project'],
                      'release': self.config['release'], 'status': 'ready', 'images': images}
        self.state_path = self.root / 'data/platform/installation.json'
        self.save_state()

    def save_state(self):
        self.state_path.write_text(json.dumps(self.state))

    def run_start(self):
        with patch('deploy.cli.citybrain.docker', side_effect=self.docker), \
                patch('deploy.cli.citybrain.asset_report', return_value=([], [])), \
                patch('deploy.cli.preflight.shutil.disk_usage', return_value=SimpleNamespace(free=2 * 1024**3)):
            return start(self.root, timeout=321)

    def assert_rejected_without_compose(self, message):
        with patch('deploy.cli.citybrain.compose') as compose:
            with self.assertRaisesRegex(RuntimeError, message):
                self.run_start()
            compose.assert_not_called()

    def test_start_checks_ready_identity_images_secrets_and_compose_before_up(self):
        with patch('deploy.cli.citybrain.compose') as compose:
            self.assertEqual(self.run_start(), {'started': True})
        self.assertEqual(compose.call_args_list, [
            unittest.mock.call(self.root, 'config', '--quiet'),
            unittest.mock.call(self.root, 'up', '-d', '--wait', '--wait-timeout', '321'),
        ])

    def test_start_rejects_different_root_project_release_or_unready_record(self):
        for key, value in (('root', str(self.root) + '-other'), ('project', 'production'),
                           ('release', 'different-release'), ('status', 'installing')):
            original = self.state[key]
            with self.subTest(key=key):
                self.state[key] = value
                self.save_state()
                self.assert_rejected_without_compose('identity differs|Run install')
            self.state[key] = original

    def test_start_rejects_missing_redirected_and_pending_installation(self):
        self.state_path.unlink()
        self.assert_rejected_without_compose('Run install')
        outside = self.root.parent / 'installation.json'
        outside.write_text(json.dumps(self.state))
        self.state_path.symlink_to(outside)
        self.assert_rejected_without_compose('Run install')
        self.state_path.unlink()
        self.save_state()
        pending = self.root / 'data/.restore-pending.json'
        pending.write_text('{}')
        self.assert_rejected_without_compose('Run install')
        pending.unlink()
        pending.symlink_to(self.root / 'missing-pending')
        self.assert_rejected_without_compose('Run install')

    def test_start_rejects_changed_image_tags_and_incomplete_image_ledger(self):
        reference = self.config['images']['onecity']
        self.docker.image_overrides[reference] = [{
            'Id': 'sha256:' + 'b' * 64, 'Os': 'linux', 'Architecture': 'amd64'}]
        self.assert_rejected_without_compose('image identities changed')
        self.docker.image_overrides.clear()
        original = self.state['images']
        for images in ({}, None, {'onecity': 'bad'}, {'onecity': original['onecity']}):
            with self.subTest(images=images):
                self.state['images'] = images
                self.save_state()
                self.assert_rejected_without_compose('image identities changed')

    def test_start_rejects_secret_permissions_foreign_labels_and_foreign_mounts(self):
        secret = self.root / 'secrets/model.key'
        secret.chmod(0o644)
        self.assert_rejected_without_compose('Preflight failed')
        secret.chmod(0o600)
        self.edge['Config']['Labels']['io.citybrain.root'] = '/production'
        self.assert_rejected_without_compose('Preflight failed')
        self.edge['Config']['Labels']['io.citybrain.root'] = str(self.root)
        self.edge['Mounts'][0]['Source'] = '/production/generated/nginx.conf'
        self.assert_rejected_without_compose('Preflight failed')

    def test_start_rejects_tampered_generated_compose_before_any_compose_call(self):
        path = self.root / 'generated/current/compose.yaml'
        document = yaml.safe_load(path.read_text())
        document['services']['graph']['volumes'].append({
            'type': 'bind', 'source': '/production/data', 'target': '/foreign-data'})
        path.write_text(yaml.safe_dump(document))
        self.assert_rejected_without_compose('Generated Compose differs')

    def test_start_rejects_invalid_compose_without_exposing_its_contents(self):
        path = self.root / 'generated/current/compose.yaml'
        path.write_text('services: [private-diagnostic-value')
        self.assert_rejected_without_compose('^Generated Compose is invalid; review before start$')

    def test_start_rejects_redirected_planned_mount_even_without_existing_containers(self):
        self.docker.containers = []
        data = self.root / 'data/deerflow'
        data.rmdir()
        outside = self.root.parent / 'production-data'
        outside.mkdir()
        data.symlink_to(outside, target_is_directory=True)
        with patch('deploy.cli.preflight.socket.socket'):
            self.assert_rejected_without_compose('Planned container mount is redirected')


if __name__ == '__main__': unittest.main()
