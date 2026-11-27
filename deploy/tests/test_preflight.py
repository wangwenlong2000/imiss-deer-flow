"""Installer image, ownership, path and published-port boundaries."""
from __future__ import annotations

import copy
import json
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import yaml

from deploy.cli.configuration import SECRET_FILES, SOURCE
from deploy.cli.preflight import assert_project_owner, inspect_images, preflight, required_images


class DockerFixture:
    def __init__(self, containers=None):
        self.containers = containers or []
        self.image_overrides = {}

    def __call__(self, *args, **kwargs):
        output, status = '', 0
        if args[0] == 'ps':
            selected = self.containers
            if '-aq' not in args:
                selected = [x for x in selected if x['Config']['Labels'].get('com.docker.compose.service') == 'edge'
                            and x.get('State', {}).get('Running')]
            output = '\n'.join(x['Id'] for x in selected)
        elif args[0] == 'inspect':
            output = json.dumps([x for x in self.containers if x['Id'] in args[1:]])
        elif args[:2] == ('image', 'inspect'):
            value = self.image_overrides.get(args[2], [{
                'Id': 'sha256:' + 'a'*64, 'Os': 'linux', 'Architecture': 'amd64', 'RepoDigests': [],
            }])
            if value is None:
                status = 1
            else:
                output = value if isinstance(value, str) else json.dumps(value)
        return subprocess.CompletedProcess(args, status, output, '')


class PreflightTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)/'site'
        self.root.mkdir()
        secrets = self.root/'secrets'
        secrets.mkdir(mode=0o700)
        for name in (*SECRET_FILES, 'private.pem'):
            path = secrets/name
            path.touch(mode=0o600)
        self.config = yaml.safe_load((SOURCE/'deploy/config/deployment.example.yaml').read_text())

    def container(self, *, port='8086', source=None):
        return {
            'Id': 'container-1', 'State': {'Running': True},
            'Config': {'Labels': {'io.citybrain.root': str(self.root),
                                  'com.docker.compose.project': self.config['project'],
                                  'com.docker.compose.service': 'edge'}},
            'Mounts': [{'Type': 'bind', 'Source': str(source or self.root/'generated/revision/nginx.conf'),
                        'Destination': '/etc/citybrain/nginx.conf'}],
            'NetworkSettings': {'Ports': {'8080/tcp': [{'HostIp': '0.0.0.0', 'HostPort': port}]}},
        }

    def report(self, docker=None, **kwargs):
        return preflight(self.root, self.config, docker=docker or DockerFixture(),
                         asset_report=lambda root, config: ([], []), **kwargs)

    def test_current_site_only_owns_its_actual_published_http_port(self):
        edge = self.container()
        with patch('deploy.cli.preflight.socket.socket') as probe:
            result = self.report(DockerFixture([edge]))
        self.assertTrue(result['passed'], result)
        probe.assert_not_called()

    def test_existing_edge_on_old_port_does_not_hide_new_port_conflict(self):
        with socket.socket() as occupied:
            occupied.bind(('0.0.0.0', 0))
            occupied.listen()
            self.config['http_port'] = occupied.getsockname()[1]
            old = self.container(port=str(self.config['http_port'] + 1))
            result = self.report(DockerFixture([old]))
        self.assertFalse(result['passed'])
        self.assertTrue(any('HTTP port' in error for error in result['errors']))

    def test_other_container_target_port_cannot_claim_edge_http_port(self):
        edge = self.container()
        edge['NetworkSettings']['Ports'] = {'9999/tcp': [{'HostPort': '8086'}]}
        with patch('deploy.cli.preflight.socket.socket') as probe:
            self.assertTrue(self.report(DockerFixture([edge]))['passed'])
            self.assertTrue(probe.called)

    def test_root_label_and_compose_project_must_both_match_exactly(self):
        for change in ({'io.citybrain.root': str(self.root)+'-other'},
                       {'com.docker.compose.project': 'another-project'},
                       {'io.citybrain.root': str(self.root/'../elsewhere')}):
            edge = self.container()
            edge['Config']['Labels'].update(change)
            with self.assertRaisesRegex(RuntimeError, 'another installation'):
                assert_project_owner(self.root, self.config, DockerFixture([edge]))

    def test_correct_labels_cannot_authorize_foreign_state_or_secret_mounts(self):
        for target, source in (
            ('/etc/citybrain/nginx.conf', self.root/'generated-other/nginx.conf'),
            ('/data/deerflow', self.root.parent/'other/data/deerflow'),
            ('/data/platform/extensions.json', self.root/'data/platform-other/extensions.json'),
            ('/app/skills', self.root/'assets/skills-other'),
            ('/run/secrets/flask_key', self.root/'secrets-other/flask.key'),
        ):
            edge = self.container()
            edge['Mounts'] = [{'Type': 'bind', 'Source': str(source), 'Destination': target}]
            with self.assertRaisesRegex(RuntimeError, 'another installation|outside this installation'):
                assert_project_owner(self.root, self.config, DockerFixture([edge]))

    def test_symlinked_configuration_source_is_not_treated_as_local(self):
        outside = self.root.parent/'outside'
        outside.mkdir()
        (self.root/'generated').symlink_to(outside, target_is_directory=True)
        edge = self.container(source=self.root/'generated/revision/nginx.conf')
        # Root's generated directory itself is a redirect and must be refused.
        with self.assertRaises(RuntimeError):
            assert_project_owner(self.root, self.config, DockerFixture([edge]))

    def test_missing_or_redirected_secret_directory_reports_errors(self):
        shutil.rmtree(self.root/'secrets')
        missing = self.report(check_port=False)
        self.assertFalse(missing['passed'])
        self.assertIn('missing or redirected secrets directory', missing['errors'])
        outside = self.root.parent/'outside-secrets'
        outside.mkdir(mode=0o700)
        (self.root/'secrets').symlink_to(outside, target_is_directory=True)
        self.assertIn('missing or redirected secrets directory', self.report(check_port=False)['errors'])

    def test_overly_permissive_and_symlinked_secret_files_are_refused(self):
        secret = self.root/'secrets/model.key'
        secret.chmod(0o644)
        self.assertTrue(any('permissions' in error for error in self.report(check_port=False)['errors']))
        secret.unlink()
        secret.symlink_to(self.root/'secrets/flask.key')
        self.assertTrue(any('redirected secret file: model.key' in error for error in self.report(check_port=False)['errors']))

    def test_only_linux_amd64_image_metadata_passes(self):
        for os_name, arch in (('windows', 'amd64'), ('linux', 'arm64')):
            fake = DockerFixture()
            fake.image_overrides[self.config['images']['onecity']] = [{
                'Id': 'sha256:'+'b'*64, 'Os': os_name, 'Architecture': arch,
            }]
            _, errors = inspect_images(self.config, fake)
            self.assertIn('unsupported image platform: onecity', errors)

    def test_missing_malformed_and_option_like_image_references_fail_closed(self):
        reference = self.config['images']['onecity']
        for metadata in (None, [], 'not-json', [{'Id': 'not-an-image-id'}], [{'Id': 17}]):
            fake = DockerFixture()
            fake.image_overrides[reference] = metadata
            inventory, errors = inspect_images(self.config, fake)
            self.assertTrue(errors)
            self.assertNotIn('onecity', inventory)
        for reference in ('--help', 'a b', 'registry/user:tag\n'):
            config = copy.deepcopy(self.config)
            config['images']['onecity'] = reference
            _, errors = inspect_images(config, DockerFixture())
            self.assertIn('invalid image reference: onecity', errors)

    def test_external_mysql_still_requires_pinned_client_but_disabled_es_does_not(self):
        self.config['mysql']['mode'] = 'external'
        self.assertIn('mysql', required_images(self.config))
        self.assertNotIn('elasticsearch', required_images(self.config))
        self.config['elasticsearch']['mode'] = 'local'
        self.assertIn('elasticsearch', required_images(self.config))


if __name__ == '__main__':
    unittest.main()
