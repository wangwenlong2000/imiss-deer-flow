"""Dynamic sandbox names never authorize lifecycle mutations on their own."""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from deploy.cli.citybrain import stop
from deploy.cli.preflight import assert_sandbox_owners


class SandboxOwnershipTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)/'site'
        (self.root/'data/platform').mkdir(parents=True)
        self.image = 'sha256:' + 'a'*64
        self.identifier = 'b'*64
        self.project = 'citybrain-isolated'
        self.thread = 'thread-123'
        self.config = {'project': self.project, 'release': 'rc1',
                       'images': {'sandbox': 'sandbox:rc1'}, 'services': {}}
        (self.root/'data/platform/installation.json').write_text(json.dumps({
            'root': str(self.root), 'project': self.project, 'release': 'rc1',
            'images': {'sandbox': {'id': self.image}},
        }))
        mounts = [
            {'Type': 'bind', 'Source': str(self.root/'assets'/name),
             'Destination': '/mnt/' + name, 'RW': False}
            for name in ('skills', 'datasets')
        ] + [
            {'Type': 'bind', 'Source': str(self.root/'data/deerflow/threads'/self.thread/'user-data'/name),
             'Destination': '/mnt/user-data/' + name, 'RW': True}
            for name in ('workspace', 'uploads', 'outputs')
        ]
        self.container = {
            'Id': self.identifier,
            'Name': '/' + self.project + '-sandbox-' + hashlib.sha256(self.thread.encode()).hexdigest()[:8],
            'Image': self.image,
            'Config': {'Labels': {'io.citybrain.root': str(self.root),
                                  'io.citybrain.project': self.project, 'io.citybrain.role': 'sandbox'}},
            'NetworkSettings': {'Networks': {self.project + '-services': {}}},
            'Mounts': mounts,
        }
        self.containers = [self.container]
        self.calls = []

    def docker(self, *args, **kwargs):
        self.calls.append(args)
        if args[0] == 'ps':
            result = '\n'.join(item['Id'] for item in self.containers)
        elif args[:2] == ('image', 'inspect'):
            result = json.dumps([{'Id': self.image}])
        elif args[0] == 'inspect':
            result = json.dumps(self.containers)
        elif args[0] == 'stop':
            result = ''
        else:
            raise AssertionError(args)
        return subprocess.CompletedProcess(args, 0, result, '')

    def test_exact_labels_image_thread_mounts_and_network_authorize_immutable_id(self):
        self.assertEqual(assert_sandbox_owners(self.root, self.config, self.docker), [self.identifier])
        self.assertFalse(any(args[0] == 'stop' for args in self.calls))

    def test_no_sandbox_does_not_require_installation_record(self):
        self.containers = []
        (self.root/'data/platform/installation.json').unlink()
        self.assertEqual(assert_sandbox_owners(self.root, self.config, self.docker), [])

    def test_old_unlabelled_or_foreign_sandbox_is_refused(self):
        for labels in ({}, {'io.citybrain.root': str(self.root), 'io.citybrain.project': 'production',
                            'io.citybrain.role': 'sandbox'}):
            with self.subTest(labels=labels):
                self.container['Config']['Labels'] = labels
                with self.assertRaisesRegex(RuntimeError, 'labels or image'):
                    assert_sandbox_owners(self.root, self.config, self.docker)

    def test_foreign_image_network_and_missing_mounts_are_refused(self):
        for field, value in (
            ('Image', 'sha256:' + 'f'*64),
            ('NetworkSettings', {'Networks': {self.project + '-services': {}, 'production': {}}}),
            ('Mounts', self.container['Mounts'][:-1]),
            ('Name', '/' + self.project + '-sandbox-deadbeef'),
        ):
            original = self.container[field]
            with self.subTest(field=field):
                self.container[field] = value
                with self.assertRaises(RuntimeError):
                    assert_sandbox_owners(self.root, self.config, self.docker)
            self.container[field] = original

    def test_foreign_misplaced_writable_asset_and_redirected_mounts_are_refused(self):
        for mutate in (
            lambda mounts: mounts[2].update(Source='/production/data/deerflow/threads/thread-123/user-data/workspace'),
            lambda mounts: mounts[2].update(Source=str(self.root/'secrets')),
            lambda mounts: mounts[0].update(RW=True),
            lambda mounts: mounts.append({'Type': 'bind', 'Source': '/var/run/docker.sock',
                                          'Destination': '/var/run/docker.sock', 'RW': True}),
            lambda mounts: mounts[3].update(Source=str(self.root/'data/deerflow/threads/other/user-data/uploads')),
        ):
            original = self.container['Mounts']
            self.container['Mounts'] = copy.deepcopy(original)
            mutate(self.container['Mounts'])
            with self.assertRaises(RuntimeError):
                assert_sandbox_owners(self.root, self.config, self.docker)
            self.container['Mounts'] = original
        asset = self.root/'assets'
        asset.mkdir()
        (asset/'skills').symlink_to(self.root/'secrets', target_is_directory=True)
        with self.assertRaisesRegex(RuntimeError, 'redirected'):
            assert_sandbox_owners(self.root, self.config, self.docker)

    def test_changed_tag_or_installation_identity_does_not_authorize_stop(self):
        path = self.root/'data/platform/installation.json'
        state = json.loads(path.read_text())
        state['images']['sandbox']['id'] = 'sha256:' + 'f'*64
        path.write_text(json.dumps(state))
        with self.assertRaisesRegex(RuntimeError, 'image identity changed'):
            assert_sandbox_owners(self.root, self.config, self.docker)
        state['root'] = '/production'
        path.write_text(json.dumps(state))
        with self.assertRaisesRegex(RuntimeError, 'matching installed image'):
            assert_sandbox_owners(self.root, self.config, self.docker)

    def test_one_invalid_candidate_prevents_every_stop_including_compose(self):
        foreign = copy.deepcopy(self.container)
        foreign['Id'] = 'c'*64
        foreign['Config']['Labels'] = {}
        self.containers.append(foreign)
        with patch('deploy.cli.citybrain.read_config', return_value=self.config), \
                patch('deploy.cli.citybrain.assert_project_owner'), \
                patch('deploy.cli.citybrain.docker', side_effect=self.docker), \
                patch('deploy.cli.citybrain.compose') as compose:
            with self.assertRaises(RuntimeError):
                stop(self.root)
            compose.assert_not_called()
        self.assertFalse(any(args[0] == 'stop' for args in self.calls))

    def test_stop_revalidates_survivors_and_never_stops_by_reusable_name(self):
        with patch('deploy.cli.citybrain.read_config', return_value=self.config), \
                patch('deploy.cli.citybrain.assert_project_owner'), \
                patch('deploy.cli.citybrain.docker', side_effect=self.docker), \
                patch('deploy.cli.citybrain.compose') as compose:
            self.assertTrue(stop(self.root)['application_writers_stopped'])
            compose.assert_called_once()
        self.assertEqual([args for args in self.calls if args[0] == 'stop'],
                         [('stop', '--time', '120', self.identifier)])
        self.assertEqual(len([args for args in self.calls if args[0] == 'inspect']), 2)


if __name__ == '__main__':
    unittest.main()
