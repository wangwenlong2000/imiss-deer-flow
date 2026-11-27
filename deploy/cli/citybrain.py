#!/usr/bin/env python3
"""Versioned CityBrain installation, verification and recovery entrypoint."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

import yaml

from deploy.cli.configuration import SOURCE, compose_document, initialize, read_config, render
from deploy.cli.preflight import assert_project_owner, assert_sandbox_owners, inspect_images, preflight


def root_arg(value):
    path = Path(value).expanduser()
    if not path.is_absolute() or path == Path('/') or path.is_symlink():
        raise ValueError('Installation root must be a real, absolute directory other than /')
    return path.resolve()


def build_source():
    return Path(os.environ.get('CITYBRAIN_SOURCE', SOURCE)).expanduser().resolve()


def docker(*args, capture=True, check=True):
    result = subprocess.run(['docker', *map(str, args)], text=True, capture_output=capture)
    if check and result.returncode:
        # Docker stderr can contain request URLs. It is not copied into public evidence.
        raise RuntimeError(f'Docker command failed ({result.returncode}); inspect private service/build logs')
    return result


def current(root):
    link = root / 'generated/current'
    if not link.is_symlink():
        raise RuntimeError('Run config-render before operating services')
    target = link.resolve()
    if target.parent != (root / 'generated').resolve() or not (target / 'compose.yaml').is_file():
        raise RuntimeError('Generated configuration link is invalid')
    return target


def compose(root, *args, capture=True, check=True):
    cfg = read_config(root)
    result = docker('compose', '-p', cfg['project'], '-f', current(root) / 'compose.yaml',
                    *args, capture=capture, check=False)
    if check and result.returncode:
        _private_json(root / 'evidence/last-command-error.private.json', {
            'returncode': result.returncode, 'stdout': result.stdout or '', 'stderr': result.stderr or ''})
        raise RuntimeError('Compose operation failed; inspect evidence/last-command-error.private.json and service logs')
    return result


def asset_report(root, cfg):
    errors, warnings = [], []
    for name in ('skills', 'models', 'datasets', 'config'):
        path = root / 'assets' / name
        if not path.is_dir() or path.is_symlink():
            errors.append('missing or redirected asset directory: ' + name)
    skills = root / 'assets/skills'
    if skills.is_dir() and not any(skills.rglob('SKILL.md')):
        errors.append('no SKILL.md files found in assets/skills (run assets import)')
    manifest = skills / 'runtime-manifest.json'
    try:
        declarations = json.loads(manifest.read_text())['skills']
        identifiers = [entry['skill_id'] for entry in declarations]
        if not identifiers or len(identifiers) != len(set(identifiers)) or any(not isinstance(entry.get('capability_groups'), list) for entry in declarations):
            raise ValueError('Invalid Skill runtime declarations')
        if len(list(skills.rglob('SKILL.md'))) != len(identifiers):
            raise ValueError('Skill runtime declarations do not cover installed Skills')
    except (OSError, ValueError, KeyError, TypeError):
        errors.append('missing or inconsistent Skill runtime manifest')
    for name in ('detectors.yaml', 'policy_matrix.yaml'):
        if not (root / 'assets/config/compliance' / name).is_file():
            errors.append('missing config/compliance/' + name + ' (run assets import)')
    detector = root / 'assets/config/compliance/detectors.yaml'
    if detector.is_file() and 'model_tfidf_knn' in detector.read_text() and not (root / 'assets/models/compliance/ml_detector_0624_fresh.json').is_file():
        errors.append('missing required compliance model ml_detector_0624_fresh.json')
    for name in ('mysql.password', 'model.key'):
        path = root / 'secrets' / name
        if not path.is_file() or not path.read_text().strip():
            errors.append('empty or missing secret: ' + name)
    if 'street' in cfg['capabilities']:
        nas = cfg['services'].get('nas_path')
        if not nas or not Path(nas).is_dir():
            errors.append('street capability requires a mounted/readable NAS path')
    if {'street', 'network', 'phone'} & set(cfg['capabilities']) and cfg['elasticsearch']['mode'] == 'disabled':
        errors.append('selected capability requires Elasticsearch')
    return errors, warnings


def _private_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, temporary = tempfile.mkstemp(prefix='.citybrain-', dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, 'w') as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


@contextmanager
def site_lock(root):
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd = os.open(root / '.citybrain.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('Another lifecycle operation is running for this installation') from None
        yield
    finally:
        os.close(fd)


def import_assets(root: Path, source: Path) -> dict:
    """Copy only declared asset trees; reject redirects and preserve executability."""
    initialize(root)
    source = source.resolve()
    targets = {'skills': source / 'skills', 'models': source / 'models', 'config': source / 'config'}
    if not (targets['skills']).is_dir():
        raise ValueError('Asset source must contain skills/, models/ and config/')
    ignore = shutil.ignore_patterns('.git', '.env', '.env.*', '__pycache__', '*.pyc', 'node_modules',
                                    '.playwright-browsers', '.cache', 'outputs', 'logs', '.venv')
    imported = []
    for name, path in targets.items():
        if name == 'models':
            path = path / 'compliance'
            destination = root / 'assets/models/compliance'
        elif name == 'config':
            path = path / 'compliance'
            destination = root / 'assets/config/compliance'
        else:
            destination = root / 'assets' / name
        if not path.is_dir():
            raise ValueError('Required source assets missing: ' + name)
        # Refuse symlinks, including ones nested beneath otherwise safe directories.
        for directory, dirs, files in os.walk(path):
            skipped = set(ignore(directory, dirs + files))
            dirs[:] = [x for x in dirs if x not in skipped]
            if any((Path(directory) / x).is_symlink() for x in dirs + [x for x in files if x not in skipped]):
                raise ValueError('Asset source contains symlinks; resolve assets before import')
        if destination.exists() and any(destination.iterdir()):
            raise RuntimeError('Asset destination is not empty: ' + name)
        shutil.copytree(path, destination, dirs_exist_ok=True, ignore=ignore)
        for directory, dirs, files in os.walk(destination):
            Path(directory).chmod(0o755)
            for name_in_dir in files:
                target = Path(directory) / name_in_dir
                target.chmod(0o755 if target.stat().st_mode & 0o111 else 0o644)
        imported.append(name)
    return {'imported': imported, 'datasets_imported': False,
            'note': 'Business datasets and model/NAS assets must be imported separately for selected capabilities'}


def _records(value):
    if not value.strip():
        return []
    return json.loads(value) if value.lstrip().startswith('[') else [json.loads(line) for line in value.splitlines() if line.strip()]


def status(root):
    cfg = read_config(root)
    assert_project_owner(root, cfg, docker)
    result = compose(root, 'ps', '--all', '--format', 'json')
    return {'services': [{'service': x.get('Service'), 'state': x.get('State'), 'health': x.get('Health')}
                         for x in _records(result.stdout)]}


def verify(root, suite, **options):
    from deploy.cli.verification import verify_deployment
    return verify_deployment(root, suite, **options)


def save_backup(root, out):
    from deploy.cli.recovery import save_backup as save
    assert_project_owner(root, read_config(root), docker)
    return save(root, out, compose=compose, read_config=read_config, docker=docker)


def restore(root, source, **options):
    from deploy.cli.recovery import restore_backup
    return restore_backup(root, source, read_config=read_config, render=render, **options)


def install(root, *, timeout=600):
    from deploy.cli.recovery import restore_database
    initialize(root)
    cfg = read_config(root)
    report = preflight(root, cfg, docker=docker, asset_report=asset_report)
    _private_json(root / 'evidence/preflight.json', report)
    if not report['passed']:
        raise RuntimeError('Preflight failed: ' + '; '.join(report['errors']))
    state_path = root / 'data/platform/installation.json'
    state = json.loads(state_path.read_text()) if state_path.is_file() else {
        'schema_version': 1, 'installation_id': os.urandom(16).hex(), 'root': str(root),
        'project': cfg['project'], 'release': cfg['release'], 'steps': []}
    if state['root'] != str(root) or state['project'] != cfg['project']:
        raise RuntimeError('Installation identity differs; use a separate directory and project')
    if state.get('release') != cfg['release']:
        raise RuntimeError('Use upgrade for a different release')
    if state.get('images') and {k:v['id'] for k,v in state['images'].items()} != {k:v['id'] for k,v in report['images'].items()}:
        raise RuntimeError('Installed image identities changed; use an explicit upgrade')
    state['images'] = report['images']
    state['status'] = 'installing'
    def completed(step):
        if step not in state['steps']:
            state['steps'].append(step)
        state['updated_at'] = datetime.now(timezone.utc).isoformat()
        _private_json(state_path, state)
    render(root)
    compose(root, 'config', '--quiet')
    completed('configuration')
    data_services = []
    if cfg['mysql']['mode'] == 'local':
        data_services.append('mysql')
    if cfg['elasticsearch']['mode'] == 'local':
        directory = root / 'data/elasticsearch'
        os.chown(directory, 1000, 0)
        directory.chmod(0o750)
        data_services.append('elasticsearch')
    if data_services:
        compose(root, 'up', '-d', '--wait', '--wait-timeout', str(timeout), *data_services)
    completed('data_services')
    restored = restore_database(root, compose=compose, read_config=read_config)
    if restored['status'] == 'restored':
        completed('restored_database')
    compose(root, '--profile', 'tools', 'run', '--rm', '--no-deps', 'migrate')
    completed('onecity_migration')
    compose(root, '--profile', 'tools', 'run', '--rm', '--no-deps', 'migrate-gateway')
    completed('gateway_migration')
    compose(root, 'up', '-d', '--wait', '--wait-timeout', str(timeout))
    completed('applications')
    health = verify(root, 'basic', timeout=timeout)
    if not health['passed']:
        raise RuntimeError('Readiness verification failed; inspect the saved verification report')
    state['status'] = 'ready'
    completed('readiness')
    return {'installed': True, 'installation_id': state['installation_id'], 'release': cfg['release'],
            'readiness_passed': True, 'business_acceptance_passed': False,
            'next': 'Run verify --suite full, then a separate-directory backup/restore rehearsal'}


def stop(root, timeout=120):
    cfg = read_config(root)
    assert_project_owner(root, cfg, docker)
    # Validate all candidates before stopping any application or sandbox.
    assert_sandbox_owners(root, cfg, docker)
    compose(root, 'stop', '--timeout', str(timeout), 'edge', 'api', 'coordinator', 'gateway', 'graph')
    # Graph shutdown may remove sandboxes or finish an in-flight creation.
    # Reinspect survivors, and stop by immutable ID rather than a reusable name.
    for identifier in assert_sandbox_owners(root, cfg, docker):
        docker('stop', '--time', str(timeout), identifier)
    return {'application_writers_stopped': True, 'data_services_left_running': True}


def start(root, *, timeout=600):
    """Start only the recorded installation, never an implicit image upgrade."""
    cfg = read_config(root)
    state_path = root / 'data/platform/installation.json'
    pending = root / 'data/.restore-pending.json'
    if (pending.exists() or pending.is_symlink() or state_path.is_symlink()
            or not state_path.is_file()):
        raise RuntimeError('Run install to complete migrations and restoration before start')
    state = json.loads(state_path.read_text())
    if not isinstance(state, dict) or state.get('status') != 'ready':
        raise RuntimeError('Run install to complete migrations and restoration before start')
    if (state.get('root') != str(root) or state.get('project') != cfg['project']
            or state.get('release') != cfg['release']):
        raise RuntimeError('Installation identity differs; use a separate directory and project')
    report = preflight(root, cfg, docker=docker, asset_report=asset_report)
    _private_json(root / 'evidence/preflight.json', report)
    if not report['passed']:
        raise RuntimeError('Preflight failed: ' + '; '.join(report['errors']))
    recorded = state.get('images')
    if (not isinstance(recorded, dict) or not recorded
            or any(not isinstance(image, dict) or not isinstance(image.get('id'), str)
                   for image in recorded.values())
            or {key: value['id'] for key, value in recorded.items()} !=
               {key: value['id'] for key, value in report['images'].items()}):
        raise RuntimeError('Installed image identities changed; use an explicit upgrade')
    generated = current(root)
    expected = compose_document(root, generated, cfg)
    try:
        rendered = yaml.safe_load((generated / 'compose.yaml').read_text())
    except yaml.YAMLError:
        raise RuntimeError('Generated Compose is invalid; review before start') from None
    if rendered != expected:
        raise RuntimeError('Generated Compose differs from this installation configuration; review before start')
    # Existing-container checks do not cover a stopped site's removed services.
    # Validate planned bind sources as well, before Docker can follow redirects.
    external = {'/var/run/docker.sock', cfg['services'].get('nas_path')}
    for service in expected['services'].values():
        for mount in service.get('volumes', []):
            if mount['source'] not in external:
                source = Path(mount['source'])
                if source != source.resolve() or not source.is_relative_to(root):
                    raise RuntimeError('Planned container mount is redirected or outside this installation')
    compose(root, 'config', '--quiet')
    compose(root, 'up', '-d', '--wait', '--wait-timeout', str(timeout))
    return {'started': True}


def main():
    parser = argparse.ArgumentParser(prog='citybrain')
    parser.add_argument('--root', default=os.environ.get('CITYBRAIN_ROOT', '/srv/citybrain'))
    sub = parser.add_subparsers(dest='command', required=True)
    for name in ('init', 'check', 'config-render', 'status'):
        sub.add_parser(name)
    p = sub.add_parser('install'); p.add_argument('--mode', choices=['fresh'], default='fresh'); p.add_argument('--timeout', type=int, default=600)
    p = sub.add_parser('build'); p.add_argument('--component', choices=['onecity', 'deerflow', 'edge', 'sandbox', 'toolbox'], action='append', required=True)
    p = sub.add_parser('start'); p.add_argument('--timeout', type=int, default=600)
    p = sub.add_parser('stop'); p.add_argument('--timeout', type=int, default=120)
    p = sub.add_parser('logs'); p.add_argument('--follow', action='store_true')
    p = sub.add_parser('verify'); p.add_argument('--suite', choices=['basic','interfaces','scenes','full'], default='basic'); p.add_argument('--base-url'); p.add_argument('--credentials-file', type=Path); p.add_argument('--scene-manifest', type=Path); p.add_argument('--timeout', type=float, default=600); p.add_argument('--report-path', type=Path)
    p = sub.add_parser('verify-cleanup'); p.add_argument('--state', type=Path, required=True)
    p = sub.add_parser('assets-import'); p.add_argument('--source', type=Path, default=build_source())
    p = sub.add_parser('backup'); p.add_argument('--output', type=Path, required=True)
    p = sub.add_parser('restore'); p.add_argument('--from', dest='source', type=Path, required=True); p.add_argument('--project'); p.add_argument('--http-port', type=int); p.add_argument('--public-url')
    p = sub.add_parser('release'); p.add_argument('action', choices=['pack','check']); p.add_argument('--version'); p.add_argument('--output', type=Path); p.add_argument('--path', type=Path); p.add_argument('--include-images', action='store_true')
    p = sub.add_parser('export-evidence'); p.add_argument('--output', type=Path, required=True)
    p = sub.add_parser('upgrade'); p.add_argument('action', choices=['plan','apply']); p.add_argument('--to', type=Path, required=True)
    p = sub.add_parser('rollback'); p.add_argument('action', choices=['plan','apply']); p.add_argument('--to', type=Path, required=True)
    args = parser.parse_args()
    try:
        root = root_arg(args.root)
        with site_lock(root):
            if args.command == 'init': result = initialize(root)
            elif args.command == 'assets-import': result = import_assets(root, args.source)
            elif args.command == 'config-render': result = render(root)
            elif args.command == 'check': result = preflight(root, read_config(root), docker=docker, asset_report=asset_report)
            elif args.command == 'install': result = install(root, timeout=args.timeout)
            elif args.command == 'verify':
                result = verify(root, args.suite, base_url=args.base_url, credentials_file=args.credentials_file, scene_manifest=args.scene_manifest, timeout=args.timeout, report_path=args.report_path)
            elif args.command == 'verify-cleanup':
                from deploy.cli.verification import cleanup_verification
                result = cleanup_verification(root, args.state)
            elif args.command == 'status': result = status(root)
            elif args.command == 'stop': result = stop(root, args.timeout)
            elif args.command == 'start': result = start(root, timeout=args.timeout)
            elif args.command == 'logs':
                assert_project_owner(root, read_config(root), docker)
                compose(root, 'logs', '--follow' if args.follow else '--tail=200', capture=False); return 0
            elif args.command == 'backup': result = save_backup(root, args.output)
            elif args.command == 'restore': result = restore(root, args.source, project=args.project, http_port=args.http_port, public_url=args.public_url)
            elif args.command == 'build':
                cfg = read_config(root)
                tags = {**cfg['images'], 'toolbox': os.getenv('CITYBRAIN_TOOLBOX_IMAGE', 'citybrain-toolbox:local')}
                recipes = {'onecity':'OneCity','deerflow':'DeerFlow','edge':'Edge','toolbox':'Toolbox'}
                for component in args.component:
                    recipe = 'docker/sandbox/Dockerfile' if component == 'sandbox' else 'deploy/images/' + recipes[component] + '.Dockerfile'
                    docker('build', '--network', 'host', '--file', build_source()/recipe, '--tag', tags[component], build_source(), capture=False)
                result = {'built': args.component}
            elif args.command in {'release','upgrade','rollback','export-evidence'}:
                from deploy.cli.release import release_command
                result = release_command(root, args, docker=docker, compose=compose, render=render, verify=verify)
            else: raise ValueError('Unsupported command')
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if result.get('passed', True) else 1
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError) as error:
        print(json.dumps({'error': str(error)}, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
