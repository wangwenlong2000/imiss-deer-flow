"""Read-only deployment prerequisites, image identity and ownership checks."""
from __future__ import annotations

import errno
import hashlib
import json
from pathlib import Path
import re
import shutil
import socket
import stat

from deploy.cli.configuration import SECRET_FILES


def required_images(config: dict) -> dict[str, str]:
    names = ['onecity', 'deerflow', 'edge', 'sandbox']
    if config['mysql']['mode'] in {'local', 'external'}:
        names.append('mysql')  # Versioned client for local/external logical backups.
    if config['elasticsearch']['mode'] == 'local':
        names.append('elasticsearch')
    return {name: config['images'][name] for name in names}


def _inspect_containers(identifiers, docker):
    if not identifiers:
        return []
    try:
        value = json.loads(docker('inspect', *identifiers).stdout)
    except (ValueError, TypeError) as error:
        raise RuntimeError('Invalid Docker container inspection response') from error
    if not isinstance(value, list) or len(value) != len(identifiers) or not all(isinstance(x, dict) for x in value):
        raise RuntimeError('Incomplete Docker container inspection response')
    return value


def _assert_container_owner(root: Path, config: dict, container: dict) -> None:
    expected_root = root.resolve()
    metadata = container.get('Config') or {}
    if not isinstance(metadata, dict):
        raise RuntimeError('Invalid Docker container label inspection')
    labels = metadata.get('Labels') or {}
    if (not isinstance(labels, dict) or labels.get('io.citybrain.root') != str(expected_root)
            or labels.get('com.docker.compose.project') != config['project']):
        raise RuntimeError('Compose project already belongs to another installation; choose a distinct project name')
    # A matching text label must not conceal mounts to another site's state.
    protected = {
        '/data/deerflow': root/'data/deerflow', '/data/platform': root/'data/platform',
        '/app/backend/.langgraph_api': root/'data/langgraph', '/var/lib/mysql': root/'data/mysql',
        '/usr/share/elasticsearch/data': root/'data/elasticsearch',
        **{'/app/'+name: root/'assets'/name for name in ('skills','models','datasets','config')},
    }
    mounts = container.get('Mounts', [])
    if not isinstance(mounts, list):
        raise RuntimeError('Invalid Docker container mount inspection')
    for mount in mounts:
        if not isinstance(mount, dict):
            raise RuntimeError('Invalid Docker container mount inspection')
        target, raw_source = mount.get('Destination', ''), mount.get('Source', '')
        if not isinstance(target, str) or not isinstance(raw_source, str):
            raise RuntimeError('Invalid Docker container mount paths')
        source = Path(raw_source)
        for prefix, expected in protected.items():
            if target == prefix or target.startswith(prefix + '/'):
                suffix = target[len(prefix):].lstrip('/')
                if (not expected.resolve().is_relative_to(expected_root)
                        or mount.get('Type') != 'bind' or not source.is_absolute()
                        or source.resolve() != (expected/suffix).resolve()):
                    raise RuntimeError('Container state mount belongs to another installation')
        if target == '/etc/citybrain' or target.startswith('/etc/citybrain/'):
            generated = (root/'generated').resolve()
            if not generated.is_relative_to(expected_root) or mount.get('Type') != 'bind' or not source.is_absolute() or not source.resolve().is_relative_to(generated):
                raise RuntimeError('Container configuration mount is outside this installation')
        if target.startswith('/run/secrets/'):
            secrets = (root/'secrets').resolve()
            if not secrets.is_relative_to(expected_root) or mount.get('Type') != 'bind' or not source.is_absolute() or not source.resolve().is_relative_to(secrets):
                raise RuntimeError('Container secret mount is outside this installation')


def assert_project_owner(root: Path, config: dict, docker) -> None:
    """A shared project name or path prefix never authorizes adopting a site."""
    root = Path(root)
    if not root.is_absolute() or root == Path('/') or root.is_symlink():
        raise RuntimeError('Installation root must be a real absolute directory other than /')
    result = docker('ps', '-aq', '--filter', 'label=com.docker.compose.project=' + config['project'])
    for container in _inspect_containers(result.stdout.split(), docker):
        _assert_container_owner(root, config, container)


def assert_sandbox_owners(root: Path, config: dict, docker) -> list[str]:
    """Validate every running name candidate before authorizing any stop.

    A sandbox is outside Compose's service collection. Its deterministic name
    only locates candidates; labels, the installed image, network and exact
    thread/asset mounts must independently establish ownership.
    """
    root = Path(root)
    expected_root = root.resolve()
    if not root.is_absolute() or root == Path('/') or root.is_symlink():
        raise RuntimeError('Invalid installation root for sandbox ownership')
    prefix = config['project'] + '-sandbox-'
    result = docker('ps', '-q', '--no-trunc', '--filter', 'name=^/' + re.escape(prefix))
    identifiers = result.stdout.split()
    if not identifiers:
        return []
    if len(set(identifiers)) != len(identifiers) or any(not re.fullmatch(r'[0-9a-f]{64}', value) for value in identifiers):
        raise RuntimeError('Invalid sandbox container identities')
    state_path = root / 'data/platform/installation.json'
    try:
        state = json.loads(state_path.read_text())
        expected_image = state['images']['sandbox']['id']
        if (state.get('root') != str(expected_root) or state.get('project') != config['project']
                or state.get('release') != config['release']
                or not re.fullmatch(r'sha256:[0-9a-f]{64}', expected_image)):
            raise ValueError()
    except (OSError, ValueError, TypeError, KeyError):
        raise RuntimeError('Sandbox stop requires a matching installed image identity') from None
    image_result = docker('image', 'inspect', config['images']['sandbox'])
    try:
        image = json.loads(image_result.stdout)
        if not isinstance(image, list) or len(image) != 1 or image[0].get('Id') != expected_image:
            raise ValueError()
    except (ValueError, TypeError, AttributeError):
        raise RuntimeError('Installed sandbox image identity changed') from None
    containers = _inspect_containers(identifiers, docker)
    if {item.get('Id') for item in containers} != set(identifiers):
        raise RuntimeError('Sandbox inspection did not return the requested container identities')
    for container in containers:
        metadata = container.get('Config') or {}
        labels = metadata.get('Labels') or {} if isinstance(metadata, dict) else {}
        if (not isinstance(labels, dict) or labels.get('io.citybrain.root') != str(expected_root)
                or labels.get('io.citybrain.project') != config['project']
                or labels.get('io.citybrain.role') != 'sandbox'
                or container.get('Image') != expected_image):
            raise RuntimeError('Dynamic sandbox labels or image belong to another installation')
        network = container.get('NetworkSettings') or {}
        networks = network.get('Networks') if isinstance(network, dict) else None
        if not isinstance(networks, dict) or set(networks) != {config['project'] + '-services'}:
            raise RuntimeError('Dynamic sandbox is not exclusively on this installation network')
        name = container.get('Name', '')
        if not isinstance(name, str) or not re.fullmatch('/' + re.escape(prefix) + r'[0-9a-f]{8}', name):
            raise RuntimeError('Dynamic sandbox name differs from the expected thread identity')
        mounts = container.get('Mounts')
        if not isinstance(mounts, list):
            raise RuntimeError('Invalid dynamic sandbox mount inspection')
        seen, threads = set(), set()
        readonly = {'/mnt/skills': root/'assets/skills', '/mnt/datasets': root/'assets/datasets'}
        nas = config.get('services', {}).get('nas_path')
        if nas:
            readonly['/mnt/nas/streetview_meta'] = Path(nas)
        for mount in mounts:
            if not isinstance(mount, dict):
                raise RuntimeError('Invalid dynamic sandbox mount inspection')
            target, raw_source = mount.get('Destination'), mount.get('Source')
            if (not isinstance(target, str) or target in seen or not isinstance(raw_source, str)
                    or mount.get('Type') != 'bind'):
                raise RuntimeError('Unexpected dynamic sandbox mount')
            seen.add(target)
            source = Path(raw_source)
            if target in readonly:
                expected = readonly[target]
                if mount.get('RW') is not False:
                    raise RuntimeError('Dynamic sandbox asset mount must be read-only')
            elif target in {'/mnt/user-data/' + part for part in ('workspace', 'uploads', 'outputs')}:
                try:
                    relative = source.relative_to(root/'data/deerflow/threads')
                except ValueError:
                    raise RuntimeError('Dynamic sandbox data mount belongs to another installation') from None
                part = target.rsplit('/', 1)[1]
                if (len(relative.parts) != 3 or not re.fullmatch(r'[A-Za-z0-9_-]+', relative.parts[0])
                        or relative.parts[1:] != ('user-data', part) or mount.get('RW') is not True):
                    raise RuntimeError('Dynamic sandbox data mount differs from its thread path')
                threads.add(relative.parts[0])
                expected = root/'data/deerflow/threads'/relative.parts[0]/'user-data'/part
            else:
                raise RuntimeError('Unexpected dynamic sandbox mount target')
            if (not source.is_absolute() or source != expected or source.resolve() != expected
                    or (target != '/mnt/nas/streetview_meta' and not expected.is_relative_to(expected_root))):
                raise RuntimeError('Dynamic sandbox mount is redirected or outside this installation')
        required = {'/mnt/skills', '/mnt/datasets', *('/mnt/user-data/' + part for part in ('workspace', 'uploads', 'outputs'))}
        if not required.issubset(seen) or len(threads) != 1:
            raise RuntimeError('Dynamic sandbox lacks the exact installation thread mounts')
        if name != '/' + prefix + hashlib.sha256(next(iter(threads)).encode()).hexdigest()[:8]:
            raise RuntimeError('Dynamic sandbox name does not match its mounted thread')
    return identifiers


def inspect_images(config: dict, docker) -> tuple[dict, list[str]]:
    inventory, errors = {}, []
    for component, reference in required_images(config).items():
        if not isinstance(reference, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._/:@-]*', reference):
            errors.append('invalid image reference: ' + component)
            continue
        result = docker('image', 'inspect', reference, check=False)
        if result.returncode:
            errors.append('missing image: ' + component)
            continue
        try:
            values = json.loads(result.stdout)
            if not isinstance(values, list) or len(values) != 1 or not isinstance(values[0], dict):
                raise ValueError()
            data = values[0]
            if not re.fullmatch(r'sha256:[0-9a-f]{64}', data.get('Id', '')):
                raise ValueError()
            digests = data.get('RepoDigests') or []
            if not isinstance(digests, list) or not all(isinstance(x, str) for x in digests):
                raise ValueError()
        except (ValueError, TypeError):
            errors.append('invalid image inspection metadata: ' + component)
            continue
        if data.get('Os') != 'linux' or data.get('Architecture') != 'amd64':
            errors.append('unsupported image platform: ' + component)
        inventory[component] = {'reference': reference, 'id': data['Id'], 'repo_digests': digests}
    return inventory, errors


def _owns_http_port(root, config, docker) -> bool:
    result = docker('ps', '--filter', 'label=com.docker.compose.project=' + config['project'],
                    '--filter', 'label=com.docker.compose.service=edge', '--format', '{{.ID}}')
    for container in _inspect_containers(result.stdout.split(), docker):
        _assert_container_owner(root, config, container)
        labels = container.get('Config', {}).get('Labels') or {}
        state, network = container.get('State') or {}, container.get('NetworkSettings') or {}
        if not isinstance(state, dict) or not isinstance(network, dict):
            raise RuntimeError('Invalid Docker edge state or port inspection')
        if labels.get('com.docker.compose.service') != 'edge' or state.get('Running') is not True:
            continue
        ports = network.get('Ports') or {}
        if not isinstance(ports, dict):
            raise RuntimeError('Invalid Docker edge port inspection')
        for binding in ports.get('8080/tcp') or []:
            if isinstance(binding, dict) and str(binding.get('HostPort')) == str(config['http_port']):
                return True
    return False


def preflight(root: Path, config: dict, *, docker, asset_report, check_port: bool = True) -> dict:
    root = Path(root)
    try:
        asset_errors, asset_warnings = asset_report(root, config)
        errors, warnings = list(asset_errors), list(asset_warnings)
    except (OSError, ValueError):
        errors, warnings = ['Asset preflight could not read installation files'], []
    secrets = root/'secrets'
    directory_ok = secrets.is_dir() and not secrets.is_symlink()
    if not directory_ok:
        errors.append('missing or redirected secrets directory')
    elif stat.S_IMODE(secrets.stat().st_mode) != 0o700:
        errors.append('secrets directory permissions must be 0700')
    for name in (*SECRET_FILES, 'private.pem'):
        path = secrets/name
        if not directory_ok or not path.is_file() or path.is_symlink():
            errors.append('missing or redirected secret file: ' + name)
        elif path.stat().st_mode & 0o077:
            errors.append('secret permissions must be 0600: ' + name)
    try:
        free = shutil.disk_usage(root).free
        if free < 1024**3:
            errors.append('less than 1 GiB free at installation root')
    except OSError:
        free = 0
        errors.append('installation root is missing or inaccessible')
    inventory = {}
    try:
        docker('version', '--format', '{{.Server.Version}}')
        docker('compose', 'version')
        assert_project_owner(root, config, docker)
        inventory, missing = inspect_images(config, docker)
        errors.extend(missing)
    except (OSError, RuntimeError, ValueError):
        errors.append('Docker/Compose unavailable or project belongs to another installation')
    if check_port:
        try:
            occupied_by_site = _owns_http_port(root, config, docker)
        except (OSError, RuntimeError, ValueError):
            errors.append('Could not establish HTTP port ownership')
            occupied_by_site = False
        if not occupied_by_site:
            families = [socket.AF_INET] + ([socket.AF_INET6] if socket.has_ipv6 else [])
            for family in families:
                try:
                    with socket.socket(family, socket.SOCK_STREAM) as probe:
                        if family == socket.AF_INET6:
                            probe.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
                        probe.bind(('::' if family == socket.AF_INET6 else '0.0.0.0', config['http_port']))
                except OSError as error:
                    if family == socket.AF_INET6 and error.errno in {errno.EAFNOSUPPORT, errno.EADDRNOTAVAIL, errno.ENOPROTOOPT}:
                        continue
                    errors.append('HTTP port already in use or unavailable')
                    break
    if config['elasticsearch']['mode'] != 'disabled':
        warnings.append('Elasticsearch snapshots require a separately verified recovery procedure')
    return {'schema_version': 1, 'passed': not errors, 'root': str(root), 'project': config['project'],
            'capabilities': config['capabilities'], 'images': inventory, 'errors': errors, 'warnings': warnings,
            'free_bytes': free, 'external_services_contacted': False, 'business_scenarios_executed': False}
