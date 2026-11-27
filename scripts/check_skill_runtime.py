#!/usr/bin/env python3
"""Offline, read-only validator for the generated Skill runtime declaration.

It reads repository files and optional previously captured package metadata. It
never imports a Skill, starts a process, installs a package, or contacts a service.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import date
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = Path('skills/runtime-manifest.json')
SCHEMA = Path('skills/skill_runtime.schema.json')
DEFAULT_METADATA = Path('docs/skill-dependencies-20261126/runtime-sandbox.json')


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def tree_sha256(path: Path, root: Path) -> str:
    h = hashlib.sha256()
    for item in sorted((p for p in path.rglob('*') if p.is_file()), key=lambda p: p.as_posix()):
        h.update(item.relative_to(root).as_posix().encode())
        h.update(b'\0')
        h.update(bytes.fromhex(sha256_file(item)))
    return h.hexdigest()


def json_pointer(root: dict, pointer: str) -> Any:
    current: Any = root
    for part in pointer.lstrip('#/').split('/'):
        if not part:
            continue
        part = part.replace('~1', '/').replace('~0', '~')
        current = current[part]
    return current


def _type_matches(value: Any, expected: str) -> bool:
    if expected == 'object': return isinstance(value, dict)
    if expected == 'array': return isinstance(value, list)
    if expected == 'string': return isinstance(value, str)
    if expected == 'boolean': return isinstance(value, bool)
    if expected == 'integer': return isinstance(value, int) and not isinstance(value, bool)
    if expected == 'number': return isinstance(value, (int, float)) and not isinstance(value, bool)
    if expected == 'null': return value is None
    return True


def validate_schema(value: Any, schema: dict, root_schema: dict | None = None, path: str = '$') -> list[str]:
    """Validate the JSON Schema subset used by skill_runtime.schema.json."""
    root_schema = root_schema or schema
    errors: list[str] = []
    if '$ref' in schema:
        try:
            ref_schema = json_pointer(root_schema, schema['$ref'])
        except (KeyError, TypeError):
            return [f'{path}: unresolved schema reference {schema["$ref"]}']
        return validate_schema(value, ref_schema, root_schema, path)
    expected = schema.get('type')
    if expected is not None:
        types = expected if isinstance(expected, list) else [expected]
        if not any(_type_matches(value, t) for t in types):
            return [f'{path}: expected type {expected}']
    if 'const' in schema and value != schema['const']:
        errors.append(f'{path}: value does not match const')
    if 'enum' in schema and value not in schema['enum']:
        errors.append(f'{path}: value is outside enum')
    if isinstance(value, str):
        if len(value) < schema.get('minLength', 0): errors.append(f'{path}: string is too short')
        pattern = schema.get('pattern')
        if pattern and not re.search(pattern, value): errors.append(f'{path}: string does not match pattern')
        if schema.get('format') == 'date':
            try: date.fromisoformat(value)
            except ValueError: errors.append(f'{path}: invalid date format')
    if isinstance(value, list):
        if len(value) < schema.get('minItems', 0): errors.append(f'{path}: array has too few items')
        if schema.get('uniqueItems') and len({json.dumps(x, sort_keys=True, ensure_ascii=False) for x in value}) != len(value):
            errors.append(f'{path}: array items are not unique')
        if 'items' in schema:
            for index, item in enumerate(value):
                errors.extend(validate_schema(item, schema['items'], root_schema, f'{path}[{index}]'))
    if isinstance(value, dict):
        missing = [key for key in schema.get('required', []) if key not in value]
        errors.extend(f'{path}: missing required property {key}' for key in missing)
        properties = schema.get('properties', {})
        for key, item in value.items():
            if key in properties:
                errors.extend(validate_schema(item, properties[key], root_schema, f'{path}.{key}'))
            elif schema.get('additionalProperties') is False:
                errors.append(f'{path}: unexpected property {key}')
            elif isinstance(schema.get('additionalProperties'), dict):
                errors.extend(validate_schema(item, schema['additionalProperties'], root_schema, f'{path}.{key}'))
    one_of = schema.get('oneOf')
    if one_of is not None:
        successes = [not validate_schema(value, child, root_schema, path) for child in one_of]
        if sum(successes) != 1:
            errors.append(f'{path}: value does not match exactly one oneOf branch')
    any_of = schema.get('anyOf')
    if any_of is not None:
        if not any(not validate_schema(value, child, root_schema, path) for child in any_of):
            errors.append(f'{path}: value does not match any anyOf branch')
    return errors


def _inside(root: Path, relative: str) -> Path | None:
    candidate = root / relative
    try:
        resolved = candidate.resolve(strict=False)
        resolved.relative_to(root.resolve())
    except (ValueError, OSError):
        return None
    return resolved


def _emit(checks: list[dict], code: str, status: str, message: str, **fields) -> None:
    checks.append({'code': code, 'status': status, 'message': message, **fields})


def _norm_package(value: str) -> str:
    return re.sub(r'[-_.]+', '-', value).lower()


def _metadata_maps(metadata: dict | None) -> tuple[dict[str, str], dict[str, dict], dict[str, dict]]:
    packages: dict[str, str] = {}
    for entry in (metadata or {}).get('packages', []):
        name = entry.get('name')
        version = entry.get('selected_version') or entry.get('version')
        if name and version:
            packages[_norm_package(name)] = str(version)
    node = (metadata or {}).get('node_packages', {})
    commands = (metadata or {}).get('commands', {})
    return packages, node, commands


def check_runtime(root: Path = ROOT, *, skill_ids: list[str] | None = None,
                  capabilities: list[str] | None = None, profiles: list[str] | None = None,
                  metadata_path: Path | None = DEFAULT_METADATA,
                  verify_evidence: bool = True) -> dict:
    """Return a JSON-serializable static check report without mutating the workspace."""
    root = root.resolve()
    checks: list[dict] = []
    manifest_path = _inside(root, str(MANIFEST))
    schema_path = _inside(root, str(SCHEMA))
    if not manifest_path or not schema_path or not manifest_path.is_file() or not schema_path.is_file():
        _emit(checks, 'manifest_or_schema_missing', 'error', 'Runtime manifest or schema is missing.')
        return _report(checks, root, [])
    try:
        manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
        schema = json.loads(schema_path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError) as exc:
        _emit(checks, 'manifest_or_schema_invalid', 'error', f'Cannot read JSON manifest/schema: {type(exc).__name__}.')
        return _report(checks, root, [])
    schema_errors = validate_schema(manifest, schema)
    if schema_errors:
        for error in schema_errors[:100]:
            _emit(checks, 'schema_invalid', 'error', error)
    else:
        _emit(checks, 'schema_valid', 'pass', 'Manifest conforms to the checked-in Skill runtime schema.')
    manifest_skills = manifest.get('skills', []) if isinstance(manifest, dict) else []
    ids = [s.get('skill_id') for s in manifest_skills]
    if len(ids) != len(set(ids)):
        _emit(checks, 'duplicate_skill_id', 'error', 'Manifest contains duplicate Skill identifiers.')
    audit_ref = (manifest.get('source_audit') or {}).get('path')
    audit_path = _inside(root, audit_ref) if audit_ref else None
    if audit_path is None or not audit_path.is_file():
        _emit(checks, 'audit_source_missing', 'error', 'Dependency audit source referenced by manifest is missing.')
    elif sha256_file(audit_path) != manifest['source_audit']['sha256']:
        _emit(checks, 'audit_source_stale', 'error', 'Dependency audit source hash differs from the manifest.')
    else:
        try:
            audit = json.loads(audit_path.read_text(encoding='utf-8'))
            expected = {s['skill_name'] for s in audit.get('skills', [])}
            actual = set(ids)
            if expected != actual:
                _emit(checks, 'skill_inventory_mismatch', 'error', f'Skill inventory differs: expected {len(expected)}, declared {len(actual)}.')
            else:
                _emit(checks, 'skill_inventory_valid', 'pass', f'All {len(actual)} audited Skills have one runtime declaration.')
        except (OSError, json.JSONDecodeError, KeyError) as exc:
            _emit(checks, 'audit_source_invalid', 'error', f'Cannot verify audit inventory: {type(exc).__name__}.')
    requested = set(skill_ids or [])
    known = set(ids)
    unknown = sorted(requested - known)
    for name in unknown:
        _emit(checks, 'unknown_skill', 'error', f'Unknown requested Skill id: {name}.', skill_id=name)
    selected = []
    for skill in manifest_skills:
        if requested and skill['skill_id'] not in requested:
            continue
        if capabilities and not (set(capabilities) & set(skill.get('capability_groups', []))):
            continue
        if profiles and skill.get('runtime_profile') not in set(profiles):
            continue
        selected.append(skill)
    if not selected and not unknown:
        _emit(checks, 'empty_selection', 'error', 'No Skills match the selected ids/capabilities/profiles.')
    packages: dict[str, str] = {}
    node_packages: dict = {}
    commands: dict = {}
    if metadata_path is not None:
        meta = _inside(root, str(metadata_path)) if not metadata_path.is_absolute() else metadata_path
        if meta and meta.is_file():
            try:
                runtime_meta = json.loads(meta.read_text(encoding='utf-8'))
                packages, node_packages, commands = _metadata_maps(runtime_meta)
                _emit(checks, 'runtime_metadata_loaded', 'pass', 'Loaded the supplied package metadata snapshot; values are inventory only.', path=str(metadata_path), scope=runtime_meta.get('scope', 'unspecified'))
            except (OSError, json.JSONDecodeError) as exc:
                _emit(checks, 'runtime_metadata_invalid', 'error', f'Cannot read runtime metadata: {type(exc).__name__}.')
        else:
            _emit(checks, 'runtime_metadata_missing', 'warning', 'No package metadata snapshot was available; package presence was not checked.', path=str(metadata_path))
    else:
        _emit(checks, 'runtime_metadata_omitted', 'warning', 'Package presence was not checked because no metadata snapshot was supplied.')
    metadata_scope = 'sandbox' in ((runtime_meta.get('scope', '') if metadata_path and 'runtime_meta' in locals() else '').lower())
    python_metadata_present = bool(metadata_path and 'runtime_meta' in locals() and runtime_meta.get('python'))
    for skill in selected:
        sid = skill['skill_id']
        skill_file = _inside(root, skill['path'] + '/SKILL.md')
        expected_skill_hash = skill.get('source_hashes', {}).get(skill['path'] + '/SKILL.md')
        if skill_file is None or not skill_file.is_file():
            _emit(checks, 'skill_file_missing', 'error', 'SKILL.md is not present at the declared path.', skill_id=sid)
        elif expected_skill_hash and sha256_file(skill_file) != expected_skill_hash:
            _emit(checks, 'skill_source_stale', 'error', 'SKILL.md differs from the manifest source hash; regenerate the manifest.', skill_id=sid, path=skill['path'] + '/SKILL.md')
        else:
            _emit(checks, 'skill_file_valid', 'pass', 'SKILL.md exists and matches its source hash.', skill_id=sid)
        for relative, expected_hash in skill.get('source_hashes', {}).items():
            target = _inside(root, relative)
            if target is None or not target.is_file():
                _emit(checks, 'source_file_missing', 'error', 'A source file in the runtime declaration is missing.', skill_id=sid, path=relative)
            elif sha256_file(target) != expected_hash:
                _emit(checks, 'source_hash_stale', 'error', 'A declared source file changed; regenerate the manifest.', skill_id=sid, path=relative)
        for action in skill.get('actions', []):
            target = _inside(root, action['path'])
            if target is None or not target.is_file():
                _emit(checks, 'action_missing', 'error', 'Declared action source is missing.', skill_id=sid, action_id=action['id'], path=action['path'])
            elif action.get('automatic_execution') is not False:
                _emit(checks, 'automatic_execution_not_disabled', 'error', 'Static runtime declarations must not grant automatic execution.', skill_id=sid, action_id=action['id'])
        for asset in skill.get('assets', []):
            target = _inside(root, asset['path'])
            if target is None or not target.exists():
                _emit(checks, 'asset_missing', 'error', 'Required source-checkout asset is missing.', skill_id=sid, asset_id=asset['asset_id'], path=asset['path'])
            elif target.is_file() and asset.get('sha256') and sha256_file(target) != asset['sha256']:
                _emit(checks, 'asset_hash_mismatch', 'error', 'Asset SHA-256 differs from the manifest.', skill_id=sid, asset_id=asset['asset_id'], path=asset['path'])
            elif target.is_dir() and asset.get('tree_sha256') and tree_sha256(target, root) != asset['tree_sha256']:
                _emit(checks, 'asset_tree_hash_mismatch', 'error', 'Asset directory tree hash differs from the manifest.', skill_id=sid, asset_id=asset['asset_id'], path=asset['path'])
            elif asset.get('kind') == 'native_binary' and asset.get('executable') and not (target.stat().st_mode & 0o111):
                _emit(checks, 'asset_not_executable', 'error', 'Native asset exists but has no execute bit.', skill_id=sid, asset_id=asset['asset_id'], path=asset['path'])
            else:
                _emit(checks, 'asset_valid', 'pass', 'Declared source-checkout asset exists and matches known metadata.', skill_id=sid, asset_id=asset['asset_id'], path=asset['path'])
        for requirement in skill.get('requirements', []):
            if requirement.get('kind') not in {'python', 'node'}:
                continue
            scope = requirement.get('runtime_observation', {}).get('scope', '')
            if any(marker in scope.lower() for marker in ('后端', 'backend')):
                _emit(checks, 'dependency_other_runtime_scope', 'skipped',
                    'Dependency is declared for the DeerFlow/backend runtime, not the sandbox package inventory.',
                    skill_id=sid, dependency=requirement['name'], runtime_scope=scope)
                continue
            level = requirement.get('level')
            if level == 'optional':
                continue
            name = requirement['name']
            observed = False
            version = None
            if requirement['kind'] == 'python':
                key = _norm_package(name)
                version = packages.get(key)
                observed = version is not None
                if key in {'python', 'python3'}:
                    version = runtime_meta.get('python') if python_metadata_present else None
                    observed = bool(version)
            else:
                record = node_packages.get(name) or node_packages.get(_norm_package(name))
                observed = isinstance(record, dict) and bool(record.get('version'))
                version = record.get('version') if observed else None
            if not metadata_path or not packages and not node_packages:
                continue
            if observed:
                _emit(checks, 'dependency_observed', 'pass', 'Required dependency appears in selected_version package metadata; this is not an import or feature test.', skill_id=sid, dependency=name, version=version, kind=requirement['kind'])
            elif level == 'required' and metadata_scope:
                _emit(checks, 'dependency_missing', 'error', 'Required dependency is absent from the supplied sandbox package metadata.', skill_id=sid, dependency=name, kind=requirement['kind'])
            elif level == 'required':
                _emit(checks, 'dependency_not_observed', 'warning', 'Required dependency was not proven in a metadata snapshot for this runtime scope.', skill_id=sid, dependency=name, kind=requirement['kind'])
            elif level == 'conditional':
                _emit(checks, 'conditional_dependency_not_observed', 'warning', 'Conditional dependency is absent or not recorded; validate only if the corresponding branch is enabled.', skill_id=sid, dependency=name, kind=requirement['kind'])
        for requirement in skill.get('requirements', []):
            if requirement.get('kind') != 'tool' or requirement.get('level') == 'optional':
                continue
            tool = requirement['name'].lower()
            if 'opengrep' in tool and any(word in tool for word in ('binary', '二进制')):
                    # The local asset check above is authoritative; PATH need not contain it.
                continue
            # Only compare actual command-like declarations. Broad package/hardware
            # descriptions such as "GDAL native libraries" are not PATH commands.
            command_aliases = {'ffmpeg': 'ffmpeg',
                'ffprobe': 'ffprobe', 'tshark': 'tshark', 'zeek': 'zeek',
                'node': 'node', 'npm': 'npm', 'bash': 'bash', 'curl': 'curl',
                'chromium-browser': 'chromium-browser'}
            alias = next((value for token, value in command_aliases.items()
                if re.fullmatch(rf'\s*{re.escape(token)}\s*', tool)), None)
            if tool.strip() in {'python', 'python3'} and python_metadata_present:
                continue
            matched = commands.get(alias) if alias else None
            if matched is None:
                if requirement.get('level') == 'required' and alias:
                    _emit(checks, 'tool_presence_unobserved', 'warning', 'Required system command has no matching command metadata; no command was executed.', skill_id=sid, tool=requirement['name'])
                if requirement.get('level') == 'required':
                    # Hardware, shared libraries, and browser-install requirements
                    # need a capability-specific runtime check, not a PATH guess.
                    if not alias:
                        _emit(checks, 'tool_manual_check', 'skipped', 'Requirement is not an exact command name; no PATH or hardware availability is inferred.', skill_id=sid, tool=requirement['name'])
            elif matched.get('status') == 'not_in_PATH':
                _emit(checks, 'tool_missing_from_path', 'error' if requirement.get('level') == 'required' else 'warning', 'System command is not in PATH in the supplied image metadata.', skill_id=sid, tool=requirement['name'])
        if verify_evidence:
            for requirement in skill.get('requirements', []):
                for evidence in requirement.get('evidence', []):
                    file_name, separator, line_text = evidence.rpartition(':')
                    if not separator or not line_text.isdigit():
                        continue
                    evidence_path = _inside(root, file_name)
                    if evidence_path is None or not evidence_path.is_file():
                        _emit(checks, 'evidence_missing', 'error', 'Dependency evidence file is missing.', skill_id=sid, evidence=evidence)
                    elif int(line_text) > len(evidence_path.read_text(encoding='utf-8', errors='replace').splitlines()):
                        _emit(checks, 'evidence_line_out_of_range', 'error', 'Dependency evidence line is outside the current file.', skill_id=sid, evidence=evidence)
    for service_id in sorted({ref for s in selected for ref in s.get('service_refs', [])}):
        _emit(checks, 'service_not_probed', 'skipped', 'External service connectivity is intentionally not tested by this offline validator.', service_id=service_id)
    return _report(checks, root, [s['skill_id'] for s in selected])


def _report(checks: list[dict], root: Path, selected: list[str]) -> dict:
    counts = {status: sum(1 for check in checks if check['status'] == status) for status in ('pass', 'warning', 'error', 'skipped')}
    return {'schema_version': '1.0', 'root': str(root), 'selected_skills': selected,
            'checks': checks, 'summary': {'skill_count': len(selected), **counts},
            'network_checks_performed': False, 'skill_code_executed': False,
            'package_install_or_mutation_performed': False}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--skill', action='append', default=[])
    parser.add_argument('--capability', action='append', default=[])
    parser.add_argument('--profile', action='append', default=[])
    parser.add_argument('--metadata-json', type=Path, default=DEFAULT_METADATA)
    parser.add_argument('--no-metadata', action='store_true')
    parser.add_argument('--no-evidence-check', action='store_true')
    parser.add_argument('--output', type=Path, help='Write the same JSON report to this path.')
    args = parser.parse_args(argv)
    report = check_runtime(args.root, skill_ids=args.skill or None,
        capabilities=args.capability or None, profiles=args.profile or None,
        metadata_path=None if args.no_metadata else args.metadata_json,
        verify_evidence=not args.no_evidence_check)
    rendered = json.dumps(report, ensure_ascii=False, indent=2) + '\n'
    if args.output:
        args.output.write_text(rendered, encoding='utf-8')
    print(rendered, end='')
    return 1 if report['summary']['error'] else 0


if __name__ == '__main__':
    raise SystemExit(main())
