#!/usr/bin/env python3
"""Build deterministic Skill declarations from the reviewed dependency inventory.

No Skill, model, external service, or package installer is executed. Runtime
observations remain historical evidence; they never become version constraints.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import re
import stat
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CATEGORIES = {
    'network-traffic-analysis': ('network', 'network'),
    'phone-network-analysis': ('phone', 'base'),
    'policies-regulations': ('legal', 'base'),
    'program_snippet': ('code', 'code'),
    'remote_sensing_image': ('remote_sensing', 'geo'),
    'road-traffic-analysis': ('traffic', 'base'),
    'spatiotemporal_trajectory': ('trajectory', 'geo'),
    'street_view_image': ('street', 'base'),
    'video_surveillance': ('video', 'video'),
    'public': ('public', 'base'),
}
DEPENDENCIES = [('python_dependencies', 'python'), ('node_dependencies', 'node'),
                ('system_tools', 'tool'), ('models_services', 'service')]


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def requirement_level(text: str) -> str:
    text = text.lower()
    # Conditional wording must win over the word "required" in branch labels.
    if any(x in text for x in ['可选', 'optional', '二选一', 'conditional', '条件', '按功能', '按语言', '时必需', '安装']):
        return 'optional' if ('optional' in text or '可选' in text) else 'conditional'
    if text.startswith('required') or text.startswith('必需') or '共享运行时必需' in text:
        return 'required'
    return 'conditional'


def pin_state(specifier: str) -> str:
    normalized = str(specifier).strip()
    if re.search(r'(^|[,;\s])==\s*[^=\s]', normalized):
        return 'exact_pin_declared'
    if any(op in normalized for op in ('>=', '<=', '~=', '!=', '^', '~', '>')):
        return 'range_declared'
    return 'not_pinned'


def service_id(name: str) -> str:
    lower = name.lower()
    for words, logical in [
        (['elasticsearch'], 'elasticsearch'), (['photon'], 'geocoding'),
        (['nas'], 'street_originals'), (['streetmodel'], 'streetmodel'),
        (['dashscope embedding'], 'policy_embedding'), (['feed', '情报'], 'threat_feeds'),
        (['tts', '火山'], 'tts'), (['veo'], 'video_generation'),
        (['gemini'], 'image_generation'), (['gpt-vis'], 'chart_rendering'),
        (['vercel'], 'vercel_deploy'), (['百度地图'], 'baidu_maps'),
        (['微信', '搜狗'], 'wechat_web'), (['github'], 'github'),
        (['skills.sh'], 'skill_registry'), (['yolo'], 'yolo_weights'),
        (['embedding', 'sentence', 'bge'], 'text_embedding'),
        (['视觉', '审帧', '图像理解'], 'vision_model'),
        (['llm', '聊天', '智能体模型'], 'chat_model'),
        (['docx', '导出'], 'document_export'),
        (['web_search', 'webfetch', 'web_fetch'], 'web_tools'),
        (['gateway', 'langgraph'], 'deerflow_api'),
    ]:
        if any(word in lower for word in words):
            return logical
    return 'external_' + hashlib.sha256(name.encode()).hexdigest()[:12]


def source_files(root: Path, skill: Path) -> list[Path]:
    result = []
    for path in skill.rglob('*'):
        if not path.is_file() or path.suffix not in {'.py', '.js', '.sh'}:
            continue
        if any(part in {'node_modules', 'rules', 'vendor', '.git', '__pycache__', 'tests', 'evals', 'assets'} for part in path.relative_to(skill).parts):
            continue
        if path.name.startswith('test_'):
            continue
        # Do not capture another nested Skill's implementation.
        if any((parent / 'SKILL.md').exists() for parent in path.parents if parent != skill and skill in parent.parents):
            continue
        result.append(path)
    return sorted(result)


def known_assets(root: Path, source: dict, skill: Path) -> list[dict]:
    """Return only assets identified by the audit or explicit adjacent config."""
    name = source['skill_name']
    candidates: list[tuple[str, str, str]] = []
    if name == 'opengrep-compliance':
        candidates.extend([
            ('opengrep-linux', str(skill.relative_to(root) / 'vendor/opengrep/linux/opengrep'), 'native_binary'),
            ('opengrep-rules', str(skill.relative_to(root) / 'rules'), 'rule_bundle'),
        ])
    if name in {'object-detection', 'video-object-analytics'}:
        candidates.append(('yolov8n', 'skills/custom/models/yolov8n.pt', 'model_weight'))
    if name in {'china-legal-consultation', 'china-legal-issue-analysis'}:
        candidates.append(('case-types-db', str(skill.relative_to(root) / 'references/data/case_types.db'), 'sqlite_database'))
    result = []
    for asset_id, relative, kind in candidates:
        path = root / relative
        record = {'asset_id': asset_id, 'path': relative, 'kind': kind,
                  'source_checkout_state': 'present' if path.exists() else 'missing',
                  'sha256': None, 'size_bytes': None, 'executable': None,
                  'scope': 'source_checkout_only'}
        if path.is_file():
            record['sha256'] = digest(path)
            record['size_bytes'] = path.stat().st_size
            if kind == 'native_binary':
                record['executable'] = bool(path.stat().st_mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH))
        elif path.is_dir():
            record['tree_sha256'] = tree_digest(path, root)
        result.append(record)
    # StreetModel weights are downloaded from the pinned revisions in its manifest,
    # but the heavyweight files are not part of this source checkout.
    if name == 'location-matcher':
        manifest = root / 'services/streetmodel/models.json'
        if manifest.is_file():
            result.append({'asset_id': 'streetmodel-model-revisions',
                'path': str(manifest.relative_to(root)), 'kind': 'model_revision_manifest',
                'source_checkout_state': 'present', 'sha256': digest(manifest),
                'size_bytes': manifest.stat().st_size, 'executable': None,
                'provisioning': 'external_model_service',
                'scope': 'manifest_only_weights_not_in_source_checkout'})
    return result


def tree_digest(directory: Path, root: Path) -> str:
    hasher = hashlib.sha256()
    for path in sorted((p for p in directory.rglob('*') if p.is_file()), key=lambda p: p.as_posix()):
        hasher.update(str(path.relative_to(root)).encode())
        hasher.update(b'\0')
        hasher.update(bytes.fromhex(digest(path)))
    return hasher.hexdigest()


def independent_assets(root: Path) -> list[dict]:
    assets = []
    for asset_id, path_text, kind, owner in [
        ('compliance-tfidf-knn', 'models/compliance/ml_detector_0624_fresh.json', 'model_asset', 'compliance-runtime'),
        ('streetmodel-model-revisions', 'services/streetmodel/models.json', 'model_revision_manifest', 'streetmodel-service'),
        ('yolov8n-shared-weight', 'skills/custom/models/yolov8n.pt', 'model_weight', 'video_surveillance'),
    ]:
        path = root / path_text
        record = {'asset_id': asset_id, 'path': path_text, 'kind': kind,
            'source_checkout_state': 'present' if path.exists() else 'missing',
            'sha256': digest(path) if path.is_file() else None,
            'size_bytes': path.stat().st_size if path.is_file() else None,
            'executable': None, 'scope': 'source_checkout_only',
            'required_by_skills': [], 'owner': owner}
        assets.append(record)
    return assets


def entrypoints(files: list[Path], root: Path) -> list[dict]:
    entries = []
    for path in files:
        text = path.read_text(encoding='utf-8')
        executable = False
        if path.suffix == '.py':
            try:
                tree = ast.parse(text)
            except SyntaxError:
                continue
            executable = any(isinstance(node, ast.If) and isinstance(node.test, ast.Compare)
                and isinstance(node.test.left, ast.Name) and node.test.left.id == '__name__'
                and any(isinstance(v, ast.Constant) and v.value == '__main__' for v in node.test.comparators)
                for node in tree.body)
        elif path.suffix == '.js':
            executable = 'scripts' in path.parts
        elif path.suffix == '.sh':
            executable = text.startswith('#!')
        if executable:
            entries.append({'id': str(path.relative_to(root)).replace('/', ':'),
                'kind': 'cli', 'path': str(path.relative_to(root)),
                'interpreter': {'.py': 'python3', '.js': 'node', '.sh': 'bash'}[path.suffix],
                'arguments': 'See SKILL.md; this declaration does not authorize automatic execution.',
                'automatic_execution': False})
    return entries


def build(root: Path) -> dict:
    audit_path = root / 'deploy/assets/skill-dependency-audit.json'
    audit = json.loads(audit_path.read_text())
    skills = []
    services = {}
    assets_by_path: dict[str, dict] = {}
    for source in sorted(audit['skills'], key=lambda item: item['relative_path']):
        path = root / source['relative_path']
        card_path = path / 'router_card.json'
        card = json.loads(card_path.read_text()) if card_path.is_file() else {}
        scope = card.get('scope', {})
        capability, profile = CATEGORIES[source['category']]
        if source['skill_name'].startswith('wechat-article-'):
            profile = 'browser' if source['skill_name'].endswith('fetch') else 'base'
        files = source_files(root, path)
        entries = entrypoints(files, root)
        declarations = []
        service_refs = []
        for key, kind in DEPENDENCIES:
            for dependency in source.get(key, []):
                condition = dependency.get('required_or_optional', 'unspecified')
                name = dependency.get('distribution') or dependency['name']
                item = {'kind': kind, 'name': name, 'level': requirement_level(condition),
                    'condition': condition, 'specifier': dependency.get('specifier', '未固定'),
                    'runtime_pin_state': pin_state(dependency.get('specifier', '未固定')),
                    'module': dependency.get('module', ''),
                    'evidence': dependency.get('evidence', []),
                    'notes': dependency.get('notes', ''),
                    'runtime_observation': {
                        'scope': dependency.get('runtime_scope', 'Not measured in the reviewed inventory'),
                        'version': dependency.get('observed_version'),
                        'status': dependency.get('status', 'Not verified'),
                    }}
                if kind == 'service':
                    logical = service_id(name)
                    item['service_id'] = logical
                    service_refs.append(logical)
                    services.setdefault(logical, []).append(name)
                declarations.append(item)
        name = source['skill_name']
        execution_type = 'script' if entries else 'prompt'
        if name in {'pipeline-graph-builder', 'pipeline-reasoner'}:
            execution_type = 'python_api'
            module = 'graph_builder' if name == 'pipeline-graph-builder' else 'reasoner'
            entries.append({'id': 'python_api', 'kind': 'python_api',
                'path': f'skills/custom/program_snippet/deerflow-pipeline-runtime/deerflow/pipeline/{module}.py',
                'interpreter': 'python3', 'arguments': 'See SKILL.md for the Python API.', 'automatic_execution': False})
        if name == 'trademark-assistant':
            execution_type = 'prompt_with_code'
        if name in {'city-video-intelligence', 'code-business-dag-analysis-pipeline'}:
            execution_type = 'composite'
        if name in {'single-video-event-analysis', 'opengrep-compliance'}:
            execution_type = 'hybrid'
        if profile == 'base' and execution_type == 'prompt' and not declarations:
            capabilities = [capability, 'basic']
        else:
            capabilities = [capability]
        hashes = {str(path.relative_to(root) / 'SKILL.md'): digest(path / 'SKILL.md')}
        if card_path.exists():
            hashes[str(card_path.relative_to(root))] = digest(card_path)
        for file in files:
            hashes[str(file.relative_to(root))] = digest(file)
        fallback_notes = [note for note in source.get('notes', []) if any(word in note.lower() for word in ['降级', 'fallback', 'mock', '回退', '模拟'])]
        # These corrections apply to the current checkout; audit notes remain history.
        if name == 'opengrep-compliance':
            fallback_notes.append('No scanning fallback: missing/non-executable binary or missing rules must fail. Runtime declaration checks the local asset, not PATH.')
        if name == 'code-business-dag-analysis-pipeline':
            fallback_notes.append('Existing splitter exception path uses one full-source chunk; this is a recorded legacy fallback, not complete code splitting. Default labels/reasoner are local rules.')
        assets = known_assets(root, source, path)
        for asset in assets:
            asset['required_by_skills'] = [name]
            prior = assets_by_path.get(asset['path'])
            if prior is None:
                assets_by_path[asset['path']] = dict(asset)
            else:
                prior['required_by_skills'].append(name)
        execution = card.get('execution', {})
        scenes = scope.get('scenes', [])
        scenario = scenes[0] if scenes else capability
        example_input = {'schema_version': '1.0', 'request_id': '<generated-request-id>',
            'skill_name': name, 'scenario': scenario, 'capability': '<select-concrete-action>',
            'query': {'raw': '<original-user-request>'},
            'inputs': {'files': [], 'data_sources': [], 'parameters': {}}}
        skills.append({'skill_id': name, 'path': source['relative_path'],
            'execution_type': execution_type, 'runtime_profile': profile,
            'runtime_profile_assignment': {'status': 'proposed', 'basis': 'reviewed skill category and dependency inventory',
                'meaning': 'Deployment/runtime grouping proposal; not a measured requirement from the Skill itself.'},
            'capability_groups': capabilities, 'actions': entries,
            'requirements': declarations, 'service_refs': sorted(set(service_refs)),
            'assets': assets,
            'router_execution_contract': {'source': str(card_path.relative_to(root)) if card else None,
                'declared': bool(execution), 'required_tools': execution.get('required_tools', []),
                'optional_tools': execution.get('optional_tools', []),
                'allowed_file_patterns': execution.get('allowed_file_patterns', []),
                'can_run_standalone': execution.get('can_run_standalone'),
                'can_compose_with': execution.get('can_compose_with', [])},
            'input_contract': {'schema': 'skills/skill_input_envelope.schema.json',
                'input_types': scope.get('input_types', []), 'allowed_file_patterns': execution.get('allowed_file_patterns', []),
                'example_template': example_input, 'example_status': 'template_with_placeholders_not_executable_input'},
            'output_contract': {'schema': 'skills/skill_result.schema.json',
                'output_types': scope.get('output_types', []),
                'status_values': ['success', 'partial_success', 'failed']},
            'scenes': scenes,
            'fallbacks': {'known': fallback_notes,
                'unknown_behavior': 'No fallback guarantee is inferred when source inventory does not document one.'},
            'source_hashes': hashes,
            'validation': {'static_inventory': True, 'business_acceptance': 'not_run',
                'reason': 'Source declaration and historical metadata do not prove functional execution.'}})
    service_catalog = [{'service_id': service_id,
            'names_from_inventory': sorted(set(names)),
            'consumers': sorted(s['skill_id'] for s in skills if service_id in s['service_refs']),
            'availability': 'inventory_only_not_probed',
            'endpoint_or_credentials': 'deployment_configuration_reference_not_embedded'}
        for service_id, names in sorted(services.items())]
    for entry in audit.get('independent_services', []):
        name = entry.get('service_name') or entry.get('name') or entry.get('relative_path')
        logical = service_id(str(name))
        if not any(item['service_id'] == logical for item in service_catalog):
            service_catalog.append({'service_id': logical, 'names_from_inventory': [str(name)],
                'consumers': [], 'availability': 'inventory_only_not_probed',
                'endpoint_or_credentials': 'deployment_configuration_reference_not_embedded',
                'evidence_path': entry.get('relative_path')})
    service_catalog.sort(key=lambda item: item['service_id'])
    for asset in independent_assets(root):
        assets_by_path.setdefault(asset['path'], asset)
    manifest = {'schema_version': '1.0',
        'source_audit': {'path': str(audit_path.relative_to(root)), 'sha256': digest(audit_path),
            'date': audit['audit_date'], 'dependency_rows': sum(len(s.get(key, [])) for s in audit['skills'] for key, _ in DEPENDENCIES),
            'evidence_scope': audit.get('evidence_scope', audit.get('method', []))},
        'runtime_profiles': {'catalog_status': 'proposed',
            'profiles': ['base', 'code', 'network', 'geo', 'video', 'browser', 'compatibility'],
            'capability_mapping': 'Category-based initial grouping for installation planning; requires operator selection and does not auto-enable a capability.'},
        'service_catalog': service_catalog,
        'asset_catalog': [assets_by_path[key] for key in sorted(assets_by_path)],
        'skills': skills}
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    root = args.root.resolve()
    target = root / 'skills/runtime-manifest.json'
    manifest = build(root)
    result = json.dumps(manifest, ensure_ascii=False, indent=2) + '\n'
    catalog = {'schema_version': '1.0', 'source_audit': manifest['source_audit'],
        'runtime_profiles': manifest['runtime_profiles'],
        'capabilities': {group: sorted(s['skill_id'] for s in manifest['skills'] if group in s['capability_groups'])
            for group in sorted({group for s in manifest['skills'] for group in s['capability_groups']})},
        'services': manifest['service_catalog'], 'assets': manifest['asset_catalog'],
        'business_acceptance': 'not_run; this catalog is a static source/metadata inventory'}
    catalog_result = json.dumps(catalog, ensure_ascii=False, indent=2) + '\n'
    doc_lines = ['# Skill 能力与运行档目录', '',
        '此目录由 `scripts/build_skill_runtime_manifest.py` 从 Skill、Router Card 和 2026-11-26 依赖盘点生成。运行档分配为部署规划建议；它不是自动执行授权或业务验收结果。', '',
        f"盘点包含 {len(manifest['skills'])} 个 Skill、{manifest['source_audit']['dependency_rows']} 条依赖声明。依赖镜像元数据只表示包/命令存在记录，不证明导入、服务连接或任务成功。", '',
        '## 运行档（拟议）', '',
        '| 运行档 | 初始用途 | 边界 |', '|---|---|---|',
        '| `base` | 提示、通用基础处理、法规/业务文本流程 | 外部模型与工具仍按各 Skill 声明配置 |',
        '| `code` | 代码解析、切分、DAG及静态扫描 | Java/C++解析器、切分器和OpenGrep资产须按Skill核实 |',
        '| `network` | 网络流量和PCAP处理 | 依赖tshark、数据格式及RAG服务的能力需分别验收 |',
        '| `geo` | 遥感、轨迹及地理处理 | 可能需要GDAL原生库、大型数据/模型；不可仅凭包清单判定 |',
        '| `video` | 视频处理、索引和视觉服务调用 | ffmpeg、GPU模型、ES和原视频资产按场景配置 |',
        '| `browser` | 网页/微信公众号抓取 | 需要Node、Playwright及浏览器资产；会访问外部站点 |',
        '| `compatibility` | 旧脚本兼容运行档 | 作为过渡选项，后续按实测拆分 |', '',
        '## Skill 按能力分组', '']
    for group, names in catalog['capabilities'].items():
        doc_lines += [f'### `{group}`', '', ', '.join(f'`{name}`' for name in names) or '无', '']
    doc_lines += ['## 判定口径', '',
        '- 清单中的源代码入口仅表示静态发现的调用候选，不代表获准自动执行。',
        '- `required`、`conditional`、`optional`沿用依赖盘点和Router Card条件；条件分支的实际触发由对应动作决定。',
        '- 模型版本、包锁定版本、外部服务在线状态、索引完整性、数据质量和业务准确率需分别验收。',
        '- 本清单不内嵌凭据、生产端点或用户数据；外部服务探测需要由部署环境显式执行。',
        '- 未在源码或盘点中确认的降级行为标记为未知，不推断静默回退。', '']
    capabilities_result = '\n'.join(doc_lines)
    catalog_path = root / 'deploy/assets/catalog.json'
    capabilities_path = root / 'deploy/assets/capabilities.md'
    if args.check:
        expected = [(target, result), (catalog_path, catalog_result), (capabilities_path, capabilities_result)]
        if any(not path.exists() or path.read_text() != expected_text for path, expected_text in expected):
            print('Skill runtime outputs are stale; run scripts/build_skill_runtime_manifest.py')
            return 1
    else:
        catalog_path.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(result)
        catalog_path.write_text(catalog_result)
        capabilities_path.write_text(capabilities_result)
    print('Skill runtime manifest, asset/service catalog, and capability document are current.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
