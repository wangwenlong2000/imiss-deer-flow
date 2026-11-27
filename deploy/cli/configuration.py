"""Validate one site configuration and generate component files atomically."""
from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import tempfile
from urllib.parse import urlsplit

import yaml

SOURCE = Path(os.environ.get('CITYBRAIN_SOURCE', Path(__file__).resolve().parents[2])).resolve()
SECRET_FILES = ('flask.key', 'admission.key', 'mysql.password', 'mysql-root.password', 'model.key', 'admin.password', 'rsa.passphrase', 'es.password')


def _validate_schema(value, schema: dict, location: str = 'deployment') -> None:
    """Validate every keyword used by the deployment schema without side effects."""
    checks = {
        'object': lambda x: isinstance(x, dict),
        'array': lambda x: isinstance(x, list),
        'string': lambda x: isinstance(x, str),
        'integer': lambda x: isinstance(x, int) and not isinstance(x, bool),
        'boolean': lambda x: isinstance(x, bool),
    }
    kind = schema.get('type')
    if kind and not checks[kind](value):
        raise ValueError(f'Invalid type for {location}')
    if 'const' in schema and (type(value) is not type(schema['const']) or value != schema['const']):
        raise ValueError(f'Unsupported value for {location}')
    if 'enum' in schema and value not in schema['enum']:
        raise ValueError(f'Invalid value for {location}')
    if isinstance(value, str):
        if len(value) < schema.get('minLength', 0):
            raise ValueError(f'Empty value for {location}')
        if 'pattern' in schema and re.search(schema['pattern'], value) is None:
            raise ValueError(f'Invalid format for {location}')
    if isinstance(value, int) and not isinstance(value, bool):
        if value < schema.get('minimum', value) or value > schema.get('maximum', value):
            raise ValueError(f'Out-of-range value for {location}')
    if isinstance(value, dict):
        properties = schema.get('properties', {})
        if set(schema.get('required', [])) - set(value):
            raise ValueError(f'Missing required fields for {location}')
        if schema.get('additionalProperties') is False and set(value) - set(properties):
            raise ValueError(f'Unknown fields for {location}')
        for key, item in value.items():
            if key in properties:
                _validate_schema(item, properties[key], f'{location}.{key}')
    if isinstance(value, list):
        if len(value) < schema.get('minItems', 0):
            raise ValueError(f'Empty list for {location}')
        if schema.get('uniqueItems') and len({json.dumps(x, sort_keys=True) for x in value}) != len(value):
            raise ValueError(f'Duplicate items for {location}')
        for item in value:
            _validate_schema(item, schema.get('items', {}), location + '[]')


def private_write(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(fd, 'w') as stream:
        stream.write(value)


def initialize(root: Path) -> dict:
    if root.is_symlink():
        raise ValueError('Deployment root must not be a symlink')
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    for name in ('config', 'secrets', 'generated', 'data', 'data/platform', 'data/deerflow', 'data/langgraph', 'data/mysql', 'data/elasticsearch', 'backups', 'evidence', 'assets', 'assets/skills', 'assets/models', 'assets/datasets', 'assets/config'):
        (root / name).mkdir(parents=True, exist_ok=True, mode=0o700)
    for name in ('skills', 'models', 'datasets', 'config'):
        (root / 'assets' / name).chmod(0o755)
    config = root / 'config/deployment.yaml'
    if not config.exists():
        template = yaml.safe_load((SOURCE / 'deploy/config/deployment.example.yaml').read_text())
        release_manifest = SOURCE / 'manifest.json'
        if release_manifest.is_file():
            release = json.loads(release_manifest.read_text())
            if release.get('scope') != 'basic-online-api' or release.get('schema_version') != 1:
                raise ValueError('Unsupported release manifest for initialization')
            template['release'] = release['version']
            for component, image in release.get('images', {}).items():
                if component in template['images']:
                    template['images'][component] = image['reference']
        private_write(config, yaml.safe_dump(template, allow_unicode=True, sort_keys=False))
    for name in SECRET_FILES:
        path = root / 'secrets' / name
        if not path.exists():
            value = '' if name == 'model.key' else secrets.token_urlsafe(36) + ('' if name == 'rsa.passphrase' else '\n')
            private_write(path, value)
    rsa = root / 'secrets/private.pem'
    if not (root/'secrets/rsa.passphrase').exists(): private_write(root/'secrets/rsa.passphrase','\n')
    if not rsa.exists():
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import rsa as generator
        key = generator.generate_private_key(public_exponent=65537, key_size=2048)
        passphrase = (root/'secrets/rsa.passphrase').read_bytes().strip()
        encryption = serialization.BestAvailableEncryption(passphrase) if passphrase else serialization.NoEncryption()
        private_write(rsa, key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, encryption).decode())
    return {'initialized': True, 'root': str(root), 'config': str(config), 'model_key_required': True}


def read_config(root: Path) -> dict:
    config = yaml.safe_load((root / 'config/deployment.yaml').read_text())
    schema = json.loads((SOURCE / 'deploy/config/deployment.schema.json').read_text())
    _validate_schema(config, schema)
    limits = config['capacity']
    if max(limits['jobs'], limits['per_user']) > limits['max_inflight']:
        raise ValueError('jobs and per_user must not exceed max_inflight')
    addresses = [config['model']['base_url'], config['elasticsearch']['url'], config['public_url']]
    addresses.extend(v for k, v in config['services'].items() if k.endswith('_url'))
    for address in addresses:
        parsed = urlsplit(address)
        if parsed.username or parsed.password:
            raise ValueError('Credentials belong in secret files, not URLs')
        if parsed.scheme not in {'http', 'https'} or not parsed.hostname or parsed.fragment or parsed.query:
            raise ValueError('Service addresses must be absolute HTTP(S) URLs without query strings')
        try:
            parsed.port
        except ValueError:
            raise ValueError('Invalid service port') from None
    if urlsplit(config['public_url']).path not in {'', '/'}:
        raise ValueError('public_url must be an origin without a path prefix')
    if config['elasticsearch']['mode'] == 'local' and config['elasticsearch']['url'].rstrip('/') != 'http://elasticsearch:9200':
        raise ValueError('Local Elasticsearch must use http://elasticsearch:9200')
    if config['sandbox']['port_start'] + config['sandbox']['replicas'] > 65535:
        raise ValueError('Sandbox port range exceeds 65535')
    if config['mysql']['mode'] == 'local' and (config['mysql']['host'], config['mysql']['port']) != ('mysql', 3306):
        raise ValueError('Local MySQL must use mysql:3306')
    if config['bootstrap_admin']['email'].endswith('.invalid'):
        raise ValueError('Set bootstrap_admin.email before installation')
    if {'street', 'video'} & set(config['capabilities']) and not config['services'].get('streetmodel_url'):
        raise ValueError('Selected street/video capability requires services.streetmodel_url')
    if config['elasticsearch']['mode'] == 'disabled' and {'street','network','phone'} & set(config['capabilities']):
        raise ValueError('Selected capability requires Elasticsearch')
    return config


def compose_document(root: Path, generated: Path, config: dict) -> dict:
    project = config['project']; images = config['images']
    network = project + '-services'
    def mount(src, dst, readonly=True):
        return {'type': 'bind', 'source': str(src), 'target': dst, 'read_only': readonly}
    private = mount(root / 'secrets', '/run/citybrain-secrets')
    generated_mount = mount(generated, '/etc/citybrain')
    data = mount(root / 'data/deerflow', '/data/deerflow', False)
    common_env = {
        'SECRET_KEY_FILE': '/run/secrets/flask_key', 'MYSQL_PASSWORD_FILE':'/run/secrets/mysql_password', 'ONECITY_CONFIG_PATH': '/etc/citybrain/onecity.toml',
        'ONECITY_RSA_PRIVATE_KEY_PATH': '/run/secrets/rsa_private_key', 'ONECITY_RSA_PRIVATE_KEY_PASSPHRASE_FILE':'/run/secrets/rsa_passphrase', 'ONECITY_APP_PORT': '7606', 'A2A_COORDINATOR_PORT': '7010',
        'DEERFLOW_API_URL': 'http://gateway:3330/api/deerflow/mobile/run',
        'DEERFLOW_THREAD_ENSURE_URL': 'http://gateway:3330/api/deerflow/mobile/threads/ensure',
        'DEERFLOW_THREAD_UPLOAD_URL': 'http://gateway:3330/api/threads/{thread_id}/uploads',
        'DEERFLOW_FILE_INTERNAL_BASE': 'http://gateway:3330', 'DEERFLOW_PUBLIC_BASE_URL': '',
        'ONECITY_DEBUG': 'false', 'ONECITY_SELF_REGISTER_ROLE_CODE': 'common',
    }
    graph_env = {
        'DEER_FLOW_CONFIG_PATH': '/etc/citybrain/deerflow.yaml', 'DEER_FLOW_EXTENSIONS_CONFIG_PATH': '/etc/citybrain/extensions.json',
        'DEER_FLOW_EXTENSIONS_STATE_PATH': '/data/platform/extensions.json', 'DEER_FLOW_HOME': '/data/deerflow',
        'DEER_FLOW_HOST_BASE_DIR': str(root / 'data/deerflow'), 'DEER_FLOW_HOST_SKILLS_PATH': str(root / 'assets/skills'),
        'DEER_FLOW_HOST_DATASETS_PATH': str(root / 'assets/datasets'), 'DEER_FLOW_SANDBOX_HOST': 'host.docker.internal',
        'CHAT_MODEL_API_KEY_FILE': '/run/secrets/model_key', 'ES_PASSWORD_FILE':'/run/secrets/es_password', 'ES_USERNAME':'',
        'DEER_FLOW_ADMISSION_SIGNING_KEY_FILE': '/run/secrets/admission_key', 'DEER_FLOW_REQUIRE_SIGNED_ADMISSION': 'true',
        'MOBILE_IAM_PROVIDER': 'app.gateway.onecity_iam:OneCityIAMProvider', 'MOBILE_IAM_ENDPOINT': 'http://coordinator:7010/internal/deerflow/iam',
        'MOBILE_IAM_REQUIRED': 'true', 'MOBILE_ADAPTER_LANGGRAPH_URL': 'http://graph:3329', 'LANGGRAPH_URL': 'http://graph:3329',
        'DEER_FLOW_GRAPH_JOBS': str(config['capacity']['jobs']), 'DEER_FLOW_MAX_INFLIGHT_RUNS': str(config['capacity']['max_inflight']),
        'DEER_FLOW_MAX_INFLIGHT_PER_USER': str(config['capacity']['per_user']), 'DEERFLOW_RUN_EVENT_LOG_ENABLED': '0',
        'LANGSMITH_TRACING': 'false', 'LANGCHAIN_TRACING_V2': 'false',
    }
    def service(image, command=None):
        value = {'image': image, 'restart': 'unless-stopped', 'pull_policy':'never', 'networks': ['services'], 'labels':{'io.citybrain.root':str(root.resolve()), 'io.citybrain.release':config['release']}}
        if command: value['command'] = command
        return value
    services = {}
    compose_secrets = {key:{'file':str(root/'secrets'/filename)} for key,filename in {'flask_key':'flask.key','admission_key':'admission.key','mysql_password':'mysql.password','mysql_root_password':'mysql-root.password','model_key':'model.key','es_password':'es.password','rsa_private_key':'private.pem','rsa_passphrase':'rsa.passphrase','admin_password':'admin.password'}.items()}
    for name, factory, port in [('api', 'api', 7606), ('coordinator', 'coordinator', 7010)]:
        s = service(images['onecity'], ['gunicorn', f'onecityagent.entrypoints.{factory}:app', '--bind', f'0.0.0.0:{port}', '--workers', '1', '--threads', '8', '--timeout', '1800', '--access-logfile', '-', '--access-logformat', '%(m)s %(U)s %(s)s'])
        s.update(environment=dict(common_env), secrets=['flask_key','mysql_password','rsa_private_key','rsa_passphrase'], volumes=[generated_mount], healthcheck={'test': ['CMD', 'python', '-c', f'import urllib.request; urllib.request.urlopen("http://127.0.0.1:{port}/healthz",timeout=3)'], 'interval': '10s', 'timeout': '5s', 'retries': 12})
        services[name] = s
        s['depends_on'] = {'mysql':{'condition':'service_healthy'}} if config['mysql']['mode']=='local' else {}
    services['coordinator']['depends_on'] = {'api':{'condition':'service_started'}}
    asset_mounts = [mount(root / 'assets' / x, '/app/' + x) for x in ('skills', 'models', 'datasets', 'config')]
    for name, command in [('gateway', ['python','/opt/citybrain/deploy/entrypoints/run.py','/app/backend/.venv/bin/uvicorn', 'app.gateway.app:app', '--host', '0.0.0.0', '--port', '3330', '--no-access-log']), ('graph', ['python','/opt/citybrain/deploy/entrypoints/run.py','/app/backend/.venv/bin/langgraph', 'dev', '--config', '/app/backend/langgraph.json', '--no-browser', '--allow-blocking', '--no-reload', '--n-jobs-per-worker', str(config['capacity']['jobs']), '--host', '0.0.0.0', '--port', '3329'])]:
        s = service(images['deerflow'], command)
        s.update(environment=dict(graph_env), secrets=['admission_key','model_key','es_password'], volumes=[generated_mount, data, *asset_mounts, mount(root / 'data/platform', '/data/platform', name != 'gateway'), mount(root / 'data/langgraph', '/app/backend/.langgraph_api', False), mount('/var/run/docker.sock', '/var/run/docker.sock')], extra_hosts=['host.docker.internal:host-gateway'], working_dir='/app/backend')
        endpoint = 'http://127.0.0.1:3330/health' if name == 'gateway' else 'http://127.0.0.1:3329/ok'
        s['healthcheck'] = {'test': ['CMD', 'python', '-c', f'import urllib.request; urllib.request.urlopen("{endpoint}",timeout=5)'], 'interval':'10s','timeout':'8s','retries':30,'start_period':'30s'}
        nas = config['services'].get('nas_path')
        if nas:s['volumes'].append(mount(nas, '/mnt/nas/streetview_meta'))
        services[name] = s
        s['depends_on']={'coordinator':{'condition':'service_started'}}
    services['gateway']['depends_on']={'graph':{'condition':'service_started'},'coordinator':{'condition':'service_started'}}
    services['edge'] = service(images['edge'])
    services['edge'].update(ports=[f"{config['http_port']}:8080"], volumes=[mount(generated / 'nginx.conf', '/etc/nginx/conf.d/default.conf')], depends_on={'api':{'condition':'service_healthy'},'coordinator':{'condition':'service_healthy'}}, healthcheck={'test':['CMD','wget','-q','-O','/dev/null','http://127.0.0.1:8080/healthz'],'interval':'10s','timeout':'5s','retries':12})
    if config['mysql']['mode'] == 'local':
        services['mysql'] = service(images['mysql'])
        services['mysql'].update(secrets=['mysql_password','mysql_root_password'], environment={'MYSQL_DATABASE': config['mysql']['database'], 'MYSQL_USER': config['mysql']['user'], 'MYSQL_PASSWORD_FILE': '/run/secrets/mysql_password', 'MYSQL_ROOT_PASSWORD_FILE': '/run/secrets/mysql_root_password'}, volumes=[mount(root / 'data/mysql','/var/lib/mysql',False)], healthcheck={'test':['CMD-SHELL','mysqladmin ping -h 127.0.0.1 --silent'],'interval':'5s','timeout':'3s','retries':30})
    if config['elasticsearch']['mode'] == 'local':
        services['elasticsearch'] = service(images['elasticsearch'])
        services['elasticsearch'].update(environment={'discovery.type':'single-node','xpack.security.enabled':'false','ES_JAVA_OPTS':'-Xms512m -Xmx512m'},volumes=[mount(root / 'data/elasticsearch','/usr/share/elasticsearch/data',False)])
    migration_env = {key:value for key,value in common_env.items() if key != 'SECRET_KEY_FILE'}
    services['migrate'] = {'image': images['onecity'], 'profiles':['tools'], 'networks':['services'], 'environment':{**migration_env,'ADMIN_EMAIL':config['bootstrap_admin']['email'],'ADMIN_NICKNAME':config['bootstrap_admin']['nickname'],'ADMIN_PASSWORD_FILE':'/run/secrets/admin_password'}, 'volumes':[generated_mount], 'secrets':['mysql_password','rsa_private_key','rsa_passphrase','admin_password'],'command':['python','-m','deploy.migrations.onecity','migrate']}
    services['migrate-gateway'] = {'image':images['deerflow'], 'profiles':['tools'], 'networks':['services'], 'environment':graph_env,'secrets':['admission_key','model_key','es_password'],'volumes':[generated_mount,data],'command':['python','-m','deploy.migrations.gateway']}
    from deploy.cli.recovery import database_tools_service
    services['database-tools'] = database_tools_service(root, config)
    for value in services.values():
        value.setdefault('pull_policy', 'never')
        value.setdefault('labels', {'io.citybrain.root':str(root.resolve()), 'io.citybrain.release':config['release']})
    return {'name':project,'services':services,'secrets':compose_secrets,'networks':{'services':{'name':network}}}


def render(root: Path) -> dict:
    config = read_config(root)
    for name in SECRET_FILES + ('private.pem','rsa.passphrase'):
        path=root/'secrets'/name
        if not path.is_file() or path.is_symlink() or path.stat().st_mode & 0o077 or (name == 'model.key' and not path.read_text().strip()):
            raise ValueError('Missing or overly permissive secret file: ' + name)
    runtime_manifest = root/'assets/skills/runtime-manifest.json'
    material = {'config': config, 'root': str(root.resolve()), 'skill_manifest_sha256':hashlib.sha256(runtime_manifest.read_bytes()).hexdigest() if runtime_manifest.is_file() else None, 'templates': {str(p.relative_to(SOURCE)): hashlib.sha256(p.read_bytes()).hexdigest() for p in (SOURCE/'config.example.yaml', SOURCE/'deploy/config/templates/nginx.conf', SOURCE/'deploy/config/deployment.schema.json', SOURCE/'deploy/cli/configuration.py') if p.is_file()}}
    digest=hashlib.sha256(json.dumps(material,sort_keys=True).encode()).hexdigest()[:16]
    parent=root/'generated'; parent.mkdir(exist_ok=True)
    destination=parent/digest
    if not destination.exists():
        staging=Path(tempfile.mkdtemp(prefix='.generation-',dir=parent))
        try:
            model={'name':config['model']['name'],'display_name':config['model']['name'],'use':'langchain_openai:ChatOpenAI','model':config['model']['name'],'api_key':'$CHAT_MODEL_API_KEY','base_url':config['model']['base_url'],'temperature':0,'max_tokens':8192,'supports_vision':True}
            mounts=[{'host_path':str(root/'assets/datasets'),'container_path':'/mnt/datasets','read_only':True}]
            if config['services'].get('nas_path'):mounts.append({'host_path':config['services']['nas_path'],'container_path':'/mnt/nas/streetview_meta','read_only':True})
            deer={'config_version':1,'models':[model],'sandbox':{'use':'deerflow.community.aio_sandbox:AioSandboxProvider','image':config['images']['sandbox'],'port':config['sandbox']['port_start'],'replicas':config['sandbox']['replicas'],'container_prefix':config['project']+'-sandbox','network':config['project']+'-services','extra_hosts':{'host.docker.internal':'host-gateway'},'mounts':mounts,'environment':{'ES_URL':config['elasticsearch']['url'], 'STREETMODEL_BASE_URL':config['services'].get('streetmodel_url','')}},'skills':{'path':'/app/skills','container_path':'/mnt/skills'},'checkpointer':{'type':'sqlite','connection_string':'/data/deerflow/checkpoints.db'},'memory':{'enabled':False},'channels':{},'skill_router':{'enabled':False},'compliance':{'enabled':True,'strict_startup':True,'scene_resolver_mode':'trusted_upstream','detectors_config_path':'/app/config/compliance/detectors.yaml','policy_matrix_path':'/app/config/compliance/policy_matrix.yaml','audit':{'enabled':True,'path':'/data/deerflow/compliance/audit','retain_days':90}}}
            # Preserve reviewed default tool/middleware/gate declarations from the canonical template.
            canonical=SOURCE/'config.example.yaml'
            if canonical.exists():
                defaults=yaml.safe_load(canonical.read_text())
                for k in ['tools','tool_groups','agents','subagents','summarization','title']:
                    if k in defaults:deer[k]=defaults[k]
                if 'compliance' in defaults:deer['compliance']={**defaults['compliance'],**deer['compliance']}
            (staging/'deerflow.yaml').write_text(yaml.safe_dump(deer,allow_unicode=True,sort_keys=False))
            db=config['mysql']
            q=lambda v:json.dumps(v,ensure_ascii=False)
            toml='[agent.database]\n'+''.join(k+' = '+q(v)+'\n' for k,v in {'DATABASE_TYPE':'mysql','DATABASE_HOST':db['host'],'DATABASE_PORT':db['port'],'DATABASE_NAME':db['database'],'DATABASE_USER':db['user'],'DATABASE_PASSWORD':'$MYSQL_PASSWORD','DATABASE_MAX_CONNECTIONS':20,'DATABASE_TIMEOUT':30}.items())
            toml+='\n[a2a]\nrouter = "http://coordinator:7010"\n\n[security]\nPRIVATE_KEY_PATH = "/run/citybrain-secrets/private.pem"\nPRIVATE_KEY = ""\n'
            toml=toml.replace('/run/citybrain-secrets/private.pem','/run/secrets/rsa_private_key')
            (staging/'onecity.toml').write_text(toml)
            extensions = {'mcpServers':{}, 'skills':{}}
            manifest = root/'assets/skills/runtime-manifest.json'
            if manifest.is_file():
                declarations = json.loads(manifest.read_text())
                enabled = set(config['capabilities'])
                for skill in declarations.get('skills', []):
                    extensions['skills'][skill['skill_id']] = {'enabled': bool(enabled.intersection(skill.get('capability_groups', [])))}
            (staging/'extensions.json').write_text(json.dumps(extensions,ensure_ascii=False,sort_keys=True)+'\n')
            (staging/'generation.json').write_text(json.dumps({'schema_version':1,'generation':digest,'root':str(root.resolve()),'release':config['release']},sort_keys=True)+'\n')
            (staging/'nginx.conf').write_text((SOURCE/'deploy/config/templates/nginx.conf').read_text())
            (staging/'compose.yaml').write_text(yaml.safe_dump(compose_document(root,destination,config),sort_keys=False))
            for p in staging.iterdir():p.chmod(0o600)
            staging.rename(destination)
        finally:
            if staging.exists():shutil.rmtree(staging)
    temporary=parent/'.current-new'
    if temporary.exists() or temporary.is_symlink():temporary.unlink()
    temporary.symlink_to(destination.name); temporary.replace(parent/'current')
    return {'rendered':True,'generation':digest,'compose':str(destination/'compose.yaml')}
