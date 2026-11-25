#!/usr/bin/env python3
"""Back up and switch only the OneCity API, Coordinator and built UI.

Runtime credentials and process environments stay in a mode-0700 directory
inside the target container. The unrelated A2A processes are left running.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import tarfile
import tempfile
from textwrap import indent
from pathlib import Path

FILES = (
    "app.py",
    "onecityagent/controller/user_controller.py",
    "onecityagent/controller/dialog_controller.py",
    "onecityagent/controller/conversation_controller.py",
    "onecityagent/service/authentication.py",
    "onecityagent/utils/http_logging.py",
    "onecityagent/service/dialog_service/access.py",
    "onecityagent/service/conversation_service/__init__.py",
    "onecityagent/service/a2a_service/coordinator_sse_acting.py",
)

# This program executes inside the container. It must not print environments,
# response bodies, credentials or full exception messages.
PROGRAM = r'''
import datetime, hashlib, json, os, pathlib, secrets, shlex, shutil, signal, subprocess, sys, time, urllib.request, urllib.error
args = json.loads(sys.argv[1])
root = pathlib.Path('/app')
state = root / '.onecity-multiuser-rollout'
state.mkdir(mode=0o700, exist_ok=True)
state.chmod(0o700)

def save(path, value):
    path.write_text(json.dumps(value))
    path.chmod(0o600)

def processes():
    found = {'api': [], 'coordinator': []}
    for item in pathlib.Path('/proc').iterdir():
        if not item.name.isdigit():
            continue
        try:
            argv = (item / 'cmdline').read_bytes().decode().split('\0')
            if not argv or 'python' not in pathlib.Path(argv[0]).name:
                continue
            cwd = (item/'cwd').resolve()
            scripts = {(pathlib.Path(a) if pathlib.Path(a).is_absolute() else cwd/a).resolve()
                       for a in argv[1:] if a and not a.startswith('-')}
            service = 'api' if root/'app.py' in scripts else ('coordinator' if root/'onecityagent/service/a2a_service/coordinator_sse_acting.py' in scripts else None)
            if service:
                env = dict(part.split('=',1) for part in (item/'environ').read_bytes().decode().split('\0') if '=' in part)
                ppid = int(next(line.split(':',1)[1] for line in (item/'status').read_text().splitlines() if line.startswith('PPid:')))
                found[service].append({'pid':int(item.name),'ppid':ppid,'argv':[a for a in argv if a], 'env':env,'cwd':str(cwd)})
        except (OSError, UnicodeError):
            continue
    return found

def stop():
    sent = set()
    for sig, grace in [(signal.SIGTERM,12),(signal.SIGKILL,3)]:
        deadline = time.monotonic()+grace
        while True:
            running = processes()
            if not any(running.values()): return
            # Stop the Werkzeug reloader before its child, including a child
            # spawned during shutdown. Never signal the container's PID 1.
            for group in running.values():
                pids = {process['pid'] for process in group}
                for process in sorted(group,key=lambda p:(p['ppid'] in pids,p['pid'])):
                    if process['pid']==1: raise RuntimeError('Entry service is container PID 1')
                    identity = (sig,process['pid'])
                    if identity in sent: continue
                    try: os.kill(process['pid'],sig)
                    except ProcessLookupError: pass
                    sent.add(identity)
            if time.monotonic()>=deadline: break
            time.sleep(.2)
    if any(processes().values()):
        raise RuntimeError('Target processes did not stop')

def launch(environments, backup, commands=None):
    commands = commands or {'api':{'argv':['/app/.venv/bin/python','app.py'],'cwd':str(root)}, 'coordinator':{'argv':['/app/.venv/bin/python','onecityagent/service/a2a_service/coordinator_sse_acting.py','--port','7010'],'cwd':str(root)}}
    for service, command in commands.items():
        env=dict(environments[service])
        env.pop('WERKZEUG_RUN_MAIN',None)
        env.pop('WERKZEUG_SERVER_FD',None)
        output = backup / (service+'-launch.log')
        stream = output.open('ab'); output.chmod(0o600)
        try:
            process = subprocess.Popen(command['argv'], cwd=command['cwd'], env=env, stdin=subprocess.DEVNULL, stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
        finally:
            stream.close()
        (root/'pids').mkdir(exist_ok=True)
        (root/'pids'/(('app' if service=='api' else 'coordinator_sse_acting')+'.pid')).write_text(str(process.pid))

def health_signature():
    statuses=[]
    for port, path in [(7606,'/v1/user/login'), (7010,'/query/deerflow/identity')]:
        try:
            response = urllib.request.urlopen('http://127.0.0.1:'+str(port)+path,timeout=2)
            status = response.status; response.close()
        except urllib.error.HTTPError as error: status=error.code
        except (OSError,urllib.error.URLError): status=None
        statuses.append(status)
    return statuses

def healthy():
    return health_signature()==[401,401]

def active_entry_connections():
    counts={'7010':0,'7606':0}
    for table in ('/proc/net/tcp','/proc/net/tcp6'):
        path=pathlib.Path(table)
        if not path.exists(): continue
        for line in path.read_text().splitlines()[1:]:
            fields=line.split()
            if len(fields)<4 or fields[3]!='01': continue
            port=str(int(fields[1].rsplit(':',1)[1],16))
            if port in counts: counts[port]+=1
    return counts

def wait_health(expected=None):
    expected=[401,401] if expected is None else expected
    for _ in range(90):
        if health_signature()==expected and all(processes().values()): return
        time.sleep(1)
    raise RuntimeError('New entry services failed their health checks')

def restore(backup):
    backup=backup.resolve()
    if backup.parent!=state.resolve() or not (backup/'manifest.json').is_file():
        raise RuntimeError('Unknown rollback backup')
    stop()
    manifest = json.loads((backup/'manifest.json').read_text())
    for target in manifest['paths']:
        source = backup/'original'/target.lstrip('/')
        live = pathlib.Path(target)
        if source.is_dir():
            # OverlayFS lower directories cannot be renamed (EXDEV). Restore
            # assets in place, then replace the entry file atomically.
            shutil.copytree(source,live,dirs_exist_ok=True)
            if (source/'index.html').is_file():
                temporary=live/('.index-'+secrets.token_hex(4))
                shutil.copy2(source/'index.html',temporary)
                temporary.replace(live/'index.html')
        elif source.exists():
            live.parent.mkdir(parents=True,exist_ok=True)
            shutil.copy2(source,live)
        elif live.exists():
            live.rename(backup/('unused-'+live.name+'-'+secrets.token_hex(4)))
    subprocess.run(['nginx','-t'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    commands_path=backup/'original-commands.json'
    commands=json.loads(commands_path.read_text()) if commands_path.exists() else None
    launch(json.loads((backup/'original-environments.json').read_text()),backup,commands)
    wait_health(manifest.get('health_signature'))
    subprocess.run(['nginx','-s','reload'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    print(json.dumps({'restored':True,'healthy':True,'backup':str(backup)}))

if args['action']=='rollback':
    restore(pathlib.Path(args['backup'])); sys.exit(0)
if args['action']=='status':
    print(json.dumps({'healthy':healthy(),'process_counts':{key:len(value) for key,value in processes().items()}})); sys.exit(0)

stamp=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+secrets.token_hex(3)
backup=state/stamp
backup.mkdir(mode=0o700)
running=processes()
if not running['api'] or len(running['coordinator'])!=1:
    raise RuntimeError('Expected one active Coordinator and an API process')
selected={}
for key,group in running.items():
    pids={process['pid'] for process in group}
    parents=[process for process in group if process['ppid'] not in pids]
    if len(parents)!=1 or any(process['pid']==1 for process in group):
        raise RuntimeError('Ambiguous entry process tree')
    selected[key]=parents[0]
original_health=health_signature()
if original_health[0] not in {200,401} or original_health[1] not in {200,401,404}:
    raise RuntimeError('Existing entry services failed baseline health')
environments={key:process['env'] for key,process in selected.items()}
save(backup/'original-environments.json',environments)
save(backup/'original-commands.json',{key:{'argv':process['argv'],'cwd':process['cwd']} for key,process in selected.items()})
paths=['/app/'+path for path in args['files']]+['/app/dist','/app/start.sh','/etc/nginx/nginx.conf','/etc/nginx/sites-enabled/default','/app/.onecity-runtime.env']
for path in paths:
    source=pathlib.Path(path)
    if source.exists():
        dest=backup/'original'/path.lstrip('/')
        dest.parent.mkdir(parents=True,exist_ok=True)
        if source.is_dir(): shutil.copytree(source,dest)
        else: shutil.copy2(source,dest)
save(backup/'manifest.json',{'paths':paths,'health_signature':original_health})
new={key:dict(value) for key,value in environments.items()}
secret=new['api'].get('SECRET_KEY') or secrets.token_urlsafe(48)
common={'SECRET_KEY':secret,'ONECITY_APP_PORT':'7606','ONECITY_DEBUG':'false','ONECITY_SELF_REGISTER_ROLE_CODE':'common',
        'DEERFLOW_API_URL':args['gateway'].rstrip('/')+'/api/deerflow/mobile/run',
        'DEERFLOW_PUBLIC_BASE_URL':args['gateway'].rstrip('/'),
        'DEERFLOW_FILE_INTERNAL_BASE':args['gateway'].rstrip('/'),
        'DEERFLOW_THREAD_ENSURE_URL':args['gateway'].rstrip('/')+'/api/deerflow/mobile/threads/ensure',
        'DEERFLOW_THREAD_UPLOAD_URL':args['gateway'].rstrip('/')+'/api/threads/{thread_id}/uploads',
        'DEERFLOW_STREAM':'true','DEERFLOW_TIMEOUT':'1800'}
for env in new.values():
    env.update(common)
    env.pop('WERKZEUG_RUN_MAIN', None)
    env.pop('WERKZEUG_SERVER_FD', None)
save(backup/'new-environments.json',new)
print(json.dumps({'backup':str(backup),'source_files':len(args['files']),'old_process_counts':{key:len(value) for key,value in running.items()}}),flush=True)
if args['action']=='prepare': sys.exit(0)

staging=pathlib.Path(args['staging'])
# Compile before stopping any live service.
for path in args['files']:
    compile((staging/path).read_text(),path,'exec')
if not (staging/'dist/index.html').is_file():
    raise RuntimeError('Staged frontend is incomplete')
startup=root/'start.sh'
if not startup.read_text().startswith('#!/bin/bash\n'):
    raise RuntimeError('Unknown startup shell format')
if not (root/'dist/index.html').is_file():
    raise RuntimeError('Existing frontend is incomplete')
prepared_dist=backup/'prepared-dist'
shutil.copytree(staging/'dist',prepared_dist)
# Do not interrupt an existing API request or SSE stream. A refusal occurs
# before the switch/rollback block, so rollback cannot accidentally stop it.
connections=active_entry_connections()
if any(connections.values()):
    print(json.dumps({'deployed':False,'reason':'active_entry_connections','connection_counts':connections,'backup':str(backup)}),flush=True)
    raise SystemExit(1)
try:
    stop()
    for path in args['files']:
        dest=root/path; dest.parent.mkdir(parents=True,exist_ok=True)
        shutil.copy2(staging/path,dest)
    # Docker OverlayFS can refuse a lower-layer directory rename. Copy static
    # assets first and replace index.html last so old clients stay consistent.
    live_dist=root/'dist'
    for item in prepared_dist.iterdir():
        if item.name=='index.html': continue
        destination=live_dist/item.name
        if item.is_dir(): shutil.copytree(item,destination,dirs_exist_ok=True)
        else: shutil.copy2(item,destination)
    temporary_index=live_dist/('.index-'+secrets.token_hex(4))
    shutil.copy2(prepared_dist/'index.html',temporary_index)
    temporary_index.replace(live_dist/'index.html')
    runtime=root/'.onecity-runtime.env'
    runtime.write_text('\n'.join('export '+key+'='+shlex.quote(value) for key,value in common.items())+'\n')
    runtime.chmod(0o600)
    content=startup.read_text()
    marker='# OneCity multi-user deployment environment'
    if marker not in content:
        content=content.replace('#!/bin/bash\n','#!/bin/bash\n'+marker+'\nif [ -f /app/.onecity-runtime.env ]; then source /app/.onecity-runtime.env; fi\n',1)
        startup.write_text(content)
    nginx=pathlib.Path('/etc/nginx/nginx.conf')
    content=nginx.read_text()
    if 'log_format onecity_safe' not in content:
        if 'http {' not in content or 'access_log /var/log/nginx/access.log;' not in content:
            raise RuntimeError('Unknown nginx access log format')
        content=content.replace('http {','http {\n        log_format onecity_safe \'$remote_addr $request_method $uri $status $body_bytes_sent $request_time\';',1)
        content=content.replace('access_log /var/log/nginx/access.log;','access_log /var/log/nginx/access.log onecity_safe;')
        nginx.write_text(content)
    # Keep internal IAM inaccessible from the public server and avoid SPA fallback.
    site=pathlib.Path('/etc/nginx/sites-enabled/default')
    content=site.read_text()
    if 'location ^~ /internal/' not in content:
        if '    location / {' not in content:
            raise RuntimeError('Unknown nginx site format')
        content=content.replace('    location / {','    location ^~ /internal/ { return 404; }\n\n    location / {',1)
        site.write_text(content)
    subprocess.run(['nginx','-t'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    launch(new,backup)
    wait_health()
    subprocess.run(['nginx','-s','reload'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    (state/'current').write_text(str(backup))
    print(json.dumps({'deployed':True,'healthy':True,'backup':str(backup),'source_sha256':{path:hashlib.sha256((root/path).read_bytes()).hexdigest() for path in args['files']}}))
except Exception as error:
    print(json.dumps({'deployed':False,'error_type':type(error).__name__,'backup':str(backup),'automatic_rollback_attempted':True,'errno':getattr(error,'errno',None),'filename':getattr(error,'filename',None)}),flush=True)
    try:
        restore(backup)
    except Exception as rollback_error:
        print(json.dumps({'restored':False,'error_type':type(rollback_error).__name__,'errno':getattr(rollback_error,'errno',None),'filename':getattr(rollback_error,'filename',None),'backup':str(backup)}),flush=True)
        raise SystemExit(2)
    raise SystemExit(1)
'''


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "deploy", "rollback", "status"))
    parser.add_argument("--source", type=Path, default=Path("/home/huangxiao/City_brain/onecityagent-integration"))
    parser.add_argument("--container", default="onecityagent-a-dev")
    parser.add_argument("--gateway", default="http://onecity-deerflow-gateway:3330")
    parser.add_argument("--backup")
    args = parser.parse_args()
    payload = {"action": args.action, "files": FILES, "gateway": args.gateway, "backup": args.backup}
    if args.action == "rollback" and not args.backup:
        parser.error("rollback requires --backup")
    if args.action == "deploy":
        if not (args.source / "web/dist/index.html").is_file():
            parser.error("successful web/dist build required")
        with tempfile.TemporaryDirectory(prefix="onecity-entry-stage-") as temporary:
            archive = Path(temporary) / "entry.tar"
            with tarfile.open(archive, "w") as bundle:
                for path in FILES:
                    bundle.add(args.source / path, arcname=path)
                bundle.add(args.source / "web/dist", arcname="dist")
            remote = "/tmp/onecity-entry-stage-" + Path(temporary).name.rsplit("-", 1)[-1]
            subprocess.run(["docker", "exec", args.container, "mkdir", "-m", "700", remote], check=True)
            subprocess.run(["docker", "cp", str(archive), args.container + ":" + remote + "/entry.tar"], check=True, capture_output=True)
            subprocess.run(["docker", "exec", args.container, "tar", "xf", remote + "/entry.tar", "-C", remote], check=True)
            payload["staging"] = remote
    container_program = "try:\n" + indent(PROGRAM, "    ") + "\nexcept Exception as error:\n    import json, sys\n    print(json.dumps({'failed': True, 'error_type': type(error).__name__}), flush=True)\n    sys.exit(1)\n"
    result = subprocess.run(["docker", "exec", "-i", args.container, "/app/.venv/bin/python", "-", json.dumps(payload)], input=container_program, text=True)
    raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
