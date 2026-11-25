#!/usr/bin/env python3
"""Opt-in production smoke using two disposable OneCity ordinary accounts.

This file does nothing when imported. Running ``prepare`` creates synthetic
accounts and leaves them enabled for a later ``check``. ``check`` and ``run``
always try to revoke their tokens and disable ONLY those accounts in finally.
``cleanup`` can abandon a prepared run. No business /run endpoint is called.

Host dependencies: Python 3.10+, Docker CLI access to the OneCity container.
The container supplies its configured models, requests and Cryptodome. Secrets
cross docker exec through stdin, never command arguments, logs or the report.
The restricted state file contains disposable passwords/tokens until cleanup.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import secrets
import stat
import subprocess
import tempfile
from urllib.parse import urlencode, urlsplit
import uuid


REPORT = Path("outputs/onecity-multiuser-integration-20261112/production-smoke.json")
MARKER = "ONECITY_SMOKE_RESULT:"
MAX_RESPONSE = 1024 * 1024

# Sent to Python's -c through stdin by a tiny non-secret bootstrap. Production
# configuration/log output remains captured, and only the marker is decoded.
WORKER = r'''
import base64, contextlib, hashlib, io, json, re, sys
from datetime import datetime
from uuid import uuid4
payload = json.loads(sys.stdin.read())
action = payload['action']
result = None
try:
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        run_id = payload.get('run_id', '')
        if not re.fullmatch(r'[a-f0-9]{32}', run_id):
            raise ValueError('invalid synthetic run id')

        def validate_account(account):
            label = account.get('label')
            if label not in ('a', 'b'):
                raise ValueError('invalid synthetic label')
            expected = 'onecity-smoke-' + run_id + '-' + label
            if account.get('email') != expected + '@smoke.invalid' or account.get('nickname') != expected:
                raise ValueError('account outside synthetic scope')

        if action == 'encrypt':
            from Cryptodome.Cipher import PKCS1_OAEP
            from Cryptodome.PublicKey import RSA
            from Cryptodome.Hash import SHA256
            # Same public key and OAEP/SHA-256 contract as web/src/utils/index.ts.
            public_key = RSA.import_key("""-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEArq9XTUSeYr2+N1h3Afl/z8Dse/2yD0ZGrKwx+EEEcdsBLca9Ynmx3nIB5obmLlSfmskLpBo0UACBmB5rEjBp2Q2f3AG3Hjd4B+gNCG6BDaawuDlgANIhGnaTLrIqWrrcm4EMzJOnAOI1fgzJRsOOUEfaS318Eq9OVO3apEyCCt0lOQK6PuksduOjVxtltDav+guVAA068NrPYmRNabVKRNLJpL8w4D44sfth5RvZ3q9t+6RTArpEtc5sh5ChzvqPOzKGMXW83C95TxmXqpbK6olN4RevSfVjEAgCydH6HN6OhtOQEcnrU97r9H0iZOWwbw3pVrZiUkuRD1R56Wzs2wIDAQAB
-----END PUBLIC KEY-----""")
            cipher = PKCS1_OAEP.new(public_key, hashAlgo=SHA256)
            result = {'ciphertext': base64.b64encode(cipher.encrypt(payload['password'].encode())).decode()}
        elif action == 'gateway':
            import requests
            path = payload['path']
            allowed = (
                path == '/api/deerflow/mobile/threads/ensure'
                or re.fullmatch(r'/api/threads/[a-fA-F0-9-]{36}/uploads/list', path)
                or re.fullmatch(r'/api/threads/[a-fA-F0-9-]{36}/artifacts/mnt/user-data/uploads/onecity-smoke\.csv', path)
            )
            if not allowed:
                raise ValueError('gateway path outside smoke scope')
            session = requests.Session()
            session.trust_env = False
            response = session.request(payload.get('method', 'GET'), payload['gateway_url'] + path,
                headers={'Authorization': 'Bearer ' + payload['token']} if payload.get('token') else {},
                json=payload.get('json'), timeout=(10, 90), allow_redirects=False)
            if len(response.content) > 1024 * 1024:
                raise ValueError('oversized response')
            result = {'status': response.status_code, 'sha256': hashlib.sha256(response.content).hexdigest(), 'size': len(response.content)}
            if '/artifacts/' not in path:
                try:
                    result['json'] = response.json()
                except ValueError:
                    result['json'] = None
        elif action in ('snapshot', 'fixture', 'cleanup'):
            from onecityagent.models.db_models import DB, User, Role, Tenant, UserTenant, Dialog
            from onecityagent.constants import UserTenantRole
            from werkzeug.security import generate_password_hash
            accounts = payload['accounts']
            for account in accounts:
                validate_account(account)
            rows = []
            with DB.connection_context():
                for account in accounts:
                    # Deliberately exclude password and access_token from SELECT.
                    columns = [User.id, User.email, User.nickname, User.role, User.status,
                        User.is_active, User.is_authenticated, User.is_anonymous, User.is_superuser]
                    candidates = list(User.select(*columns).where(User.email == account['email']).dicts())
                    if len(candidates) > 1:
                        raise ValueError('duplicate synthetic email')
                    user = candidates[0] if candidates else None
                    if user and account.get('user_id') and str(user['id']) != account['user_id']:
                        raise ValueError('synthetic account id mismatch')
                    # Email is generated before registration; nickname may later
                    # be edited by the settings check, hence ID is the durable guard.
                    if user and not account.get('user_id') and user['nickname'] != account['nickname']:
                        raise ValueError('synthetic account marker mismatch')
                    if action == 'fixture':
                        ordinary = Role.select(Role.id, Role.code).where((Role.code == 'common') & (Role.status == 1)).first()
                        if ordinary is None:
                            raise ValueError('enabled common role missing')
                        now = datetime.now()
                        if user is None:
                            uid = uuid4().hex
                            User.create(id=uid, email=account['email'], nickname=account['nickname'],
                                password=generate_password_hash(account['password']), access_token=None,
                                is_active='1', is_authenticated='1', is_anonymous='0', is_superuser=False,
                                role=ordinary.id, status='1', language='Chinese', color_schema='Bright',
                                timezone='UTC+8\tAsia/Shanghai', login_channel='password', create_date=now, update_date=now)
                            user = {'id': uid, 'email': account['email'], 'nickname': account['nickname']}
                        memberships = list(UserTenant.select(UserTenant.tenant_id).where(UserTenant.user_id == user['id']).dicts())
                        if not memberships:
                            tid = uuid4().hex
                            Tenant.create(id=tid, name=account['nickname'] + "'s Workspace", status='1',
                                llm_id='', embd_id='', asr_id='', img2txt_id='', rerank_id='', tts_id='',
                                parser_ids='', credit=0, create_date=now, update_date=now)
                            UserTenant.create(id=uuid4().hex, user_id=user['id'], tenant_id=tid,
                                role=str(UserTenantRole.OWNER.value), invited_by=user['id'], status='1', create_date=now, update_date=now)
                        rows.append({'user_id': str(user['id'])})
                    elif action == 'snapshot':
                        if user is None:
                            rows.append(None)
                            continue
                        role = Role.select(Role.code).where(Role.id == user['role']).first()
                        memberships = list(UserTenant.select(UserTenant.tenant_id).where((UserTenant.user_id == user['id']) & (UserTenant.status == '1')).dicts())
                        user['role_code'] = role.code if role else None
                        user['workspace_ids'] = [str(row['tenant_id']) for row in memberships]
                        # Compare only this synthetic user's known login token in SQL.
                        user['token_matches'] = User.select(User.id).where((User.id == user['id']) & (User.email == account['email']) & (User.access_token == account.get('token'))).exists() if account.get('token') else None
                        rows.append(user)
                    else:
                        if user is None:
                            rows.append({'found': False, 'disabled': True})
                            continue
                        guard = (User.id == user['id']) & (User.email == account['email'])
                        with DB.get_instance().atomic():
                            User.update(access_token=None, status='0', is_active='0', is_authenticated='0').where(guard).execute()
                            UserTenant.update(status='0').where(UserTenant.user_id == user['id']).execute()
                            # Only dialogs whose exact IDs were allocated by this run.
                            dialog_id = account.get('dialog_id', '')
                            dialog_ids = []
                            if re.fullmatch(r'citybrain-[a-f0-9]{21,24}[ab]', dialog_id):
                                dialog_ids.extend([dialog_id, dialog_id[:32]])
                            validation_ids = account.get('validation_dialog_ids', [])
                            if (not isinstance(validation_ids, list) or len(validation_ids) > 200
                                    or any(not isinstance(value, str) or not re.fullmatch(r'citybrain-[a-f0-9]{22}', value) for value in validation_ids)):
                                raise ValueError('invalid synthetic validation dialog ids')
                            dialog_ids.extend(validation_ids)
                            if dialog_ids:
                                Dialog.update(status=0).where(Dialog.id.in_(dialog_ids) & (Dialog.creator_id == user['id'])).execute()
                        disabled = User.select(User.id).where(guard & (User.access_token.is_null(True)) & (User.status == '0') & (User.is_active == '0') & (User.is_authenticated == '0')).exists()
                        rows.append({'found': True, 'disabled': bool(disabled)})
            result = {'accounts': rows}
        else:
            raise ValueError('unknown worker action')
    print('ONECITY_SMOKE_RESULT:' + json.dumps({'ok': True, 'result': result}, default=str))
except BaseException as exc:
    # Never echo exception strings: HTTP errors or SQL errors may contain secrets.
    print('ONECITY_SMOKE_RESULT:' + json.dumps({'ok': False, 'error_type': type(exc).__name__}))
    sys.exit(1)
'''


class SmokeFailure(RuntimeError):
    """Contains a stable check name only, never an HTTP body or credential."""


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_private(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Replace atomically so an interrupted write leaves recoverable credentials.
    fd, temporary = tempfile.mkstemp(prefix="." + path.name + "-", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w") as handle:
            json.dump(value, handle, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def read_private(path: Path) -> dict:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(fd) as handle:
        info = os.fstat(handle.fileno())
        if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077 or not stat.S_ISREG(info.st_mode):
            raise SmokeFailure("state_file_requires_owner_only_permissions")
        state = json.load(handle)
    if not re.fullmatch(r"[a-f0-9]{32}", state.get("run_id", "")) or len(state.get("accounts", [])) != 2:
        raise SmokeFailure("invalid_state_file")
    for label, account in zip(("a", "b"), state["accounts"]):
        prefix = f"onecity-smoke-{state['run_id']}-{label}"
        if account.get("label") != label or account.get("email") != prefix + "@smoke.invalid" or account.get("nickname") != prefix:
            raise SmokeFailure("state_account_outside_synthetic_scope")
    return state


def validate_targets(args) -> None:
    parsed = urlsplit(args.onecity_url)
    if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost"} or parsed.path not in {"", "/"} or parsed.username or parsed.query or parsed.fragment:
        raise SmokeFailure("onecity_url_requires_loopback_http")
    gateway = urlsplit(args.gateway_url)
    if gateway.scheme != "http" or gateway.hostname != "onecity-deerflow-gateway" or gateway.port != 3330 or gateway.path not in {"", "/"} or gateway.username or gateway.query or gateway.fragment:
        raise SmokeFailure("gateway_url_requires_private_gateway")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", args.container):
        raise SmokeFailure("invalid_container_name")


def worker(args, state: dict, action: str, **payload):
    data = {"action": action, "run_id": state["run_id"], **payload}
    # The source itself is also supplied via stdin; only a static bootstrap is
    # in the process argv. Split source and JSON at an unambiguous marker.
    bootstrap = "import sys; s=sys.stdin.read(); code,data=s.split('\\n__ONECITY_PAYLOAD__\\n',1); import io; sys.stdin=io.StringIO(data); exec(compile(code,'<onecity-production-smoke>','exec'))"
    try:
        process = subprocess.run(["docker", "exec", "-i", "-w", "/app", args.container,
            "/app/.venv/bin/python", "-c", bootstrap], input=WORKER + "\n__ONECITY_PAYLOAD__\n" + json.dumps(data),
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise SmokeFailure("docker_worker_" + type(exc).__name__) from None
    lines = [line[len(MARKER):] for line in process.stdout.splitlines() if line.startswith(MARKER)]
    if not lines:
        raise SmokeFailure("docker_worker_no_safe_result")
    try:
        parsed = json.loads(lines[-1])
    except ValueError:
        raise SmokeFailure("docker_worker_invalid_safe_result") from None
    if process.returncode or not parsed.get("ok"):
        raise SmokeFailure("docker_worker_" + str(parsed.get("error_type", "failed")))
    return parsed["result"]


def request(args, path: str, *, token=None, method="GET", data=None, body=None, content_type=None) -> dict:
    parsed = urlsplit(args.onecity_url)
    headers = {}
    if token:
        headers["Authorization"] = "Bearer " + token
    if data is not None:
        body = json.dumps(data).encode()
        content_type = "application/json"
    if content_type:
        headers["Content-Type"] = content_type
    connection = http.client.HTTPConnection(parsed.hostname, parsed.port or 80, timeout=100)
    try:
        connection.request(method, path, body=body, headers=headers)
        response = connection.getresponse()
        content = response.read(MAX_RESPONSE + 1)
        if len(content) > MAX_RESPONSE:
            raise SmokeFailure("oversized_onecity_response")
        result = {"status": response.status, "headers": dict(response.getheaders()), "body": content}
        try:
            result["json"] = json.loads(content)
        except ValueError:
            result["json"] = None
        return result
    except (OSError, http.client.HTTPException) as exc:
        raise SmokeFailure("onecity_transport_" + type(exc).__name__) from None
    finally:
        connection.close()


def require(report: dict, name: str, condition: bool, statuses=()) -> None:
    report["checks"].append({"name": name, "passed": bool(condition), **({"http_status": list(statuses)} if statuses else {})})
    if not condition:
        raise SmokeFailure(name)


def success(response: dict) -> bool:
    return response["status"] == 200 and isinstance(response.get("json"), dict) and response["json"].get("code") == 0


def snapshot(args, state):
    return worker(args, state, "snapshot", accounts=state["accounts"])["accounts"]


def prepare(args, state: dict, report: dict) -> None:
    snapshots = snapshot(args, state)
    require(report, "new_random_accounts_do_not_exist", all(row is None for row in snapshots))
    for account in state["accounts"]:
        encrypted = worker(args, state, "encrypt", password=account["password"])["ciphertext"]
        account["encrypted_password"] = encrypted
        if args.fixture_mode == "internal":
            account["provisioning"] = "synthetic_catalogue"
        else:
            registered = request(args, "/v1/user/register", method="POST", data={
                "email": account["email"], "nickname": account["nickname"], "password": encrypted,
                # Deliberately request admin: registration must still choose common.
                "role": "admin", "is_superuser": True,
            })
            account["registration_http_status"] = registered["status"]
            account["provisioning"] = "registration" if success(registered) else "synthetic_catalogue_fallback"
            write_private(args.state, state)
            if not success(registered) and args.fixture_mode == "registration":
                require(report, "real_registration_" + account["label"], False, [registered["status"]])
        if account["provisioning"] != "registration":
            worker(args, state, "fixture", accounts=[account])
        # Snapshot may locate partially registered synthetic rows; never query an
        # existing user's token or accept an account without its generated marker.
        rows = worker(args, state, "snapshot", accounts=[account])["accounts"]
        row = rows[0]
        require(report, "ordinary_account_workspace_" + account["label"], bool(row) and row["role_code"] == "common" and not row["is_superuser"] and len(row["workspace_ids"]) == 1)
        account["user_id"] = str(row["id"])
        account["workspace_id"] = row["workspace_ids"][0]
        write_private(args.state, state)
    require(report, "two_independent_workspaces", state["accounts"][0]["workspace_id"] != state["accounts"][1]["workspace_id"])
    state["phase"] = "prepared"
    report["provisioning"] = [account["provisioning"] for account in state["accounts"]]
    report["registration_http_status"] = [account.get("registration_http_status") for account in state["accounts"]]
    write_private(args.state, state)


def native(args, state, path, *, account=None, method="GET", data=None):
    return worker(args, state, "gateway", gateway_url=args.gateway_url.rstrip("/"), path=path,
        token=account.get("token") if account else None, method=method, json=data)


def upload(args, account, content: bytes, *, dialog_id=None):
    boundary = "onecitysmoke" + secrets.token_hex(16)
    body = (f"--{boundary}\r\nContent-Disposition: form-data; name=\"files\"; filename=\"onecity-smoke.csv\"\r\nContent-Type: text/csv\r\n\r\n".encode()
        + content + f"\r\n--{boundary}--\r\n".encode())
    return request(args, "/query/deerflow/upload?" + urlencode({"conversation_id": dialog_id or account["dialog_id"]}),
        token=account["token"], method="POST", body=body, content_type="multipart/form-data; boundary=" + boundary)


def check(args, state: dict, report: dict) -> None:
    require(report, "prepared_state_required", state.get("phase") == "prepared")
    report["provisioning"] = [account.get("provisioning", "unknown") for account in state["accounts"]]
    report["registration_http_status"] = [account.get("registration_http_status") for account in state["accounts"]]
    unauthenticated = request(args, "/query/deerflow/identity")
    require(report, "onecity_identity_requires_login", unauthenticated["status"] == 401, [unauthenticated["status"]])
    missing_native = native(args, state, "/api/deerflow/mobile/threads/ensure", method="POST", data={"conversation_id": state["accounts"][0]["dialog_id"]})
    require(report, "private_gateway_requires_login", missing_native["status"] == 401, [missing_native["status"]])
    for account in state["accounts"]:
        label = account["label"]
        login = request(args, "/v1/user/login", method="POST", data={"email": account["email"], "password": account["encrypted_password"]})
        token = next((value for key, value in login["headers"].items() if key.lower() == "authorization"), None)
        require(report, "real_rsa_login_" + label, success(login) and bool(token) and login["json"]["data"].get("access_token") == token, [login["status"]])
        account["token"] = token
        write_private(args.state, state)
        identity = request(args, "/query/deerflow/identity", token=token)
        identity_data = (identity.get("json") or {}).get("data") or {}
        require(report, "server_resolved_identity_" + label, success(identity) and identity_data.get("user_id") == account["user_id"] and identity_data.get("org_id") == account["workspace_id"] and identity_data.get("role_code") == "common", [identity["status"]])
        dialog = request(args, "/query/deerflow/dialog", token=token, method="POST", data={"conversation_id": account["dialog_id"], "name": "Disposable production smoke"})
        require(report, "persist_owned_dialog_" + label, success(dialog), [dialog["status"]])
        ensured = native(args, state, "/api/deerflow/mobile/threads/ensure", account=account, method="POST", data={"conversation_id": account["dialog_id"], "source": "citybrain"})
        require(report, "real_private_gateway_ensure_" + label, success(ensured), [ensured["status"]])
        thread_id = ensured["json"]["data"].get("thread_id")
        try:
            uuid.UUID(str(thread_id))
        except (ValueError, TypeError, AttributeError):
            require(report, "valid_native_thread_" + label, False)
        account["thread_id"] = thread_id
        write_private(args.state, state)
    a, b = state["accounts"]
    require(report, "separate_native_threads", a["thread_id"] != b["thread_id"])
    contents = [b"sample,value\nsynthetic-alpha,11\n", b"sample,value\nsynthetic-beta,22\n"]
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(upload, args, account, content) for account, content in zip(state["accounts"], contents)]
        uploads = [future.result() for future in futures]
    for account, content, uploaded in zip(state["accounts"], contents, uploads):
        label = account["label"]
        data = (uploaded.get("json") or {}).get("data") or {}
        require(report, "parallel_safe_csv_upload_" + label, success(uploaded) and data.get("thread_id") == account["thread_id"] and bool(data.get("files")), [uploaded["status"]])
        artifact = f"/api/threads/{account['thread_id']}/artifacts/mnt/user-data/uploads/onecity-smoke.csv"
        account["artifact_url"] = artifact
        downloaded = request(args, "/query/deerflow/file/download?" + urlencode({"conversation_id": account["dialog_id"], "url": artifact}), token=account["token"])
        require(report, "authenticated_proxy_download_" + label, downloaded["status"] == 200 and downloaded["body"] == content, [downloaded["status"]])
        direct = native(args, state, artifact, account=account)
        require(report, "authenticated_native_download_" + label, direct["status"] == 200 and direct["sha256"] == hashlib.sha256(content).hexdigest(), [direct["status"]])
    write_private(args.state, state)
    for owner, stranger in [(a, b), (b, a)]:
        suffix = owner["label"] + "_from_" + stranger["label"]
        foreign_dialog = request(args, "/query/deerflow/dialog", token=stranger["token"], method="POST", data={"conversation_id": owner["dialog_id"]})
        require(report, "foreign_dialog_hidden_" + suffix, foreign_dialog["status"] == 404, [foreign_dialog["status"]])
        foreign_upload = upload(args, stranger, b"sample,value\nsynthetic-denied,0\n", dialog_id=owner["dialog_id"])
        require(report, "foreign_upload_denied_" + suffix, foreign_upload["status"] == 404, [foreign_upload["status"]])
        for dialog in [owner["dialog_id"], stranger["dialog_id"]]:
            downloaded = request(args, "/query/deerflow/file/download?" + urlencode({"conversation_id": dialog, "url": owner["artifact_url"]}), token=stranger["token"])
            require(report, "foreign_artifact_denied_" + suffix + ("_own_dialog" if dialog == stranger["dialog_id"] else "_foreign_dialog"), downloaded["status"] == 404, [downloaded["status"]])
        foreign_ensure = native(args, state, "/api/deerflow/mobile/threads/ensure", account=stranger, method="POST", data={"conversation_id": owner["dialog_id"], "source": "citybrain"})
        require(report, "native_foreign_ensure_denied_" + suffix, foreign_ensure["status"] in {403, 404}, [foreign_ensure["status"]])
        foreign_list = native(args, state, f"/api/threads/{owner['thread_id']}/uploads/list", account=stranger)
        require(report, "native_foreign_thread_hidden_" + suffix, foreign_list["status"] == 404, [foreign_list["status"]])
        foreign_file = native(args, state, owner["artifact_url"], account=stranger)
        require(report, "native_foreign_artifact_hidden_" + suffix, foreign_file["status"] == 404, [foreign_file["status"]])
    before = snapshot(args, state)
    for account in state["accounts"]:
        changed = request(args, "/v1/user/setting", token=account["token"], method="POST", data={
            "nickname": account["nickname"] + "-edited", "role": "admin", "is_superuser": True,
            "is_active": "0", "is_authenticated": "0", "is_anonymous": "1", "status": "0",
            "access_token": "synthetic-unaccepted-" + secrets.token_hex(16),
        })
        require(report, "profile_update_" + account["label"], success(changed), [changed["status"]])
    after = snapshot(args, state)
    protected = ["id", "email", "role", "role_code", "is_superuser", "is_active", "is_authenticated", "is_anonymous", "status", "workspace_ids"]
    for account, old, new in zip(state["accounts"], before, after):
        require(report, "settings_cannot_change_authorization_" + account["label"], bool(new) and all(old[key] == new[key] for key in protected) and new["token_matches"] and new["nickname"] == account["nickname"] + "-edited")
        logged_out = request(args, "/v1/user/logout", token=account["token"])
        require(report, "real_logout_" + account["label"], success(logged_out), [logged_out["status"]])
        rejected = request(args, "/query/deerflow/identity", token=account["token"])
        revoked = native(args, state, f"/api/threads/{account['thread_id']}/uploads/list", account=account)
        require(report, "logout_revoked_on_both_platforms_" + account["label"], rejected["status"] == revoked["status"] == 401, [rejected["status"], revoked["status"]])


def cleanup(args, state: dict, report: dict) -> bool:
    try:
        cleaned = worker(args, state, "cleanup", accounts=state["accounts"])
        ok = all(row.get("disabled") for row in cleaned["accounts"])
        report["cleanup"] = {"completed": bool(ok), "synthetic_accounts_disabled": sum(bool(row.get("found") and row.get("disabled")) for row in cleaned["accounts"]), "retained": "synthetic catalogue rows (accounts, memberships and dialogs disabled) and synthetic thread/upload data"}
        if ok:
            for account in state["accounts"]:
                for key in ["password", "encrypted_password", "token"]:
                    account.pop(key, None)
            state["phase"] = "cleaned"
            write_private(args.state, state)
        return ok
    except BaseException as exc:
        report["cleanup"] = {"completed": False, "error": str(exc) if isinstance(exc, SmokeFailure) else type(exc).__name__, "retry": "Run cleanup with the same restricted state file."}
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("mode", choices=["prepare", "check", "run", "cleanup"])
    parser.add_argument("--state", type=Path, help="Owner-only state path; required for check/cleanup.")
    parser.add_argument("--onecity-url", default="http://127.0.0.1:8086")
    parser.add_argument("--gateway-url", default="http://onecity-deerflow-gateway:3330")
    parser.add_argument("--container", default="onecityagent-a-dev")
    parser.add_argument("--fixture-mode", choices=["registration", "auto", "internal"], default="registration", help="Default requires real registration. Explicit auto records any fallback to exact synthetic catalogue insertion; internal skips registration.")
    parser.add_argument("--output", type=Path, default=REPORT)
    args = parser.parse_args()
    state = None
    report = {"started_at": now(), "mode": args.mode, "status": "failed", "checks": [], "business_model_runs": 0, "uses_existing_accounts": False, "validation_scope": "production authentication, cross-platform IAM, thread creation and same-named synthetic CSV access; no model execution or scheduler capacity measurement"}
    must_cleanup = args.mode in {"check", "run", "cleanup"}
    try:
        validate_targets(args)
        if args.mode in {"prepare", "run"}:
            if args.state and args.state.exists():
                raise SmokeFailure("refusing_to_overwrite_state")
            run_id = uuid.uuid4().hex
            state = {"schema_version": 1, "run_id": run_id, "phase": "initializing", "created_at": now(), "accounts": []}
            for label in ("a", "b"):
                prefix = f"onecity-smoke-{run_id}-{label}"
                # Generated secrets never enter stdout, report or process argv.
                password = "Ts8@" + "".join(secrets.choice("abcdefghjkmnpqrstuvwxyzABCDEFGHJKMNPQRSTUVWXYZ23456789") for _ in range(12))
                state["accounts"].append({"label": label, "email": prefix + "@smoke.invalid", "nickname": prefix, "password": password, "dialog_id": "citybrain-" + run_id[:21] + label})
            if args.state is None:
                fd, temporary = tempfile.mkstemp(prefix="onecity-production-smoke-", suffix=".json")
                os.close(fd)
                args.state = Path(temporary)
            write_private(args.state, state)
        else:
            if args.state is None:
                raise SmokeFailure("state_required")
            state = read_private(args.state)
        report["run_id"] = state["run_id"]
        report["state_file"] = str(args.state)
        if args.mode in {"prepare", "run"}:
            prepare(args, state, report)
        if args.mode in {"check", "run"}:
            check(args, state, report)
        report["status"] = "prepared" if args.mode == "prepare" else "passed"
    except BaseException as exc:
        # No traceback or exception message from arbitrary libraries is emitted.
        report["error"] = str(exc) if isinstance(exc, SmokeFailure) else type(exc).__name__
        report["status"] = "failed"
        must_cleanup = state is not None
    finally:
        if must_cleanup and state is not None:
            if not cleanup(args, state, report):
                report["status"] = "failed"
        report["ended_at"] = now()
        try:
            write_private(args.output, report)
        except BaseException as exc:
            print(json.dumps({"status": "failed", "error": "report_write_" + type(exc).__name__}))
            return 1
    print(json.dumps({"status": report["status"], "checks_passed": sum(check["passed"] for check in report["checks"]), "output": str(args.output), "state_file": str(args.state) if args.state else None, "cleanup_completed": report.get("cleanup", {}).get("completed")}, ensure_ascii=False))
    return 0 if report["status"] in {"prepared", "passed"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
