#!/usr/bin/env python3
"""Create four new ordinary accounts through OneCity's public registration API.

Passwords stay in an owner-only JSON file outside the repository. Login tokens
exist only in memory and are revoked after verification. Accounts and their new
workspaces remain enabled for a later browser recording. No direct database write,
existing credential lookup, model request or deployment change is performed.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import secrets
import string
import stat
import tempfile
import uuid
from urllib.parse import urlencode

import verify_onecity_production as production


def browser_password() -> str:
    """Match the deployed login form's 8–16 character password validator."""
    groups = [string.ascii_lowercase, string.ascii_uppercase, string.digits, "@#$"]
    alphabet = "".join(groups)
    characters = [secrets.choice(group) for group in groups]
    characters.extend(secrets.choice(alphabet) for _ in range(12))
    secrets.SystemRandom().shuffle(characters)
    return "".join(characters)


def require(condition: bool, name: str) -> None:
    if not condition:
        raise production.SmokeFailure(name)


def prepare_materials(args, credential_path: Path) -> int:
    descriptor = os.open(credential_path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(descriptor) as handle:
        info = os.fstat(handle.fileno())
        require(info.st_uid == os.getuid() and stat.S_ISREG(info.st_mode) and not stat.S_IMODE(info.st_mode) & 0o077, "owner_only_credentials_required")
        state = json.load(handle)
    require(state.get("login_url") == args.onecity_url.rstrip("/") + "/login" and re.fullmatch(r"[a-f0-9]{32}", state.get("run_id", "")), "recording_target_mismatch")
    require(len(state.get("accounts", [])) == 4 and state.get("phase") == "ready_for_recording", "four_prepared_accounts_required")
    for label, account in zip("ABCD", state["accounts"]):
        require(account.get("label") == label and re.fullmatch(r"recording_20261112_[a-f0-9]{8}_" + label.lower(), account.get("username", "")), "recording_account_scope")
        require(account.get("email") == account["username"] + "@recording.invalid" and account.get("role_code") == "common", "recording_account_marker")

    tokens, current_stage, current_label = {}, "login", None
    try:
        for index, account in enumerate(state["accounts"], 1):
            current_label, current_stage = account["label"], "login"
            encrypted = production.worker(args, state, "encrypt", password=account["password"])["ciphertext"]
            response = production.request(args, "/v1/user/login", method="POST", data={"email": account["email"], "password": encrypted})
            require(production.success(response), "recording_login_failed")
            token = next((value for key, value in response["headers"].items() if key.lower() == "authorization"), None)
            require(bool(token) and response["json"]["data"]["id"] == account["user_id"], "recording_login_owner_mismatch")
            tokens[account["label"]] = token

            current_stage = "create_owned_dialog"
            dialog_id = "citybrain-" + state["run_id"][:20] + account["label"].lower()
            require(len(dialog_id) <= 32, "dialog_id_length")
            created = production.request(args, "/query/deerflow/dialog", token=token, method="POST", data={"conversation_id": dialog_id, "name": "权限验证资料（预置）"})
            require(production.success(created), "recording_dialog_failed")
            account["dialog_id"] = dialog_id
            production.write_private(credential_path, state)

            current_stage = "upload_synthetic_csv"
            content = f"sample,value\nrow_1,{index}\nrow_2,{index * 10}\n".encode()
            boundary = "onecityrecording" + secrets.token_hex(16)
            body = (f'--{boundary}\r\nContent-Disposition: form-data; name="files"; filename="业务数据.csv"\r\nContent-Type: text/csv\r\n\r\n'.encode() + content + f"\r\n--{boundary}--\r\n".encode())
            uploaded = production.request(args, "/query/deerflow/upload?" + urlencode({"conversation_id": dialog_id}), token=token, method="POST", body=body, content_type="multipart/form-data; boundary=" + boundary)
            require(production.success(uploaded), "recording_upload_failed")
            data = uploaded["json"]["data"]
            thread_id = data.get("thread_id")
            require(str(uuid.UUID(str(thread_id))) == thread_id and bool(data.get("files")), "recording_upload_thread_required")
            artifact_path = f"/api/threads/{thread_id}/artifacts/mnt/user-data/uploads/业务数据.csv"
            download_path = "/query/deerflow/file/download?" + urlencode({"conversation_id": dialog_id, "url": artifact_path})
            downloaded = production.request(args, download_path, token=token)
            require(downloaded["status"] == 200 and downloaded["body"] == content, "recording_owner_download_failed")
            account.update({"thread_id": thread_id, "filename": "业务数据.csv", "expected_total": index * 11, "artifact_path": artifact_path, "download_path": download_path, "download_url": args.onecity_url.rstrip("/") + download_path, "owner_download_status": downloaded["status"]})
            production.write_private(credential_path, state)

        current_stage, current_label = "cross_user_download_denial", "B"
        denied = production.request(args, state["accounts"][0]["download_path"], token=tokens["B"])
        require(denied["status"] == 404, "recording_cross_user_download_not_denied")
        state["materials_ready"] = True
        state["download_verification"] = {"a_owner": 200, "b_accessing_a": denied["status"]}
        production.write_private(credential_path, state)
        public_fields = ["label", "username", "email", "dialog_id", "thread_id", "filename", "expected_total", "download_path", "download_url", "owner_download_status"]
        manifest = {"prepared_at": datetime.now(timezone.utc).isoformat(), "login_url": state["login_url"], "platform_url": state["platform_url"], "credential_file": str(credential_path), "accounts_remain_enabled": True, "independent_workspaces": True, "contains_credentials": False, "calls_models": False, "accounts": [{key: account[key] for key in public_fields} for account in state["accounts"]], "permission_check": {"a_owner": 200, "b_accessing_a": denied["status"]}, "authenticated_fetch_required": True}
        production.write_private(args.manifest, manifest)
        print(json.dumps({"materials_ready": True, "manifest": str(args.manifest.resolve()), "credential_file": str(credential_path), "owner_download_status": 200, "foreign_download_status": denied["status"], "accounts_remain_enabled": True}))
        return 0
    except BaseException as error:
        print(json.dumps({"materials_ready": False, "credential_file": str(credential_path), "failed_stage": current_stage, "failed_label": current_label, "error_type": type(error).__name__}))
        return 1
    finally:
        # Revoke only the tokens created above. The accounts stay enabled and
        # their passwords remain available for a fresh browser login.
        for label, token in tokens.items():
            try:
                logged_out = production.request(args, "/v1/user/logout", token=token)
                require(production.success(logged_out), "recording_setup_logout_failed")
            except Exception as error:
                print(json.dumps({"verification_token_revoked": False, "label": label, "error_type": type(error).__name__}))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--onecity-url", default="http://127.0.0.1:8086")
    parser.add_argument("--container", default="onecityagent-a-dev")
    parser.add_argument("--materials-for", type=Path, help="Prepare only the four accounts in this owner-only credential file.")
    parser.add_argument("--manifest", type=Path, default=Path("outputs/recording-handoff-20261112/setup.json"))
    args = parser.parse_args()
    args.gateway_url = "http://onecity-deerflow-gateway:3330"
    production.validate_targets(args)
    if args.materials_for:
        return prepare_materials(args, args.materials_for)

    private_directory = Path(tempfile.mkdtemp(prefix=".onecity-recording-20261112-", dir=Path.home()))
    private_directory.chmod(0o700)
    credential_path = private_directory / "accounts.json"
    suffix = secrets.token_hex(4)
    state = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "run_id": uuid.uuid4().hex,
        "login_url": args.onecity_url.rstrip("/") + "/login",
        "platform_url": args.onecity_url.rstrip("/") + "/city-brain",
        "registration_api": "/v1/user/register",
        "accounts_remain_enabled": True,
        "accounts": [],
    }
    for label in "ABCD":
        username = f"recording_20261112_{suffix}_{label.lower()}"
        email = username + "@recording.invalid"
        password = browser_password()
        require(len(email) <= 50 and bool(re.fullmatch(r"[A-Za-z0-9_]+@[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)+", email)), "browser_email_format")
        require(bool(re.fullmatch(r"(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@#$])[A-Za-z\d@#$]{8,16}", password)), "browser_password_format")
        state["accounts"].append({"label": label, "username": username, "email": email, "nickname": username, "password": password, "phase": "generated"})
    production.write_private(credential_path, state)

    current_label, current_stage = None, "initialization"
    try:
        for account in state["accounts"]:
            current_label = account["label"]
            current_stage = "encryption"
            ciphertext = production.worker(args, state, "encrypt", password=account["password"])["ciphertext"]
            current_stage = "ordinary_registration"
            registered = production.request(args, "/v1/user/register", method="POST", data={
                "email": account["email"], "nickname": account["nickname"], "password": ciphertext,
            })
            require(production.success(registered), "ordinary_registration_failed")
            account["phase"] = "registered"
            production.write_private(credential_path, state)

            current_stage = "rsa_login"
            login = production.request(args, "/v1/user/login", method="POST", data={"email": account["email"], "password": ciphertext})
            require(production.success(login), "rsa_login_failed")
            token = next((value for key, value in login["headers"].items() if key.lower() == "authorization"), None)
            user = login["json"]["data"]
            require(bool(token) and user.get("access_token") == token, "login_token_contract")
            require((user.get("role") or {}).get("role_code") == "common" and not user.get("is_superuser"), "ordinary_role_required")
            require(str(user.get("is_active")) == "1" and str(user.get("is_authenticated")) == "1", "enabled_account_required")

            current_stage = "server_identity"
            identity = production.request(args, "/query/deerflow/identity", token=token)
            require(production.success(identity), "server_identity_failed")
            principal = identity["json"]["data"]
            require(principal.get("user_id") == user["id"] and principal.get("role_code") == "common" and principal.get("identity_class") == "ordinary_user", "server_identity_must_be_ordinary")
            require(bool(principal.get("org_id")), "independent_workspace_required")
            account.update({"user_id": user["id"], "workspace_id": principal["org_id"], "role_code": "common", "identity_class": "ordinary_user", "login_verified": True, "enabled": True, "phase": "verified"})
            production.write_private(credential_path, state)

            current_stage = "revoke_verification_token"
            logged_out = production.request(args, "/v1/user/logout", token=token)
            require(production.success(logged_out), "verification_logout_failed")
            revoked = production.request(args, "/query/deerflow/identity", token=token)
            require(revoked["status"] == 401, "verification_token_revocation_failed")
            account["verification_token_revoked"] = True
            del token, user, login
            production.write_private(credential_path, state)

        current_stage = "independent_workspaces"
        require(len({account["workspace_id"] for account in state["accounts"]}) == 4, "four_independent_workspaces_required")
        state["phase"] = "ready_for_recording"
        state["independent_workspaces"] = True
        production.write_private(credential_path, state)
        print(json.dumps({"ready": True, "credential_file": str(credential_path), "credential_file_mode": oct(credential_path.stat().st_mode & 0o777), "login_url": state["login_url"], "platform_url": state["platform_url"], "accounts": [{"label": account["label"], "username": account["username"], "email": account["email"], "login_verified": account["login_verified"], "role_code": account["role_code"]} for account in state["accounts"]], "independent_workspaces": True, "accounts_remain_enabled": True}))
        return 0
    except BaseException as error:
        state["phase"] = "incomplete"
        state["failed_stage"] = current_stage
        state["failed_label"] = current_label
        state["error_type"] = type(error).__name__
        production.write_private(credential_path, state)
        print(json.dumps({"ready": False, "credential_file": str(credential_path), "failed_stage": current_stage, "failed_label": current_label, "error_type": type(error).__name__, "accounts_remain_enabled": True}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
