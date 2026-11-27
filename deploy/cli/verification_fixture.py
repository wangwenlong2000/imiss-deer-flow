"""Disable only disposable identities allocated by one verification run.

Invoked explicitly inside the API container, with private JSON on stdin. No
application credentials or identifiers from an arbitrary account are accepted.
"""
from __future__ import annotations

import contextlib
import io
import json
import re
import sys


def validate_state(state: dict) -> None:
    if not isinstance(state, dict):
        raise ValueError("invalid synthetic state")
    run_id = state.get("run_id", "")
    if not isinstance(run_id, str) or not re.fullmatch(r"[a-f0-9]{32}", run_id):
        raise ValueError("invalid synthetic run id")
    accounts = state.get("accounts")
    if not isinstance(accounts, list) or len(accounts) != 2:
        raise ValueError("two synthetic accounts required")
    for label, account in zip(("a", "b"), accounts):
        prefix = "citybrain-verify-" + run_id + "-" + label
        if (not isinstance(account, dict) or account.get("label") != label
                or account.get("email") != prefix + "@acceptance.invalid"
                or account.get("nickname") != prefix):
            raise ValueError("identity outside synthetic scope")
    dialogs = state.get("dialog_ids", [])
    if not isinstance(dialogs, list) or len(dialogs) > 1000 or any(
        not isinstance(value, str) or not re.fullmatch(r"citybrain-[a-f0-9]{22}", value) for value in dialogs
    ):
        raise ValueError("invalid synthetic dialog ids")
    return None


def cleanup(state: dict, *, app=None) -> dict:
    validate_state(state)
    accounts = state["accounts"]
    dialogs = state.get("dialog_ids", [])
    if app is None:
        from onecityagent.application.api import create_api_app
        from onecityagent.application.settings import load_settings_from_env
        app = create_api_app(load_settings_from_env())
    from onecityagent.models.db_models import DB, Dialog, Role, User, UserTenant
    disabled = 0
    with app.app_context(), DB.connection_context():
        for account in accounts:
            rows = list(User.select().where(User.email == account["email"]))
            if len(rows) > 1:
                raise ValueError("ambiguous synthetic account")
            if not rows:
                continue
            user = rows[0]
            if (user.nickname != account["nickname"] or (account.get("user_id") and str(user.id) != account["user_id"])
                    or user.is_superuser):
                raise ValueError("synthetic identity guard mismatch")
            role = Role.get_or_none(Role.id == user.role)
            if role is None or role.code != "common":
                raise ValueError("synthetic account is not ordinary")
            with DB.get_instance().atomic():
                User.update(access_token=None, status="0", is_active="0", is_authenticated="0").where(User.id == user.id).execute()
                UserTenant.update(status="0").where(UserTenant.user_id == user.id).execute()
                if dialogs:
                    Dialog.update(status=0).where((Dialog.creator_id == user.id) & Dialog.id.in_(dialogs)).execute()
            disabled += 1
    return {"cleaned": True, "disabled_accounts": disabled}


def main() -> int:
    try:
        raw = sys.stdin.read(256 * 1024 + 1)
        if len(raw) > 256 * 1024:
            raise ValueError("oversized fixture state")
        state = json.loads(raw)
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            result = cleanup(state)
        print(json.dumps(result))
        return 0
    except Exception as exc:
        print(json.dumps({"cleaned": False, "error_type": type(exc).__name__}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
