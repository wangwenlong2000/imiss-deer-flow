"""Persistent, live delivery probes restricted to isolated acceptance sites.

Run ``seed`` against the original site before stopping it for backup, then
``verify-restored --state ORIGINAL/evidence/delivery-persistent.private.json``
against the new root. Credentials remain owner-only and are deliberately not
part of the public report. ``gateway`` must run on the selected services network
with the Docker socket and site root mounted; it exercises the POST SSE protocol
and reads only hashes/metadata from actual Graph tool messages.

These probes preserve their ordinary accounts to make recovery testable. They
never disable accounts, alter protection settings, or manage Docker resources.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import secrets
import socket
import stat
import subprocess
import time
from urllib.parse import quote, unquote, urlencode, urlsplit
from uuid import UUID, uuid4

from deploy.cli.configuration import read_config
from deploy.cli.preflight import assert_project_owner, inspect_images
from deploy.cli.verification import (
    HTTPClient, MAX_LINE, MAX_STREAM, VerificationFailure, _assert_artifact,
    _default_cases, _encrypt, _login, _multipart, _private_json,
    _public_key_fingerprint, _require, _sha, _site_key_fingerprint, _success,
    _utc, summarize_stream,
)


def _docker(*args, check=True):
    result = subprocess.run(["docker", *map(str, args)], stdin=subprocess.DEVNULL,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True, timeout=60)
    if check and result.returncode:
        raise VerificationFailure("docker_identity_inspection_failed")
    return result


def _read_private(path: Path):
    _require(path.is_absolute() and not path.is_symlink(), "private_state_path_invalid")
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(fd) as stream:
        info = os.fstat(stream.fileno())
        _require(stat.S_ISREG(info.st_mode) and stat.S_IMODE(info.st_mode) == 0o600,
                 "private_state_permissions_invalid")
        raw = stream.read(1024 * 1024 + 1)
    _require(len(raw) <= 1024 * 1024, "private_state_too_large")
    return json.loads(raw)


def _guard_site(root: Path, project: str, *, gateway=False):
    _require(root.is_absolute() and root == root.resolve() and not root.is_symlink()
             and root.parent == Path("/tmp")
             and root.name.startswith("citybrain-acceptance-"), "isolated_root_required")
    _require(bool(re.fullmatch(r"citybrain-acceptance-[a-z0-9][a-z0-9-]{0,79}", project)),
             "isolated_project_required")
    config = read_config(root)
    _require(config["project"] == project, "requested_project_mismatch")
    _require(config["capabilities"] == ["basic"] and config["elasticsearch"]["mode"] == "disabled",
             "basic_disabled_es_profile_required")
    identity = _read_private(root / "data/platform/installation.json")
    _require(identity.get("root") == str(root) and identity.get("project") == project
             and identity.get("release") == config["release"]
             and identity.get("status") == "ready", "installation_identity_not_ready")
    assert_project_owner(root, config, _docker)
    images, errors = inspect_images(config, _docker)
    _require(not errors and {k: v["id"] for k, v in images.items()} ==
             {k: v["id"] for k, v in identity.get("images", {}).items()},
             "installed_image_identity_mismatch")
    identifiers = _docker("ps", "-q", "--filter", "label=com.docker.compose.project=" + project).stdout.split()
    _require(bool(identifiers), "isolated_services_not_running")
    containers = json.loads(_docker("inspect", *identifiers).stdout)
    seen = set()
    for container in containers:
        service = container.get("Config", {}).get("Labels", {}).get("com.docker.compose.service")
        component = {"api": "onecity", "coordinator": "onecity", "gateway": "deerflow",
                     "graph": "deerflow", "edge": "edge", "mysql": "mysql"}.get(service)
        if component:
            _require(container.get("Image") == images[component]["id"], "running_image_identity_mismatch")
            seen.add(service)
    _require({"api", "coordinator", "gateway", "graph", "edge"}.issubset(seen),
             "required_isolated_services_missing")
    if gateway:
        network = project + "-services"
        current = json.loads(_docker("inspect", socket.gethostname()).stdout)[0]
        _require(network in current.get("NetworkSettings", {}).get("Networks", {}),
                 "probe_not_on_selected_services_network")
        metadata = json.loads(_docker("network", "inspect", network).stdout)[0]
        _require(metadata.get("Labels", {}).get("com.docker.compose.project") == project,
                 "services_network_project_mismatch")
    return config, identity


def _client(root: Path, config: dict, timeout: float, *, gateway=False):
    base = "http://edge:8080" if gateway else config["public_url"]
    parsed = urlsplit(base)
    if not gateway:
        _require(parsed.hostname in {"127.0.0.1", "localhost", "::1"}
                 and parsed.port == config["http_port"], "isolated_loopback_origin_required")
    client = HTTPClient(base, timeout)
    data = _success(client.request("/v1/user/public-key"))
    _require(isinstance(data, dict) and data.get("algorithm") == "RSA-OAEP-256"
             and isinstance(data.get("public_key"), str), "site_public_key_invalid")
    expected, actual = _site_key_fingerprint(root), _public_key_fingerprint(data["public_key"])
    _require(secrets.compare_digest(expected, actual) and data.get("key_id") == actual,
             "public_endpoint_site_identity_mismatch")
    return client, data["public_key"]


def _validate_state(state: dict):
    _require(isinstance(state, dict) and state.get("schema_version") == 1
             and bool(re.fullmatch(r"[a-f0-9]{32}", str(state.get("probe_id", "")))),
             "persistent_state_invalid")
    source = Path(state.get("source_root", ""))
    _require(source.is_absolute() and source.parent == Path("/tmp")
             and source.name.startswith("citybrain-acceptance-"), "persistent_source_root_invalid")
    accounts = state.get("accounts")
    _require(isinstance(accounts, list) and len(accounts) == 2, "persistent_accounts_invalid")
    for label, account in zip(("a", "b"), accounts):
        prefix = "citybrain-delivery-" + state["probe_id"] + "-" + label
        _require(isinstance(account, dict) and account.get("label") == label
                 and account.get("email") == prefix + "@acceptance.invalid"
                 and account.get("nickname") == prefix
                 and isinstance(account.get("password"), str) and len(account["password"]) >= 32,
                 "persistent_account_identity_invalid")


def _login_accounts(client, pem, accounts):
    for account in accounts:
        _login(client, account, pem)
        identity = _success(client.request("/query/deerflow/identity", token=account["token"]))
        _require(isinstance(identity, dict) and identity.get("role_code") == "common"
                 and identity.get("user_id") and identity.get("org_id"), "persistent_ordinary_identity_invalid")
        if account.get("identity"):
            _require(all(identity.get(k) == account["identity"].get(k) for k in ("user_id", "org_id", "role_code")),
                     "restored_account_identity_changed")
        account["identity"] = identity
    _require(accounts[0]["identity"]["user_id"] != accounts[1]["identity"]["user_id"]
             and accounts[0]["identity"]["org_id"] != accounts[1]["identity"]["org_id"],
             "persistent_workspaces_not_distinct")


def _logout(client, accounts):
    success = True
    for account in accounts:
        token = account.pop("token", None)
        if token:
            try:
                _success(client.request("/v1/user/logout", token=token))
                _require(client.request("/v1/user/info", token=token).status == 401,
                         "probe_logout_not_revoked")
            except Exception:
                success = False
    return success


def _new_dialog(client, account, name):
    dialog = "citybrain-" + uuid4().hex[:22]
    _success(client.request("/query/deerflow/dialog", token=account["token"], method="POST",
                            data={"conversation_id": dialog, "name": name}))
    return dialog


def _history_rows(client, account, dialog):
    rows = _success(client.request("/v1/conversation/list_by_dialog?" + urlencode({"dialog_id": dialog}),
                                   token=account["token"]))
    if isinstance(rows, dict):
        rows = rows.get("conversations", rows.get("records", rows.get("list")))
    _require(isinstance(rows, list), "persistent_history_response_invalid")
    return rows


def _assert_history(client, accounts, dialog, marker):
    first, other = accounts
    _success(client.request("/v1/dialog/get/" + dialog, token=first["token"]))
    _require(any(marker in json.dumps(row, ensure_ascii=False) for row in _history_rows(client, first, dialog)),
             "persistent_history_marker_missing")
    for path in ("/v1/dialog/get/" + dialog,
                 "/v1/conversation/list_by_dialog?" + urlencode({"dialog_id": dialog})):
        _require(client.request(path, token=other["token"]).status == 404, "persistent_history_isolation_failed")
    return {"history_saved_and_read": True, "cross_account_denied": True}


def _download(client, accounts, dialog, artifact_url):
    path = "/query/deerflow/file/download?" + urlencode({"conversation_id": dialog, "url": artifact_url})
    result = client.request(path, token=accounts[0]["token"])
    _require(result.status == 200, "persistent_file_download_failed")
    _require(client.request(path, token=accounts[1]["token"]).status == 404,
             "persistent_file_isolation_failed")
    return result.body


def _valid_uploaded_artifact_url(value):
    if (not isinstance(value, str) or not value or "\\" in value
            or any(ord(char) <= 32 or ord(char) == 127 for char in value)):
        return False
    try:
        parsed = urlsplit(value)
        if "#" in value or parsed.username is not None or parsed.password is not None:
            return False
        if parsed.scheme:
            if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname or parsed.port == 0:
                return False
        elif parsed.netloc or not value.startswith("/"):
            return False
        path = unquote(parsed.path)
        parts = path.split("/")
        if ("\\" in path or any(part in {".", ".."} for part in parts)
                or len(parts) < 6 or parts[1:3] != ["api", "threads"]
                or parts[4] != "artifacts" or not parts[5]):
            return False
        UUID(parts[3])
    except (ValueError, UnicodeError):
        return False
    return True


def _upload(client, accounts, dialog, upload):
    body, content_type = _multipart(upload)
    data = _success(client.request("/query/deerflow/upload?" + urlencode({"conversation_id": dialog}),
                                   token=accounts[0]["token"], method="POST", body=body, content_type=content_type))
    _require(isinstance(data, dict) and isinstance(data.get("files"), list) and data["files"],
             "persistent_upload_missing")
    attachments = data["files"]
    artifact_url = attachments[0].get("artifact_url") or attachments[0].get("url")
    # OneCity returns either a relative path or its normalized HTTP(S) URL.
    # Both still go through the authenticated OneCity download proxy below.
    _require(_valid_uploaded_artifact_url(artifact_url), "persistent_upload_url_invalid")
    expected = upload.get("bytes", upload.get("content", "").encode())
    _require(_download(client, accounts, dialog, artifact_url) == expected, "persistent_upload_bytes_changed")
    return attachments, artifact_url


def _status(client, token, dialog, summary):
    path = "/query/deerflow/status?" + urlencode({"conversation_id": dialog,
                                                 **{k: summary[k] for k in ("thread_id", "run_id")}})
    deadline = time.monotonic() + 30
    while True:
        result = _success(client.request(path, token=token))
        _require(isinstance(result, dict) and all(result.get(k) == summary[k] for k in ("thread_id", "run_id")),
                 "persistent_status_identity_mismatch")
        if result.get("status") not in {"pending", "running", "submitted"} or time.monotonic() >= deadline:
            break
        time.sleep(0.25)
    _require(result.get("status") in {"success", "completed"}, "persistent_native_run_unsuccessful")
    return {"thread_id": result["thread_id"], "run_id": result["run_id"], "status": result["status"]}


def _run_case(client, accounts, case, *, gateway=False, root=None):
    dialog = _new_dialog(client, accounts[0], "Persistent delivery " + case["id"])
    attachments, uploaded_url = [], None
    if case.get("upload"):
        attachments, uploaded_url = _upload(client, accounts, dialog, case["upload"])
    if gateway:
        summary, answer = _gateway_run(client, accounts, dialog, case["query"], attachments)
    else:
        path = "/query/deerflow?" + urlencode({"conversation_id": dialog, "query": case["query"],
                                               "attachments": json.dumps(attachments, ensure_ascii=False)})
        summary, answer = summarize_stream(client.stream(path, token=accounts[0]["token"]))
    for expected in case.get("expected_text", []):
        pattern = r"(?<!\d)" + re.escape(expected) + r"(?!\d)" if expected.isdigit() else re.escape(expected)
        _require(bool(re.search(pattern, answer)), "persistent_answer_assertion_failed")
    tools = sorted(x for x in summary.get("source_events", []) if x.startswith("tool_call:"))
    if case.get("required_tools"):
        _require(any("tool_call:" + name in tools for name in case["required_tools"]),
                 "persistent_tool_call_not_observed")
    record = {"id": case["id"], "conversation_id": dialog, **summary,
              "native_status": _status(client, accounts[0]["token"], dialog, summary),
              "tool_call_events": tools, "passed": True}
    private = {"case_id": case["id"], "conversation_id": dialog, "thread_id": summary["thread_id"],
               "run_id": summary["run_id"], "upload_url": uploaded_url,
               "upload_sha256": _sha(case["upload"]["content"].encode()) if case.get("upload") else None}
    if case.get("expected_artifact"):
        expected = case["expected_artifact"]
        artifact_url = "/api/threads/" + summary["thread_id"] + "/artifacts/mnt/user-data/outputs/" + quote(expected["path"], safe="/")
        data = _download(client, accounts, dialog, artifact_url)
        _assert_artifact(data, expected)
        record.update(artifact_sha256=_sha(data), artifact_size=len(data), artifact_values_verified=True,
                      uploaded_file_isolation_verified=True, result_file_isolation_verified=True)
        private.update(artifact_url=artifact_url, artifact_sha256=_sha(data), expected_artifact=expected)
    if gateway:
        record["graph_tool_evidence"] = _graph_tools(root, summary["thread_id"], summary["run_id"])
        if case.get("required_tools"):
            _require(any(x["name"] in case["required_tools"] for x in record["graph_tool_evidence"]),
                     "graph_computation_tool_result_missing")
    return record, private


def seed(root, config, client, pem, state_path, report):
    if state_path.exists():
        state = _read_private(state_path)
        _validate_state(state)
        _require(state["source_root"] == str(root) and state.get("source_project") == config["project"],
                 "seed_state_belongs_to_other_site")
        _require(not state.get("seed_completed"), "seed_already_completed")
    else:
        probe_id = uuid4().hex
        state = {"schema_version": 1, "probe_id": probe_id, "source_root": str(root),
                 "source_project": config["project"], "accounts": [], "cases": []}
        for label in ("a", "b"):
            prefix = "citybrain-delivery-" + probe_id + "-" + label
            state["accounts"].append({"label": label, "email": prefix + "@acceptance.invalid",
                                      "nickname": prefix, "password": secrets.token_urlsafe(36), "registered": False})
        _private_json(state_path, state)
    accounts = state["accounts"]
    try:
        for account in accounts:
            if not account.get("registered"):
                _success(client.request("/v1/user/register", method="POST", data={
                    "email": account["email"], "nickname": account["nickname"],
                    "password": _encrypt(account["password"], pem)}))
                account["registered"] = True
                _private_json(state_path, state)
        _login_accounts(client, pem, accounts)
        report["ordinary_accounts_verified"] = 2
        if not state.get("history_dialog"):
            state["history_dialog"] = _new_dialog(client, accounts[0], "Persistent recovery history")
            state["history_marker"] = "delivery-history-" + state["probe_id"]
            _private_json(state_path, state)
        _success(client.request("/v1/conversation/set", token=accounts[0]["token"], method="POST",
                                data={"dialog_id": state["history_dialog"], "sort_order": 1,
                                      "message": state["history_marker"]}))
        report["history"] = _assert_history(client, accounts, state["history_dialog"], state["history_marker"])
        state["cases"] = []
        for case in _default_cases():
            record, private = _run_case(client, accounts, case)
            report["cases"].append(record)
            state["cases"].append(private)
            _private_json(state_path, state)
        report["passed"] = True
    finally:
        report["tokens_revoked"] = _logout(client, accounts)
        if not report["tokens_revoked"]:
            report["passed"] = False
        state["seed_completed"] = report["passed"]
        _private_json(state_path, state)


def verify_restored(root, config, client, pem, source, report):
    state = _read_private(source)
    _validate_state(state)
    _require(state.get("seed_completed") is True and state["source_root"] != str(root)
             and state.get("source_project") != config["project"], "independent_completed_restore_required")
    source_installation = _read_private(root / "data/platform/restored-source-installation.json")
    _require(source_installation.get("root") == state["source_root"]
             and source_installation.get("project") == state["source_project"], "restore_source_provenance_mismatch")
    restored = _read_private(root / "data/platform/last-restore.json")
    _require(restored.get("database_restored") is True and restored.get("assets_verified") is True,
             "database_restore_not_verified")
    accounts = state["accounts"]
    try:
        _login_accounts(client, pem, accounts)
        report["ordinary_accounts_verified"] = 2
        report["history"] = _assert_history(client, accounts, state["history_dialog"], state["history_marker"])
        for previous in state["cases"]:
            dialog = previous["conversation_id"]
            _success(client.request("/v1/dialog/get/" + dialog, token=accounts[0]["token"]))
            result = {"case_id": previous["case_id"], "conversation_id": dialog,
                      "native_status": _status(client, accounts[0]["token"], dialog, previous)}
            if previous.get("upload_url"):
                uploaded = _download(client, accounts, dialog, previous["upload_url"])
                _require(_sha(uploaded) == previous["upload_sha256"], "restored_upload_checksum_mismatch")
                result["upload_sha256"] = _sha(uploaded)
            if previous.get("artifact_url"):
                artifact = _download(client, accounts, dialog, previous["artifact_url"])
                _require(_sha(artifact) == previous["artifact_sha256"], "restored_artifact_checksum_mismatch")
                _assert_artifact(artifact, previous["expected_artifact"])
                result.update(artifact_sha256=_sha(artifact), artifact_values_verified=True)
            report["restored_cases"].append(result)
        for case in _default_cases():
            record, _ = _run_case(client, accounts, case)
            report["cases"].append(record)
        report.update(passed=True, separate_root_and_project=True, database_restored=True,
                      assets_verified=True, cross_machine_verified=False)
    finally:
        report["tokens_revoked"] = _logout(client, accounts)
        if not report["tokens_revoked"]:
            report["passed"] = False


def parse_gateway_sse(lines):
    """Require JSON done followed by the protocol's bare [DONE] sentinel."""
    values, event, terminal, sentinel, count = [], "message", None, False, 0
    for raw in lines:
        _require(len(raw) <= MAX_LINE, "gateway_line_too_large")
        line = raw.decode("utf-8").rstrip("\r\n")
        if line.startswith(":"):
            continue
        if line:
            field, _, value = line.partition(":")
            value = value[1:] if value.startswith(" ") else value
            if field == "event":
                event = value
            elif field == "data":
                values.append(value)
                _require(sum(map(len, values)) <= MAX_LINE, "gateway_frame_too_large")
            continue
        if not values:
            continue
        data = "\n".join(values)
        if data == "[DONE]":
            _require(terminal is not None and not sentinel, "gateway_sentinel_without_terminal")
            sentinel = True
            yield {"event": "sentinel"}
            return
        _require(terminal is None and not sentinel, "gateway_data_after_terminal")
        payload = json.loads(data)
        _require(isinstance(payload, dict), "gateway_frame_not_object")
        count += 1
        _require(count <= 100000, "gateway_event_limit_exceeded")
        if event == "done":
            terminal = payload
        yield {"event": event, "data": payload}
        values, event = [], "message"
    _require(not values, "gateway_truncated_frame")
    _require(sentinel, "gateway_missing_done_sentinel")


def _gateway_run(onecity, accounts, dialog, query, attachments):
    gateway = HTTPClient("http://gateway:3330", onecity.timeout)
    payload = {"conversation_id": dialog, "query": query, "stream": True, "attachments": attachments,
               "compliance_request": {"operation": {"type": "analysis", "target_audience": "self"},
                                      "resource": {"resource_id": dialog}}}
    body = json.dumps(payload, ensure_ascii=False).encode()
    connection, response = gateway._connect("/api/deerflow/mobile/run", accounts[0]["token"],
                                            "POST", body, "application/json")
    started = time.monotonic()
    counts, sources, statuses, answers, identities, terminal = Counter(), set(), [], [], {}, None
    try:
        _require(response.status == 200, "gateway_http_" + str(response.status))
        _require("text/event-stream" in response.getheader("Content-Type", ""), "gateway_sse_response_invalid")
        def lines():
            total = 0
            while True:
                _require(time.monotonic() - started <= gateway.timeout, "gateway_sse_deadline_exceeded")
                line = response.readline(MAX_LINE + 1)
                if not line:
                    return
                total += len(line)
                _require(total <= MAX_STREAM, "gateway_stream_too_large")
                yield line
        for frame in parse_gateway_sse(lines()):
            event, data = frame["event"], frame.get("data", {})
            counts[event] += 1
            _require(event not in {"error", "clarification"}, "gateway_execution_unsuccessful")
            if event in {"accepted", "start", "status", "done"}:
                _require(data.get("conversation_id") == dialog, "gateway_conversation_changed")
            for key in ("thread_id", "run_id"):
                if data.get(key):
                    value = str(UUID(str(data[key])))
                    _require(key not in identities or identities[key] == value, "gateway_run_identity_changed")
                    identities[key] = value
            metadata = data.get("metadata") or {}
            _require(not data.get("mock") and not (isinstance(metadata, dict) and
                     (metadata.get("mock") or metadata.get("source") == "mock")), "gateway_mock_not_accepted")
            if isinstance(metadata, dict) and metadata.get("source_event"):
                sources.add(metadata["source_event"])
                if metadata["source_event"] == "final_answer":
                    answers.append(str(data.get("content") or ""))
            if event == "status":
                _require(data.get("run_id") and data.get("thread_id"), "gateway_status_identity_missing")
                statuses.append(data.get("status"))
            if event == "done":
                terminal = data
        _require(counts["accepted"] == counts["start"] == counts["done"] == counts["sentinel"] == 1,
                 "gateway_lifecycle_incomplete")
        _require(terminal and terminal.get("code") == 0 and terminal.get("status") == "completed"
                 and all(terminal.get(key) == identities.get(key) and identities.get(key)
                         for key in ("thread_id", "run_id")), "gateway_terminal_unsuccessful")
        _require(all(value not in {"failed", "error", "cancelled", "interrupted", "timeout"} for value in statuses),
                 "gateway_failed_status_observed")
        answer = "\n".join(answers)
        _require(answer.strip(), "gateway_final_answer_missing")
        path = "/api/deerflow/mobile/runs/" + identities["thread_id"] + "/" + identities["run_id"]
        status = _success(gateway.request(path, token=accounts[0]["token"]))
        _require(isinstance(status, dict) and all(status.get(key) == identities[key] for key in identities)
                 and status.get("status") == "success", "gateway_native_status_mismatch")
        return {**identities, "event_count": sum(counts.values()), "gateway_event_counts": dict(counts),
                "source_events": sorted(sources), "statuses": statuses, "finished": True,
                "terminal_identity": dict(identities), "stream_identity_consistent": True,
                "answer_sha256": _sha(answer.encode()), "answer_chars": len(answer),
                "gateway_post_request": {"method": "POST", "path": "/api/deerflow/mobile/run",
                                         "conversation_id": dialog, "stream": True},
                "run_id_origin": "server_admission",
                "gateway_admitted_identity": dict(identities),
                "gateway_status": {key: status[key] for key in ("thread_id", "run_id", "status")},
                "gateway_terminal": {key: terminal[key] for key in ("conversation_id", "thread_id", "run_id", "code", "status")},
                "bare_done_received": True}, answer
    finally:
        connection.close()


def _graph_tools(root, thread_id, run_id):
    secret = root / "secrets/admission.key"
    _require(not secret.is_symlink() and stat.S_IMODE(secret.stat().st_mode) == 0o600,
             "graph_identity_secret_permissions_invalid")
    key = secret.read_bytes().strip()
    _require(len(key) >= 32, "graph_identity_secret_invalid")
    credential = hmac.new(key, b"deerflow-internal-graph-v1", hashlib.sha256).hexdigest()
    async def snapshot():
        from langgraph_sdk import get_client
        client = get_client(url="http://graph:3329", headers={"x-deerflow-internal-token": credential})
        run = await client.runs.get(thread_id, run_id)
        latest = await client.runs.list(thread_id, limit=1)
        state = await client.threads.get_state(thread_id)
        _require(len(json.dumps(state).encode()) <= MAX_STREAM, "graph_state_too_large")
        return run, latest, state
    run, latest, state = asyncio.run(snapshot())
    _require(run.get("run_id") == run_id and run.get("thread_id") == thread_id
             and run.get("status") == "success", "graph_native_run_identity_mismatch")
    _require(isinstance(latest, list) and latest and latest[0].get("run_id") == run_id,
             "graph_state_not_latest_run")
    messages = state.get("values", {}).get("messages", [])
    _require(isinstance(messages, list), "graph_messages_invalid")
    calls, tools = {}, []
    for message in messages:
        if not isinstance(message, dict):
            continue
        for call in message.get("tool_calls", []) or []:
            if isinstance(call, dict) and call.get("id"):
                calls[call["id"]] = call
        if message.get("type") != "tool":
            continue
        call = calls.get(message.get("tool_call_id"))
        _require(isinstance(call, dict) and message.get("status") != "error", "graph_tool_result_invalid")
        _require(isinstance(message.get("tool_call_id"), str) and
                 re.fullmatch(r"[A-Za-z0-9_-]{1,256}", message["tool_call_id"]), "graph_tool_call_id_invalid")
        name = call.get("name") or message.get("name")
        _require(isinstance(name, str) and re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", name), "graph_tool_name_invalid")
        content = json.dumps(message.get("content"), sort_keys=True, ensure_ascii=False).encode()
        args = json.dumps(call.get("args"), sort_keys=True, ensure_ascii=False).encode()
        tools.append({"name": name, "tool_call_id": message["tool_call_id"],
                      "result_sha256": _sha(content), "result_bytes": len(content),
                      "arguments_sha256": _sha(args), "run_id": run_id, "thread_id": thread_id,
                      "result_status": message.get("status") or "success"})
    return tools


def gateway(root, config, client, pem, source, report):
    state = _read_private(source)
    _validate_state(state)
    _require(state.get("seed_completed") is True and state["source_root"] == str(root)
             and state.get("source_project") == config["project"], "gateway_seed_identity_mismatch")
    accounts = state["accounts"]
    try:
        _login_accounts(client, pem, accounts)
        for case in _default_cases():
            record, _ = _run_case(client, accounts, case, gateway=True, root=root)
            report["cases"].append(record)
        report["passed"] = True
    finally:
        report["tokens_revoked"] = _logout(client, accounts)
        if not report["tokens_revoked"]:
            report["passed"] = False


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["seed", "verify-restored", "gateway"])
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--state", type=Path)
    parser.add_argument("--timeout", type=float, default=900)
    args = parser.parse_args(argv)
    report = {"schema_version": 1, "probe": args.action, "passed": False, "started_at": _utc(),
              "cases": [], "restored_cases": [], "raw_model_outputs_recorded": False,
              "credentials_recorded": False, "business_requests_are_real": True}
    target = None
    try:
        config, identity = _guard_site(args.root, args.project, gateway=args.action == "gateway")
        _require(0 < args.timeout <= 3600, "probe_timeout_invalid")
        target = args.root / "evidence" / ("delivery-" + args.action + ".json")
        report.update(root=str(args.root), project=args.project,
                      installation_id=identity["installation_id"], release=config["release"])
        client, pem = _client(args.root, config, args.timeout, gateway=args.action == "gateway")
        source = args.state or args.root / "evidence/delivery-persistent.private.json"
        _require(source.is_absolute() and source.name.endswith(".private.json")
                 and source.parent.name == "evidence" and source.parent.parent.parent == Path("/tmp")
                 and source.parent.parent.name.startswith("citybrain-acceptance-"), "probe_state_location_invalid")
        if args.action == "seed":
            _require(source == args.root / "evidence/delivery-persistent.private.json", "seed_state_location_invalid")
            seed(args.root, config, client, pem, source, report)
        elif args.action == "verify-restored":
            verify_restored(args.root, config, client, pem, source, report)
        else:
            gateway(args.root, config, client, pem, source, report)
    except Exception as exc:
        report["passed"] = False
        report["error"] = str(exc) if isinstance(exc, VerificationFailure) else type(exc).__name__
    report["finished_at"] = _utc()
    report["exit_code"] = 0 if report["passed"] else 1
    if target is not None:
        _private_json(target, report)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return report["exit_code"]


if __name__ == "__main__":
    raise SystemExit(main())
