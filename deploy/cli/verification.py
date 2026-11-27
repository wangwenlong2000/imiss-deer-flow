"""Explicit live HTTP acceptance checks for one installed CityBrain site.

Importing this module sends no requests. The basic suite checks readiness and
anonymous boundaries. Interfaces creates two isolated ordinary users (or uses
an explicitly supplied owner-only credentials file). Scenes sends real model
and tool requests; missing scenario coverage fails rather than silently passing.
Raw credentials, HTTP bodies and model output are never written to reports.
"""
from __future__ import annotations

import base64
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import http.client
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import secrets
import stat
import subprocess
import tempfile
import time
from typing import Callable, Iterable
from urllib.parse import quote, urlencode, urlsplit
from uuid import UUID, uuid4

from deploy.cli.configuration import read_config

MAX_BODY = 4 * 1024 * 1024
MAX_STREAM = 32 * 1024 * 1024
MAX_LINE = 1024 * 1024
SUITES = {"basic", "interfaces", "scenes", "full"}
_SAFE_ID = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$")


class VerificationFailure(RuntimeError):
    """A fixed diagnostic identifier, never an upstream response or secret."""


def _require(value, code: str):
    if not value:
        raise VerificationFailure(code)
    return value


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _private_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.is_symlink():
        raise VerificationFailure("evidence_target_is_symlink")
    fd, temporary = tempfile.mkstemp(prefix=".verify-", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


@dataclass
class Response:
    status: int
    headers: dict[str, str]
    body: bytes

    def json(self):
        try:
            return json.loads(self.body)
        except (ValueError, UnicodeDecodeError):
            raise VerificationFailure("response_is_not_json") from None


class HTTPClient:
    """No ambient proxy, cookie jar, redirects or third-party auth forwarding."""

    def __init__(self, base_url: str, timeout: float = 600):
        self.base = urlsplit(base_url.rstrip("/"))
        _require(
            self.base.scheme in {"http", "https"} and self.base.hostname
            and not self.base.username and not self.base.password
            and not self.base.query and not self.base.fragment,
            "invalid_verification_base_url",
        )
        _require(0 < timeout <= 3600, "invalid_verification_timeout")
        self.timeout = timeout

    def _connect(self, path: str, token: str | None, method: str, body, content_type):
        _require(path.startswith("/") and not path.startswith("//"), "invalid_request_path")
        connection_type = http.client.HTTPSConnection if self.base.scheme == "https" else http.client.HTTPConnection
        connection = connection_type(self.base.hostname, self.base.port, timeout=min(self.timeout, 60))
        headers = {"Accept": "application/json, text/event-stream", "User-Agent": "citybrain-acceptance/1"}
        if token:
            _require("\r" not in token and "\n" not in token, "invalid_authentication_token")
            headers["Authorization"] = "Bearer " + token
        if content_type:
            headers["Content-Type"] = content_type
        try:
            connection.request(method, self.base.path.rstrip("/") + path, body=body, headers=headers)
            return connection, connection.getresponse()
        except BaseException:
            connection.close()
            raise

    def request(self, path: str, *, token=None, method="GET", data=None, body=None, content_type=None) -> Response:
        if data is not None:
            body = json.dumps(data, ensure_ascii=False).encode()
            content_type = "application/json"
        connection, response = self._connect(path, token, method, body, content_type)
        try:
            payload = response.read(MAX_BODY + 1)
            _require(len(payload) <= MAX_BODY, "response_too_large")
            return Response(response.status, {k.lower(): v for k, v in response.getheaders()}, payload)
        finally:
            connection.close()

    def stream(self, path: str, *, token: str) -> Iterable[dict]:
        connection, response = self._connect(path, token, "GET", None, None)
        started = time.monotonic()
        try:
            _require(response.status == 200, "stream_http_" + str(response.status))
            _require("text/event-stream" in response.getheader("Content-Type", ""), "stream_content_type_invalid")
            def lines():
                total = 0
                while True:
                    _require(time.monotonic() - started <= self.timeout, "stream_deadline_exceeded")
                    line = response.readline(MAX_LINE + 1)
                    if not line:
                        return
                    total += len(line)
                    _require(len(line) <= MAX_LINE and total <= MAX_STREAM, "stream_size_limit_exceeded")
                    yield line
            yield from parse_sse(lines())
        finally:
            connection.close()


def parse_sse(lines: Iterable[bytes]) -> Iterable[dict]:
    """Parse SSE frames, including multiline data, comments and CRLF."""
    values: list[str] = []
    event = "message"
    size = 0
    for raw in lines:
        _require(len(raw) <= MAX_LINE, "stream_line_too_large")
        try:
            line = raw.decode("utf-8").rstrip("\r\n")
        except UnicodeDecodeError:
            raise VerificationFailure("stream_encoding_invalid") from None
        if line.startswith(":"):
            continue
        if not line:
            if values:
                try:
                    value = json.loads("\n".join(values))
                except ValueError:
                    raise VerificationFailure("stream_event_not_json") from None
                _require(isinstance(value, dict), "stream_event_not_object")
                yield {**value, "_sse_event": event}
            values, event, size = [], "message", 0
            continue
        field, _, content = line.partition(":")
        if content.startswith(" "):
            content = content[1:]
        if field == "data":
            size += len(content)
            _require(size <= MAX_LINE, "stream_frame_too_large")
            values.append(content)
        elif field == "event":
            event = content
    # An incomplete frame is not a confirmed terminal event.
    _require(not values, "stream_truncated_frame")


def summarize_stream(events: Iterable[dict]) -> tuple[dict, str]:
    """Require a real run identity and explicit successful terminal event."""
    answer: list[str] = []
    sources: set[str] = set()
    statuses: list[str] = []
    errors = []
    identity: dict[str, str] = {}
    terminal_identity: dict[str, str] = {}
    done = False
    count = 0
    for event in events:
        count += 1
        _require(count <= 100000, "stream_event_limit_exceeded")
        metadata = event.get("metadata") or {}
        _require(isinstance(metadata, dict), "stream_metadata_invalid")
        _require(not (event.get("mock") or metadata.get("mock") or metadata.get("source") == "mock"), "mock_execution_not_accepted")
        source = metadata.get("source_event") or event.get("_sse_event")
        if isinstance(source, str):
            sources.add(source)
        for origin in (metadata, event):
            for key in ("thread_id", "run_id"):
                if origin.get(key) is None:
                    continue
                try:
                    value = str(UUID(str(origin[key])))
                except (ValueError, TypeError, AttributeError):
                    raise VerificationFailure("stream_invalid_run_identity") from None
                _require(key not in identity or identity[key] == value, "stream_run_identity_changed")
                identity[key] = value
                if event.get("type") == "[DONE]":
                    terminal_identity[key] = value
        status = event.get("status")
        if isinstance(status, str) and (not statuses or statuses[-1] != status):
            statuses.append(status)
        if event.get("type") == "error" or status in {"failed", "cancelled", "interrupted"} or source == "execution_failure":
            errors.append("execution_error")
        if source == "final_answer":
            answer.append(str(event.get("content") or ""))
        if event.get("type") == "[DONE]":
            _require(event.get("finished") is True, "stream_terminal_unsuccessful")
            done = True
            break
    _require(done, "stream_missing_terminal")
    _require(not errors, "stream_execution_error")
    _require(bool(answer) and bool("".join(answer).strip()), "stream_missing_final_answer")
    _require(all(key in identity for key in ("thread_id", "run_id")), "stream_missing_real_run_identity")
    _require(terminal_identity == identity, "stream_terminal_run_identity_missing")
    thread_id, run_id = identity["thread_id"], identity["run_id"]
    text = "\n".join(answer)
    return {"thread_id": thread_id, "run_id": run_id, "event_count": count,
            "source_events": sorted(sources), "statuses": statuses,
            "finished": True, "stream_identity_consistent": True,
            "terminal_identity": terminal_identity,
            "answer_sha256": _sha(text.encode()), "answer_chars": len(text)}, text


def _success(response: Response):
    _require(response.status == 200, "http_" + str(response.status))
    payload = response.json()
    _require(isinstance(payload, dict) and payload.get("code") == 0, "api_result_unsuccessful")
    return payload.get("data")


def _encrypt(password: str, pem: str) -> str:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding, rsa
    key = serialization.load_pem_public_key(pem.encode())
    _require(isinstance(key, rsa.RSAPublicKey) and key.key_size >= 2048, "public_key_invalid")
    return base64.b64encode(key.encrypt(password.encode(), padding.OAEP(
        mgf=padding.MGF1(algorithm=hashes.SHA256()), algorithm=hashes.SHA256(), label=None))).decode()


def _public_key_fingerprint(pem: str) -> str:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    key = serialization.load_pem_public_key(pem.encode())
    _require(isinstance(key, rsa.RSAPublicKey) and key.key_size >= 2048, "public_key_invalid")
    return _sha(key.public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo))


def _site_key_fingerprint(root: Path) -> str:
    """Derive the expected site identity locally; never transmit private data."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    values = []
    for name in ("private.pem", "rsa.passphrase"):
        fd = os.open(root / "secrets" / name, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        with os.fdopen(fd, "rb") as source:
            info = os.fstat(source.fileno())
            _require(stat.S_ISREG(info.st_mode) and not stat.S_IMODE(info.st_mode) & 0o077,
                     "site_identity_secret_permissions_invalid")
            value = source.read(64 * 1024 + 1)
            _require(len(value) <= 64 * 1024, "site_identity_secret_too_large")
            values.append(value)
    password = values[1].strip() or None
    key = serialization.load_pem_private_key(values[0], password=password)
    _require(isinstance(key, rsa.RSAPrivateKey) and key.key_size >= 2048, "site_private_key_invalid")
    return _sha(key.public_key().public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo))


def _login(client: HTTPClient, account: dict, public_key: str) -> str:
    response = client.request("/v1/user/login", method="POST", data={
        "email": account["email"], "password": _encrypt(account["password"], public_key),
    })
    payload = _success(response)
    _require(isinstance(payload, dict), "login_data_invalid")
    token = response.headers.get("authorization") or payload.get("access_token")
    if isinstance(token, str) and token.lower().startswith("bearer "):
        token = token[7:]
    _require(isinstance(token, str) and token.strip(), "login_token_missing")
    account["token"] = token
    account["user_id"] = str(payload.get("id") or "")
    return token


def _read_credentials(path: Path) -> list[dict]:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(fd) as handle:
        mode = os.fstat(handle.fileno())
        _require(stat.S_ISREG(mode.st_mode) and not stat.S_IMODE(mode.st_mode) & 0o077, "credentials_file_permissions_invalid")
        data = json.load(handle)
    accounts = data.get("accounts") if isinstance(data, dict) else None
    _require(isinstance(accounts, list) and len(accounts) == 2, "two_test_accounts_required")
    for row in accounts:
        _require(isinstance(row, dict) and isinstance(row.get("email"), str) and isinstance(row.get("password"), str)
                 and row["email"] and row["password"], "test_credentials_invalid")
    _require(accounts[0]["email"] != accounts[1]["email"], "distinct_test_accounts_required")
    return [{"label": label, "email": row["email"], "password": row["password"]} for label, row in zip(("a", "b"), accounts)]


def _default_cases() -> list[dict]:
    return [
        # A no-tool Q&A must not request computation, which requires tool proof.
        {"id": "basic-model-answer", "capability": "basic",
         "query": "请将英文单词apple翻译成简体中文，只回答对应的水果名称，不调用工具。",
         "expected_text": ["苹果"]},
        {"id": "basic-tool-artifact", "capability": "basic",
         "required_tools": ["bash", "python", "python_repl"],
         "query": "请读取本会话上传的acceptance.csv，必须调用本地Python或bash按group分组计算value总和，"
                  "写入/mnt/user-data/outputs/acceptance-result.csv，列名group,total。"
                  "回答给出两组总和和结果文件下载链接。不联网、不安装依赖、不使用其他数据。",
         "upload": {"filename": "acceptance.csv", "content": "group,value\nA,17\nB,23\nA,31\nB,47\n"},
         "expected_text": ["48", "70"],
         "expected_artifact": {"path": "acceptance-result.csv", "csv": {
             "key_column": "group", "value_column": "total", "expected": {"A": 48, "B": 70}}}},
    ]


def _scene_cases(manifest: Path | None, capabilities: list[str]) -> list[dict]:
    cases = _default_cases()
    if manifest is not None:
        data = json.loads(manifest.read_text(encoding="utf-8"))
        _require(isinstance(data, dict) and data.get("schema_version") == 1 and isinstance(data.get("cases"), list), "scene_manifest_invalid")
        for original in data["cases"]:
            _require(isinstance(original, dict), "scene_case_invalid")
            row = dict(original)
            _require(_SAFE_ID.fullmatch(str(row.get("id", ""))) and row.get("capability") in capabilities
                     and isinstance(row.get("query"), str) and row["query"].strip(), "scene_case_invalid")
            _require(row.get("expected_text") or row.get("expected_artifact"), "scene_assertions_required")
            if "expected_text" in row:
                _require(isinstance(row["expected_text"], list) and all(isinstance(x, str) and x for x in row["expected_text"]), "scene_text_assertions_invalid")
            upload = row.get("upload")
            if upload:
                _require(isinstance(upload, dict) and isinstance(upload.get("filename"), str), "scene_upload_invalid")
                if "path" in upload:
                    file = (manifest.parent / upload["path"]).resolve()
                    _require(file.is_file() and file.stat().st_size <= MAX_BODY, "scene_input_missing_or_too_large")
                    row["upload"] = {"filename": upload["filename"], "bytes": file.read_bytes()}
            artifact = row.get("expected_artifact")
            if artifact:
                _require(isinstance(artifact, dict) and isinstance(artifact.get("path"), str)
                         and (artifact.get("sha256") or artifact.get("csv")), "artifact_content_assertion_required")
            cases.append(row)
    _require(len({row["id"] for row in cases}) == len(cases), "duplicate_scene_case_id")
    return cases


def _multipart(upload: dict) -> tuple[bytes, str]:
    filename = upload.get("filename", "")
    _require(isinstance(filename, str) and filename == PurePosixPath(filename).name
             and filename not in {"", ".", ".."} and not any(c in filename for c in '\r\n"\\'), "invalid_upload_filename")
    content = upload.get("bytes")
    if content is None:
        _require(isinstance(upload.get("content"), str), "upload_content_missing")
        content = upload["content"].encode()
    _require(isinstance(content, bytes) and len(content) <= MAX_BODY, "upload_size_invalid")
    boundary = "citybrain" + uuid4().hex
    body = (f'--{boundary}\r\nContent-Disposition: form-data; name="files"; filename="{filename}"\r\n'
            'Content-Type: application/octet-stream\r\n\r\n').encode() + content + f"\r\n--{boundary}--\r\n".encode()
    return body, "multipart/form-data; boundary=" + boundary


def _assert_artifact(body: bytes, expected: dict) -> None:
    _require(bool(body), "artifact_empty")
    if expected.get("sha256"):
        _require(_sha(body) == expected["sha256"], "artifact_checksum_mismatch")
    if expected.get("csv"):
        spec = expected["csv"]
        try:
            rows = list(csv.DictReader(io.StringIO(body.decode("utf-8-sig"))))
            values = {row[spec["key_column"]]: float(row[spec["value_column"]]) for row in rows}
            wanted = {key: float(value) for key, value in spec["expected"].items()}
        except (UnicodeDecodeError, ValueError, KeyError, TypeError):
            raise VerificationFailure("artifact_csv_invalid") from None
        _require(len(rows) == len(values) == len(wanted) and values == wanted, "artifact_values_mismatch")


def _cleanup_fixtures(root: Path, config: dict, state: dict) -> dict:
    # The worker accepts only exact randomized synthetic identities and disables
    # them; it cannot delete or alter an arbitrary configured account.
    compose = root / "generated/current/compose.yaml"
    _require(compose.is_file(), "cleanup_compose_missing")
    result = subprocess.run([
        "docker", "compose", "-p", config["project"], "-f", str(compose),
        "exec", "-T", "api", "python", "/opt/citybrain/deploy/entrypoints/run.py",
        "python", "-m", "deploy.cli.verification_fixture",
    ], input=json.dumps(state), text=True, capture_output=True, timeout=60)
    _require(result.returncode == 0, "synthetic_account_cleanup_failed")
    try:
        value = json.loads(result.stdout)
    except ValueError:
        raise VerificationFailure("cleanup_response_invalid") from None
    _require(value.get("cleaned") is True, "synthetic_account_cleanup_unconfirmed")
    return {"cleaned": True, "disabled_accounts": value.get("disabled_accounts", 0)}


def verify_deployment(
    root: Path, suite: str, *, base_url: str | None = None,
    credentials_file: Path | None = None, scene_manifest: Path | None = None,
    timeout: float = 600, report_path: Path | None = None,
    cleanup_runner: Callable[[dict], dict] | None = None,
) -> dict:
    """Run an explicitly requested suite and return a secret-free evidence record.

    Default interfaces/scenes/full create randomized ordinary accounts through
    the real registration endpoint, then disable only those users via a guarded
    in-container cleanup worker. A failed cleanup fails acceptance and preserves
    owner-only recovery state. Explicit credentials are logged out, never disabled.
    A basic pass is readiness evidence only and is never scene acceptance.
    """
    _require(suite in SUITES, "unknown_verification_suite")
    root = Path(root).resolve()
    config = read_config(root)
    client = HTTPClient(base_url or config["public_url"], timeout)
    run_id = uuid4().hex
    output = Path(report_path) if report_path else root / "evidence/verification" / run_id / "report.json"
    state_path = output.parent / "cleanup-state.json"
    report = {"schema_version": 1, "run_id": run_id, "suite": suite, "started_at": _utc(),
              "evidence_kind": "live_http", "release": config["release"],
              "checks": [], "cases": [], "business_requests_sent": False,
              "business_scenarios_executed": False, "selected_capabilities": config["capabilities"],
              "covered_capabilities": [], "uncovered_capabilities": [], "report_path": str(output)}
    report["data_policy"] = {
        "disposable_accounts": credentials_file is None and suite != "basic",
        "cleanup_action": "revoke_tokens_and_disable_synthetic_accounts" if credentials_file is None else "revoke_supplied_test_account_tokens",
        "test_artifacts_and_audit_retained": suite in {"scenes", "full"},
        "raw_model_outputs_recorded": False,
    }
    accounts: list[dict] = []
    synthetic = credentials_file is None
    state = {"schema_version": 1, "run_id": run_id, "accounts": [], "dialog_ids": []}

    def check(name: str, action: Callable):
        started = time.monotonic()
        record = {"check": name, "status": "fail"}
        try:
            details = action()
            record["status"] = "pass"
            if isinstance(details, dict):
                record.update(details)
        except Exception as exc:
            record["error"] = str(exc) if isinstance(exc, VerificationFailure) else type(exc).__name__
        record["seconds"] = round(time.monotonic() - started, 3)
        report["checks"].append(record)
        return record["status"] == "pass"

    def expect_http(path, expected):
        result = client.request(path)
        _require(result.status == expected, "unexpected_http_" + str(result.status))
        if path == "/healthz":
            payload = result.json()
            _require(isinstance(payload, dict) and payload.get("status") == "ok", "readiness_payload_invalid")
        if expected == 401:
            _require(isinstance(result.json(), dict), "anonymous_error_not_json")
        return {"http_status": result.status}

    try:
        ready = check("api_readiness", lambda: expect_http("/healthz", 200))
        check("api_anonymous_rejected", lambda: expect_http("/v1/user/info", 401))
        check("coordinator_anonymous_rejected", lambda: expect_http("/query/deerflow/identity", 401))
        check("public_internal_iam_hidden", lambda: expect_http("/internal/deerflow/iam", 404))
        key_data: dict = {}
        def load_key():
            expected = _site_key_fingerprint(root)
            data = _success(client.request("/v1/user/public-key"))
            _require(isinstance(data, dict) and data.get("algorithm") == "RSA-OAEP-256"
                     and isinstance(data.get("public_key"), str), "runtime_public_key_contract_invalid")
            actual = _public_key_fingerprint(data["public_key"])
            _require(secrets.compare_digest(expected, actual), "public_endpoint_site_identity_mismatch")
            _require(isinstance(data.get("key_id"), str) and secrets.compare_digest(data["key_id"], actual),
                     "runtime_public_key_id_mismatch")
            _encrypt("key-contract-probe", data["public_key"])
            key_data.update(data)
            return {"key_id": actual, "site_identity_matched": True}
        key_ready = check("runtime_public_key", load_key)

        if suite != "basic" and ready:
            if key_ready:
                pem = key_data["public_key"]
                if synthetic:
                    def bootstrap_login():
                        admin = {"email": config["bootstrap_admin"]["email"],
                                 "password": (root / "secrets/admin.password").read_text().strip()}
                        token = _login(client, admin, pem)
                        try:
                            _success(client.request("/v1/user/info", token=token))
                        finally:
                            _success(client.request("/v1/user/logout", token=token))
                        return {"authenticated": True, "token_revoked": True}
                    check("bootstrap_admin_login", bootstrap_login)
                    for label in ("a", "b"):
                        prefix = "citybrain-verify-" + run_id + "-" + label
                        accounts.append({"label": label, "email": prefix + "@acceptance.invalid",
                                         "nickname": prefix, "password": secrets.token_urlsafe(32)})
                    state["accounts"] = accounts
                    _private_json(state_path, state)
                    for account in accounts:
                        def register(account=account):
                            _success(client.request("/v1/user/register", method="POST", data={
                                "email": account["email"], "nickname": account["nickname"],
                                "password": _encrypt(account["password"], pem),
                                # A role field is deliberately not used for authorization.
                            }))
                        check("register_ordinary_" + account["label"], register)
                else:
                    accounts = _read_credentials(Path(credentials_file))
                for account in accounts:
                    def login(account=account):
                        _login(client, account, pem)
                        profile = _success(client.request("/v1/user/info", token=account["token"]))
                        _require(isinstance(profile, dict) and "password" not in profile and "access_token" not in profile, "profile_exposes_credentials")
                        identity = _success(client.request("/query/deerflow/identity", token=account["token"]))
                        _require(isinstance(identity, dict) and identity.get("role_code") == "common"
                                 and identity.get("user_id") and identity.get("org_id"), "ordinary_identity_invalid")
                        account["identity"] = identity
                    check("login_identity_" + account["label"], login)
                    if synthetic:
                        _private_json(state_path, state)
                if len(accounts) == 2 and all(a.get("identity") for a in accounts):
                    first, other = accounts
                    check("distinct_account_workspaces", lambda: _require(
                        first["identity"]["user_id"] != other["identity"]["user_id"]
                        and first["identity"]["org_id"] != other["identity"]["org_id"], "test_accounts_share_identity_or_workspace"))
                    dialog = "citybrain-" + uuid4().hex[:22]
                    state["dialog_ids"].append(dialog)
                    if synthetic:
                        _private_json(state_path, state)
                    def create_dialog():
                        _success(client.request("/query/deerflow/dialog", token=first["token"], method="POST",
                                                data={"conversation_id": dialog, "name": "Installation acceptance " + run_id[:8]}))
                    owned = check("create_owned_dialog", create_dialog)
                    if owned:
                        check("owned_history_readable", lambda: _require(isinstance(_success(client.request(
                            "/v1/dialog/get/" + dialog, token=first["token"])), dict), "owned_history_invalid"))
                        def history_round_trip():
                            saved = _success(client.request("/v1/conversation/set", token=first["token"], method="POST", data={
                                "dialog_id": dialog, "sort_order": 1, "message": "installation-acceptance-" + run_id,
                            }))
                            _require(isinstance(saved, dict), "saved_history_invalid")
                            rows = _success(client.request("/v1/conversation/list_by_dialog?" + urlencode({"dialog_id": dialog}), token=first["token"]))
                            # Some API versions return a list, some a records wrapper.
                            if isinstance(rows, dict):
                                rows = rows.get("conversations", rows.get("records", rows.get("list")))
                            _require(isinstance(rows, list) and any("installation-acceptance-" + run_id in json.dumps(row, ensure_ascii=False) for row in rows), "history_round_trip_mismatch")
                            denied = client.request("/v1/conversation/list_by_dialog?" + urlencode({"dialog_id": dialog}), token=other["token"])
                            _require(denied.status == 404, "cross_account_message_history_accessible")
                            return {"history_saved_and_read": True, "cross_account_denied": True}
                        check("message_history_round_trip_and_isolation", history_round_trip)
                        def cross_account():
                            denied = client.request("/v1/dialog/get/" + dialog, token=other["token"])
                            _require(denied.status == 404, "cross_account_history_accessible")
                            denied = client.request("/query/deerflow/dialog", token=other["token"], method="POST",
                                                    data={"conversation_id": dialog, "name": "must not replace"})
                            _require(denied.status == 404, "cross_account_dialog_accessible")
                        check("cross_account_history_and_dialog_denied", cross_account)
                        def invalid_control():
                            denied = client.request("/query/deerflow/status?" + urlencode({
                                "conversation_id": dialog, "thread_id": str(uuid4()), "run_id": str(uuid4())}), token=other["token"])
                            _require(denied.status == 404, "cross_account_run_accessible")
                        check("cross_account_run_control_denied", invalid_control)
                    if suite in {"scenes", "full"}:
                        cases: list[dict] = []
                        def load_cases():
                            cases.extend(_scene_cases(Path(scene_manifest) if scene_manifest else None, config["capabilities"]))
                        if check("scene_manifest_and_assertions", load_cases):
                            for case in cases:
                                _run_case(client, case, first, other, state, report, check, state_path if synthetic else None)
                            covered = {row["capability"] for row in report["cases"] if row["passed"]}
                            failed = {row["capability"] for row in report["cases"] if not row["passed"]}
                            report["covered_capabilities"] = sorted(covered - failed)
                            report["uncovered_capabilities"] = sorted(set(config["capabilities"]) - (covered - failed))
                            check("selected_capabilities_covered", lambda: _require(not report["uncovered_capabilities"], "selected_capabilities_not_covered"))
    except Exception as exc:
        report["checks"].append({"check": "verification_setup", "status": "fail",
                                 "error": str(exc) if isinstance(exc, VerificationFailure) else type(exc).__name__})
    finally:
        for account in accounts:
            token = account.get("token")
            if token:
                def revoke(token=token):
                    _success(client.request("/v1/user/logout", token=token))
                    _require(client.request("/v1/user/info", token=token).status == 401, "logged_out_token_still_valid")
                check("revoke_test_token_" + account["label"], revoke)
        if synthetic and state["accounts"]:
            def cleanup():
                result = cleanup_runner(state) if cleanup_runner else _cleanup_fixtures(root, config, state)
                _require(isinstance(result, dict) and result.get("cleaned") is True, "synthetic_account_cleanup_unconfirmed")
                _private_json(state_path, {"schema_version": 1, "run_id": run_id, "cleaned": True,
                                          "dialog_ids": state["dialog_ids"], "disabled_accounts": result.get("disabled_accounts", 0)})
                return {"disabled_accounts": result.get("disabled_accounts", 0)}
            check("synthetic_fixture_cleanup", cleanup)
        # A dependency failure must not reduce a requested suite to readiness.
        checks = {row["check"]: row["status"] for row in report["checks"]}
        if suite != "basic":
            for required in ("login_identity_a", "login_identity_b", "create_owned_dialog", "cross_account_history_and_dialog_denied"):
                if required not in checks:
                    report["checks"].append({"check": required, "status": "fail", "error": "required_check_not_executed"})
        if suite in {"scenes", "full"} and not report["cases"]:
            report["checks"].append({"check": "business_scenarios_executed", "status": "fail", "error": "no_scenario_executed"})
            report["uncovered_capabilities"] = list(config["capabilities"])
        report["business_scenarios_executed"] = bool(report["cases"]) and any(row.get("request_sent") for row in report["cases"])
        report["finished_at"] = _utc()
        report["passed"] = bool(report["checks"]) and all(row["status"] == "pass" for row in report["checks"])
        report["exit_code"] = 0 if report["passed"] else 1
        _private_json(output, report)
    return report


def _run_case(client, case, account, other, state, report, check, state_path):
    record = {"id": case["id"], "capability": case["capability"], "passed": False, "request_sent": False}
    report["cases"].append(record)
    dialog = "citybrain-" + uuid4().hex[:22]
    record["conversation_id"] = dialog
    state["dialog_ids"].append(dialog)
    if state_path:
        _private_json(state_path, state)
    stream_summary = None
    observed: dict[str, str] = {}

    def execute():
        nonlocal stream_summary
        _success(client.request("/query/deerflow/dialog", token=account["token"], method="POST",
                                data={"conversation_id": dialog, "name": "Acceptance " + case["id"]}))
        attachments = []
        if case.get("upload"):
            body, content_type = _multipart(case["upload"])
            upload = _success(client.request("/query/deerflow/upload?" + urlencode({"conversation_id": dialog}),
                                             method="POST", token=account["token"], body=body, content_type=content_type))
            _require(isinstance(upload, dict) and isinstance(upload.get("files"), list) and upload["files"], "upload_has_no_files")
            attachments = upload["files"]
            record["uploaded_files"] = len(attachments)
            artifact_url = attachments[0].get("artifact_url") or attachments[0].get("url")
            _require(bool(artifact_url), "upload_artifact_url_missing")
            download = "/query/deerflow/file/download?" + urlencode({"conversation_id": dialog, "url": artifact_url})
            own_file = client.request(download, token=account["token"])
            _require(own_file.status == 200, "uploaded_file_download_failed")
            expected = case["upload"].get("bytes", case["upload"].get("content", "").encode())
            _require(own_file.body == expected, "uploaded_file_content_mismatch")
            _require(client.request(download, token=other["token"]).status == 404, "cross_account_uploaded_file_accessible")
            record["uploaded_file_isolation_verified"] = True
        path = "/query/deerflow?" + urlencode({"conversation_id": dialog, "query": case["query"],
                                                "attachments": json.dumps(attachments, ensure_ascii=False)})
        report["business_requests_sent"] = True
        record["request_sent"] = True
        def capture_events():
            for event in client.stream(path, token=account["token"]):
                metadata = event.get("metadata") or {}
                if isinstance(metadata, dict):
                    for key in ("thread_id", "run_id"):
                        value = metadata.get(key) or event.get(key)
                        if value:
                            try:
                                observed[key] = str(UUID(str(value)))
                            except (ValueError, TypeError, AttributeError):
                                pass
                yield event
        stream_summary, answer = summarize_stream(capture_events())
        record.update(stream_summary)
        tool_calls = sorted(source for source in stream_summary["source_events"] if source.startswith("tool_call:"))
        record["tool_call_events"] = tool_calls
        if case.get("required_tools"):
            _require(any("tool_call:" + name in tool_calls for name in case["required_tools"]),
                     "required_computation_tool_not_observed")
        for expected in case.get("expected_text", []):
            pattern = r"(?<!\d)" + re.escape(expected) + r"(?!\d)" if expected.isdigit() else re.escape(expected)
            _require(bool(re.search(pattern, answer)), "answer_assertion_failed")
        expected_artifact = case.get("expected_artifact")
        if expected_artifact:
            path = PurePosixPath(expected_artifact["path"])
            _require(not path.is_absolute() and ".." not in path.parts and str(path) not in {"", "."}, "artifact_path_invalid")
            artifact = "/api/threads/" + stream_summary["thread_id"] + "/artifacts/mnt/user-data/outputs/" + quote(str(path), safe="/")
            download = "/query/deerflow/file/download?" + urlencode({"conversation_id": dialog, "url": artifact})
            data = client.request(download, token=account["token"])
            _require(data.status == 200, "result_artifact_download_failed")
            _assert_artifact(data.body, expected_artifact)
            _require(client.request(download, token=other["token"]).status == 404, "cross_account_result_artifact_accessible")
            record["artifact_sha256"] = _sha(data.body)
            record["artifact_size"] = len(data.body)
            record["artifact_values_verified"] = True
        status_path = "/query/deerflow/status?" + urlencode({"conversation_id": dialog, **{k: stream_summary[k] for k in ("thread_id", "run_id")}})
        record["status_request"] = {"conversation_id": dialog,
                                    **{key: stream_summary[key] for key in ("thread_id", "run_id")}}
        deadline = time.monotonic() + min(client.timeout, 30)
        while True:
            result = _success(client.request(status_path, token=account["token"]))
            _require(isinstance(result, dict) and all(result.get(key) == stream_summary[key]
                     for key in ("thread_id", "run_id")), "native_run_identity_mismatch")
            status = result.get("status") if isinstance(result, dict) else None
            if status not in {"pending", "running", "submitted"} or time.monotonic() >= deadline:
                break
            time.sleep(0.25)
        _require(status in {"success", "completed"}, "native_run_not_successful")
        record["native_run_status"] = status
        record["native_run_identity"] = {key: result[key] for key in ("thread_id", "run_id")}
        if case.get("required_tools"):
            # Gateway publishes real tool_call:<name> segments, not ToolMessage
            # bodies. Do not invent a tool_result event or expose raw output.
            # The downloaded, numerically verified artifact and successful exact
            # native run are the public protocol's result evidence.
            record["tool_execution_evidence"] = {
                "call_events": tool_calls, "result_evidence": "downloaded_artifact_values_and_native_run_success",
                "artifact_sha256": record.get("artifact_sha256"), "run_id": stream_summary["run_id"],
            }
        _success(client.request("/v1/dialog/get/" + dialog, token=account["token"]))
        _require(client.request("/v1/dialog/get/" + dialog, token=other["token"]).status == 404, "cross_account_scene_history_accessible")
        record["passed"] = True
        return {"case_id": case["id"], "native_run_status": status}
    passed = check("scene_" + case["id"], execute)
    if not passed and all(key in observed for key in ("thread_id", "run_id")):
        def cancel():
            data = _success(client.request("/query/deerflow/cancel", token=account["token"], method="POST",
                                           data={"conversation_id": dialog, **observed}))
            _require(isinstance(data, dict), "cancel_response_invalid")
            return {"cancel_attempted": True}
        check("cancel_failed_scene_" + case["id"], cancel)


def cleanup_verification(root: Path, state_path: Path, *, cleanup_runner=None) -> dict:
    """Resume guarded cleanup after an interrupted acceptance run.

    The state file must be an owner-only regular file with the exact synthetic
    run shape. This operation only disables identities; it does not need their
    passwords or tokens and never copies state contents to the returned report.
    """
    root, state_path = Path(root).resolve(), Path(state_path)
    fd = os.open(state_path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(fd) as stream:
        info = os.fstat(stream.fileno())
        _require(stat.S_ISREG(info.st_mode) and not stat.S_IMODE(info.st_mode) & 0o077, "cleanup_state_permissions_invalid")
        raw = stream.read(256 * 1024 + 1)
        _require(len(raw) <= 256 * 1024, "cleanup_state_too_large")
        state = json.loads(raw)
    _require(isinstance(state, dict) and state.get("schema_version") == 1
             and isinstance(state.get("run_id"), str) and re.fullmatch(r"[a-f0-9]{32}", state["run_id"]), "cleanup_state_invalid")
    if state.get("cleaned") is True:
        return {"cleaned": True, "already_cleaned": True, "run_id": state["run_id"], "disabled_accounts": state.get("disabled_accounts", 0)}
    # The worker independently validates all identities and guards against any
    # attempt to convert a recovery state into an arbitrary-account mutation.
    from deploy.cli.verification_fixture import validate_state
    validate_state(state)
    config = read_config(root)
    result = cleanup_runner(state) if cleanup_runner else _cleanup_fixtures(root, config, state)
    _require(isinstance(result, dict) and result.get("cleaned") is True, "synthetic_account_cleanup_unconfirmed")
    summary = {"schema_version": 1, "run_id": state["run_id"], "cleaned": True,
               "dialog_ids": state.get("dialog_ids", []), "disabled_accounts": result.get("disabled_accounts", 0)}
    _private_json(state_path, summary)
    return {"cleaned": True, "run_id": state["run_id"], "disabled_accounts": summary["disabled_accounts"]}
