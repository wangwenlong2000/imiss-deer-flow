"""Local HTTP fixtures test the verifier; they are not business acceptance evidence."""
from __future__ import annotations

import base64
from contextlib import contextmanager
import csv
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import hashlib
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

import yaml
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from deploy.cli.configuration import initialize
from deploy.cli.verification import (
    HTTPClient, VerificationFailure, _assert_artifact, _multipart, _scene_cases,
    cleanup_verification, parse_sse, summarize_stream, verify_deployment,
)
from deploy.cli.verification_fixture import cleanup


class FixtureState:
    def __init__(self):
        self.key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.pem = self.key.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo).decode()
        self.accounts = {"admin@example.test": {"password": "fixture-admin-secret", "id": "admin", "role": "admin"}}
        self.tokens = {}
        self.dialogs = {}
        self.history = {}
        self.files = {}
        self.runs = {}
        self.query_count = 0
        self.login_count = 0
        self.advertised_key_id = None
        self.fail_stream = False
        self.omit_run_identity = False
        self.omit_terminal_identity = False
        self.change_stream_identity = False
        self.status_identity_mode = "match"
        self.omit_tool_call = False
        self.model_answer = "苹果"
        self.cleanup_calls = 0
        self.cancel_calls = 0

    def decrypt(self, encoded):
        return self.key.decrypt(base64.b64decode(encoded), padding.OAEP(
            mgf=padding.MGF1(hashes.SHA256()), algorithm=hashes.SHA256(), label=None)).decode()

    def cleanup(self, state):
        self.cleanup_calls += 1
        for account in state["accounts"]:
            self.accounts.pop(account["email"], None)
        return {"cleaned": True, "disabled_accounts": len(state["accounts"])}


@contextmanager
def server_fixture():
    state = FixtureState()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def _send(self, status, data=None, *, body=None, content_type="application/json", headers=None):
            if body is None:
                body = json.dumps(data).encode()
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            for key, value in (headers or {}).items():
                self.send_header(key, value)
            self.end_headers()
            self.wfile.write(body)

        def _ok(self, data=None, headers=None):
            self._send(200, {"code": 0, "data": data}, headers=headers)

        def _request(self):
            parsed = urlsplit(self.path)
            path, query = parsed.path, parse_qs(parsed.query)
            raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            payload = json.loads(raw) if raw and "application/json" in self.headers.get("Content-Type", "") else {}
            token = self.headers.get("Authorization", "").removeprefix("Bearer ")
            email = state.tokens.get(token)
            account = state.accounts.get(email, {})
            if path == "/healthz":
                return self._send(200, {"status": "ok"})
            if path.startswith("/internal/"):
                return self._send(404, {"error": "hidden"})
            if path == "/v1/user/public-key":
                return self._ok({"public_key": state.pem, "algorithm": "RSA-OAEP-256", "key_id": state.advertised_key_id or hashlib.sha256(state.key.public_key().public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)).hexdigest()})
            if path == "/v1/user/register":
                password = state.decrypt(payload["password"])
                state.accounts[payload["email"]] = {"password": password, "id": uuid4().hex, "role": "common"}
                return self._ok(True)
            if path == "/v1/user/login":
                state.login_count += 1
                user = state.accounts.get(payload["email"])
                if not user or state.decrypt(payload["password"]) != user["password"]:
                    return self._send(401, {"code": 401})
                value = "private-token-" + uuid4().hex
                state.tokens[value] = payload["email"]
                return self._ok({"id": user["id"], "access_token": value}, headers={"Authorization": value})
            if not email:
                return self._send(401, {"code": 401, "error": "anonymous"})
            if path == "/v1/user/logout":
                state.tokens.pop(token)
                return self._ok(True)
            if path == "/v1/user/info":
                return self._ok({"id": account["id"], "email": email})
            if path == "/query/deerflow/identity":
                return self._ok({"user_id": account["id"], "org_id": "workspace-" + account["id"], "role_code": account["role"]})
            if path == "/query/deerflow/dialog":
                dialog = payload["conversation_id"]
                if dialog in state.dialogs and state.dialogs[dialog] != email:
                    return self._send(404, {"code": 404})
                state.dialogs[dialog] = email
                return self._ok({"dialog_id": dialog})
            if path.startswith("/v1/dialog/get/"):
                dialog = path.rsplit("/", 1)[-1]
                if state.dialogs.get(dialog) != email:
                    return self._send(404, {"code": 404})
                return self._ok({"id": dialog, "name": "fixture dialog"})
            if path == "/v1/conversation/set":
                dialog = payload["dialog_id"]
                if state.dialogs.get(dialog) != email:
                    return self._send(404, {"code": 404})
                row = {"id": uuid4().hex, **payload}
                state.history.setdefault(dialog, []).append(row)
                return self._ok(row)
            if path == "/v1/conversation/list_by_dialog":
                dialog = query["dialog_id"][0]
                if state.dialogs.get(dialog) != email:
                    return self._send(404, {"code": 404})
                return self._ok(state.history.get(dialog, []))
            dialog = query.get("conversation_id", [payload.get("conversation_id")])[0]
            if state.dialogs.get(dialog) != email:
                return self._send(404, {"code": 404})
            if path == "/query/deerflow/upload":
                thread_id = str(uuid4())
                url = "/api/threads/" + thread_id + "/artifacts/mnt/user-data/uploads/acceptance.csv"
                content = raw.split(b"\r\n\r\n", 1)[1].rsplit(b"\r\n--", 1)[0]
                state.files[(dialog, url)] = content
                return self._ok({"thread_id": thread_id, "files": [{"filename": "acceptance.csv", "artifact_url": url}]})
            if path == "/query/deerflow/file/download":
                url = query["url"][0]
                body = state.files.get((dialog, url))
                if body is None:
                    return self._send(404, {"code": 404})
                return self._send(200, body=body, content_type="text/csv")
            if path == "/query/deerflow/cancel":
                state.cancel_calls += 1
                return self._ok({"status": "cancelled"})
            if path == "/query/deerflow/status":
                identity = {key: query[key][0] for key in ("thread_id", "run_id")}
                if state.status_identity_mode == "mismatch":
                    identity["run_id"] = str(uuid4())
                elif state.status_identity_mode == "missing":
                    identity = {}
                return self._ok({"status": "success", **identity})
            if path == "/query/deerflow":
                state.query_count += 1
                thread_id, run_id = str(uuid4()), str(uuid4())
                question = query["query"][0]
                answer = state.model_answer if "英文单词apple" in question else "A组48，B组70，文件acceptance-result.csv。"
                if "acceptance.csv" in question:
                    url = "/api/threads/" + thread_id + "/artifacts/mnt/user-data/outputs/acceptance-result.csv"
                    state.files[(dialog, url)] = b"group,total\nA,48\nB,70\n"
                events = []
                if not state.omit_run_identity:
                    events.append({"type": "system", "status": "running", "metadata": {"source_event": "status", "thread_id": thread_id, "run_id": run_id}})
                if "acceptance.csv" in question and not state.omit_tool_call:
                    events.append({"type": "agent", "content": "fixture calculation", "metadata": {"source_event": "tool_call:bash"}})
                events.append({"type": "agent", "content": answer, "metadata": {"source_event": "final_answer"}})
                if not state.fail_stream:
                    terminal = {"type": "[DONE]", "finished": True}
                    if not state.omit_run_identity and not state.omit_terminal_identity:
                        terminal.update({"thread_id": thread_id, "run_id": str(uuid4()) if state.change_stream_identity else run_id})
                    events.append(terminal)
                body = b"".join(b"data: " + json.dumps(event).encode() + b"\n\n" for event in events)
                return self._send(200, body=body, content_type="text/event-stream")
            return self._send(404, {"code": 404})

        def do_GET(self):
            self._request()

        def do_POST(self):
            self._request()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield state, "http://127.0.0.1:" + str(server.server_port)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


class StreamContractTests(unittest.TestCase):
    def test_multiline_sse_comments_and_crlf(self):
        events = list(parse_sse([b": keepalive\r\n", b"event: segment\r\n", b'data: {"content":\r\n', b'data: "ok"}\r\n', b"\r\n"]))
        self.assertEqual(events, [{"content": "ok", "_sse_event": "segment"}])

    def test_truncated_frame_does_not_become_terminal(self):
        with self.assertRaisesRegex(VerificationFailure, "truncated_frame"):
            list(parse_sse([b'data: {"type":"[DONE]","finished":true}']))

    def test_error_stream_cannot_pass_even_with_finished_marker(self):
        with self.assertRaisesRegex(VerificationFailure, "execution_error"):
            summarize_stream([{"type": "error"}, {"type": "[DONE]", "finished": True}])

    def test_success_without_real_run_ids_is_rejected(self):
        with self.assertRaisesRegex(VerificationFailure, "real_run_identity"):
            summarize_stream([{"content": "144", "metadata": {"source_event": "final_answer"}}, {"type": "[DONE]", "finished": True}])

    def test_mock_flag_is_not_accepted_as_business_execution(self):
        with self.assertRaisesRegex(VerificationFailure, "mock_execution"):
            summarize_stream([{"mock": True}])

    def test_terminal_identity_supports_fast_runs_without_status_event(self):
        identity = {"thread_id": str(uuid4()), "run_id": str(uuid4())}
        summary, _ = summarize_stream([
            {"content": "144", "metadata": {"source_event": "final_answer"}},
            {"type": "[DONE]", "finished": True, **identity},
        ])
        self.assertEqual(summary["terminal_identity"], identity)
        self.assertTrue(summary["stream_identity_consistent"])

    def test_stream_identity_cannot_change_between_frames_or_within_a_frame(self):
        identity = {"thread_id": str(uuid4()), "run_id": str(uuid4())}
        for key in ("thread_id", "run_id"):
            changed = {**identity, key: str(uuid4())}
            for events in (
                [{"metadata": identity}, {"type": "[DONE]", "finished": True, **changed}],
                [{"metadata": identity, **changed}],
            ):
                with self.subTest(key=key, events=events), self.assertRaisesRegex(VerificationFailure, "identity_changed"):
                    summarize_stream(events)

    def test_terminal_must_repeat_both_observed_identities(self):
        identity = {"thread_id": str(uuid4()), "run_id": str(uuid4())}
        for terminal in ({}, {"thread_id": identity["thread_id"]}, {"run_id": identity["run_id"]}):
            with self.subTest(terminal=terminal), self.assertRaisesRegex(VerificationFailure, "terminal_run_identity_missing"):
                summarize_stream([
                    {"content": "144", "metadata": {"source_event": "final_answer", **identity}},
                    {"type": "[DONE]", "finished": True, **terminal},
                ])

    def test_invalid_intermediate_identity_cannot_be_overwritten_by_valid_terminal(self):
        with self.assertRaisesRegex(VerificationFailure, "invalid_run_identity"):
            summarize_stream([
                {"metadata": {"run_id": "not-a-run"}},
                {"type": "[DONE]", "finished": True, "thread_id": str(uuid4()), "run_id": str(uuid4())},
            ])

    def test_csv_duplicate_keys_or_incorrect_values_fail(self):
        expected = {"csv": {"key_column": "group", "value_column": "total", "expected": {"A": 48, "B": 70}}}
        _assert_artifact(b"group,total\nA,48\nB,70\n", expected)
        for body in (b"group,total\nA,48\nA,48\nB,70\n", b"group,total\nA,48\nB,71\n"):
            with self.assertRaisesRegex(VerificationFailure, "values_mismatch"):
                _assert_artifact(body, expected)

    def test_multipart_refuses_header_injection(self):
        with self.assertRaisesRegex(VerificationFailure, "upload_filename"):
            _multipart({"filename": 'data.csv"\r\nInjected: yes', "content": "synthetic"})

    def test_http_client_rejects_credentials_and_redirect_target_urls(self):
        for url in ("http://user:password@example.test", "file:///tmp/site", "http://example.test/?token=secret"):
            with self.assertRaises(VerificationFailure):
                HTTPClient(url)


class VerificationHTTPTests(unittest.TestCase):
    def site(self, root, url, capabilities=None, *, private_key):
        initialize(root)
        path = root / "config/deployment.yaml"
        config = yaml.safe_load(path.read_text())
        config["bootstrap_admin"]["email"] = "admin@example.test"
        config["public_url"] = url
        config["capabilities"] = capabilities or ["basic"]
        path.write_text(yaml.safe_dump(config))
        (root / "secrets/admin.password").write_text("fixture-admin-secret")
        passphrase = (root / "secrets/rsa.passphrase").read_bytes().strip()
        (root / "secrets/private.pem").write_bytes(private_key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
            serialization.BestAvailableEncryption(passphrase) if passphrase else serialization.NoEncryption()))

    def test_basic_is_readiness_only_and_creates_no_business_or_accounts(self):
        with tempfile.TemporaryDirectory() as temp, server_fixture() as (state, url):
            root = Path(temp)
            self.site(root, url, private_key=state.key)
            result = verify_deployment(root, "basic")
            self.assertTrue(result["passed"], result)
            self.assertFalse(result["business_scenarios_executed"])
            self.assertEqual(state.query_count, 0)
            self.assertEqual(len(state.accounts), 1)
            self.assertEqual(Path(result["report_path"]).stat().st_mode & 0o777, 0o600)
            binding = next(row for row in result["checks"] if row["check"] == "runtime_public_key")
            self.assertTrue(binding["site_identity_matched"])

    def test_different_site_key_fails_before_sending_any_login_credentials(self):
        with tempfile.TemporaryDirectory() as temp, server_fixture() as (state, url):
            root = Path(temp)
            wrong = rsa.generate_private_key(public_exponent=65537, key_size=2048)
            self.site(root, url, private_key=wrong)
            result = verify_deployment(root, "full", cleanup_runner=state.cleanup)
            self.assertFalse(result["passed"])
            self.assertTrue(any(row.get("error") == "public_endpoint_site_identity_mismatch" for row in result["checks"]))
            self.assertEqual(state.login_count, 0)
            self.assertEqual(state.query_count, 0)
            self.assertEqual(len(state.accounts), 1)
            self.assertEqual(state.cleanup_calls, 0)

    def test_basic_rejects_missing_local_site_private_key(self):
        with tempfile.TemporaryDirectory() as temp, server_fixture() as (state, url):
            root = Path(temp)
            self.site(root, url, private_key=state.key)
            (root / "secrets/private.pem").unlink()
            result = verify_deployment(root, "basic")
            self.assertFalse(result["passed"])
            self.assertTrue(any(row["check"] == "runtime_public_key" and row["status"] == "fail" for row in result["checks"]))
            self.assertEqual(state.login_count, 0)

    def test_basic_rejects_public_key_id_that_does_not_match_actual_key(self):
        with tempfile.TemporaryDirectory() as temp, server_fixture() as (state, url):
            root = Path(temp)
            self.site(root, url, private_key=state.key)
            state.advertised_key_id = "0" * 64
            result = verify_deployment(root, "basic")
            self.assertFalse(result["passed"])
            self.assertTrue(any(row.get("error") == "runtime_public_key_id_mismatch" for row in result["checks"]))

    def test_basic_rejects_wrong_private_key_passphrase_without_reporting_it(self):
        with tempfile.TemporaryDirectory() as temp, server_fixture() as (state, url):
            root = Path(temp)
            self.site(root, url, private_key=state.key)
            (root / "secrets/rsa.passphrase").write_text("synthetic-wrong-private-passphrase")
            result = verify_deployment(root, "basic")
            self.assertFalse(result["passed"])
            self.assertNotIn("synthetic-wrong-private-passphrase", json.dumps(result))

    def test_interfaces_register_login_history_isolation_logout_and_cleanup(self):
        with tempfile.TemporaryDirectory() as temp, server_fixture() as (state, url):
            root = Path(temp)
            self.site(root, url, private_key=state.key)
            result = verify_deployment(root, "interfaces", cleanup_runner=state.cleanup)
            self.assertTrue(result["passed"], result)
            names = {check["check"] for check in result["checks"] if check["status"] == "pass"}
            self.assertIn("message_history_round_trip_and_isolation", names)
            self.assertIn("cross_account_run_control_denied", names)
            self.assertEqual(state.tokens, {})
            self.assertEqual(state.cleanup_calls, 1)
            self.assertEqual(len(state.accounts), 1)
            encoded = json.dumps(result)
            self.assertNotIn("fixture-admin-secret", encoded)
            self.assertNotIn("private-token-", encoded)
            cleanup_state = json.loads((Path(result["report_path"]).parent / "cleanup-state.json").read_text())
            self.assertTrue(cleanup_state["cleaned"])
            self.assertNotIn("accounts", cleanup_state)

    def test_full_calls_both_model_and_tool_cases_and_checks_artifact(self):
        with tempfile.TemporaryDirectory() as temp, server_fixture() as (state, url):
            root = Path(temp)
            self.site(root, url, private_key=state.key)
            result = verify_deployment(root, "full", cleanup_runner=state.cleanup)
            self.assertTrue(result["passed"], result)
            self.assertEqual(state.query_count, 2)
            self.assertTrue(result["business_scenarios_executed"])
            self.assertEqual(result["covered_capabilities"], ["basic"])
            self.assertTrue(result["cases"][1]["artifact_values_verified"])
            self.assertTrue(result["cases"][1]["uploaded_file_isolation_verified"])
            self.assertEqual(result["cases"][0]["native_run_status"], "success")
            for case in result["cases"]:
                identity = {key: case[key] for key in ("thread_id", "run_id")}
                self.assertEqual(case["terminal_identity"], identity)
                self.assertEqual(case["native_run_identity"], identity)
                self.assertEqual(case["status_request"], {"conversation_id": case["conversation_id"], **identity})
            self.assertEqual(result["cases"][1]["tool_execution_evidence"]["call_events"], ["tool_call:bash"])

    def test_real_model_qa_still_requires_the_known_semantic_answer(self):
        with tempfile.TemporaryDirectory() as temp, server_fixture() as (state, url):
            root = Path(temp)
            self.site(root, url, private_key=state.key)
            state.model_answer = "香蕉"
            result = verify_deployment(root, "full", cleanup_runner=state.cleanup)
            self.assertFalse(result["passed"])
            self.assertFalse(result["cases"][0]["passed"])
            self.assertTrue(result["cases"][1]["passed"])
            self.assertTrue(any(row.get("error") == "answer_assertion_failed"
                                for row in result["checks"]))
            self.assertEqual(state.cleanup_calls, 1)

    def test_changed_stream_identity_fails_and_cleans_up(self):
        with tempfile.TemporaryDirectory() as temp, server_fixture() as (state, url):
            root = Path(temp)
            self.site(root, url, private_key=state.key)
            state.change_stream_identity = True
            result = verify_deployment(root, "full", cleanup_runner=state.cleanup)
            self.assertFalse(result["passed"])
            self.assertTrue(any(row.get("error") == "stream_run_identity_changed" for row in result["checks"]))
            self.assertEqual(state.cleanup_calls, 1)

    def test_successful_native_status_requires_matching_returned_identity(self):
        for mode in ("mismatch", "missing"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp, server_fixture() as (state, url):
                root = Path(temp)
                self.site(root, url, private_key=state.key)
                state.status_identity_mode = mode
                result = verify_deployment(root, "full", cleanup_runner=state.cleanup)
                self.assertFalse(result["passed"])
                self.assertTrue(any(row.get("error") == "native_run_identity_mismatch" for row in result["checks"]))

    def test_correct_csv_without_real_computation_tool_event_is_not_accepted(self):
        with tempfile.TemporaryDirectory() as temp, server_fixture() as (state, url):
            root = Path(temp)
            self.site(root, url, private_key=state.key)
            state.omit_tool_call = True
            result = verify_deployment(root, "full", cleanup_runner=state.cleanup)
            self.assertFalse(result["passed"])
            self.assertTrue(result["cases"][0]["passed"])
            self.assertFalse(result["cases"][1]["passed"])
            self.assertTrue(any(row.get("error") == "required_computation_tool_not_observed" for row in result["checks"]))

    def test_missing_selected_scene_data_fails_instead_of_reporting_full_acceptance(self):
        with tempfile.TemporaryDirectory() as temp, server_fixture() as (state, url):
            root = Path(temp)
            self.site(root, url, ["basic", "code"], private_key=state.key)
            result = verify_deployment(root, "scenes", cleanup_runner=state.cleanup)
            self.assertFalse(result["passed"])
            self.assertEqual(result["uncovered_capabilities"], ["code"])
            self.assertEqual(state.query_count, 2)

    def test_interrupted_stream_fails_and_cancels_only_observed_run(self):
        with tempfile.TemporaryDirectory() as temp, server_fixture() as (state, url):
            root = Path(temp)
            self.site(root, url, private_key=state.key)
            state.fail_stream = True
            result = verify_deployment(root, "full", cleanup_runner=state.cleanup)
            self.assertFalse(result["passed"])
            self.assertEqual(state.cancel_calls, 2)
            self.assertEqual(state.cleanup_calls, 1)
            self.assertTrue(any(row.get("error") == "stream_missing_terminal" for row in result["checks"]))

    def test_missing_run_trace_cannot_be_accepted_as_real_model_execution(self):
        with tempfile.TemporaryDirectory() as temp, server_fixture() as (state, url):
            root = Path(temp)
            self.site(root, url, private_key=state.key)
            state.omit_run_identity = True
            result = verify_deployment(root, "full", cleanup_runner=state.cleanup)
            self.assertFalse(result["passed"])
            self.assertEqual(state.cancel_calls, 0)
            self.assertEqual(result["covered_capabilities"], [])

    def test_failed_cleanup_fails_acceptance_and_keeps_private_recovery_state(self):
        with tempfile.TemporaryDirectory() as temp, server_fixture() as (state, url):
            root = Path(temp)
            self.site(root, url, private_key=state.key)
            result = verify_deployment(root, "interfaces", cleanup_runner=lambda state: {"cleaned": False})
            self.assertFalse(result["passed"])
            path = Path(result["report_path"]).parent / "cleanup-state.json"
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(len(json.loads(path.read_text())["accounts"]), 2)
            self.assertNotIn("private-token-", json.dumps(result))
            recovered = cleanup_verification(root, path, cleanup_runner=state.cleanup)
            self.assertTrue(recovered["cleaned"])
            cleaned = json.loads(path.read_text())
            self.assertNotIn("accounts", cleaned)
            self.assertTrue(cleaned["cleaned"])
            again = cleanup_verification(root, path, cleanup_runner=lambda state: self.fail("already-cleaned state must not execute cleanup"))
            self.assertTrue(again["already_cleaned"])

    def test_invalid_manifest_fails_with_no_silent_scenario_success(self):
        with tempfile.TemporaryDirectory() as temp, server_fixture() as (state, url):
            root = Path(temp)
            self.site(root, url, private_key=state.key)
            path = root / "scenes.json"
            path.write_text(json.dumps({"schema_version": 1, "cases": [{"id": "unasserted", "capability": "basic", "query": "anything"}]}))
            result = verify_deployment(root, "scenes", scene_manifest=path, cleanup_runner=state.cleanup)
            self.assertFalse(result["passed"])
            self.assertFalse(result["business_scenarios_executed"])
            self.assertEqual(state.query_count, 0)


class CleanupGuardTests(unittest.TestCase):
    def test_cleanup_recovery_refuses_permissive_or_non_synthetic_state(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state = root / "unsafe-state.json"
            state.write_text(json.dumps({"schema_version": 1, "run_id": "c" * 32, "accounts": []}))
            state.chmod(0o644)
            with self.assertRaisesRegex(VerificationFailure, "permissions_invalid"):
                cleanup_verification(root, state)
            state.chmod(0o600)
            with self.assertRaisesRegex(ValueError, "two synthetic accounts"):
                cleanup_verification(root, state)

    def test_cleanup_rejects_non_synthetic_accounts_before_database_import(self):
        with self.assertRaisesRegex(ValueError, "outside synthetic scope"):
            cleanup({"run_id": "a" * 32, "accounts": [
                {"label": "a", "email": "admin@example.test", "nickname": "Administrator"},
                {"label": "b", "email": "admin2@example.test", "nickname": "Administrator"},
            ]})

    def test_cleanup_rejects_unbounded_dialog_scope_before_database_import(self):
        run_id = "b" * 32
        accounts = [{"label": label, "email": "citybrain-verify-" + run_id + "-" + label + "@acceptance.invalid",
                     "nickname": "citybrain-verify-" + run_id + "-" + label} for label in ("a", "b")]
        with self.assertRaisesRegex(ValueError, "dialog ids"):
            cleanup({"run_id": run_id, "accounts": accounts, "dialog_ids": ["some-existing-dialog"]})


if __name__ == "__main__":
    unittest.main()
