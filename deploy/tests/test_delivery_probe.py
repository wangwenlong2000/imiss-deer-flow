"""No-network checks for the delivery probe's isolation and SSE boundaries."""
from pathlib import Path
import io
import json
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from deploy.cli.verification import Response, VerificationFailure
from deploy.tools.delivery_probe import _gateway_run, _guard_site, _read_private, _upload, parse_gateway_sse


def frames(value):
    return list(parse_gateway_sse(value.encode().splitlines(keepends=True)))


class DeliveryProbeTests(unittest.TestCase):
    def upload(self, artifact_url, *, other_status=404, downloaded=b"group,value\nA,17\n", field="artifact_url"):
        requests = []
        attachment = {field: artifact_url, "filename": "acceptance.csv"}
        def request(path, *, token=None, **kwargs):
            requests.append((path, token))
            if path.startswith("/query/deerflow/upload?"):
                return Response(200, {}, json.dumps({"code": 0, "data": {"files": [attachment]}}).encode())
            self.assertTrue(path.startswith("/query/deerflow/file/download?"))
            self.assertEqual(parse_qs(urlsplit(path).query),
                             {"conversation_id": ["citybrain-test"], "url": [artifact_url]})
            return Response(200 if token == "owner" else other_status, {}, downloaded)
        client = SimpleNamespace(request=request)
        result = _upload(client, [{"token": "owner"}, {"token": "other"}], "citybrain-test",
                         {"filename": "acceptance.csv", "content": "group,value\nA,17\n"})
        self.assertEqual(result, ([attachment], artifact_url))
        self.assertEqual([token for _, token in requests], ["owner", "owner", "other"])

    def test_upload_accepts_onecity_normalized_http_urls_and_relative_artifacts_through_proxy(self):
        path = "/api/threads/a7df45c5-821b-4db8-a78f-4f1de38d6779/artifacts/mnt/user-data/uploads/acceptance.csv"
        for url in (path, "http://gateway:3330" + path, "https://citybrain.example.test" + path + "?download=true"):
            with self.subTest(url=url):
                self.upload(url)
        self.upload("http://gateway:3330" + path, field="url")

    def test_upload_rejects_invalid_nonartifact_or_unsafe_urls_before_download(self):
        path = "/api/threads/a7df45c5-821b-4db8-a78f-4f1de38d6779/artifacts/mnt/user-data/uploads/acceptance.csv"
        urls = (None, "", "api/threads/example/artifacts/file", "//gateway:3330" + path,
                "file://gateway" + path, "ftp://gateway" + path, "javascript:" + path,
                "http:///" + path, "http://gateway:bad" + path, "http://gateway:0" + path,
                "http://user:password@gateway" + path, "http://@gateway" + path,
                "http://gateway" + path + "#fragment", path + "#", "http://gateway\n" + path,
                "http://gateway\\evil" + path,
                "/healthz", path.replace("/artifacts/", "/uploads/"),
                path.replace("/artifacts/", "/artifacts/../"), path.replace("/artifacts/", "/artifacts/%2e%2e/"),
                path.replace("/uploads/", "/uploads%5c/"), path.replace("a7df45c5-821b-4db8-a78f-4f1de38d6779", "not-a-thread"),
                path.split("/artifacts/")[0] + "/artifacts/")
        for url in urls:
            with self.subTest(url=url), self.assertRaisesRegex(VerificationFailure, "persistent_upload_url_invalid"):
                self.upload(url)

    def test_upload_retains_content_and_cross_account_isolation_assertions_for_absolute_urls(self):
        url = "http://gateway:3330/api/threads/a7df45c5-821b-4db8-a78f-4f1de38d6779/artifacts/mnt/user-data/uploads/acceptance.csv"
        with self.assertRaisesRegex(VerificationFailure, "persistent_file_isolation_failed"):
            self.upload(url, other_status=200)
        with self.assertRaisesRegex(VerificationFailure, "persistent_upload_bytes_changed"):
            self.upload(url, downloaded=b"wrong-content")

    def gateway(self, *, terminal_status="completed", changed_run=False, status_mismatch=False,
                response_status=200):
        thread = "a7df45c5-821b-4db8-a78f-4f1de38d6779"
        run = "41575745-77c2-4660-a396-2beaa2534f69"
        other = "51575745-77c2-4660-a396-2beaa2534f69"
        identity = {"conversation_id": "citybrain-test", "thread_id": thread, "run_id": run}
        data = [("accepted", {"conversation_id": "citybrain-test"}),
                ("start", {"conversation_id": "citybrain-test", "thread_id": thread}),
                ("status", {**identity, "status": "running"}),
                ("segment", {"metadata": {"source_event": "final_answer"}, "content": "苹果"}),
                ("done", {**identity, "run_id": other if changed_run else run,
                          "code": 0, "status": terminal_status})]
        stream = "".join("event: " + event + "\ndata: " + json.dumps(body) + "\n\n" for event, body in data)
        response = io.BytesIO((stream + "data: [DONE]\n\n").encode())
        response.status = response_status
        response.getheader = lambda *_: "text/event-stream"
        self.gateway_response = response
        native = {"thread_id": thread, "run_id": other if status_mismatch else run,
                  "status": "success", "unapproved_field": "MUST_NOT_APPEAR_IN_REPORT"}
        def connect(path, token, method, body, content_type):
            self.assertEqual((path, token, method, content_type),
                             ("/api/deerflow/mobile/run", "owner", "POST", "application/json"))
            self.gateway_payload = json.loads(body)
            return SimpleNamespace(close=lambda: None), response
        fake = SimpleNamespace(_connect=connect, timeout=5,
                               request=lambda *_, **__: Response(200, {}, json.dumps({"code": 0, "data": native}).encode()))
        with patch("deploy.tools.delivery_probe.HTTPClient", return_value=fake):
            return _gateway_run(SimpleNamespace(timeout=5), [{"token": "owner"}, {"token": "other"}],
                                "citybrain-test", "fixed synthetic question", [])

    def test_gateway_requires_structured_terminal_and_bare_sentinel(self):
        result = frames('event: accepted\r\ndata: {"code":0}\r\n\r\n'
                        'event: done\ndata: {"code":0,"status":"completed"}\n\n'
                        'data: [DONE]\n\n')
        self.assertEqual([item["event"] for item in result], ["accepted", "done", "sentinel"])

    def test_gateway_truncated_terminal_is_rejected(self):
        for stream in ('event: done\ndata: {"code":0}\n\n',
                       'event: done\ndata: {"code":0}',
                       'data: [DONE]\n\n',
                       'event: done\ndata: {"code":0}\n\nevent: segment\ndata: {}\n\n'):
            with self.subTest(stream=stream), self.assertRaises(VerificationFailure):
                frames(stream)

    def test_gateway_status_and_result_are_same_admitted_run(self):
        summary, answer = self.gateway()
        self.assertEqual(answer, "苹果")
        self.assertEqual(summary["gateway_admitted_identity"], summary["terminal_identity"])
        self.assertEqual(summary["gateway_status"]["run_id"], summary["run_id"])
        self.assertEqual(set(summary["gateway_status"]), {"thread_id", "run_id", "status"})
        self.assertNotIn("MUST_NOT_APPEAR_IN_REPORT", json.dumps(summary))

    def test_gateway_posts_operation_resource_hints_without_claiming_trusted_identity(self):
        self.gateway()
        self.assertEqual(self.gateway_payload, {
            "conversation_id": "citybrain-test", "query": "fixed synthetic question", "stream": True,
            "attachments": [], "compliance_request": {
                "operation": {"type": "analysis", "target_audience": "self"},
                "resource": {"resource_id": "citybrain-test"}}})

    def test_gateway_http_error_exposes_only_status_without_reading_response_body(self):
        for status in (401, 403, 422, 503):
            with self.subTest(status=status), self.assertRaisesRegex(VerificationFailure, "^gateway_http_" + str(status) + "$"):
                self.gateway(response_status=status)
            self.assertEqual(self.gateway_response.tell(), 0)

    def test_gateway_rejects_bad_terminal_drifting_id_and_wrong_native_status(self):
        for options in ({"terminal_status": "failed"}, {"changed_run": True}, {"status_mismatch": True}):
            with self.subTest(options=options), self.assertRaises(VerificationFailure):
                self.gateway(**options)

    def test_non_acceptance_root_and_project_fail_before_docker(self):
        with patch("deploy.tools.delivery_probe._docker", side_effect=AssertionError("Docker must not run")):
            for root, project in ((Path("/srv/citybrain"), "citybrain-acceptance-rc1"),
                                  (Path("/tmp/citybrain-acceptance-rc1"), "production"),
                                  (Path("/tmp/citybrain-acceptance-rc1/../production"), "citybrain-acceptance-rc1")):
                with self.subTest(root=root, project=project), self.assertRaises(VerificationFailure):
                    _guard_site(root, project)

    def test_credentials_must_be_regular_owner_only_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "probe.private.json"
            path.write_text('{"accounts":[]}')
            path.chmod(0o644)
            with self.assertRaises(VerificationFailure):
                _read_private(path)
            path.chmod(0o600)
            self.assertEqual(_read_private(path), {"accounts": []})
            redirected = Path(directory) / "link.private.json"
            redirected.symlink_to(path)
            with self.assertRaises(VerificationFailure):
                _read_private(redirected)


if __name__ == "__main__":
    unittest.main()
