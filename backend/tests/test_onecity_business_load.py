"""Validate load-test scoring against transport and task outcomes."""

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import verify_onecity_business_load as load


def test_percentile_uses_nearest_rank():
    assert load.quantiles(list(range(1, 21)))["p95"] == 19
    assert load.quantiles([]) == {"count": 0}


def test_file_scale_preserves_the_complete_scan_boundary():
    assert len(load.make_case(2, 1)["content"]) < 200_000
    assert len(load.make_case(2, 1, oversize=True)["content"]) > 200_000
    assert load.make_case(0, 1)["expected"] == {"sampleA": 34, "sampleB": 42}


@pytest.mark.parametrize(
    "answer,finished,errors,expected",
    [("总人数144人", True, [], True), ("总人数1440人", True, [], False), ("总人数144人", False, [], False), ("总人数144人", True, [{"type": "error", "code": 503}], False)],
)
def test_business_success_requires_correct_answer_and_successful_transport(tmp_path, monkeypatch, answer, finished, errors, expected):
    events = [
        {"status": "running", "metadata": {"source_event": "status", "thread_id": "thread-a", "run_id": "run-a"}},
        {"content": answer, "metadata": {"source_event": "final_answer"}},
        *errors,
        {"type": "[DONE]", "finished": finished},
    ]

    class Connection:
        status = 200

        def __init__(self, *args, **kwargs):
            self.lines = iter([b"data: " + json.dumps(event).encode() + b"\n" for event in events])

        def request(self, *args, **kwargs):
            pass

        def getresponse(self):
            return self

        def readline(self, *args):
            return next(self.lines, b"")

        def close(self):
            pass

    monkeypatch.setattr(load.http.client, "HTTPConnection", Connection)
    monkeypatch.setattr(load.smoke, "request", lambda *args, **kwargs: {"status": 200, "json": {"code": 0}})
    monkeypatch.setattr(load, "native_run", lambda *args, **kwargs: {"status": "success"})
    args = SimpleNamespace(onecity_url="http://127.0.0.1:8086", deadline=10, output=tmp_path)
    account = {"token": "synthetic-token", "test_label": "account-a"}
    result = load.run_case(args, load.make_case(0, 0), account, "case-a")
    assert result["passed"] is expected
