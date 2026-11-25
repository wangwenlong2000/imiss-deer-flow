"""A closed SSE connection must preserve the actual task outcome."""

import asyncio
import hashlib
import json
from types import SimpleNamespace
from uuid import uuid4

import langgraph_sdk
import pytest
from fastapi import HTTPException

from app.gateway.routers import mobile


@pytest.mark.parametrize("run_status,expected", [("success", "completed"), ("error", "failed"), ("timeout", "failed"), ("interrupted", "cancelled")])
def test_structured_done_preserves_native_terminal_outcome(monkeypatch, run_status, expected):
    async def produce(request, queue):
        await queue.put({"kind": "start", "thread_id": "thread-a"})
        await queue.put({"kind": "chunk", "raw_event": {"event": "values", "data": {"messages": [
            {"type": "human", "content": "synthetic"}, {"type": "ai", "content": "result"},
        ]}}})
        await queue.put({"kind": "done", "run_status": run_status})

    monkeypatch.setattr(mobile, "_produce_langgraph_stream", produce)

    async def exercise():
        req = mobile.DeerFlowMobileRunRequest(query="synthetic", conversation_id="dialog-a", stream=True)
        events = [event async for event in mobile._stream_deerflow_mobile_run(req)]
        done = next(event for event in events if event.startswith("event: done\n"))
        return json.loads(done.split("data: ", 1)[1])

    done = asyncio.run(exercise())
    assert done["status"] == expected
    assert (done["code"] == 0) == (expected == "completed")


def test_graph_error_event_cannot_end_as_completed(monkeypatch):
    async def produce(request, queue):
        await queue.put({"kind": "start", "thread_id": "thread-a"})
        await queue.put({"kind": "chunk", "raw_event": {"event": "error", "data": {"error": "InternalServerError", "message": "Synthetic provider failure"}}})
        await queue.put({"kind": "done"})

    monkeypatch.setattr(mobile, "_produce_langgraph_stream", produce)

    async def exercise():
        req = mobile.DeerFlowMobileRunRequest(query="synthetic", conversation_id="dialog-a", stream=True)
        events = [event async for event in mobile._stream_deerflow_mobile_run(req)]
        return json.loads(next(event for event in events if event.startswith("event: done\n")).split("data: ", 1)[1])

    assert asyncio.run(exercise())["status"] == "failed"


@pytest.mark.parametrize("status", ["pending", "running", "error", "timeout", "interrupted"])
def test_terminal_checkpoint_fallback_waits_for_exact_native_run_success(monkeypatch, status):
    calls = []

    class Runs:
        async def get(self, thread_id, run_id):
            calls.append((thread_id, run_id))
            return {"status": status}

    class Threads:
        async def get_state(self, thread_id):
            pytest.fail("A persisted checkpoint cannot establish successful task completion")

    monkeypatch.setattr(langgraph_sdk, "get_client", lambda **kwargs: SimpleNamespace(runs=Runs(), threads=Threads()))
    event = asyncio.run(mobile._load_terminal_state_event(
        "thread-a", run_id="run-a", current_query="synthetic", initial_checkpoint_id="old",
    ))
    assert event is None
    assert calls == [("thread-a", "run-a")]


@pytest.mark.parametrize("legacy", [False, True])
def test_resume_checks_exact_current_query_and_rejects_newer_run(monkeypatch, legacy):
    current = ["run-a"]
    query = "current query"

    class Runs:
        async def get(self, thread_id, run_id):
            return {"status": "success", "metadata": {} if legacy else {"mobile_query_sha256": hashlib.sha256(query.encode()).hexdigest()}}

        async def list(self, thread_id, **kwargs):
            return [{"run_id": current[0]}]

    class Threads:
        async def get_state(self, thread_id):
            return {"values": {"messages": [
                {"type": "human", "content": query},
                {"type": "human", "name": "todo_routing_guidance", "content": "internal planning"},
                {"type": "ai", "content": "answer"},
            ]}}

    client = SimpleNamespace(runs=Runs(), threads=Threads())
    assert asyncio.run(mobile._resume_task_state(client, "thread-a", "run-a", query))["values"]
    with pytest.raises(HTTPException) as changed:
        asyncio.run(mobile._resume_task_state(client, "thread-a", "run-a", "old query"))
    assert changed.value.status_code == 409
    current[0] = "run-b"
    with pytest.raises(HTTPException) as newer:
        asyncio.run(mobile._resume_task_state(client, "thread-a", "run-a", query))
    assert newer.value.status_code == 409


def test_resume_rechecks_latest_run_after_loading_checkpoint():
    class Runs:
        def __init__(self):
            self.lookups = 0

        async def get(self, *args):
            return {"status": "success", "metadata": {"mobile_query_sha256": hashlib.sha256(b"query").hexdigest()}}

        async def list(self, *args, **kwargs):
            self.lookups += 1
            return [{"run_id": "run-a" if self.lookups == 1 else "run-b"}]

    class Threads:
        async def get_state(self, *args):
            return {"values": {"messages": [{"type": "human", "content": "query"}]}}

    with pytest.raises(HTTPException) as changed:
        asyncio.run(mobile._resume_task_state(SimpleNamespace(runs=Runs(), threads=Threads()), "thread-a", "run-a", "query"))
    assert changed.value.status_code == 409


@pytest.mark.parametrize("status", ["pending", "running", None])
def test_cancellation_requires_a_confirmed_native_terminal_state(monkeypatch, status):
    class Runs:
        async def cancel(self, *args, **kwargs):
            assert kwargs == {"wait": True, "action": "interrupt"}

        async def get(self, *args):
            return {"status": status}

    async def owned(*args):
        return SimpleNamespace(runs=Runs()), {"status": "running"}

    monkeypatch.setattr(mobile, "_mobile_owned_run", owned)
    with pytest.raises(HTTPException) as result:
        asyncio.run(mobile.mobile_cancel_run(uuid4(), uuid4(), None))
    assert result.value.status_code == 503


@pytest.mark.parametrize('run_status,expected', [('success', 'awaiting_clarification'), ('error', 'failed'), ('timeout', 'failed'), ('interrupted', 'awaiting_clarification')])
def test_native_failure_takes_precedence_over_pending_clarification(monkeypatch, run_status, expected):
    async def produce(request, queue):
        await queue.put({'kind': 'start', 'thread_id': 'thread-a'})
        await queue.put({'kind': 'chunk', 'raw_event': {'event': 'updates', 'data': {'clarification': {}}}})
        await queue.put({'kind': 'done', 'run_status': run_status})

    monkeypatch.setattr(mobile, '_produce_langgraph_stream', produce)
    monkeypatch.setattr(mobile, '_extract_clarification_from_event', lambda *args, **kwargs: {'type': 'clarification', 'content': '请补充范围'})

    async def collect():
        req = mobile.DeerFlowMobileRunRequest(query='synthetic', conversation_id='dialog-a', stream=True)
        return [item async for item in mobile._stream_deerflow_mobile_run(req)]

    events = asyncio.run(collect())
    done = json.loads(next(event for event in events if event.startswith('event: done\n')).split('data: ', 1)[1])
    assert done['status'] == expected
    assert (done['code'] == 0) == (expected == 'awaiting_clarification')
    assert any(event.startswith('event: clarification\n') for event in events) == (expected == 'awaiting_clarification')


def test_graph_error_takes_precedence_over_pending_clarification(monkeypatch):
    async def produce(request, queue):
        await queue.put({'kind': 'start', 'thread_id': 'thread-a'})
        await queue.put({'kind': 'chunk', 'raw_event': {'event': 'error', 'data': {'error': 'SyntheticFailure', 'message': 'Synthetic error'}}})
        await queue.put({'kind': 'done', 'run_status': 'success'})

    monkeypatch.setattr(mobile, '_produce_langgraph_stream', produce)
    monkeypatch.setattr(mobile, '_extract_clarification_from_event', lambda *args, **kwargs: {'type': 'clarification', 'content': '请补充范围'})

    async def collect():
        req = mobile.DeerFlowMobileRunRequest(query='synthetic', conversation_id='dialog-a', stream=True)
        return [item async for item in mobile._stream_deerflow_mobile_run(req)]

    done = next(event for event in asyncio.run(collect()) if event.startswith('event: done\n'))
    assert json.loads(done.split('data: ', 1)[1])['status'] == 'failed'
