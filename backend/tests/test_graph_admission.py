"""Admission must bound native runs without holding a lock across SSE output."""

import asyncio
import json
from unittest.mock import AsyncMock

import httpx
import pytest

from app import graph_http


def test_parallel_creation_is_bounded_but_streams_do_not_serialize(monkeypatch):
    async def scenario():
        unfinished = 0
        started = 0
        two_started = asyncio.Event()
        finish = asyncio.Event()

        async def count():
            return unfinished

        async def native(scope, receive, send):
            nonlocal unfinished, started
            await receive()
            # Yield during creation to expose a check-then-create race.
            await asyncio.sleep(0)
            unfinished += 1
            await send({"type": "http.response.start", "status": 200, "headers": []})
            started += 1
            if started == 2:
                two_started.set()
            await finish.wait()
            unfinished -= 1
            await send({"type": "http.response.body", "body": b"done"})

        monkeypatch.setattr(graph_http, "unfinished_run_count", count)
        app = graph_http.RunAdmissionMiddleware(native, max_inflight=2)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
            first = asyncio.create_task(client.post("/threads/a/runs/stream", json={}))
            second = asyncio.create_task(client.post("/threads/b/runs/stream", json={}))
            try:
                await asyncio.wait_for(two_started.wait(), timeout=2)
                full = await client.post("/threads/c/runs/stream", json={})
                assert full.status_code == 429
                assert full.headers["retry-after"] == "3"
                assert unfinished == 2
            finally:
                finish.set()
                responses = await asyncio.gather(first, second)
            assert [r.text for r in responses] == ["done", "done"]
            assert (await client.post("/threads/c/runs/stream", json={})).status_code == 200

    asyncio.run(scenario())


@pytest.mark.parametrize("path", ["/runs", "/runs/stream", "/runs/wait", "/threads/t/runs", "/threads/t/runs/stream", "/threads/t/runs/wait", "/threads/t/runs/"])
def test_existing_background_runs_count_even_without_connections(monkeypatch, path):
    async def scenario():
        native = AsyncMock()
        monkeypatch.setattr(graph_http, "unfinished_run_count", AsyncMock(return_value=10))
        app = graph_http.RunAdmissionMiddleware(native, max_inflight=10)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
            response = await client.post(path, json={})
        assert response.status_code == 429
        native.assert_not_awaited()

    asyncio.run(scenario())


def test_batch_reserves_all_runs_and_replays_unchanged_body(monkeypatch):
    async def scenario():
        bodies = []

        async def native(scope, receive, send):
            bodies.append((await receive())["body"])
            await send({"type": "http.response.start", "status": 201, "headers": []})
            await send({"type": "http.response.body", "body": b"[]"})

        monkeypatch.setattr(graph_http, "unfinished_run_count", AsyncMock(return_value=2))
        app = graph_http.RunAdmissionMiddleware(native, max_inflight=4)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
            payload = json.dumps([{"input": "甲"}, {"input": "乙"}], ensure_ascii=False).encode()
            assert (await client.post("/runs/batch", content=payload)).status_code == 201
            assert bodies == [payload]
            assert (await client.post("/runs/batch", json=[{}, {}, {}])).status_code == 429
            # Native validation retains responsibility for malformed requests.
            assert (await client.post("/runs/batch", content=b"invalid")).status_code == 201
            assert bodies[-1] == b"invalid"

    asyncio.run(scenario())


@pytest.mark.parametrize("method,path", [("GET", "/threads/t/runs/r/stream"), ("GET", "/threads/t/runs/r"), ("POST", "/threads/t/runs/r/cancel"), ("POST", "/runs/cancel"), ("POST", "/threads/search"), ("DELETE", "/threads/t"), ("GET", "/ok")])
def test_full_queue_does_not_block_reads_cancel_or_stream_reconnect(monkeypatch, method, path):
    async def scenario():
        count = AsyncMock(side_effect=AssertionError("must not check capacity"))
        monkeypatch.setattr(graph_http, "unfinished_run_count", count)

        async def native(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

        app = graph_http.RunAdmissionMiddleware(native, max_inflight=1)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
            assert (await client.request(method, path)).text == "ok"
        count.assert_not_awaited()

    asyncio.run(scenario())


def test_native_failure_and_queue_read_failure_release_creation_lock(monkeypatch):
    async def scenario():
        calls = 0

        async def native(scope, receive, send):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise ValueError("invalid input")
            await send({"type": "http.response.start", "status": 201, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

        count = AsyncMock(side_effect=[RuntimeError("unavailable"), 0, 0])
        monkeypatch.setattr(graph_http, "unfinished_run_count", count)
        app = graph_http.RunAdmissionMiddleware(native, max_inflight=2)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
            assert (await client.post("/runs", json={})).status_code == 503
            with pytest.raises(ValueError, match="invalid input"):
                await client.post("/runs", json={})
            assert (await asyncio.wait_for(client.post("/runs", json={}), timeout=2)).status_code == 201

    asyncio.run(scenario())


def test_cancelled_creation_releases_lock(monkeypatch):
    async def scenario():
        entered = asyncio.Event()
        pause = asyncio.Event()

        async def count():
            entered.set()
            await pause.wait()
            return 0

        async def native(scope, receive, send):
            await send({"type": "http.response.start", "status": 201, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

        monkeypatch.setattr(graph_http, "unfinished_run_count", count)
        app = graph_http.RunAdmissionMiddleware(native, max_inflight=2)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
            task = asyncio.create_task(client.post("/runs", json={}))
            await entered.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            pause.set()
            assert (await asyncio.wait_for(client.post("/runs", json={}), timeout=2)).status_code == 201

    asyncio.run(scenario())


@pytest.mark.parametrize("value", ["0", "-1", "invalid"])
def test_invalid_limit_is_rejected(monkeypatch, value):
    monkeypatch.setenv("DEER_FLOW_MAX_INFLIGHT_RUNS", value)
    with pytest.raises(ValueError):
        graph_http.RunAdmissionMiddleware(AsyncMock())
