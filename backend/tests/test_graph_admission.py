"""Admission must bound native runs without holding a lock across SSE output."""

import asyncio
import json
from unittest.mock import AsyncMock

import httpx
import pytest

from app import graph_http
from app.admission_claims import build_admission_metadata


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


def _signed_metadata(monkeypatch, *, user_id, thread_id, identity_class="ordinary_user", task_type="analysis"):
    monkeypatch.setenv("DEER_FLOW_ADMISSION_SIGNING_KEY", "test-signing-key-with-at-least-32-bytes")
    return build_admission_metadata(
        user_id=user_id,
        org_id="org-a",
        identity_class=identity_class,
        task_type=task_type,
        thread_id=thread_id,
    )


def test_user_limit_keeps_another_authenticated_user_admissible(monkeypatch):
    async def scenario():
        existing = _signed_metadata(monkeypatch, user_id="user-a", thread_id="existing-thread")
        monkeypatch.setattr(graph_http, "unfinished_run_count", AsyncMock(return_value=1))
        monkeypatch.setattr(
            graph_http,
            "unfinished_run_records",
            AsyncMock(return_value=[{"status": "running", "thread_id": "existing-thread", "metadata": existing}]),
        )
        async def native(scope, receive, send):
            await send({"type": "http.response.start", "status": 201, "headers": []})
            await send({"type": "http.response.body", "body": b"created"})

        app = graph_http.RunAdmissionMiddleware(native, max_inflight=10, max_per_user=1)

        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
            denied = await client.post(
                "/threads/a-thread/runs/stream",
                json={"metadata": _signed_metadata(monkeypatch, user_id="user-a", thread_id="a-thread")},
            )
            admitted = await client.post(
                "/threads/b-thread/runs/stream",
                json={"metadata": _signed_metadata(monkeypatch, user_id="user-b", thread_id="b-thread")},
            )

        assert denied.status_code == 429
        assert admitted.status_code == 201

    asyncio.run(scenario())


def test_task_type_limits_apply_across_users(monkeypatch):
    async def scenario():
        active = _signed_metadata(
            monkeypatch,
            user_id="user-a",
            thread_id="existing-thread",
            identity_class="ordinary_user",
            task_type="analysis",
        )
        monkeypatch.setattr(graph_http, "unfinished_run_count", AsyncMock(return_value=1))
        monkeypatch.setattr(
            graph_http,
            "unfinished_run_records",
            AsyncMock(return_value=[{"status": "pending", "thread_id": "existing-thread", "metadata": active}]),
        )

        async def native(scope, receive, send):
            await send({"type": "http.response.start", "status": 201, "headers": []})
            await send({"type": "http.response.body", "body": b"created"})

        app = graph_http.RunAdmissionMiddleware(
            native,
            max_inflight=10,
            max_per_user=3,
            task_type_limits={"analysis": 1},
        )
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
            same_task = await client.post(
                "/threads/analysis-thread/runs",
                json={
                    "metadata": _signed_metadata(
                        monkeypatch,
                        user_id="user-b",
                        thread_id="analysis-thread",
                        identity_class="ordinary_user",
                        task_type="analysis",
                    )
                },
            )
            other_task = await client.post(
                "/threads/research-thread/runs",
                json={
                    "metadata": _signed_metadata(
                        monkeypatch,
                        user_id="user-b",
                        thread_id="research-thread",
                        identity_class="ordinary_user",
                        task_type="research",
                    )
                },
            )
        assert same_task.status_code == 429
        assert other_task.status_code == 201

    asyncio.run(scenario())


def test_identity_class_limits_use_compliance_defined_profiles(monkeypatch):
    async def scenario():
        active = _signed_metadata(
            monkeypatch,
            user_id="user-a",
            thread_id="existing-thread",
            identity_class="ordinary_user",
            task_type="analysis",
        )
        monkeypatch.setattr(graph_http, "unfinished_run_count", AsyncMock(return_value=1))
        monkeypatch.setattr(
            graph_http,
            "unfinished_run_records",
            AsyncMock(return_value=[{"status": "running", "thread_id": "existing-thread", "metadata": active}]),
        )

        async def native(scope, receive, send):
            await send({"type": "http.response.start", "status": 201, "headers": []})
            await send({"type": "http.response.body", "body": b"created"})

        app = graph_http.RunAdmissionMiddleware(
            native,
            max_inflight=10,
            max_per_user=3,
            identity_class_limits={"ordinary_user": 1},
        )
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
            same_class = await client.post(
                "/threads/user-b-thread/runs",
                json={
                    "metadata": _signed_metadata(
                        monkeypatch,
                        user_id="user-b",
                        thread_id="user-b-thread",
                        identity_class="ordinary_user",
                        task_type="research",
                    )
                },
            )
            other_class = await client.post(
                "/threads/operator-thread/runs",
                json={
                    "metadata": _signed_metadata(
                        monkeypatch,
                        user_id="user-c",
                        thread_id="operator-thread",
                        identity_class="internal_operator",
                        task_type="research",
                    )
                },
            )

        assert same_class.status_code == 429
        assert other_class.status_code == 201

    asyncio.run(scenario())


def test_invalid_signed_claim_is_rejected_instead_of_trusted_or_unscoped(monkeypatch):
    async def scenario():
        metadata = _signed_metadata(monkeypatch, user_id="user-a", thread_id="a-thread")
        metadata["deerflow_admission"]["user_key"] = "0" * 64
        monkeypatch.setattr(graph_http, "unfinished_run_count", AsyncMock(return_value=0))
        native = AsyncMock()
        app = graph_http.RunAdmissionMiddleware(native, max_inflight=10)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
            response = await client.post("/threads/a-thread/runs", json={"metadata": metadata})
        assert response.status_code == 403
        native.assert_not_awaited()

    asyncio.run(scenario())


def test_required_signed_admission_blocks_direct_unattributed_runs(monkeypatch):
    async def scenario():
        monkeypatch.setattr(graph_http, "unfinished_run_count", AsyncMock(return_value=0))
        monkeypatch.setattr(graph_http, "unfinished_run_records", AsyncMock(return_value=[]))

        async def native(scope, receive, send):
            await send({"type": "http.response.start", "status": 201, "headers": []})
            await send({"type": "http.response.body", "body": b"created"})

        app = graph_http.RunAdmissionMiddleware(native, max_inflight=10, require_signed_admission=True)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
            unsigned = await client.post(
                "/threads/unsigned-thread/runs",
                json={"metadata": {"user_id": "caller-claimed-user", "identity_class": "publisher"}},
            )
            signed = await client.post(
                "/threads/signed-thread/runs",
                json={
                    "metadata": _signed_metadata(
                        monkeypatch,
                        user_id="verified-user",
                        thread_id="signed-thread",
                    )
                },
            )

        assert unsigned.status_code == 403
        assert signed.status_code == 201

    asyncio.run(scenario())


def test_batch_applies_user_limit_to_all_signed_runs(monkeypatch):
    async def scenario():
        monkeypatch.setattr(graph_http, "unfinished_run_count", AsyncMock(return_value=0))
        monkeypatch.setattr(graph_http, "unfinished_run_records", AsyncMock(return_value=[]))

        async def native(scope, receive, send):
            await send({"type": "http.response.start", "status": 201, "headers": []})
            await send({"type": "http.response.body", "body": b"created"})

        app = graph_http.RunAdmissionMiddleware(native, max_inflight=10, max_per_user=1)
        runs = [
            {
                "thread_id": f"thread-{index}",
                "metadata": _signed_metadata(monkeypatch, user_id="same-user", thread_id=f"thread-{index}"),
            }
            for index in (1, 2)
        ]
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
            response = await client.post("/runs/batch", json=runs)
        assert response.status_code == 429

    asyncio.run(scenario())
