from __future__ import annotations

import asyncio
import os
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import langgraph_sdk
import pytest
from fastapi import HTTPException
from starlette.requests import Request

from app.gateway import conversation_store, thread_access
from app.gateway.mobile_iam import MobileIAMPrincipal
from app.gateway.routers import mobile


class _StatusError(Exception):
    def __init__(self, status_code):
        self.status_code = status_code


def _request():
    return Request({"type": "http", "headers": []})


@pytest.fixture
def store_home(monkeypatch, tmp_path):
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path))
    return tmp_path


@pytest.fixture
def threads(monkeypatch, store_home):
    class Threads:
        records = {}
        create_calls = 0
        client_headers = []

        async def get(self, thread_id):
            await asyncio.sleep(0)
            if thread_id not in self.records:
                raise _StatusError(404)
            return self.records[thread_id]

        async def create(self, *, thread_id, metadata, if_exists):
            await asyncio.sleep(0)
            if thread_id in self.records:
                raise _StatusError(409)
            self.create_calls += 1
            self.records[thread_id] = {"thread_id": thread_id, "metadata": metadata}
            return self.records[thread_id]

    backend = Threads()

    def get_client(*, url, headers):
        backend.client_headers.append(headers)
        return SimpleNamespace(threads=backend)

    monkeypatch.setattr(langgraph_sdk, "get_client", get_client)
    monkeypatch.setattr(
        mobile, "internal_graph_headers", lambda: {"X-Internal-Test": "verified"}
    )
    monkeypatch.setattr(
        thread_access, "internal_graph_headers", lambda: {"X-Internal-Test": "verified"}
    )
    return backend


def test_sqlite_reservation_is_atomic_durable_and_tenant_scoped(store_home):
    def reserve(_):
        return conversation_store.get_or_create_conversation_thread(
            org_id="org-a", user_id="same-user", conversation_id="same-dialog"
        )

    with ThreadPoolExecutor(max_workers=12) as pool:
        ids = list(pool.map(reserve, range(32)))
    assert len(set(ids)) == 1
    assert reserve(None) == ids[0]  # A fresh connection recovers persisted state.
    other_org = conversation_store.get_or_create_conversation_thread(
        org_id="org-b", user_id="same-user", conversation_id="same-dialog"
    )
    other_user = conversation_store.get_or_create_conversation_thread(
        org_id="org-a", user_id="other-user", conversation_id="same-dialog"
    )
    development = conversation_store.get_or_create_conversation_thread(
        org_id="", user_id="same-user", conversation_id="same-dialog"
    )
    assert len({ids[0], other_org, other_user, development}) == 4
    with sqlite3.connect(store_home / "gateway.sqlite3") as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM mobile_conversations"
        ).fetchone() == (4,)
        connection.execute(
            "UPDATE mobile_conversations SET thread_id = 'persisted-thread' WHERE org_id = 'org-a' AND user_id = 'same-user'"
        )
    recovered = subprocess.run(
        [
            sys.executable,
            "-c",
            "from app.gateway.conversation_store import get_or_create_conversation_thread; print(get_or_create_conversation_thread(org_id='org-a', user_id='same-user', conversation_id='same-dialog'))",
        ],
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        check=True,
    )
    assert recovered.stdout.strip() == "persisted-thread"


def test_concurrent_thread_creation_reuses_one_thread_and_recovers_runtime_restart(
    threads,
):
    async def run():
        req = mobile.DeerFlowMobileEnsureThreadRequest(conversation_id="dialog-1")
        results = await asyncio.gather(
            *(
                mobile.ensure_mobile_thread(req, org_id="org-a", user_id="user-a")
                for _ in range(8)
            )
        )
        assert len({result[0] for result in results}) == 1
        assert sum(created for _, created in results) == 1
        assert threads.create_calls == 1
        thread_id = results[0][0]
        assert await mobile.ensure_mobile_thread(
            req, org_id="org-a", user_id="user-a"
        ) == (thread_id, False)
        threads.records.clear()  # LangGraph's development runtime restarted.
        assert await mobile.ensure_mobile_thread(
            req, org_id="org-a", user_id="user-a"
        ) == (thread_id, True)
        assert threads.create_calls == 2

    asyncio.run(run())
    assert all(
        headers == {"X-Internal-Test": "verified"} for headers in threads.client_headers
    )


def test_verified_ownership_overrides_caller_metadata_and_splits_organizations(threads):
    req = mobile.DeerFlowMobileEnsureThreadRequest(
        conversation_id="same-dialog",
        user_id="forged-user",
        metadata={
            "org_id": "forged-org",
            "user_id": "forged-user",
            "owner_org_id": "forged-org",
            "tenant_id": "forged-org",
            "label": "keep",
        },
    )

    async def run():
        first, _ = await mobile.ensure_mobile_thread(
            req, org_id="org-a", user_id="same-user"
        )
        second, _ = await mobile.ensure_mobile_thread(
            req, org_id="org-b", user_id="same-user"
        )
        assert first != second
        metadata = threads.records[first]["metadata"]
        assert metadata == {
            "org_id": "org-a",
            "user_id": "same-user",
            "conversation_id": "same-dialog",
            "source": "trusted_upstream",
            "label": "keep",
        }
        threads.records[first]["metadata"]["org_id"] = "org-b"
        with pytest.raises(HTTPException) as denied:
            await mobile.ensure_mobile_thread(req, org_id="org-a", user_id="same-user")
        assert denied.value.status_code == 404

    asyncio.run(run())


def test_development_thread_cannot_forge_authenticated_organization(threads):
    request = mobile.DeerFlowMobileEnsureThreadRequest(
        conversation_id="dialog", user_id="user-a", metadata={"org_id": "org-a"}
    )
    thread_id, _ = asyncio.run(mobile.ensure_mobile_thread(request))
    assert threads.records[thread_id]["metadata"]["org_id"] == ""


def test_unavailable_sqlite_store_returns_503(threads, monkeypatch):
    def unavailable(**_kwargs):
        raise sqlite3.OperationalError("disk unavailable")

    monkeypatch.setattr(mobile, "get_or_create_conversation_thread", unavailable)
    with pytest.raises(HTTPException) as result:
        asyncio.run(
            mobile.ensure_mobile_thread(
                mobile.DeerFlowMobileEnsureThreadRequest(conversation_id="dialog"),
                org_id="org",
                user_id="user",
            )
        )
    assert result.value.status_code == 503
    assert threads.create_calls == 0


def test_development_explicit_thread_still_works_without_a_store(threads, monkeypatch):
    def unexpected_store(**_kwargs):
        raise AssertionError("Existing development thread should not be remapped")

    monkeypatch.setattr(mobile, "get_or_create_conversation_thread", unexpected_store)
    request = mobile.MobileChatRequest(
        query="hello", session_id="dialog", thread_id="legacy-thread"
    )
    assert (
        asyncio.run(
            mobile._resolve_mobile_thread(request, SimpleNamespace(threads=threads))
        )
        == "legacy-thread"
    )


@pytest.mark.parametrize("failure", [_StatusError(500), OSError("backend down")])
def test_thread_service_failures_are_503_not_404(threads, monkeypatch, failure):
    async def unavailable(_thread_id):
        raise failure

    monkeypatch.setattr(threads, "get", unavailable)
    with pytest.raises(HTTPException) as result:
        asyncio.run(
            mobile.ensure_mobile_thread(
                mobile.DeerFlowMobileEnsureThreadRequest(conversation_id="dialog"),
                org_id="org",
                user_id="user",
            )
        )
    assert result.value.status_code == 503


def _install_identity(monkeypatch, *, org_id="org-a", user_id="user-a"):
    principal = MobileIAMPrincipal(
        org_id=org_id, user_id=user_id, identity_class="ordinary_user"
    )

    async def authenticate(_request):
        return object(), principal

    monkeypatch.setattr(thread_access, "authenticate_mobile_request", authenticate)
    return principal


def test_thread_access_checks_org_and_user_before_iam_authorization(
    threads, monkeypatch
):
    _install_identity(monkeypatch)
    calls = []

    async def authorize(_provider, principal, authorization):
        calls.append((principal, authorization))

    monkeypatch.setattr(thread_access, "authorize_mobile_request", authorize)
    threads.records["thread"] = {
        "metadata": {
            "org_id": "org-b",
            "user_id": "user-a",
            "conversation_id": "dialog",
        }
    }
    with pytest.raises(HTTPException) as denied:
        asyncio.run(
            thread_access.require_thread_access(
                _request(), "thread", operation_type="analysis"
            )
        )
    assert denied.value.status_code == 404
    assert calls == []
    threads.records["thread"]["metadata"].update(org_id="org-a", user_id="user-b")
    with pytest.raises(HTTPException) as denied:
        asyncio.run(
            thread_access.require_thread_access(
                _request(), "thread", operation_type="analysis"
            )
        )
    assert denied.value.status_code == 404


def test_thread_authorization_is_cached_only_within_one_request_and_operation(
    threads, monkeypatch
):
    _install_identity(monkeypatch)
    threads.records["thread"] = {
        "metadata": {
            "org_id": "org-a",
            "user_id": "user-a",
            "conversation_id": "dialog",
        }
    }
    calls = []

    async def authorize(_provider, _principal, authorization):
        calls.append(authorization)

    monkeypatch.setattr(thread_access, "authorize_mobile_request", authorize)

    async def run():
        request = _request()
        await thread_access.require_thread_access(
            request, "thread", operation_type="analysis"
        )
        await thread_access.require_thread_access(
            request, "thread", operation_type="analysis"
        )
        await thread_access.require_thread_access(
            request, "thread", operation_type="upload"
        )
        await thread_access.require_thread_access(
            _request(), "thread", operation_type="analysis"
        )

    asyncio.run(run())
    assert [call.operation_type for call in calls] == ["analysis", "upload", "analysis"]
    assert all(call.resource_id == "dialog" for call in calls)


@pytest.mark.parametrize(
    "failure,status",
    [
        (_StatusError(404), 404),
        (_StatusError(500), 503),
        (OSError("backend down"), 503),
    ],
)
def test_access_hides_missing_threads_but_reports_service_failures(
    threads, monkeypatch, failure, status
):
    _install_identity(monkeypatch)

    async def unavailable(_thread_id):
        raise failure

    monkeypatch.setattr(threads, "get", unavailable)
    with pytest.raises(HTTPException) as result:
        asyncio.run(
            thread_access.require_thread_access(
                _request(), "thread", operation_type="analysis"
            )
        )
    assert result.value.status_code == status


def test_missing_request_does_not_bypass_an_enabled_iam_provider(monkeypatch):
    seen = []

    async def authenticate(request):
        seen.append(request.headers.get("authorization"))
        raise HTTPException(status_code=401, detail="Authentication required")

    monkeypatch.setattr(thread_access, "authenticate_mobile_request", authenticate)
    with pytest.raises(HTTPException) as result:
        asyncio.run(
            thread_access.require_thread_access(
                None, "thread", operation_type="analysis"
            )
        )
    assert result.value.status_code == 401
    assert seen == [None]


def test_memory_scope_uses_only_verified_identity(monkeypatch):
    from deerflow.agents.memory.scope import memory_scope_from_context

    monkeypatch.setenv(
        "DEER_FLOW_ADMISSION_SIGNING_KEY", "test-signing-key-with-at-least-32-bytes"
    )
    request = mobile.MobileChatRequest(
        query="hello", compliance_identity={"org_id": "org-a", "user_id": "user-a"}
    )
    assert "memory_scope" not in mobile._build_mobile_run_context(request, "thread")
    request._iam_authenticated = True
    context = mobile._build_mobile_run_context(request, "thread")
    scope = memory_scope_from_context(context)
    assert (scope.org_id, scope.user_id) == ("org-a", "user-a")
    # The signature verifier must reject moving a user's scope to another thread.
    with pytest.raises(PermissionError):
        memory_scope_from_context({**context, "thread_id": "other-thread"})
