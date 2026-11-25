"""Data-centre ownership boundaries and shared-catalogue concurrency."""

import asyncio
import json
import multiprocessing
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI, HTTPException

from app.gateway.data_source_store import DataSourceStore
from app.gateway.mobile_iam import MobileIAMPrincipal
from app.gateway.routers import data_center
from deerflow.config.paths import Paths


@pytest.fixture
def api(tmp_path, monkeypatch):
    paths = Paths(tmp_path)
    monkeypatch.setattr(data_center, "get_paths", lambda: paths)
    monkeypatch.setenv("DATA_CENTER_ORG_SHARED_SOURCES", "{}")
    monkeypatch.setenv("DATA_CENTER_ORG_ES_INDICES", "{}")
    monkeypatch.setenv("DATA_CENTER_ES_INDEX_ALLOWLIST", "")
    thread_checks = []

    async def authenticate(request):
        identity = request.headers.get("authorization", "")
        if not identity:
            raise HTTPException(status_code=401, detail="Authentication required")
        if identity == "development":
            return None, None
        org, user, permission = identity.split(":")
        permissions = ["data_center:*"] if permission == "all" else [f"data_center:{permission}"]
        return object(), MobileIAMPrincipal(
            user_id=user, org_id=org, identity_class="ordinary_user", permissions=permissions,
        )

    async def thread_access(request, thread_id, *, operation_type):
        thread_checks.append((thread_id, operation_type))
        if thread_id.startswith("denied"):
            raise HTTPException(status_code=404, detail="Thread not found")

    monkeypatch.setattr(data_center, "authenticate_mobile_request", authenticate)
    monkeypatch.setattr(data_center, "require_thread_access", thread_access)
    monkeypatch.setattr(data_center, "_scan_uploads_for_compliance", lambda *args: None)
    app = FastAPI()
    app.include_router(data_center.router)
    return app, paths, thread_checks


def run(coroutine):
    return asyncio.run(coroutine)


async def call(api, method, route, *, identity="org-a:alice:all", **kwargs):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=api[0]), base_url="http://test") as client:
        return await client.request(method, f"/api/data-center{route}", headers={"Authorization": identity}, **kwargs)


async def upload(api, identity="org-a:alice:all", content=b"alice"):
    response = await call(api, "POST", "/sources/upload", identity=identity, files={"files": ("same.csv", content)})
    assert response.status_code == 200, response.text
    return response.json()["sources"][0]


def test_authenticated_sources_are_filtered_and_not_accessible_across_users_or_organizations(api):
    async def scenario():
        source = await upload(api)
        await upload(api, "org-a:bob:all", b"bob")
        await upload(api, "org-b:alice:all", b"other organization")
        listed = await call(api, "GET", "/sources")
        assert [item["id"] for item in listed.json()["sources"]] == [source["id"]]
        for stranger in ("org-a:bob:all", "org-b:alice:all"):
            for method, suffix in (("GET", ""), ("GET", "/download"), ("DELETE", "")):
                response = await call(api, method, f"/sources/{source['id']}{suffix}", identity=stranger)
                assert response.status_code == 404
            response = await call(api, "POST", "/sources/attach-to-thread", identity=stranger,
                                  json={"thread_id": "target", "source_ids": [source["id"]]})
            assert response.status_code == 404
        downloaded = await call(api, "GET", f"/sources/{source['id']}/download")
        assert downloaded.content == b"alice"
    run(scenario())


def test_every_data_center_route_requires_authentication(api):
    async def scenario():
        for method, route, body in (
            ("GET", "/sources", None), ("GET", "/sources/unknown", None),
            ("GET", "/sources/unknown/download", None), ("DELETE", "/sources/unknown", None),
            ("POST", "/sources/register-upload", {"thread_id": "thread", "filename": "x.csv"}),
            ("POST", "/sources/attach-to-thread", {"thread_id": "thread", "source_ids": []}),
            ("GET", "/es/health", None), ("GET", "/es/indices", None),
            ("GET", "/es/indices/private", None), ("GET", "/es/indices/private/samples", None),
        ):
            response = await call(api, method, route, identity="", **({"json": body} if body else {}))
            assert response.status_code == 401, (method, route, response.text)
        response = await call(api, "POST", "/sources/upload", identity="", files={"files": ("x.csv", b"x")})
        assert response.status_code == 401
    run(scenario())


@pytest.mark.parametrize("method,route,body", [
    ("GET", "/sources", None), ("GET", "/sources/unknown", None),
    ("GET", "/sources/unknown/download", None), ("DELETE", "/sources/unknown", None),
    ("POST", "/sources/register-upload", {"thread_id": "thread", "filename": "x.csv"}),
    ("POST", "/sources/attach-to-thread", {"thread_id": "thread", "source_ids": []}),
    ("GET", "/es/health", None), ("GET", "/es/indices", None),
    ("GET", "/es/indices/private", None), ("GET", "/es/indices/private/samples", None),
])
def test_operations_require_explicit_trusted_permission(api, method, route, body):
    response = run(call(api, method, route, identity="org-a:alice:none", **({"json": body} if body else {})))
    assert response.status_code == 403


def test_upload_permission_cannot_be_supplied_in_the_request(api):
    response = run(call(api, "POST", "/sources/upload", identity="org-a:alice:read",
                        files={"files": ("x.csv", b"x")}, data={"permissions": "data_center:*", "owner_user_id": "bob"}))
    assert response.status_code == 403
    assert not (api[1].base_dir / "data-center" / "uploads").exists()


def test_legacy_json_is_imported_once_without_adopting_ownership(api):
    legacy = api[1].base_dir / "data-center" / "sources.json"
    legacy.parent.mkdir()
    legacy.write_text(json.dumps([{
        "id": "legacy", "name": "Legacy", "type": "uploaded_file", "status": "ready",
        "owner_user_id": "alice", "owner_org_id": "org-a",
    }]))
    assert run(call(api, "GET", "/sources")).json()["sources"] == []
    legacy.write_text("[]")
    response = run(call(api, "GET", "/sources", identity="development"))
    assert [source["id"] for source in response.json()["sources"]] == ["legacy"]
    assert response.json()["sources"][0]["owner_user_id"] is None


def test_explicit_registration_cannot_adopt_a_legacy_unowned_file(api):
    folder = api[1].sandbox_uploads_dir("owned")
    folder.mkdir(parents=True)
    file_path = folder / "legacy.csv"
    file_path.write_bytes(b"legacy")
    legacy = api[1].base_dir / "data-center" / "sources.json"
    legacy.parent.mkdir()
    legacy.write_text(json.dumps([{
        "id": "legacy", "name": "Legacy", "type": "uploaded_file", "status": "ready",
        "thread_id": "owned", "path": str(file_path),
    }]))
    response = run(call(api, "POST", "/sources/register-upload", json={"thread_id": "owned", "filename": "legacy.csv"}))
    assert response.status_code == 404
    assert run(call(api, "GET", "/sources")).json()["count"] == 0


def test_organization_sharing_is_trusted_explicit_and_cannot_cross_organizations(api, monkeypatch):
    source = run(upload(api))
    monkeypatch.setenv("DATA_CENTER_ORG_SHARED_SOURCES", json.dumps({"org-a": [source["id"]], "org-b": [source["id"]]}))
    shared = run(call(api, "GET", "/sources", identity="org-a:bob:read"))
    assert shared.json()["count"] == 1
    assert run(call(api, "GET", "/sources", identity="org-b:alice:read")).json()["count"] == 0
    assert run(call(api, "DELETE", f"/sources/{source['id']}", identity="org-a:bob:all")).status_code == 404
    assert run(call(api, "GET", f"/sources/{source['id']}/download", identity="org-a:bob:read")).status_code == 403
    assert run(call(api, "GET", f"/sources/{source['id']}/download", identity="org-a:bob:export")).content == b"alice"


def test_organization_share_configuration_cannot_adopt_unowned_resources(api, monkeypatch):
    source = run(upload(api, "development"))
    monkeypatch.setenv("DATA_CENTER_ORG_SHARED_SOURCES", json.dumps({"org-a": [source["id"]]}))
    assert run(call(api, "GET", "/sources")).json()["count"] == 0
    assert run(call(api, "GET", f"/sources/{source['id']}/download")).status_code == 404


@pytest.mark.parametrize("filename", ["../secret.csv", "/tmp/secret.csv", "sub/secret.csv", "..\\secret.csv"])
def test_registration_rejects_paths_even_for_an_owned_thread(api, filename):
    response = run(call(api, "POST", "/sources/register-upload", json={"thread_id": "owned", "filename": filename}))
    assert response.status_code == 400


def test_registration_rejects_symlink_and_requires_thread_authorization(api):
    uploads = api[1].sandbox_uploads_dir("owned")
    uploads.mkdir(parents=True)
    secret = api[1].base_dir / "private.csv"
    secret.write_bytes(b"secret")
    (uploads / "link.csv").symlink_to(secret)
    response = run(call(api, "POST", "/sources/register-upload", json={"thread_id": "owned", "filename": "link.csv"}))
    assert response.status_code == 400
    denied = run(call(api, "POST", "/sources/register-upload", json={"thread_id": "denied-thread", "filename": "x.csv"}))
    assert denied.status_code == 404


def test_mount_authorizes_all_sources_and_target_before_writing(api, monkeypatch):
    async def scenario():
        own = await upload(api)
        other = await upload(api, "org-a:bob:all")
        save = AsyncMock()
        monkeypatch.setattr(data_center, "save_thread_upload_from_bytes", save)
        response = await call(api, "POST", "/sources/attach-to-thread", json={"thread_id": "owned", "source_ids": [own["id"], other["id"]]})
        assert response.status_code == 404
        save.assert_not_called()
        response = await call(api, "POST", "/sources/attach-to-thread", json={"thread_id": "denied-target", "source_ids": [own["id"]]})
        assert response.status_code == 404
        save.assert_not_called()
    run(scenario())


def test_thread_source_mount_checks_source_export_and_target_upload(api, monkeypatch):
    async def scenario():
        folder = api[1].sandbox_uploads_dir("source-thread")
        folder.mkdir(parents=True)
        (folder / "data.csv").write_bytes(b"thread data")
        registered = await call(api, "POST", "/sources/register-upload", json={"thread_id": "source-thread", "filename": "data.csv"})
        assert registered.status_code == 200
        save = AsyncMock(return_value={"filename": "data.csv"})
        monkeypatch.setattr(data_center, "save_thread_upload_from_bytes", save)
        api[2].clear()
        attached = await call(api, "POST", "/sources/attach-to-thread", json={"thread_id": "target-thread", "source_ids": [registered.json()["id"]]})
        assert attached.status_code == 200
        assert api[2] == [("target-thread", "upload"), ("source-thread", "export")]
        assert save.await_args.kwargs["content"] == b"thread data"
    run(scenario())


def test_mount_preserves_same_name_source_files_and_existing_thread_file(api, monkeypatch):
    async def scenario():
        first = await upload(api, content=b"first")
        second = await upload(api, content=b"second")
        target = api[1].sandbox_uploads_dir("target")
        target.mkdir(parents=True)
        (target / "same.csv").write_bytes(b"original")

        async def save(**kwargs):
            destination = target / kwargs["filename"]
            destination.write_bytes(kwargs["content"])
            return {"filename": kwargs["filename"]}

        monkeypatch.setattr(data_center, "save_thread_upload_from_bytes", save)
        response = await call(api, "POST", "/sources/attach-to-thread", json={"thread_id": "target", "source_ids": [first["id"], second["id"]]})
        assert response.status_code == 200
        assert [file["filename"] for file in response.json()["files"]] == ["same_1.csv", "same_2.csv"]
        assert (target / "same.csv").read_bytes() == b"original"
        assert (target / "same_1.csv").read_bytes() == b"first"
        assert (target / "same_2.csv").read_bytes() == b"second"
    run(scenario())


def test_es_requires_explicit_organization_index_grant(api, monkeypatch):
    es_get = AsyncMock(return_value=[{"index": "allowed", "docs.count": "1"}, {"index": "private", "docs.count": "9"}])
    monkeypatch.setattr(data_center, "_es_get", es_get)
    assert run(call(api, "GET", "/es/indices")).status_code == 403
    es_get.assert_not_called()
    monkeypatch.setenv("DATA_CENTER_ORG_ES_INDICES", '{"org-a":["allowed"]}')
    response = run(call(api, "GET", "/es/indices"))
    assert [index["name"] for index in response.json()["items"]] == ["allowed"]
    assert run(call(api, "GET", "/es/indices/private")).status_code == 403
    assert run(call(api, "GET", "/es/indices/private/samples")).status_code == 403
    assert run(call(api, "GET", "/es/indices", identity="org-b:alice:all")).status_code == 403


def test_same_name_concurrent_uploads_preserve_records_bytes_and_owners(api):
    async def scenario():
        sources = await asyncio.gather(*[upload(api, content=f"row-{index}".encode()) for index in range(20)])
        assert len({source["path"] for source in sources}) == 20
        records = (await call(api, "GET", "/sources")).json()["sources"]
        assert len(records) == 20
        for index, source in enumerate(sources):
            response = await call(api, "GET", f"/sources/{source['id']}/download")
            assert response.content == f"row-{index}".encode()
    run(scenario())


def test_upload_registration_failure_removes_all_written_files(api, monkeypatch):
    def fail(*args):
        raise sqlite3.OperationalError("injected failure")
    monkeypatch.setattr(DataSourceStore, "insert_many", fail)
    with pytest.raises(sqlite3.OperationalError):
        run(call(api, "POST", "/sources/upload", files=[("files", ("one.csv", b"one")), ("files", ("two.csv", b"two"))]))
    assert not list((api[1].base_dir / "data-center" / "uploads").rglob("*.csv"))


def test_delete_transaction_failure_restores_bytes_and_registration(api, monkeypatch):
    source = run(upload(api))
    original = DataSourceStore.deleting

    @contextmanager
    def fail_commit(self, source_id):
        with original(self, source_id) as record:
            yield record
            raise sqlite3.OperationalError("injected failure")

    monkeypatch.setattr(DataSourceStore, "deleting", fail_commit)
    with pytest.raises(sqlite3.OperationalError):
        run(call(api, "DELETE", f"/sources/{source['id']}"))
    assert Path(source["path"]).read_bytes() == b"alice"
    assert run(call(api, "GET", f"/sources/{source['id']}/download")).content == b"alice"


def test_development_upload_download_and_delete_remain_available_without_iam(api):
    source = run(upload(api, "development", b"development"))
    assert run(call(api, "GET", f"/sources/{source['id']}/download", identity="development")).content == b"development"
    removed = run(call(api, "DELETE", f"/sources/{source['id']}", identity="development"))
    assert removed.status_code == 200
    assert removed.json()["file_deleted"] is True
    assert not Path(source["path"]).exists()
    assert run(call(api, "GET", "/sources", identity="development")).json()["count"] == 0


def test_async_elasticsearch_request_does_not_block_independent_requests(api, monkeypatch):
    async def scenario():
        started = asyncio.Event()
        release = asyncio.Event()

        class Response:
            def raise_for_status(self):
                pass

            def json(self):
                return {"status": "green"}

        class ESClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def get(self, *args, **kwargs):
                started.set()
                await release.wait()
                return Response()

        # Patch only the ES factory; the ASGI client is constructed first.
        client = httpx.AsyncClient(transport=httpx.ASGITransport(app=api[0]), base_url="http://test")
        monkeypatch.setenv("DATA_CENTER_ES_URL", "http://isolated-es")
        monkeypatch.setattr(data_center.httpx, "AsyncClient", lambda *args, **kwargs: ESClient())
        async with client:
            health = asyncio.create_task(client.get("/api/data-center/es/health", headers={"Authorization": "development"}))
            await asyncio.wait_for(started.wait(), 1)
            listing = await asyncio.wait_for(client.get("/api/data-center/sources", headers={"Authorization": "development"}), 1)
            assert listing.status_code == 200
            release.set()
            assert (await health).status_code == 200
    run(scenario())


def _writer(database, prefix, count):
    store = DataSourceStore(Path(database))
    for index in range(count):
        store.insert_many([{"id": f"{prefix}-{index}", "name": "same"}])


def test_sqlite_catalogue_preserves_multi_process_and_multi_thread_inserts(tmp_path):
    database = tmp_path / "gateway.sqlite3"
    DataSourceStore(database)
    context = multiprocessing.get_context("fork")
    processes = [context.Process(target=_writer, args=(str(database), f"process-{index}", 15)) for index in range(3)]
    for process in processes:
        process.start()
    with ThreadPoolExecutor(max_workers=3) as executor:
        list(executor.map(lambda index: _writer(str(database), f"thread-{index}", 15), range(3)))
    for process in processes:
        process.join(timeout=15)
        assert process.exitcode == 0
    assert len(DataSourceStore(database).list()) == 90


def test_concurrent_thread_file_registration_deduplicates_atomically(tmp_path):
    database = tmp_path / "gateway.sqlite3"
    store = DataSourceStore(database)
    with ThreadPoolExecutor(max_workers=8) as executor:
        records = list(executor.map(lambda index: store.register_file({"id": f"source-{index}"}, "/owned/data.csv"), range(20)))
    assert len({record["id"] for record in records}) == 1
    assert len(store.list()) == 1
