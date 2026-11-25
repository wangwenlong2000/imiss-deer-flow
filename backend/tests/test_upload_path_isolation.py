"""Reject symlink redirections and keep concurrent upload rollback isolated."""

import asyncio
import multiprocessing
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException, UploadFile
from langchain_core.messages import HumanMessage

from app.gateway.routers import uploads
from app.gateway.upload_transaction import thread_upload_transaction
from deerflow.agents.middlewares.uploads_middleware import UploadsMiddleware
from deerflow.config.paths import Paths


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    paths = Paths(tmp_path)
    monkeypatch.setattr(uploads, "get_paths", lambda: paths)
    monkeypatch.setattr(uploads, "require_thread_access", AsyncMock())
    # The transaction helper independently resolves Paths by design.
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path))
    provider = MagicMock()
    provider.acquire.return_value = "local"
    monkeypatch.setattr(uploads, "get_sandbox_provider", lambda: provider)
    monkeypatch.setattr(uploads, "_scan_uploads_for_compliance", lambda *args: None)
    return paths


def _file(name="file.txt", data=b"new"):
    return UploadFile(filename=name, file=BytesIO(data))


def test_upload_rejects_file_symlink_without_changing_other_thread(isolated):
    folder = isolated.sandbox_uploads_dir("alice")
    folder.mkdir(parents=True)
    private = isolated.sandbox_uploads_dir("bob")
    private.mkdir(parents=True)
    target = private / "file.txt"
    target.write_bytes(b"private")
    (folder / "file.txt").symlink_to(target)
    with pytest.raises(HTTPException) as caught:
        asyncio.run(uploads.upload_files("alice", files=[_file()]))
    assert caught.value.status_code == 403
    assert target.read_bytes() == b"private"


def test_upload_rejects_symlink_directory_before_creating_files(isolated):
    alice = isolated.sandbox_uploads_dir("alice")
    bob = isolated.sandbox_uploads_dir("bob")
    bob.mkdir(parents=True)
    alice.parent.mkdir(parents=True)
    alice.symlink_to(bob, target_is_directory=True)
    with pytest.raises(HTTPException) as caught:
        asyncio.run(uploads.upload_files("alice", files=[_file()]))
    assert caught.value.status_code == 403
    assert list(bob.iterdir()) == []


def test_converter_symlink_target_rejected_before_original_is_written(isolated, monkeypatch):
    folder = isolated.sandbox_uploads_dir("alice")
    folder.mkdir(parents=True)
    secret = isolated.base_dir / "secret.md"
    secret.write_bytes(b"private markdown")
    (folder / "report.md").symlink_to(secret)
    convert = AsyncMock()
    monkeypatch.setattr(uploads, "convert_file_to_markdown", convert)
    with pytest.raises(HTTPException) as caught:
        asyncio.run(uploads.upload_files("alice", files=[_file("report.pdf", b"pdf")]))
    assert caught.value.status_code == 403
    assert secret.read_bytes() == b"private markdown"
    assert not (folder / "report.pdf").exists()
    convert.assert_not_called()


def test_rollback_refuses_file_replaced_with_other_thread_symlink(isolated):
    folder = isolated.sandbox_uploads_dir("alice")
    folder.mkdir(parents=True)
    target = folder / "file.txt"
    target.write_bytes(b"original")
    snapshot = uploads._snapshot_artifact(target, "/mnt/user-data/uploads/file.txt")
    target.unlink()
    secret = isolated.base_dir / "secret.txt"
    secret.write_bytes(b"private")
    target.symlink_to(secret)
    uploads._rollback_uploads("alice", [snapshot])
    assert secret.read_bytes() == b"private"


def test_upload_list_omits_symlink_metadata(isolated):
    folder = isolated.sandbox_uploads_dir("alice")
    folder.mkdir(parents=True)
    secret = isolated.base_dir / "private.txt"
    secret.write_bytes(b"private")
    (folder / "link.txt").symlink_to(secret)
    (folder / "own.txt").write_bytes(b"own")
    response = asyncio.run(uploads.list_uploaded_files("alice"))
    assert [file["filename"] for file in response["files"]] == ["own.txt"]


@pytest.mark.parametrize("in_metadata", [False, True])
def test_middleware_omits_symlinks_for_new_and_historical_files(isolated, in_metadata):
    folder = isolated.sandbox_uploads_dir("alice")
    folder.mkdir(parents=True)
    secret = isolated.base_dir / "private.txt"
    secret.write_bytes(b"private")
    (folder / "link.txt").symlink_to(secret)
    message = HumanMessage(content="analyze", additional_kwargs={"files": [{"filename": "link.txt", "size": 7}]} if in_metadata else {})
    result = UploadsMiddleware(base_dir=str(isolated.base_dir)).before_agent(
        {"messages": [message]}, SimpleNamespace(context={"thread_id": "alice"}),
    )
    assert result is None


def test_middleware_omits_redirected_upload_directory(isolated):
    folder = isolated.sandbox_uploads_dir("alice")
    other = isolated.sandbox_uploads_dir("bob")
    other.mkdir(parents=True)
    (other / "private.txt").write_bytes(b"private")
    folder.parent.mkdir(parents=True)
    folder.symlink_to(other, target_is_directory=True)
    message = HumanMessage(content="analyze", additional_kwargs={"files": [{"filename": "private.txt", "size": 7}]})
    result = UploadsMiddleware(base_dir=str(isolated.base_dir)).before_agent(
        {"messages": [message]}, SimpleNamespace(context={"thread_id": "alice"}),
    )
    assert result is None


def test_same_thread_upload_waits_until_failed_upload_has_rolled_back(isolated, monkeypatch):
    async def scenario():
        first_entered = asyncio.Event()
        release_first = asyncio.Event()
        first_snapshot = True

        async def convert(path):
            nonlocal first_snapshot
            if first_snapshot:
                first_snapshot = False
                first_entered.set()
                await release_first.wait()
                raise RuntimeError("first conversion failed")
            markdown = path.with_suffix(".md")
            markdown.write_bytes(b"second markdown")
            return markdown

        monkeypatch.setattr(uploads, "convert_file_to_markdown", convert)
        first = asyncio.create_task(uploads.upload_files("alice", files=[_file("report.pdf", b"first")]))
        await asyncio.wait_for(first_entered.wait(), 1)
        second = asyncio.create_task(uploads.upload_files("alice", files=[_file("report.pdf", b"second")]))
        # A different thread remains runnable while the same-thread writer waits.
        different = await asyncio.wait_for(uploads.upload_files("bob", files=[_file("data.txt", b"other")]), 1)
        assert different.success
        await asyncio.sleep(0.04)
        assert not second.done()
        release_first.set()
        with pytest.raises(HTTPException):
            await first
        assert (await second).success
        assert (isolated.sandbox_uploads_dir("alice") / "report.pdf").read_bytes() == b"second"
        assert (isolated.sandbox_uploads_dir("alice") / "report.md").read_bytes() == b"second markdown"
    asyncio.run(scenario())


def _lock_worker(base_dir, started, release):
    async def hold():
        async with thread_upload_transaction("alice", base_dir=Path(base_dir)):
            started.set()
            while not release.is_set():
                await asyncio.sleep(0.01)
    asyncio.run(hold())


def test_upload_transaction_coordinates_processes_and_releases_cancelled_waiter(tmp_path):
    context = multiprocessing.get_context("fork")
    started, release = context.Event(), context.Event()
    process = context.Process(target=_lock_worker, args=(str(tmp_path), started, release))
    process.start()
    assert started.wait(3)

    async def scenario():
        entered = asyncio.Event()

        async def wait():
            async with thread_upload_transaction("alice", base_dir=tmp_path):
                entered.set()

        waiter = asyncio.create_task(wait())
        await asyncio.sleep(0.04)
        assert not entered.is_set()
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        release.set()
        await asyncio.wait_for(wait(), 3)
        assert entered.is_set()

    try:
        asyncio.run(scenario())
    finally:
        release.set()
        process.join(timeout=3)
        if process.is_alive():
            process.terminate()
            process.join()
    assert process.exitcode == 0
