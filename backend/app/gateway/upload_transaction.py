"""Serialize upload transactions for a thread across Gateway workers."""

from __future__ import annotations

import asyncio
import fcntl
import hashlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from deerflow.config.paths import get_paths


@asynccontextmanager
async def thread_upload_transaction(thread_id: str, *, base_dir: Path | None = None) -> AsyncIterator[None]:
    """Keep snapshot/write/scan/restore together without blocking the event loop.

    Lock files stay outside mutable per-thread files. Never unlink a lock file:
    another worker may already be waiting on that same inode.
    """
    directory = (base_dir or get_paths().base_dir) / "upload-locks"
    directory.mkdir(parents=True, exist_ok=True)
    lock_key = hashlib.sha256(thread_id.encode("utf-8")).hexdigest()
    with (directory / f"{lock_key}.lock").open("a+b") as stream:
        try:
            while True:
                try:
                    fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    await asyncio.sleep(0.02)
            yield
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
