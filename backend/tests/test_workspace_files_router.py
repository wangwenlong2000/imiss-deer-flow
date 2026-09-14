import asyncio
import os
from pathlib import Path

import pytest
from fastapi import HTTPException

from app.gateway.routers import artifacts


def _list_files(monkeypatch: pytest.MonkeyPatch, root: Path, limit: int = 500):
    monkeypatch.setattr(
        artifacts,
        "resolve_thread_virtual_path",
        lambda _thread_id, _virtual_path: root,
    )
    return asyncio.run(artifacts.list_workspace_files("thread-1", limit=limit))


def test_workspace_files_returns_empty_for_missing_directory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    response = _list_files(monkeypatch, tmp_path / "missing")

    assert response.root == "/mnt/user-data/outputs"
    assert response.files == []


def test_workspace_files_lists_visible_files_newest_first(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    older = tmp_path / "report.txt"
    older.write_text("report", encoding="utf-8")
    nested = tmp_path / "charts"
    nested.mkdir()
    newer = nested / "chart.png"
    newer.write_bytes(b"png")
    hidden = tmp_path / ".internal"
    hidden.write_text("secret", encoding="utf-8")
    hidden_dir = tmp_path / ".cache"
    hidden_dir.mkdir()
    (hidden_dir / "cached.txt").write_text("cache", encoding="utf-8")
    os.utime(older, (100, 100))
    os.utime(newer, (200, 200))

    response = _list_files(monkeypatch, tmp_path)

    assert [item.relative_path for item in response.files] == [
        "charts/chart.png",
        "report.txt",
    ]
    assert response.files[0].virtual_path == "/mnt/user-data/outputs/charts/chart.png"
    assert response.files[0].mime_type == "image/png"
    assert response.files[1].size_bytes == len("report")


def test_workspace_files_skips_symbolic_links(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    target = tmp_path / "target.txt"
    target.write_text("target", encoding="utf-8")
    (tmp_path / "linked.txt").symlink_to(target)

    response = _list_files(monkeypatch, tmp_path)

    assert [item.relative_path for item in response.files] == ["target.txt"]


def test_workspace_files_rejects_non_directory_output_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    output_file = tmp_path / "outputs"
    output_file.write_text("not a directory", encoding="utf-8")

    with pytest.raises(HTTPException) as exc_info:
        _list_files(monkeypatch, output_file)

    assert exc_info.value.status_code == 400
