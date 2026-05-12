from pathlib import Path
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.gateway.routers import host_files


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(host_files.router)
    return TestClient(app)


def test_host_file_router_serves_text_file_from_allowed_root(tmp_path: Path):
    allowed_root = tmp_path / "datasets"
    allowed_root.mkdir(parents=True)
    target_file = allowed_root / "sample.txt"
    target_file.write_text("hello host file", encoding="utf-8")

    with patch.object(host_files, "get_file_access_roots", return_value=[allowed_root]):
        with _client() as client:
            response = client.get(f"/api/host-files/{target_file.as_posix().lstrip('/')}")

    assert response.status_code == 200
    assert response.text == "hello host file"
    assert response.headers["content-type"].startswith("text/plain")


def test_host_file_router_rejects_path_outside_allowed_roots(tmp_path: Path):
    allowed_root = tmp_path / "datasets"
    allowed_root.mkdir(parents=True)
    blocked_file = tmp_path / "secret.txt"
    blocked_file.write_text("blocked", encoding="utf-8")

    with patch.object(host_files, "get_file_access_roots", return_value=[allowed_root]):
        with _client() as client:
            response = client.get(f"/api/host-files/{blocked_file.as_posix().lstrip('/')}")

    assert response.status_code == 403


def test_host_file_router_rejects_symlink_escape(tmp_path: Path):
    allowed_root = tmp_path / "datasets"
    allowed_root.mkdir(parents=True)
    blocked_file = tmp_path / "secret.txt"
    blocked_file.write_text("blocked", encoding="utf-8")
    escaped_link = allowed_root / "escaped.txt"
    escaped_link.symlink_to(blocked_file)

    with patch.object(host_files, "get_file_access_roots", return_value=[allowed_root]):
        with _client() as client:
            response = client.get(f"/api/host-files/{escaped_link.as_posix().lstrip('/')}")

    assert response.status_code == 403