import asyncio
import stat
from io import BytesIO
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import UploadFile

from app.gateway.routers import uploads


def test_upload_files_writes_thread_storage_and_skips_local_sandbox_sync(tmp_path):
    thread_uploads_dir = tmp_path / "uploads"
    thread_uploads_dir.mkdir(parents=True)

    provider = MagicMock()
    provider.acquire.return_value = "local"
    sandbox = MagicMock()
    provider.get.return_value = sandbox

    with (
        patch.object(uploads, "get_uploads_dir", return_value=thread_uploads_dir),
        patch.object(uploads, "get_sandbox_provider", return_value=provider),
    ):
        file = UploadFile(filename="notes.txt", file=BytesIO(b"hello uploads"))
        result = asyncio.run(uploads.upload_files("thread-local", files=[file]))

    assert result.success is True
    assert len(result.files) == 1
    assert result.files[0]["filename"] == "notes.txt"
    notes_path = thread_uploads_dir / "notes.txt"
    assert notes_path.read_bytes() == b"hello uploads"
    assert stat.S_IMODE(notes_path.stat().st_mode) == 0o666

    sandbox.update_file.assert_not_called()


def test_upload_files_syncs_non_local_sandbox_and_marks_markdown_file(tmp_path):
    thread_uploads_dir = tmp_path / "uploads"
    thread_uploads_dir.mkdir(parents=True)

    provider = MagicMock()
    provider.acquire.return_value = "aio-1"
    sandbox = MagicMock()
    provider.get.return_value = sandbox

    async def fake_convert(file_path: Path) -> Path:
        md_path = file_path.with_suffix(".md")
        md_path.write_text("converted", encoding="utf-8")
        return md_path

    with (
        patch.object(uploads, "get_uploads_dir", return_value=thread_uploads_dir),
        patch.object(uploads, "get_sandbox_provider", return_value=provider),
        patch.object(uploads, "convert_file_to_markdown", AsyncMock(side_effect=fake_convert)),
    ):
        file = UploadFile(filename="report.pdf", file=BytesIO(b"pdf-bytes"))
        result = asyncio.run(uploads.upload_files("thread-aio", files=[file]))

    assert result.success is True
    assert len(result.files) == 1
    file_info = result.files[0]
    assert file_info["filename"] == "report.pdf"
    assert file_info["markdown_file"] == "report.md"

    report_pdf_path = thread_uploads_dir / "report.pdf"
    report_md_path = thread_uploads_dir / "report.md"
    assert report_pdf_path.read_bytes() == b"pdf-bytes"
    assert report_md_path.read_text(encoding="utf-8") == "converted"
    assert stat.S_IMODE(report_pdf_path.stat().st_mode) == 0o666
    assert stat.S_IMODE(report_md_path.stat().st_mode) == 0o666

    sandbox.update_file.assert_any_call("/mnt/user-data/uploads/report.pdf", b"pdf-bytes")
    sandbox.update_file.assert_any_call("/mnt/user-data/uploads/report.md", b"converted")


def test_compliance_rejection_removes_new_host_and_sandbox_artifacts(tmp_path):
    """A rejected upload must not remain readable after the failed response."""
    thread_uploads_dir = tmp_path / "uploads"
    thread_uploads_dir.mkdir(parents=True)

    provider = MagicMock()
    provider.acquire.return_value = "aio-1"
    sandbox = MagicMock()
    provider.get.return_value = sandbox

    with (
        patch.object(uploads, "get_uploads_dir", return_value=thread_uploads_dir),
        patch.object(uploads, "get_sandbox_provider", return_value=provider),
        patch.object(uploads, "_scan_uploads_for_compliance", return_value="blocked"),
    ):
        file = UploadFile(filename="secret.txt", file=BytesIO(b"sensitive content"))
        result = asyncio.run(uploads.upload_files("thread-aio", files=[file]))

    assert result.success is False
    assert not (thread_uploads_dir / "secret.txt").exists()
    sandbox.execute_command.assert_called_once_with("rm -f -- /mnt/user-data/uploads/secret.txt")


def test_compliance_rejection_restores_overwritten_file_and_generated_markdown(tmp_path):
    """Rollback restores a prior same-name file instead of deleting user data."""
    thread_uploads_dir = tmp_path / "uploads"
    thread_uploads_dir.mkdir(parents=True)
    original = thread_uploads_dir / "report.pdf"
    original.write_bytes(b"old-pdf")
    original_markdown = thread_uploads_dir / "report.md"
    original_markdown.write_text("old-markdown", encoding="utf-8")

    provider = MagicMock()
    provider.acquire.return_value = "aio-1"
    sandbox = MagicMock()
    provider.get.return_value = sandbox

    async def fake_convert(file_path: Path) -> Path:
        md_path = file_path.with_suffix(".md")
        md_path.write_text("new-markdown", encoding="utf-8")
        return md_path

    with (
        patch.object(uploads, "get_uploads_dir", return_value=thread_uploads_dir),
        patch.object(uploads, "get_sandbox_provider", return_value=provider),
        patch.object(uploads, "convert_file_to_markdown", AsyncMock(side_effect=fake_convert)),
        patch.object(uploads, "_scan_uploads_for_compliance", return_value="blocked"),
    ):
        file = UploadFile(filename="report.pdf", file=BytesIO(b"new-pdf"))
        result = asyncio.run(uploads.upload_files("thread-aio", files=[file]))

    assert result.success is False
    assert original.read_bytes() == b"old-pdf"
    assert original_markdown.read_text(encoding="utf-8") == "old-markdown"
    sandbox.update_file.assert_any_call("/mnt/user-data/uploads/report.pdf", b"old-pdf")
    sandbox.update_file.assert_any_call("/mnt/user-data/uploads/report.md", b"old-markdown")


def test_upload_files_rejects_dotdot_and_dot_filenames(tmp_path):
    thread_uploads_dir = tmp_path / "uploads"
    thread_uploads_dir.mkdir(parents=True)

    provider = MagicMock()
    provider.acquire.return_value = "local"
    sandbox = MagicMock()
    provider.get.return_value = sandbox

    with (
        patch.object(uploads, "get_uploads_dir", return_value=thread_uploads_dir),
        patch.object(uploads, "get_sandbox_provider", return_value=provider),
    ):
        # These filenames must be rejected outright
        for bad_name in ["..", "."]:
            file = UploadFile(filename=bad_name, file=BytesIO(b"data"))
            result = asyncio.run(uploads.upload_files("thread-local", files=[file]))
            assert result.success is True
            assert result.files == [], f"Expected no files for unsafe filename {bad_name!r}"

        # Path-traversal prefixes are stripped to the basename and accepted safely
        file = UploadFile(filename="../etc/passwd", file=BytesIO(b"data"))
        result = asyncio.run(uploads.upload_files("thread-local", files=[file]))
        assert result.success is True
        assert len(result.files) == 1
        assert result.files[0]["filename"] == "passwd"

    # Only the safely normalised file should exist
    assert [f.name for f in thread_uploads_dir.iterdir()] == ["passwd"]
