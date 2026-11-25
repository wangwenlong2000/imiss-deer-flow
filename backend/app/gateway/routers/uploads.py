"""Upload router for handling file uploads."""

import logging
import shlex
from dataclasses import dataclass
from pathlib import Path

from fastapi import APIRouter, File, HTTPException, Request, UploadFile
from pydantic import BaseModel

from app.gateway.thread_access import require_thread_access
from app.gateway.upload_transaction import thread_upload_transaction
from deerflow.config.paths import VIRTUAL_PATH_PREFIX, get_paths
from deerflow.sandbox.sandbox_provider import get_sandbox_provider
from deerflow.utils.file_conversion import CONVERTIBLE_EXTENSIONS, convert_file_to_markdown

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/threads/{thread_id}/uploads", tags=["uploads"])

_CROSS_USER_WRITABLE_FILE_MODE = 0o666


@dataclass(frozen=True)
class _UploadArtifactSnapshot:
    """State needed to roll an upload back after a compliance rejection."""

    path: Path
    virtual_path: str
    existed: bool
    content: bytes | None


def _check_upload_path(path: Path, uploads_dir: Path) -> None:
    """Reject redirected upload directories and symlink file targets.

    Paths are derived from the authorized thread, never from a resolved target
    that might already point into another thread's files.
    """
    root = uploads_dir.absolute()
    target = path.absolute()
    try:
        safe = root == root.resolve() and target == target.resolve() and target.is_relative_to(root)
    except (OSError, RuntimeError):
        safe = False
    if not safe:
        raise HTTPException(status_code=403, detail="Upload path is outside the thread upload directory")


def _snapshot_artifact(path: Path, virtual_path: str) -> _UploadArtifactSnapshot:
    """Capture one file before an upload can overwrite or create it."""
    _check_upload_path(path, path.parent)
    if path.is_file():
        return _UploadArtifactSnapshot(
            path=path,
            virtual_path=virtual_path,
            existed=True,
            content=path.read_bytes(),
        )
    return _UploadArtifactSnapshot(path=path, virtual_path=virtual_path, existed=False, content=None)


def _restore_artifact(snapshot: _UploadArtifactSnapshot) -> None:
    """Restore the host-side artifact without following an unexpected path."""
    _check_upload_path(snapshot.path, snapshot.path.parent)
    if snapshot.existed:
        snapshot.path.write_bytes(snapshot.content or b"")
    else:
        try:
            snapshot.path.unlink()
        except FileNotFoundError:
            pass


def _rollback_uploads(thread_id: str, snapshots: list[_UploadArtifactSnapshot]) -> None:
    """Restore host files and remove copies from a non-local sandbox.

    The sandbox API has no delete-file primitive. The virtual path is generated
    from a basename validated by ``save_thread_upload_from_bytes``; quoting it
    here keeps the fallback shell operation bounded to that exact file.
    """
    restored = []
    for snapshot in reversed(snapshots):
        try:
            _restore_artifact(snapshot)
            restored.append(snapshot)
        except (OSError, HTTPException):
            logger.exception("compliance: failed to restore upload artifact %s", snapshot.path)

    try:
        sandbox_provider = get_sandbox_provider()
        sandbox_id = sandbox_provider.acquire(thread_id)
        sandbox = sandbox_provider.get(sandbox_id)
        if sandbox_id == "local" or sandbox is None:
            return
        for snapshot in restored:
            try:
                if snapshot.existed:
                    sandbox.update_file(snapshot.virtual_path, snapshot.content or b"")
                else:
                    sandbox.execute_command(f"rm -f -- {shlex.quote(snapshot.virtual_path)}")
            except Exception:
                logger.exception("compliance: failed to roll back sandbox artifact %s", snapshot.virtual_path)
    except Exception:
        logger.exception("compliance: failed to acquire sandbox while rolling back uploads")


class UploadResponse(BaseModel):
    """Response model for file upload."""

    success: bool
    files: list[dict[str, str]]
    message: str
    compliance: dict | None = None


def get_uploads_dir(thread_id: str) -> Path:
    """Get the uploads directory for a thread.

    Args:
        thread_id: The thread ID.

    Returns:
        Path to the uploads directory.
    """
    base_dir = get_paths().sandbox_uploads_dir(thread_id)
    _check_upload_path(base_dir, base_dir)
    base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir


def _set_cross_user_writable(path: Path) -> None:
    """Make uploaded artifacts writable across container users.

    This prevents overwrite failures when the initial write and sandbox sync
    happen under different users in shared bind mounts.
    """
    try:
        path.chmod(_CROSS_USER_WRITABLE_FILE_MODE)
    except OSError as exc:
        logger.warning(f"Failed to chmod {path} to 0o666: {exc}")

async def save_thread_upload_from_bytes(
    thread_id: str,
    filename: str,
    content: bytes,
    *,
    rollback: list[_UploadArtifactSnapshot] | None = None,
) -> dict[str, str]:
    """Save bytes as a thread upload and run the same conversion/sandbox sync logic as manual upload."""
    safe_filename = Path(filename).name
    if (
        not safe_filename
        or safe_filename in {".", ".."}
        or "/" in safe_filename
        or "\\" in safe_filename
    ):
        raise HTTPException(status_code=400, detail=f"Invalid filename: {filename}")

    uploads_dir = get_uploads_dir(thread_id)
    paths = get_paths()
    file_path = uploads_dir / safe_filename
    _check_upload_path(file_path, uploads_dir)
    file_ext = file_path.suffix.lower()
    if file_ext in CONVERTIBLE_EXTENSIONS:
        # The converter writes its sibling markdown file. Check that target
        # before conversion, including when the input file itself is safe.
        _check_upload_path(file_path.with_suffix(".md"), uploads_dir)

    sandbox_provider = get_sandbox_provider()
    sandbox_id, sandbox = None, None
    if getattr(sandbox_provider, "uploads_are_shared", False) is not True:
        sandbox_id = sandbox_provider.acquire(thread_id)
        sandbox = sandbox_provider.get(sandbox_id)

    if rollback is not None:
        rollback.append(_snapshot_artifact(file_path, f"{VIRTUAL_PATH_PREFIX}/uploads/{safe_filename}"))
    file_path.write_bytes(content)
    _set_cross_user_writable(file_path)

    relative_path = str(paths.sandbox_uploads_dir(thread_id) / safe_filename)
    virtual_path = f"{VIRTUAL_PATH_PREFIX}/uploads/{safe_filename}"

    if sandbox is not None and sandbox_id != "local":
        sandbox.update_file(virtual_path, content)

    file_info = {
        "filename": safe_filename,
        "size": str(len(content)),
        "path": relative_path,
        "virtual_path": virtual_path,
        "artifact_url": f"/api/threads/{thread_id}/artifacts/mnt/user-data/uploads/{safe_filename}",
    }

    logger.info(f"Saved file: {safe_filename} ({len(content)} bytes) to {relative_path}")

    if file_ext in CONVERTIBLE_EXTENSIONS:
        md_path = file_path.with_suffix(".md")
        if rollback is not None:
            rollback.append(_snapshot_artifact(md_path, f"{VIRTUAL_PATH_PREFIX}/uploads/{md_path.name}"))
        md_path = await convert_file_to_markdown(file_path)
        if md_path:
            _check_upload_path(md_path, uploads_dir)
            _set_cross_user_writable(md_path)
            md_relative_path = str(paths.sandbox_uploads_dir(thread_id) / md_path.name)
            md_virtual_path = f"{VIRTUAL_PATH_PREFIX}/uploads/{md_path.name}"

            if sandbox is not None and sandbox_id != "local":
                sandbox.update_file(md_virtual_path, md_path.read_bytes())

            file_info["markdown_file"] = md_path.name
            file_info["markdown_path"] = md_relative_path
            file_info["markdown_virtual_path"] = md_virtual_path
            file_info["markdown_artifact_url"] = (
                f"/api/threads/{thread_id}/artifacts/mnt/user-data/uploads/{md_path.name}"
            )

    return file_info


@router.post("", response_model=UploadResponse)
async def upload_files(
    thread_id: str,
    request: Request = None,
    files: list[UploadFile] = File(...),
) -> UploadResponse:
    await require_thread_access(request, thread_id, operation_type="upload")
    async with thread_upload_transaction(thread_id):
        return await _upload_files_locked(thread_id, files)


async def _upload_files_locked(thread_id: str, files: list[UploadFile]) -> UploadResponse:
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")

    uploaded_files = []
    rollback: list[_UploadArtifactSnapshot] = []

    for file in files:
        if not file.filename or Path(file.filename).name in {"", ".", ".."}:
            continue
        if file.filename in {".", ".."} or Path(file.filename).name in {".", "..", ""}:
            continue

        try:
            content = await file.read()
            file_info = await save_thread_upload_from_bytes(
                thread_id=thread_id,
                filename=file.filename,
                content=content,
                rollback=rollback,
            )
            uploaded_files.append(file_info)

        except HTTPException:
            _rollback_uploads(thread_id, rollback)
            raise
        except BaseException as exc:
            if not isinstance(exc, Exception):
                _rollback_uploads(thread_id, rollback)
                raise
            _rollback_uploads(thread_id, rollback)
            logger.error(f"Failed to upload {file.filename}: {exc}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to upload {file.filename}: {str(exc)}",
            ) from exc

    # Compliance InputGate: scan after the files land and any conversion has run,
    # but BEFORE reporting success — a violating upload must never be acknowledged
    # as accepted. App calls harness; the dependency direction stays legal.
    checks: list[dict] = []
    warning = _scan_uploads_for_compliance(thread_id, uploaded_files, checks)
    if warning is not None:
        _rollback_uploads(thread_id, rollback)
        return UploadResponse(success=False, files=[], message=warning, compliance={"checks": checks})

    return UploadResponse(
        success=True,
        files=uploaded_files,
        message=f"Successfully uploaded {len(uploaded_files)} file(s)",
        compliance={"checks": checks} if checks else None,
    )


def _scan_uploads_for_compliance(thread_id: str, uploaded_files: list[dict], checks: list[dict] | None = None) -> str | None:
    """Return a refusal message when an upload violates policy, else ``None``.

    Failures here follow the input gate's ``fail_mode``: with the default
    ``closed`` the upload is rejected, because an upload scanner that silently
    stops scanning is worse than one that is switched off.
    """
    try:
        from deerflow.agents.middlewares.compliance_input_gate_middleware import scan_upload_paths
        from deerflow.compliance.normalizers.user_input import IMAGE_SUFFIXES, TEXT_SUFFIXES
        from deerflow.compliance.runtime import INCOMPLETE_SCAN_NOTICE, decision_check, gate_enabled, user_notice

        if not gate_enabled("InputGate"):
            return None

        paths: list[str] = []
        for info in uploaded_files:
            original_path = str(info.get("path") or "")
            filename = str(info.get("filename") or original_path)
            suffix = Path(filename).suffix.lower()
            markdown_path = str(info.get("markdown_path") or "")

            if suffix in IMAGE_SUFFIXES:
                # Image contents are outside this scanner's scope.
                continue
            if suffix in TEXT_SUFFIXES:
                if original_path:
                    paths.append(original_path)
                continue
            if markdown_path and Path(markdown_path).is_file():
                paths.append(markdown_path)
            else:
                # The normalizer records unsupported/missing extraction as an
                # incomplete unit, which is refused and audited by the engine.
                paths.append(original_path)

        if not paths:
            return None

        decision = scan_upload_paths(paths, thread_id=thread_id)
        check = decision_check(decision)
        check["scan_complete"] = not decision.diagnostics.truncated and not decision.diagnostics.failed_closed
        if checks is not None:
            checks.append(check)
        if decision.diagnostics.truncated:
            notice = (
                "【合规检查未完成】附件文本提取为空、不可读或格式不支持，已撤回本次上传。请提供可读取的 UTF-8 文本或重新提取后重试。"
                if any(warning.startswith("upload_normalization:") for warning in decision.diagnostics.warnings)
                else INCOMPLETE_SCAN_NOTICE
            )
            return f"{notice} 审计编号：{decision.audit_ref}。" if decision.audit_ref else notice
        if "refuse" in decision.actions:
            logger.warning("compliance: rejected upload for thread %s: %s", thread_id, decision.actions)
            return user_notice(decision)
        if decision.hits:
            logger.info("compliance: upload flagged for thread %s: %s", thread_id, decision.actions)
        return None
    except Exception:
        logger.exception("compliance: upload scan failed for thread %s", thread_id)
        try:
            from deerflow.compliance.runtime import FAIL_CLOSED_NOTICE, gate_config

            if getattr(gate_config("InputGate"), "fail_mode", "closed") == "closed":
                return FAIL_CLOSED_NOTICE
        except Exception:
            logger.exception("compliance: could not resolve the input gate fail mode; rejecting the upload")
            return "【合规检查失败】合规检测未能完成，为安全起见已拒绝本次上传。"
        return None


@router.get("/list", response_model=dict)
async def list_uploaded_files(thread_id: str, request: Request = None) -> dict:
    """List all files in a thread's uploads directory.

    Args:
        thread_id: The thread ID to list files for.

    Returns:
        Dictionary containing list of files with their metadata.
    """
    await require_thread_access(request, thread_id, operation_type="analysis")
    uploads_dir = get_uploads_dir(thread_id)
    _check_upload_path(uploads_dir, uploads_dir)

    if not uploads_dir.exists():
        return {"files": [], "count": 0}

    files = []
    for file_path in sorted(uploads_dir.iterdir()):
        if not file_path.is_symlink() and file_path.is_file():
            _check_upload_path(file_path, uploads_dir)
            stat = file_path.stat()
            relative_path = str(get_paths().sandbox_uploads_dir(thread_id) / file_path.name)
            files.append(
                {
                    "filename": file_path.name,
                    "size": stat.st_size,
                    "path": relative_path,  # Actual filesystem path
                    "virtual_path": f"{VIRTUAL_PATH_PREFIX}/uploads/{file_path.name}",  # Path for Agent in sandbox
                    "artifact_url": f"/api/threads/{thread_id}/artifacts/mnt/user-data/uploads/{file_path.name}",  # HTTP URL
                    "extension": file_path.suffix,
                    "modified": stat.st_mtime,
                }
            )

    return {"files": files, "count": len(files)}


@router.delete("/{filename}")
async def delete_uploaded_file(thread_id: str, filename: str, request: Request = None) -> dict:
    """Delete a file from a thread's uploads directory.

    Args:
        thread_id: The thread ID.
        filename: The filename to delete.

    Returns:
        Success message.
    """
    await require_thread_access(request, thread_id, operation_type="upload")
    uploads_dir = get_uploads_dir(thread_id)
    file_path = uploads_dir / filename
    _check_upload_path(file_path, uploads_dir)

    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {filename}")

    # Security check: ensure the path is within the uploads directory
    try:
        file_path.resolve().relative_to(uploads_dir.resolve())
    except ValueError:
        raise HTTPException(status_code=403, detail="Access denied")

    try:
        file_path.unlink()
        logger.info(f"Deleted file: {filename}")
        return {"success": True, "message": f"Deleted {filename}"}
    except Exception as e:
        logger.error(f"Failed to delete {filename}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to delete {filename}: {str(e)}")
