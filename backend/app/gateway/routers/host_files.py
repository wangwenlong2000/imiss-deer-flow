import logging
import mimetypes
import os
from pathlib import Path
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, PlainTextResponse, Response

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/host-files", tags=["host_files"])


def is_text_file_by_content(path: Path, sample_size: int = 8192) -> bool:
    """Check if file is text by examining content for null bytes."""
    try:
        with open(path, "rb") as file_obj:
            chunk = file_obj.read(sample_size)
            return b"\x00" not in chunk
    except Exception:
        return False


def _local_project_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _host_project_root() -> Path:
    host_base_dir = os.getenv("DEER_FLOW_HOST_BASE_DIR")
    if host_base_dir:
        return Path(host_base_dir).resolve().parents[1]
    return _local_project_root()


def get_file_access_roots() -> list[Path]:
    """Return the whitelisted host filesystem roots exposed by the gateway."""
    return [Path("/mnt/nas"), _host_project_root() / "datasets"]


def resolve_host_file_path(requested_path: str) -> Path:
    """Resolve a request path to a real file under an allowed host root."""
    candidate = Path("/" + requested_path.lstrip("/")).resolve(strict=False)
    allowed_roots = [root.resolve(strict=False) for root in get_file_access_roots()]

    if not any(candidate.is_relative_to(root) for root in allowed_roots):
        raise HTTPException(status_code=403, detail=f"Access denied: {candidate}")

    if not candidate.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {candidate}")

    if not candidate.is_file():
        raise HTTPException(status_code=400, detail=f"Path is not a file: {candidate}")

    real_candidate = candidate.resolve()
    if not any(real_candidate.is_relative_to(root.resolve()) for root in get_file_access_roots()):
        raise HTTPException(status_code=403, detail=f"Access denied: {real_candidate}")

    return real_candidate


@router.get(
    "/{path:path}",
    summary="Get Host File",
    description="Retrieve a whitelisted host file from /mnt/nas or the workspace datasets directory.",
)
async def get_host_file(path: str, request: Request) -> Response:
    actual_path = resolve_host_file_path(path)
    logger.info("Serving host file: requested_path=%s actual_path=%s", path, actual_path)

    mime_type, _ = mimetypes.guess_type(actual_path)
    encoded_filename = quote(actual_path.name)

    if request.query_params.get("download"):
        return FileResponse(path=actual_path, filename=actual_path.name, media_type=mime_type, headers={"Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}"})

    if mime_type == "text/html":
        return HTMLResponse(content=actual_path.read_text(encoding="utf-8"))

    if mime_type and mime_type.startswith("text/"):
        return PlainTextResponse(content=actual_path.read_text(encoding="utf-8"), media_type=mime_type)

    if is_text_file_by_content(actual_path):
        return PlainTextResponse(content=actual_path.read_text(encoding="utf-8"), media_type=mime_type)

    return Response(content=actual_path.read_bytes(), media_type=mime_type, headers={"Content-Disposition": f"inline; filename*=UTF-8''{encoded_filename}"})