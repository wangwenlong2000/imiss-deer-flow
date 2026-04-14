"""Data center router for dataset/source registration and listing."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from uuid import uuid4

from fastapi import APIRouter, File, HTTPException, UploadFile
from pydantic import BaseModel, Field

from deerflow.config.paths import get_paths

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/data-center", tags=["data_center"])

DataSourceType = Literal["local_dataset", "uploaded_file", "database", "vector_store"]
DataSourceStatus = Literal["ready", "syncing", "error", "disabled"]
OwnerScope = Literal["thread", "workspace", "global"]


class DataSourceRecord(BaseModel):
    id: str
    name: str
    type: DataSourceType
    status: DataSourceStatus
    description: str | None = None
    path: str | None = None
    virtual_path: str | None = None
    updated_at: str | None = None
    owner_scope: OwnerScope = "workspace"
    selectable_in_chat: bool = True
    thread_id: str | None = None
    metadata: dict[str, str | int | float | bool | None] = Field(default_factory=dict)


class DataSourceListResponse(BaseModel):
    sources: list[DataSourceRecord]
    count: int


class RegisterUploadRequest(BaseModel):
    thread_id: str
    filename: str
    name: str | None = None
    description: str | None = None


class UploadDataSourceResponse(BaseModel):
    success: bool
    sources: list[DataSourceRecord]
    message: str


def _project_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _datasets_root() -> Path:
    return _project_root() / "datasets"


def _registry_dir() -> Path:
    path = get_paths().base_dir / "data-center"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _registry_file() -> Path:
    return _registry_dir() / "sources.json"


def _workspace_uploads_dir() -> Path:
    path = _registry_dir() / "uploads"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _read_registry() -> list[DataSourceRecord]:
    registry_file = _registry_file()
    if not registry_file.exists():
        return []

    try:
        raw = json.loads(registry_file.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        logger.warning("Invalid data-center registry, resetting: %s", exc)
        return []

    records = []
    for item in raw if isinstance(raw, list) else []:
        try:
            records.append(DataSourceRecord.model_validate(item))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Skipping malformed data-center record: %s", exc)
    return records


def _write_registry(records: list[DataSourceRecord]) -> None:
    payload = [record.model_dump(mode="json") for record in records]
    _registry_file().write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _iso_now() -> str:
    return datetime.now(UTC).isoformat()


def _normalize_filename(filename: str) -> str:
    safe_name = Path(filename).name
    if not safe_name or safe_name in {".", ".."}:
        raise HTTPException(status_code=400, detail="Invalid filename")
    return safe_name


def _deduplicated_path(base_dir: Path, filename: str) -> Path:
    candidate = base_dir / filename
    if not candidate.exists():
        return candidate

    stem = candidate.stem
    suffix = candidate.suffix
    index = 1
    while True:
        next_candidate = base_dir / f"{stem}_{index}{suffix}"
        if not next_candidate.exists():
            return next_candidate
        index += 1


def _enumerate_local_datasets() -> list[DataSourceRecord]:
    datasets_root = _datasets_root()
    if not datasets_root.exists():
        return []

    records: list[DataSourceRecord] = []
    for child in sorted(datasets_root.iterdir()):
        if not child.is_dir() or child.name.startswith("."):
            continue

        file_count = sum(1 for _ in child.rglob("*") if _.is_file())
        records.append(
            DataSourceRecord(
                id=f"local-{child.name}",
                name=child.name,
                type="local_dataset",
                status="ready",
                description=f"Built-in dataset folder with {file_count} file(s).",
                path=str(child),
                updated_at=datetime.fromtimestamp(child.stat().st_mtime, UTC).isoformat(),
                owner_scope="global",
                selectable_in_chat=True,
                metadata={
                    "file_count": file_count,
                    "source_kind": "filesystem_dataset",
                },
            )
        )
    return records


def _find_registered_source(source_id: str) -> DataSourceRecord | None:
    all_sources = {source.id: source for source in [*_enumerate_local_datasets(), *_read_registry()]}
    return all_sources.get(source_id)


@router.get("/sources", response_model=DataSourceListResponse)
async def list_data_sources() -> DataSourceListResponse:
    sources = [*_enumerate_local_datasets(), *_read_registry()]
    return DataSourceListResponse(sources=sources, count=len(sources))


@router.get("/sources/{source_id}", response_model=DataSourceRecord)
async def get_data_source_detail(source_id: str) -> DataSourceRecord:
    source = _find_registered_source(source_id)
    if not source:
        raise HTTPException(status_code=404, detail=f"Data source not found: {source_id}")
    return source


@router.post("/sources/upload", response_model=UploadDataSourceResponse)
async def upload_data_sources(
    files: list[UploadFile] = File(...),
) -> UploadDataSourceResponse:
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")

    upload_dir = _workspace_uploads_dir()
    records = _read_registry()
    created: list[DataSourceRecord] = []

    for file in files:
        if not file.filename:
            continue

        safe_name = _normalize_filename(file.filename)
        target_path = _deduplicated_path(upload_dir, safe_name)
        content = await file.read()
        target_path.write_bytes(content)

        record = DataSourceRecord(
            id=f"upload-{uuid4().hex[:12]}",
            name=target_path.stem,
            type="uploaded_file",
            status="ready",
            description=f"Uploaded from data center: {target_path.name}",
            path=str(target_path),
            virtual_path=None,
            updated_at=_iso_now(),
            owner_scope="workspace",
            selectable_in_chat=True,
            thread_id=None,
            metadata={
                "filename": target_path.name,
                "size_bytes": len(content),
                "source_kind": "workspace_upload",
            },
        )
        records.append(record)
        created.append(record)

    _write_registry(records)
    return UploadDataSourceResponse(
        success=True,
        sources=created,
        message=f"Uploaded and registered {len(created)} data source(s)",
    )


@router.post("/sources/register-upload", response_model=DataSourceRecord)
async def register_uploaded_file(payload: RegisterUploadRequest) -> DataSourceRecord:
    uploads_dir = get_paths().sandbox_uploads_dir(payload.thread_id)
    file_path = uploads_dir / payload.filename

    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail=f"Uploaded file not found: {payload.filename}")

    records = _read_registry()
    existing = next(
        (
            record
            for record in records
            if record.type == "uploaded_file"
            and record.thread_id == payload.thread_id
            and Path(record.path or "").name == payload.filename
        ),
        None,
    )
    if existing:
        return existing

    record = DataSourceRecord(
        id=f"upload-{uuid4().hex[:12]}",
        name=payload.name or file_path.stem,
        type="uploaded_file",
        status="ready",
        description=payload.description or f"Registered from thread upload: {payload.filename}",
        path=str(file_path),
        virtual_path=f"/mnt/user-data/uploads/{payload.filename}",
        updated_at=_iso_now(),
        owner_scope="workspace",
        selectable_in_chat=True,
        thread_id=payload.thread_id,
        metadata={
            "filename": payload.filename,
            "size_bytes": file_path.stat().st_size,
        },
    )
    records.append(record)
    _write_registry(records)
    return record
