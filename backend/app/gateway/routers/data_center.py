"""Data center router for dataset/source registration and listing."""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

import httpx
from fastapi import APIRouter, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from app.gateway.data_source_store import DataSourceStore
from app.gateway.mobile_iam import MobileIAMPrincipal, authenticate_mobile_request
from app.gateway.thread_access import require_thread_access
from app.gateway.upload_transaction import thread_upload_transaction
from deerflow.config.paths import get_paths

from .uploads import _rollback_uploads, _scan_uploads_for_compliance, save_thread_upload_from_bytes

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
    owner_user_id: str | None = None
    owner_org_id: str | None = None
    metadata: dict[str, str | int | float | bool | None] = Field(default_factory=dict)


class DataSourceListResponse(BaseModel):
    sources: list[DataSourceRecord]
    count: int


class RegisterUploadRequest(BaseModel):
    thread_id: str
    filename: str
    name: str | None = None
    description: str | None = None

class DeleteDataSourceResponse(BaseModel):
    success: bool
    source_id: str
    file_deleted: bool
    message: str


class UploadDataSourceResponse(BaseModel):
    success: bool
    sources: list[DataSourceRecord]
    message: str

class AttachDataSourcesToThreadRequest(BaseModel):
    thread_id: str
    source_ids: list[str]


class AttachDataSourcesToThreadResponse(BaseModel):
    success: bool
    files: list[dict[str, str]]
    message: str

class EsHealthResponse(BaseModel):
    status: str | None = None
    cluster_name: str | None = None
    number_of_nodes: int | None = None
    active_primary_shards: int | None = None


class EsIndexItem(BaseModel):
    name: str
    display_name: str
    type: str = "elasticsearch"
    health: str | None = None
    status: str | None = None
    uuid: str | None = None
    pri: int = 0
    rep: int = 0
    docs_count: int = 0
    docs_deleted: int = 0
    store_size_bytes: int = 0
    pri_store_size_bytes: int = 0


class EsIndexListResponse(BaseModel):
    total: int
    items: list[EsIndexItem]

class EsFieldItem(BaseModel):
    name: str
    type: str


class EsIndexDetailResponse(BaseModel):
    name: str
    docs_count: int = 0
    docs_deleted: int = 0
    store_size_bytes: int = 0
    field_count: int = 0
    fields: list[EsFieldItem]


class EsSampleItem(BaseModel):
    id: str | None = None
    score: float | None = None
    source: dict[str, Any]


class EsSamplesResponse(BaseModel):
    index: str
    total: Any = None
    items: list[EsSampleItem]


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

def _store() -> DataSourceStore:
    try:
        return DataSourceStore(get_paths().base_dir / "gateway.sqlite3", _registry_file())
    except Exception as exc:
        logger.exception("Data-source catalogue is unavailable")
        raise HTTPException(status_code=503, detail="Data-source catalogue is unavailable") from exc


def _read_registry() -> list[DataSourceRecord]:
    records = []
    for raw in _store().list():
        try:
            records.append(DataSourceRecord.model_validate(raw))
        except ValueError:
            logger.warning("Skipping malformed legacy data-source record")
    return records


async def _principal(request: Request, operation: str) -> MobileIAMPrincipal | None:
    _, principal = await authenticate_mobile_request(request)
    # These permissions are trusted IAM attributes. They authorize an operation;
    # resource ownership is checked independently below, including for '*'.
    if principal and not {"data_center:*", f"data_center:{operation}"}.intersection(principal.permissions):
        raise HTTPException(status_code=403, detail="Data-center operation is not authorized")
    return principal


def _org_configuration(name: str) -> dict[str, list[str]]:
    try:
        values = json.loads(os.getenv(name, "{}"))
        if not isinstance(values, dict) or any(
            not isinstance(key, str) or not isinstance(items, list) or any(not isinstance(item, str) for item in items)
            for key, items in values.items()
        ):
            raise ValueError("Invalid organization resource map")
        return values
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=503, detail="Data-center access configuration is invalid") from exc


def _visible(source: DataSourceRecord, principal: MobileIAMPrincipal | None, *, shared: bool = True) -> bool:
    if principal is None:
        return True
    if not source.owner_user_id or source.owner_org_id != principal.org_id:
        return False
    if source.owner_user_id == principal.user_id:
        return True
    return shared and source.id in _org_configuration("DATA_CENTER_ORG_SHARED_SOURCES").get(principal.org_id, [])


async def _source_access(
    request: Request, source_id: str, principal: MobileIAMPrincipal | None, operation: str,
) -> DataSourceRecord:
    source = _find_registered_source(source_id)
    if source is None or not _visible(source, principal, shared=operation != "delete"):
        raise HTTPException(status_code=404, detail="Data source not found")
    if source.thread_id:
        await require_thread_access(
            request, source.thread_id,
            operation_type="export" if operation in {"export", "attach"} else "upload" if operation == "delete" else "analysis",
        )
    return source


def _source_file(source: DataSourceRecord) -> Path:
    if source.type != "uploaded_file":
        raise HTTPException(status_code=400, detail="Only uploaded files support this operation")
    if not source.path:
        raise HTTPException(status_code=404, detail="Data source has no file path")
    try:
        root = get_paths().sandbox_uploads_dir(source.thread_id) if source.thread_id else _workspace_uploads_dir()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid data-source thread") from exc
    file_path = Path(source.path).absolute()
    # Reject redirected directories and symlinks, including links into another
    # user's thread, rather than treating their target as owned by this record.
    if root.absolute() != root.resolve() or file_path != file_path.resolve() or not file_path.is_relative_to(root.resolve()):
        raise HTTPException(status_code=400, detail="Data-source file is outside its upload directory")
    if not file_path.is_file():
        raise HTTPException(status_code=404, detail="Original file not found")
    return file_path


async def _es_access(request: Request, index_name: str | None = None) -> MobileIAMPrincipal | None:
    principal = await _principal(request, "es_read")
    if principal:
        allowed = _org_configuration("DATA_CENTER_ORG_ES_INDICES").get(principal.org_id, [])
        if not allowed or (index_name is not None and index_name not in allowed):
            raise HTTPException(status_code=403, detail="Elasticsearch resource is not authorized")
    return principal


def _iso_now() -> str:
    return datetime.now(UTC).isoformat()

def _data_center_es_url() -> str | None:
    return (
        os.getenv("DATA_CENTER_ES_URL")
        or os.getenv("POLICY_ES_URL")
        or os.getenv("ES_URL")
    )


def _data_center_es_username() -> str | None:
    return (
        os.getenv("DATA_CENTER_ES_USERNAME")
        or os.getenv("POLICY_ES_USERNAME")
        or os.getenv("ES_USERNAME")
        or os.getenv("ES_USER")
    )


def _data_center_es_password() -> str | None:
    return (
        os.getenv("DATA_CENTER_ES_PASSWORD")
        or os.getenv("POLICY_ES_PASSWORD")
        or os.getenv("ES_PASSWORD")
    )

def _data_center_es_hide_system_indices() -> bool:
    return os.getenv("DATA_CENTER_ES_HIDE_SYSTEM_INDICES", "true").lower() == "true"


def _data_center_es_allowlist() -> set[str]:
    raw = os.getenv("DATA_CENTER_ES_INDEX_ALLOWLIST", "").strip()
    return {item.strip() for item in raw.split(",") if item.strip()}


def _data_center_es_auth():
    username = _data_center_es_username()
    password = _data_center_es_password()

    if username and password:
        return (username, password)

    return None


def _check_data_center_es_config() -> str:
    es_url = _data_center_es_url()

    if not es_url:
        raise HTTPException(
            status_code=500,
            detail="DATA_CENTER_ES_URL or POLICY_ES_URL is not configured",
        )

    return es_url.rstrip("/")


def _to_int(value) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def _is_allowed_es_index(index_name: str) -> bool:
    if not index_name:
        return False

    if _data_center_es_hide_system_indices() and index_name.startswith("."):
        return False

    allowlist = _data_center_es_allowlist()
    if allowlist and index_name not in allowlist:
        return False

    return True


def _validate_es_index_name(index_name: str) -> None:
    if not index_name:
        raise HTTPException(status_code=400, detail="index_name is required")

    if "/" in index_name or "\\" in index_name:
        raise HTTPException(status_code=400, detail="illegal index name")

    if "*" in index_name or "?" in index_name:
        raise HTTPException(status_code=400, detail="wildcard index is not allowed")

    if not re.match(r"^[a-zA-Z0-9._-]+$", index_name):
        raise HTTPException(status_code=400, detail="illegal index name")

    if not _is_allowed_es_index(index_name):
        raise HTTPException(status_code=403, detail="index is not allowed")


def _es_display_name(index_name: str) -> str:
    display_names = {
        "cn_law_articles": "政策法规库",
        "citybench_evidence": "城市治理证据库",
        "citybrain-skill-router-cards": "Skill 路由卡片库",
    }
    return display_names.get(index_name, index_name)


async def _es_get(path: str, params: dict | None = None):
    es_url = _check_data_center_es_config()
    url = f"{es_url}/{path.lstrip('/')}"

    try:
        async with httpx.AsyncClient(timeout=15, auth=_data_center_es_auth()) as client:
            response = await client.get(url, params=params)
        response.raise_for_status()
        return response.json()

    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=502, detail="Elasticsearch request failed") from exc

    except Exception as exc:
        logger.exception("Failed to request Elasticsearch")
        raise HTTPException(status_code=502, detail="Elasticsearch is unavailable") from exc

async def _es_post(path: str, json_body: dict[str, Any]):
    es_url = _check_data_center_es_config()
    url = f"{es_url}/{path.lstrip('/')}"

    try:
        async with httpx.AsyncClient(timeout=20, auth=_data_center_es_auth()) as client:
            response = await client.post(url, json=json_body)
        response.raise_for_status()
        return response.json()

    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=502, detail="Elasticsearch request failed") from exc

    except Exception as exc:
        logger.exception("Failed to post request to Elasticsearch")
        raise HTTPException(status_code=502, detail="Elasticsearch is unavailable") from exc


def _flatten_es_mapping(properties: dict[str, Any], prefix: str = "") -> list[EsFieldItem]:
    fields: list[EsFieldItem] = []

    for field_name, field_config in properties.items():
        full_name = f"{prefix}.{field_name}" if prefix else field_name
        field_type = field_config.get("type", "object")

        fields.append(
            EsFieldItem(
                name=full_name,
                type=field_type,
            )
        )

        nested_properties = field_config.get("properties")
        if isinstance(nested_properties, dict):
            fields.extend(_flatten_es_mapping(nested_properties, full_name))

    return fields


def _compact_es_value(value: Any, max_text_length: int = 300) -> Any:
    if isinstance(value, str):
        if len(value) > max_text_length:
            return value[:max_text_length] + "..."
        return value

    if isinstance(value, list):
        return [_compact_es_value(item, max_text_length) for item in value[:20]]

    if isinstance(value, dict):
        compacted: dict[str, Any] = {}
        for key, child_value in value.items():
            lower_key = key.lower()
            if "embedding" in lower_key or "vector" in lower_key:
                continue
            compacted[key] = _compact_es_value(child_value, max_text_length)
        return compacted

    return value


def _compact_es_source(source: dict[str, Any]) -> dict[str, Any]:
    """
    预览样例数据时，去掉 embedding/vector 这类大字段，并截断长文本，防止页面卡顿。
    """
    result: dict[str, Any] = {}

    for key, value in source.items():
        lower_key = key.lower()

        if "embedding" in lower_key or "vector" in lower_key:
            continue

        result[key] = _compact_es_value(value)

    return result


def _normalize_filename(filename: str) -> str:
    safe_name = Path(filename).name
    if not safe_name or safe_name in {".", ".."}:
        raise HTTPException(status_code=400, detail="Invalid filename")
    return safe_name


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
    raw = _store().get(source_id)
    if raw is None:
        return next((source for source in _enumerate_local_datasets() if source.id == source_id), None)
    try:
        return DataSourceRecord.model_validate(raw)
    except ValueError:
        return None


@router.get("/es/health", response_model=EsHealthResponse)
async def get_es_health(request: Request) -> EsHealthResponse:
    await _es_access(request)
    data = await _es_get("/_cluster/health")

    return EsHealthResponse(
        status=data.get("status"),
        cluster_name=data.get("cluster_name"),
        number_of_nodes=data.get("number_of_nodes"),
        active_primary_shards=data.get("active_primary_shards"),
    )


@router.get("/es/indices", response_model=EsIndexListResponse)
async def list_es_indices(request: Request) -> EsIndexListResponse:
    principal = await _es_access(request)
    allowed = _org_configuration("DATA_CENTER_ORG_ES_INDICES").get(principal.org_id, []) if principal else None
    data = await _es_get(
        "/_cat/indices",
        params={
            "format": "json",
            "bytes": "b",
        },
    )

    items: list[EsIndexItem] = []

    for item in data:
        index_name = item.get("index")
        if not index_name or not _is_allowed_es_index(index_name) or (allowed is not None and index_name not in allowed):
            continue

        items.append(
            EsIndexItem(
                name=index_name,
                display_name=_es_display_name(index_name),
                health=item.get("health"),
                status=item.get("status"),
                uuid=item.get("uuid"),
                pri=_to_int(item.get("pri")),
                rep=_to_int(item.get("rep")),
                docs_count=_to_int(item.get("docs.count")),
                docs_deleted=_to_int(item.get("docs.deleted")),
                store_size_bytes=_to_int(item.get("store.size")),
                pri_store_size_bytes=_to_int(item.get("pri.store.size")),
            )
        )

    items.sort(key=lambda item: item.docs_count, reverse=True)

    return EsIndexListResponse(
        total=len(items),
        items=items,
    )

@router.get("/es/indices/{index_name}", response_model=EsIndexDetailResponse)
async def get_es_index_detail(index_name: str, request: Request) -> EsIndexDetailResponse:
    await _es_access(request, index_name)
    _validate_es_index_name(index_name)

    mapping = await _es_get(f"/{index_name}/_mapping")
    stats = await _es_get(f"/{index_name}/_stats/docs,store")

    index_mapping = mapping.get(index_name, {}).get("mappings", {})
    properties = index_mapping.get("properties", {})

    fields = _flatten_es_mapping(properties) if isinstance(properties, dict) else []

    index_stats = stats.get("indices", {}).get(index_name, {})
    total_stats = index_stats.get("total", {})

    docs_info = total_stats.get("docs", {})
    store_info = total_stats.get("store", {})

    return EsIndexDetailResponse(
        name=index_name,
        docs_count=docs_info.get("count", 0),
        docs_deleted=docs_info.get("deleted", 0),
        store_size_bytes=store_info.get("size_in_bytes", 0),
        field_count=len(fields),
        fields=fields,
    )


@router.get("/es/indices/{index_name}/samples", response_model=EsSamplesResponse)
async def get_es_index_samples(
    index_name: str,
    request: Request,
    size: int = Query(default=5, ge=1, le=20),
) -> EsSamplesResponse:
    await _es_access(request, index_name)
    _validate_es_index_name(index_name)

    body = {
        "size": size,
        "query": {
            "match_all": {}
        },
        "_source": {
            "excludes": [
                "*embedding*",
                "*vector*",
                "dense_vector",
            ]
        },
    }

    data = await _es_post(f"/{index_name}/_search", body)

    hits = data.get("hits", {}).get("hits", [])

    items: list[EsSampleItem] = []

    for hit in hits:
        items.append(
            EsSampleItem(
                id=hit.get("_id"),
                score=hit.get("_score"),
                source=_compact_es_source(hit.get("_source", {})),
            )
        )

    return EsSamplesResponse(
        index=index_name,
        total=data.get("hits", {}).get("total"),
        items=items,
    )


@router.get("/sources", response_model=DataSourceListResponse)
async def list_data_sources(request: Request) -> DataSourceListResponse:
    principal = await _principal(request, "read")
    sources = [source for source in _read_registry() if _visible(source, principal)]
    return DataSourceListResponse(sources=sources, count=len(sources))


@router.get("/sources/{source_id}/download")
async def download_data_source(source_id: str, request: Request):
    principal = await _principal(request, "export")
    source = await _source_access(request, source_id, principal, "export")
    file_path = _source_file(source)
    return FileResponse(
        path=file_path,
        filename=_normalize_filename(str(source.metadata.get("filename") or file_path.name)),
        media_type="application/octet-stream",
    )


@router.get("/sources/{source_id}", response_model=DataSourceRecord)
async def get_data_source_detail(source_id: str, request: Request) -> DataSourceRecord:
    principal = await _principal(request, "read")
    return await _source_access(request, source_id, principal, "read")


@router.post("/sources/upload", response_model=UploadDataSourceResponse)
async def upload_data_sources(
    request: Request,
    files: list[UploadFile] = File(...),
) -> UploadDataSourceResponse:
    principal = await _principal(request, "upload")
    if not files or not any(file.filename for file in files):
        raise HTTPException(status_code=400, detail="No files provided")

    store = _store()
    upload_dir = _workspace_uploads_dir()
    if upload_dir.absolute() != upload_dir.resolve():
        raise HTTPException(status_code=400, detail="Invalid data-center upload directory")
    created: list[DataSourceRecord] = []
    created_paths: list[Path] = []
    try:
        for file in files:
            if not file.filename:
                continue
            safe_name = _normalize_filename(file.filename)
            source_id = f"upload-{uuid4().hex}"
            # Each upload owns a unique directory; concurrent requests with the
            # same filename can never overwrite one another's bytes.
            target_dir = upload_dir / source_id
            target_dir.mkdir()
            target_path = target_dir / safe_name
            created_paths.append(target_path)
            content = await file.read()
            with target_path.open("xb") as stream:
                stream.write(content)
            created.append(DataSourceRecord(
                id=source_id,
                name=Path(safe_name).stem,
                type="uploaded_file",
                status="ready",
                description=f"Uploaded from data center: {safe_name}",
                path=str(target_path),
                updated_at=_iso_now(),
                owner_scope="workspace",
                owner_user_id=principal.user_id if principal else None,
                owner_org_id=principal.org_id if principal else None,
                metadata={"filename": safe_name, "size_bytes": len(content), "source_kind": "workspace_upload"},
            ))
        store.insert_many([record.model_dump(mode="json") for record in created])
    except BaseException:
        for target in created_paths:
            try:
                target.unlink(missing_ok=True)
                target.parent.rmdir()
            except OSError:
                logger.exception("Failed to clean up an unregistered data-source upload")
        raise
    return UploadDataSourceResponse(
        success=True,
        sources=created,
        message=f"Uploaded and registered {len(created)} data source(s)",
    )


@router.post("/sources/register-upload", response_model=DataSourceRecord)
async def register_uploaded_file(payload: RegisterUploadRequest, request: Request) -> DataSourceRecord:
    principal = await _principal(request, "upload")
    await require_thread_access(request, payload.thread_id, operation_type="upload")
    safe_name = _normalize_filename(payload.filename)
    # Registration accepts a single filename, never a path supplied by a client.
    if safe_name != payload.filename or "\\" in payload.filename:
        raise HTTPException(status_code=400, detail="A plain upload filename is required")
    try:
        uploads_dir = get_paths().sandbox_uploads_dir(payload.thread_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid thread ID") from exc
    record = DataSourceRecord(
        id=f"upload-{uuid4().hex}",
        name=payload.name or Path(safe_name).stem,
        type="uploaded_file",
        status="ready",
        description=payload.description or f"Registered from thread upload: {safe_name}",
        path=str(uploads_dir / safe_name),
        virtual_path=f"/mnt/user-data/uploads/{safe_name}",
        updated_at=_iso_now(),
        owner_scope="workspace",
        thread_id=payload.thread_id,
        owner_user_id=principal.user_id if principal else None,
        owner_org_id=principal.org_id if principal else None,
        metadata={"filename": safe_name},
    )
    file_path = _source_file(record)
    record.metadata["size_bytes"] = file_path.stat().st_size
    stored = DataSourceRecord.model_validate(_store().register_file(
        record.model_dump(mode="json"), str(file_path),
    ))
    # Existing records are never re-owned or silently shared on registration.
    if not _visible(stored, principal, shared=False):
        raise HTTPException(status_code=404, detail="Data source not found")
    return stored


@router.delete("/sources/{source_id}", response_model=DeleteDataSourceResponse)
async def delete_data_source(source_id: str, request: Request) -> DeleteDataSourceResponse:
    principal = await _principal(request, "delete")
    target = await _source_access(request, source_id, principal, "delete")
    if target.type != "uploaded_file":
        raise HTTPException(status_code=400, detail="Only uploaded files can be deleted")

    staged: tuple[Path, Path] | None = None
    try:
        with _store().deleting(source_id) as raw:
            if raw is None:
                raise HTTPException(status_code=404, detail="Data source not found")
            current = DataSourceRecord.model_validate(raw)
            if not _visible(current, principal, shared=False):
                raise HTTPException(status_code=404, detail="Data source not found")
            if current.path and Path(current.path).exists():
                file_path = _source_file(current)
                # Stage deletion on the same filesystem. Restore on transaction
                # failure; never leave a committed record pointing at removed bytes.
                trash_path = file_path.with_name(f".deleted-{uuid4().hex}")
                file_path.rename(trash_path)
                staged = (file_path, trash_path)
    except BaseException:
        if staged:
            staged[1].rename(staged[0])
        raise
    file_deleted = staged is not None
    if staged:
        try:
            staged[1].unlink()
            # UUID upload directories are disposable. Registered thread uploads
            # and legacy shared upload directories must remain in place.
            if staged[0].parent.name == source_id:
                staged[0].parent.rmdir()
        except OSError:
            logger.warning("Deferred cleanup of a deleted data-source file")
    return DeleteDataSourceResponse(
        success=True, source_id=source_id, file_deleted=file_deleted,
        message=f"Deleted data source: {target.name}",
    )


@router.post("/sources/attach-to-thread", response_model=AttachDataSourcesToThreadResponse)
async def attach_data_sources_to_thread(
    payload: AttachDataSourcesToThreadRequest,
    request: Request,
) -> AttachDataSourcesToThreadResponse:
    principal = await _principal(request, "attach")
    await require_thread_access(request, payload.thread_id, operation_type="upload")
    # Authorize every selected source before copying any file into the target.
    sources = [await _source_access(request, source_id, principal, "attach") for source_id in payload.source_ids]
    paths = [(source, _source_file(source)) for source in sources]
    attached_files: list[dict[str, str]] = []
    async with thread_upload_transaction(payload.thread_id, base_dir=get_paths().base_dir):
        rollback = []
        occupied_names: set[str] = set()
        try:
            for source, source_path in paths:
                original_name = _normalize_filename(str(source.metadata.get("filename") or source_path.name))
                filename = original_name
                target_dir = get_paths().sandbox_uploads_dir(payload.thread_id)
                index = 1
                while filename in occupied_names or (target_dir / filename).exists() or (target_dir / filename).is_symlink():
                    filename = f"{Path(original_name).stem}_{index}{Path(original_name).suffix}"
                    index += 1
                occupied_names.add(filename)
                file_info = await save_thread_upload_from_bytes(
                    thread_id=payload.thread_id,
                    filename=filename,
                    content=source_path.read_bytes(),
                    rollback=rollback,
                )
                attached_files.append(file_info)
            warning = _scan_uploads_for_compliance(payload.thread_id, attached_files)
            if warning:
                raise HTTPException(status_code=400, detail=warning)
        except BaseException:
            _rollback_uploads(payload.thread_id, rollback)
            raise
    return AttachDataSourcesToThreadResponse(
        success=True, files=attached_files,
        message=f"Attached {len(attached_files)} data source file(s) to thread",
    )
