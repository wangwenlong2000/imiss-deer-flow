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

from fastapi import APIRouter, File, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse

import requests
from requests.auth import HTTPBasicAuth

from pydantic import BaseModel, Field

from deerflow.config.paths import get_paths

from .uploads import save_thread_upload_from_bytes

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

def _read_registry() -> list[DataSourceRecord]:
    try:
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
            except Exception as exc:
                logger.warning("Skipping malformed data-center record: %s", exc)
        return records

    except Exception as exc:
        logger.exception("Failed to read data-center registry")
        return []


def _write_registry(records: list[DataSourceRecord]) -> None:
    payload = [record.model_dump(mode="json") for record in records]
    _registry_file().write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


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
        return HTTPBasicAuth(username, password)

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


def _es_get(path: str, params: dict | None = None):
    es_url = _check_data_center_es_config()
    url = f"{es_url}/{path.lstrip('/')}"

    try:
        response = requests.get(
            url,
            auth=_data_center_es_auth(),
            params=params,
            timeout=15,
        )
        response.raise_for_status()
        return response.json()

    except requests.exceptions.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else 500
        detail = exc.response.text if exc.response is not None else str(exc)
        raise HTTPException(status_code=status_code, detail=detail) from exc

    except Exception as exc:
        logger.exception("Failed to request Elasticsearch")
        raise HTTPException(status_code=500, detail=str(exc)) from exc

def _es_post(path: str, json_body: dict[str, Any]):
    es_url = _check_data_center_es_config()
    url = f"{es_url}/{path.lstrip('/')}"

    try:
        response = requests.post(
            url,
            auth=_data_center_es_auth(),
            json=json_body,
            timeout=20,
        )
        response.raise_for_status()
        return response.json()

    except requests.exceptions.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else 500
        detail = exc.response.text if exc.response is not None else str(exc)
        raise HTTPException(status_code=status_code, detail=detail) from exc

    except Exception as exc:
        logger.exception("Failed to post request to Elasticsearch")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


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

def _is_relative_to(path: Path, parent: Path) -> bool: #删除保护
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


@router.get("/es/health", response_model=EsHealthResponse)
async def get_es_health() -> EsHealthResponse:
    data = _es_get("/_cluster/health")

    return EsHealthResponse(
        status=data.get("status"),
        cluster_name=data.get("cluster_name"),
        number_of_nodes=data.get("number_of_nodes"),
        active_primary_shards=data.get("active_primary_shards"),
    )


@router.get("/es/indices", response_model=EsIndexListResponse)
async def list_es_indices() -> EsIndexListResponse:
    data = _es_get(
        "/_cat/indices",
        params={
            "format": "json",
            "bytes": "b",
        },
    )

    items: list[EsIndexItem] = []

    for item in data:
        index_name = item.get("index")
        if not index_name or not _is_allowed_es_index(index_name):
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
async def get_es_index_detail(index_name: str) -> EsIndexDetailResponse:
    _validate_es_index_name(index_name)

    mapping = _es_get(f"/{index_name}/_mapping")
    stats = _es_get(f"/{index_name}/_stats/docs,store")

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
    size: int = Query(default=5, ge=1, le=20),
) -> EsSamplesResponse:
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

    data = _es_post(f"/{index_name}/_search", body)

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
async def list_data_sources() -> DataSourceListResponse:
    sources = _read_registry()
    # sources = [*_enumerate_local_datasets(), *_read_registry()]  #显示本地dataset目录，待测试
    return DataSourceListResponse(
        sources=sources,
        count=len(sources),
    )


@router.get("/sources/{source_id}/download")   #新增下载接口
async def download_data_source(source_id: str):
    source = _find_registered_source(source_id)

    if not source:
        raise HTTPException(
            status_code=404,
            detail=f"Data source not found: {source_id}",
        )

    if source.type != "uploaded_file":
        raise HTTPException(
            status_code=400,
            detail="Only uploaded files can be downloaded",
        )

    if not source.path:
        raise HTTPException(
            status_code=404,
            detail="Data source has no file path",
        )

    file_path = Path(source.path)

    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(
            status_code=404,
            detail="Original file not found",
        )

    filename = str(source.metadata.get("filename") or file_path.name)

    return FileResponse(
        path=file_path,
        filename=filename,
        media_type="application/octet-stream",
    )


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



@router.delete("/sources/{source_id}", response_model=DeleteDataSourceResponse)   #删除数据源接口
async def delete_data_source(source_id: str) -> DeleteDataSourceResponse:
    records = _read_registry()
    target = next((record for record in records if record.id == source_id), None)

    if not target:
        local_source = next(
            (source for source in _enumerate_local_datasets() if source.id == source_id),
            None,
        )
        if local_source:
            raise HTTPException(
                status_code=400,
                detail="Built-in local datasets cannot be deleted",
            )

        raise HTTPException(
            status_code=404,
            detail=f"Data source not found: {source_id}",
        )

    if target.type != "uploaded_file":
        raise HTTPException(
            status_code=400,
            detail="Only uploaded files can be deleted from the data center",
        )

    file_deleted = False

    if target.path:
        file_path = Path(target.path)
        uploads_root = _workspace_uploads_dir().resolve()

        try:
            resolved_file_path = file_path.resolve()

            if resolved_file_path.exists():
                if not _is_relative_to(resolved_file_path, uploads_root):
                    raise HTTPException(
                        status_code=400,
                        detail="Refusing to delete file outside data-center uploads directory",
                    )

                if resolved_file_path.is_file():
                    resolved_file_path.unlink()
                    file_deleted = True
                else:
                    raise HTTPException(
                        status_code=400,
                        detail="Data source path is not a file",
                    )
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception("Failed to delete data source file: %s", target.path)
            raise HTTPException(
                status_code=500,
                detail=f"Failed to delete data source file: {exc}",
            ) from exc

    remaining_records = [record for record in records if record.id != source_id]
    _write_registry(remaining_records)

    return DeleteDataSourceResponse(
        success=True,
        source_id=source_id,
        file_deleted=file_deleted,
        message=f"Deleted data source: {target.name}",
    )

@router.post("/sources/attach-to-thread", response_model=AttachDataSourcesToThreadResponse)
async def attach_data_sources_to_thread(
    payload: AttachDataSourcesToThreadRequest,
) -> AttachDataSourcesToThreadResponse:
    if not payload.thread_id:
        raise HTTPException(status_code=400, detail="thread_id is required")

    if not payload.source_ids:
        return AttachDataSourcesToThreadResponse(
            success=True,
            files=[],
            message="No data sources selected",
        )

    attached_files: list[dict[str, str]] = []

    for source_id in payload.source_ids:
        source = _find_registered_source(source_id)

        if not source:
            raise HTTPException(
                status_code=404,
                detail=f"Data source not found: {source_id}",
            )

        if source.type != "uploaded_file":
            raise HTTPException(
                status_code=400,
                detail=f"Only uploaded files can be attached to chat: {source_id}",
            )

        if not source.path:
            raise HTTPException(
                status_code=404,
                detail=f"Data source has no file path: {source_id}",
            )

        source_path = Path(source.path)

        if not source_path.exists() or not source_path.is_file():
            raise HTTPException(
                status_code=404,
                detail=f"Original file not found: {source_id}",
            )

        filename = str(source.metadata.get("filename") or source_path.name)
        content = source_path.read_bytes()

        file_info = await save_thread_upload_from_bytes(
            thread_id=payload.thread_id,
            filename=filename,
            content=content,
        )

        attached_files.append(file_info)

    return AttachDataSourcesToThreadResponse(
        success=True,
        files=attached_files,
        message=f"Attached {len(attached_files)} data source file(s) to thread",
    )
