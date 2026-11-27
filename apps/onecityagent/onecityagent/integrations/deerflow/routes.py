"""Authenticated OneCity-to-DeerFlow HTTP and SSE routes.

This module imports neither legacy configuration nor A2A/model clients.
The factory supplies the database, settings and optional HTTP transport.
"""
import json
import re
import time
import uuid
from datetime import datetime
from threading import Lock
from urllib.parse import quote, unquote, urlparse

import requests
from flask import Blueprint, Response, current_app, jsonify, request, stream_with_context
from flasgger import swag_from
from werkzeug.local import LocalProxy

from onecityagent.constants import StatusEnum
from onecityagent.logger import logger
from onecityagent.models.db_models import DB, Dialog, Role, Tenant, User, UserTenant
from onecityagent.application.settings import setting

blueprint = Blueprint("deerflow", __name__)
# An injected requests-compatible transport makes normal app tests independent
# of external services without replacing the route implementation.
requests = LocalProxy(lambda: current_app.extensions.get("onecity.deerflow_http", __import__("requests")))

_ALLOWED_DEERFLOW_ROLES = {
    "意图识别智能体",
    "规划智能体",
    "系统",
    "执行智能体",
    "反馈智能体",
}

# DeerFlow manual_ui 模式使用的固定测试身份。这里故意只接受身份键，
# 场景、角色和组织信息均由 OneCityAgent 后端生成，不能由浏览器任意拼装。
_DEERFLOW_COMPLIANCE_PROFILES = {
    "ordinary_user": {
        "scene": "self_use",
        "role": "user",
        "org_id": "personal",
    },
    "internal_operator": {
        "scene": "internal_org",
        "role": "internal_operator",
        "org_id": "city-governance",
    },
    "cross_org_operator": {
        "scene": "cross_org",
        "role": "cross_org_operator",
        "org_id": "partner-org",
    },
    "publisher": {
        "scene": "public_release",
        "role": "publisher",
        "org_id": "publicity",
    },
    "researcher": {
        "scene": "research_anon",
        "role": "researcher",
        "org_id": "research",
    },
}


def _build_deerflow_compliance_profile(identity):
    """Build the exact manual profile accepted by DeerFlow's server whitelist."""
    identity_key = str(identity or "ordinary_user").strip()
    profile = _DEERFLOW_COMPLIANCE_PROFILES.get(identity_key)
    if profile is None:
        raise ValueError(f"不支持的合规身份: {identity_key}")

    user_context = {
        "user_id": f"manual-{identity_key}",
        "roles": [profile["role"]],
        "org_id": profile["org_id"],
    }
    compliance_context = {
        "source": "manual_ui",
        "enabled": True,
        "identity": identity_key,
        "scene": profile["scene"],
        "user_context": user_context,
    }
    return user_context, compliance_context


def _build_sse_payload(role, content, conversation_id, status=None, metadata=None):
    """把 DeerFlow segment 转成移动侧前端已支持的 SSE payload。"""
    event_type = "系统" if role == "系统" else "agent"
    payload = {
        "type": event_type,
        "role": role,
        "content": content,
        "conversation_id": conversation_id,
    }
    if status:
        payload["status"] = status
    if metadata:
        payload["metadata"] = metadata
    return payload


def _yield_sse(payload):
    """统一输出移动侧 SSE data 行。"""
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"


def _yield_sse_comment(content):
    """输出 SSE 注释行，用于 heartbeat/keepalive。"""
    safe_content = str(content).replace("\n", " ").strip()
    return f": {safe_content}\n\n"


def _get_deerflow_public_base_url():
    """获取附件下载链接的公开前缀；未配置时不改写相对链接。"""
    public_base = setting("DEERFLOW_PUBLIC_BASE_URL", "").strip().rstrip("/")
    if public_base:
        return public_base
    return ""


def _rewrite_artifact_url(url, conversation_id=None):
    """把 B 侧返回的 /api/threads/... 相对链接按需改写为完整链接。"""
    if not isinstance(url, str) or not url:
        return url
    return _normalize_deerflow_artifact_url(url)


def _rewrite_artifact_links_in_text(content, conversation_id=None):
    """按需把文本里的 /api/threads/... 相对下载链接改为完整链接。"""
    if not isinstance(content, str):
        return content
    if "/api/threads/" not in content:
        return content
    pattern = re.compile(r"(?:https?://[^/\s)]+)?/api/threads/[^\s)]+")

    def replace_link(match):
        raw_url = match.group(0)
        trailing = ""
        while raw_url and raw_url[-1] in ".,，。;；":
            trailing = raw_url[-1] + trailing
            raw_url = raw_url[:-1]
        return _rewrite_artifact_url(raw_url, conversation_id=conversation_id) + trailing

    return pattern.sub(replace_link, content)


def _rewrite_artifact_links_in_metadata(metadata, conversation_id=None):
    """按需改写 metadata.files[].artifact_url。"""
    if not isinstance(metadata, dict):
        return metadata
    files = metadata.get("files")
    if isinstance(files, list):
        for item in files:
            if isinstance(item, dict) and "artifact_url" in item:
                item["artifact_url"] = _rewrite_artifact_url(
                    item.get("artifact_url"), conversation_id=conversation_id
                )
                if item.get("markdown_artifact_url"):
                    item["markdown_artifact_url"] = _rewrite_artifact_url(
                        item.get("markdown_artifact_url"), conversation_id=conversation_id
                    )
    return metadata



def _derive_deerflow_url_from_run(suffix):
    """基于 DEERFLOW_API_URL 推导同一 DeerFlow 服务下的其他接口地址。"""
    api_url = setting("DEERFLOW_API_URL", "").strip()
    if not api_url:
        return ""

    parsed = urlparse(api_url)
    if not parsed.scheme or not parsed.netloc:
        return ""

    base = f"{parsed.scheme}://{parsed.netloc}"
    return f"{base}/{suffix.lstrip('/')}"



def _normalize_deerflow_thread_id(raw_thread_id=None, conversation_id=None):
    """
    将 A 侧城市超脑会话 ID 稳定映射为 UUID。

    说明：新版链路优先使用 B 侧 ensure 接口返回的真实 thread_id；
    这个函数只作为上传 URL 等兜底场景使用，不能替代 ensure。
    """
    raw_value = str(raw_thread_id or conversation_id or "").strip()

    if raw_value:
        try:
            return str(uuid.UUID(raw_value))
        except Exception:
            return str(uuid.uuid5(uuid.NAMESPACE_URL, f"onecityagent-citybrain-thread:{raw_value}"))

    return str(uuid.uuid4())


_DEERFLOW_THREAD_LOCKS = tuple(Lock() for _ in range(64))


def _deerflow_thread_cache_key(user_id, conversation_id):
    """Prevent one platform user from reusing another user's thread cache entry."""
    return f"{str(user_id or '').strip()}\x00{str(conversation_id or '').strip()}"


def _is_uuid_like(value):
    """判断一个值是否是 DeerFlow/LangGraph 可用的 UUID thread_id。"""
    try:
        uuid.UUID(str(value or "").strip())
        return True
    except Exception:
        return False


def _resolve_deerflow_thread_unlocked(
    conversation_id,
    user_id=None,
    explicit_thread_id=None,
    source="citybrain",
    authorization_header=None,
):
    """Resolve only through DeerFlow's persisted, authenticated owner mapping.

    Browser UUIDs are hints, never authority. Rechecking ensure on every request
    keeps mappings stable across restarts/processes and applies revoked grants.
    """
    safe_conversation_id = str(conversation_id or "").strip() or uuid.uuid4().hex
    explicit = str(explicit_thread_id or "").strip()

    thread_info = _ensure_deerflow_thread(
        safe_conversation_id,
        user_id=user_id,
        source=source,
        authorization_header=authorization_header,
    )
    if explicit and explicit != thread_info["thread_id"]:
        raise PermissionError("The requested thread does not belong to this conversation")
    thread_info["source"] = "deerflow_ensure"
    return thread_info


def _resolve_deerflow_thread(
    conversation_id,
    user_id=None,
    explicit_thread_id=None,
    source="citybrain",
    authorization_header=None,
):
    """Serialize first-time thread creation for this user and conversation."""
    cache_key = _deerflow_thread_cache_key(user_id, conversation_id)
    lock = _DEERFLOW_THREAD_LOCKS[hash(cache_key) % len(_DEERFLOW_THREAD_LOCKS)]
    with lock:
        return _resolve_deerflow_thread_unlocked(
            conversation_id,
            user_id=user_id,
            explicit_thread_id=explicit_thread_id,
            source=source,
            authorization_header=authorization_header,
        )



def _get_deerflow_thread_ensure_url():
    """获取 B 侧 DeerFlow mobile 真实 thread 确保接口。"""
    explicit = setting("DEERFLOW_THREAD_ENSURE_URL", "").strip()
    if explicit:
        return explicit
    return _derive_deerflow_url_from_run("api/deerflow/mobile/threads/ensure")


def _ensure_deerflow_thread(
    conversation_id,
    user_id=None,
    source="citybrain",
    authorization_header=None,
):
    """
    确保当前 citybrain 会话对应一个真实存在的 DeerFlow/LangGraph thread。

    注意：
    - 不再由 A 侧自己生成 UUID 当 DeerFlow thread_id；
    - 必须调用 B 侧 /api/deerflow/mobile/threads/ensure；
    - 返回的 thread_id 才能用于 /api/threads/{thread_id}/uploads 和 /api/deerflow/mobile/run。
    """
    ensure_url = _get_deerflow_thread_ensure_url()
    if not ensure_url:
        raise RuntimeError("DeerFlow is not configured")
    safe_conversation_id = str(conversation_id or "").strip() or uuid.uuid4().hex
    payload = {
        "conversation_id": safe_conversation_id,
        "source": source,
        "user_id": str(user_id or "public"),
    }

    timeout = int(setting("DEERFLOW_ENSURE_TIMEOUT", "120"))
    logger.info("Ensuring authenticated DeerFlow conversation")

    response = requests.post(
        ensure_url,
        json=payload,
        headers={"Authorization": authorization_header} if authorization_header else {},
        allow_redirects=False,
        timeout=(10, timeout),
    )
    response.raise_for_status()
    if not 200 <= response.status_code < 300:
        raise RuntimeError("DeerFlow returned a redirect or invalid status")

    try:
        raw_data = response.json()
    except Exception as e:
        raise RuntimeError("DeerFlow ensure returned invalid JSON") from e

    data = raw_data.get("data") if isinstance(raw_data, dict) else None
    if not isinstance(data, dict):
        data = raw_data if isinstance(raw_data, dict) else {}

    thread_id = data.get("thread_id")
    if not thread_id and isinstance(raw_data, dict):
        thread_id = raw_data.get("thread_id")
    assistant_id = data.get("assistant_id") if isinstance(data, dict) else None
    created = data.get("created") if isinstance(data, dict) else None

    if not _is_uuid_like(thread_id):
        raise RuntimeError("DeerFlow ensure did not return a valid thread UUID")

    return {
        "conversation_id": safe_conversation_id,
        "thread_id": str(thread_id),
        "assistant_id": assistant_id,
        "created": created,
        "raw": raw_data,
    }


def _extract_platform_token(authorization_header):
    if not isinstance(authorization_header, str):
        return ""
    value = str(authorization_header or "").strip()
    if value.lower() == "bearer":
        return ""
    if value.lower().startswith("bearer "):
        value = value[7:].strip()
    if any(character.isspace() for character in value):
        return ""
    return value


def _deerflow_error_details(error):
    """Keep useful upstream status without disclosing URLs, tokens or bodies."""
    upstream = error.response if isinstance(error, requests.HTTPError) else None
    upstream_status = upstream.status_code if upstream is not None else None
    status = upstream_status if upstream_status in {400, 401, 403, 404, 409, 422, 429} else 502
    if isinstance(error, PermissionError) or status in {403, 404}:
        status = 404
    if isinstance(error, requests.Timeout):
        status = 504
    messages = {
        400: "请求参数无效",
        401: "登录已失效，请重新登录",
        404: "会话或文件不存在",
        409: "当前会话已有任务正在执行，请稍后重试",
        422: "请求参数无效",
        429: "并发名额已满，请稍后重试",
        502: "DeerFlow 服务暂不可用，请稍后重试",
        504: "DeerFlow 响应超时，请稍后重试",
    }
    headers = {}
    retry_after = upstream.headers.get("Retry-After", "") if upstream is not None else ""
    if status == 429 and retry_after.isdigit() and len(retry_after) <= 6:
        headers["Retry-After"] = retry_after
    logger.warning("DeerFlow request failed: status={}, error_type={}", status, type(error).__name__)
    return status, messages[status], headers


def _deerflow_error_response(error, data=None):
    status, message, headers = _deerflow_error_details(error)
    return jsonify({"code": status, "message": message, "error": message, "data": data or {}}), status, headers


def _load_deerflow_platform_user(authorization_header):
    """Resolve a live OneCityAgent access token to its database user."""
    token = _extract_platform_token(authorization_header)
    if not token:
        return None
    try:
        with DB.connection_context():
            user = User.get_or_none(
                (User.access_token == token)
                & (User.status == StatusEnum.VALID.value)
                & (User.is_active == "1")
                & (User.is_authenticated == "1")
            )
            if user is None:
                return None
            role = Role.get_or_none(
                (Role.id == user.role) & (Role.status == StatusEnum.VALID.value)
            ) if user.role else None
            memberships = list(
                UserTenant.select(UserTenant.tenant_id)
                .join(Tenant, on=(Tenant.id == UserTenant.tenant_id))
                .where(
                    (UserTenant.user_id == user.id)
                    & (UserTenant.status == StatusEnum.VALID.value)
                    & (Tenant.status == StatusEnum.VALID.value)
                )
                .order_by((UserTenant.role == 1).desc(), UserTenant.tenant_id)
                .tuples()
            )
        if not memberships:
            return None
        return {
            "user_id": str(user.id),
            "role_id": str(user.role or ""),
            "role_code": str(role.code if role else ""),
            "org_id": str(memberships[0][0]),
        }
    except Exception as exc:
        logger.error(f"OneCityAgent DeerFlow identity lookup failed ({type(exc).__name__})")
        return None


def _deerflow_permissions(role_code):
    """Return explicitly configured extra privileges plus the safe base set."""
    allowed = {"analysis", "upload"}
    raw = setting("ONECITY_DEERFLOW_ROLE_PERMISSIONS", "").strip()
    if not raw or not role_code:
        return sorted(allowed)
    try:
        mapping = json.loads(raw)
        configured = mapping.get(role_code, []) if isinstance(mapping, dict) else []
        if not isinstance(configured, list):
            raise ValueError("role permissions must be a JSON list")
        valid = {"analysis", "upload", "export", "share", "public_release", "research"}
        allowed.update(str(item) for item in configured if str(item) in valid)
        return sorted(allowed)
    except Exception as exc:
        logger.error(f"Invalid ONECITY_DEERFLOW_ROLE_PERMISSIONS ({type(exc).__name__})")
        raise


def _deerflow_identity_class(role_code):
    raw = setting("ONECITY_DEERFLOW_IDENTITY_CLASS_MAP", "").strip()
    if not raw or not role_code:
        return "ordinary_user"
    mapping = json.loads(raw)
    identity_class = mapping.get(role_code, "ordinary_user") if isinstance(mapping, dict) else "ordinary_user"
    allowed = {"ordinary_user", "internal_operator", "cross_org_operator", "publisher", "researcher"}
    if identity_class not in allowed:
        raise ValueError("identity class map contains an unsupported profile")
    return identity_class


def _deerflow_actor(platform_user):
    return {
        "user_id": platform_user["user_id"],
        "org_id": platform_user["org_id"],
        "identity_class": _deerflow_identity_class(platform_user["role_code"]),
        "roles": [platform_user["role_code"]] if platform_user["role_code"] else [],
        "permissions": _deerflow_permissions(platform_user["role_code"]),
    }


def _deerflow_identity_mapping_configured(role_code):
    raw = setting("ONECITY_DEERFLOW_IDENTITY_CLASS_MAP", "").strip()
    if not raw or not role_code:
        return False
    try:
        mapping = json.loads(raw)
    except Exception:
        return False
    return isinstance(mapping, dict) and role_code in mapping


def _deerflow_resource_decision(platform_user, body):
    operation_type = str(body.get("operation_type") or "")
    target_audience = str(body.get("target_audience") or "")
    resource_id = str(body.get("resource_id") or "").strip()
    allowed_operations = set(_deerflow_permissions(platform_user["role_code"]))
    if operation_type not in allowed_operations or target_audience != "self" or not resource_id:
        return None
    if body.get("conversation_id") and str(body["conversation_id"]).strip() != resource_id:
        return None

    try:
        with DB.connection_context():
            dialog = Dialog.get_or_none(
                (Dialog.id == resource_id) & (Dialog.status == 1)
            )
        if dialog is None or str(dialog.creator_id) != platform_user["user_id"]:
            return None
        if str(dialog.tenant_id or "") != platform_user["org_id"]:
            return None
        if body.get("target_org_id") and str(body["target_org_id"]) != str(dialog.tenant_id):
            return None
        return {
            "allowed": True,
            "permission_verified": True,
            "operation": {"type": operation_type, "target_audience": target_audience},
            "resource": {
                "resource_id": resource_id,
                "owner_user_id": str(dialog.creator_id),
                "owner_org_id": str(dialog.tenant_id),
                "owner_department_id": None,
                "target_org_id": str(body["target_org_id"]) if body.get("target_org_id") else None,
                "classification": "internal",
                "is_anonymized": False,
            },
        }
    except Exception as exc:
        logger.error(f"OneCityAgent DeerFlow resource lookup failed ({type(exc).__name__})")
        return None


@blueprint.route("/internal/deerflow/iam", methods=["POST"])
def deerflow_iam_bridge():
    """Validate a platform token and authorize an owned OneCity dialog."""
    platform_user = _load_deerflow_platform_user(request.headers.get("Authorization"))
    if platform_user is None:
        return jsonify({"error": "unauthenticated"}), 401

    try:
        actor = _deerflow_actor(platform_user)
    except Exception as exc:
        logger.error(f"OneCityAgent DeerFlow IAM configuration failed ({type(exc).__name__})")
        return jsonify({"error": "iam_unavailable"}), 503

    body = request.get_json(silent=True) or {}
    if not isinstance(body, dict):
        return jsonify({"error": "invalid_request"}), 400
    if body.get("action") == "authenticate":
        return jsonify({"principal": actor}), 200
    if body.get("action") != "authorize":
        return jsonify({"error": "invalid_action"}), 400

    decision = _deerflow_resource_decision(platform_user, body)
    if decision is None:
        return jsonify({"error": "forbidden"}), 403
    return jsonify({"decision": decision}), 200


@blueprint.route("/query/deerflow/identity", methods=["GET"])
def deerflow_identity_summary():
    """Expose the server-resolved OneCity identity for the authenticated UI."""
    platform_user = _load_deerflow_platform_user(request.headers.get("Authorization"))
    if platform_user is None:
        return jsonify({"code": 401, "message": "请先登录 OneCityAgent"}), 401

    try:
        actor = _deerflow_actor(platform_user)
    except Exception as exc:
        logger.error(f"OneCityAgent DeerFlow identity configuration failed ({type(exc).__name__})")
        return jsonify({"code": 503, "message": "身份配置暂不可用"}), 503

    return jsonify({
        "code": 0,
        "data": {
            "user_id": actor["user_id"],
            "org_id": actor["org_id"],
            "role_code": platform_user["role_code"],
            "identity_class": actor["identity_class"],
            "roles": actor["roles"],
            "permissions": actor["permissions"],
            "identity_mapping_configured": _deerflow_identity_mapping_configured(platform_user["role_code"]),
        },
    }), 200


@blueprint.route("/query/deerflow/dialog", methods=["POST"])
def create_deerflow_dialog():
    """Persist a CityBrain dialog and bind it to the authenticated user."""
    platform_user = _load_deerflow_platform_user(request.headers.get("Authorization"))
    if platform_user is None:
        return jsonify({"code": 401, "message": "请先登录 OneCityAgent"}), 401

    body = request.get_json(silent=True) or {}
    if not isinstance(body, dict):
        return jsonify({"code": 400, "message": "请求参数无效"}), 400
    conversation_id = str(body.get("conversation_id") or "").strip()
    name = str(body.get("name") or "城市超脑会话").strip()[:255]
    if not re.fullmatch(r"citybrain-[A-Za-z0-9-]{1,22}", conversation_id):
        return jsonify({"code": 400, "message": "无效的城市超脑会话 ID"}), 400

    try:
        with DB.connection_context():
            existing = Dialog.get_or_none(Dialog.id == conversation_id)
            if existing is not None:
                if (str(existing.creator_id) != platform_user["user_id"]
                        or str(existing.tenant_id) != platform_user["org_id"] or existing.status != 1):
                    return jsonify({"code": 404, "message": "会话不存在"}), 404
                return jsonify({"code": 0, "data": {"dialog_id": conversation_id}}), 200

            now = datetime.now()
            Dialog.create(
                id=conversation_id,
                name=name or "城市超脑会话",
                creator_id=platform_user["user_id"],
                tenant_id=platform_user["org_id"],
                description="OneCityAgent DeerFlow conversation",
                status=1,
                finished=0,
                create_date=now,
                update_date=now,
            )
        return jsonify({"code": 0, "data": {"dialog_id": conversation_id}}), 200
    except Exception as exc:
        # A retry or a parallel browser request may win the unique-ID insert.
        # Treat that as idempotent only when the persisted owner is the caller.
        try:
            with DB.connection_context():
                existing = Dialog.get_or_none(Dialog.id == conversation_id)
            if (existing is not None and str(existing.creator_id) == platform_user["user_id"]
                    and str(existing.tenant_id) == platform_user["org_id"] and existing.status == 1):
                return jsonify({"code": 0, "data": {"dialog_id": conversation_id}}), 200
            if existing is not None:
                return jsonify({"code": 404, "message": "会话不存在"}), 404
        except Exception:
            pass
        logger.error(f"OneCityAgent DeerFlow dialog creation failed ({type(exc).__name__})")
        return jsonify({"code": 500, "message": "会话创建失败"}), 500


def _get_deerflow_thread_upload_url(thread_id):
    """DeerFlow 原生线程级文件上传接口：/api/threads/{thread_id}/uploads。"""
    explicit = setting("DEERFLOW_THREAD_UPLOAD_URL", "").strip()
    safe_thread_id = _normalize_deerflow_thread_id(thread_id, thread_id)
    if explicit:
        return explicit.format(thread_id=safe_thread_id)
    return _derive_deerflow_url_from_run(f"api/threads/{safe_thread_id}/uploads")


def _get_deerflow_public_base_url():
    """获取浏览器可访问的 DeerFlow 公网基地址。"""
    explicit = setting("DEERFLOW_PUBLIC_BASE_URL", "").strip().rstrip("/")
    if explicit:
        return explicit

    api_url = setting("DEERFLOW_API_URL", "").strip()
    parsed = urlparse(api_url) if api_url else None
    if parsed and parsed.scheme and parsed.netloc:
        host = parsed.hostname or ""
        if host.startswith("127.") or host == "localhost" or host.startswith("172.") or host.startswith("10."):
            return ""
        return f"{parsed.scheme}://{parsed.netloc}"

    return ""


def _normalize_deerflow_artifact_url(url_value):
    """把 B 侧返回的相对 artifact_url 转成可给浏览器点击的完整 URL。"""
    if not isinstance(url_value, str) or not url_value:
        return ""
    if url_value.startswith("http://") or url_value.startswith("https://"):
        return url_value
    if url_value.startswith("/"):
        return _get_deerflow_public_base_url() + url_value
    return url_value


def _get_deerflow_internal_file_url(file_url):
    """把浏览器可访问的 DeerFlow 文件 URL 转成 A 容器内可访问的 URL。"""
    if not isinstance(file_url, str) or not file_url:
        return file_url

    public_url = _normalize_deerflow_artifact_url(file_url)
    internal_base = setting("DEERFLOW_FILE_INTERNAL_BASE", "").strip().rstrip("/")
    parsed_file_url = urlparse(public_url)

    if internal_base and parsed_file_url.path:
        return internal_base + parsed_file_url.path + (f"?{parsed_file_url.query}" if parsed_file_url.query else "")

    api_url = setting("DEERFLOW_API_URL", "").strip()
    parsed_api_url = urlparse(api_url) if api_url else None
    if parsed_api_url and parsed_api_url.scheme and parsed_api_url.netloc and parsed_file_url.path:
        return f"{parsed_api_url.scheme}://{parsed_api_url.netloc}{parsed_file_url.path}" + (f"?{parsed_file_url.query}" if parsed_file_url.query else "")

    return public_url


def _safe_deerflow_internal_file_url(file_url):
    """Accept only DeerFlow thread artifact paths on configured service hosts."""
    public_url = _normalize_deerflow_artifact_url(file_url)
    parsed = urlparse(public_url)
    decoded_path = unquote(parsed.path)
    if "\\" in decoded_path or any(part in {".", ".."} for part in decoded_path.split("/")):
        return None
    path_parts = decoded_path.split("/")
    if len(path_parts) < 6 or path_parts[1:3] != ["api", "threads"] or path_parts[4] != "artifacts":
        return None
    if not _is_uuid_like(path_parts[3]) or not path_parts[5]:
        return None

    if parsed.netloc:
        allowed_origins = set()
        for configured_url in (
            _get_deerflow_public_base_url(),
            setting("DEERFLOW_API_URL", "").strip(),
            setting("DEERFLOW_FILE_INTERNAL_BASE", "").strip(),
        ):
            configured = urlparse(configured_url) if configured_url else None
            if configured and configured.scheme and configured.netloc:
                allowed_origins.add((configured.scheme.lower(), configured.netloc.lower()))
        if (parsed.scheme.lower(), parsed.netloc.lower()) not in allowed_origins:
            return None

    # Ignore caller-supplied query parameters; only the fixed download flag is needed.
    return _get_deerflow_internal_file_url(parsed.path + "?download=true")


def _authorized_deerflow_file_url(file_url, conversation_id, platform_user, authorization_header):
    """Bind a file request to this exact authorized dialog and persisted thread."""
    internal_url = _safe_deerflow_internal_file_url(file_url)
    if internal_url is None:
        raise PermissionError("Invalid artifact URL")
    thread_info = _resolve_deerflow_thread(
        conversation_id,
        user_id=platform_user["user_id"],
        authorization_header=authorization_header,
    )
    artifact_thread_id = unquote(urlparse(internal_url).path).split("/")[3]
    if artifact_thread_id != thread_info["thread_id"]:
        raise PermissionError("Artifact belongs to another conversation")
    return internal_url


def _get_file_extension(filename_or_url):
    raw = str(filename_or_url or "").split("?")[0].split("#")[0]
    name = raw.rsplit("/", 1)[-1]
    if "." not in name:
        return ""
    return name.rsplit(".", 1)[-1].lower()


def _normalize_uploaded_files_response(raw_data, thread_id=None, conversation_id=None):
    """兼容 DeerFlow 原生线程上传返回结构，统一提取 files。"""
    if not isinstance(raw_data, dict):
        return []

    files = raw_data.get("files")
    if not isinstance(files, list) and isinstance(raw_data.get("data"), dict):
        files = raw_data.get("data", {}).get("files")
    if not isinstance(files, list):
        return []

    normalized = []
    for item in files:
        if not isinstance(item, dict):
            continue

        artifact_url_raw = item.get("artifact_url") or item.get("url") or ""
        markdown_artifact_url_raw = item.get("markdown_artifact_url") or ""
        artifact_url_public = _rewrite_artifact_url(
            artifact_url_raw, conversation_id=conversation_id
        )
        markdown_artifact_url_public = _rewrite_artifact_url(
            markdown_artifact_url_raw, conversation_id=conversation_id
        )
        filename = item.get("filename") or item.get("original_filename") or ""
        markdown_file = item.get("markdown_file") or ""

        normalized.append({
            "filename": filename,
            "original_filename": item.get("original_filename") or filename,
            "size": item.get("size"),
            "mime_type": item.get("mime_type") or item.get("content_type") or "",
            "path": item.get("path") or "",
            "virtual_path": item.get("virtual_path") or "",
            "artifact_url": artifact_url_public,
            "markdown_file": markdown_file,
            "markdown_path": item.get("markdown_path") or "",
            "markdown_virtual_path": item.get("markdown_virtual_path") or "",
            "markdown_artifact_url": markdown_artifact_url_public,
            "url": artifact_url_public,
            "preview_url": markdown_artifact_url_public or artifact_url_public,
            "thread_id": thread_id or item.get("thread_id") or "",
        })
    return normalized


def _normalize_attachments_param(raw_value):
    """解析前端通过 query 参数传来的 attachments JSON，保留 B 侧需要的字段。"""
    if not raw_value:
        return []
    try:
        attachments = json.loads(raw_value)
    except Exception as e:
        logger.warning(f"attachments 参数不是合法 JSON，已忽略: {e}")
        return []

    if not isinstance(attachments, list):
        return []

    normalized = []
    for item in attachments:
        if not isinstance(item, dict):
            continue

        filename = item.get("filename") or item.get("original_filename") or ""
        normalized_item = {
            "filename": filename,
            "size": item.get("size"),
            "virtual_path": item.get("virtual_path") or "",
            "artifact_url": item.get("artifact_url") or item.get("url") or "",
            "markdown_virtual_path": item.get("markdown_virtual_path") or "",
            "markdown_artifact_url": item.get("markdown_artifact_url") or item.get("preview_url") or "",
        }

        if item.get("mime_type"):
            normalized_item["mime_type"] = item.get("mime_type")
        if item.get("path"):
            normalized_item["path"] = item.get("path")
        if item.get("markdown_path"):
            normalized_item["markdown_path"] = item.get("markdown_path")
        if item.get("thread_id"):
            normalized_item["thread_id"] = item.get("thread_id")
        if item.get("file_id"):
            normalized_item["file_id"] = item.get("file_id")

        if any(normalized_item.values()):
            normalized.append(normalized_item)

    return normalized


def _attach_files_to_payload(payload, attachments, thread_id=None):
    """按 B 侧最新约定把上传文件信息放入 attachments，并显式携带 thread_id。"""
    if thread_id:
        payload["thread_id"] = thread_id
    if attachments:
        payload["attachments"] = attachments
    return payload


def _mock_deerflow_segments(query):
    """没有真实 DeerFlow adapter 时，用固定 mock 数据验证链路。"""
    return [
        {
            "agent_name": "意图识别智能体",
            "content": (
                "路由任务：城市超脑平台已接收用户问题。\n"
                f"用户问题：{query}\n"
                "识别场景：DeerFlow 集成测试；场景模式：multi；"
                "候选场景：phone_network, policy_regulation。"
            ),
            "status": "completed",
        },
        {
            "agent_name": "规划智能体",
            "content": [
                "解析用户问题并确定需要调用 DeerFlow",
                "由 DeerFlow 侧完成意图识别、任务规划、Skill/RAG/工具调用",
                "接收 DeerFlow adapter 返回的 ordered segments",
                "将 segments 转换为移动侧 SSE 智能体卡片格式",
            ],
            "status": "completed",
        },
        {
            "agent_name": "系统",
            "content": "执行第1步：调用 DeerFlow adapter，获取意图识别和规划结果。",
            "status": "running",
        },
        {
            "agent_name": "执行智能体",
            "content": "模拟执行：已从 DeerFlow adapter 获取意图识别结果和任务规划结果。",
            "status": "completed",
        },
        {
            "agent_name": "反馈智能体",
            "content": str({
                "执行是否正确": True,
                "反馈建议": "第1步完成，继续模拟 Skill/RAG 执行过程。"
            }),
            "status": "completed",
        },
        {
            "agent_name": "系统",
            "content": "执行第2步：模拟 DeerFlow 调用电话网络分析 Skill 和法规检索 Skill。",
            "status": "running",
        },
        {
            "agent_name": "执行智能体",
            "content": (
                "模拟执行：已完成重点号码筛选、异常通信特征分析、政策法规检索，"
                "并整理为移动侧可展示的执行摘要。"
            ),
            "status": "completed",
        },
        {
            "agent_name": "反馈智能体",
            "content": str({
                "执行是否正确": True,
                "反馈建议": "第2步完成，DeerFlow mock segments 已按顺序返回。"
            }),
            "status": "completed",
        },
        {
            "agent_name": "系统",
            "content": "执行第3步：汇总 DeerFlow 输出，生成最终反馈。",
            "status": "running",
        },
        {
            "agent_name": "执行智能体",
            "content": "模拟执行：已完成 DeerFlow 结果汇总，并准备结束城市超脑平台任务。",
            "status": "completed",
        },
        {
            "agent_name": "反馈智能体",
            "content": str({
                "执行是否正确": True,
                "反馈建议": "城市超脑平台 mock 链路已跑通。后续可将 mock 替换为真实 DeerFlow adapter 返回的 segments。"
            }),
            "status": "completed",
        },
    ]


def _extract_deerflow_segments(raw_data):
    """兼容 B 侧 DeerFlow adapter 可能返回的几种非流式外层结构。"""
    if isinstance(raw_data, list):
        return raw_data

    if not isinstance(raw_data, dict):
        return []

    for key in ("ordered_segments", "segments", "events", "messages", "data", "result"):
        value = raw_data.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            nested = _extract_deerflow_segments(value)
            if nested:
                return nested
    return []


def _segment_sort_key(item):
    """优先按 order 排序；没有 order 的保持相对靠后。"""
    if isinstance(item, dict):
        order = item.get("order")
        try:
            return int(order)
        except (TypeError, ValueError):
            return 10 ** 9
    return 10 ** 9


def _normalize_single_deerflow_segment(item, query=None, conversation_id=None):
    """把 B 侧单条 segment 统一成 A 侧移动端可展示的 segment。"""
    if not isinstance(item, dict):
        return {
            "agent_name": "执行智能体",
            "content": str(item),
            "status": "completed",
            "metadata": {"source": "deerflow"},
        }

    # 兼容 event: segment 的 data 外层可能包一层 segment/data。
    if isinstance(item.get("segment"), dict):
        item = item.get("segment")
    elif isinstance(item.get("data"), dict) and not (item.get("role") or item.get("agent_name")):
        item = item.get("data")

    role = item.get("agent_name") or item.get("role") or item.get("agent") or item.get("type") or "执行智能体"
    if role == "agent":
        role = "执行智能体"
    if role not in _ALLOWED_DEERFLOW_ROLES:
        logger.warning(f"DeerFlow segment role 不在移动侧允许范围内: {role}，已降级为执行智能体")
        role = "执行智能体"

    content = _rewrite_artifact_links_in_text(
        item.get("content", ""), conversation_id=conversation_id
    )
    metadata = item.get("metadata") or {}
    if isinstance(metadata, dict):
        metadata.setdefault("source", "deerflow")
        metadata = _rewrite_artifact_links_in_metadata(metadata, conversation_id=conversation_id)

    return {
        "agent_name": role,
        "content": content,
        "status": item.get("status") or "completed",
        "metadata": metadata,
    }


def _normalize_deerflow_segments(raw_data, query, conversation_id=None):
    """兼容 B 侧 DeerFlow adapter 非流式返回的几种格式，统一成 ordered segments。"""
    if raw_data is None:
        raise RuntimeError("DeerFlow did not return a result")

    segments = _extract_deerflow_segments(raw_data)
    if not isinstance(segments, list) or not segments:
        raise RuntimeError("DeerFlow did not return valid result segments")

    segments = sorted(segments, key=_segment_sort_key)
    return [
        _normalize_single_deerflow_segment(item, query=query, conversation_id=conversation_id)
        for item in segments
    ]


def _load_deerflow_segments(
    query,
    conversation_id,
    user_id,
    attachments=None,
    thread_id=None,
    compliance_identity="ordinary_user",
    authorization_header=None,
):
    """非流式兜底调用；默认 /query/deerflow 优先使用 _stream_deerflow_sse_events。"""
    api_url = setting("DEERFLOW_API_URL", "").strip()
    timeout = int(setting("DEERFLOW_TIMEOUT", "1800"))

    if not api_url:
        logger.info("未配置 DEERFLOW_API_URL，/query/deerflow 使用 mock segments")
        return _mock_deerflow_segments(query)

    identity_context, compliance_context = _build_deerflow_compliance_profile(compliance_identity)
    payload = _attach_files_to_payload({
        "query": query,
        "conversation_id": conversation_id,
        "session_id": conversation_id,
        "user_id": user_id,
        "stream": False,
        "compliance_identity": identity_context,
        "compliance_context": compliance_context,
        "compliance_request": {
            "operation": {"type": "analysis", "target_audience": "self"},
            "resource": {"resource_id": conversation_id},
        },
    }, attachments or [], thread_id or conversation_id)
    logger.info("Calling authenticated DeerFlow non-streaming adapter")
    response = requests.post(
        api_url,
        json=payload,
        headers={"Authorization": authorization_header} if authorization_header else {},
        allow_redirects=False,
        timeout=(10, timeout),
    )
    response.raise_for_status()
    if not 200 <= response.status_code < 300:
        raise RuntimeError("DeerFlow returned a redirect or invalid status")
    result = response.json()
    if result.get("status") in {"failed", "incomplete", "cancelled"} or result.get("code") not in {None, 0}:
        raise RuntimeError("DeerFlow task did not complete successfully")
    return _normalize_deerflow_segments(result, query, conversation_id=conversation_id)


def _yield_mock_deerflow_events(query, conversation_id):
    """未配置真实 DeerFlow 时输出 mock SSE。"""
    yield _yield_sse({
        "type": "系统",
        "role": "系统",
        "content": "未配置 DEERFLOW_API_URL，当前使用城市超脑 mock 数据。",
        "conversation_id": conversation_id,
        "status": "running",
    })
    for seg in _mock_deerflow_segments(query):
        role = seg.get("agent_name") or seg.get("role") or "执行智能体"
        yield _yield_sse(_build_sse_payload(
            role=role,
            content=seg.get("content", ""),
            conversation_id=conversation_id,
            status=seg.get("status"),
            metadata=seg.get("metadata"),
        ))
        time.sleep(0.01)
    yield _yield_sse({
        "type": "[DONE]",
        "conversation_id": conversation_id,
        "finished": True,
    })


def _stream_deerflow_sse_events(
    query,
    conversation_id,
    user_id,
    attachments=None,
    thread_id=None,
    compliance_identity="ordinary_user",
    authorization_header=None,
    resume_run_id=None,
):
    """调用 B 侧 DeerFlow SSE 流式接口，并实时转成移动侧前端可消费的 SSE。"""
    api_url = setting("DEERFLOW_API_URL", "").strip()
    timeout = int(setting("DEERFLOW_TIMEOUT", "1800"))

    if not api_url:
        logger.info("未配置 DEERFLOW_API_URL，/query/deerflow 使用 mock segments")
        yield from _yield_mock_deerflow_events(query, conversation_id)
        return

    identity_context, compliance_context = _build_deerflow_compliance_profile(compliance_identity)
    payload = _attach_files_to_payload({
        "query": query,
        "conversation_id": conversation_id,
        "session_id": conversation_id,
        "user_id": user_id,
        "stream": True,
        "compliance_identity": identity_context,
        "compliance_context": compliance_context,
        "compliance_request": {
            "operation": {"type": "analysis", "target_audience": "self"},
            "resource": {"resource_id": conversation_id},
        },
    }, attachments or [], thread_id or conversation_id)

    logger.info("Calling authenticated DeerFlow streaming adapter")

    current_event = None
    saw_done = False

    request_upstream = requests.post
    request_arguments = {"json": payload}
    if resume_run_id:
        api_url = _derive_deerflow_url_from_run(f"api/deerflow/mobile/runs/{thread_id}/{resume_run_id}/stream")
        request_upstream = requests.get
        request_arguments = {"params": {"query": query, "conversation_id": conversation_id}}

    with request_upstream(
        api_url,
        **request_arguments,
        headers={"Authorization": authorization_header} if authorization_header else {},
        stream=True,
        allow_redirects=False,
        timeout=(10, timeout),
    ) as resp:
        resp.raise_for_status()
        if not 200 <= resp.status_code < 300:
            raise RuntimeError("DeerFlow returned a redirect or invalid status")
        yield _yield_sse({
            "type": "系统",
            "role": "系统",
            "content": "城市超脑平台已接收请求，正在调用 DeerFlow 流式分析链路。",
            "conversation_id": conversation_id,
            "status": "submitted",
        })

        # State events are deliberately small. Reading the default 512-byte
        # chunks can retain a pending/running update until the next heartbeat.
        for line in resp.iter_lines(chunk_size=1, decode_unicode=True):
            if line is None:
                continue

            line = line.strip()
            if not line:
                continue

            # B 侧如果直接输出 SSE 注释行，A 侧原样转换为注释行，保证链路活跃。
            if line.startswith(":"):
                yield _yield_sse_comment(line[1:].strip())
                continue

            if line.startswith("event:"):
                current_event = line[len("event:"):].strip()
                continue

            if not line.startswith("data:"):
                continue

            raw_data = line[len("data:"):].strip()

            # DeerFlow / LangGraph 常见结束标记：data: [DONE]
            # 这不是普通文本，不能展示到前端；应转换为 A 侧标准 DONE 事件。
            if raw_data in {"[DONE]", "DONE"}:
                saw_done = True
                yield _yield_sse({
                    "type": "[DONE]",
                    "conversation_id": conversation_id,
                    "finished": True,
                })
                break

            try:
                data = json.loads(raw_data) if raw_data else {}
            except Exception as e:
                logger.warning("DeerFlow stream contains invalid JSON")
                yield _yield_sse({
                    "type": "系统",
                    "role": "系统",
                    "content": "DeerFlow 返回了无效流式数据。",
                    "conversation_id": conversation_id,
                    "status": "running",
                })
                current_event = None
                continue

            if not isinstance(data, dict):
                raise RuntimeError("DeerFlow stream returned an invalid event")

            if current_event == "accepted":
                yield _yield_sse({
                    "type": "系统",
                    "role": "系统",
                    "content": "DeerFlow 已接收任务，正在排队或准备执行。",
                    "conversation_id": conversation_id,
                    "status": data.get("status") or "submitted",
                    "metadata": {"source": "deerflow", "source_event": "accepted"},
                })
                current_event = None
                continue

            if current_event == "start":
                metadata = {"source": "deerflow", "source_event": "start"}
                if isinstance(data, dict) and data.get("thread_id"):
                    metadata["thread_id"] = data.get("thread_id")
                yield _yield_sse({
                    "type": "系统",
                    "role": "系统",
                    "content": "DeerFlow 已提交分析任务。",
                    "conversation_id": conversation_id,
                    "status": data.get("status") or "submitted",
                    "metadata": metadata,
                })
                current_event = None
                continue

            if current_event in {"status", "heartbeat"}:
                status = data.get("status") if isinstance(data, dict) else None
                if status in {"pending", "running", "submitted"}:
                    content = {
                        "pending": "任务排队中，轮到后将自动开始处理。",
                        "running": "任务正在执行。",
                        "submitted": "任务已提交，正在确认调度状态。",
                    }[status]
                    yield _yield_sse({
                        "type": "系统",
                        "role": "系统",
                        "content": content,
                        "conversation_id": conversation_id,
                        "status": status,
                        "metadata": {
                            "source": "deerflow", "source_event": current_event,
                            "thread_id": data.get("thread_id"), "run_id": data.get("run_id"),
                        },
                    })
                else:
                    yield _yield_sse_comment("deerflow heartbeat")
                current_event = None
                continue

            if current_event == "compliance":
                yield _yield_sse({
                    "type": "compliance",
                    "role": "合规检测",
                    "content": "",
                    "conversation_id": conversation_id,
                    "status": "completed",
                    "compliance_result": data,
                    "metadata": {
                        "source": "deerflow",
                        "source_event": "compliance",
                    },
                })
                current_event = None
                continue

            if current_event == "segment" or (
                isinstance(data, dict) and (data.get("role") or data.get("agent_name") or data.get("segment"))
            ):
                seg = _normalize_single_deerflow_segment(
                    data, query=query, conversation_id=conversation_id
                )
                role = seg.get("agent_name") or "执行智能体"
                yield _yield_sse(_build_sse_payload(
                    role=role,
                    content=seg.get("content", ""),
                    conversation_id=conversation_id,
                    status=seg.get("status"),
                    metadata=seg.get("metadata"),
                ))
                current_event = None
                continue

            if current_event == "done":
                saw_done = True
                terminal_status = data.get("status") or "completed"
                successful = terminal_status in {"completed", "success", "awaiting_clarification"}
                if not successful:
                    yield _yield_sse({
                        "type": "error", "role": "系统", "conversation_id": conversation_id,
                        "content": "任务已取消。" if terminal_status == "cancelled" else "任务未能完整完成，请查看错误信息后重试。",
                        "status": terminal_status, "code": data.get("code") or 500,
                    })
                yield _yield_sse({
                    "type": "[DONE]",
                    "conversation_id": conversation_id,
                    "finished": successful,
                    **({"status": terminal_status} if not successful else {}),
                    "thread_id": data.get("thread_id") if isinstance(data, dict) else None,
                    "run_id": data.get("run_id") or resume_run_id,
                    "compliance_results": data.get("compliance_results", []) if isinstance(data, dict) else [],
                })
                break

            if current_event == "error":
                message = data.get("message") or data.get("error") or data.get("content") or str(data)
                error_payload = {
                    "type": "error",
                    "role": "系统",
                    "content": f"DeerFlow 执行失败：{message}",
                    "conversation_id": conversation_id,
                    "code": data.get("code") or 500,
                    "http_status": data.get("code") or 500,
                    "status": "failed",
                }
                if str(data.get("retry_after", "")).isdigit():
                    error_payload["retry_after"] = str(data["retry_after"])
                yield _yield_sse(error_payload)
                yield _yield_sse({
                    "type": "[DONE]",
                    "conversation_id": conversation_id,
                    "finished": False,
                })
                saw_done = True
                break

            current_event = None

    if not saw_done:
        yield _yield_sse({
            "type": "error",
            "role": "系统",
            "content": "DeerFlow 连接已中断，任务结果尚未确认，请刷新会话查看。",
            "conversation_id": conversation_id,
        })
        yield _yield_sse({
            "type": "[DONE]",
            "conversation_id": conversation_id,
            "finished": False,
        })




@blueprint.route('/query/deerflow/upload', methods=['POST'])
def upload_deerflow_files():
    """城市超脑平台文件上传代理：A 侧接收文件后转发到 DeerFlow 原生线程级上传接口。"""
    authorization_header = request.headers.get("Authorization")
    platform_user = _load_deerflow_platform_user(authorization_header)
    if platform_user is None:
        return jsonify({"code": 401, "message": "请先登录 OneCityAgent", "data": {"files": []}}), 401

    raw_thread_param = request.args.get('thread_id') or request.form.get('thread_id') or ''
    raw_conversation_id = (
        request.args.get('conversation_id')
        or request.form.get('conversation_id')
        or (raw_thread_param if raw_thread_param and not _is_uuid_like(raw_thread_param) else '')
        or uuid.uuid4().hex
    )
    explicit_thread_id = raw_thread_param if _is_uuid_like(raw_thread_param) else ''
    user_id = platform_user["user_id"]
    uploaded_files = request.files.getlist('files')
    if not uploaded_files:
        return jsonify({"code": 400, "message": "未收到上传文件", "data": {"files": []}}), 400
    if _deerflow_resource_decision(
        platform_user,
        {
            "operation_type": "upload",
            "target_audience": "self",
            "resource_id": raw_conversation_id,
        },
    ) is None:
        return jsonify({"code": 404, "message": "无权访问该会话", "data": {"files": []}}), 404

    try:
        thread_info = _resolve_deerflow_thread(
            raw_conversation_id,
            user_id=user_id,
            explicit_thread_id=explicit_thread_id,
            authorization_header=authorization_header,
        )
    except Exception as exc:
        return _deerflow_error_response(exc, {"files": []})
    conversation_id = thread_info["conversation_id"]
    thread_id = thread_info["thread_id"]
    upload_url = _get_deerflow_thread_upload_url(thread_id)

    multipart_files = []
    try:
        for file_storage in uploaded_files:
            multipart_files.append((
                'files',
                (
                    file_storage.filename,
                    file_storage.stream,
                    file_storage.mimetype or 'application/octet-stream',
                )
            ))

        logger.info("Forwarding authenticated DeerFlow upload, files={}", len(multipart_files))
        response = requests.post(
            upload_url,
            files=multipart_files,
            headers={"Authorization": authorization_header} if authorization_header else {},
            allow_redirects=False,
            timeout=(10, int(setting("DEERFLOW_UPLOAD_TIMEOUT", "600"))),
        )
        response.raise_for_status()
        if not 200 <= response.status_code < 300:
            raise RuntimeError("DeerFlow returned a redirect or invalid status")

        try:
            raw_data = response.json()
        except Exception:
            logger.warning("DeerFlow 上传接口未返回 JSON")
            return jsonify({"code": 502, "message": "DeerFlow 上传接口返回格式不是 JSON", "data": {"files": [], "thread_id": thread_id}}), 502

        if isinstance(raw_data, dict) and raw_data.get("success") is False:
            return jsonify({
                "code": 422,
                "message": raw_data.get("message") or "附件未通过合规检查，已撤回上传",
                "data": {"files": [], "thread_id": thread_id, "conversation_id": conversation_id, "compliance": raw_data.get("compliance")},
            }), 422

        files = _normalize_uploaded_files_response(
            raw_data, thread_id=thread_id, conversation_id=conversation_id
        )
        return jsonify({
            "code": 0,
            "message": raw_data.get("message") or "success" if isinstance(raw_data, dict) else "success",
            "data": {
                "files": files,
                "conversation_id": conversation_id,
                "thread_id": thread_id,
                "assistant_id": thread_info.get("assistant_id"),
                "created": thread_info.get("created"),
                "compliance": raw_data.get("compliance") if isinstance(raw_data, dict) else None,
            }
        })
    except Exception as exc:
        return _deerflow_error_response(exc, {"files": [], "thread_id": thread_id})


@blueprint.route('/query/deerflow/file/preview', methods=['GET'])
def preview_deerflow_file():
    """城市超脑平台结果文件预览代理：md/txt/csv 预览；xlsx 第一阶段提示下载。"""
    authorization_header = request.headers.get("Authorization")
    platform_user = _load_deerflow_platform_user(authorization_header)
    if platform_user is None:
        return jsonify({"code": 401, "message": "请先登录 OneCityAgent", "data": {}}), 401
    conversation_id = request.args.get("conversation_id", "").strip()
    if not conversation_id or _deerflow_resource_decision(
        platform_user,
        {
            "operation_type": "analysis",
            "target_audience": "self",
            "resource_id": conversation_id,
        },
    ) is None:
        return jsonify({"code": 404, "message": "文件不存在", "data": {}}), 404

    file_url = request.args.get('url', '').strip()
    filename = request.args.get('filename', '').strip()

    if not file_url:
        return jsonify({"code": 400, "message": "缺少文件 URL", "data": {}}), 400

    public_url = _normalize_deerflow_artifact_url(file_url)
    ext = _get_file_extension(filename or public_url)
    download_url = public_url
    try:
        internal_url = _authorized_deerflow_file_url(public_url, conversation_id, platform_user, authorization_header)
    except Exception as exc:
        return _deerflow_error_response(exc)

    if ext in {"xlsx", "xls"}:
        return jsonify({
            "code": 0,
            "message": "success",
            "data": {
                "filename": filename or public_url.rsplit('/', 1)[-1],
                "file_type": ext,
                "preview_type": "unsupported",
                "content": "Excel 文件暂不支持在线预览，请下载后查看。",
                "truncated": False,
                "url": download_url,
            }
        })

    if ext not in {"md", "txt", "csv", "json", "log"}:
        return jsonify({
            "code": 0,
            "message": "success",
            "data": {
                "filename": filename or public_url.rsplit('/', 1)[-1],
                "file_type": ext or "unknown",
                "preview_type": "unsupported",
                "content": "该类型文件暂不支持在线预览，请下载后查看。",
                "truncated": False,
                "url": download_url,
            }
        })

    max_chars = int(setting("DEERFLOW_PREVIEW_MAX_CHARS", "200000"))

    try:
        logger.info("Reading an authenticated DeerFlow artifact preview")
        response = requests.get(
            internal_url,
            headers={"Authorization": authorization_header} if authorization_header else {},
            allow_redirects=False,
            timeout=(10, int(setting("DEERFLOW_PREVIEW_TIMEOUT", "120"))),
        )
        if response.status_code in {401, 403, 404}:
            return jsonify({"code": 404, "message": "文件不存在", "data": {}}), 404
        if not 200 <= response.status_code < 300:
            return jsonify({"code": 502, "message": "DeerFlow 文件服务暂不可用", "data": {}}), 502
        response.raise_for_status()

        content_type = response.headers.get('Content-Type', '')
        if 'application/json' in content_type:
            try:
                data = response.json()
                if isinstance(data, dict) and isinstance(data.get('data'), dict) and 'content' in data.get('data', {}):
                    return jsonify(data)
            except Exception:
                pass

        response.encoding = response.encoding or 'utf-8'
        content = response.text
        truncated = False
        if len(content) > max_chars:
            content = content[:max_chars] + "\n\n……内容过长，已截断预览，请下载完整文件查看。"
            truncated = True

        preview_type = "markdown" if ext == "md" else "csv" if ext == "csv" else "text"
        return jsonify({
            "code": 0,
            "message": "success",
            "data": {
                "filename": filename or public_url.rsplit('/', 1)[-1],
                "file_type": ext,
                "preview_type": preview_type,
                "content": content,
                "truncated": truncated,
                "url": download_url,
            }
        })
    except Exception as exc:
        return _deerflow_error_response(exc)


@blueprint.route('/query/deerflow/file/download', methods=['GET'])
def download_deerflow_file():
    """Proxy a thread artifact after OneCity ownership and DeerFlow checks."""
    authorization_header = request.headers.get("Authorization")
    platform_user = _load_deerflow_platform_user(authorization_header)
    if platform_user is None:
        return jsonify({"code": 401, "message": "请先登录 OneCityAgent"}), 401

    conversation_id = request.args.get("conversation_id", "").strip()
    if not conversation_id or _deerflow_resource_decision(
        platform_user,
        {
            "operation_type": "analysis",
            "target_audience": "self",
            "resource_id": conversation_id,
        },
    ) is None:
        return jsonify({"code": 404, "message": "文件不存在"}), 404

    try:
        internal_url = _authorized_deerflow_file_url(request.args.get("url", ""), conversation_id, platform_user, authorization_header)
    except Exception as exc:
        return _deerflow_error_response(exc)

    try:
        upstream = requests.get(
            internal_url,
            headers={"Authorization": authorization_header},
            stream=True,
            allow_redirects=False,
            timeout=(10, int(setting("DEERFLOW_PREVIEW_TIMEOUT", "120"))),
        )
        if upstream.status_code in {401, 403, 404}:
            upstream.close()
            return jsonify({"code": 404, "message": "文件不存在"}), 404
        if not 200 <= upstream.status_code < 300:
            upstream.close()
            return jsonify({"code": 502, "message": "DeerFlow 文件服务暂不可用"}), 502

        filename = unquote(urlparse(internal_url).path.rsplit("/", 1)[-1]) or "download"
        safe_ascii_name = (filename.encode("ascii", "ignore").decode("ascii") or "download")
        safe_ascii_name = safe_ascii_name.replace("\r", "_").replace("\n", "_").replace('"', "_")
        headers = {
            "Content-Disposition": (
                f'attachment; filename="{safe_ascii_name}"; '
                f"filename*=UTF-8''{quote(filename)}"
            ),
            "Cache-Control": "private, no-store",
        }
        if upstream.headers.get("Content-Length"):
            headers["Content-Length"] = upstream.headers["Content-Length"]

        def stream_file():
            try:
                yield from upstream.iter_content(chunk_size=64 * 1024)
            finally:
                upstream.close()

        return Response(
            stream_file(),
            mimetype=upstream.headers.get("Content-Type", "application/octet-stream"),
            headers=headers,
            direct_passthrough=True,
        )
    except Exception as exc:
        return _deerflow_error_response(exc)

@blueprint.route('/query/deerflow', methods=['GET'])
@blueprint.route('/query/deerflow/resume', methods=['GET'])
@swag_from({
    'tags': ['Query'],
    'description': '城市超脑平台专用查询接口。接收 query 后调用 DeerFlow adapter，并返回移动侧兼容的 SSE 事件流。',
    'parameters': [
        {
            'name': 'query',
            'in': 'query',
            'type': 'string',
            'required': True,
            'description': '查询内容'
        },
        {
            'name': 'conversation_id',
            'in': 'query',
            'type': 'string',
            'required': False,
            'description': '会话ID'
        },
        {
            'name': 'user_id',
            'in': 'query',
            'type': 'string',
            'required': False,
            'description': '用户ID'
        }
    ],
    'responses': {
        200: {
            'description': 'SSE事件流',
        },
        400: {
            'description': '缺少查询参数',
        }
    }
})
def query_deerflow_sse():
    logger.info("Authenticated DeerFlow query requested")
    authorization_header = request.headers.get("Authorization")
    platform_user = _load_deerflow_platform_user(authorization_header)
    if platform_user is None:
        return jsonify({"error": "请先登录 OneCityAgent"}), 401

    query = request.args.get('query')
    if not query:
        return jsonify({"error": "缺少查询参数"}), 400
    raw_thread_param = request.args.get('thread_id') or ''
    resume_run_id = request.args.get("run_id") if request.path.endswith("/resume") else None
    if request.path.endswith("/resume") and (
        not _is_uuid_like(resume_run_id) or not _is_uuid_like(raw_thread_param)
    ):
        return jsonify({"error": "恢复任务需要有效的线程和任务ID"}), 400
    conversation_id = (
        request.args.get('conversation_id')
        or (raw_thread_param if raw_thread_param and not _is_uuid_like(raw_thread_param) else '')
        or uuid.uuid4().hex
    )
    user_id = platform_user["user_id"]
    try:
        compliance_identity = _deerflow_identity_class(platform_user["role_code"])
        decision = _deerflow_resource_decision(
            platform_user,
            {
                "operation_type": "analysis",
                "target_audience": "self",
                "resource_id": conversation_id,
            },
        )
    except Exception as exc:
        logger.error(f"OneCityAgent DeerFlow IAM configuration failed ({type(exc).__name__})")
        return jsonify({"error": "IAM配置不可用"}), 503
    if decision is None:
        return jsonify({"error": "无权访问该会话"}), 404
    try:
        _build_deerflow_compliance_profile(compliance_identity)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    explicit_thread_id = raw_thread_param if _is_uuid_like(raw_thread_param) else ''
    try:
        thread_info = _resolve_deerflow_thread(
            conversation_id,
            user_id=user_id,
            explicit_thread_id=explicit_thread_id,
            authorization_header=authorization_header,
        )
    except Exception as exc:
        return _deerflow_error_response(exc)
    conversation_id = thread_info["conversation_id"]
    thread_id = thread_info["thread_id"]
    attachments = _normalize_attachments_param(request.args.get('attachments', ''))

    use_stream = bool(resume_run_id) or setting("DEERFLOW_STREAM", "true").strip().lower() not in {"0", "false", "no"}
    upstream_events = None
    first_event = None
    segments = None
    try:
        if use_stream:
            upstream_events = _stream_deerflow_sse_events(
                query, conversation_id, user_id, attachments, thread_id,
                compliance_identity, authorization_header,
                **({"resume_run_id": resume_run_id} if resume_run_id else {}),
            )
            # Obtain the upstream HTTP result before committing our response.
            # This preserves 401/404/429 and Retry-After through fetch SSE.
            first_event = next(upstream_events)
        else:
            segments = _load_deerflow_segments(
                query, conversation_id, user_id, attachments, thread_id,
                compliance_identity, authorization_header,
            )
    except Exception as exc:
        if upstream_events is not None:
            upstream_events.close()
        return _deerflow_error_response(exc)

    def generate():
        try:
            if use_stream:
                yield first_event
                yield from upstream_events
            else:
                for seg in segments:
                    role = seg.get("agent_name") or seg.get("role") or "执行智能体"
                    if role not in _ALLOWED_DEERFLOW_ROLES:
                        role = "执行智能体"
                    payload = _build_sse_payload(
                        role=role,
                        content=seg.get("content", ""),
                        conversation_id=conversation_id,
                        status=seg.get("status"),
                        metadata=seg.get("metadata"),
                    )
                    yield _yield_sse(payload)
                    time.sleep(0.01)

                yield _yield_sse({
                    "type": "[DONE]",
                    "conversation_id": conversation_id,
                    "finished": True,
                })
        except Exception as exc:
            status, message, _ = _deerflow_error_details(exc)
            yield _yield_sse({
                "type": "error",
                "role": "系统",
                "content": message,
                "conversation_id": conversation_id,
                "http_status": status,
            })
            yield _yield_sse({
                "type": "[DONE]",
                "conversation_id": conversation_id,
                "finished": False,
            })
        finally:
            if upstream_events is not None:
                upstream_events.close()

    response = Response(stream_with_context(generate()), mimetype='text/event-stream')
    response.headers['Cache-Control'] = 'no-cache'
    response.call_on_close(lambda: upstream_events.close() if upstream_events is not None else None)
    response.headers['X-Accel-Buffering'] = 'no'
    return response


@blueprint.route('/query/deerflow/status', methods=['GET'])
@blueprint.route('/query/deerflow/cancel', methods=['POST'])
def control_deerflow_task():
    """Control an exact run only after rechecking its current dialog owner."""
    authorization = request.headers.get("Authorization")
    platform_user = _load_deerflow_platform_user(authorization)
    if platform_user is None:
        return jsonify({"code": 401, "message": "请先登录 OneCityAgent"}), 401
    values = request.get_json(silent=True) if request.method == "POST" else request.args
    if not values or (request.method == "POST" and not isinstance(values, dict)):
        return jsonify({"code": 400, "message": "缺少任务参数"}), 400
    conversation = str(values.get("conversation_id") or "").strip()
    thread_id, run_id = values.get("thread_id"), values.get("run_id")
    if not conversation or not _is_uuid_like(thread_id) or not _is_uuid_like(run_id):
        return jsonify({"code": 400, "message": "缺少有效的会话、线程或任务ID"}), 400
    try:
        if _deerflow_resource_decision(platform_user, {
            "operation_type": "analysis", "target_audience": "self", "resource_id": conversation,
        }) is None:
            raise PermissionError("Dialog not found")
        _resolve_deerflow_thread(conversation, user_id=platform_user["user_id"], explicit_thread_id=thread_id,
                                 authorization_header=authorization)
        suffix = "/cancel" if request.method == "POST" else ""
        url = _derive_deerflow_url_from_run(f"api/deerflow/mobile/runs/{thread_id}/{run_id}{suffix}")
        upstream = requests.post if suffix else requests.get
        response = upstream(url, headers={"Authorization": authorization}, timeout=(10, 30), allow_redirects=False)
        response.raise_for_status()
        if response.status_code != 200:
            raise RuntimeError("Unexpected task service response")
        data = response.json().get("data", {})
        return jsonify({"code": 0, "data": {"thread_id": str(thread_id), "run_id": str(run_id), "status": data.get("status")}})
    except Exception as exc:
        return _deerflow_error_response(exc)

