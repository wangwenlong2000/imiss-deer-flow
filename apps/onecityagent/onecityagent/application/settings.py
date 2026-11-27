"""Explicit OneCity runtime configuration.

Importing this module never reads files or process secrets. Entry points call
``load_settings_from_env``; tests and embedding applications pass a mapping to
the application factories.
"""

from __future__ import annotations

import os
import re
import tomllib
from collections.abc import Mapping
from pathlib import Path
from typing import Any


def enabled(value: Any) -> bool:
    return value is True or str(value).strip().lower() in {"1", "true", "yes", "on"}


_ENV_REFERENCE = re.compile(r"^\$([A-Za-z_][A-Za-z0-9_]*)$")


def resolve_env_reference(value: Any, *, setting_name: str) -> Any:
    """Expand a TOML value that consists solely of ``$ENV_NAME``.

    Requiring a complete environment-variable token prevents implicit shell
    interpolation or accidental transformations of ordinary strings.
    """
    if not isinstance(value, str):
        return value
    match = _ENV_REFERENCE.fullmatch(value)
    if match is None:
        return value
    name = match.group(1)
    if name not in os.environ:
        raise RuntimeError(f"{setting_name} references an unset environment variable")
    return os.environ[name]


def load_settings_from_env(*, allow_legacy_path: bool = False) -> dict[str, Any]:
    """Load a sanitized config TOML and explicit environment overrides."""
    path = os.getenv("ONECITY_CONFIG_PATH", "").strip()
    if not path and allow_legacy_path:
        path = str(Path(__file__).resolve().parents[1] / "config" / "config.toml")
    if not path:
        raise RuntimeError("ONECITY_CONFIG_PATH is required for a runtime entrypoint")
    config_path = Path(path)
    with config_path.open("rb") as source:
        raw = tomllib.load(source)

    database = raw.get("agent", {}).get("database", {})
    security = raw.get("security", raw.get("agent", {}).get("security", {}))
    a2a = raw.get("a2a", {})
    settings: dict[str, Any] = {
        "DATABASE": {
            "type": database.get("DATABASE_TYPE", "mysql"),
            "host": database.get("DATABASE_HOST", "localhost"),
            "port": database.get("DATABASE_PORT", 3306),
            "name": database.get("DATABASE_NAME", "onecityagent"),
            "user": database.get("DATABASE_USER", "root"),
            "password": resolve_env_reference(
                database.get("DATABASE_PASSWORD", ""),
                setting_name="agent.database.DATABASE_PASSWORD",
            ),
            "max_connections": database.get("DATABASE_MAX_CONNECTIONS", 100),
            "timeout": database.get("DATABASE_TIMEOUT", 300),
        },
        "SECRET_KEY": os.getenv("SECRET_KEY", ""),
        "ONECITY_RSA_PRIVATE_KEY_PATH": os.getenv(
            "ONECITY_RSA_PRIVATE_KEY_PATH",
            str(
                (
                    config_path.parent.parent
                    / security.get("PRIVATE_KEY_PATH", "config/private.pem")
                ).resolve()
            ),
        ),
        "ONECITY_RSA_PRIVATE_KEY_PASSPHRASE": os.getenv(
            "ONECITY_RSA_PRIVATE_KEY_PASSPHRASE", security.get("PRIVATE_KEY", "")
        ),
        "ONECITY_APP_PORT": int(os.getenv("ONECITY_APP_PORT", "7606")),
        "A2A_COORDINATOR_PORT": int(
            os.getenv("A2A_COORDINATOR_PORT", str(_port(a2a.get("router")) or 7010))
        ),
        "ONECITY_DEBUG": enabled(os.getenv("ONECITY_DEBUG", "false")),
        "ONECITY_SELF_REGISTER_ROLE_CODE": os.getenv(
            "ONECITY_SELF_REGISTER_ROLE_CODE", "common"
        ),
        "ONECITY_ENABLE_LEGACY": enabled(os.getenv("ONECITY_ENABLE_LEGACY", "false")),
        "ONECITY_PROJECT_ROOT": str(Path(__file__).resolve().parents[2]),
        "ONECITY_CONFIG_PATH": str(config_path.resolve()),
    }
    for key in (
        "DEERFLOW_API_URL",
        "DEERFLOW_PUBLIC_BASE_URL",
        "DEERFLOW_FILE_INTERNAL_BASE",
        "DEERFLOW_THREAD_ENSURE_URL",
        "DEERFLOW_THREAD_UPLOAD_URL",
        "DEERFLOW_STREAM",
        "DEERFLOW_TIMEOUT",
        "DEERFLOW_ENSURE_TIMEOUT",
        "DEERFLOW_UPLOAD_TIMEOUT",
        "DEERFLOW_PREVIEW_TIMEOUT",
        "DEERFLOW_PREVIEW_MAX_CHARS",
        "ONECITY_DEERFLOW_IDENTITY_CLASS_MAP",
        "ONECITY_DEERFLOW_ROLE_PERMISSIONS",
    ):
        if key in os.environ:
            settings[key] = os.environ[key]
    return settings


def _port(url: Any) -> int | None:
    if not isinstance(url, str):
        return None
    from urllib.parse import urlparse

    try:
        return urlparse(url).port
    except ValueError:
        return None


def setting(key: str, default: Any = None, *, aliases: tuple[str, ...] = ()) -> Any:
    """Read the active application's injected setting, never global env state."""
    try:
        from flask import current_app, has_app_context

        if has_app_context():
            values = current_app.extensions.get("onecity.settings", {})
            for candidate in (key, *aliases):
                if candidate in values:
                    return values[candidate]
    except (ImportError, RuntimeError):
        pass
    return default


def runtime_value(key: str, legacy_key: str, default: Any = None) -> Any:
    """Resolve a core setting with a legacy-config fallback only on demand."""
    value = setting(key, aliases=(legacy_key,), default=None)
    if value is not None:
        return value
    from flask import has_app_context

    if has_app_context():
        return default
    # Legacy tools outside the WSGI applications retain their historic behavior.
    from onecityagent.config import Config

    return getattr(Config(), legacy_key, default)


def normalize_settings(settings: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(settings, Mapping):
        raise TypeError("settings must be a mapping")
    result = dict(settings)
    database = result.get("DATABASE")
    if database is None and "database" in result:
        database = result["database"]
    if database is None:
        db_keys = ("DATABASE_TYPE", "DATABASE_HOST", "DATABASE_PORT", "DATABASE_NAME", "DATABASE_USER", "DATABASE_PASSWORD")
        if any(key in result for key in db_keys):
            database = {
                "type": result.get("DATABASE_TYPE", "mysql"),
                "host": result.get("DATABASE_HOST", "localhost"),
                "port": result.get("DATABASE_PORT", 3306),
                "name": result.get("DATABASE_NAME", "onecityagent"),
                "user": result.get("DATABASE_USER", "root"),
                "password": result.get("DATABASE_PASSWORD", ""),
                "max_connections": result.get("DATABASE_MAX_CONNECTIONS", 100),
                "timeout": result.get("DATABASE_TIMEOUT", 300),
            }
    if database is not None:
        result["DATABASE"] = dict(database)
    return result
