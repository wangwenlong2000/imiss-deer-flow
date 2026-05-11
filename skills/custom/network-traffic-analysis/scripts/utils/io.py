from __future__ import annotations

import json
from pathlib import Path
from typing import Any

# Global module-level caches
duckdb = None
yaml = None
CACHE_DIR = None

def ensure_cache_dir() -> None:
    from constants import CACHE_DIR as _CACHE_DIR
    global CACHE_DIR
    CACHE_DIR = _CACHE_DIR
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

def check_duckdb() -> dict[str, str]:
    """Check duckdb availability without raising. For self-check use only."""
    try:
        import duckdb as _duckdb
        return {"status": "ok", "version": _duckdb.__version__}
    except ImportError as exc:
        return {"status": "failed", "error": str(exc)}


def ensure_duckdb() -> Any:
    global duckdb
    if duckdb is not None:
        return duckdb
    try:
        import duckdb as duckdb_module
    except ImportError as exc:
        raise ImportError(
            "Missing dependency 'duckdb'. Install required analyzer dependencies first: pip install duckdb openpyxl pyyaml"
        ) from exc
    duckdb = duckdb_module
    return duckdb


def ensure_yaml() -> Any:
    global yaml
    if yaml is not None:
        return yaml
    try:
        import yaml as yaml_module
    except ImportError as exc:
        raise ImportError("Missing dependency 'pyyaml'. Install it first: pip install pyyaml") from exc
    yaml = yaml_module
    return yaml


def ensure_pytz() -> None:
    try:
        import pytz  # noqa: F401
    except ImportError as exc:
        raise ImportError("Missing dependency 'pytz'. Install it first: pip install pytz") from exc


def save_json(path: Path, payload: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def load_json(path: Path) -> Any | None:
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)
