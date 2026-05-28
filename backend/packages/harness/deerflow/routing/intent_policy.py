"""Config-backed policy for intent routing."""

from __future__ import annotations

import copy
import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any


def _default_policy_path() -> Path:
    return Path(__file__).with_name("intent") / "router_policy.json"


def _custom_policy_paths() -> list[Path]:
    paths: list[Path] = []

    env_value = os.getenv("DEERFLOW_INTENT_ROUTER_POLICY")
    if env_value:
        for item in env_value.split(os.pathsep):
            if item.strip():
                paths.append(Path(item.strip()))

    backend_root = Path(__file__).resolve().parents[4]
    paths.append(backend_root / "config" / "intent_router_policy.json")
    return paths


@lru_cache(maxsize=8)
def load_router_policy(path: str | None = None) -> dict[str, Any]:
    policy = _load_json(Path(path) if path else _default_policy_path())
    if path is None:
        for custom_path in _custom_policy_paths():
            custom_policy = _load_json(custom_path)
            if custom_policy:
                policy = _deep_merge(policy, custom_policy)
    return policy


def clear_router_policy_cache() -> None:
    load_router_policy.cache_clear()


def policy_list(*keys: str) -> list[Any]:
    value = policy_value(*keys, default=[])
    return value if isinstance(value, list) else []


def policy_float(*keys: str) -> float:
    value = policy_value(*keys, default=None)
    try:
        return float(value)
    except (TypeError, ValueError):
        raise KeyError("Missing or invalid float policy: " + ".".join(keys)) from None


def policy_int(*keys: str) -> int:
    value = policy_value(*keys, default=None)
    try:
        return int(value)
    except (TypeError, ValueError):
        raise KeyError("Missing or invalid int policy: " + ".".join(keys)) from None


def policy_value(*keys: str, default: Any = None) -> Any:
    current: Any = load_router_policy()
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


def _load_json(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return {}
    if not isinstance(data, dict):
        return {}
    return data


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged
