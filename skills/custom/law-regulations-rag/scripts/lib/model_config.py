import os
from pathlib import Path
from typing import Any

import yaml


def resolve_env_value(value: Any) -> Any:
    if isinstance(value, str) and value.startswith("$"):
        return os.getenv(value[1:], "")
    return value


def load_model_config(config_path: str | Path, model_name: str) -> dict:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"模型配置文件不存在: {path}")

    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    models = data.get("models", [])

    if not isinstance(models, list):
        raise ValueError(f"{path} 中的 models 必须是列表")

    for item in models:
        if not isinstance(item, dict):
            continue
        if item.get("name") == model_name:
            return {k: resolve_env_value(v) for k, v in item.items()}

    raise ValueError(f"未在 {path} 中找到模型配置: {model_name}")


def require_model_config(config_path: str | Path, model_name: str, expected_type: str | None = None) -> dict:
    cfg = load_model_config(config_path, model_name)

    if expected_type and cfg.get("type") != expected_type:
        raise ValueError(
            f"模型 {model_name} 的 type 应为 {expected_type}，实际为 {cfg.get('type')}"
        )

    return cfg