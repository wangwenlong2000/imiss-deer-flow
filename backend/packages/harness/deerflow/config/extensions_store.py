"""Atomic management updates for static or separately mounted extensions state."""

from __future__ import annotations

import fcntl
import json
import os
import tempfile
import threading
from pathlib import Path
from typing import Any, Literal

from deerflow.config.extensions_config import ExtensionsConfig

_write_lock = threading.RLock()


def update_extensions(
    section: Literal["mcpServers", "skills"],
    values: dict[str, Any],
    *,
    base_config_path: Path | None = None,
) -> None:
    """Write one management section without losing unrelated or concurrent updates.

    Skill updates merge individual names; MCP updates replace the collection, as
    required by the existing API. File locking covers the entire read-modify-write
    operation across workers. The release file remains untouched in state mode.
    """
    if section not in {"mcpServers", "skills"}:
        raise ValueError("Unsupported extensions management section")
    ExtensionsConfig.model_validate({section: values})
    base_path = base_config_path or ExtensionsConfig.resolve_config_path()
    state_path = ExtensionsConfig.resolve_state_path()
    if state_path is not None and base_path is not None and state_path.resolve() == base_path.resolve():
        raise ValueError("Extensions base configuration and runtime state must use different paths")
    target = state_path or base_path or Path.cwd().parent / "extensions_config.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    with _write_lock, (target.parent / f".{target.name}.lock").open("a", encoding="utf-8") as lock:
        os.chmod(lock.name, 0o600)
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        current = ExtensionsConfig.read_json_object(target) if target.exists() else {}
        if state_path is not None and set(current) - {"mcpServers", "skills"}:
            raise ValueError("Runtime extensions state may only contain MCP and Skill settings")
        if section == "skills":
            current[section] = {**current.get(section, {}), **values}
        else:
            current[section] = values
        ExtensionsConfig.model_validate(current)
        temporary_path = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=target.parent, prefix=f".{target.name}.", delete=False) as stream:
                temporary_path = Path(stream.name)
                json.dump(current, stream, indent=2, ensure_ascii=False)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary_path, target)
            directory = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
