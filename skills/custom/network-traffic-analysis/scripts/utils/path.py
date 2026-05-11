from __future__ import annotations

import hashlib
import json
import os
import re
from contextlib import suppress
from pathlib import Path
from typing import Any


SUPPORTED_PATTERNS = ("*.csv", "*.parquet", "*.json", "*.jsonl", "*.xlsx", "*.xls")
CACHE_DIR = Path(__import__("tempfile").gettempdir()) / ".network-traffic-analysis-cache"


def normalize_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def is_explicit_path_reference(value: str) -> bool:
    ref = value.strip()
    normalized = ref.replace("\\", "/")
    return (
        normalized.startswith("/")
        or normalized.startswith("./")
        or normalized.startswith("../")
        or bool(re.match(r"^[A-Za-z]:[/\\]", ref))
        or "/" in normalized
        or "\\" in ref
    )


def skill_root() -> Path:
    """Locate the skill root by searching upward for SKILL.md."""
    script_path = Path(__file__).resolve()  # scripts/utils/path.py
    for candidate in script_path.parents:
        if (candidate / "SKILL.md").exists():
            return candidate
    return script_path.parents[2]  # scripts/ -> custom/ -> skills/


def repo_root() -> Path:
    """Search for .git or pyproject.toml; fall back to skill_root."""
    script_path = Path(__file__).resolve()
    for candidate in script_path.parents:
        if (candidate / ".git").exists() or (candidate / "pyproject.toml").exists():
            return candidate
    return skill_root()


def dataset_root() -> Path:
    """Resolve dataset root: env var -> /mnt/datasets -> repo fallback."""
    env_path = os.environ.get("NETWORK_TRAFFIC_DATASET_ROOT")
    if env_path:
        return Path(env_path)
    mounted = Path("/mnt/datasets/network-traffic")
    if mounted.exists():
        return mounted
    return repo_root() / "datasets" / "network-traffic"


def uploads_root() -> Path:
    return Path("/mnt/user-data/uploads")


def workspace_root() -> Path:
    env_path = os.environ.get("NETWORK_TRAFFIC_WORKSPACE_ROOT")
    if env_path:
        return Path(env_path)
    return Path("/mnt/user-data/workspace")


def network_traffic_workspace_root() -> Path:
    """Domain-scoped workspace for the network-traffic-analysis skill.

    Returns $NETWORK_TRAFFIC_WORKSPACE_ROOT if set (preserving backward compat
    with scripts that passed it as the generic workspace_root override),
    otherwise /mnt/user-data/workspace/network-traffic.
    """
    env_path = os.environ.get("NETWORK_TRAFFIC_WORKSPACE_ROOT")
    if env_path:
        return Path(env_path)
    return workspace_root() / "network-traffic"


def outputs_root() -> Path:
    env_path = os.environ.get("NETWORK_TRAFFIC_OUTPUTS_ROOT")
    if env_path:
        return Path(env_path)
    return Path("/mnt/user-data/outputs")


def processed_dataset_root() -> Path:
    """Writable processed dataset root.

    Priority: NETWORK_TRAFFIC_PROCESSED_ROOT env -> dataset_root()/processed.
    """
    env = os.environ.get("NETWORK_TRAFFIC_PROCESSED_ROOT")
    if env:
        return Path(env)
    return dataset_root() / "processed"


def is_relative_to_path(path: Path, root: Path) -> bool:
    """Return True if path.resolve() is under root.resolve()."""
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def to_repo_relative_display(value: str | Path) -> str:
    path = Path(value).expanduser()
    if not path.is_absolute():
        return path.as_posix()
    try:
        return path.resolve().relative_to(repo_root()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def _resolve_reference(reference: str) -> ResolutionResult:
    """Internal resolve helper — delegates to file_resolution to avoid circular import."""
    from file_resolution import resolve_reference as _resolve
    return _resolve(reference)


def discover_files(values: list[str]) -> list[str]:
    files: list[str] = []
    for value in values:
        path = Path(value)
        if path.is_dir():
            for pattern in SUPPORTED_PATTERNS:
                files.extend(str(p) for p in sorted(path.rglob(pattern)))
        elif path.exists():
            files.append(str(path))
        elif is_explicit_path_reference(value):
            raise ValueError(f"File path '{value}' does not exist.")
        else:
            result = _resolve_reference(value)
            if result.status == "resolved":
                files.extend(result.matches)
            elif result.status == "ambiguous":
                sample = "\n".join(f"  - {to_repo_relative_display(p)}" for p in result.matches[:10])
                raise ValueError(
                    f"File reference '{value}' matched multiple datasets. "
                    f"Use a more specific path.\nCandidates:\n{sample}"
                )
            else:
                pass  # Not found; will surface later
    deduped: list[str] = []
    seen: set[str] = set()
    for item in files:
        norm = str(Path(item))
        if norm not in seen:
            deduped.append(norm)
            seen.add(norm)
    return deduped


def resolve_file_reference(reference: str) -> list[str]:
    result = _resolve_reference(reference)
    if result.status == "resolved":
        return result.matches
    if result.status == "ambiguous":
        sample = "\n".join(f"  - {to_repo_relative_display(path)}" for path in result.matches[:10])
        raise ValueError(
            f"File reference '{reference}' matched multiple datasets. "
            f"Use a more specific path.\nCandidates:\n{sample}"
        )
    raise ValueError(result.message)


def compute_cache_key(files: list[str], mapping: dict[str, Any], *, ingestion_mode: str = "lenient") -> str:
    hasher = hashlib.sha256()
    for file_path in sorted(files):
        hasher.update(file_path.encode("utf-8"))
        try:
            with open(file_path, "rb") as f:
                while chunk := f.read(8192):
                    hasher.update(chunk)
        except OSError:
            pass
    hasher.update(json.dumps(mapping, sort_keys=True).encode("utf-8"))
    hasher.update(json.dumps({
        "ingestion_mode": ingestion_mode,
        "ingestion_metadata_schema_version": 2,
    }, sort_keys=True).encode("utf-8"))
    return hasher.hexdigest()


def _metadata_candidates_for_file(file_path: Path) -> list[Path]:
    candidates = [file_path.parent / "metadata.json"]
    with suppress(Exception):
        if file_path.parent.parent != file_path.parent:
            candidates.append(file_path.parent.parent / "metadata.json")
    deduped: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate.resolve()) if candidate.exists() else str(candidate)
        if key not in seen:
            deduped.append(candidate)
            seen.add(key)
    return deduped


# Lazy import to avoid circular dependency at module load time
def __getattr__(name: str) -> Any:
    if name == "ResolutionResult":
        from file_resolution import ResolutionResult
        return ResolutionResult
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
