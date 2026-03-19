from __future__ import annotations

from dataclasses import dataclass
import re
from pathlib import Path

SUPPORTED_PATTERNS = ("*.csv", "*.parquet", "*.json", "*.jsonl", "*.xlsx", "*.xls")


def normalize_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def get_default_search_roots() -> list[Path]:
    base = repo_root() / "datasets" / "network-traffic"
    return [base / "processed", base / "raw"]


@dataclass(frozen=True)
class ResolutionResult:
    reference: str
    status: str
    matches: list[str]
    strategy: str
    message: str


def _iter_candidates(roots: list[Path]) -> list[Path]:
    candidates: list[Path] = []
    for root in roots:
        if not root.exists():
            continue
        for pattern in SUPPORTED_PATTERNS:
            candidates.extend(sorted(root.rglob(pattern)))
    return candidates


def _dedupe(paths: list[Path]) -> list[Path]:
    unique: list[Path] = []
    seen: set[str] = set()
    for path in paths:
        key = str(path.resolve()).lower()
        if key not in seen:
            unique.append(path.resolve())
            seen.add(key)
    return unique


def _has_path_hint(value: str) -> bool:
    return "/" in value or "\\" in value


def resolve_reference(reference: str) -> ResolutionResult:
    ref = reference.replace("\\", "/").strip()
    roots = [root for root in get_default_search_roots() if root.exists()]
    if not roots:
        return ResolutionResult(
            reference=reference,
            status="not_found",
            matches=[],
            strategy="none",
            message="No datasets/network-traffic/raw or processed roots are available.",
        )

    candidates = _dedupe(_iter_candidates(roots))
    ref_lower = ref.lower()
    basename = Path(ref).name.lower()
    stem = Path(ref).stem.lower()
    normalized_ref = normalize_name(ref)
    normalized_basename = normalize_name(basename)
    normalized_stem = normalize_name(stem)
    has_path_hint = _has_path_hint(ref)

    exact_name: list[Path] = []
    exact_relative_path: list[Path] = []
    rel_suffix: list[Path] = []
    full_suffix: list[Path] = []
    normalized_name: list[Path] = []
    normalized_stem_matches: list[Path] = []

    for candidate in candidates:
        full = candidate.as_posix().lower()
        name = candidate.name.lower()
        rels = [candidate.relative_to(root).as_posix().lower() for root in roots if candidate.is_relative_to(root)]

        if name == basename:
            exact_name.append(candidate)
        if any(rel == ref_lower for rel in rels):
            exact_relative_path.append(candidate)
        if any(rel.endswith(ref_lower) for rel in rels):
            rel_suffix.append(candidate)
        if full.endswith(ref_lower):
            full_suffix.append(candidate)
        if normalize_name(name) == normalized_basename or normalize_name(full) == normalized_ref:
            normalized_name.append(candidate)
        if normalized_stem and normalize_name(candidate.stem) == normalized_stem:
            normalized_stem_matches.append(candidate)

    if has_path_hint:
        strategies = [
            ("exact_relative_path", _dedupe(exact_relative_path)),
            ("full_suffix", _dedupe(full_suffix)),
            ("relative_suffix", _dedupe(rel_suffix)),
            ("normalized_name", _dedupe(normalized_name)),
            ("normalized_stem", _dedupe(normalized_stem_matches)),
            ("exact_name", _dedupe(exact_name)),
        ]
    else:
        strategies = [
            ("exact_name", _dedupe(exact_name)),
            ("exact_relative_path", _dedupe(exact_relative_path)),
            ("relative_suffix", _dedupe(rel_suffix)),
            ("full_suffix", _dedupe(full_suffix)),
            ("normalized_name", _dedupe(normalized_name)),
            ("normalized_stem", _dedupe(normalized_stem_matches)),
        ]

    for strategy, matches in strategies:
        if len(matches) == 1:
            return ResolutionResult(
                reference=reference,
                status="resolved",
                matches=[str(matches[0])],
                strategy=strategy,
                message=f"Resolved '{reference}' using {strategy}.",
            )
        if len(matches) > 1:
            return ResolutionResult(
                reference=reference,
                status="ambiguous",
                matches=[str(match) for match in matches[:20]],
                strategy=strategy,
                message=f"Reference '{reference}' matched multiple local datasets; use a more specific path.",
            )

    return ResolutionResult(
        reference=reference,
        status="not_found",
        matches=[],
        strategy="none",
        message=f"Reference '{reference}' was not found under datasets/network-traffic/raw or datasets/network-traffic/processed.",
    )
