#!/usr/bin/env python3
"""Local open-source embedding helpers for code RAG."""

from __future__ import annotations

from functools import lru_cache
from typing import Any


DEFAULT_EMBEDDING_MODEL = "BAAI/bge-m3"
FALLBACK_EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"


@lru_cache(maxsize=4)
def _load_model(model_name: str, device: str | None = None, cache_folder: str | None = None) -> Any:
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError as exc:
        raise RuntimeError(
            "sentence-transformers is required for local embedding. "
            "Install with: pip install sentence-transformers"
        ) from exc

    kwargs: dict[str, Any] = {}
    if device:
        kwargs["device"] = device
    if cache_folder:
        kwargs["cache_folder"] = cache_folder
    return SentenceTransformer(model_name, **kwargs)


def embedding_dimension(model_name: str = DEFAULT_EMBEDDING_MODEL, *, device: str | None = None, cache_folder: str | None = None) -> int:
    model = _load_model(model_name, device=device, cache_folder=cache_folder)
    dimension = model.get_sentence_embedding_dimension()
    if dimension is None:
        vector = embed_texts(["dimension probe"], model_name=model_name, device=device, cache_folder=cache_folder)[0]
        return len(vector)
    return int(dimension)


def embed_texts(
    texts: list[str],
    *,
    model_name: str = DEFAULT_EMBEDDING_MODEL,
    batch_size: int = 32,
    device: str | None = None,
    cache_folder: str | None = None,
    normalize: bool = True,
) -> list[list[float]]:
    if not texts:
        return []

    model = _load_model(model_name, device=device, cache_folder=cache_folder)
    vectors = model.encode(
        texts,
        batch_size=batch_size,
        normalize_embeddings=normalize,
        show_progress_bar=False,
    )
    if hasattr(vectors, "tolist"):
        return vectors.tolist()
    return [list(vector) for vector in vectors]


def build_embedding_text(document: dict[str, Any]) -> str:
    metadata = document.get("metadata", {})
    return "\n".join(
        [
            f"path: {metadata.get('path', document.get('path', ''))}",
            f"language: {metadata.get('language', document.get('language', ''))}",
            f"kind: {metadata.get('kind', document.get('kind', ''))}",
            f"symbol: {metadata.get('symbol', document.get('symbol', ''))}",
            f"tags: {', '.join(metadata.get('tags', document.get('tags', [])) or [])}",
            f"imports: {', '.join(metadata.get('imports', document.get('imports', [])) or [])}",
            "code:",
            document.get("code", ""),
        ]
    )

