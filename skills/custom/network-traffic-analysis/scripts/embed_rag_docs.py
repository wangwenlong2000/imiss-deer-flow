#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from file_resolution import is_explicit_path_reference, resolve_reference
from utils.path import repo_root

try:
    import yaml
except ImportError as exc:
    raise ImportError("Missing dependency 'pyyaml'. Install it first: pip install pyyaml") from exc

DEFAULT_MODEL = "text-embedding-v3-large"
LOCAL_PROVIDERS = {"sentence-transformers", "local"}
REMOTE_PROVIDERS = {"openai", "openai-compatible", "dashscope"}


def load_dotenv_file() -> None:
    dotenv_path = repo_root() / ".env"
    if not dotenv_path.exists():
        return
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ[key] = value


def to_repo_relative_display(value: str | Path) -> str:
    path = Path(value).expanduser()
    if not path.is_absolute():
        return path.as_posix()
    try:
        return path.resolve().relative_to(repo_root()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def resolve_env_value(value: Any) -> Any:
    if isinstance(value, str) and value.startswith("$"):
        return os.getenv(value[1:], "")
    return value


def parse_bool(value: Any, default: bool = True) -> bool:
    if value in (None, ""):
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    return normalized in {"1", "true", "yes", "y", "on"}


def get_config_path() -> Path:
    configured = os.getenv("DEER_FLOW_CONFIG_PATH")
    if configured:
        path = Path(configured).expanduser()
        if not path.is_absolute():
            path = Path.cwd() / path
        if not path.exists():
            raise FileNotFoundError(f"Config file specified by DEER_FLOW_CONFIG_PATH not found at {path}")
        return path.resolve()
    cwd = Path.cwd()
    for candidate in (cwd / "config.yaml", cwd.parent / "config.yaml"):
        if candidate.exists():
            return candidate.resolve()
    raise FileNotFoundError("config.yaml file not found in the current directory or its parent directory")


def load_app_config() -> dict[str, Any]:
    config_path = get_config_path()
    with open(config_path, encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    payload["_config_path"] = str(config_path)
    return payload


def resolve_embedding_config(config: dict[str, Any]) -> dict[str, Any]:
    embedding = dict(config.get("embedding") or {})
    dimensions_raw = resolve_env_value(embedding.get("dimensions"))
    dimensions = None
    if dimensions_raw not in (None, "") and str(dimensions_raw).strip().lower() != "none":
        dimensions = int(dimensions_raw)
    provider = str(resolve_env_value(embedding.get("provider")) or "openai").strip().lower()
    api_key = str(resolve_env_value(embedding.get("api_key")) or "")
    if not api_key:
        api_key = str(os.getenv("OPENAI_API_KEY", "") or os.getenv("DASHSCOPE_API_KEY", ""))
    local_model_path = resolve_env_value(embedding.get("local_model_path"))
    if local_model_path and local_model_path.strip():
        p = Path(local_model_path)
        if not p.is_absolute():
            repo_root = Path(config.get("_config_path", ".")).resolve().parent
            local_model_path = str(repo_root / p)
        local_model_path = str(local_model_path) if Path(local_model_path).is_dir() else None
    return {
        "provider": provider,
        "model": str(resolve_env_value(embedding.get("model")) or DEFAULT_MODEL),
        "api_key": api_key,
        "base_url": str(resolve_env_value(embedding.get("base_url")) or ""),
        "dimensions": dimensions,
        "device": str(resolve_env_value(embedding.get("device")) or ""),
        "normalize": parse_bool(resolve_env_value(embedding.get("normalize")), True),
        "allow_download": parse_bool(resolve_env_value(embedding.get("allow_download")), False),
        "local_model_path": local_model_path,
        "config_path": str(config.get("_config_path", "")),
    }


def discover_files(values: list[str]) -> list[str]:
    files: list[str] = []
    for value in values:
        path = Path(value)
        if path.is_dir():
            files.extend(str(p.resolve()) for p in sorted(path.rglob("rag_docs.jsonl")))
        elif path.exists():
            files.append(str(path.resolve()))
        elif is_explicit_path_reference(value):
            raise ValueError(f"RAG docs path '{value}' does not exist.")
        else:
            files.extend(resolve_file_reference(value))
    deduped: list[str] = []
    seen: set[str] = set()
    for item in files:
        normalized = str(Path(item).resolve())
        if normalized not in seen:
            deduped.append(normalized)
            seen.add(normalized)
    return deduped


def resolve_file_reference(reference: str) -> list[str]:
    result = resolve_reference(reference)
    if result.status == "resolved":
        return result.matches
    if result.status == "ambiguous":
        sample = "\n".join(f"  - {to_repo_relative_display(path)}" for path in result.matches[:10])
        raise ValueError(
            f"RAG docs reference '{reference}' matched multiple datasets. Use a more specific path.\nCandidates:\n{sample}"
        )
    raise ValueError(result.message)


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []
    with open(path, encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            payload = line.strip()
            if not payload:
                continue
            try:
                documents.append(json.loads(payload))
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSONL in '{path}' at line {line_number}: {exc}") from exc
    if not documents:
        raise ValueError(f"No documents found in '{to_repo_relative_display(path)}'.")
    return documents


def ensure_document_shape(documents: list[dict[str, Any]], source_path: Path) -> None:
    required = {"doc_id", "dataset_name", "source_file", "doc_type", "title", "content", "summary", "keywords", "metadata"}
    for index, document in enumerate(documents, start=1):
        missing = sorted(required - set(document))
        if missing:
            raise ValueError(
                f"Document {index} in '{to_repo_relative_display(source_path)}' is missing required fields: {', '.join(missing)}"
            )


def batched(values: list[dict[str, Any]], batch_size: int) -> list[list[dict[str, Any]]]:
    batch_size = max(int(batch_size), 1)
    return [values[index:index + batch_size] for index in range(0, len(values), batch_size)]


def progress_label(processed: int, total: int) -> str:
    if total <= 0:
        return "0.0%"
    return f"{(processed / total) * 100:.1f}%"


def load_openai_client(api_key: str, base_url: str | None = None) -> Any:
    try:
        from openai import OpenAI
    except ImportError as exc:
        raise ImportError("Missing dependency 'openai'. Install it before using remote embeddings: pip install openai") from exc
    kwargs: dict[str, Any] = {"api_key": api_key}
    if base_url:
        kwargs["base_url"] = base_url
    return OpenAI(**kwargs)


def require_sentence_transformers() -> None:
    try:
        import sentence_transformers  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "Missing dependency 'sentence-transformers'. Install it before using local embeddings: pip install sentence-transformers"
        ) from exc


def _load_model_from_path(model_name: str, **kwargs):
    """Load a SentenceTransformer model, reloading modules if needed."""
    from sentence_transformers import SentenceTransformer
    try:
        return SentenceTransformer(model_name, **kwargs)
    except Exception:
        for module_name in list(sys.modules):
            if module_name == "sentence_transformers" or module_name.startswith("sentence_transformers."):
                sys.modules.pop(module_name, None)
        require_sentence_transformers()
        from sentence_transformers import SentenceTransformer as RefreshedSentenceTransformer
        return RefreshedSentenceTransformer(model_name, **kwargs)


def load_sentence_transformer(
    model_name: str,
    *,
    device: str | None = None,
    local_model_path: str | None = None,
    allow_download: bool = False,
) -> Any:
    kwargs: dict[str, Any] = {}
    if device:
        kwargs["device"] = device

    # 1. Try configured local_model_path first (from config.yaml)
    if local_model_path and Path(local_model_path).is_dir():
        return _load_model_from_path(local_model_path, **kwargs)

    # 2. Try HF local cache without network
    prev = os.environ.pop("HF_HUB_OFFLINE", None)
    os.environ["HF_HUB_OFFLINE"] = "1"
    try:
        return _load_model_from_path(model_name, **kwargs)
    except Exception as offline_error:
        if not allow_download:
            raise RuntimeError(
                "Local model/cache unavailable and embedding.allow_download=false. "
                f"model={model_name}, local_model_path={local_model_path or ''}"
            ) from offline_error
    finally:
        if prev is None:
            os.environ.pop("HF_HUB_OFFLINE", None)
        else:
            os.environ["HF_HUB_OFFLINE"] = prev

    # 3. Cache miss and no local copy — allow download from HuggingFace Hub
    return _load_model_from_path(model_name, **kwargs)


def embed_batch_remote(
    client: Any,
    model: str,
    dimensions: int | None,
    documents: list[dict[str, Any]],
    *,
    max_attempts: int = 3,
) -> list[list[float]]:
    payload = [document["content"] for document in documents]
    params: dict[str, Any] = {"model": model, "input": payload}
    if dimensions is not None:
        params["dimensions"] = dimensions

    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = client.embeddings.create(**params)
            return [item.embedding for item in response.data]
        except Exception as exc:
            last_error = exc
            if attempt == max_attempts:
                break
            time.sleep(min(1.5 * attempt, 5.0))
    assert last_error is not None
    raise last_error


def embed_batch_local(
    model: Any,
    documents: list[dict[str, Any]],
    *,
    batch_size: int,
    normalize: bool,
) -> list[list[float]]:
    payload = [document["content"] for document in documents]
    vectors = model.encode(
        payload,
        batch_size=max(batch_size, 1),
        normalize_embeddings=normalize,
        show_progress_bar=False,
    )
    return [vector.tolist() if hasattr(vector, "tolist") else list(vector) for vector in vectors]


def enrich_documents(
    documents: list[dict[str, Any]],
    vectors: list[list[float]],
    *,
    model: str,
    local_model_path: str | None,
    dimensions: int | None,
) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for document, vector in zip(documents, vectors, strict=True):
        payload = dict(document)
        payload["embedding_model"] = model
        if local_model_path:
            payload["embedding_local_model_path"] = to_repo_relative_display(local_model_path)
        payload["embedding_dimensions"] = dimensions or len(vector)
        payload["embedding"] = vector
        enriched.append(payload)
    return enriched


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def build_manifest(
    *,
    input_files: list[str],
    output_file: Path,
    documents: list[dict[str, Any]],
    model: str,
    local_model_path: str | None,
    dimensions: int | None,
) -> dict[str, Any]:
    doc_types: dict[str, int] = {}
    for document in documents:
        doc_types[document["doc_type"]] = doc_types.get(document["doc_type"], 0) + 1
    sample_doc_ids = [document["doc_id"] for document in documents[:10]]
    actual_dimensions = documents[0].get("embedding_dimensions", dimensions or 0) if documents else dimensions or 0
    return {
        "input_files": [to_repo_relative_display(item) for item in input_files],
        "document_count": len(documents),
        "document_types": doc_types,
        "embedding_model": model,
        "embedding_local_model_path": to_repo_relative_display(local_model_path) if local_model_path else "",
        "embedding_dimensions": actual_dimensions,
        "output_file": to_repo_relative_display(output_file),
        "samples": sample_doc_ids,
    }


def embedding_cache_key(
    document: dict[str, Any],
    *,
    model: str,
    dimensions: int | None,
    normalize: bool,
) -> str:
    payload = {
        "doc_id": document.get("doc_id", ""),
        "schema_version": document.get("schema_version", ""),
        "content": document.get("content", ""),
        "summary": document.get("summary", ""),
        "title": document.get("title", ""),
        "keywords": document.get("keywords", []),
        "model": model,
        "dimensions": dimensions,
        "normalize": normalize,
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def load_existing_embedding_cache(
    path: Path,
    *,
    model: str,
    dimensions: int | None,
    normalize: bool,
) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    cache: dict[str, dict[str, Any]] = {}
    for row in load_jsonl(path):
        key = row.get("embedding_cache_key")
        if not key:
            key = embedding_cache_key(row, model=model, dimensions=dimensions, normalize=normalize)
        emb = row.get("embedding")
        if key and emb:
            row["embedding_cache_key"] = key
            cache[key] = row
    return cache


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate embeddings for network traffic RAG documents.")
    parser.add_argument("--files", nargs="+", required=True, help="rag_docs.jsonl files, directories, or shorthand references")
    parser.add_argument("--model", default=None, help="Embedding model name. Defaults to config.yaml embedding.model")
    parser.add_argument("--dimensions", type=int, default=None, help="Optional embedding dimension override. Defaults to config.yaml embedding.dimensions")
    parser.add_argument("--batch-size", type=int, default=10, help="Embedding request batch size")
    parser.add_argument("--output-file", default=None, help="Explicit output JSONL path. Defaults beside rag_docs.jsonl")
    parser.add_argument("--format", choices=["text", "json"], default="text", help="Output format")
    parser.add_argument(
        "--no-reuse-existing",
        dest="reuse_existing",
        action="store_false",
        help="Disable embedding reuse and recompute all documents.",
    )
    parser.set_defaults(reuse_existing=True)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        load_dotenv_file()
        config = load_app_config()
        embedding_config = resolve_embedding_config(config)
        provider = embedding_config["provider"]
        model_name = args.model or embedding_config["model"]
        dimensions = args.dimensions if args.dimensions is not None else embedding_config["dimensions"]

        if provider in REMOTE_PROVIDERS:
            api_key = embedding_config["api_key"].strip()
            if not api_key:
                raise ValueError("No embedding API key resolved. Set embedding.api_key in config.yaml or configure the provider key in .env.")
            embedder = load_openai_client(api_key, embedding_config["base_url"] or None)
            embed_fn = lambda docs: embed_batch_remote(embedder, model_name, dimensions, docs)  # noqa: E731
        elif provider in LOCAL_PROVIDERS:
            if dimensions is not None:
                raise ValueError("Local sentence-transformers models do not support overriding dimensions. Leave embedding.dimensions empty.")
            embedder = load_sentence_transformer(
                model_name,
                device=embedding_config["device"] or None,
                local_model_path=embedding_config.get("local_model_path"),
                allow_download=embedding_config["allow_download"],
            )
            embed_fn = lambda docs: embed_batch_local(  # noqa: E731
                embedder,
                docs,
                batch_size=args.batch_size,
                normalize=embedding_config["normalize"],
            )
        else:
            raise ValueError(
                f"Unsupported embedding provider '{provider}'. Expected one of: {', '.join(sorted(LOCAL_PROVIDERS | REMOTE_PROVIDERS))}."
            )

        files = discover_files(args.files)
        if not files:
            parser.error("No rag_docs.jsonl files were found from --files")
        if len(files) != 1 and args.output_file:
            raise ValueError("--output-file can only be used with a single input file.")

        # Load embedding cache for incremental reuse
        if args.output_file:
            cache_path = Path(args.output_file).resolve().with_name("rag_embeddings.jsonl")
        elif len(files) == 1:
            cache_path = Path(files[0]).resolve().with_name("rag_embeddings.jsonl")
        else:
            cache_path = Path(files[0]).resolve().with_name("rag_embeddings.jsonl")
        existing_cache = (
            load_existing_embedding_cache(
                cache_path,
                model=model_name,
                dimensions=dimensions,
                normalize=embedding_config["normalize"],
            )
            if args.reuse_existing
            else {}
        )

        all_enriched: list[dict[str, Any]] = []
        total_reused = 0
        total_computed = 0
        total_input_files = len(files)
        for file_path in files:
            source_path = Path(file_path).resolve()
            if source_path.name != "rag_docs.jsonl":
                raise ValueError(f"Expected rag_docs.jsonl input, got '{to_repo_relative_display(source_path)}'.")
            documents = load_jsonl(source_path)
            ensure_document_shape(documents, source_path)

            document_count = len(documents)
            log = sys.stderr if args.format == "json" else sys.stdout
            print(
                f"Embedding input {files.index(file_path) + 1}/{total_input_files}: "
                f"{to_repo_relative_display(source_path)} ({document_count} documents, batch_size={max(args.batch_size, 1)})",
                file=log,
            )

            # Compute cache keys and identify hits/misses
            output_rows: list[dict[str, Any] | None] = []
            pending_docs: list[dict[str, Any]] = []
            pending_indices: list[int] = []
            for i, doc in enumerate(documents):
                key = embedding_cache_key(doc, model=model_name, dimensions=dimensions, normalize=embedding_config["normalize"])
                cached = existing_cache.get(key)
                if cached is not None:
                    output_rows.append(cached)
                    total_reused += 1
                else:
                    output_rows.append(None)
                    pending_indices.append(i)
                    pending_docs.append(doc)

            if not pending_docs:
                print(f"  All {document_count} documents found in cache, skipping embedding.", file=log)
                all_enriched.extend([r for r in output_rows if r is not None])
                continue

            # Embed only cache-miss documents
            chunked_pending = batched(pending_docs, max(args.batch_size, 1))
            total_chunks = len(chunked_pending)
            processed_docs = 0
            print(
                f"  {len(pending_docs)}/{document_count} documents need embedding, {len(documents) - len(pending_docs)} cached",
                file=log,
            )
            all_vectors: list[list[float]] = []
            for chunk_index, chunk in enumerate(chunked_pending, start=1):
                chunk_vectors = embed_fn(chunk)
                all_vectors.extend(chunk_vectors)
                processed_docs += len(chunk)
                print(
                    f"Embedding progress: {progress_label(processed_docs, len(pending_docs))} "
                    f"({processed_docs}/{len(pending_docs)} docs, batch {chunk_index}/{total_chunks})",
                    file=log,
                )

            # Enrich pending docs with their embeddings and attach cache keys
            enriched_pending = enrich_documents(
                pending_docs,
                all_vectors,
                model=model_name,
                local_model_path=embedding_config.get("local_model_path"),
                dimensions=dimensions,
            )
            for idx, enriched in zip(pending_indices, enriched_pending):
                key = embedding_cache_key(documents[idx], model=model_name, dimensions=dimensions, normalize=embedding_config["normalize"])
                enriched["embedding_cache_key"] = key
                output_rows[idx] = enriched
                total_computed += 1

            all_enriched.extend([r for r in output_rows if r is not None])

        if args.output_file:
            output_path = Path(args.output_file).resolve()
        else:
            output_path = Path(files[0]).resolve().with_name("rag_embeddings.jsonl")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path = output_path.with_name("embedding_manifest.json")

        write_jsonl(output_path, all_enriched)
        manifest = build_manifest(
            input_files=files,
            output_file=output_path,
            documents=all_enriched,
            model=model_name,
            local_model_path=embedding_config.get("local_model_path"),
            dimensions=dimensions,
        )
        manifest["embedding_provider"] = provider
        manifest["config_path"] = to_repo_relative_display(embedding_config["config_path"]) if embedding_config["config_path"] else ""
        manifest["base_url"] = embedding_config["base_url"] or ""
        manifest["device"] = embedding_config["device"] or ""
        manifest["normalize"] = embedding_config["normalize"]
        manifest["allow_download"] = embedding_config["allow_download"]
        manifest["reuse_existing"] = args.reuse_existing
        if args.reuse_existing:
            total_processed = total_reused + total_computed
            manifest["embedding_reuse"] = {
                "reused_count": total_reused,
                "computed_count": total_computed,
                "cache_hit_rate": round(total_reused / total_processed, 4) if total_processed > 0 else 0.0,
            }
        manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

        if args.format == "json":
            print(json.dumps({
                "status": "success",
                "manifest": manifest,
            }, ensure_ascii=False))
        else:
            lines = [
                f"Embedding provider: {provider}",
                f"Embedding model: {model_name}",
                f"Input files: {len(files)}",
                f"Document count: {len(all_enriched)}",
            ]
            if args.reuse_existing:
                reuse = manifest.get("embedding_reuse", {})
                lines.append(f"Cache reused: {reuse.get('reused_count', 0)}, computed: {reuse.get('computed_count', 0)}, hit rate: {reuse.get('cache_hit_rate', 0):.1%}")
            lines.extend([
                f"rag_embeddings: {to_repo_relative_display(output_path)}",
                f"manifest: {to_repo_relative_display(manifest_path)}",
            ])
            print("\n".join(lines))
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
