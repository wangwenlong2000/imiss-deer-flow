#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from file_resolution import is_explicit_path_reference, resolve_reference

try:
    import yaml
except ImportError:
    os.system(f"{sys.executable} -m pip install pyyaml -q")
    import yaml

DEFAULT_MODEL = "text-embedding-v3-large"
LOCAL_PROVIDERS = {"sentence-transformers", "local"}
REMOTE_PROVIDERS = {"openai", "openai-compatible", "dashscope"}


def repo_root() -> Path:
    script_path = Path(__file__).resolve()
    for candidate in script_path.parents:
        if (candidate / "config.yaml").exists():
            return candidate
    return script_path.parents[3]


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
    return {
        "provider": provider,
        "model": str(resolve_env_value(embedding.get("model")) or DEFAULT_MODEL),
        "api_key": api_key,
        "base_url": str(resolve_env_value(embedding.get("base_url")) or ""),
        "dimensions": dimensions,
        "device": str(resolve_env_value(embedding.get("device")) or ""),
        "normalize": parse_bool(resolve_env_value(embedding.get("normalize")), True),
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
    except ImportError:
        os.system(f"{sys.executable} -m pip install openai -q")
        from openai import OpenAI
    kwargs: dict[str, Any] = {"api_key": api_key}
    if base_url:
        kwargs["base_url"] = base_url
    return OpenAI(**kwargs)


def install_sentence_transformers() -> None:
    subprocess.check_call(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--upgrade",
            "sentence-transformers",
        ]
    )


def load_sentence_transformer(model_name: str, *, device: str | None = None) -> Any:
    try:
        from sentence_transformers import SentenceTransformer
    except Exception:
        for module_name in list(sys.modules):
            if module_name == "sentence_transformers" or module_name.startswith("sentence_transformers."):
                sys.modules.pop(module_name, None)
        install_sentence_transformers()
        from sentence_transformers import SentenceTransformer
    kwargs: dict[str, Any] = {}
    if device:
        kwargs["device"] = device
    try:
        return SentenceTransformer(model_name, **kwargs)
    except Exception:
        for module_name in list(sys.modules):
            if module_name == "sentence_transformers" or module_name.startswith("sentence_transformers."):
                sys.modules.pop(module_name, None)
        install_sentence_transformers()
        from sentence_transformers import SentenceTransformer as RefreshedSentenceTransformer
        return RefreshedSentenceTransformer(model_name, **kwargs)


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
    dimensions: int | None,
) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for document, vector in zip(documents, vectors, strict=True):
        payload = dict(document)
        payload["embedding_model"] = model
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
        "embedding_dimensions": actual_dimensions,
        "output_file": to_repo_relative_display(output_file),
        "samples": sample_doc_ids,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate embeddings for network traffic RAG documents.")
    parser.add_argument("--files", nargs="+", required=True, help="rag_docs.jsonl files, directories, or shorthand references")
    parser.add_argument("--model", default=None, help="Embedding model name. Defaults to config.yaml embedding.model")
    parser.add_argument("--dimensions", type=int, default=None, help="Optional embedding dimension override. Defaults to config.yaml embedding.dimensions")
    parser.add_argument("--batch-size", type=int, default=10, help="Embedding request batch size")
    parser.add_argument("--output-file", default=None, help="Explicit output JSONL path. Defaults beside rag_docs.jsonl")
    parser.add_argument("--format", choices=["text", "json"], default="text", help="Output format")
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
            embedder = load_sentence_transformer(model_name, device=embedding_config["device"] or None)
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

        all_enriched: list[dict[str, Any]] = []
        total_input_files = len(files)
        for file_path in files:
            source_path = Path(file_path).resolve()
            if source_path.name != "rag_docs.jsonl":
                raise ValueError(f"Expected rag_docs.jsonl input, got '{to_repo_relative_display(source_path)}'.")
            documents = load_jsonl(source_path)
            ensure_document_shape(documents, source_path)

            document_count = len(documents)
            chunked_documents = batched(documents, max(args.batch_size, 1))
            total_chunks = len(chunked_documents)
            processed_docs = 0
            print(
                f"Embedding input {files.index(file_path) + 1}/{total_input_files}: "
                f"{to_repo_relative_display(source_path)} ({document_count} documents, batch_size={max(args.batch_size, 1)})"
            )
            vectors: list[list[float]] = []
            for chunk_index, chunk in enumerate(chunked_documents, start=1):
                chunk_vectors = embed_fn(chunk)
                vectors.extend(chunk_vectors)
                processed_docs += len(chunk)
                print(
                    f"Embedding progress: {progress_label(processed_docs, document_count)} "
                    f"({processed_docs}/{document_count} docs, batch {chunk_index}/{total_chunks})"
                )

            all_enriched.extend(
                enrich_documents(documents, vectors, model=model_name, dimensions=dimensions)
            )

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
            dimensions=dimensions,
        )
        manifest["embedding_provider"] = provider
        manifest["config_path"] = to_repo_relative_display(embedding_config["config_path"]) if embedding_config["config_path"] else ""
        manifest["base_url"] = embedding_config["base_url"] or ""
        manifest["device"] = embedding_config["device"] or ""
        manifest["normalize"] = embedding_config["normalize"]
        manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

        if args.format == "json":
            print(json.dumps(manifest, ensure_ascii=False))
        else:
            print(
                "\n".join(
                    [
                        f"Embedding provider: {provider}",
                        f"Embedding model: {model_name}",
                        f"Input files: {len(files)}",
                        f"Document count: {len(all_enriched)}",
                        f"rag_embeddings: {to_repo_relative_display(output_path)}",
                        f"manifest: {to_repo_relative_display(manifest_path)}",
                    ]
                )
            )
        return 0
    except Exception as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
