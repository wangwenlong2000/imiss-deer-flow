#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from file_resolution import is_explicit_path_reference, resolve_reference

try:
    import yaml
except ImportError:
    os.system(f"{sys.executable} -m pip install pyyaml -q")
    import yaml

DEFAULT_INDEX_NAME = "network-traffic-rag"


def repo_root() -> Path:
    script_path = Path(__file__).resolve()
    for candidate in script_path.parents:
        if (candidate / "config.yaml").exists():
            return candidate
    return script_path.parents[3]


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


def resolve_elasticsearch_config(config: dict[str, Any]) -> dict[str, Any]:
    elasticsearch = dict(config.get("elasticsearch") or {})
    hosts = resolve_env_value(elasticsearch.get("hosts")) or "http://localhost:9200"
    if isinstance(hosts, str):
        host_list = [item.strip() for item in hosts.split(",") if item.strip()]
    elif isinstance(hosts, list):
        host_list = [str(resolve_env_value(item)).strip() for item in hosts if str(resolve_env_value(item)).strip()]
    else:
        host_list = []
    return {
        "hosts": host_list or ["http://localhost:9200"],
        "index_name": str(resolve_env_value(elasticsearch.get("index_name")) or DEFAULT_INDEX_NAME),
        "api_key": str(resolve_env_value(elasticsearch.get("api_key")) or ""),
        "username": str(resolve_env_value(elasticsearch.get("username")) or ""),
        "password": str(resolve_env_value(elasticsearch.get("password")) or ""),
        "verify_certs": parse_bool(resolve_env_value(elasticsearch.get("verify_certs")), True),
        "request_timeout": int(resolve_env_value(elasticsearch.get("request_timeout")) or 30),
        "config_path": str(config.get("_config_path", "")),
    }


def discover_files(values: list[str]) -> list[str]:
    files: list[str] = []
    for value in values:
        path = Path(value)
        if path.is_dir():
            files.extend(str(p.resolve()) for p in sorted(path.rglob("rag_embeddings.jsonl")))
        elif path.exists():
            files.append(str(path.resolve()))
        elif is_explicit_path_reference(value):
            raise ValueError(f"Embedding file path '{value}' does not exist.")
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
            f"Embedding reference '{reference}' matched multiple datasets. Use a more specific path.\nCandidates:\n{sample}"
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


def ensure_document_shape(documents: list[dict[str, Any]], source_path: Path) -> int:
    required = {
        "doc_id",
        "dataset_name",
        "source_file",
        "doc_type",
        "title",
        "content",
        "summary",
        "keywords",
        "metadata",
        "embedding",
        "embedding_model",
        "embedding_dimensions",
    }
    embedding_dimensions: int | None = None
    for index, document in enumerate(documents, start=1):
        missing = sorted(required - set(document))
        if missing:
            raise ValueError(
                f"Document {index} in '{to_repo_relative_display(source_path)}' is missing required fields: {', '.join(missing)}"
            )
        embedding = document.get("embedding")
        if not isinstance(embedding, list) or not embedding:
            raise ValueError(f"Document {index} in '{to_repo_relative_display(source_path)}' has an empty embedding.")
        current_dimensions = len(embedding)
        if embedding_dimensions is None:
            embedding_dimensions = current_dimensions
        elif embedding_dimensions != current_dimensions:
            raise ValueError(
                f"Embedding dimension mismatch in '{to_repo_relative_display(source_path)}': "
                f"expected {embedding_dimensions}, got {current_dimensions} at document {index}."
            )
    assert embedding_dimensions is not None
    return embedding_dimensions


def build_index_mapping(dimensions: int) -> dict[str, Any]:
    return {
        "mappings": {
            "properties": {
                "doc_id": {"type": "keyword"},
                "dataset_name": {"type": "keyword"},
                "source_file": {"type": "keyword"},
                "doc_type": {"type": "keyword"},
                "title": {"type": "text"},
                "content": {"type": "text"},
                "summary": {"type": "text"},
                "keywords": {"type": "keyword"},
                "embedding_model": {"type": "keyword"},
                "embedding_dimensions": {"type": "integer"},
                "embedding": {
                    "type": "dense_vector",
                    "dims": dimensions,
                    "index": True,
                    "similarity": "cosine",
                },
                "metadata": {
                    "properties": {
                        "protocol": {"type": "keyword"},
                        "app_protocol": {"type": "keyword"},
                        "traffic_family": {"type": "keyword"},
                        "src_ip": {"type": "ip", "ignore_malformed": True},
                        "dst_ip": {"type": "ip", "ignore_malformed": True},
                        "dst_port": {"type": "integer"},
                        "time_bucket": {"type": "keyword"},
                        "risk_level": {"type": "keyword"},
                        "tags": {"type": "keyword"},
                    }
                },
            }
        },
    }


def load_elasticsearch_client(config: dict[str, Any]) -> tuple[Any, Any]:
    try:
        from elasticsearch import Elasticsearch
        from elasticsearch.helpers import bulk
    except ImportError:
        os.system(f"{sys.executable} -m pip install elasticsearch -q")
        from elasticsearch import Elasticsearch
        from elasticsearch.helpers import bulk

    kwargs: dict[str, Any] = {
        "hosts": config["hosts"],
        "verify_certs": config["verify_certs"],
        "request_timeout": config["request_timeout"],
    }
    if config["api_key"]:
        kwargs["api_key"] = config["api_key"]
    elif config["username"] and config["password"]:
        kwargs["basic_auth"] = (config["username"], config["password"])
    client = Elasticsearch(**kwargs)
    return client, bulk


def ensure_index(client: Any, index_name: str, dimensions: int) -> str:
    if client.indices.exists(index=index_name):
        mapping = client.indices.get_mapping(index=index_name)
        properties = mapping.get(index_name, {}).get("mappings", {}).get("properties", {})
        embedding_mapping = properties.get("embedding", {})
        existing_dimensions = embedding_mapping.get("dims")
        if existing_dimensions and int(existing_dimensions) != dimensions:
            raise ValueError(
                f"Elasticsearch index '{index_name}' already exists with embedding dims={existing_dimensions}, "
                f"but current documents use dims={dimensions}."
            )
        return "existing"
    client.indices.create(index=index_name, body=build_index_mapping(dimensions))
    return "created"


def to_bulk_action(index_name: str, document: dict[str, Any]) -> dict[str, Any]:
    return {
        "_op_type": "index",
        "_index": index_name,
        "_id": document["doc_id"],
        "_source": document,
    }


def build_manifest(
    *,
    input_files: list[str],
    output_file: Path,
    index_name: str,
    indexed_count: int,
    documents: list[dict[str, Any]],
    index_status: str,
    config: dict[str, Any],
) -> dict[str, Any]:
    doc_types: dict[str, int] = {}
    for document in documents:
        doc_types[document["doc_type"]] = doc_types.get(document["doc_type"], 0) + 1
    sample_doc_ids = [document["doc_id"] for document in documents[:10]]
    return {
        "input_files": [to_repo_relative_display(item) for item in input_files],
        "indexed_count": indexed_count,
        "document_types": doc_types,
        "index_name": index_name,
        "index_status": index_status,
        "hosts": config["hosts"],
        "output_file": to_repo_relative_display(output_file),
        "samples": sample_doc_ids,
        "config_path": to_repo_relative_display(config["config_path"]) if config["config_path"] else "",
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Index network traffic RAG embeddings into Elasticsearch.")
    parser.add_argument("--files", nargs="+", required=True, help="rag_embeddings.jsonl files, directories, or shorthand references")
    parser.add_argument("--index-name", default=None, help="Override Elasticsearch index name. Defaults to config.yaml elasticsearch.index_name")
    parser.add_argument("--output-file", default=None, help="Explicit output manifest path. Defaults beside rag_embeddings.jsonl")
    parser.add_argument("--chunk-size", type=int, default=1000, help="Bulk indexing chunk size")
    parser.add_argument("--format", choices=["text", "json"], default="text", help="Output format")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        config = load_app_config()
        elasticsearch_config = resolve_elasticsearch_config(config)
        index_name = args.index_name or elasticsearch_config["index_name"]

        files = discover_files(args.files)
        if not files:
            parser.error("No rag_embeddings.jsonl files were found from --files")
        if len(files) != 1 and args.output_file:
            raise ValueError("--output-file can only be used with a single input file.")

        all_documents: list[dict[str, Any]] = []
        dimensions: int | None = None
        for file_path in files:
            source_path = Path(file_path).resolve()
            if source_path.name != "rag_embeddings.jsonl":
                raise ValueError(f"Expected rag_embeddings.jsonl input, got '{to_repo_relative_display(source_path)}'.")
            documents = load_jsonl(source_path)
            current_dimensions = ensure_document_shape(documents, source_path)
            if dimensions is None:
                dimensions = current_dimensions
            elif dimensions != current_dimensions:
                raise ValueError(
                    f"Embedding dimensions differ across files. Expected {dimensions}, got {current_dimensions} in {to_repo_relative_display(source_path)}."
                )
            all_documents.extend(documents)

        if dimensions is None:
            raise ValueError("No valid documents were found for Elasticsearch indexing.")

        client, bulk = load_elasticsearch_client(elasticsearch_config)
        if not client.ping():
            raise ConnectionError(
                f"Unable to connect to Elasticsearch hosts: {', '.join(elasticsearch_config['hosts'])}"
            )
        index_status = ensure_index(client, index_name, dimensions)
        actions = [to_bulk_action(index_name, document) for document in all_documents]
        chunk_size = max(int(args.chunk_size), 1)
        total_actions = len(actions)
        success_count = 0
        total_chunks = (total_actions + chunk_size - 1) // chunk_size
        for chunk_index in range(total_chunks):
            start = chunk_index * chunk_size
            end = min(start + chunk_size, total_actions)
            chunk = actions[start:end]
            chunk_success, errors = bulk(client, chunk, stats_only=False, raise_on_error=False)
            success_count += chunk_success
            if errors:
                raise RuntimeError(
                    f"Bulk indexing completed with {len(errors)} errors in chunk {chunk_index + 1}/{total_chunks}. "
                    f"First error: {errors[0]}"
                )
            if args.format == "text":
                print(
                    f"Indexed chunk {chunk_index + 1}/{total_chunks}: documents {start + 1}-{end} / {total_actions}"
                )

        if args.output_file:
            output_path = Path(args.output_file).resolve()
        else:
            output_path = Path(files[0]).resolve().with_name("index_manifest.json")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        manifest = build_manifest(
            input_files=files,
            output_file=output_path,
            index_name=index_name,
            indexed_count=success_count,
            documents=all_documents,
            index_status=index_status,
            config=elasticsearch_config,
        )
        output_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

        if args.format == "json":
            print(json.dumps(manifest, ensure_ascii=False))
        else:
            print(
                "\n".join(
                    [
                        f"Indexed RAG docs into Elasticsearch index: {index_name}",
                        f"Input files: {len(files)}",
                        f"Document count: {len(all_documents)}",
                        f"Indexed count: {success_count}",
                        f"manifest: {to_repo_relative_display(output_path)}",
                    ]
                )
            )
        return 0
    except Exception as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
