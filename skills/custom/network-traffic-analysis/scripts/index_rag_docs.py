#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

from file_resolution import is_explicit_path_reference, resolve_reference
from utils.config import (
    get_config_path,
    load_app_config,
    load_dotenv_file,
    parse_bool,
    resolve_elasticsearch_config,
    resolve_env_value,
)
from utils.path import repo_root, to_repo_relative_display


def flatten_rag_doc_for_index(doc: dict[str, Any]) -> dict[str, Any]:
    """Flatten a RAG document for Elasticsearch indexing.

    Preserves the original `payload` but copies common filter fields to the
    top level so ES queries do not need nested field access.  v1 documents
    (without payload) get empty defaults.
    """
    payload = doc.get("payload") or {}
    action = payload.get("action", {})
    finding = payload.get("finding", {})
    entities = payload.get("entities", {})
    security = payload.get("security_context", {})
    diagnostics = payload.get("diagnostics", {})

    threat_tags = security.get("threat_tags", [])
    attack_stages = security.get("attack_stages", [])
    mitre_techniques = security.get("mitre_techniques", [])
    ioc_candidates = security.get("ioc_candidates", [])

    # Flatten IOC objects into indexable keyword arrays
    # ioc_candidates: list of {"value": "...", "type": "...", ...}
    ioc_values: list[str] = []
    ioc_types: list[str] = []
    for ioc in ioc_candidates:
        if isinstance(ioc, dict):
            val = ioc.get("value")
            ioc_type = ioc.get("type")
            if val:
                ioc_values.append(str(val))
            if ioc_type:
                ioc_types.append(str(ioc_type))
        elif isinstance(ioc, str):
            # Legacy flat string fallback
            ioc_values.append(ioc)

    # Flatten metadata provenance fields to top level for ES filtering
    metadata = doc.get("metadata") or {}
    row_index_raw = metadata.get("row_index")
    try:
        row_index_val = int(row_index_raw) if row_index_raw not in (None, "", "0") else None
    except (TypeError, ValueError):
        row_index_val = None

    flattened = {
        **doc,
        "action_name": action.get("name", doc.get("action_name", "")),
        "action_category": action.get("category", ""),
        "finding_id": finding.get("id", doc.get("finding_id", "")),
        "severity": finding.get("severity", doc.get("metadata", {}).get("risk_level", doc.get("severity", ""))),
        "confidence": finding.get("confidence", doc.get("confidence")),
        "risk_score": finding.get("risk_score", doc.get("risk_score")),
        "src_ips": entities.get("src_ips", []),
        "dst_ips": entities.get("dst_ips", []),
        "domains": entities.get("domains", []),
        "ports": entities.get("ports", []),
        "protocols": entities.get("protocols", []),
        "services": entities.get("services", []),
        "threat_tags": threat_tags,
        "attack_stages": attack_stages,
        "mitre_techniques": mitre_techniques,
        "ioc_candidates": ioc_candidates,
        "ioc_values": ioc_values,
        "ioc_types": ioc_types,
        "data_quality": diagnostics.get("data_quality", {}),
        "coverage": diagnostics.get("coverage", {}),
        "limitations": diagnostics.get("limitations", []),
        "warnings": diagnostics.get("warnings", []),
        "row_index": row_index_val,
        "flow_id": metadata.get("flow_id", doc.get("flow_id", "")),
        "time_bucket": metadata.get("time_bucket", doc.get("time_bucket", "")),
        "provenance_type": metadata.get("provenance_type", doc.get("provenance_type", "")),
        "raw_source_file": metadata.get("raw_source_file", doc.get("raw_source_file", "")),
        "evidence_refs": metadata.get("evidence_refs", doc.get("evidence_refs", [])),
        "entity_type": payload.get("entity_type", doc.get("entity_type", "")),
        "entity_value": payload.get("entity_value", doc.get("entity_value", "")),
        "risk_level": metadata.get("risk_level", doc.get("risk_level", "")),
        "dataset_id": doc.get("dataset_id", ""),
        "source_sha256": doc.get("source_sha256", ""),
        "artifact_generation_id": doc.get("artifact_generation_id", ""),
    }
    return flattened


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
                "dataset_id": {"type": "keyword"},
                "source_file": {"type": "keyword"},
                "doc_type": {"type": "keyword"},
                "schema_version": {"type": "keyword"},
                "title": {"type": "text"},
                "content": {"type": "text"},
                "summary": {"type": "text"},
                "keywords": {"type": "keyword"},
                "embedding_model": {"type": "keyword"},
                "embedding_dimensions": {"type": "integer"},
                "action_name": {"type": "keyword"},
                "action_category": {"type": "keyword"},
                "finding_id": {"type": "keyword"},
                "severity": {"type": "keyword"},
                "confidence": {"type": "float"},
                "risk_score": {"type": "float"},
                "src_ips": {"type": "ip", "ignore_malformed": True},
                "dst_ips": {"type": "ip", "ignore_malformed": True},
                "domains": {"type": "keyword"},
                "ports": {"type": "integer"},
                "protocols": {"type": "keyword"},
                "services": {"type": "keyword"},
                "threat_tags": {"type": "keyword"},
                "attack_stages": {"type": "keyword"},
                "mitre_techniques": {"type": "keyword"},
                "ioc_candidates": {"type": "object", "enabled": False},
                "ioc_values": {"type": "keyword"},
                "ioc_types": {"type": "keyword"},
                "row_index": {"type": "integer", "ignore_malformed": True},
                "flow_id": {"type": "keyword"},
                "time_bucket": {"type": "keyword"},
                "provenance_type": {"type": "keyword"},
                "raw_source_file": {"type": "keyword"},
                "evidence_refs": {"type": "keyword"},
                "entity_type": {"type": "keyword"},
                "entity_value": {"type": "keyword"},
                "risk_level": {"type": "keyword"},
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
                        "schema_version": {"type": "keyword"},
                        "action_name": {"type": "keyword"},
                        "finding_id": {"type": "keyword"},
                        "severity": {"type": "keyword"},
                        "row_index": {"type": "integer", "ignore_malformed": True},
                        "flow_id": {"type": "keyword"},
                        "source_file": {"type": "keyword"},
                        "raw_source_file": {"type": "keyword"},
                        "provenance_type": {"type": "keyword"},
                        "evidence_refs": {"type": "keyword"},
                    }
                },
                "payload": {"type": "object", "enabled": False},
            }
        },
    }


def load_elasticsearch_client(config: dict[str, Any]) -> tuple[Any, Any]:
    try:
        from elasticsearch import Elasticsearch
        from elasticsearch.helpers import bulk
    except ImportError as exc:
        raise ImportError(
            "Missing dependency 'elasticsearch'. Install it before indexing RAG docs: pip install elasticsearch"
        ) from exc

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


def source_delete_filters(documents: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Extract unique (dataset_name, source_file, schema_version) delete filters."""
    filters: set[tuple[str, str, str]] = set()
    for doc in documents:
        dataset_name = str(doc.get("dataset_name") or "")
        source_file = str(doc.get("source_file") or "")
        schema_version = str(doc.get("schema_version") or "")
        if not dataset_name or not source_file:
            continue
        filters.add((dataset_name, source_file, schema_version))
    return [
        {
            "dataset_name": dataset_name,
            "source_file": source_file,
            "schema_version": schema_version,
        }
        for dataset_name, source_file, schema_version in sorted(filters)
    ]


def delete_existing_sources(client: Any, index_name: str, filters: list[dict[str, str]]) -> dict[str, Any]:
    """Delete existing documents matching each filter before indexing new ones."""
    results = []
    total_deleted = 0

    for item in filters:
        must = [
            {"term": {"dataset_name": item["dataset_name"]}},
            {"term": {"source_file": item["source_file"]}},
        ]
        if item.get("schema_version"):
            must.append({"term": {"schema_version": item["schema_version"]}})

        body = {"query": {"bool": {"must": must}}}
        response = client.delete_by_query(
            index=index_name,
            body=body,
            conflicts="proceed",
            refresh=True,
        )
        deleted = int(response.get("deleted", 0))
        total_deleted += deleted
        results.append({
            "filter": item,
            "deleted": deleted,
        })

    return {
        "replace_mode": "source",
        "deleted_before_index": total_deleted,
        "delete_filters": results,
    }


def to_bulk_action(index_name: str, document: dict[str, Any]) -> dict[str, Any]:
    return {
        "_op_type": "index",
        "_index": index_name,
        "_id": document["doc_id"],
        "_source": document,
    }


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_manifest(
    *,
    input_files: list[str],
    output_file: Path,
    index_name: str,
    indexed_count: int,
    documents: list[dict[str, Any]],
    index_status: str,
    config: dict[str, Any],
    index_duration_seconds: float,
    es_count_by_dataset: dict[str, int] | None = None,
    replace_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    doc_types = Counter(document["doc_type"] for document in documents)
    dataset_names = sorted({str(document.get("dataset_name", "")) for document in documents if document.get("dataset_name")})
    provenance_type_counts = Counter(
        str(
            document.get("provenance_type")
            or (document.get("metadata") or {}).get("provenance_type")
            or "empty"
        )
        for document in documents
    )
    sample_doc_ids = [document["doc_id"] for document in documents[:10]]

    # Dataset-scoped counts
    input_count_by_dataset: dict[str, int] = {}
    doc_types_by_dataset: dict[str, dict[str, int]] = {}
    for doc in documents:
        ds = doc.get("dataset_name", "unknown")
        input_count_by_dataset[ds] = input_count_by_dataset.get(ds, 0) + 1
        dt = doc.get("doc_type", "unknown")
        if ds not in doc_types_by_dataset:
            doc_types_by_dataset[ds] = {}
        doc_types_by_dataset[ds][dt] = doc_types_by_dataset[ds].get(dt, 0) + 1

    return {
        "input_files": [to_repo_relative_display(item) for item in input_files],
        "sha256_source_files": {to_repo_relative_display(item): sha256_file(Path(item)) for item in input_files},
        "sha256_embedding_file": sha256_file(Path(input_files[0])) if len(input_files) == 1 else "",
        "indexed_count": indexed_count,
        "document_types": dict(doc_types),
        "dataset_names": dataset_names,
        "provenance_type_counts": dict(provenance_type_counts),
        "index_name": index_name,
        "index_status": index_status,
        "hosts": config["hosts"],
        "output_file": to_repo_relative_display(output_file),
        "samples": sample_doc_ids,
        "config_path": to_repo_relative_display(config["config_path"]) if config["config_path"] else "",
        "input_document_count": len(documents),
        "unique_doc_id_count": len(set(d["doc_id"] for d in documents)),
        "bulk_success_count": indexed_count,
        "index_duration_seconds": round(index_duration_seconds, 3),
        # Dataset-scoped validation fields
        "input_document_count_by_dataset": input_count_by_dataset,
        "document_types_by_dataset": doc_types_by_dataset,
        "es_count_by_dataset_after_refresh": es_count_by_dataset or {},
        # Replace-source cleanup info
        "replace_mode": replace_result.get("replace_mode", "none") if replace_result else "none",
        "deleted_before_index": replace_result.get("deleted_before_index", 0) if replace_result else 0,
        "delete_filters": replace_result.get("delete_filters", []) if replace_result else [],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Index network traffic RAG embeddings into Elasticsearch.")
    parser.add_argument("--files", nargs="+", required=True, help="rag_embeddings.jsonl files, directories, or shorthand references")
    parser.add_argument("--index-name", default=None, help="Override Elasticsearch index name. Defaults to config.yaml elasticsearch.index_name (set via ES_INDEX)")
    parser.add_argument("--output-file", default=None, help="Explicit output manifest path. Defaults beside rag_embeddings.jsonl")
    parser.add_argument("--chunk-size", type=int, default=1000, help="Bulk indexing chunk size")
    parser.add_argument("--format", choices=["text", "json"], default="text", help="Output format")
    parser.add_argument("--allow-duplicate-doc-ids", action="store_true", help="Allow duplicate doc_id values; later documents overwrite earlier ones.")
    parser.add_argument("--es-host", default=None, help="Override Elasticsearch host(s)")
    parser.add_argument("--es-username", default=None, help="Override Elasticsearch username")
    parser.add_argument("--es-password", default=None, help="Override Elasticsearch password")
    parser.add_argument("--es-api-key", default=None, help="Override Elasticsearch API key")
    parser.add_argument("--replace-source", action="store_true", help="Delete existing docs for the same dataset_name + source_file + schema_version before indexing.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        index_started_at = time.time()
        load_dotenv_file()
        config = load_app_config()
        es_cli_overrides = {
            "es_host": args.es_host,
            "es_username": args.es_username,
            "es_password": args.es_password,
            "es_api_key": args.es_api_key,
            "es_index": args.index_name,
        }
        elasticsearch_config = resolve_elasticsearch_config(config, cli_overrides=es_cli_overrides)
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

        id_counts = Counter(doc["doc_id"] for doc in all_documents)
        duplicate_ids = [doc_id for doc_id, count in id_counts.items() if count > 1]
        if duplicate_ids and not args.allow_duplicate_doc_ids:
            sample = ", ".join(duplicate_ids[:10])
            raise ValueError(
                f"Duplicate doc_id found: {len(duplicate_ids)} duplicate IDs. "
                f"Sample: {sample}. Fix RAG document identity or pass --allow-duplicate-doc-ids explicitly."
            )

        client, bulk = load_elasticsearch_client(elasticsearch_config)
        if not client.ping():
            raise ConnectionError(
                f"Unable to connect to Elasticsearch hosts: {', '.join(elasticsearch_config['hosts'])}"
            )
        index_status = ensure_index(client, index_name, dimensions)

        replace_result: dict[str, Any] = {
            "replace_mode": "none",
            "deleted_before_index": 0,
            "delete_filters": [],
        }
        if args.replace_source:
            filters = source_delete_filters(all_documents)
            if not filters:
                raise ValueError(
                    "--replace-source requested but no dataset_name/source_file filters could be derived."
                )
            replace_result = delete_existing_sources(client, index_name, filters)

        actions = [to_bulk_action(index_name, flatten_rag_doc_for_index(document)) for document in all_documents]
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

        client.indices.refresh(index=index_name)
        es_count = client.count(index=index_name)["count"]
        # Per-dataset ES counts for shared-index validation
        es_count_by_dataset: dict[str, int] = {}
        for ds_name in sorted({d.get("dataset_name", "unknown") for d in all_documents if d.get("dataset_name")}):
            ds_count_resp = client.count(
                index=index_name,
                body={"query": {"term": {"dataset_name": ds_name}}},
            )
            es_count_by_dataset[ds_name] = ds_count_resp["count"]
        id_counts = Counter(doc["doc_id"] for doc in all_documents)
        duplicate_ids = [doc_id for doc_id, count in id_counts.items() if count > 1]
        schema_versions = sorted({d.get("schema_version", "") for d in all_documents if d.get("schema_version")})
        manifest = build_manifest(
            input_files=files,
            output_file=output_path,
            index_name=index_name,
            indexed_count=success_count,
            documents=all_documents,
            index_status=index_status,
            config=elasticsearch_config,
            index_duration_seconds=time.time() - index_started_at,
            es_count_by_dataset=es_count_by_dataset,
            replace_result=replace_result,
        )
        manifest["es_count_after_refresh"] = es_count
        manifest["duplicate_doc_id_count"] = len(duplicate_ids)
        manifest["duplicate_doc_id_samples"] = duplicate_ids[:10]
        manifest["schema_versions"] = schema_versions
        manifest["mapping_version"] = "rag-v2-provenance"
        output_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

        if args.format == "json":
            print(json.dumps({
                "status": "success",
                "manifest": manifest,
            }, ensure_ascii=False))
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
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
