#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from utils.config import (
    get_config_path,
    load_app_config,
    load_dotenv_file,
    resolve_elasticsearch_config,
)

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[3]
PREPARE_SCRIPT = SCRIPT_DIR / "prepare_pcap.py"
BUILD_DOCS_SCRIPT = SCRIPT_DIR / "build_rag_docs.py"
EMBED_SCRIPT = SCRIPT_DIR / "embed_rag_docs.py"
INDEX_SCRIPT = SCRIPT_DIR / "index_rag_docs.py"
SEARCH_SCRIPT = SCRIPT_DIR / "rag_search.py"
PCAP_PATTERNS = ("*.pcap", "*.pcapng", "*.cap")


def detect_repo_root() -> Path:
    script_path = Path(__file__).resolve()
    for candidate in script_path.parents:
        if (candidate / "config.yaml").exists():
            return candidate
    return REPO_ROOT


def to_repo_relative_display(value: str | Path) -> str:
    path = Path(value).expanduser()
    if not path.is_absolute():
        return path.as_posix()
    try:
        return path.resolve().relative_to(detect_repo_root()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def resolve_script_artifact_path(value: str | Path) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path.resolve()
    candidate = (detect_repo_root() / path).resolve()
    if candidate.exists():
        return candidate
    return path.resolve()


def repo_default_raw_dir() -> Path:
    return detect_repo_root() / "datasets" / "network-traffic" / "raw"


def repo_default_processed_dir() -> Path:
    return detect_repo_root() / "datasets" / "network-traffic" / "processed"


def discover_pcaps(raw_dir: Path) -> list[Path]:
    files: list[Path] = []
    for pattern in PCAP_PATTERNS:
        files.extend(sorted(raw_dir.rglob(pattern)))
    deduped: list[Path] = []
    seen: set[str] = set()
    for path in files:
        resolved = str(path.resolve())
        if resolved not in seen:
            deduped.append(path.resolve())
            seen.add(resolved)
    return deduped


def sanitize_name(value: str) -> str:
    filtered = "".join(ch if ch.isalnum() or ch in "._-" else "-" for ch in value.strip())
    return filtered.strip("-._") or "dataset"


def classify_input(path: Path) -> str:
    """Classify an input file as pcap or flow_csv."""
    suffix = path.suffix.lower()
    name = path.name.lower()
    if suffix in {".pcap", ".pcapng", ".cap"}:
        return "pcap"
    if suffix == ".csv" or name.endswith(".flow.csv"):
        return "flow_csv"
    raise ValueError(f"Unsupported input file for RAG build: {path}")


def dataset_name_from_flow_csv(path: Path) -> str:
    """Infer dataset name from flow.csv filename.

    Handles `.flow.csv` suffix (e.g. Neris.flow.csv -> Neris) and falls back
    to stem for other CSV naming conventions.
    """
    name = path.name
    if name.lower().endswith(".flow.csv"):
        name = name[: -len(".flow.csv")]
    else:
        name = path.stem
    return sanitize_name(name)


def dataset_name_from_pcap(path: Path) -> str:
    return sanitize_name(path.stem)


def discover_inputs(args) -> list[Path]:
    """Discover input files from --files or fallback to --raw-dir."""
    if args.files:
        result: list[Path] = []
        for path_str in args.files:
            p = Path(path_str).expanduser()
            if not p.is_absolute():
                p = (detect_repo_root() / p).resolve()
            if p.is_dir():
                result.extend(sorted(p.rglob("*.pcap")))
                result.extend(sorted(p.rglob("*.pcapng")))
                result.extend(sorted(p.rglob("*.cap")))
            elif p.exists():
                result.append(p)
            else:
                raise FileNotFoundError(f"Input path not found: {p}")
        deduped: list[Path] = []
        seen: set[str] = set()
        for f in result:
            r = str(f.resolve())
            if r not in seen:
                deduped.append(f)
                seen.add(r)
        return deduped
    # Fallback to raw-dir discovery
    raw_dir = Path(args.raw_dir).expanduser().resolve()
    if not raw_dir.exists():
        raise FileNotFoundError(f"Raw directory not found: {raw_dir}")
    return discover_pcaps(raw_dir)


def extract_manifest(result: dict[str, Any]) -> dict[str, Any]:
    """Extract manifest from script output, compatible with old and new structures."""
    manifest = result.get("manifest")
    if isinstance(manifest, dict):
        return manifest
    return result


def run_json_command(
    command: list[str],
    *,
    description: str,
    stream_output: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    if dry_run:
        print(f"  [dry-run] {' '.join(command)}")
        return {"dry_run": True}
    if not stream_output:
        completed = subprocess.run(command, capture_output=True, text=True)
        if completed.returncode != 0:
            raise RuntimeError(
                f"{description} failed.\nCommand: {' '.join(command)}\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}"
            )
        output = completed.stdout.strip()
        try:
            return json.loads(output)
        except json.JSONDecodeError:
            lines = [ln for ln in output.splitlines() if ln.strip()]
            if lines:
                joined = "".join(lines)
                try:
                    return json.loads(joined)
                except json.JSONDecodeError:
                    pass
                last = lines[-1].strip()
                try:
                    return json.loads(last)
                except json.JSONDecodeError:
                    pass
            raise RuntimeError(
                f"{description} returned non-JSON output.\nCommand: {' '.join(command)}\nRaw output:\n{completed.stdout}"
            )

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert process.stdout is not None
    captured_lines: list[str] = []
    for line in process.stdout:
        captured_lines.append(line.rstrip("\n"))
        print(f"    {line.rstrip()}")
    returncode = process.wait()
    output_text = "\n".join(captured_lines).strip()
    if returncode != 0:
        raise RuntimeError(
            f"{description} failed.\nCommand: {' '.join(command)}\nOutput:\n{output_text}"
        )
    if not captured_lines:
        raise RuntimeError(
            f"{description} returned no output.\nCommand: {' '.join(command)}"
        )
    payload = captured_lines[-1].strip()
    try:
        return json.loads(payload)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"{description} returned non-JSON output.\nCommand: {' '.join(command)}\nRaw output:\n{output_text}"
        ) from exc


def maybe_run_json_command(
    command: list[str],
    *,
    description: str,
    stream_output: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Wrapper that supports dry-run mode."""
    return run_json_command(command, description=description, stream_output=stream_output, dry_run=dry_run)


def dataset_artifact_paths(processed_dir: Path, dataset_name: str) -> dict[str, Path]:
    dataset_dir = processed_dir / dataset_name
    rag_dir = dataset_dir / "rag"
    return {
        "dataset_dir": dataset_dir,
        "flow_csv": dataset_dir / f"{dataset_name}.flow.csv",
        "rag_docs": rag_dir / "rag_docs.jsonl",
        "rag_embeddings": rag_dir / "rag_embeddings.jsonl",
        "index_manifest": rag_dir / "index_manifest.json",
    }


def build_es_overrides(index_name: str, args) -> list[str]:
    """Build CLI flags to forward ES config to child scripts."""
    flags: list[str] = []
    for flag_name, value in [
        ("--es-host", args.es_host),
        ("--es-username", args.es_username),
        ("--es-password", args.es_password),
        ("--es-api-key", args.es_api_key),
        ("--index-name", index_name),
    ]:
        if value:
            flags.extend([flag_name, value])
    return flags


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a unified Elasticsearch RAG index from pcap or flow.csv files."
    )
    # Input discovery
    parser.add_argument("--files", nargs="+", default=None, help="pcap or flow.csv files/directories (alternative to --raw-dir)")
    parser.add_argument("--dataset-name", default=None, help="Override dataset name (default: inferred from filename)")
    parser.add_argument("--raw-dir", default=str(repo_default_raw_dir()), help="Directory containing raw pcap files (fallback if --files not set)")
    parser.add_argument("--processed-dir", default=str(repo_default_processed_dir()), help="Directory containing processed datasets")

    # Index config
    parser.add_argument("--index-name", default=None, help="Override Elasticsearch index name (falls back to NETWORK_TRAFFIC_ES_INDEX via config.yaml)")
    parser.add_argument("--format", choices=["text", "json"], default="text", help="Output format")
    parser.add_argument("--verbose", action="store_true", help="Stream child-script output while running")

    # Skip/force flags
    parser.add_argument("--skip-docs", action="store_true", help="Skip RAG document generation, reuse existing rag_docs.jsonl")
    parser.add_argument("--skip-embeddings", action="store_true", help="Skip embedding generation, reuse existing rag_embeddings.jsonl")
    parser.add_argument("--skip-index", action="store_true", help="Skip ES indexing")
    parser.add_argument("--force-docs", action="store_true", help="Regenerate RAG documents even if they exist")
    parser.add_argument("--force-embeddings", action="store_true", help="Regenerate embeddings even if they exist")
    parser.add_argument("--force-index", action="store_true", help="Reindex into ES even if manifest exists")
    parser.add_argument("--rebuild-existing", action="store_true", help="Legacy: same as --force-docs --force-embeddings --force-index")

    # Embedding overrides
    parser.add_argument("--model", default=None, help="Embedding model name (forwarded to embed script)")
    parser.add_argument("--batch-size", type=int, default=None, help="Embedding batch size (forwarded to embed script)")
    parser.add_argument("--dimensions", type=int, default=None, help="Embedding dimensions (forwarded to embed script)")

    # Verification
    parser.add_argument("--dry-run", action="store_true", help="Show what would be processed without executing")
    parser.add_argument("--verify-search", action="store_true", help="Run rag_search after indexing to verify searchability")
    parser.add_argument("--verify-query", default="network traffic overview anomaly scan", help="Query for verify-search (default: 'network traffic overview anomaly scan')")

    # ES overrides
    parser.add_argument("--es-host", default=None, help="ES host override (forwarded to index/search)")
    parser.add_argument("--es-username", default=None, help="ES username override")
    parser.add_argument("--es-password", default=None, help="ES password override")
    parser.add_argument("--es-api-key", default=None, help="ES API key override")
    parser.add_argument("--replace-source", action="store_true", help="Delete existing docs for the same dataset/source before indexing.")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        load_dotenv_file()

        # Load config and resolve index_name before any processing
        config = load_app_config()
        elasticsearch_config = resolve_elasticsearch_config(
            config,
            cli_overrides={
                "es_host": args.es_host,
                "es_username": args.es_username,
                "es_password": args.es_password,
                "es_api_key": args.es_api_key,
                "es_index": args.index_name,
            },
        )
        index_name = elasticsearch_config["index_name"]

        # Map legacy --rebuild-existing to force flags
        if args.rebuild_existing:
            args.force_docs = True
            args.force_embeddings = True
            args.force_index = True

        processed_dir = Path(args.processed_dir).expanduser().resolve()
        input_files = discover_inputs(args)
        if not input_files:
            raise ValueError("No input files found")

        es_overrides = build_es_overrides(index_name, args)
        stream_output = args.verbose
        results: list[dict[str, Any]] = []

        if args.format == "text":
            print(f"发现 {len(input_files)} 个输入文件，目标索引：{index_name}")
            if args.dry_run:
                print("[dry-run 模式] 不会执行任何操作")
            if args.verbose:
                print("已开启详细日志，将输出各子脚本的实时结果。")

        for file_index, input_file in enumerate(input_files, start=1):
            input_kind = classify_input(input_file)
            dataset_name = args.dataset_name or (
                dataset_name_from_flow_csv(input_file) if input_kind == "flow_csv"
                else dataset_name_from_pcap(input_file)
            )
            artifacts = dataset_artifact_paths(processed_dir, dataset_name)

            if args.format == "text":
                prefix = f"[{file_index}/{len(input_files)}]"
                print(f"{prefix} 开始处理 {dataset_name} (输入类型: {input_kind})")
                print(f"  原始文件：{to_repo_relative_display(input_file)}")

            # Step 1: Get flow_csv
            flow_csv_path: Path | None = None
            if input_kind == "flow_csv":
                flow_csv_path = input_file.resolve()
                if args.format == "text" and not args.skip_docs:
                    print("  步骤 1/4：使用已有 flow.csv 作为输入")
            else:
                # pcap → prepare_pcap
                if args.format == "text" and not args.skip_docs:
                    print("  步骤 1/4：预处理 pcap")
                prepare_result = maybe_run_json_command(
                    [
                        sys.executable,
                        str(PREPARE_SCRIPT),
                        "--files",
                        str(input_file),
                        "--dataset-name",
                        dataset_name,
                        "--format",
                        "json",
                    ],
                    description=f"prepare_pcap for {input_file.name}",
                    stream_output=stream_output,
                    dry_run=args.dry_run,
                )
                if not prepare_result.get("dry_run"):
                    flow_csv_path = Path(prepare_result["flow_csv"]).resolve()
                elif args.format == "text":
                    print(f"  [dry-run] 将生成 flow_csv")

            if flow_csv_path is None and not args.dry_run:
                raise RuntimeError(f"flow_csv not available for {dataset_name}")

            if args.format == "text" and flow_csv_path and not args.skip_docs:
                print(f"  flow 文件：{to_repo_relative_display(flow_csv_path)}")

            # Step 2: Build RAG docs
            rag_docs_path: Path | None = None
            existing_rag_docs = artifacts["rag_docs"]
            if not args.skip_docs:
                need_build = args.force_docs or not existing_rag_docs.exists()
                if not need_build:
                    rag_docs_path = existing_rag_docs.resolve()
                    if args.format == "text":
                        print("  步骤 2/4：复用已有 RAG 文档")
                else:
                    if args.format == "text":
                        print("  步骤 2/4：构建 RAG 文档")
                    build_args = [
                        sys.executable,
                        str(BUILD_DOCS_SCRIPT),
                        "--files",
                        str(flow_csv_path or ""),
                        "--dataset-name",
                        dataset_name,
                        "--format",
                        "json",
                    ]
                    build_result = maybe_run_json_command(
                        build_args,
                        description=f"build_rag_docs for {dataset_name}",
                        stream_output=stream_output,
                        dry_run=args.dry_run,
                    )
                    if not build_result.get("dry_run"):
                        build_manifest = extract_manifest(build_result)
                        rag_docs_path = resolve_script_artifact_path(build_manifest.get("output_file", ""))
                    else:
                        # dry-run: predict default output path beside flow_csv
                        rag_docs_path = existing_rag_docs.resolve() if existing_rag_docs.exists() else (
                            existing_rag_docs.parent / "rag_docs.jsonl"
                        )
                        if args.format == "text":
                            print(f"  [dry-run] 将生成 rag_docs.jsonl")
            else:
                if args.format == "text":
                    print("  步骤 2/4：跳过 RAG 文档生成（--skip-docs）")
                if existing_rag_docs.exists():
                    rag_docs_path = existing_rag_docs.resolve()

            # Step 3: Embedding
            rag_embeddings_path: Path | None = None
            existing_rag_embeddings = artifacts["rag_embeddings"]
            if not args.skip_embeddings and rag_docs_path:
                need_embed = args.force_embeddings or not existing_rag_embeddings.exists()
                if not need_embed:
                    rag_embeddings_path = existing_rag_embeddings.resolve()
                    if args.format == "text":
                        print("  步骤 3/4：复用已有 embedding")
                else:
                    if args.format == "text":
                        print("  步骤 3/4：生成 embedding")
                    embed_args = [
                        sys.executable,
                        str(EMBED_SCRIPT),
                        "--files",
                        str(rag_docs_path),
                    ]
                    if args.force_embeddings:
                        embed_args.append("--no-reuse-existing")
                    embed_args.extend([
                        "--format",
                        "json",
                    ])
                    if args.model:
                        embed_args.extend(["--model", args.model])
                    if args.batch_size is not None:
                        embed_args.extend(["--batch-size", str(args.batch_size)])
                    if args.dimensions is not None:
                        embed_args.extend(["--dimensions", str(args.dimensions)])
                    embed_result = maybe_run_json_command(
                        embed_args,
                        description=f"embed_rag_docs for {dataset_name}",
                        stream_output=stream_output,
                        dry_run=args.dry_run,
                    )
                    if not embed_result.get("dry_run"):
                        embed_manifest = extract_manifest(embed_result)
                        rag_embeddings_path = resolve_script_artifact_path(embed_manifest["output_file"])
                    else:
                        # dry-run: predict default output path beside rag_docs
                        rag_embeddings_path = existing_rag_embeddings.resolve() if existing_rag_embeddings.exists() else (
                            existing_rag_embeddings.parent / "rag_embeddings.jsonl"
                        )
                        if args.format == "text":
                            print(f"  [dry-run] 将生成 rag_embeddings.jsonl")
            elif args.skip_embeddings:
                if args.format == "text":
                    print("  步骤 3/4：跳过 embedding（--skip-embeddings）")
                if existing_rag_embeddings.exists():
                    rag_embeddings_path = existing_rag_embeddings.resolve()
            else:
                if args.format == "text":
                    print("  步骤 3/4：跳过 embedding（无 rag_docs 路径）")

            if args.format == "text" and rag_embeddings_path and not args.skip_index:
                print(f"  embedding 文件：{to_repo_relative_display(rag_embeddings_path)}")

            # Step 4: Index to ES
            index_result: dict[str, Any] = {}
            existing_index_manifest = artifacts["index_manifest"]
            if not args.skip_index and rag_embeddings_path:
                need_index = args.force_index or not existing_index_manifest.exists()
                if not need_index:
                    if args.format == "text":
                        print("  步骤 4/4：复用已有 index manifest")
                    # Read existing manifest for result summary
                    if existing_index_manifest.exists():
                        try:
                            index_result = json.loads(existing_index_manifest.read_text(encoding="utf-8"))
                        except Exception:
                            pass
                else:
                    if args.format == "text":
                        label = "写入 Elasticsearch（replace-source）" if args.replace_source else "写入 Elasticsearch"
                        print(f"  步骤 4/4：{label}")
                    index_args = [
                        sys.executable,
                        str(INDEX_SCRIPT),
                        "--files",
                        str(rag_embeddings_path),
                        "--format",
                        "json",
                    ]
                    if args.replace_source:
                        index_args.append("--replace-source")
                    index_args.extend(es_overrides)
                    index_result = maybe_run_json_command(
                        index_args,
                        description=f"index_rag_docs for {dataset_name}",
                        stream_output=stream_output,
                        dry_run=args.dry_run,
                    )
                    index_manifest = extract_manifest(index_result)
                    if not index_result.get("dry_run"):
                        # Dataset-scoped count validation for shared indices.
                        # Always validates when real indexing runs (including --force-index).
                        # NOTE: dataset-level counts assume the dataset is being
                        # replaced or has no stale documents from prior builds.
                        input_count_by_ds = index_manifest.get("input_document_count_by_dataset", {})
                        es_count_by_ds = index_manifest.get("es_count_by_dataset_after_refresh", {})
                        if need_index:
                            ds_input = input_count_by_ds.get(dataset_name, 0)
                            ds_es = es_count_by_ds.get(dataset_name, -1)
                            if ds_input != ds_es:
                                raise RuntimeError(
                                    f"ES count mismatch for {dataset_name}: "
                                    f"input_document_count={ds_input}, "
                                    f"es_count_after_refresh={ds_es}"
                                )
                        if args.format == "text":
                            print(f"  完成 {dataset_name}：新增 {index_manifest.get('bulk_success_count', 0)} 条索引文档")
                    elif args.format == "text":
                        print(f"  [dry-run] 将写入 Elasticsearch")
            elif args.skip_index:
                if args.format == "text":
                    print("  步骤 4/4：跳过 ES 索引（--skip-index）")

            # Collect result
            result_entry: dict[str, Any] = {
                "dataset_name": dataset_name,
                "input_file": to_repo_relative_display(input_file),
                "input_kind": input_kind,
            }
            if flow_csv_path:
                result_entry["flow_csv"] = to_repo_relative_display(flow_csv_path)
            if rag_docs_path:
                result_entry["rag_docs"] = to_repo_relative_display(rag_docs_path)
            if rag_embeddings_path:
                result_entry["rag_embeddings"] = to_repo_relative_display(rag_embeddings_path)
            if index_result and not index_result.get("dry_run"):
                index_manifest = extract_manifest(index_result)
                result_entry["index_manifest"] = index_manifest.get("output_file", "")
                result_entry["input_document_count"] = index_manifest.get("input_document_count", 0)
                result_entry["unique_doc_id_count"] = index_manifest.get("unique_doc_id_count", 0)
                result_entry["duplicate_doc_id_count"] = index_manifest.get("duplicate_doc_id_count", 0)
                result_entry["bulk_success_count"] = index_manifest.get("bulk_success_count", 0)
                result_entry["es_count_after_refresh"] = index_manifest.get("es_count_after_refresh", 0)
            results.append(result_entry)

        # Verify search — per-dataset loop so each dataset is independently validated
        verify_results: list[dict[str, Any]] = []
        if args.verify_search and not args.dry_run and not args.skip_index:
            processed_dataset_names: list[str] = []
            for r in results:
                if r.get("input_document_count") or r.get("index_manifest"):
                    processed_dataset_names.append(r["dataset_name"])

            if args.format == "text":
                print(f"\n验证搜索：{args.verify_query}")

            for ds_name in processed_dataset_names:
                ds_search_args = [
                    sys.executable,
                    str(SEARCH_SCRIPT),
                    "--query",
                    args.verify_query,
                    "--dataset-name",
                    ds_name,
                    "--size",
                    "5",
                    "--format",
                    "json",
                ]
                ds_search_args.extend(es_overrides)
                try:
                    ds_search_result = maybe_run_json_command(
                        ds_search_args,
                        description=f"verify-search for {ds_name}",
                        stream_output=False,
                        dry_run=args.dry_run,
                    )
                    hit_count = ds_search_result.get("hit_count", 0)
                    verify_results.append({
                        "dataset_name": ds_name,
                        "verify_query": args.verify_query,
                        "verify_search_hit_count": hit_count,
                        "verify_search_succeeded": hit_count > 0,
                    })
                    if args.format == "text":
                        status = "成功" if hit_count > 0 else "无命中"
                        print(f"  {ds_name}: {status}（命中 {hit_count} 条）")
                except Exception as exc:
                    verify_results.append({
                        "dataset_name": ds_name,
                        "verify_query": args.verify_query,
                        "verify_search_hit_count": 0,
                        "verify_search_succeeded": False,
                        "verify_search_error": str(exc),
                    })
                    if args.format == "text":
                        print(f"  {ds_name}: 失败（{exc}）")

        # Build summary
        summary = {
            "index_name": index_name,
            "input_count": len(input_files),
            "processed_count": len(results),
            "dry_run": args.dry_run,
            "results": results,
        }
        if verify_results:
            summary["verify_search"] = verify_results

        # Summary text output
        if args.format == "json":
            print(json.dumps(summary, ensure_ascii=False))
        else:
            print(
                "\n".join(
                    [
                        "",
                        f"索引名称：{index_name}",
                        f"输入文件数：{len(input_files)}",
                        f"已处理数据集：{len(results)}",
                        f"干运行：{args.dry_run}",
                    ]
                )
            )
            if verify_results and not args.dry_run:
                for v in verify_results:
                    status = "成功" if v.get("verify_search_succeeded") else "失败"
                    print(
                        f"验证搜索 [{v.get('dataset_name', '')}]: {v.get('verify_query', '')} → "
                        f"{status} ({v.get('verify_search_hit_count', 0)} 条命中)"
                    )

        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
