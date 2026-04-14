#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent


def detect_repo_root() -> Path:
    script_path = Path(__file__).resolve()
    for candidate in script_path.parents:
        if (candidate / "config.yaml").exists():
            return candidate
    return SCRIPT_DIR.parents[3]


REPO_ROOT = detect_repo_root()
PREPARE_SCRIPT = SCRIPT_DIR / "prepare_pcap.py"
BUILD_DOCS_SCRIPT = SCRIPT_DIR / "build_rag_docs.py"
EMBED_SCRIPT = SCRIPT_DIR / "embed_rag_docs.py"
INDEX_SCRIPT = SCRIPT_DIR / "index_rag_docs.py"
PCAP_PATTERNS = ("*.pcap", "*.pcapng", "*.cap")
DEFAULT_INDEX_NAME = "network-traffic-rag"


def to_repo_relative_display(value: str | Path) -> str:
    path = Path(value).expanduser()
    if not path.is_absolute():
        return path.as_posix()
    try:
        return path.resolve().relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def load_dotenv_file() -> None:
    dotenv_path = REPO_ROOT / ".env"
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


def resolve_script_artifact_path(value: str | Path) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path.resolve()
    candidate = (REPO_ROOT / path).resolve()
    if candidate.exists():
        return candidate
    return path.resolve()


def repo_default_raw_dir() -> Path:
    return REPO_ROOT / "datasets" / "network-traffic" / "raw"


def repo_default_processed_dir() -> Path:
    return REPO_ROOT / "datasets" / "network-traffic" / "processed"


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


def dataset_name_from_pcap(path: Path) -> str:
    return sanitize_name(path.stem)


def run_json_command(
    command: list[str],
    *,
    description: str,
    stream_output: bool = False,
) -> dict[str, Any]:
    if not stream_output:
        completed = subprocess.run(command, capture_output=True, text=True)
        if completed.returncode != 0:
            raise RuntimeError(
                f"{description} failed.\nCommand: {' '.join(command)}\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}"
            )
        payload = completed.stdout.strip().splitlines()[-1].strip()
        try:
            return json.loads(payload)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"{description} returned non-JSON output.\nCommand: {' '.join(command)}\nRaw output:\n{completed.stdout}"
            ) from exc

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


def manifest_path_for_dataset(processed_dir: Path, dataset_name: str) -> Path:
    return processed_dir / dataset_name / "rag" / "index_manifest.json"


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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Preprocess all network-traffic pcaps and build a unified Elasticsearch RAG index.")
    parser.add_argument("--raw-dir", default=str(repo_default_raw_dir()), help="Directory containing raw pcap files")
    parser.add_argument("--processed-dir", default=str(repo_default_processed_dir()), help="Directory containing processed datasets")
    parser.add_argument("--index-name", default=DEFAULT_INDEX_NAME, help="Elasticsearch index name")
    parser.add_argument("--rebuild-existing", action="store_true", help="Rebuild datasets that already have an index manifest")
    parser.add_argument("--format", choices=["text", "json"], default="text", help="Output format")
    parser.add_argument("--verbose", action="store_true", help="Stream child-script output while running")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        load_dotenv_file()
        raw_dir = Path(args.raw_dir).expanduser().resolve()
        processed_dir = Path(args.processed_dir).expanduser().resolve()
        if not raw_dir.exists():
            raise FileNotFoundError(f"Raw directory not found: {raw_dir}")

        pcap_files = discover_pcaps(raw_dir)
        if not pcap_files:
            raise ValueError(f"No pcap files found under {raw_dir}")

        results: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        stream_output = args.verbose

        if args.format == "text":
            print(f"发现 {len(pcap_files)} 个 pcap 文件，目标索引：{args.index_name}")
            if args.verbose:
                print("已开启详细日志，将输出各子脚本的实时结果。")

        for index, pcap_file in enumerate(pcap_files, start=1):
            dataset_name = dataset_name_from_pcap(pcap_file)
            artifacts = dataset_artifact_paths(processed_dir, dataset_name)
            existing_manifest = artifacts["index_manifest"]
            if existing_manifest.exists() and not args.rebuild_existing:
                if args.format == "text":
                    print(f"[{index}/{len(pcap_files)}] 跳过 {dataset_name}：已存在索引清单")
                skipped.append(
                    {
                        "dataset_name": dataset_name,
                        "pcap_file": to_repo_relative_display(pcap_file),
                        "reason": "index_manifest_exists",
                        "index_manifest": to_repo_relative_display(existing_manifest),
                    }
                )
                continue

            if args.format == "text":
                print(f"[{index}/{len(pcap_files)}] 开始处理 {dataset_name}")
                print(f"  原始文件：{to_repo_relative_display(pcap_file)}")

            if artifacts["flow_csv"].exists() and not args.rebuild_existing:
                flow_csv = str(artifacts["flow_csv"].resolve())
                if args.format == "text":
                    print("  步骤 1/4：复用已有 flow.csv")
            else:
                if args.format == "text":
                    print("  步骤 1/4：预处理 pcap")
                prepare_result = run_json_command(
                    [
                        sys.executable,
                        str(PREPARE_SCRIPT),
                        "--files",
                        str(pcap_file),
                        "--dataset-name",
                        dataset_name,
                        "--format",
                        "json",
                    ],
                    description=f"prepare_pcap for {pcap_file.name}",
                    stream_output=stream_output,
                )
                flow_csv = str(resolve_script_artifact_path(prepare_result["flow_csv"]))

            if args.format == "text":
                print(f"  已生成 flow：{to_repo_relative_display(flow_csv)}")

            if artifacts["rag_docs"].exists() and not args.rebuild_existing:
                rag_docs = str(artifacts["rag_docs"].resolve())
                if args.format == "text":
                    print("  步骤 2/4：复用已有 RAG 文档")
            else:
                if args.format == "text":
                    print("  步骤 2/4：构建 RAG 文档")
                build_docs_result = run_json_command(
                    [
                        sys.executable,
                        str(BUILD_DOCS_SCRIPT),
                        "--files",
                        flow_csv,
                        "--dataset-name",
                        dataset_name,
                        "--format",
                        "json",
                    ],
                    description=f"build_rag_docs for {dataset_name}",
                    stream_output=stream_output,
                )
                rag_docs = str(resolve_script_artifact_path(build_docs_result["output_file"]))

            if args.format == "text":
                print(f"  已生成文档：{to_repo_relative_display(rag_docs)}")

            if artifacts["rag_embeddings"].exists() and not args.rebuild_existing:
                rag_embeddings = str(artifacts["rag_embeddings"].resolve())
                if args.format == "text":
                    print("  步骤 3/4：复用已有 embedding")
            else:
                if args.format == "text":
                    print("  步骤 3/4：生成 embedding")
                embed_result = run_json_command(
                    [
                        sys.executable,
                        str(EMBED_SCRIPT),
                        "--files",
                        rag_docs,
                        "--format",
                        "json",
                    ],
                    description=f"embed_rag_docs for {dataset_name}",
                    stream_output=stream_output,
                )
                rag_embeddings = str(resolve_script_artifact_path(embed_result["output_file"]))

            if args.format == "text":
                print(f"  已生成向量：{to_repo_relative_display(rag_embeddings)}")
                print("  步骤 4/4：写入 Elasticsearch")
            index_result = run_json_command(
                [
                    sys.executable,
                    str(INDEX_SCRIPT),
                    "--files",
                    rag_embeddings,
                    "--index-name",
                    args.index_name,
                    "--format",
                    "json",
                ],
                description=f"index_rag_docs for {dataset_name}",
                stream_output=stream_output,
            )

            results.append(
                {
                    "dataset_name": dataset_name,
                    "pcap_file": to_repo_relative_display(pcap_file),
                    "flow_csv": to_repo_relative_display(flow_csv),
                    "rag_docs": to_repo_relative_display(rag_docs),
                    "rag_embeddings": to_repo_relative_display(rag_embeddings),
                    "index_manifest": index_result["output_file"],
                    "indexed_count": index_result["indexed_count"],
                    "index_name": index_result["index_name"],
                }
            )
            if args.format == "text":
                print(f"  完成 {dataset_name}：新增 {index_result['indexed_count']} 条索引文档")

        summary = {
            "raw_dir": to_repo_relative_display(raw_dir),
            "processed_dir": to_repo_relative_display(processed_dir),
            "index_name": args.index_name,
            "pcap_count": len(pcap_files),
            "processed_count": len(results),
            "skipped_count": len(skipped),
            "processed": results,
            "skipped": skipped,
        }

        if args.format == "json":
            print(json.dumps(summary, ensure_ascii=False))
        else:
            print(
                "\n".join(
                    [
                        "",
                        f"Raw directory: {to_repo_relative_display(raw_dir)}",
                        f"PCAP files discovered: {len(pcap_files)}",
                        f"Processed datasets: {len(results)}",
                        f"Skipped datasets: {len(skipped)}",
                        f"Unified index: {args.index_name}",
                    ]
                )
            )
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
