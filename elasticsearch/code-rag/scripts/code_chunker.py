#!/usr/bin/env python3
"""Code filtering, chunking, and metadata generation for Elasticsearch code RAG."""

from __future__ import annotations

import argparse
import ast
import fnmatch
import hashlib
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


DEFAULT_INCLUDE_GLOBS = ["*.py", "*.ts", "*.tsx", "*.js", "*.jsx", "*.md", "*.yaml", "*.yml", "*.json"]
DEFAULT_EXCLUDE_DIRS = {
    ".git",
    ".hg",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "__pycache__",
    "node_modules",
    "dist",
    "build",
    "coverage",
    "generated",
    "vendor",
    ".next",
    ".turbo",
    "tmp",
}
DEFAULT_EXCLUDE_GLOBS = {".env", ".env.*", "*.pem", "*.key"}


@dataclass(frozen=True)
class CodeChunk:
    id: str
    repo: str
    path: str
    absolute_path: str
    language: str
    kind: str
    symbol: str
    start_line: int
    end_line: int
    imports: list[str]
    tags: list[str]
    content_hash: str
    file_hash: str
    code: str
    metadata: dict[str, Any]


def detect_language(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".py":
        return "python"
    if suffix in {".ts", ".tsx"}:
        return "typescript"
    if suffix in {".js", ".jsx"}:
        return "javascript"
    if suffix == ".md":
        return "markdown"
    if suffix in {".yaml", ".yml"}:
        return "yaml"
    if suffix == ".json":
        return "json"
    return "text"


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def matches_any_glob(relative_path: str, filename: str, patterns: set[str] | list[str]) -> bool:
    return any(fnmatch.fnmatch(filename, pattern) or fnmatch.fnmatch(relative_path, pattern) for pattern in patterns)


def iter_source_files(
    root_path: Path,
    *,
    include_globs: list[str] | None = None,
    exclude_dirs: set[str] | None = None,
    exclude_globs: set[str] | None = None,
    max_file_size_bytes: int = 512_000,
    max_files_scanned: int = 20_000,
) -> list[Path]:
    include_globs = include_globs or DEFAULT_INCLUDE_GLOBS
    exclude_dirs = exclude_dirs or DEFAULT_EXCLUDE_DIRS
    exclude_globs = exclude_globs or DEFAULT_EXCLUDE_GLOBS
    files: list[Path] = []
    visited = 0

    for path in root_path.rglob("*"):
        if not path.is_file():
            continue
        if visited >= max_files_scanned:
            break
        visited += 1

        rel_parts = path.relative_to(root_path).parts
        if any(part in exclude_dirs for part in rel_parts[:-1]):
            continue
        rel = path.relative_to(root_path).as_posix()
        if matches_any_glob(rel, path.name, exclude_globs):
            continue
        if not matches_any_glob(rel, path.name, include_globs):
            continue
        try:
            if path.stat().st_size > max_file_size_bytes:
                continue
        except OSError:
            continue
        files.append(path)
    return files


def line_slice(lines: list[str], start_line: int, end_line: int) -> str:
    return "\n".join(lines[max(1, start_line) - 1 : max(start_line, end_line)])


def raw_chunks(text: str, path: Path) -> list[dict[str, Any]]:
    language = detect_language(path)
    if language == "python":
        return raw_python_chunks(text, language=language)
    return raw_file_chunk(text, language=language)


def raw_file_chunk(text: str, *, language: str) -> list[dict[str, Any]]:
    return [
        {
            "code": text,
            "kind": "file",
            "symbol": "module",
            "start_line": 1,
            "end_line": max(1, len(text.splitlines())),
            "language": language,
        }
    ]


def raw_python_chunks(text: str, *, language: str) -> list[dict[str, Any]]:
    lines = text.splitlines()
    if not lines:
        return raw_file_chunk(text, language=language)
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return raw_file_chunk(text, language=language)

    chunks: list[dict[str, Any]] = []
    header_end = python_header_end_line(tree)
    if header_end > 0:
        chunks.append(
            {
                "code": line_slice(lines, 1, header_end),
                "kind": "file_header",
                "symbol": "module",
                "start_line": 1,
                "end_line": header_end,
                "language": language,
            }
        )

    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            chunks.append(python_node_chunk(lines, node, kind="function", symbol=node.name, language=language))
        elif isinstance(node, ast.ClassDef):
            chunks.append(python_node_chunk(lines, node, kind="class", symbol=node.name, language=language))
            for child in node.body:
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    chunks.append(
                        python_node_chunk(
                            lines,
                            child,
                            kind="method",
                            symbol=f"{node.name}.{child.name}",
                            language=language,
                        )
                    )
    return chunks or raw_file_chunk(text, language=language)


def python_header_end_line(tree: ast.Module) -> int:
    header_end = 0
    for node in tree.body:
        if isinstance(node, ast.Expr) and isinstance(getattr(node, "value", None), ast.Constant) and isinstance(node.value.value, str):
            header_end = getattr(node, "end_lineno", node.lineno)
            continue
        if isinstance(node, (ast.Import, ast.ImportFrom, ast.Assign, ast.AnnAssign)):
            header_end = getattr(node, "end_lineno", node.lineno)
            continue
        break
    return header_end


def python_node_chunk(lines: list[str], node: ast.AST, *, kind: str, symbol: str, language: str) -> dict[str, Any]:
    start_line = getattr(node, "lineno", 1)
    end_line = getattr(node, "end_lineno", start_line)
    return {
        "code": line_slice(lines, start_line, end_line),
        "kind": kind,
        "symbol": symbol,
        "start_line": start_line,
        "end_line": end_line,
        "language": language,
    }


def extract_imports(text: str, language: str) -> list[str]:
    if language == "python":
        return extract_python_imports(text)
    if language in {"javascript", "typescript"}:
        return extract_js_imports(text)
    return []


def extract_python_imports(text: str) -> list[str]:
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return []
    imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name.split(".", maxsplit=1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.append(node.module.split(".", maxsplit=1)[0])
    return sorted(set(imports))


def extract_js_imports(text: str) -> list[str]:
    imports: list[str] = []
    for pattern in (r"\bfrom\s+['\"]([^'\"]+)['\"]", r"\bimport\s+['\"]([^'\"]+)['\"]", r"\brequire\(\s*['\"]([^'\"]+)['\"]\s*\)"):
        imports.extend(match.group(1) for match in re.finditer(pattern, text))
    return sorted(set(imports))


def infer_tags(*, relative_path: str, language: str, kind: str, symbol: str, imports: list[str]) -> list[str]:
    lower_path = relative_path.lower()
    lower_symbol = symbol.lower()
    lower_imports = {item.lower() for item in imports}
    tags = {language, kind}
    if "test" in lower_path or lower_path.endswith("_test.py") or ".test." in lower_path:
        tags.add("test")
    if "config" in lower_path or "config" in lower_symbol:
        tags.add("config")
    if "tool" in lower_path or "tool" in lower_symbol or "langchain" in lower_imports:
        tags.add("tool")
    if "agent" in lower_path or "agent" in lower_symbol or "langgraph" in lower_imports:
        tags.add("agent")
    if "api" in lower_path or "router" in lower_path or "fastapi" in lower_imports:
        tags.add("api")
    if "rag" in lower_path or "retrieval" in lower_path or "search" in lower_symbol:
        tags.add("retrieval")
    if any(item in lower_imports for item in {"ast", "tree_sitter", "tree-sitter"}):
        tags.add("code-analysis")
    return sorted(tags)


def build_chunks_for_file(path: Path, *, root_path: Path, repo: str) -> list[CodeChunk]:
    text = path.read_text(encoding="utf-8", errors="ignore")
    relative_path = path.relative_to(root_path).as_posix()
    file_hash = sha256_text(text)
    language = detect_language(path)
    imports = extract_imports(text, language)
    chunks: list[CodeChunk] = []
    for raw in raw_chunks(text, path):
        content_hash = sha256_text(raw["code"])
        chunk_id = sha256_text(f"{relative_path}:{raw['start_line']}:{raw['end_line']}:{content_hash}")[:24]
        tags = infer_tags(
            relative_path=relative_path,
            language=raw["language"],
            kind=raw["kind"],
            symbol=raw["symbol"],
            imports=imports,
        )
        metadata = {
            "id": chunk_id,
            "repo": repo,
            "path": relative_path,
            "absolute_path": str(path),
            "language": raw["language"],
            "symbol": raw["symbol"],
            "kind": raw["kind"],
            "start_line": raw["start_line"],
            "end_line": raw["end_line"],
            "imports": imports,
            "tags": tags,
            "content_hash": content_hash,
            "file_hash": file_hash,
        }
        chunks.append(CodeChunk(code=raw["code"], metadata=metadata, **metadata))
    return chunks


def build_chunks(root_path: Path, *, repo: str, max_files_scanned: int = 20_000) -> list[CodeChunk]:
    return [
        chunk
        for path in iter_source_files(root_path, max_files_scanned=max_files_scanned)
        for chunk in build_chunks_for_file(path, root_path=root_path, repo=repo)
    ]


def chunk_to_document(chunk: CodeChunk) -> dict[str, Any]:
    return asdict(chunk)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build code chunks and print JSONL")
    parser.add_argument("--root-path", required=True)
    parser.add_argument("--repo", default="deerflow")
    parser.add_argument("--max-files-scanned", type=int, default=20_000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root_path = Path(args.root_path).expanduser().resolve()
    for chunk in build_chunks(root_path, repo=args.repo, max_files_scanned=args.max_files_scanned):
        print(json.dumps(chunk_to_document(chunk), ensure_ascii=False))


if __name__ == "__main__":
    main()

