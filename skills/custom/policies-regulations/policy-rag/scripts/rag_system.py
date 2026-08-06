#!/usr/bin/env python3
"""
RAG 文档智能系统 - 统一入口
基于 DashScope / Qwen Embedding API 的文档处理与检索系统

功能：
1. 文档智能分割（Word/Markdown）
2. Excel 审批流程转换与检索
3. 制度文件语义检索

Usage:
    python rag_system.py <command> [options]

Commands:
    split <input_file> [output_dir]     - 智能分割文档
    convert <excel_file> [output_dir]   - 转换 Excel 审批表
    search <query> [flows_dir]          - 检索审批流程
    index <docs_dir> [index_dir]        - 建立制度文件索引
    search-docs <query> [index_dir]     - 检索制度文件
"""

import json
import hashlib
import os
import re
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np


SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_ROOT = SCRIPT_DIR.parent


def load_env_value(key: str, default: str = "") -> str:
    """优先从环境变量读取；没有则从 scripts/.env 和 skill 根目录 .env 读取。"""
    value = os.environ.get(key, "").strip()
    if value:
        return value

    candidate_files = [SCRIPT_DIR / ".env", SKILL_ROOT / ".env"]
    for env_path in candidate_files:
        if not env_path.exists():
            continue
        try:
            for line in env_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                if k.strip() == key:
                    return v.strip().strip('"').strip("'")
        except Exception:
            continue
    return default


def require_api_key() -> str:
    api_key = load_env_value("DASHSCOPE_API_KEY", "")
    if api_key:
        return api_key
    print(
        "错误：未找到 DASHSCOPE_API_KEY。请在 scripts/.env 或技能根目录 .env 中配置，或设置环境变量。",
        file=sys.stderr,
    )
    sys.exit(1)


# 路径约定：datasets 只读，workspace 可写，outputs 用于持久化交付物
DATA_ROOT = Path(load_env_value("RAG_DATA_DIR", "/mnt/datasets/policies-regulations/policy-rag"))
WORKSPACE_ROOT = Path(load_env_value("RAG_WORKSPACE_DIR", "/mnt/user-data/workspace/policy-rag"))
OUTPUT_ROOT = Path(load_env_value("RAG_OUTPUT_DIR", "/mnt/user-data/outputs/policy-rag"))

RAW_DIR = Path(load_env_value("RAG_RAW_DIR", str(DATA_ROOT / "raw")))
READONLY_FLOWS_DIR = Path(load_env_value("RAG_FLOWS_DIR", str(DATA_ROOT / "flows")))
READONLY_INDEX_DIR = Path(load_env_value("RAG_INDEX_DIR", str(DATA_ROOT / "index")))

PROCESSED_DIR = Path(load_env_value("RAG_PROCESSED_DIR", str(WORKSPACE_ROOT / "processed")))
RUNTIME_FLOWS_DIR = Path(load_env_value("RAG_RUNTIME_FLOWS_DIR", str(WORKSPACE_ROOT / "flows")))
RUNTIME_INDEX_DIR = Path(load_env_value("RAG_RUNTIME_INDEX_DIR", str(WORKSPACE_ROOT / "index")))

EMBED_CACHE_DIR = Path(load_env_value("RAG_EMBED_CACHE_DIR", str(WORKSPACE_ROOT / "cache" / "embed_cache")))
INDEX_CACHE_DIR = Path(load_env_value("RAG_INDEX_CACHE_DIR", str(WORKSPACE_ROOT / "cache" / "index")))


# DashScope 配置
DASHSCOPE_BASE_HTTP_API_URL = load_env_value("DASHSCOPE_BASE_HTTP_API_URL", "").strip()
EMBEDDING_MODEL = load_env_value("DASHSCOPE_EMBEDDING_MODEL", "text-embedding-v3")
EMBEDDING_DIM = int(load_env_value("DASHSCOPE_EMBEDDING_DIM", "1024"))
DASHSCOPE_MAX_BATCH_SIZE = int(load_env_value("DASHSCOPE_MAX_BATCH_SIZE", "10"))
CHAT_MODEL = load_env_value("DASHSCOPE_CHAT_MODEL", "qwen-plus")


def ensure_runtime_dirs() -> None:
    for p in [
        WORKSPACE_ROOT,
        OUTPUT_ROOT,
        PROCESSED_DIR,
        RUNTIME_FLOWS_DIR,
        RUNTIME_INDEX_DIR,
        EMBED_CACHE_DIR,
        INDEX_CACHE_DIR,
    ]:
        p.mkdir(parents=True, exist_ok=True)


ensure_runtime_dirs()


def log(msg: str, level: str = "INFO") -> None:
    prefix = {"INFO": "ℹ️", "SUCCESS": "✅", "ERROR": "❌", "WARN": "⚠️", "PROGRESS": "🔄"}
    print(f"{prefix.get(level, '•')} {msg}")


class DashScopeEmbeddingClient:
    """DashScope / Qwen Embedding API 客户端。"""

    def __init__(self, base_url: str = DASHSCOPE_BASE_HTTP_API_URL, embedding_model: str = EMBEDDING_MODEL):
        import dashscope

        self.base_url = (base_url or "").strip()
        self.embedding_model = embedding_model
        self.api_key = load_env_value("DASHSCOPE_API_KEY", "").strip()
        self.dimension = int(load_env_value("DASHSCOPE_EMBEDDING_DIM", str(EMBEDDING_DIM)))
        self.max_batch_size = max(1, int(load_env_value("DASHSCOPE_MAX_BATCH_SIZE", str(DASHSCOPE_MAX_BATCH_SIZE))))
        self.dashscope = dashscope
        if self.base_url:
            self.dashscope.base_http_api_url = self.base_url
        self._embedding_cache: Dict[str, np.ndarray] = {}
        self._embedding_dim = self.dimension
        self.cache_dir = EMBED_CACHE_DIR
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _hash_text(self, text: str) -> str:
        return hashlib.md5(text.encode("utf-8")).hexdigest()

    def _normalize_text(self, text: str) -> str:
        return (text or "").strip()[:4000]

    def _ensure_api_key(self) -> None:
        if not self.api_key:
            raise RuntimeError("未检测到 DASHSCOPE_API_KEY，请先配置百炼 API Key")

    def _extract_embedding_vectors(self, resp) -> List[List[float]]:
        output = getattr(resp, "output", None)
        if output is None and isinstance(resp, dict):
            output = resp.get("output") or resp
        if output is None:
            return []

        if hasattr(output, "get"):
            embeddings = output.get("embeddings") or output.get("data") or []
            single_embedding = output.get("embedding")
        else:
            embeddings = getattr(output, "embeddings", None) or getattr(output, "data", None) or []
            single_embedding = getattr(output, "embedding", None)

        if isinstance(single_embedding, list) and single_embedding:
            return [single_embedding]

        vectors: List[List[float]] = []
        if isinstance(embeddings, list):
            sort_key = lambda item: item.get("text_index", item.get("index", 0)) if isinstance(item, dict) else 0
            for item in sorted(embeddings, key=sort_key):
                if isinstance(item, dict):
                    dense = item.get("embedding")
                    if dense is None and isinstance(item.get("dense_embedding"), list):
                        dense = item.get("dense_embedding")
                    if isinstance(dense, list):
                        vectors.append(dense)
                elif isinstance(item, list):
                    vectors.append(item)
        return vectors

    def _request_embed(self, input_data, timeout: int = 30):
        from http import HTTPStatus

        self._ensure_api_key()
        kwargs = {
            "api_key": self.api_key,
            "model": self.embedding_model,
            "input": input_data,
        }
        if self.dimension > 0:
            kwargs["dimension"] = self.dimension

        resp = self.dashscope.TextEmbedding.call(**kwargs)
        status_code = getattr(resp, "status_code", None)
        if status_code in (HTTPStatus.OK, 200):
            return resp

        code = getattr(resp, "code", None)
        message = getattr(resp, "message", None)
        request_id = getattr(resp, "request_id", None)
        raise RuntimeError(
            f"DashScope embedding 调用失败: status_code={status_code}, code={code}, message={message}, request_id={request_id}"
        )

    def _request_with_retry(self, input_data, timeout: int = 30, retries: int = 3):
        last_error: Optional[Exception] = None
        for attempt in range(1, retries + 1):
            try:
                return self._request_embed(input_data=input_data, timeout=timeout)
            except Exception as e:
                last_error = e
                if attempt < retries:
                    sleep_s = min(0.5 * attempt, 2.0)
                    log(f"Embedding 请求失败，{sleep_s:.1f}s 后重试 ({attempt}/{retries})", "WARN")
                    time.sleep(sleep_s)
        raise RuntimeError(f"DashScope embedding 最终失败: {last_error}")

    def _cache_file(self, cache_key: str) -> Path:
        return self.cache_dir / f"{cache_key}.npy"

    def _load_disk_cache(self, cache_key: str) -> Optional[np.ndarray]:
        cache_file = self._cache_file(cache_key)
        if not cache_file.exists():
            return None
        try:
            arr = np.load(cache_file)
            if arr.ndim != 1:
                return None
            self._embedding_dim = int(arr.shape[0])
            return arr.astype(np.float32)
        except Exception:
            return None

    def _save_disk_cache(self, cache_key: str, embedding: np.ndarray) -> None:
        cache_file = self._cache_file(cache_key)
        try:
            np.save(cache_file, embedding.astype(np.float32))
        except Exception as e:
            log(f"写入 embedding 磁盘缓存失败: {e}", "WARN")

    def healthcheck(self) -> bool:
        try:
            arr = self.get_embedding("测试", strict=True)
            return bool(arr.size and float(np.linalg.norm(arr)) > 0.0)
        except Exception as e:
            log(f"DashScope 连通性检查失败: {e}", "ERROR")
            return False

    def get_embedding(self, text: str, timeout: int = 30, strict: bool = False) -> np.ndarray:
        normalized = self._normalize_text(text)
        cache_key = self._hash_text(normalized)

        if cache_key in self._embedding_cache:
            return self._embedding_cache[cache_key]
        disk_cached = self._load_disk_cache(cache_key)
        if disk_cached is not None:
            self._embedding_cache[cache_key] = disk_cached
            return disk_cached

        try:
            resp = self._request_with_retry(input_data=normalized, timeout=timeout)
            vectors = self._extract_embedding_vectors(resp)
            embedding = vectors[0] if vectors else []

            if embedding:
                arr = np.array(embedding, dtype=np.float32)
                self._embedding_dim = arr.shape[0]
            else:
                if strict:
                    raise ValueError("DashScope 返回空 embedding")
                arr = np.zeros(self._embedding_dim, dtype=np.float32)

            self._embedding_cache[cache_key] = arr
            self._save_disk_cache(cache_key, arr)
            return arr
        except Exception as e:
            log(f"Embedding 失败: {e}", "ERROR")
            if strict:
                raise
            return np.zeros(self._embedding_dim, dtype=np.float32)

    def get_embeddings_batch(self, texts: List[str], batch_size: int = 10, strict: bool = False) -> List[np.ndarray]:
        if not texts:
            return []

        normalized_texts = [self._normalize_text(t) for t in texts]
        results: List[Optional[np.ndarray]] = [None] * len(texts)
        missing: List[Tuple[int, str, str]] = []

        for idx, normalized in enumerate(normalized_texts):
            cache_key = self._hash_text(normalized)
            cached = self._embedding_cache.get(cache_key)
            if cached is not None:
                results[idx] = cached
                continue
            disk_cached = self._load_disk_cache(cache_key)
            if disk_cached is not None:
                self._embedding_cache[cache_key] = disk_cached
                results[idx] = disk_cached
                continue
            missing.append((idx, normalized, cache_key))

        if not missing:
            return [r if r is not None else np.zeros(self._embedding_dim, dtype=np.float32) for r in results]

        effective_batch_size = max(1, min(batch_size, self.max_batch_size, 10))

        for start in range(0, len(missing), effective_batch_size):
            batch = missing[start:start + effective_batch_size]
            batch_texts = [b[1] for b in batch]
            try:
                resp = self._request_with_retry(input_data=batch_texts, timeout=60)
                emb_list = self._extract_embedding_vectors(resp)
                if len(emb_list) != len(batch):
                    raise ValueError(f"批量返回数量异常: got={len(emb_list)}, expected={len(batch)}")

                for (idx, _text, cache_key), emb in zip(batch, emb_list):
                    if emb:
                        arr = np.array(emb, dtype=np.float32)
                        self._embedding_dim = arr.shape[0]
                    else:
                        if strict:
                            raise ValueError("DashScope 批量接口返回空 embedding")
                        arr = np.zeros(self._embedding_dim, dtype=np.float32)
                    self._embedding_cache[cache_key] = arr
                    self._save_disk_cache(cache_key, arr)
                    results[idx] = arr
            except Exception as e:
                log(f"批量 Embedding 失败，回退单条请求: {e}", "WARN")
                for idx, text, cache_key in batch:
                    arr = self.get_embedding(text, strict=strict)
                    self._embedding_cache[cache_key] = arr
                    self._save_disk_cache(cache_key, arr)
                    results[idx] = arr

            done = min(start + effective_batch_size, len(missing))
            if done % effective_batch_size == 0 or done == len(missing):
                log(f"Embedding 进度: {done}/{len(missing)}")

        return [r if r is not None else np.zeros(self._embedding_dim, dtype=np.float32) for r in results]


@dataclass
class TextChunk:
    content: str
    start_line: int
    end_line: int
    chunk_type: str
    embedding: Optional[np.ndarray] = None
    similarity_to_prev: float = 0.0


class SmartDocumentSplitter:
    def __init__(self, embedder: DashScopeEmbeddingClient, require_embedding: bool = True):
        self.embedder = embedder
        self.chapter_min_chars = 40
        self.require_embedding = require_embedding

    def parse_document(self, content: str) -> List[TextChunk]:
        lines = content.split("\n")
        chunks: List[TextChunk] = []
        chapter_starts: List[int] = []

        for i, line in enumerate(lines):
            if self._is_chapter_title(line):
                chapter_starts.append(i)

        if not chapter_starts:
            body = content.strip()
            if not body:
                return []
            fallback = body if body.startswith("# ") else f"# 全文\n\n{body}"
            return [
                TextChunk(
                    content=fallback,
                    start_line=0,
                    end_line=max(len(lines) - 1, 0),
                    chunk_type="chapter",
                )
            ]

        first_chapter_line = chapter_starts[0]
        preface = "\n".join(lines[:first_chapter_line]).strip()
        if preface:
            preface_content = preface if preface.startswith("# ") else f"# 前言\n\n{preface}"
            chunks.append(
                TextChunk(
                    content=preface_content,
                    start_line=0,
                    end_line=first_chapter_line - 1,
                    chunk_type="chapter",
                )
            )

        for idx, start in enumerate(chapter_starts):
            end = chapter_starts[idx + 1] - 1 if idx + 1 < len(chapter_starts) else len(lines) - 1
            chapter_content = "\n".join(lines[start:end + 1]).strip()
            if not chapter_content:
                continue
            chunks.append(TextChunk(content=chapter_content, start_line=start, end_line=end, chunk_type="chapter"))
        return chunks

    def _is_chapter_title(self, line: str) -> bool:
        stripped = line.strip()
        if not stripped:
            return False
        if re.match(r"^#\s+[^#]+$", stripped):
            return True
        if re.match(r"^#{1,3}\s*第[一二三四五六七八九十百千万\d]+章", stripped):
            return True
        if re.match(r"^第[一二三四五六七八九十百千万\d]+章", stripped):
            return True
        if re.match(r"^(Chapter|CHAPTER)\s+\d+\b", stripped):
            return True
        return stripped in {"前言", "总则", "附则", "附件"}

    def _extract_title(self, content: str) -> str:
        lines = [line.strip() for line in content.strip().split("\n") if line.strip()]
        if not lines:
            return "未命名章节"
        first_line = lines[0]
        markdown_title = re.match(r"^#{1,3}\s+(.+)$", first_line)
        if markdown_title:
            return markdown_title.group(1).strip()
        chapter_title = re.match(r"^(第[一二三四五六七八九十百千万\d]+章.*)$", first_line)
        if chapter_title:
            return chapter_title.group(1).strip()
        return first_line[:30] + "..." if len(first_line) > 30 else first_line

    def _extract_tags(self, content: str, base_tags: List[str]) -> List[str]:
        tags = set(base_tags)
        keyword_map = {
            "审批": "审批权限",
            "预算编制": "预算编制",
            "执行": "预算执行",
            "调整": "预算调整",
            "调剂": "预算调剂",
            "追加": "预算追加",
            "资本": "资本性支出",
            "费用": "费用预算",
            "原则": "原则",
            "流程": "流程",
            "Q1": "问答",
            "怎么办": "操作指南",
            "采购": "采购管理",
            "供应商": "供应商管理",
            "招标": "招标管理",
        }
        for keyword, tag in keyword_map.items():
            if keyword in content:
                tags.add(tag)
        return list(tags)[:8]

    def generate_yaml_header(self, chunk: TextChunk, doc_info: Dict) -> str:
        title = self._extract_title(chunk.content) or f"章节_{chunk.start_line}"
        tags = self._extract_tags(chunk.content, doc_info.get("tags", []))
        high_priority = any(kw in chunk.content for kw in ["审批", "调整", "权限", "流程", "怎么办"])
        yaml_lines = [
            "---",
            f'title: "{doc_info.get("department", "")}-{title}"',
            f'category: "{doc_info.get("category", "")}"',
            f'tags: {json.dumps(tags, ensure_ascii=False)}',
            f'department: "{doc_info.get("department", "")}"',
            f'chunk_type: "{chunk.chunk_type}"',
        ]
        if "article" in doc_info:
            yaml_lines.append(f'article: "{doc_info["article"]}"')
        if "chapter_no" in doc_info:
            yaml_lines.append(f'chapter_no: {doc_info["chapter_no"]}')
        if "chapter_title" in doc_info:
            yaml_lines.append(f'chapter_title: "{doc_info["chapter_title"]}"')
        if "chapter_path" in doc_info:
            yaml_lines.append(f'chapter_path: "{doc_info["chapter_path"]}"')
        if "breadcrumb" in doc_info:
            yaml_lines.append(f'breadcrumb: "{doc_info["breadcrumb"]}"')
        if high_priority:
            yaml_lines.append("high_priority: true")
        if "effective_date" in doc_info:
            yaml_lines.append(f'effective_date: "{doc_info["effective_date"]}"')
        yaml_lines.append("---\n")
        return "\n".join(yaml_lines)

    def split_document(self, input_path: str, output_dir: str, doc_info: Dict = None) -> List[Path]:
        import shutil

        log(f"正在读取文档: {input_path}")
        with open(input_path, "r", encoding="utf-8") as f:
            content = f.read()

        log("正在解析文档结构...")
        final_chunks = self.parse_document(content)
        log(f"按章节拆分: {len(final_chunks)} 个章节")
        if not final_chunks:
            raise RuntimeError("未解析出可拆分章节")

        embedding_texts = [f"{self._extract_title(chunk.content)}\n{chunk.content[:3000]}" for chunk in final_chunks]
        if self.require_embedding:
            log("正在调用 DashScope 文本向量服务生成章节 Embedding...", "PROGRESS")
            embeddings = self.embedder.get_embeddings_batch(embedding_texts, batch_size=8, strict=True)
            for chunk, emb in zip(final_chunks, embeddings):
                chunk.embedding = emb
            invalid = [i for i, emb in enumerate(embeddings, 1) if float(np.linalg.norm(emb)) == 0.0]
            if invalid:
                raise RuntimeError(f"章节 embedding 失败（零向量）: {invalid}")
            log(f"章节 Embedding 完成: {len(embeddings)} 个")

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        chapters_dir = output_path / "chapters"
        if chapters_dir.exists():
            shutil.rmtree(chapters_dir)
        chapters_dir.mkdir(parents=True, exist_ok=True)

        doc_info = doc_info or {}
        if "title" not in doc_info:
            doc_info["title"] = Path(input_path).stem

        generated_files: List[Path] = []
        chapter_entries: List[Dict] = []
        for i, chunk in enumerate(final_chunks, 1):
            title = self._extract_title(chunk.content)
            safe_title = re.sub(r'[\\/*?:"<>|]', "_", title)[:60]
            filename = f"{i:02d}_{safe_title}.md"
            filepath = chapters_dir / filename
            relpath = filepath.relative_to(output_path)
            chapter_info = {
                **doc_info,
                "chapter_no": i,
                "chapter_title": title,
                "chapter_path": str(relpath),
                "breadcrumb": f"{doc_info.get('title', Path(input_path).stem)} > {title}",
            }
            yaml_header = self.generate_yaml_header(chunk, chapter_info)
            full_content = yaml_header + chunk.content
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(full_content)
            generated_files.append(filepath)
            chapter_entries.append(
                {
                    "chapter_no": i,
                    "chapter_title": title,
                    "filename": filename,
                    "filepath": str(relpath),
                    "start_line": chunk.start_line,
                    "end_line": chunk.end_line,
                    "char_count": len(chunk.content),
                    "embedding_dim": int(chunk.embedding.shape[0]) if chunk.embedding is not None else 0,
                    "embedding_norm": float(np.linalg.norm(chunk.embedding)) if chunk.embedding is not None else 0.0,
                    "embedding_cache_key": hashlib.md5(embedding_texts[i - 1].strip()[:4000].encode("utf-8")).hexdigest(),
                }
            )
            log(f"已生成章节: {relpath}")

        self._generate_index(output_path, chapter_entries, doc_info)
        log(f"章节拆分完成！共生成 {len(generated_files)} 个章节文件", "SUCCESS")
        return generated_files

    def _generate_index(self, output_path: Path, chapters: List[Dict], doc_info: Dict) -> None:
        import shutil

        title = doc_info.get("title", "制度文档")
        index_content = f"""---
title: \"{doc_info.get('title', '制度总览')}\"
category: \"{doc_info.get('category', '')}\"
tags: {json.dumps(doc_info.get('tags', []), ensure_ascii=False)}
department: \"{doc_info.get('department', '')}\"
type: \"index\"
split_mode: \"chapter\"
---

# {title} - 章节索引

## 文档信息

| 项目 | 内容 |
|------|------|
| **制度名称** | {title} |
| **编制部门** | {doc_info.get('department', '-')} |
| **生效日期** | {doc_info.get('effective_date', '-')} |
| **拆分粒度** | 章节（仅到章） |
| **章节数量** | {len(chapters)} |

## 章节目录

"""
        for chapter in chapters:
            index_content += (
                f"- {chapter['chapter_no']:02d}. "
                f"[{chapter['chapter_title']}](./{chapter['filepath']}) "
                f"（行 {chapter['start_line'] + 1}-{chapter['end_line'] + 1}）\n"
            )
        index_content += "\n"

        index_path = output_path / "00_章节总览.md"
        with open(index_path, "w", encoding="utf-8") as f:
            f.write(index_content)

        index_root = output_path / "index"
        if index_root.exists():
            shutil.rmtree(index_root)
        chapter_index_dir = index_root / "chapters"
        chapter_index_dir.mkdir(parents=True, exist_ok=True)

        chapter_nodes = []
        for chapter in chapters:
            node = {
                "id": f"chapter-{chapter['chapter_no']:02d}",
                "title": chapter["chapter_title"],
                "level": 1,
                "filepath": chapter["filepath"],
                "start_line": chapter["start_line"],
                "end_line": chapter["end_line"],
                "char_count": chapter["char_count"],
                "embedding_dim": chapter.get("embedding_dim", 0),
                "embedding_norm": chapter.get("embedding_norm", 0.0),
                "embedding_cache_key": chapter.get("embedding_cache_key", ""),
                "breadcrumb": [title, chapter["chapter_title"]],
            }
            chapter_nodes.append(node)
            safe_title = re.sub(r'[\\/*?:"<>|]', "_", chapter["chapter_title"])[:60]
            node_file = chapter_index_dir / f"{chapter['chapter_no']:02d}_{safe_title}.json"
            with open(node_file, "w", encoding="utf-8") as f:
                json.dump(node, f, ensure_ascii=False, indent=2)

        tree = {
            "document_title": title,
            "split_mode": "chapter",
            "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "chapter_count": len(chapter_nodes),
            "chapters": chapter_nodes,
        }
        tree_path = index_root / "chapter_tree.json"
        with open(tree_path, "w", encoding="utf-8") as f:
            json.dump(tree, f, ensure_ascii=False, indent=2)

        index_readme = [
            f"# {title} - 索引目录",
            "",
            "- `chapter_tree.json`: 章节目录树总索引",
            "- `chapters/*.json`: 每章单独索引节点",
            "",
            "## 章节列表",
        ]
        for chapter in chapters:
            index_readme.append(f"- {chapter['chapter_no']:02d}. {chapter['chapter_title']}")
        with open(index_root / "README.md", "w", encoding="utf-8") as f:
            f.write("\n".join(index_readme) + "\n")

        log("已生成: 00_章节总览.md")
        log("已生成: index/chapter_tree.json")
        log("已生成: index/chapters/*.json")


class ApprovalFlowConverter:
    def __init__(self):
        self.headers = {}

    def parse_excel(self, excel_path: str) -> List[Dict]:
        import pandas as pd

        df = pd.read_excel(excel_path, header=None)
        data_start = 0
        for i in range(len(df)):
            row_vals = df.iloc[i].astype(str).tolist()
            if any("供应链中心" in str(v) for v in row_vals if pd.notna(v)):
                data_start = i
                break

        flows = []
        current_dept = ""
        current_l1 = ""
        current_l2 = ""

        for i in range(data_start, len(df)):
            row = df.iloc[i]
            dept = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
            l1 = str(row.iloc[1]) if pd.notna(row.iloc[1]) else ""
            l2 = str(row.iloc[2]) if pd.notna(row.iloc[2]) else ""
            l3 = str(row.iloc[3]) if pd.notna(row.iloc[3]) else ""
            l4 = str(row.iloc[4]) if pd.notna(row.iloc[4]) else ""
            scope = str(row.iloc[5]) if pd.notna(row.iloc[5]) else ""
            remark = str(row.iloc[6]) if len(row) > 6 and pd.notna(row.iloc[6]) else ""

            if not any([dept, l1, l2, l3, l4]) or "一级流程" in l1:
                continue
            if dept and dept not in ["nan", "部门"]:
                current_dept = dept
            if l1 and l1 not in ["nan", "一级流程"]:
                current_l1 = l1
            if l2 and l2 not in ["nan", "二级流程"]:
                current_l2 = l2

            flow_name_parts = []
            if current_l1 and current_l1 not in ["nan", "一级流程"]:
                flow_name_parts.append(current_l1)
            if current_l2 and current_l2 not in ["nan", "二级流程"]:
                flow_name_parts.append(current_l2)
            if l3 and l3 not in ["nan", "三级流程"]:
                flow_name_parts.append(l3)

            flow_name = " / ".join(flow_name_parts)
            item_detail = l4 if l4 and l4 not in ["nan", "四级流程"] else ""
            approval_nodes = self._extract_approval_nodes(row, df)
            approval_path = self._build_approval_path(approval_nodes)
            final_approver = self._find_final_approver(approval_nodes)

            flow = {
                "流程大类": current_l1,
                "流程子类": current_l2,
                "流程名称": flow_name,
                "具体事项": item_detail,
                "适用范围": scope if scope not in ["nan", "适用单位"] else "",
                "部门": current_dept if current_dept not in ["nan", "部门"] else "",
                "备注": remark if remark not in ["nan", "备注"] else "",
                "审批节点": approval_nodes,
                "审批路径": approval_path,
                "最终审批人": final_approver,
            }
            if approval_nodes:
                flows.append(flow)
        return flows

    def _extract_approval_nodes(self, row, df) -> List[Dict]:
        import pandas as pd
        from collections import OrderedDict

        nodes = []
        level_groups = OrderedDict()
        for col_idx in range(7, min(54, len(row))):
            val = row.iloc[col_idx]
            if not pd.notna(val):
                continue
            val_str = str(val).strip()
            if not val_str or val_str in ["✔", "nan"]:
                continue
            col_name = self._get_col_name(col_idx, df)
            levels = re.findall(r"[①②③④]", val_str)
            is_approval = "审批" in val_str
            for level in levels:
                if level not in level_groups:
                    level_groups[level] = {"roles": [], "actions": [], "is_approval": False}
                level_groups[level]["roles"].append(col_name)
                level_groups[level]["actions"].append(val_str)
                if is_approval:
                    level_groups[level]["is_approval"] = True

        for level, info in level_groups.items():
            node_type = "审批" if info["is_approval"] else "审核"
            nodes.append(
                {
                    "level": level,
                    "type": node_type,
                    "roles": list(set(info["roles"])),
                    "actions": list(set(info["actions"])),
                    "description": f"{level}{node_type}",
                }
            )
        return nodes

    def _get_col_name(self, col_idx: int, df) -> str:
        import pandas as pd

        parts = []
        for row_idx in range(1, 5):
            if row_idx < len(df):
                val = df.iloc[row_idx, col_idx]
                if pd.notna(val):
                    val_str = str(val).strip()
                    if val_str and val_str not in parts:
                        parts.append(val_str)
        return " / ".join(parts) if parts else f"节点{col_idx}"

    def _build_approval_path(self, nodes: List[Dict]) -> str:
        if not nodes:
            return ""
        return " → ".join(f"{node['level']}{node['type']}" for node in nodes)

    def _find_final_approver(self, nodes: List[Dict]) -> str:
        for node in reversed(nodes):
            if node["type"] == "审批":
                return " / ".join(node["roles"])
        if nodes:
            return " / ".join(nodes[-1]["roles"])
        return ""

    def to_embedding_text(self, flow: Dict) -> str:
        parts = [
            f"流程：{flow.get('流程名称', '')}",
            f"事项：{flow.get('具体事项', '')}",
            f"适用范围：{flow.get('适用范围', '')}",
            f"部门：{flow.get('部门', '')}",
        ]
        for node in flow.get("审批节点", []):
            roles = "、".join(node.get("roles", []))
            parts.append(f"{node['level']}{node['type']}：{roles}")
        if flow.get("备注") and flow["备注"] not in ["✔", "nan", ""]:
            parts.append(f"备注：{flow['备注']}")
        return "\n".join(parts)

    def convert(self, excel_path: str, output_dir: str) -> List[Dict]:
        log(f"正在解析: {excel_path}")
        flows = self.parse_excel(excel_path)
        if not flows:
            log("未找到流程数据", "WARN")
            return []

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        json_path = output_path / f"{Path(excel_path).stem}_v2.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(flows, f, ensure_ascii=False, indent=2)

        txt_path = output_path / f"{Path(excel_path).stem}_v2.txt"
        with open(txt_path, "w", encoding="utf-8") as f:
            for flow in flows:
                f.write(self.to_embedding_text(flow))
                f.write("\n\n---\n\n")

        for flow in flows:
            flow["_embedding_text"] = self.to_embedding_text(flow)

        log(f"已转换 {len(flows)} 个流程", "SUCCESS")
        log(f"JSON: {json_path}")
        log(f"TXT: {txt_path}")
        return flows


class ApprovalFlowSearcher:
    def __init__(self, embedder: DashScopeEmbeddingClient):
        self.embedder = embedder
        self.flows: List[Dict] = []
        self.flow_embeddings: List[np.ndarray] = []
        self.level3_groups: Dict[str, List[Dict]] = defaultdict(list)
        self.flow_keyword_sets: List[set] = []
        self.flow_search_texts: List[str] = []

    def load_flows(self, flows_dir: str) -> None:
        flows_dir = Path(flows_dir)
        json_files = list(flows_dir.glob("*_flows.json")) + list(flows_dir.glob("*_v2.json"))
        log(f"找到 {len(json_files)} 个流程文件")
        for file in json_files:
            with open(file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    for flow in data:
                        embedding_text = flow.get("_embedding_text", "")
                        if not embedding_text:
                            converter = ApprovalFlowConverter()
                            embedding_text = converter.to_embedding_text(flow)
                        flow["_embedding_text"] = embedding_text
                        self.flows.append(flow)
                        emb = self.embedder.get_embedding(embedding_text)
                        self.flow_embeddings.append(emb)
                        level3_key = self._get_level3_key(flow)
                        if level3_key:
                            self.level3_groups[level3_key].append(flow)
        log(f"已加载 {len(self.flows)} 个流程")
        log(f"发现 {len(self.level3_groups)} 个三级流程分组")

    def _get_level3_key(self, flow: Dict) -> str:
        name = flow.get("流程名称", "")
        parts = name.split(" / ")
        return parts[2] if len(parts) >= 3 else name

    def _extract_level3(self, flow: Dict) -> str:
        return self._get_level3_key(flow)

    def _extract_level4(self, flow: Dict) -> str:
        item = flow.get("具体事项", "")
        if item and item not in ["nan", ""]:
            return item
        name = flow.get("流程名称", "")
        parts = name.split(" / ")
        if len(parts) >= 4:
            return parts[3]
        return "默认流程"

    def _build_search_text(self, flow: Dict) -> str:
        parts = [
            flow.get("流程名称", ""),
            flow.get("具体事项", ""),
            flow.get("适用范围", ""),
            flow.get("部门", ""),
            flow.get("备注", ""),
            flow.get("审批路径", ""),
        ]
        for node in flow.get("审批节点", []):
            parts.extend(node.get("roles", []))
            parts.extend(node.get("actions", []))
        return " ".join(p for p in parts if p and p != "nan")

    def _extract_tokens(self, text: str) -> set:
        if not text:
            return set()
        tokens = re.findall(r"[\u4e00-\u9fff]{2,}|[A-Za-z0-9]+(?:\.\d+)?", text)
        return {t.lower() for t in tokens if len(t.strip()) >= 2}

    def _keyword_overlap_score(self, query_tokens: set, flow_tokens: set) -> float:
        if not query_tokens or not flow_tokens:
            return 0.0
        overlap = len(query_tokens & flow_tokens)
        if overlap == 0:
            return 0.0
        return overlap / np.sqrt(len(query_tokens) * len(flow_tokens))

    def _numeric_match_score(self, query: str, flow_text: str) -> float:
        numbers = re.findall(r"\d+(?:\.\d+)?\s*[万亿千百%]?", query)
        if not numbers:
            return 0.0
        normalized = flow_text.replace(" ", "")
        hits = sum(1 for n in numbers if n.replace(" ", "") in normalized)
        return hits / len(numbers)

    def _prepare_search_cache(self) -> None:
        if len(self.flow_search_texts) == len(self.flows) and len(self.flow_keyword_sets) == len(self.flows):
            return
        self.flow_search_texts = []
        self.flow_keyword_sets = []
        for flow in self.flows:
            search_text = self._build_search_text(flow)
            self.flow_search_texts.append(search_text)
            self.flow_keyword_sets.append(self._extract_tokens(search_text))

    def search(self, query: str, top_k: int = 5) -> List[Tuple[float, Dict]]:
        self._prepare_search_cache()
        query_emb = self.embedder.get_embedding(query)
        query_tokens = self._extract_tokens(query)
        results = []
        for i, flow in enumerate(self.flows):
            semantic = self._cosine_similarity(query_emb, self.flow_embeddings[i])
            lexical = self._keyword_overlap_score(query_tokens, self.flow_keyword_sets[i])
            numeric = self._numeric_match_score(query, self.flow_search_texts[i])
            score = 0.72 * semantic + 0.22 * lexical + 0.06 * numeric
            flow["_score_detail"] = {
                "semantic": round(float(semantic), 4),
                "lexical": round(float(lexical), 4),
                "numeric": round(float(numeric), 4),
            }
            results.append((score, flow))
        results.sort(key=lambda x: x[0], reverse=True)
        return results[:top_k]

    def _cosine_similarity(self, a: np.ndarray, b: np.ndarray) -> float:
        norm_a, norm_b = np.linalg.norm(a), np.linalg.norm(b)
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return float(np.dot(a, b) / (norm_a * norm_b))

    def find_related_branches(self, flow: Dict, query: str = "") -> List[Dict]:
        level3_key = self._get_level3_key(flow)
        if level3_key in self.level3_groups:
            branches = self.level3_groups[level3_key]
            if query:
                query_tokens = self._extract_tokens(query)
                return sorted(
                    branches,
                    key=lambda x: (
                        self._keyword_overlap_score(query_tokens, self._extract_tokens(self._build_search_text(x)))
                        + 0.2 * self._numeric_match_score(query, self._build_search_text(x))
                    ),
                    reverse=True,
                )
            return sorted(branches, key=lambda x: x.get("具体事项", ""))
        return [flow]

    def format_single_flow(self, flow: Dict, index: int = 1, show_header: bool = True) -> str:
        lines: List[str] = []
        if show_header:
            lines.append(f"{index}. **{self._extract_level4(flow)}**")
        if flow.get("适用范围"):
            lines.append(f"   适用范围：{flow['适用范围']}")
        nodes = flow.get("审批节点", [])
        if nodes:
            lines.append("\n   📊 审批流程：")
            for i, node in enumerate(nodes):
                level = node.get("level", "")
                node_type = node.get("type", "")
                roles = "、".join(node.get("roles", [])[:2])
                indent = "   "
                if i == 0:
                    lines.append(f"{indent}┌─────────────┐")
                    lines.append(f"{indent}│ {roles:11} │ ◀── {level}{node_type}")
                    lines.append(f"{indent}└──────┬──────┘")
                elif i == len(nodes) - 1:
                    lines.append(f"{indent}       │")
                    lines.append(f"{indent}       ▼")
                    lines.append(f"{indent}┌─────────────┐")
                    lines.append(f"{indent}│ {roles:11} │ ◀── {level}{node_type}（最终）")
                    lines.append(f"{indent}└─────────────┘")
                else:
                    lines.append(f"{indent}       │")
                    lines.append(f"{indent}       ▼")
                    lines.append(f"{indent}┌─────────────┐")
                    lines.append(f"{indent}│ {roles:11} │ ◀── {level}{node_type}")
                    lines.append(f"{indent}└──────┬──────┘")
        if flow.get("最终审批人"):
            lines.append(f"\n   ✅ 最终审批人：{flow['最终审批人']}")
        if flow.get("备注") and flow["备注"] not in ["✔", "nan", ""]:
            lines.append(f"   📝 备注：{flow['备注']}")
        return "\n".join(lines)

    def answer(self, query: str) -> str:
        log(f"正在检索: {query}")
        results = self.search(query, top_k=3)
        if not results or results[0][0] < 0.45:
            return """❌ 未找到相关审批流程

💡 建议尝试：
• 预付款白名单供应商申请
• 采购100万以上怎么审批
• 固定资产采购流程
• 说明适用范围（总部/直管企业/事业部）"""

        best_sim, best_flow = results[0]
        level3_name = self._extract_level3(best_flow)
        branches = self.find_related_branches(best_flow, query=query)
        lines = [f"✅ 匹配度：{best_sim:.1%}", f"📁 **三级流程：{level3_name}**\n"]
        if len(branches) > 1:
            lines.append(f"⚠️ 该事项下有 **{len(branches)} 个分支流程**，请根据具体情况选择：\n")
            for i, branch in enumerate(branches, 1):
                lines.append(self.format_single_flow(branch, i, show_header=True))
                lines.append("")
            lines.append("-" * 50)
            lines.append("💡 提示：请根据您的具体情况选择对应的分支流程")
        else:
            lines.append(self.format_single_flow(best_flow, show_header=False))

        other_related = [r for r in results[1:] if r[0] > 0.5]
        if other_related:
            lines.append("\n📚 其他可能相关的流程：")
            for sim, flow in other_related:
                name = flow.get("流程名称", "未知")
                item = flow.get("具体事项", "")
                display = f"{name} - {item}" if item else name
                lines.append(f"• {display} ({sim:.0%})")
        return "\n".join(lines)


class DocumentIndex:
    def __init__(self, embedder: DashScopeEmbeddingClient):
        self.embedder = embedder
        self.documents: List[Dict] = []
        self.embeddings: List[np.ndarray] = []

    def index_documents(self, docs_dir: str, index_dir: str) -> None:
        import shutil

        docs_dir_p = Path(docs_dir)
        index_dir_p = Path(index_dir)
        index_dir_p.mkdir(parents=True, exist_ok=True)
        self.documents = []
        self.embeddings = []

        raw_md_files = list(docs_dir_p.rglob("*.md"))
        md_files = []
        skipped = 0
        for path in raw_md_files:
            rel = path.relative_to(docs_dir_p)
            rel_parts = rel.parts
            if rel_parts and rel_parts[0] == "index":
                skipped += 1
                continue
            if rel.name.startswith("00_") and "总览" in rel.name:
                skipped += 1
                continue
            md_files.append(path)
        log(f"找到 {len(md_files)} 个可索引 Markdown 文件（跳过 {skipped} 个索引/总览文件）")

        embedding_texts = []
        for file in md_files:
            with open(file, "r", encoding="utf-8") as f:
                content = f.read()
            doc_info = self._parse_frontmatter(content)
            doc_info["filepath"] = str(file.relative_to(docs_dir_p))
            doc_info["content"] = content
            self.documents.append(doc_info)
            embedding_texts.append(f"{doc_info.get('title', '')}\n{content[:1000]}")

        self.embeddings = self.embedder.get_embeddings_batch(embedding_texts, batch_size=10)
        index_data = {
            "documents": [{k: v for k, v in doc.items() if k != "embedding"} for doc in self.documents],
            "embeddings": [emb.tolist() for emb in self.embeddings],
        }
        index_path = index_dir_p / "document_index.json"
        with open(index_path, "w", encoding="utf-8") as f:
            json.dump(index_data, f, ensure_ascii=False, indent=2)

        structure_root = index_dir_p / "structure"
        if structure_root.exists():
            shutil.rmtree(structure_root)
        structure_root.mkdir(parents=True, exist_ok=True)
        for doc in self.documents:
            rel_path = Path(doc["filepath"])
            node_path = (structure_root / rel_path).with_suffix(".json")
            node_path.parent.mkdir(parents=True, exist_ok=True)
            node = {
                "title": doc.get("title", rel_path.stem),
                "filepath": doc["filepath"],
                "category": doc.get("category", ""),
                "tags": doc.get("tags", []),
            }
            with open(node_path, "w", encoding="utf-8") as f:
                json.dump(node, f, ensure_ascii=False, indent=2)

        structure_tree = self._build_directory_tree([Path(doc["filepath"]) for doc in self.documents])
        structure_tree_path = index_dir_p / "document_index_tree.json"
        with open(structure_tree_path, "w", encoding="utf-8") as f:
            json.dump(structure_tree, f, ensure_ascii=False, indent=2)

        log(f"索引完成，已保存到 {index_path}", "SUCCESS")
        log(f"目录结构索引: {structure_tree_path}")
        log(f"目录节点索引目录: {structure_root}")

    def _parse_frontmatter(self, content: str) -> Dict:
        import yaml

        if content.startswith("---"):
            parts = content.split("---", 2)
            if len(parts) >= 3:
                try:
                    frontmatter = yaml.safe_load(parts[1])
                    if frontmatter:
                        frontmatter["content"] = parts[2].strip()
                        return frontmatter
                except Exception:
                    pass
        return {"content": content}

    def _build_directory_tree(self, rel_paths: List[Path]) -> Dict:
        root = {"_type": "directory", "_children": {}}
        for rel_path in rel_paths:
            cursor = root
            parts = rel_path.parts
            for part in parts[:-1]:
                cursor = cursor["_children"].setdefault(part, {"_type": "directory", "_children": {}})
            cursor["_children"].setdefault(parts[-1], {"_type": "file", "path": str(rel_path)})

        def to_node(name: str, node: Dict) -> Dict:
            if node.get("_type") == "file":
                return {"name": name, "type": "file", "path": node["path"]}
            children = [
                to_node(child_name, child_node)
                for child_name, child_node in sorted(node["_children"].items(), key=lambda x: x[0])
            ]
            return {"name": name, "type": "directory", "children": children}

        return to_node(".", root)

    def load_index(self, index_dir: str) -> None:
        index_path = Path(index_dir) / "document_index.json"
        with open(index_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.documents = data["documents"]
        self.embeddings = [np.array(emb) for emb in data["embeddings"]]
        log(f"已加载 {len(self.documents)} 个文档")

    def search(self, query: str, top_k: int = 3) -> List[Tuple[float, Dict]]:
        query_emb = self.embedder.get_embedding(query)
        results = []
        for i, doc in enumerate(self.documents):
            sim = self._cosine_similarity(query_emb, self.embeddings[i])
            results.append((sim, doc))
        results.sort(key=lambda x: x[0], reverse=True)
        return results[:top_k]

    def _cosine_similarity(self, a: np.ndarray, b: np.ndarray) -> float:
        norm_a, norm_b = np.linalg.norm(a), np.linalg.norm(b)
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return float(np.dot(a, b) / (norm_a * norm_b))


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1]
    embedder: Optional[DashScopeEmbeddingClient] = None

    if command == "split":
        if len(sys.argv) < 3:
            log("用法: python rag_system.py split <input_file> [output_dir]", "ERROR")
            sys.exit(1)
        embedder = DashScopeEmbeddingClient()
        if not embedder.healthcheck():
            log("无法调用 DashScope 文本向量服务，请检查 DASHSCOPE_API_KEY / 模型名 / 网络", "ERROR")
            sys.exit(1)
        input_file = sys.argv[2]
        output_dir = sys.argv[3] if len(sys.argv) > 3 else str(PROCESSED_DIR)
        doc_info = {
            "title": Path(input_file).stem,
            "department": "供应链中心",
            "category": "供应链-采购管理",
            "tags": ["制度"],
            "effective_date": "2025-01-01",
        }
        splitter = SmartDocumentSplitter(embedder)
        splitter.split_document(input_file, output_dir, doc_info)

    elif command == "convert":
        if len(sys.argv) < 3:
            log("用法: python rag_system.py convert <excel_file> [output_dir]", "ERROR")
            sys.exit(1)
        excel_file = sys.argv[2]
        output_dir = sys.argv[3] if len(sys.argv) > 3 else str(RUNTIME_FLOWS_DIR)
        converter = ApprovalFlowConverter()
        converter.convert(excel_file, output_dir)

    elif command == "search":
        if len(sys.argv) < 3:
            log("用法: python rag_system.py search <query> [flows_dir]", "ERROR")
            sys.exit(1)
        embedder = DashScopeEmbeddingClient()
        if not embedder.healthcheck():
            log("无法调用 DashScope 文本向量服务，请检查 DASHSCOPE_API_KEY / 模型名 / 网络", "ERROR")
            sys.exit(1)
        query = sys.argv[2]
        flows_dir = sys.argv[3] if len(sys.argv) > 3 else str(READONLY_FLOWS_DIR)
        searcher = ApprovalFlowSearcher(embedder)
        searcher.load_flows(flows_dir)
        print("\n" + searcher.answer(query))

    elif command == "index":
        if len(sys.argv) < 3:
            log("用法: python rag_system.py index <docs_dir> [index_dir]", "ERROR")
            sys.exit(1)
        embedder = DashScopeEmbeddingClient()
        if not embedder.healthcheck():
            log("无法调用 DashScope 文本向量服务，请检查 DASHSCOPE_API_KEY / 模型名 / 网络", "ERROR")
            sys.exit(1)
        docs_dir = sys.argv[2]
        index_dir = sys.argv[3] if len(sys.argv) > 3 else str(RUNTIME_INDEX_DIR)
        index = DocumentIndex(embedder)
        index.index_documents(docs_dir, index_dir)

    elif command == "search-docs":
        if len(sys.argv) < 3:
            log("用法: python rag_system.py search-docs <query> [index_dir]", "ERROR")
            sys.exit(1)
        embedder = DashScopeEmbeddingClient()
        if not embedder.healthcheck():
            log("无法调用 DashScope 文本向量服务，请检查 DASHSCOPE_API_KEY / 模型名 / 网络", "ERROR")
            sys.exit(1)
        query = sys.argv[2]
        index_dir = sys.argv[3] if len(sys.argv) > 3 else str(RUNTIME_INDEX_DIR)
        index = DocumentIndex(embedder)
        index.load_index(index_dir)
        results = index.search(query, top_k=3)
        print(f"\n🔍 检索: {query}\n")
        for sim, doc in results:
            print(f"✅ 匹配度: {sim:.1%}")
            print(f"📄 {doc.get('title', '未知')}")
            print(f"   文件: {doc.get('filepath', '-')}")
            print()
    else:
        log(f"未知命令: {command}", "ERROR")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
