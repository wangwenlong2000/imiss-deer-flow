from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import re
from pathlib import Path
from typing import Any, Iterable


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_MODEL = "BAAI/bge-small-zh-v1.5"
DEFAULT_DEVICE = "cpu"
DEFAULT_COLLECTION = "xian_traffic_2024_min"
DEFAULT_INDEX = DEFAULT_COLLECTION
DEFAULT_DB_DIR = str(SCRIPT_DIR / "rag_db")
DEFAULT_CHUNKS_FILE = str(SCRIPT_DIR / "rag_db" / "xian2024_chunks.jsonl")
DEFAULT_SUMMARY_FILE = str(SCRIPT_DIR / "rag_db" / "xian2024_index_summary.json")
DEFAULT_ES_URL = "http://172.17.0.1:3128"
DEFAULT_VECTOR_FIELD = "embedding"
DEFAULT_RETRIEVAL_MODE = "auto"

RAG_DEPENDENCY_HINT = (
    "Missing optional RAG dependencies. Install them in the Python environment "
    "used by DeerFlow, for example: pip install -r "
    "/mnt/skills/custom/road-traffic-analysis/scripts/requirements-rag.txt"
)

NOISE_SUBSTRINGS = (
    "西安市城市规划设计研究院",
    "西安市交通运输局",
    "城市规划设计研究院",
    "规划设计研究院",
    "设计研究院",
    "研究院",
)


def require_sentence_transformer() -> Any:
    try:
        from sentence_transformers import SentenceTransformer
    except ModuleNotFoundError:
        raise RuntimeError(f"{RAG_DEPENDENCY_HINT}. Missing: sentence-transformers")
    return SentenceTransformer


def get_sentence_transformer() -> Any | None:
    if importlib.util.find_spec("sentence_transformers") is None:
        return None
    from sentence_transformers import SentenceTransformer

    return SentenceTransformer


def require_pdf_dependency() -> Any:
    try:
        import fitz
    except ModuleNotFoundError:
        raise RuntimeError(f"{RAG_DEPENDENCY_HINT}. Missing: PyMuPDF")
    return fitz


def require_requests() -> Any:
    try:
        import requests
    except ModuleNotFoundError:
        raise RuntimeError(f"{RAG_DEPENDENCY_HINT}. Missing: requests")
    return requests


def normalize_text(text: str) -> str:
    text = text or ""
    text = text.replace("\uf06e", "")
    text = text.replace("（", "(").replace("）", ")")
    text = re.sub(r"\s+", "", text)
    text = re.sub(r"[\u3000:：,，.。;；/\\\-_*|]+", "", text)
    return text


def clean_line(text: str) -> str:
    text = (text or "").replace("\uf06e", "")
    for noise in NOISE_SUBSTRINGS:
        text = text.replace(noise, "")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def should_skip_line(text: str) -> bool:
    compact = normalize_text(text)
    if not compact:
        return True
    if re.search(r"\.{6,}", text):
        return True
    if re.fullmatch(r"\d{1,3}", compact):
        return True
    if compact in {
        normalize_text("2024 年西安市城市交通发展年度报告"),
        "西",
        "西安",
        "研究院",
        "设计研究院",
        "西安市城市",
    }:
        return True
    return False


def find_pdf(path_arg: str | None) -> Path:
    if path_arg:
        pdf = Path(path_arg)
        if not pdf.exists():
            raise FileNotFoundError(f"PDF not found: {pdf}")
        return pdf

    pdfs = sorted(Path.cwd().glob("*.pdf"))
    if not pdfs:
        raise FileNotFoundError("No PDF found in current directory.")
    return pdfs[0]


def extract_lines(doc: fitz.Document) -> list[dict]:
    lines: list[dict] = []
    global_index = 0

    for page_no, page in enumerate(doc, start=1):
        raw_text = page.get_text("text", sort=True) or ""
        page_line_no = 0
        for raw_line in raw_text.splitlines():
            text = clean_line(raw_line)
            if should_skip_line(text):
                continue
            page_line_no += 1
            lines.append(
                {
                    "global_index": global_index,
                    "page": page_no,
                    "page_line_no": page_line_no,
                    "text": text,
                    "norm": normalize_text(text),
                }
            )
            global_index += 1

    return lines


def page_line_map(lines: Iterable[dict]) -> dict[int, list[dict]]:
    by_page: dict[int, list[dict]] = {}
    for line in lines:
        by_page.setdefault(line["page"], []).append(line)
    return by_page


def dedupe_toc(toc: list[list]) -> list[dict]:
    rows: list[dict] = []
    seen: set[tuple[int, str, int]] = set()

    for order, item in enumerate(toc):
        level, title, page = item
        title = clean_line(title)
        key = (level, normalize_text(title), page)
        if key in seen:
            continue
        seen.add(key)
        rows.append({"order": order, "level": level, "title": title, "page": page})

    return rows


def find_heading_line(page_lines: list[dict], title: str) -> dict | None:
    title_norm = normalize_text(title)
    if not title_norm:
        return None

    for line in page_lines:
        if line["norm"] == title_norm:
            return line

    candidates = []
    for line in page_lines:
        line_norm = line["norm"]
        if not line_norm:
            continue
        if title_norm in line_norm or line_norm in title_norm:
            overlap = min(len(title_norm), len(line_norm))
            if overlap >= 4:
                candidates.append((abs(len(title_norm) - len(line_norm)), line))

    if candidates:
        candidates.sort(key=lambda item: item[0])
        return candidates[0][1]

    return None


def enrich_toc(toc_rows: list[dict], lines: list[dict]) -> tuple[list[dict], list[dict]]:
    by_page = page_line_map(lines)
    stack: list[str] = []
    enriched: list[dict] = []
    missing: list[dict] = []

    for row in toc_rows:
        page_lines = by_page.get(row["page"], [])
        line = find_heading_line(page_lines, row["title"])
        if line is None:
            missing.append(row)
            continue

        level = row["level"]
        stack = stack[: level - 1]
        stack.append(row["title"])
        enriched.append(
            {
                **row,
                "line_index": line["global_index"],
                "page_line_no": line["page_line_no"],
                "section_path": " > ".join(stack),
            }
        )

    enriched.sort(key=lambda item: (item["line_index"], item["level"], item["order"]))

    compact: list[dict] = []
    seen_positions: set[tuple[int, str]] = set()
    for item in enriched:
        key = (item["line_index"], normalize_text(item["title"]))
        if key in seen_positions:
            continue
        seen_positions.add(key)
        compact.append(item)

    return compact, missing


def split_long_section(
    records: list[dict],
    max_chars: int,
    overlap_chars: int,
    min_chars: int,
) -> list[list[dict]]:
    chunks: list[list[dict]] = []
    current: list[dict] = []
    current_len = 0

    for record in records:
        line_len = len(record["text"])
        if current and current_len + line_len > max_chars:
            if current_len >= min_chars:
                chunks.append(current)

            overlap: list[dict] = []
            overlap_len = 0
            for old in reversed(current):
                overlap.insert(0, old)
                overlap_len += len(old["text"])
                if overlap_len >= overlap_chars:
                    break

            current = overlap[:]
            current_len = sum(len(item["text"]) for item in current)

        current.append(record)
        current_len += line_len

    if current and current_len >= min_chars:
        chunks.append(current)

    return chunks


def make_chunk_id(source: str, section_path: str, part: int, text: str) -> str:
    digest = hashlib.sha1(
        f"{source}|{section_path}|{part}|{text[:160]}".encode("utf-8")
    ).hexdigest()[:12]
    return f"xian2024_{digest}"


def build_chunks(
    doc: fitz.Document,
    pdf_path: Path,
    max_chars: int,
    overlap_chars: int,
    min_chars: int,
) -> tuple[list[dict], list[dict], list[dict]]:
    lines = extract_lines(doc)
    toc_rows = dedupe_toc(doc.get_toc(simple=True))
    headings, missing_headings = enrich_toc(toc_rows, lines)

    if not headings:
        raise RuntimeError("No TOC headings could be matched in PDF text.")

    chunks: list[dict] = []

    for idx, heading in enumerate(headings):
        start_line = heading["line_index"]
        end_line = headings[idx + 1]["line_index"] if idx + 1 < len(headings) else len(lines)
        section_records = [
            line for line in lines if start_line <= line["global_index"] < end_line
        ]

        text_len = sum(len(item["text"]) for item in section_records)
        if text_len < min_chars:
            continue

        parts = split_long_section(section_records, max_chars, overlap_chars, min_chars)
        for part_index, part_records in enumerate(parts, start=1):
            text = "\n".join(item["text"] for item in part_records).strip()
            page_start = min(item["page"] for item in part_records)
            page_end = max(item["page"] for item in part_records)
            chunks.append(
                {
                    "id": make_chunk_id(pdf_path.name, heading["section_path"], part_index, text),
                    "text": text,
                    "metadata": {
                        "source": pdf_path.name,
                        "section_path": heading["section_path"],
                        "section_title": heading["title"],
                        "section_level": int(heading["level"]),
                        "page_start": int(page_start),
                        "page_end": int(page_end),
                        "part_index": int(part_index),
                        "chars": int(len(text)),
                    },
                }
            )

    return chunks, headings, missing_headings


def write_chunks_jsonl(chunks: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for chunk in chunks:
            f.write(json.dumps(chunk, ensure_ascii=False) + "\n")


def read_chunks_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Chunks file not found: {path}")

    chunks: list[dict] = []
    with path.open(encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            raw = line.strip()
            if not raw:
                continue
            item = json.loads(raw)
            if "id" not in item or "text" not in item:
                raise ValueError(f"Invalid chunk at {path}:{line_no}; expected id and text.")
            item.setdefault("metadata", {})
            chunks.append(item)

    if not chunks:
        raise ValueError(f"No chunks found in {path}")
    return chunks


def resolve_index(args: argparse.Namespace) -> str:
    return args.index or args.collection or DEFAULT_INDEX


class ElasticsearchRestClient:
    def __init__(
        self,
        base_url: str,
        username: str | None = None,
        password: str | None = None,
        api_key: str | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.requests = require_requests()
        self.auth = (username, password) if username and password else None
        self.headers = {"Content-Type": "application/json"}
        if api_key:
            self.headers["Authorization"] = f"ApiKey {api_key}"

    def request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        ok_statuses: tuple[int, ...] = (200,),
    ) -> Any:
        response = self.requests.request(
            method,
            f"{self.base_url}/{path.lstrip('/')}",
            auth=self.auth,
            headers=self.headers,
            data=json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None,
            timeout=60,
        )
        if response.status_code not in ok_statuses:
            raise RuntimeError(
                f"Elasticsearch {method} {path} failed with HTTP {response.status_code}: "
                f"{response.text[:1000]}"
            )
        if not response.text.strip():
            return None
        return response.json()

    def index_exists(self, index: str) -> bool:
        response = self.requests.head(
            f"{self.base_url}/{index}",
            auth=self.auth,
            headers=self.headers,
            timeout=30,
        )
        if response.status_code == 200:
            return True
        if response.status_code == 404:
            return False
        raise RuntimeError(
            f"Elasticsearch HEAD /{index} failed with HTTP {response.status_code}: "
            f"{response.text[:1000]}"
        )

    def delete_index(self, index: str) -> None:
        self.request("DELETE", index, ok_statuses=(200, 404))

    def create_index(self, index: str, body: dict[str, Any]) -> None:
        self.request("PUT", index, body=body, ok_statuses=(200,))

    def bulk_index(self, index: str, rows: list[dict[str, Any]]) -> None:
        lines: list[str] = []
        for row in rows:
            lines.append(json.dumps({"index": {"_index": index, "_id": row["id"]}}, ensure_ascii=False))
            lines.append(json.dumps(row, ensure_ascii=False))
        payload = "\n".join(lines) + "\n"
        response = self.requests.post(
            f"{self.base_url}/_bulk",
            auth=self.auth,
            headers={"Content-Type": "application/x-ndjson", **self.headers},
            data=payload.encode("utf-8"),
            timeout=120,
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"Elasticsearch bulk index failed with HTTP {response.status_code}: "
                f"{response.text[:1000]}"
            )
        result = response.json()
        if result.get("errors"):
            errors = [item for item in result.get("items", []) if item.get("index", {}).get("error")]
            raise RuntimeError(f"Elasticsearch bulk index had errors: {errors[:3]}")

    def refresh(self, index: str) -> None:
        self.request("POST", f"{index}/_refresh", ok_statuses=(200,))

    def search(self, index: str, body: dict[str, Any]) -> dict[str, Any]:
        return self.request("POST", f"{index}/_search", body=body, ok_statuses=(200,))


def build_es_client(args: argparse.Namespace) -> ElasticsearchRestClient:
    return ElasticsearchRestClient(
        base_url=args.es_url or os.getenv("ES_URL", DEFAULT_ES_URL),
        username=args.es_username or os.getenv("ES_USERNAME"),
        password=args.es_password or os.getenv("ES_PASSWORD"),
        api_key=args.es_api_key or os.getenv("ES_API_KEY"),
    )


def infer_embedding_dims(model_name: str, device: str) -> int:
    SentenceTransformer = require_sentence_transformer()
    model = SentenceTransformer(model_name, device=device)
    vector = model.encode(
        ["dimension probe"],
        normalize_embeddings=True,
        show_progress_bar=False,
    )
    return len(vector[0])


def ensure_es_index(
    es: ElasticsearchRestClient,
    index: str,
    dims: int | None,
    vector_field: str,
    use_vector: bool,
    recreate: bool,
) -> None:
    if es.index_exists(index):
        if not recreate:
            return
        es.delete_index(index)

    body = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "analysis": {
                "analyzer": {
                    "xian_rag_text": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["lowercase"],
                    }
                }
            },
        },
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "content": {"type": "text", "analyzer": "xian_rag_text"},
                "model": {"type": "keyword"},
                "metadata": {
                    "properties": {
                        "source": {"type": "keyword"},
                        "section_path": {
                            "type": "text",
                            "fields": {"keyword": {"type": "keyword"}},
                        },
                        "section_title": {
                            "type": "text",
                            "fields": {"keyword": {"type": "keyword"}},
                        },
                        "section_level": {"type": "integer"},
                        "page_start": {"type": "integer"},
                        "page_end": {"type": "integer"},
                        "part_index": {"type": "integer"},
                        "chars": {"type": "integer"},
                    }
                },
            }
        },
    }
    if use_vector:
        if dims is None:
            raise ValueError("Vector index requires embedding dimensions.")
        body["mappings"]["properties"][vector_field] = {
            "type": "dense_vector",
            "dims": dims,
            "index": True,
            "similarity": "cosine",
        }
    es.create_index(index, body)


def add_to_elasticsearch(
    chunks: list[dict],
    es: ElasticsearchRestClient,
    index: str,
    model_name: str,
    device: str,
    batch_size: int,
    vector_field: str,
    retrieval_mode: str,
    recreate: bool,
) -> str:
    SentenceTransformer = get_sentence_transformer()
    use_vector = retrieval_mode == "vector" or (
        retrieval_mode == "auto" and SentenceTransformer is not None
    )
    if retrieval_mode == "vector" and SentenceTransformer is None:
        raise RuntimeError(f"{RAG_DEPENDENCY_HINT}. Missing: sentence-transformers")

    model = None
    dims: int | None = None
    if use_vector:
        model = SentenceTransformer(model_name, device=device)
        dims = int(model.get_sentence_embedding_dimension() or 0)
        if dims <= 0:
            dims = infer_embedding_dims(model_name, device)

    ensure_es_index(
        es,
        index,
        dims=dims,
        vector_field=vector_field,
        use_vector=use_vector,
        recreate=recreate,
    )

    for start in range(0, len(chunks), batch_size):
        batch = chunks[start : start + batch_size]
        embeddings: list[list[float] | None]
        if use_vector and model is not None:
            documents = [item["text"] for item in batch]
            embeddings = model.encode(
                documents,
                batch_size=min(batch_size, 32),
                normalize_embeddings=True,
                show_progress_bar=False,
            ).tolist()
        else:
            embeddings = [None] * len(batch)

        rows = []
        for item, embedding in zip(batch, embeddings):
            row = {
                "id": item["id"],
                "content": item["text"],
                "metadata": item.get("metadata", {}),
                "model": model_name,
            }
            if embedding is not None:
                row[vector_field] = embedding
            rows.append(row)
        es.bulk_index(index, rows)
    es.refresh(index)
    return "vector" if use_vector else "text"


def cmd_build(args: argparse.Namespace) -> None:
    headings: list[dict] = []
    missing_headings: list[dict] = []
    chunks_file = Path(args.chunks_file)
    summary_file = Path(args.summary_file)

    if args.pdf:
        fitz = require_pdf_dependency()
        pdf_path = find_pdf(args.pdf)
        doc = fitz.open(str(pdf_path))
        chunks, headings, missing_headings = build_chunks(
            doc=doc,
            pdf_path=pdf_path,
            max_chars=args.max_chars,
            overlap_chars=args.overlap_chars,
            min_chars=args.min_chars,
        )
        write_chunks_jsonl(chunks, chunks_file)
        pdf_summary: dict[str, Any] = {
            "pdf": str(pdf_path.resolve()),
            "pages": len(doc),
            "toc_headings": len(doc.get_toc(simple=True)),
            "matched_headings": len(headings),
            "missing_headings": len(missing_headings),
        }
    else:
        chunks = read_chunks_jsonl(chunks_file)
        pdf_summary = {
            "pdf": None,
            "source_chunks_file": str(chunks_file.resolve()),
            "note": "Imported existing chunks file. Pass --pdf to rebuild chunks from PDF.",
        }

    es = build_es_client(args)
    index = resolve_index(args)
    effective_mode = add_to_elasticsearch(
        chunks=chunks,
        es=es,
        index=index,
        model_name=args.model,
        device=args.device,
        batch_size=args.batch_size,
        vector_field=args.vector_field,
        retrieval_mode=args.retrieval_mode,
        recreate=not args.no_recreate,
    )

    summary = {
        **pdf_summary,
        "backend": "elasticsearch",
        "index": index,
        "es_url": args.es_url or os.getenv("ES_URL", DEFAULT_ES_URL),
        "retrieval_mode": effective_mode,
        "chunks": len(chunks),
        "model": args.model,
        "vector_field": args.vector_field,
        "chunks_file": str(chunks_file.resolve()),
        "sample_chunks": [
            {
                "id": chunk["id"],
                **chunk["metadata"],
                "preview": chunk["text"][:160],
            }
            for chunk in chunks[:5]
        ],
    }
    summary_file.parent.mkdir(parents=True, exist_ok=True)
    summary_file.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def cmd_query(args: argparse.Namespace) -> None:
    es = build_es_client(args)
    index = resolve_index(args)
    candidate_count = max(args.top_k * 10, 50)
    SentenceTransformer = get_sentence_transformer()
    use_vector = args.retrieval_mode == "vector" or (
        args.retrieval_mode == "auto" and SentenceTransformer is not None
    )
    if args.retrieval_mode == "vector" and SentenceTransformer is None:
        raise RuntimeError(f"{RAG_DEPENDENCY_HINT}. Missing: sentence-transformers")

    if use_vector:
        model = SentenceTransformer(args.model, device=args.device)
        query_embedding = model.encode(
            [args.question],
            normalize_embeddings=True,
            show_progress_bar=False,
        ).tolist()[0]
        body = {
            "size": candidate_count,
            "_source": ["id", "content", "metadata"],
            "knn": {
                "field": args.vector_field,
                "query_vector": query_embedding,
                "k": candidate_count,
                "num_candidates": max(candidate_count * 2, 100),
            },
        }
    else:
        body = {
            "size": candidate_count,
            "_source": ["id", "content", "metadata"],
            "query": {
                "multi_match": {
                    "query": args.question,
                    "fields": [
                        "content^3",
                        "metadata.section_title^2",
                        "metadata.section_path",
                    ],
                }
            },
        }
    result = es.search(index, body)

    rows = []
    for idx, hit in enumerate(result.get("hits", {}).get("hits", [])):
        source = hit.get("_source", {})
        doc_text = source.get("content", "")
        metadata = source.get("metadata", {})
        es_score = float(hit.get("_score") or 0.0)
        distance = (1.0 - es_score) if use_vector else (1.0 / (1.0 + es_score))
        question_norm = normalize_text(args.question)
        title_norm = normalize_text(metadata.get("section_title", ""))
        path_norm = normalize_text(metadata.get("section_path", ""))
        title_hit = bool(question_norm and (question_norm in title_norm or question_norm in path_norm))
        rerank_score = float(distance) - (0.25 if title_hit else 0)
        rows.append(
            {
                "rank": idx + 1,
                "rerank_score": round(rerank_score, 4),
                "distance": round(float(distance), 4),
                "es_score": round(es_score, 4),
                "retrieval_mode": "vector" if use_vector else "text",
                "title_hit": title_hit,
                "section_path": metadata.get("section_path"),
                "pages": f"{metadata.get('page_start')}-{metadata.get('page_end')}",
                "preview": re.sub(r"\s+", " ", doc_text)[:260],
            }
        )

    rows.sort(key=lambda item: item["rerank_score"])
    for rank, row in enumerate(rows[: args.top_k], start=1):
        row["rank"] = rank
    rows = rows[: args.top_k]
    print(json.dumps(rows, ensure_ascii=False, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Elasticsearch RAG index/query for xian2024 PDF chunks.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    build = subparsers.add_parser(
        "build",
        help="Import existing chunks into Elasticsearch, or pass --pdf to rebuild chunks first.",
    )
    build.add_argument("--pdf", default=None, help="Optional PDF path. If omitted, imports --chunks-file.")
    build.add_argument("--db-dir", default=DEFAULT_DB_DIR, help=argparse.SUPPRESS)
    build.add_argument("--chunks-file", default=DEFAULT_CHUNKS_FILE)
    build.add_argument("--summary-file", default=DEFAULT_SUMMARY_FILE)
    build.add_argument("--collection", default=None, help="Backward-compatible alias for --index.")
    build.add_argument("--index", default=DEFAULT_INDEX, help="Elasticsearch index name.")
    build.add_argument("--model", default=DEFAULT_MODEL)
    build.add_argument("--device", default=DEFAULT_DEVICE, help="Embedding device, default: cpu.")
    build.add_argument("--vector-field", default=DEFAULT_VECTOR_FIELD)
    build.add_argument(
        "--retrieval-mode",
        choices=("auto", "vector", "text"),
        default=DEFAULT_RETRIEVAL_MODE,
        help="auto uses vector retrieval when sentence-transformers is installed, otherwise text search.",
    )
    build.add_argument("--max-chars", type=int, default=1000)
    build.add_argument("--overlap-chars", type=int, default=120)
    build.add_argument("--min-chars", type=int, default=80)
    build.add_argument("--batch-size", type=int, default=32)
    build.add_argument(
        "--no-recreate",
        action="store_true",
        help="Do not delete and recreate the target Elasticsearch index before import.",
    )
    build.add_argument("--es-url", default=None, help="Elasticsearch URL, defaults to ES_URL env.")
    build.add_argument("--es-username", default=None, help="Elasticsearch basic auth username.")
    build.add_argument("--es-password", default=None, help="Elasticsearch basic auth password.")
    build.add_argument("--es-api-key", default=None, help="Elasticsearch API key.")
    build.set_defaults(func=cmd_build)

    query = subparsers.add_parser("query", help="Query the Elasticsearch RAG index.")
    query.add_argument("question")
    query.add_argument("--db-dir", default=DEFAULT_DB_DIR, help=argparse.SUPPRESS)
    query.add_argument("--collection", default=None, help="Backward-compatible alias for --index.")
    query.add_argument("--index", default=DEFAULT_INDEX, help="Elasticsearch index name.")
    query.add_argument("--model", default=DEFAULT_MODEL)
    query.add_argument("--device", default=DEFAULT_DEVICE, help="Embedding device, default: cpu.")
    query.add_argument("--vector-field", default=DEFAULT_VECTOR_FIELD)
    query.add_argument(
        "--retrieval-mode",
        choices=("auto", "vector", "text"),
        default=DEFAULT_RETRIEVAL_MODE,
        help="auto uses vector retrieval when sentence-transformers is installed, otherwise text search.",
    )
    query.add_argument("--top-k", type=int, default=5)
    query.add_argument("--es-url", default=None, help="Elasticsearch URL, defaults to ES_URL env.")
    query.add_argument("--es-username", default=None, help="Elasticsearch basic auth username.")
    query.add_argument("--es-password", default=None, help="Elasticsearch basic auth password.")
    query.add_argument("--es-api-key", default=None, help="Elasticsearch API key.")
    query.set_defaults(func=cmd_query)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    try:
        args.func(args)
    except RuntimeError as exc:
        parser.exit(2, f"error: {exc}\n")


if __name__ == "__main__":
    main()
