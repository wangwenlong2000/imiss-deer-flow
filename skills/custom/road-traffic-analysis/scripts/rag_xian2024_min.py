from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
from pathlib import Path
from typing import Any, Iterable


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_MODEL = "BAAI/bge-small-zh-v1.5"
DEFAULT_DEVICE = "cpu"
DEFAULT_COLLECTION = "xian_traffic_2024_min"
DEFAULT_DB_DIR = str(SCRIPT_DIR / "rag_db" / "chroma")
DEFAULT_CHUNKS_FILE = str(SCRIPT_DIR / "rag_db" / "xian2024_chunks.jsonl")
DEFAULT_SUMMARY_FILE = str(SCRIPT_DIR / "rag_db" / "xian2024_index_summary.json")

RAG_DEPENDENCY_HINT = (
    "Missing optional RAG dependencies. Install them in the Python environment "
    "used by DeerFlow, for example: pip install chromadb==1.5.8 "
    "sentence-transformers==5.4.1 PyMuPDF==1.27.2.3"
)

NOISE_SUBSTRINGS = (
    "西安市城市规划设计研究院",
    "西安市交通运输局",
    "城市规划设计研究院",
    "规划设计研究院",
    "设计研究院",
    "研究院",
)


def require_rag_dependencies(needs_pdf: bool = False) -> tuple[Any, Any | None, Any]:
    missing: list[str] = []

    try:
        import chromadb
    except ModuleNotFoundError:
        chromadb = None
        missing.append("chromadb")

    try:
        from sentence_transformers import SentenceTransformer
    except ModuleNotFoundError:
        SentenceTransformer = None
        missing.append("sentence-transformers")

    fitz = None
    if needs_pdf:
        try:
            import fitz
        except ModuleNotFoundError:
            missing.append("PyMuPDF")

    if missing:
        raise RuntimeError(f"{RAG_DEPENDENCY_HINT}. Missing: {', '.join(missing)}")

    return chromadb, fitz, SentenceTransformer


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


def reset_collection(client: chromadb.ClientAPI, name: str):
    try:
        client.delete_collection(name)
    except Exception:
        pass
    return client.create_collection(name=name, metadata={"hnsw:space": "cosine"})


def add_to_chroma(
    chunks: list[dict],
    db_dir: Path,
    collection_name: str,
    model_name: str,
    device: str,
    batch_size: int,
) -> None:
    chromadb, _, SentenceTransformer = require_rag_dependencies()
    if db_dir.exists():
        shutil.rmtree(db_dir)
    db_dir.mkdir(parents=True, exist_ok=True)
    client = chromadb.PersistentClient(path=str(db_dir))
    collection = reset_collection(client, collection_name)
    model = SentenceTransformer(model_name, device=device)

    for start in range(0, len(chunks), batch_size):
        batch = chunks[start : start + batch_size]
        documents = [item["text"] for item in batch]
        embeddings = model.encode(
            documents,
            batch_size=min(batch_size, 32),
            normalize_embeddings=True,
            show_progress_bar=False,
        ).tolist()

        collection.add(
            ids=[item["id"] for item in batch],
            documents=documents,
            metadatas=[item["metadata"] for item in batch],
            embeddings=embeddings,
        )


def cmd_build(args: argparse.Namespace) -> None:
    _, fitz, _ = require_rag_dependencies(needs_pdf=True)
    pdf_path = find_pdf(args.pdf)
    doc = fitz.open(str(pdf_path))
    chunks, headings, missing_headings = build_chunks(
        doc=doc,
        pdf_path=pdf_path,
        max_chars=args.max_chars,
        overlap_chars=args.overlap_chars,
        min_chars=args.min_chars,
    )

    chunks_file = Path(args.chunks_file)
    summary_file = Path(args.summary_file)
    write_chunks_jsonl(chunks, chunks_file)
    add_to_chroma(
        chunks=chunks,
        db_dir=Path(args.db_dir),
        collection_name=args.collection,
        model_name=args.model,
        device=args.device,
        batch_size=args.batch_size,
    )

    summary = {
        "pdf": str(pdf_path.resolve()),
        "pages": len(doc),
        "toc_headings": len(doc.get_toc(simple=True)),
        "matched_headings": len(headings),
        "missing_headings": len(missing_headings),
        "chunks": len(chunks),
        "model": args.model,
        "db_dir": str(Path(args.db_dir).resolve()),
        "collection": args.collection,
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
    chromadb, _, SentenceTransformer = require_rag_dependencies()
    client = chromadb.PersistentClient(path=args.db_dir)
    collection = client.get_collection(args.collection)
    model = SentenceTransformer(args.model, device=args.device)
    query_embedding = model.encode(
        [args.question],
        normalize_embeddings=True,
        show_progress_bar=False,
    ).tolist()

    candidate_count = min(collection.count(), max(args.top_k * 10, 50))
    result = collection.query(
        query_embeddings=query_embedding,
        n_results=candidate_count,
        include=["documents", "metadatas", "distances"],
    )

    rows = []
    for idx, doc_text in enumerate(result["documents"][0]):
        metadata = result["metadatas"][0][idx]
        distance = result["distances"][0][idx]
        question_norm = normalize_text(args.question)
        title_norm = normalize_text(metadata["section_title"])
        path_norm = normalize_text(metadata["section_path"])
        title_hit = bool(question_norm and (question_norm in title_norm or question_norm in path_norm))
        rerank_score = float(distance) - (0.25 if title_hit else 0)
        rows.append(
            {
                "rank": idx + 1,
                "rerank_score": round(rerank_score, 4),
                "distance": round(float(distance), 4),
                "title_hit": title_hit,
                "section_path": metadata["section_path"],
                "pages": f"{metadata['page_start']}-{metadata['page_end']}",
                "preview": re.sub(r"\s+", " ", doc_text)[:260],
            }
        )

    rows.sort(key=lambda item: item["rerank_score"])
    for rank, row in enumerate(rows[: args.top_k], start=1):
        row["rank"] = rank
    rows = rows[: args.top_k]
    print(json.dumps(rows, ensure_ascii=False, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Minimal local vector DB for xian2024 PDF.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    build = subparsers.add_parser("build", help="Chunk the PDF and build a Chroma vector DB.")
    build.add_argument("--pdf", default=None, help="PDF path. Defaults to the first PDF in cwd.")
    build.add_argument("--db-dir", default=DEFAULT_DB_DIR)
    build.add_argument("--chunks-file", default=DEFAULT_CHUNKS_FILE)
    build.add_argument("--summary-file", default=DEFAULT_SUMMARY_FILE)
    build.add_argument("--collection", default=DEFAULT_COLLECTION)
    build.add_argument("--model", default=DEFAULT_MODEL)
    build.add_argument("--device", default=DEFAULT_DEVICE, help="Embedding device, default: cpu.")
    build.add_argument("--max-chars", type=int, default=1000)
    build.add_argument("--overlap-chars", type=int, default=120)
    build.add_argument("--min-chars", type=int, default=80)
    build.add_argument("--batch-size", type=int, default=32)
    build.set_defaults(func=cmd_build)

    query = subparsers.add_parser("query", help="Query the local Chroma vector DB.")
    query.add_argument("question")
    query.add_argument("--db-dir", default=DEFAULT_DB_DIR)
    query.add_argument("--collection", default=DEFAULT_COLLECTION)
    query.add_argument("--model", default=DEFAULT_MODEL)
    query.add_argument("--device", default=DEFAULT_DEVICE, help="Embedding device, default: cpu.")
    query.add_argument("--top-k", type=int, default=5)
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
