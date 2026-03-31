import argparse
import json
import re
from pathlib import Path
from typing import Dict, List, Any


def normalize_text(text: str) -> str:
    if text is None:
        return ""
    text = str(text).replace("\\n", "\n").replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def normalize_single_line(text: str) -> str:
    text = normalize_text(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def split_keywords(keyword_str: str) -> List[str]:
    if not keyword_str:
        return []
    parts = re.split(r"[；;、，,]", keyword_str)
    return [p.strip() for p in parts if p.strip()]


def parse_law_and_article(name: str):
    name = name.strip()
    m = re.match(r"^(.*?)(第[一二三四五六七八九十百千万零〇两0-9]+条.*)$", name)
    if m:
        return m.group(1), m.group(2)
    return name, ""


def load_corpus_jsonl(path: str) -> List[Dict[str, Any]]:
    docs = []
    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                docs.append(json.loads(line))
            except Exception as e:
                raise ValueError(f"corpus 第 {line_no} 行 JSON 解析失败: {e}")
    return docs


def load_queries_json(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_jsonl(path: Path, rows: List[Dict[str, Any]]):
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def save_tsv(path: Path, rows: List[List[Any]]):
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write("\t".join(map(str, row)) + "\n")


def preprocess(corpus_path: str, queries_path: str, out_dir: str):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    raw_corpus = load_corpus_jsonl(corpus_path)
    raw_queries = load_queries_json(queries_path)

    corpus_clean = []
    corpus_by_id = {}
    corpus_by_name = {}

    report = {
        "corpus_total": 0,
        "queries_total": 0,
        "duplicate_doc_ids": [],
        "missing_positive_ids": [],
        "title_mismatch": [],
        "evidence_text_mismatch": [],
        "duplicate_questions": []
    }

    seen_doc_ids = set()
    for doc in raw_corpus:
        doc_id = int(doc["id"])
        title = normalize_single_line(doc["name"])
        text = normalize_text(doc["content"])
        law_name, article = parse_law_and_article(title)

        if doc_id in seen_doc_ids:
            report["duplicate_doc_ids"].append(doc_id)
        seen_doc_ids.add(doc_id)

        clean_doc = {
            "doc_id": doc_id,
            "title": title,
            "law_name": law_name,
            "article": article,
            "text": text,
            "full_text": f"{title}\n{text}".strip()
        }
        corpus_clean.append(clean_doc)
        corpus_by_id[doc_id] = clean_doc
        corpus_by_name[title] = clean_doc

    report["corpus_total"] = len(corpus_clean)

    queries_clean = []
    rag_train = []
    qrels_rows = [["query_id", "doc_id", "score"]]

    question_seen = {}

    for item in raw_queries:
        query_id = int(item["query_id"])
        question = normalize_single_line(item.get("问题", ""))
        keywords = split_keywords(item.get("关键词", ""))
        answer = normalize_text(item.get("答案文本", ""))

        positive_ids = [int(x) for x in item.get("match_id", [])]
        positive_titles = [normalize_single_line(x) for x in item.get("match_name", [])]

        if question in question_seen:
            report["duplicate_questions"].append({
                "question": question,
                "query_ids": [question_seen[question], query_id]
            })
        else:
            question_seen[question] = query_id

        evidences = []
        related_regulations = item.get("相关法规", {})

        for idx, doc_id in enumerate(positive_ids):
            if doc_id not in corpus_by_id:
                report["missing_positive_ids"].append({
                    "query_id": query_id,
                    "missing_doc_id": doc_id
                })
                continue

            doc = corpus_by_id[doc_id]
            expected_title = positive_titles[idx] if idx < len(positive_titles) else ""

            if expected_title and expected_title != doc["title"]:
                report["title_mismatch"].append({
                    "query_id": query_id,
                    "doc_id": doc_id,
                    "match_name": expected_title,
                    "corpus_title": doc["title"]
                })

            rel_text = related_regulations.get(expected_title)
            if rel_text is not None:
                rel_text_norm = normalize_text(rel_text)
                corpus_text_norm = normalize_text(doc["text"])
                if rel_text_norm != corpus_text_norm:
                    report["evidence_text_mismatch"].append({
                        "query_id": query_id,
                        "doc_id": doc_id,
                        "title": expected_title
                    })

            evidences.append({
                "doc_id": doc_id,
                "title": doc["title"],
                "text": doc["text"]
            })
            qrels_rows.append([query_id, doc_id, 1])

        query_row = {
            "query_id": query_id,
            "question": question,
            "keywords": keywords,
            "answer": answer,
            "positive_ids": positive_ids,
            "positive_titles": positive_titles,
            "evidences": evidences
        }
        queries_clean.append(query_row)

        rag_train.append({
            "query_id": query_id,
            "question": question,
            "keywords": keywords,
            "context_ids": positive_ids,
            "contexts": evidences,
            "answer": answer
        })

    report["queries_total"] = len(queries_clean)

    save_jsonl(out_dir / "corpus_clean.jsonl", corpus_clean)
    save_jsonl(out_dir / "queries_clean.jsonl", queries_clean)
    save_jsonl(out_dir / "rag_train.jsonl", rag_train)
    save_tsv(out_dir / "qrels.tsv", qrels_rows)

    with open(out_dir / "preprocess_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    result = {
        "status": "ok",
        "corpus_total": len(corpus_clean),
        "queries_total": len(queries_clean),
        "output_dir": str(out_dir),
        "files": {
            "corpus_clean": str(out_dir / "corpus_clean.jsonl"),
            "queries_clean": str(out_dir / "queries_clean.jsonl"),
            "rag_train": str(out_dir / "rag_train.jsonl"),
            "qrels": str(out_dir / "qrels.tsv"),
            "report": str(out_dir / "preprocess_report.json"),
        }
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


def parse_args():
    parser = argparse.ArgumentParser(description="Preprocess law-regulations dataset")
    parser.add_argument("--corpus-path", required=True, help="Path to raw corpus.jsonl")
    parser.add_argument("--queries-path", required=True, help="Path to raw queries.json")
    parser.add_argument("--out-dir", required=True, help="Output directory for processed files")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    preprocess(
        corpus_path=args.corpus_path,
        queries_path=args.queries_path,
        out_dir=args.out_dir,
    )