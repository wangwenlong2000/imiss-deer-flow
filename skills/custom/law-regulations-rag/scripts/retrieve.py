import argparse
import json
from pathlib import Path

from dotenv import load_dotenv
from lib.bm25_indexer import BM25Indexer, ChineseTokenizer
from lib.model_config import require_model_config
from lib.vector_indexer import EmbeddingModel, VectorIndexer

load_dotenv()


def parse_args():
    parser = argparse.ArgumentParser(description="Dual-path retrieve from BM25 and Vector indexes.")
    parser.add_argument("--query", required=True)
    parser.add_argument("--index-dir", required=True)
    parser.add_argument("--top-k", type=int, default=5)

    parser.add_argument("--user-dict-path", default="")
    parser.add_argument("--model-config", required=True)
    parser.add_argument("--embedding-config-name", default="law-embedding")
    parser.add_argument("--index-type", choices=["HNSW", "IVF", "FLAT"], default="HNSW")
    return parser.parse_args()


def ensure_exists(path: Path, desc: str):
    if not path.exists():
        raise FileNotFoundError(f"{desc} 不存在: {path}")


def normalize_scores(results):
    if not results:
        return []
    max_score = max(r["score"] for r in results) or 1.0
    new_results = []
    for r in results:
        item = dict(r)
        item["_norm_score"] = item["score"] / max_score
        new_results.append(item)
    return new_results


def search_bm25(index_dir: Path, query: str, top_k: int, user_dict_path: str):
    index_path = index_dir / "law_bm25.pkl"
    ensure_exists(index_path, "BM25 索引文件")

    indexer = BM25Indexer(
        tokenizer=ChineseTokenizer(
            user_dict_path=user_dict_path or None,
            enable_bigrams=True,
        )
    ).load(index_path)

    return indexer.search(query, top_k=top_k)


def search_vector(index_dir: Path, query: str, top_k: int, model_config: str, embedding_config_name: str, index_type: str):
    index_path = index_dir / "law_vector.index"
    metadata_path = index_dir / "law_vector_metadata.pkl"

    ensure_exists(index_path, "向量索引文件")
    ensure_exists(metadata_path, "向量索引元数据文件")

    emb_cfg = require_model_config(
        model_config,
        embedding_config_name,
        expected_type="embedding",
    )

    embedding_model = EmbeddingModel(
        model_name=emb_cfg["model"],
        api_key=emb_cfg["api_key"],
        base_url=emb_cfg["base_url"],
        query_prefix=emb_cfg.get("query_prefix", ""),
        dimensions=emb_cfg.get("dimensions", 1024),
        max_batch_size=emb_cfg.get("max_batch_size", 10),
        retry_times=emb_cfg.get("retry_times", 3),
        retry_interval=emb_cfg.get("retry_interval", 1.5),
    )

    indexer = VectorIndexer(
        embedding_model=embedding_model,
        index_type=index_type,
    ).load(index_path, metadata_path)

    return indexer.search(query, top_k=top_k)


def hybrid_merge(bm25_results, vector_results, top_k: int, bm25_weight: float = 0.6, vector_weight: float = 0.4):
    bm25_results = normalize_scores(bm25_results)
    vector_results = normalize_scores(vector_results)

    merged = {}

    for r in bm25_results:
        doc_id = r["doc_id"]
        merged[doc_id] = {
            "doc_id": r["doc_id"],
            "title": r["title"],
            "text": r["text"],
            "bm25_score": r["score"],
            "vector_score": 0.0,
            "score": bm25_weight * r["_norm_score"],
        }

    for r in vector_results:
        doc_id = r["doc_id"]
        if doc_id not in merged:
            merged[doc_id] = {
                "doc_id": r["doc_id"],
                "title": r["title"],
                "text": r["text"],
                "bm25_score": 0.0,
                "vector_score": r["score"],
                "score": vector_weight * r["_norm_score"],
            }
        else:
            merged[doc_id]["vector_score"] = r["score"]
            merged[doc_id]["score"] += vector_weight * r["_norm_score"]

    results = list(merged.values())
    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:top_k]


def main():
    args = parse_args()
    index_dir = Path(args.index_dir)
    ensure_exists(index_dir, "索引目录")

    bm25_results = search_bm25(
        index_dir=index_dir,
        query=args.query,
        top_k=args.top_k,
        user_dict_path=args.user_dict_path,
    )

    vector_results = search_vector(
        index_dir=index_dir,
        query=args.query,
        top_k=args.top_k,
        model_config=args.model_config,
        embedding_config_name=args.embedding_config_name,
        index_type=args.index_type,
    )

    results = hybrid_merge(
        bm25_results=bm25_results,
        vector_results=vector_results,
        top_k=args.top_k,
    )

    print(json.dumps(
        {
            "status": "ok",
            "mode": "both",
            "query": args.query,
            "top_k": args.top_k,
            "results": results,
        },
        ensure_ascii=False,
        indent=2,
    ))


if __name__ == "__main__":
    main()