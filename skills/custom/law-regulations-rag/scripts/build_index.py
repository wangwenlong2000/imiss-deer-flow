import argparse
import json
from pathlib import Path

from lib.model_config import require_model_config
from dotenv import load_dotenv
load_dotenv()


def write_json(path, obj):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_args():
    parser = argparse.ArgumentParser(description="Build BM25 / Vector index for law-regulations dataset.")
    parser.add_argument("--corpus-path", required=True, help="Path to corpus json/jsonl")
    parser.add_argument("--queries-path", default="", help="Optional path to queries for offline evaluation")
    parser.add_argument("--user-dict-path", default="datasets/law-regulations/lexicon/thuocl_law_user_dict.txt", help="Optional pkuseg user dict path")
    parser.add_argument("--output-dir", required=True, help="Directory to save indexes")
    parser.add_argument("--mode", choices=["bm25", "vector", "both"], default="both")
    parser.add_argument("--force-rebuild", action="store_true")
    parser.add_argument("--run-eval", action="store_true")

    parser.add_argument("--title-boost", type=int, default=2)

    parser.add_argument("--index-type", choices=["HNSW", "IVF", "FLAT"], default="HNSW")
    parser.add_argument("--batch-size", type=int, default=32)

    parser.add_argument("--model-config", required=True, help="Path to models.yaml")
    parser.add_argument("--embedding-config-name", default="law-embedding", help="Embedding model config name")
    return parser.parse_args()


def main():
    args = parse_args()

    corpus_path = Path(args.corpus_path)
    queries_path = Path(args.queries_path) if args.queries_path else None
    user_dict_path = Path(args.user_dict_path) if args.user_dict_path else None
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    result = {
        "status": "ok",
        "corpus_path": str(corpus_path),
        "queries_path": str(queries_path) if queries_path else "",
        "output_dir": str(output_dir),
        "mode": args.mode,
        "built": {},
        "evaluated": {},
    }

    if args.mode in {"bm25", "both"}:
        from lib.bm25_indexer import (
            BM25Indexer,
            ChineseTokenizer,
            build_lecoqa_bm25_index,
            evaluate_recall as evaluate_bm25_recall,
        )

        bm25_index_path = output_dir / "law_bm25.pkl"
        bm25_eval_path = output_dir / "law_bm25_eval.json"

        if (not args.force_rebuild) and bm25_index_path.exists():
            bm25_indexer = BM25Indexer(
                tokenizer=ChineseTokenizer(
                    user_dict_path=str(user_dict_path) if user_dict_path else None,
                    enable_bigrams=True,
                )
            ).load(bm25_index_path)
            result["built"]["bm25"] = {
                "index_path": str(bm25_index_path),
                "reused": True,
            }
        else:
            bm25_indexer = build_lecoqa_bm25_index(
                corpus_path=corpus_path,
                output_index_path=bm25_index_path,
                user_dict_path=user_dict_path,
                title_boost=args.title_boost,
            )
            result["built"]["bm25"] = {
                "index_path": str(bm25_index_path),
                "reused": False,
            }

        if args.run_eval and queries_path and queries_path.exists():
            bm25_metrics = evaluate_bm25_recall(
                indexer=bm25_indexer,
                queries_path=queries_path,
                top_ks=(1, 5, 10),
                max_samples=None,
            )
            write_json(bm25_eval_path, bm25_metrics)
            result["evaluated"]["bm25"] = {
                "eval_path": str(bm25_eval_path),
                "metrics": bm25_metrics,
            }

    if args.mode in {"vector", "both"}:
        from lib.vector_indexer import (
            EmbeddingModel,
            VectorIndexer,
            build_lecoqa_vector_index,
            evaluate_recall as evaluate_vector_recall,
        )

        vector_index_path = output_dir / "law_vector.index"
        vector_metadata_path = output_dir / "law_vector_metadata.pkl"
        vector_eval_path = output_dir / "law_vector_eval.json"

        emb_cfg = require_model_config(
            args.model_config,
            args.embedding_config_name,
            expected_type="embedding",
        )

        model_name = emb_cfg["model"]
        api_key = emb_cfg["api_key"]
        base_url = emb_cfg["base_url"]
        query_prefix = emb_cfg.get("query_prefix", "")
        dimensions = emb_cfg.get("dimensions", 1024)
        max_batch_size = emb_cfg.get("max_batch_size", 10)
        retry_times = emb_cfg.get("retry_times", 3)
        retry_interval = emb_cfg.get("retry_interval", 1.5)

        if not api_key:
            raise ValueError("embedding 模型配置中的 api_key 为空，请检查环境变量或 models.yaml")

        if (not args.force_rebuild) and vector_index_path.exists() and vector_metadata_path.exists():
            embedding_model = EmbeddingModel(
                model_name=model_name,
                api_key=api_key,
                base_url=base_url,
                query_prefix=query_prefix,
                dimensions=dimensions,
                max_batch_size=max_batch_size,
                retry_times=retry_times,
                retry_interval=retry_interval,
            )
            vector_indexer = VectorIndexer(
                embedding_model=embedding_model,
                index_type=args.index_type,
            ).load(vector_index_path, vector_metadata_path)

            result["built"]["vector"] = {
                "index_path": str(vector_index_path),
                "metadata_path": str(vector_metadata_path),
                "reused": True,
                "model_name": model_name,
                "base_url": base_url,
            }
        else:
            vector_indexer = build_lecoqa_vector_index(
                corpus_path=corpus_path,
                index_path=vector_index_path,
                metadata_path=vector_metadata_path,
                model_name=model_name,
                api_key=api_key,
                base_url=base_url,
                index_type=args.index_type,
                batch_size=args.batch_size,
                query_prefix=query_prefix,
                dimensions=dimensions,
                max_batch_size=max_batch_size,
                retry_times=retry_times,
                retry_interval=retry_interval,
            )
            result["built"]["vector"] = {
                "index_path": str(vector_index_path),
                "metadata_path": str(vector_metadata_path),
                "reused": False,
                "model_name": model_name,
                "base_url": base_url,
            }

        if args.run_eval and queries_path and queries_path.exists():
            vector_metrics = evaluate_vector_recall(
                indexer=vector_indexer,
                queries_path=queries_path,
                top_ks=(1, 5, 10),
                max_samples=None,
            )
            write_json(vector_eval_path, vector_metrics)
            result["evaluated"]["vector"] = {
                "eval_path": str(vector_eval_path),
                "metrics": vector_metrics,
            }

    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()