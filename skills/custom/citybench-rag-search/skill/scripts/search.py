#!/usr/bin/env python3
"""
CityBench RAG 混合检索 —— 三档自动降级版本。

运行模式（自动选择，不需要用户指定）:
  - es:    完整三通道 RAG（BM25 + DashScope kNN + RRF），需要 ES + DASHSCOPE_API_KEY
  - local: 本地 evidence.jsonl 文件 + 纯 Python BM25 + 关键词 RRF
  - demo:  内置 sample evidence (data/sample_evidence.jsonl)，零外部服务依赖（默认兜底）

无论环境如何，本脚本永远能返回结果，不会因为 ES/DashScope 不可达而完全失败。

用法:
    python3 scripts/search.py --query "北京早高峰签到热点" --output-dir /tmp/out
    python3 scripts/search.py --query "签到异常" --city Beijing --anomaly-only --output-dir /tmp/out
    python3 scripts/search.py --query "夜生活" --city Shanghai --top-k 5 --output-dir /tmp/out

参数:
    --query         必需，查询文本
    --output-dir    必需，结果输出目录
    --city          可选，过滤城市（Beijing / Shanghai / Guangzhou / Shenzhen）
    --time-start    可选，ISO 时间起点
    --time-end      可选，ISO 时间终点
    --geohash       可选，geohash 前缀过滤（如 wx4g）
    --anomaly-only  可选，只返回异常记录
    --top-k         可选，返回数量（默认 10）
    --mode          可选，强制模式 auto|es|local|demo（默认 auto）
    --local-file    可选，local 模式的 evidence.jsonl 路径
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import sys
import time
from collections import Counter
from pathlib import Path
from typing import List, Optional

# 同目录下的 landmarks 模块（geohash → 业务地名查表）
sys.path.insert(0, str(Path(__file__).parent.resolve()))
try:
    import landmarks  # type: ignore
except ImportError:
    landmarks = None  # 没有也能跑，只是不带 landmark 字段

logger = logging.getLogger(__name__)


# ───────────────────── 纯 stdlib BM25Okapi 实现（0 依赖）─────────────────────

class BM25Okapi:
    """
    自包含 BM25Okapi 实现 —— 不依赖 rank-bm25 / numpy / sklearn。
    经典公式: score(D,Q) = Σ_qi IDF(qi) * f(qi,D)*(k1+1) / (f(qi,D) + k1*(1 - b + b*|D|/avgdl))
    """

    def __init__(self, corpus_tokens: list[list[str]], k1: float = 1.5, b: float = 0.75):
        self.k1 = k1
        self.b = b
        self.corpus = corpus_tokens
        self.N = len(corpus_tokens)
        self.doc_len = [len(d) for d in corpus_tokens]
        self.avgdl = (sum(self.doc_len) / self.N) if self.N else 0.0
        self.doc_freqs: list[Counter] = [Counter(d) for d in corpus_tokens]
        # idf
        df: Counter = Counter()
        for d in corpus_tokens:
            for term in set(d):
                df[term] += 1
        self.idf = {
            term: math.log(1 + (self.N - n + 0.5) / (n + 0.5))
            for term, n in df.items()
        }

    def get_scores(self, query_tokens: list[str]) -> list[float]:
        scores = [0.0] * self.N
        if self.N == 0 or not query_tokens:
            return scores
        for qi in query_tokens:
            idf = self.idf.get(qi)
            if idf is None or idf <= 0:
                continue
            for i in range(self.N):
                f = self.doc_freqs[i].get(qi, 0)
                if f == 0:
                    continue
                dl = self.doc_len[i]
                denom = f + self.k1 * (1 - self.b + self.b * dl / (self.avgdl or 1))
                scores[i] += idf * (f * (self.k1 + 1)) / denom
        return scores

# ───────────────────── 路径常量 ─────────────────────

SCRIPT_DIR = Path(__file__).parent.resolve()
SKILL_ROOT = SCRIPT_DIR.parent
DEMO_EVIDENCE = SKILL_ROOT / "data" / "sample_evidence.jsonl"

ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
ES_INDEX_NAME = os.getenv("ES_INDEX_NAME", "citybench_evidence")
DASHSCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY", "")
RRF_K = int(os.getenv("RRF_RANK_CONSTANT", "60"))


# ───────────────────── 中文 + 英文混合分词 ─────────────────────

_PUNCT_RE = re.compile(r"[\s\W_]+", re.UNICODE)


def tokenize(text: str) -> list[str]:
    """简单的中英文混合分词：英文按空格/标点切，中文按字符切。"""
    if not text:
        return []
    cleaned = _PUNCT_RE.sub(" ", text.lower())
    tokens = []
    for chunk in cleaned.split():
        # 英文/数字直接加
        if all(c.isascii() for c in chunk):
            tokens.append(chunk)
        else:
            # 中文按单字 + 二字 bigram
            chars = [c for c in chunk if not c.isspace()]
            tokens.extend(chars)
            for i in range(len(chars) - 1):
                tokens.append(chars[i] + chars[i + 1])
    return [t for t in tokens if t]


# ───────────────────── 通用 evidence loader & filter ─────────────────────

def load_evidence_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Evidence file not found: {path}")
    records = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError as e:
                    logger.warning(f"Skip malformed line: {e}")
    return records


def apply_filters(
    records: list[dict],
    city: Optional[str],
    time_start: Optional[str],
    time_end: Optional[str],
    geohash: Optional[str],
    anomaly_only: bool,
) -> list[dict]:
    out = []
    for r in records:
        meta = r.get("meta", {})
        geo = meta.get("geo_scope", {})
        tr = meta.get("time_range", {})
        feat = meta.get("features", {})

        if city and geo.get("city") != city:
            continue
        if geohash and not str(geo.get("geohash", "")).startswith(geohash):
            continue
        if time_start and tr.get("start", "") < time_start:
            continue
        if time_end and tr.get("end", "9999") > time_end:
            continue
        if anomaly_only and not feat.get("anomaly_flag"):
            continue
        out.append(r)
    return out


# ───────────────────── 模式 1: DEMO（纯 Python，零外部依赖）─────────────────────

def search_demo_or_local(
    evidence_path: Path,
    query: str,
    city: Optional[str],
    time_start: Optional[str],
    time_end: Optional[str],
    geohash: Optional[str],
    anomaly_only: bool,
    top_k: int,
) -> tuple[list[dict], dict]:
    """
    本地检索：BM25 文本 + keyword overlap (类别+城市)，RRF 融合。
    模拟三通道：
      - Channel A: metadata filter (city/time/geohash/anomaly)  → 已在 apply_filters 完成
      - Channel B: BM25 over `text` 字段                         → 通道 B
      - Channel C: keyword overlap over top_categories+city+geohash → 通道 C 替代 kNN
    """
    all_records = load_evidence_jsonl(evidence_path)
    filtered = apply_filters(all_records, city, time_start, time_end, geohash, anomaly_only)

    if not filtered:
        return [], {
            "channel_a_filtered": 0,
            "channel_b_bm25_count": 0,
            "channel_c_keyword_count": 0,
            "total_in_index": len(all_records),
        }

    # Channel B: BM25
    corpus_tokens = [tokenize(r.get("text", "")) for r in filtered]
    bm25 = BM25Okapi(corpus_tokens)
    query_tokens = tokenize(query)
    bm25_scores = bm25.get_scores(query_tokens)
    bm25_ranked = sorted(
        range(len(filtered)), key=lambda i: bm25_scores[i], reverse=True
    )

    # Channel C: keyword overlap (类别名 + 城市名)
    qset = set(query_tokens)
    kw_scores = []
    for r in filtered:
        feat = r.get("meta", {}).get("features", {})
        geo = r.get("meta", {}).get("geo_scope", {})
        tags = list(feat.get("top_categories", []))
        tags.append(geo.get("city", ""))
        tags.append(geo.get("geohash", ""))
        cat_tokens: list[str] = []
        for t in tags:
            if t:
                cat_tokens.extend(tokenize(str(t)))
        cat_set = set(cat_tokens)
        overlap = len(qset & cat_set)
        kw_scores.append(overlap)
    kw_ranked = sorted(range(len(filtered)), key=lambda i: kw_scores[i], reverse=True)

    # RRF Fusion
    rrf: dict[int, float] = {}
    for rank, idx in enumerate(bm25_ranked, start=1):
        if bm25_scores[idx] <= 0:
            continue
        rrf[idx] = rrf.get(idx, 0.0) + 1.0 / (RRF_K + rank)
    for rank, idx in enumerate(kw_ranked, start=1):
        if kw_scores[idx] <= 0:
            continue
        rrf[idx] = rrf.get(idx, 0.0) + 1.0 / (RRF_K + rank)

    # 兜底：如果 BM25 + keyword 都 0 分（query 完全不命中），按 checkin_count 降序返回
    if not rrf:
        sorted_idx = sorted(
            range(len(filtered)),
            key=lambda i: filtered[i].get("meta", {}).get("features", {}).get("checkin_count", 0),
            reverse=True,
        )
        rrf = {idx: 1.0 / (RRF_K + r + 1) for r, idx in enumerate(sorted_idx)}

    sorted_indices = sorted(rrf, key=lambda i: rrf[i], reverse=True)[:top_k]
    results = [
        {
            "id": filtered[i].get("evidence_id", f"local_{i}"),
            "rrf_score": round(rrf[i], 6),
            "source": filtered[i],
        }
        for i in sorted_indices
    ]
    diag = {
        "channel_a_filtered": len(filtered),
        "channel_b_bm25_hits": sum(1 for s in bm25_scores if s > 0),
        "channel_c_keyword_hits": sum(1 for s in kw_scores if s > 0),
        "total_in_index": len(all_records),
    }
    return results, diag


# ───────────────────── 模式 2: ES（生产环境完整三通道）─────────────────────

def try_es_search(
    query: str,
    city: Optional[str],
    time_start: Optional[str],
    time_end: Optional[str],
    geohash: Optional[str],
    anomaly_only: bool,
    top_k: int,
) -> Optional[tuple[list[dict], dict]]:
    """尝试 ES + DashScope 完整 RAG。失败返回 None，调用方应降级。"""
    try:
        from elasticsearch import Elasticsearch
    except ImportError:
        logger.info("elasticsearch package not installed → skip ES mode")
        return None
    if not DASHSCOPE_API_KEY:
        logger.info("DASHSCOPE_API_KEY not set → skip ES mode")
        return None

    try:
        es = Elasticsearch(ES_HOST, request_timeout=5, max_retries=1)
        if not es.ping():
            logger.info(f"ES ping failed at {ES_HOST} → skip ES mode")
            return None
        if not es.indices.exists(index=ES_INDEX_NAME):
            logger.info(f"Index {ES_INDEX_NAME} does not exist → skip ES mode")
            return None
    except Exception as e:
        logger.info(f"ES connection error: {e} → skip ES mode")
        return None

    try:
        from openai import OpenAI
    except ImportError:
        logger.info("openai package not installed → skip ES mode")
        return None

    # ── DashScope embedding ──
    try:
        client = OpenAI(
            api_key=DASHSCOPE_API_KEY,
            base_url=os.getenv("DASHSCOPE_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1"),
        )
        resp = client.embeddings.create(
            model=os.getenv("EMBEDDING_MODEL", "text-embedding-v3"),
            input=[query],
            dimensions=int(os.getenv("EMBEDDING_DIMENSIONS", "1024")),
            encoding_format="float",
        )
        qvec = resp.data[0].embedding
    except Exception as e:
        logger.warning(f"DashScope embedding failed: {e} → skip ES mode")
        return None

    # ── 构建 filter ──
    filters: list[dict] = []
    if city:
        filters.append({"term": {"meta.geo_scope.city": city}})
    if time_start:
        filters.append({"range": {"meta.time_range.start": {"gte": time_start}}})
    if time_end:
        filters.append({"range": {"meta.time_range.end": {"lte": time_end}}})
    if geohash:
        filters.append({"prefix": {"meta.geo_scope.geohash": geohash}})
    if anomaly_only:
        filters.append({"term": {"meta.features.anomaly_flag": True}})

    cand = top_k * 5

    # BM25
    bm25_body = {
        "query": {
            "bool": {
                "must": [{"match": {"text": {"query": query, "analyzer": "ik_smart"}}}],
                "filter": filters,
            }
        },
        "size": cand,
        "_source": True,
    }
    bm25_hits = es.search(index=ES_INDEX_NAME, body=bm25_body)["hits"]["hits"]

    # kNN
    inner = {"bool": {"filter": filters}} if filters else {"match_all": {}}
    knn_body = {
        "query": {
            "script_score": {
                "query": inner,
                "script": {
                    "source": "cosineSimilarity(params.query_vector, 'text_vector') + 1.0",
                    "params": {"query_vector": qvec},
                },
            }
        },
        "size": cand,
        "_source": True,
    }
    knn_hits = es.search(index=ES_INDEX_NAME, body=knn_body)["hits"]["hits"]

    # RRF
    rrf: dict[str, float] = {}
    sources: dict[str, dict] = {}
    for rank, h in enumerate(bm25_hits, start=1):
        rrf[h["_id"]] = rrf.get(h["_id"], 0.0) + 1.0 / (RRF_K + rank)
        sources[h["_id"]] = h["_source"]
    for rank, h in enumerate(knn_hits, start=1):
        rrf[h["_id"]] = rrf.get(h["_id"], 0.0) + 1.0 / (RRF_K + rank)
        sources[h["_id"]] = h["_source"]
    sorted_ids = sorted(rrf, key=lambda x: rrf[x], reverse=True)[:top_k]
    results = [
        {"id": _id, "rrf_score": round(rrf[_id], 6), "source": sources[_id]}
        for _id in sorted_ids
    ]
    diag = {
        "channel_b_bm25_count": len(bm25_hits),
        "channel_c_knn_count": len(knn_hits),
        "es_index": ES_INDEX_NAME,
    }
    return results, diag


# ───────────────────── CLI ─────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description="CityBench RAG hybrid search (3-tier auto-fallback)")
    parser.add_argument("--query", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--city")
    parser.add_argument("--time-start")
    parser.add_argument("--time-end")
    parser.add_argument("--geohash")
    parser.add_argument("--anomaly-only", action="store_true")
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--mode", choices=["auto", "es", "local", "demo"], default="auto")
    parser.add_argument("--local-file", help="Path to local evidence.jsonl (for --mode local)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    start = time.time()
    results: list[dict] = []
    diag: dict = {}
    used_mode = "demo"

    # ── 模式选择 ──
    if args.mode in ("auto", "es"):
        es_result = try_es_search(
            query=args.query, city=args.city,
            time_start=args.time_start, time_end=args.time_end,
            geohash=args.geohash, anomaly_only=args.anomaly_only,
            top_k=args.top_k,
        )
        if es_result is not None:
            results, diag = es_result
            used_mode = "es"

    if not results and args.mode in ("auto", "local") and args.local_file:
        local_path = Path(args.local_file)
        if local_path.exists():
            results, diag = search_demo_or_local(
                evidence_path=local_path, query=args.query, city=args.city,
                time_start=args.time_start, time_end=args.time_end,
                geohash=args.geohash, anomaly_only=args.anomaly_only,
                top_k=args.top_k,
            )
            used_mode = "local"

    if not results and args.mode in ("auto", "demo"):
        if not DEMO_EVIDENCE.exists():
            logger.error(f"Demo evidence not found at {DEMO_EVIDENCE}")
            return 1
        results, diag = search_demo_or_local(
            evidence_path=DEMO_EVIDENCE, query=args.query, city=args.city,
            time_start=args.time_start, time_end=args.time_end,
            geohash=args.geohash, anomaly_only=args.anomaly_only,
            top_k=args.top_k,
        )
        used_mode = "demo"

    elapsed = time.time() - start

    # ── 业务地名 enrichment（geohash → 陆家嘴金融区 等）──
    if landmarks is not None:
        for r in results:
            src = r.get("source", {})
            if isinstance(src, dict):
                landmarks.enrich_evidence(src)

    # ── 写文件 ──
    results_path = out_dir / "search_results.jsonl"
    with open(results_path, "w", encoding="utf-8") as f:
        for r in results:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    summary = {
        "query": args.query,
        "mode": used_mode,
        "filters": {
            "city": args.city,
            "time_start": args.time_start,
            "time_end": args.time_end,
            "geohash": args.geohash,
            "anomaly_only": args.anomaly_only,
        },
        "top_k": args.top_k,
        "result_count": len(results),
        "elapsed_seconds": round(elapsed, 3),
        "diagnostics": diag,
        "top_scores": [
            {"id": r["id"], "rrf_score": r["rrf_score"]} for r in results[:5]
        ],
        "top_evidence_preview": [
            {
                "id": r["id"],
                "text": r["source"].get("text", "")[:200],
                "city": r["source"].get("meta", {}).get("geo_scope", {}).get("city"),
                "geohash": r["source"].get("meta", {}).get("geo_scope", {}).get("geohash"),
                "landmark": r["source"].get("meta", {}).get("geo_scope", {}).get("landmark"),
                "lat": r["source"].get("meta", {}).get("geo_scope", {}).get("lat"),
                "lon": r["source"].get("meta", {}).get("geo_scope", {}).get("lon"),
                "checkin_count": r["source"].get("meta", {}).get("features", {}).get("checkin_count"),
                "anomaly": r["source"].get("meta", {}).get("features", {}).get("anomaly_flag"),
            }
            for r in results[:5]
        ],
    }
    summary_path = out_dir / "summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, ensure_ascii=False, indent=2, fp=f)

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"\n[mode={used_mode}] Results: {results_path}")
    print(f"[mode={used_mode}] Summary: {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
