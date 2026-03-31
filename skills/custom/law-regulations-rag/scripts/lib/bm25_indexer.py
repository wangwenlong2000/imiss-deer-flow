import json
import pickle
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Union

from rank_bm25 import BM25Okapi

try:
    import spacy_pkuseg as pkuseg
except ImportError:
    pkuseg = None


# =========================
# 基础工具
# =========================

def normalize_text(text: Optional[str]) -> str:
    if text is None:
        return ""
    text = str(text)
    text = text.replace("\\n", "\n").replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\u3000", " ")
    text = text.replace("（", "(").replace("）", ")")
    text = text.replace("【", "[").replace("】", "]")
    text = text.replace("：", ":")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()

def normalize_single_line(text: Optional[str]) -> str:
    text = normalize_text(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def read_json_or_jsonl(path: Union[str, Path]) -> List[Dict[str, Any]]:
    path = Path(path)
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []

    if path.suffix.lower() == ".jsonl":
        rows = []
        with path.open("r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except Exception as e:
                    raise ValueError(f"{path} 第 {line_no} 行 JSON 解析失败: {e}")
        return rows

    data = json.loads(text)
    if isinstance(data, list):
        return data
    raise ValueError(f"{path} 不是 list 格式的 JSON")


def write_json(path: Union[str, Path], obj: Any):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


# =========================
# LeCoQA 数据结构
# =========================

@dataclass
class LegalArticleDoc:
    doc_id: int
    title: str
    text: str
    search_text: str


def load_lecoqa_corpus(
    corpus_path: Union[str, Path],
    title_boost: int = 2,
) -> List[LegalArticleDoc]:
    """
    读取 corpus_clean.jsonl
    期望字段：
    {
      "doc_id": 705,
      "title": "...",
      "text": "..."
    }
    """
    rows = read_json_or_jsonl(corpus_path)
    docs: List[LegalArticleDoc] = []

    for row in rows:
        doc_id = int(row["doc_id"])
        title = normalize_single_line(row["title"])
        text = normalize_text(row.get("text", ""))

        # 只做最简单的标题强化
        title_block = "\n".join([title] * max(1, title_boost))
        search_text = "\n".join([p for p in [title_block, text] if p]).strip()

        docs.append(
            LegalArticleDoc(
                doc_id=doc_id,
                title=title,
                text=text,
                search_text=search_text,
            )
        )

    return docs


def load_lecoqa_queries(queries_path: Union[str, Path]) -> List[Dict[str, Any]]:
    """
    兼容 queries_clean.jsonl 或预处理后的 json
    评测时只使用：
    - query_id
    - question
    - positive_ids
    """
    rows = read_json_or_jsonl(queries_path)
    cleaned = []

    for row in rows:
        query_id = int(row["query_id"])
        question = normalize_single_line(row.get("question", row.get("问题", "")))
        positive_ids = row.get("positive_ids", row.get("match_id", []))
        positive_ids = [int(x) for x in positive_ids]

        cleaned.append(
            {
                "query_id": query_id,
                "question": question,
                "positive_ids": positive_ids,
            }
        )

    return cleaned


# =========================
# 中文分词器
# =========================

class ChineseTokenizer:
    """
    优先用 pkuseg + user_dict
    如果没装 spacy-pkuseg，则退化成规则切分
    """

    def __init__(
        self,
        user_dict_path: Optional[str] = None,
        enable_bigrams: bool = True,
    ):
        self.user_dict_path = user_dict_path
        self.enable_bigrams = enable_bigrams

        self.seg = None
        if pkuseg is not None:
            seg_kwargs = {"postag": False}
            if user_dict_path:
                user_dict_path = str(user_dict_path)
                if not Path(user_dict_path).exists():
                    raise FileNotFoundError(f"BM25 用户词典不存在: {user_dict_path}")
                seg_kwargs["user_dict"] = user_dict_path
                print(f"[BM25] 使用 spacy_pkuseg，自定义词典: {user_dict_path}")
            else:
                print("[BM25] 使用 spacy_pkuseg，未提供自定义词典")

            self.seg = pkuseg.pkuseg(**seg_kwargs)
        else:
            print("[BM25] 未安装 spacy_pkuseg，退化为规则切分")

    def get_config(self) -> Dict[str, Any]:
        return {
            "user_dict_path": self.user_dict_path,
            "enable_bigrams": self.enable_bigrams,
        }

    def normalize(self, text: str) -> str:
        return normalize_single_line(text)

    def extract_rule_tokens(self, text: str) -> List[str]:
        tokens: List[str] = []
        patterns = [
            r"第[一二三四五六七八九十百千万零〇两\d]+条",
            r"第[一二三四五六七八九十百千万零〇两\d]+章",
            r"第[一二三四五六七八九十百千万零〇两\d]+款",
            r"第[一二三四五六七八九十百千万零〇两\d]+项",
        ]
        for pattern in patterns:
            tokens.extend(re.findall(pattern, text))
        return tokens

    def _fallback_cut(self, text: str) -> List[str]:
        pieces = re.findall(r"[\u4e00-\u9fff]+|[A-Za-z0-9_.-]+", text)
        tokens = []
        for piece in pieces:
            piece = piece.strip()
            if not piece:
                continue
            tokens.append(piece)

            if (
                self.enable_bigrams
                and len(piece) <= 20
                and re.fullmatch(r"[\u4e00-\u9fff]+", piece)
            ):
                for i in range(len(piece) - 1):
                    tokens.append(piece[i:i + 2])

        return tokens

    def _base_tokenize(self, text: str) -> List[str]:
        text = self.normalize(text)
        if not text:
            return []

        tokens: List[str] = []

        if self.seg is not None:
            tokens.extend([w.strip() for w in self.seg.cut(text) if w.strip()])
        else:
            tokens.extend(self._fallback_cut(text))

        tokens.extend(self.extract_rule_tokens(text))
        return tokens

    def tokenize_document(self, text: str) -> List[str]:
        return self._base_tokenize(text)

    def tokenize_query(self, text: str) -> List[str]:
        text = self.normalize(text)
        if not text:
            return []

        tokens = self._base_tokenize(text)

        if self.enable_bigrams:
            compact = re.sub(r"\s+", "", text)
            if len(compact) <= 20:
                for i in range(len(compact) - 1):
                    bg = compact[i:i + 2]
                    if re.fullmatch(r"[\u4e00-\u9fff]{2}", bg):
                        tokens.append(bg)

        return list(dict.fromkeys(tokens))


# =========================
# BM25
# =========================

class BM25Indexer:
    def __init__(
        self,
        tokenizer: Optional[ChineseTokenizer] = None,
        k1: float = 1.5,
        b: float = 0.75,
    ):
        self.tokenizer = tokenizer or ChineseTokenizer()
        self.k1 = k1
        self.b = b

        self.bm25 = None
        self.corpus: List[str] = []
        self.tokenized_corpus: List[List[str]] = []
        self.metadata: List[Dict[str, Any]] = []

    def build(self, docs: List[LegalArticleDoc]) -> "BM25Indexer":
        print(f"[BM25] 开始构建 BM25 索引，文档数: {len(docs)}")

        self.corpus = [doc.search_text for doc in docs]
        self.metadata = [
            {
                "doc_id": doc.doc_id,
                "title": doc.title,
                "text": doc.text,
            }
            for doc in docs
        ]

        print("[BM25] 开始分词...")
        self.tokenized_corpus = [
            self.tokenizer.tokenize_document(text) for text in self.corpus
        ]
        print("[BM25] 分词完成，开始构建 BM25Okapi...")
        self._rebuild_bm25()
        print("[BM25] BM25 索引构建完成")
        return self

    def _rebuild_bm25(self):
        if not self.tokenized_corpus:
            self.bm25 = None
            return

        self.bm25 = BM25Okapi(self.tokenized_corpus, k1=self.k1, b=self.b)

    def _ensure_ready(self):
        if self.bm25 is None or not self.corpus:
            raise ValueError("Index has not been built or loaded.")

    def _get_scores(self, query: str) -> List[float]:
        tokens = self.tokenizer.tokenize_query(query)
        if not tokens:
            return [0.0] * len(self.corpus)

        scores = self.bm25.get_scores(tokens)
        return scores.tolist() if hasattr(scores, "tolist") else list(scores)

    def search(
        self,
        query: str,
        top_k: int = 10,
    ) -> List[Dict[str, Any]]:
        self._ensure_ready()

        query = self.tokenizer.normalize(query)
        if not query:
            return []

        scores = self._get_scores(query)

        results = []
        for idx, score in enumerate(scores):
            if score <= 0:
                continue

            meta = self.metadata[idx]
            results.append(
                {
                    "doc_id": meta["doc_id"],
                    "title": meta["title"],
                    "text": meta["text"],
                    "score": float(score),
                }
            )

        results.sort(key=lambda x: x["score"], reverse=True)
        return results[:top_k]

    def save(self, path: Union[str, Path]):
        self._ensure_ready()
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        with path.open("wb") as f:
            pickle.dump(
                {
                    "corpus": self.corpus,
                    "tokenized_corpus": self.tokenized_corpus,
                    "metadata": self.metadata,
                    "k1": self.k1,
                    "b": self.b,
                    "tokenizer_config": self.tokenizer.get_config(),
                },
                f,
            )

        print(f"[BM25] 索引已保存到: {path}")

    def load(self, path: Union[str, Path]) -> "BM25Indexer":
        path = Path(path)
        with path.open("rb") as f:
            data = pickle.load(f)

        self.corpus = data["corpus"]
        self.tokenized_corpus = data["tokenized_corpus"]
        self.metadata = data["metadata"]
        self.k1 = data.get("k1", 1.5)
        self.b = data.get("b", 0.75)

        tk_cfg = data.get("tokenizer_config", {})
        self.tokenizer = ChineseTokenizer(
            user_dict_path=tk_cfg.get("user_dict_path"),
            enable_bigrams=tk_cfg.get("enable_bigrams", True),
        )

        self._rebuild_bm25()
        return self


# =========================
# 构建与评测
# =========================

def build_lecoqa_bm25_index(
    corpus_path: Union[str, Path],
    output_index_path: Union[str, Path],
    user_dict_path: Optional[Union[str, Path]] = None,
    title_boost: int = 2,
) -> BM25Indexer:
    docs = load_lecoqa_corpus(corpus_path, title_boost=title_boost)

    tokenizer = ChineseTokenizer(
        user_dict_path=str(user_dict_path) if user_dict_path else None,
        enable_bigrams=True,
    )

    indexer = BM25Indexer(tokenizer=tokenizer)
    indexer.build(docs)
    indexer.save(output_index_path)
    return indexer


def evaluate_recall(
    indexer: BM25Indexer,
    queries_path: Union[str, Path],
    top_ks: Sequence[int] = (1, 5, 10),
    max_samples: Optional[int] = None,
) -> Dict[str, Any]:
    queries = load_lecoqa_queries(queries_path)
    if max_samples is not None:
        queries = queries[:max_samples]

    hit_counts = {k: 0 for k in top_ks}
    bad_cases = []

    for item in queries:
        question = item["question"]
        positive_ids = set(item["positive_ids"])

        results = indexer.search(query=question, top_k=max(top_ks))
        pred_ids = [r["doc_id"] for r in results]

        for k in top_ks:
            top_pred = set(pred_ids[:k])
            if positive_ids & top_pred:
                hit_counts[k] += 1

        if not (positive_ids & set(pred_ids[:max(top_ks)])):
            bad_cases.append(
                {
                    "query_id": item["query_id"],
                    "question": question,
                    "positive_ids": sorted(list(positive_ids)),
                    "pred_top10": pred_ids[:10],
                }
            )

    total = len(queries) or 1
    metrics = {
        f"Recall@{k}": round(hit_counts[k] / total, 4)
        for k in top_ks
    }
    metrics["total_queries"] = len(queries)
    metrics["bad_case_count"] = len(bad_cases)
    metrics["bad_cases_preview"] = bad_cases[:20]
    return metrics


# =========================
# main
# =========================

if __name__ == "__main__":
    corpus_path = "data/processed/corpus_clean.jsonl"
    queries_path = "data/processed/queries_clean.jsonl"
    user_dict_path = "data/lexicon/thuocl_law_user_dict.txt"

    output_index_path = "data/index/lecoqa_bm25.pkl"
    output_eval_path = "data/index/lecoqa_bm25_eval.json"

    # True: 无论索引是否存在都重新构建
    # False: 如果索引已存在，则直接加载
    force_rebuild = False

    print("=" * 60)
    print("LeCoQA BM25 索引")
    print("=" * 60)

    if (not force_rebuild) and Path(output_index_path).exists():
        print(f"检测到已有索引，直接加载: {output_index_path}")
        indexer = BM25Indexer(
            tokenizer=ChineseTokenizer(
                user_dict_path=user_dict_path,
                enable_bigrams=True,
            )
        ).load(output_index_path)
    else:
        print("未检测到可用索引，开始重新构建...")
        indexer = build_lecoqa_bm25_index(
            corpus_path=corpus_path,
            output_index_path=output_index_path,
            user_dict_path=user_dict_path,
            title_boost=2,
        )
        print(f"索引已保存到: {output_index_path}")

    print("\n开始离线评测 Recall@1/5/10 ...")
    metrics = evaluate_recall(
        indexer=indexer,
        queries_path=queries_path,
        top_ks=(1, 5, 10),
        max_samples=100,
    )

    write_json(output_eval_path, metrics)
    print(json.dumps(metrics, ensure_ascii=False, indent=2))
    print(f"评测结果已保存到: {output_eval_path}")

    print("\n手工查询示例：")
    test_queries = [
        "谁可以成为个体工商户？",
        "夫妻一方经营个体工商户所欠债务，谁偿还？",
        "监护资格撤销后可以恢复吗？",
    ]

    for query in test_queries:
        print(f"\n查询: {query}")
        results = indexer.search(query=query, top_k=5)
        for i, r in enumerate(results, 1):
            print(f"{i}. {r['doc_id']} | {r['title']} | score={r['score']:.4f}")
            print(f"   {r['text'][:100]}...")