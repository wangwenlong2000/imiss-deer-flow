import json
import os
import pickle
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Union

import numpy as np
from openai import OpenAI

try:
    import faiss

    HAS_FAISS = True
except ImportError:
    faiss = None
    HAS_FAISS = False


# =========================
# 基础工具
# =========================

def normalize_text(text: Optional[str]) -> str:
    if text is None:
        return ""
    text = str(text)
    text = text.replace("\\n", "\n").replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\u3000", " ")
    return text.strip()


def normalize_single_line(text: Optional[str]) -> str:
    text = normalize_text(text)
    return " ".join(text.split())


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
# 数据结构
# =========================

@dataclass
class LegalArticleDoc:
    doc_id: int
    title: str
    text: str
    search_text: str


def load_lecoqa_corpus(corpus_path: Union[str, Path]) -> List[LegalArticleDoc]:
    """
    读取 processed/corpus_clean.jsonl
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

        # 向量检索：标题 + 正文
        search_text = "\n".join([p for p in [title, text] if p]).strip()

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
    读取 processed/queries_clean.jsonl
    评测只使用：
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
# Embedding API 封装
# =========================

class EmbeddingModel:
    def __init__(
        self,
        model_name: str = "text-embedding-v3",
        api_key: Optional[str] = None,
        base_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1",
        query_prefix: str = "",
        dimensions: int = 1024,
        max_batch_size: int = 10,
        retry_times: int = 3,
        retry_interval: float = 1.5,
    ):
        self.model_name = model_name
        self.api_key = api_key or os.getenv("DASHSCOPE_API_KEY")
        self.base_url = base_url
        self.query_prefix = query_prefix
        self.dimensions = dimensions
        self.max_batch_size = max_batch_size
        self.retry_times = retry_times
        self.retry_interval = retry_interval
        self.embedding_dim = dimensions

        if not self.api_key:
            raise ValueError("未找到 embedding api_key，请检查 models.yaml 或环境变量")

        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url,
        )

    def load(self):
        return

    def _l2_normalize(self, arr: np.ndarray) -> np.ndarray:
        norms = np.linalg.norm(arr, axis=1, keepdims=True)
        norms = np.clip(norms, 1e-12, None)
        return arr / norms

    def _encode_batch(self, texts: List[str], normalize: bool = True) -> np.ndarray:
        last_error = None

        for attempt in range(1, self.retry_times + 1):
            try:
                resp = self.client.embeddings.create(
                    model=self.model_name,
                    input=texts[0] if len(texts) == 1 else texts,
                    dimensions=self.dimensions,
                    encoding_format="float",
                )

                vectors = [item.embedding for item in resp.data]
                arr = np.asarray(vectors, dtype=np.float32)

                if normalize:
                    arr = self._l2_normalize(arr)

                if self.embedding_dim is None and arr.shape[1] > 0:
                    self.embedding_dim = int(arr.shape[1])

                return arr

            except Exception as e:
                last_error = e
                if attempt < self.retry_times:
                    time.sleep(self.retry_interval * attempt)
                else:
                    break

        raise RuntimeError(f"Embedding 最终失败: {last_error}") from last_error

    def encode(self, texts: List[str], batch_size: int = 32, normalize: bool = True) -> np.ndarray:
        if not texts:
            return np.zeros((0, self.embedding_dim or 0), dtype=np.float32)

        total_outer_batches = (len(texts) + batch_size - 1) // batch_size
        print(f"[Embedding] 开始编码，共 {len(texts)} 条文本，outer batch_size={batch_size}，共 {total_outer_batches} 批")

        all_embeddings = []
        outer_batch_idx = 0

        for start in range(0, len(texts), batch_size):
            outer_batch_idx += 1
            outer_batch = texts[start:start + batch_size]
            inner_total = (len(outer_batch) + self.max_batch_size - 1) // self.max_batch_size

            print(
                f"[Embedding] 正在处理第 {outer_batch_idx}/{total_outer_batches} 个外层批次，"
                f"本批 {len(outer_batch)} 条，将拆成 {inner_total} 个 API 子批次"
            )

            inner_batch_idx = 0
            for inner_start in range(0, len(outer_batch), self.max_batch_size):
                inner_batch_idx += 1
                inner_batch = outer_batch[inner_start:inner_start + self.max_batch_size]

                print(
                    f"[Embedding] 调用 API：外层批次 {outer_batch_idx}/{total_outer_batches}，"
                    f"子批次 {inner_batch_idx}/{inner_total}，大小={len(inner_batch)}"
                )

                emb = self._encode_batch(inner_batch, normalize=normalize)
                all_embeddings.append(emb)

        print("[Embedding] 全部文本编码完成")
        return np.vstack(all_embeddings).astype(np.float32)

    def encode_query(self, query: str) -> np.ndarray:
        query = query.strip()
        if self.query_prefix:
            query = self.query_prefix + query
        return self.encode([query], normalize=True)[0]


# =========================
# 向量索引器
# =========================

class VectorIndexer:
    def __init__(
        self,
        embedding_model: Optional[EmbeddingModel] = None,
        index_type: str = "HNSW",
    ):
        self.embedding_model = embedding_model or EmbeddingModel()
        self.index_type = index_type.upper()

        self.index = None
        self.embeddings: Optional[np.ndarray] = None
        self.embedding_dim: Optional[int] = None
        self.metadata: List[Dict[str, Any]] = []

    def build(self, docs: List[LegalArticleDoc], batch_size: int = 32) -> "VectorIndexer":
        if not docs:
            raise ValueError("docs 为空，无法构建向量索引")

        print(f"[VectorIndexer] 开始构建向量索引，文档数: {len(docs)}")
        self.embedding_model.load()

        texts = [doc.search_text for doc in docs]
        print("[VectorIndexer] 开始生成 embedding ...")
        self.embeddings = self.embedding_model.encode(texts, batch_size=batch_size).astype("float32")
        self.embedding_dim = int(self.embeddings.shape[1])
        print(f"[VectorIndexer] embedding 生成完成，shape={self.embeddings.shape}")

        self.metadata = [
            {
                "doc_id": doc.doc_id,
                "title": doc.title,
                "text": doc.text,
            }
            for doc in docs
        ]
        print(f"[VectorIndexer] metadata 构建完成，共 {len(self.metadata)} 条")

        print(f"[VectorIndexer] 开始构建 {self.index_type} 索引 ...")
        self._build_index()
        print("[VectorIndexer] 索引构建完成")
        return self

    def _build_index(self):
        if self.embeddings is None or len(self.embeddings) == 0:
            raise ValueError("embeddings 为空，无法构建索引")

        if not HAS_FAISS:
            print("[VectorIndexer] Warning: faiss 未安装，将使用 numpy fallback 检索")
            self.index = None
            return

        embeddings = self.embeddings.astype("float32")
        n = len(embeddings)

        print(f"[VectorIndexer] 使用 index_type={self.index_type}，向量数={n}，维度={self.embedding_dim}")

        if self.index_type == "HNSW":
            index = faiss.IndexHNSWFlat(self.embedding_dim, 32, faiss.METRIC_INNER_PRODUCT)
            index.hnsw.efConstruction = 200
            index.hnsw.efSearch = 50
        elif self.index_type == "IVF":
            nlist = max(1, min(100, n // 10))
            print(f"[VectorIndexer] IVF 参数 nlist={nlist}")
            quantizer = faiss.IndexFlatIP(self.embedding_dim)
            index = faiss.IndexIVFFlat(
                quantizer,
                self.embedding_dim,
                nlist,
                faiss.METRIC_INNER_PRODUCT,
            )
            if not index.is_trained:
                print("[VectorIndexer] IVF 开始训练 ...")
                index.train(embeddings)
                print("[VectorIndexer] IVF 训练完成")
        elif self.index_type == "FLAT":
            index = faiss.IndexFlatIP(self.embedding_dim)
        else:
            print(f"[VectorIndexer] Warning: 未知 index_type={self.index_type}，自动回退为 FLAT")
            index = faiss.IndexFlatIP(self.embedding_dim)

        print("[VectorIndexer] 开始 add embeddings 到索引 ...")
        index.add(embeddings)
        self.index = index
        print(f"[VectorIndexer] Built {self.index_type} index with {n} vectors")

    def search(self, query: str, top_k: int = 10) -> List[Dict[str, Any]]:
        if not self.metadata:
            return []
        if self.index is None and self.embeddings is None:
            return []

        query = query.strip()
        if not query:
            return []

        query_embedding = self.embedding_model.encode_query(query).astype("float32").reshape(1, -1)
        search_k = min(max(top_k * 3, top_k), len(self.metadata))

        if self.index is not None and HAS_FAISS:
            if hasattr(self.index, "hnsw"):
                self.index.hnsw.efSearch = max(top_k, 50)

            scores, indices = self.index.search(query_embedding, search_k)
            score_list = scores[0]
            idx_list = indices[0]
        else:
            score_array = np.dot(self.embeddings, query_embedding.T).flatten()
            idx_list = np.argsort(score_array)[::-1][:search_k]
            score_list = score_array[idx_list]

        results = []
        for idx, score in zip(idx_list, score_list):
            idx = int(idx)
            if idx < 0 or idx >= len(self.metadata):
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

            if len(results) >= top_k:
                break

        return results

    def save(self, index_path: Union[str, Path], metadata_path: Union[str, Path]):
        index_path = Path(index_path)
        metadata_path = Path(metadata_path)

        index_path.parent.mkdir(parents=True, exist_ok=True)
        metadata_path.parent.mkdir(parents=True, exist_ok=True)

        print(f"[VectorIndexer] 开始保存索引到: {index_path}")
        print(f"[VectorIndexer] 开始保存元数据到: {metadata_path}")

        if self.embeddings is not None:
            embeddings_path = str(index_path).replace(".index", "_embeddings.npy")
            np.save(embeddings_path, self.embeddings)
            print(f"[VectorIndexer] embeddings 已保存到: {embeddings_path}")

        if HAS_FAISS and self.index is not None:
            faiss.write_index(self.index, str(index_path))
            print(f"[VectorIndexer] faiss 索引已保存到: {index_path}")

        with metadata_path.open("wb") as f:
            pickle.dump(
                {
                    "metadata": self.metadata,
                    "embedding_dim": self.embedding_dim,
                    "index_type": self.index_type,
                    "model_name": self.embedding_model.model_name,
                    "base_url": self.embedding_model.base_url,
                    "query_prefix": self.embedding_model.query_prefix,
                    "dimensions": self.embedding_model.dimensions,
                    "max_batch_size": self.embedding_model.max_batch_size,
                    "retry_times": self.embedding_model.retry_times,
                    "retry_interval": self.embedding_model.retry_interval,
                },
                f,
            )

        print("[VectorIndexer] 保存完成")

    def load(self, index_path: Union[str, Path], metadata_path: Union[str, Path]) -> "VectorIndexer":
        index_path = Path(index_path)
        metadata_path = Path(metadata_path)

        self.index = None
        self.embeddings = None
        self.embedding_dim = None

        if HAS_FAISS and index_path.exists():
            self.index = faiss.read_index(str(index_path))

        embeddings_path = str(index_path).replace(".index", "_embeddings.npy")
        if os.path.exists(embeddings_path):
            self.embeddings = np.load(embeddings_path)

        with metadata_path.open("rb") as f:
            data = pickle.load(f)

        self.metadata = data["metadata"]
        self.embedding_dim = data.get("embedding_dim")
        self.index_type = data.get("index_type", self.index_type)

        model_name = data.get("model_name", self.embedding_model.model_name)
        base_url = data.get("base_url", self.embedding_model.base_url)
        query_prefix = data.get("query_prefix", self.embedding_model.query_prefix)
        dimensions = data.get("dimensions", getattr(self.embedding_model, "dimensions", 1024))
        max_batch_size = data.get("max_batch_size", getattr(self.embedding_model, "max_batch_size", 10))
        retry_times = data.get("retry_times", getattr(self.embedding_model, "retry_times", 3))
        retry_interval = data.get("retry_interval", getattr(self.embedding_model, "retry_interval", 1.5))

        self.embedding_model = EmbeddingModel(
            model_name=model_name,
            api_key=self.embedding_model.api_key,
            base_url=base_url,
            query_prefix=query_prefix,
            dimensions=dimensions,
            max_batch_size=max_batch_size,
            retry_times=retry_times,
            retry_interval=retry_interval,
        )

        if self.index is not None and hasattr(self.index, "d"):
            self.embedding_dim = int(self.index.d)
        elif self.embeddings is not None:
            self.embedding_dim = int(self.embeddings.shape[1])

        return self


# =========================
# 构建与评测
# =========================

def build_lecoqa_vector_index(
    corpus_path: Union[str, Path],
    index_path: Union[str, Path],
    metadata_path: Union[str, Path],
    model_name: str = "text-embedding-v3",
    api_key: Optional[str] = None,
    base_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1",
    index_type: str = "HNSW",
    batch_size: int = 32,
    query_prefix: str = "",
    dimensions: Optional[int] = 1024,
    max_batch_size: int = 10,
    retry_times: int = 3,
    retry_interval: float = 1.5,
) -> VectorIndexer:
    docs = load_lecoqa_corpus(corpus_path)

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
    indexer = VectorIndexer(
        embedding_model=embedding_model,
        index_type=index_type,
    )
    indexer.build(docs, batch_size=batch_size)
    indexer.save(index_path, metadata_path)
    return indexer


def evaluate_recall(
    indexer: VectorIndexer,
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