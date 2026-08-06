"""Pure-Python TF-IDF nearest-neighbor classifier."""
from __future__ import annotations

import json
import math
from collections import Counter
from pathlib import Path
from typing import Any

from .feature_extractor import ALL_LABELS, extract_features, map_label


SparseVector = dict[int, float]


class TfidfKNNModel:
    def __init__(
        self,
        *,
        labels: list[str] | None = None,
        min_df: int = 1,
        max_features: int = 50000,
        k: int = 1,
        max_count: int = 8,
    ) -> None:
        self.labels = labels or list(ALL_LABELS)
        self.min_df = min_df
        self.max_features = max_features
        self.k = k
        self.max_count = max_count
        self.vocabulary: dict[str, int] = {}
        self.idf: dict[int, float] = {}
        self.train_labels: list[str] = []
        self.train_refs: list[dict[str, Any]] = []
        self.train_vectors: list[SparseVector] = []
        self.total_docs = 0

    def fit(self, rows: list[dict[str, Any]]) -> "TfidfKNNModel":
        raw_features = [extract_features(row) for row in rows]
        document_frequency: Counter[str] = Counter()
        collection_frequency: Counter[str] = Counter()
        for features in raw_features:
            document_frequency.update(features.keys())
            collection_frequency.update(features)

        vocab_terms = [
            term
            for term, _ in collection_frequency.most_common()
            if document_frequency[term] >= self.min_df
        ][: self.max_features]
        self.vocabulary = {term: idx for idx, term in enumerate(vocab_terms)}
        self.total_docs = len(rows)
        self.idf = {
            self.vocabulary[term]: math.log((self.total_docs + 1.0) / (document_frequency[term] + 1.0)) + 1.0
            for term in vocab_terms
        }
        self.train_labels = [map_label(row) for row in rows]
        self.train_refs = [
            {
                "data_id": row.get("data_id"),
                "sample_id": row.get("sample_id"),
                "data_type": row.get("data_type"),
            }
            for row in rows
        ]
        self.train_vectors = [self._vectorize_features(features) for features in raw_features]
        return self

    def _vectorize_features(self, features: Counter[str]) -> SparseVector:
        vector: SparseVector = {}
        for term, count in features.items():
            idx = self.vocabulary.get(term)
            if idx is None:
                continue
            vector[idx] = min(int(count), self.max_count) * self.idf[idx]
        norm = math.sqrt(sum(value * value for value in vector.values()))
        if not norm:
            return {}
        return {idx: value / norm for idx, value in vector.items()}

    def vectorize(self, row: dict[str, Any]) -> SparseVector:
        return self._vectorize_features(extract_features(row))

    @staticmethod
    def cosine(left: SparseVector, right: SparseVector) -> float:
        if not left or not right:
            return 0.0
        if len(left) > len(right):
            left, right = right, left
        return sum(value * right.get(idx, 0.0) for idx, value in left.items())

    def nearest_neighbors(self, row: dict[str, Any], *, limit: int | None = None) -> list[dict[str, Any]]:
        query = self.vectorize(row)
        scored = [
            (self.cosine(query, vector), idx)
            for idx, vector in enumerate(self.train_vectors)
        ]
        scored.sort(reverse=True)
        limit = limit or self.k
        return [
            {
                "rank": rank,
                "similarity": round(score, 6),
                "label": self.train_labels[idx],
                "ref": self.train_refs[idx],
            }
            for rank, (score, idx) in enumerate(scored[:limit], start=1)
        ]

    def predict_proba(self, row: dict[str, Any], *, min_similarity: float = 0.0) -> dict[str, float]:
        neighbors = self.nearest_neighbors(row, limit=self.k)
        if not neighbors or neighbors[0]["similarity"] < min_similarity:
            return {label: 1.0 if label == "none" else 0.0 for label in self.labels}

        votes = {label: 0.0 for label in self.labels}
        for item in neighbors:
            votes[item["label"]] += max(float(item["similarity"]), 0.0)
        total = sum(votes.values())
        if total <= 0:
            return {label: 1.0 if label == "none" else 0.0 for label in self.labels}
        return {label: value / total for label, value in votes.items()}

    def predict(
        self,
        row: dict[str, Any],
        *,
        min_active_confidence: float = 0.0,
        min_similarity: float = 0.0,
    ) -> tuple[str, float, dict[str, float]]:
        probabilities = self.predict_proba(row, min_similarity=min_similarity)
        label = max(self.labels, key=lambda item: probabilities.get(item, 0.0))
        confidence = probabilities.get(label, 0.0)
        if label != "none" and confidence < min_active_confidence:
            label = "none"
            confidence = probabilities.get("none", 0.0)
        return label, confidence, probabilities

    def explain(self, row: dict[str, Any], label: str, *, limit: int = 12) -> list[dict[str, Any]]:
        query = self.vectorize(row)
        neighbors = self.nearest_neighbors(row, limit=1)
        if not neighbors:
            return []
        neighbor_index = neighbors[0]["rank"] - 1
        if neighbor_index >= len(self.train_vectors):
            return []
        # nearest_neighbors returns sorted ranks, so recover the actual nearest by recomputing the top index.
        scored = [(self.cosine(query, vector), idx) for idx, vector in enumerate(self.train_vectors)]
        scored.sort(reverse=True)
        _, idx = scored[0]
        neighbor = self.train_vectors[idx]
        inverse_vocab = {idx: term for term, idx in self.vocabulary.items()}
        overlaps: list[tuple[float, int]] = []
        for feat_idx, value in query.items():
            overlap = value * neighbor.get(feat_idx, 0.0)
            if overlap > 0:
                overlaps.append((overlap, feat_idx))
        overlaps.sort(reverse=True)
        return [
            {
                "feature": inverse_vocab.get(feat_idx, str(feat_idx)),
                "overlap": round(score, 6),
                "nearest_label": self.train_labels[idx],
                "nearest_ref": self.train_refs[idx],
            }
            for score, feat_idx in overlaps[:limit]
        ]

    def to_dict(self) -> dict[str, Any]:
        vectors = [
            [[idx, round(value, 8)] for idx, value in sorted(vector.items())]
            for vector in self.train_vectors
        ]
        return {
            "model_type": "pure_python_tfidf_knn",
            "labels": self.labels,
            "min_df": self.min_df,
            "max_features": self.max_features,
            "k": self.k,
            "max_count": self.max_count,
            "vocabulary": self.vocabulary,
            "idf": {str(idx): round(value, 8) for idx, value in self.idf.items()},
            "train_labels": self.train_labels,
            "train_refs": self.train_refs,
            "train_vectors": vectors,
            "total_docs": self.total_docs,
            "static_business_lexicon": False,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "TfidfKNNModel":
        model = cls(
            labels=list(payload.get("labels") or ALL_LABELS),
            min_df=int(payload.get("min_df", 1)),
            max_features=int(payload.get("max_features", 50000)),
            k=int(payload.get("k", 1)),
            max_count=int(payload.get("max_count", 8)),
        )
        model.vocabulary = {str(term): int(idx) for term, idx in payload.get("vocabulary", {}).items()}
        model.idf = {int(idx): float(value) for idx, value in payload.get("idf", {}).items()}
        model.train_labels = [str(label) for label in payload.get("train_labels", [])]
        model.train_refs = list(payload.get("train_refs", []))
        model.train_vectors = [
            {int(idx): float(value) for idx, value in vector}
            for vector in payload.get("train_vectors", [])
        ]
        model.total_docs = int(payload.get("total_docs", 0))
        return model

    def save(self, path: str | Path) -> None:
        out_path = Path(path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, ensure_ascii=False)

    @classmethod
    def load(cls, path: str | Path) -> "TfidfKNNModel":
        with Path(path).open("r", encoding="utf-8") as f:
            return cls.from_dict(json.load(f))
