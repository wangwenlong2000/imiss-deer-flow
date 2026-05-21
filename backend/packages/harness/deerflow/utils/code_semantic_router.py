"""Semantic intent routing for low-level Python code snippets.

Installation for the embedding fallback:

    pip install sentence-transformers

The :class:`CodeSemanticRouter` keeps rule matching lightweight and loads the
SentenceTransformer model once per model name across router instances.
"""

from __future__ import annotations

import ast
import math
import re
import threading
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import Any, TypedDict


class CodeSemanticResult(TypedDict):
    intent: str
    tags: list[str]


@dataclass(frozen=True)
class _FeatureRule:
    name: str
    intent: str
    tags: tuple[str, ...]
    import_names: tuple[str, ...] = ()
    patterns: tuple[str, ...] = ()


@dataclass(frozen=True)
class _IntentPrototype:
    intent: str
    tags: tuple[str, ...]
    example: str


class CodeSemanticRouter:
    """Extract intent and metadata tags from Python source code.

    The router applies three levels:
    1. in-memory feature rules;
    2. embedding similarity against semantic prototypes;
    3. an ``unknown`` result if the optional embedding dependency is missing.

    ``llm_generator`` can be wired to a local/open-source LLM. It receives the
    original code and nearest prototype, then must return ``{"intent": ..., "tags": [...]}``.
    Without it, the router deterministically renders an intent/tags result from
    the retrieved prototype.
    """

    _model_lock = threading.Lock()
    _models: dict[str, Any] = {}
    _prototype_embeddings: dict[str, list[Any]] = {}

    def __init__(
        self,
        model_name: str = "all-MiniLM-L6-v2",
        *,
        enable_embedding: bool = True,
        llm_generator: Callable[[str, CodeSemanticResult], CodeSemanticResult] | None = None,
        model_factory: Callable[[str], Any] | None = None,
    ) -> None:
        self.model_name = model_name
        self.feature_rules = self._build_feature_dictionary()
        self.prototypes = self._build_intent_prototypes()
        self.llm_generator = llm_generator
        self._model: Any | None = None

        if enable_embedding:
            self._model = self._load_model(model_name, model_factory)
            self._ensure_prototype_embeddings()

    def route(self, code: str) -> CodeSemanticResult:
        """Return a dictionary with ``intent`` and ``tags`` for a Python code string."""

        source = code.strip()
        if not source:
            return {"intent": "unknown", "tags": []}

        rule_result = self._route_by_rules(source)
        if rule_result is not None:
            return rule_result

        embedding_result = self._route_by_embedding(source)
        if embedding_result is not None:
            return embedding_result

        return {"intent": "unknown", "tags": []}

    def _route_by_rules(self, code: str) -> CodeSemanticResult | None:
        imports = self._extract_import_roots(code)
        matched_rules: list[_FeatureRule] = []

        for rule in self.feature_rules:
            if any(name in imports for name in rule.import_names):
                matched_rules.append(rule)
                continue
            if any(re.search(pattern, code, flags=re.IGNORECASE) for pattern in rule.patterns):
                matched_rules.append(rule)

        if not matched_rules:
            return None

        names = {rule.name for rule in matched_rules}
        tags = self._unique(tag for rule in matched_rules for tag in rule.tags)

        if "http" in names and "json" in names:
            return {"intent": "HTTP + JSON fetch", "tags": self._unique([*tags, "api"])}
        if "http" in names:
            return {"intent": "HTTP/Network request", "tags": tags}
        if "dataframe" in names:
            return {"intent": "Data Processing", "tags": tags}
        if "vision" in names:
            return {"intent": "Vision / Image Processing", "tags": tags}
        if "hardware" in names:
            return {"intent": "Hardware I/O control", "tags": tags}

        return {
            "intent": " + ".join(rule.intent for rule in matched_rules[:2]),
            "tags": tags,
        }

    def _route_by_embedding(self, code: str) -> CodeSemanticResult | None:
        if self._model is None:
            return None

        query_embedding = self._model.encode(code)
        prototype_embeddings = self._prototype_embeddings.get(self.model_name, [])
        if not prototype_embeddings:
            return None

        best_index = max(
            range(len(prototype_embeddings)),
            key=lambda index: self._cosine_similarity(query_embedding, prototype_embeddings[index]),
        )
        prototype = self.prototypes[best_index]
        candidate: CodeSemanticResult = {
            "intent": prototype.intent,
            "tags": list(prototype.tags),
        }

        if self.llm_generator is not None:
            return self._normalize_result(self.llm_generator(code, candidate))

        return candidate

    def _ensure_prototype_embeddings(self) -> None:
        if self.model_name in self._prototype_embeddings:
            return
        if self._model is None:
            return

        with self._model_lock:
            if self.model_name not in self._prototype_embeddings:
                examples = [prototype.example for prototype in self.prototypes]
                self._prototype_embeddings[self.model_name] = list(self._model.encode(examples))

    @classmethod
    def _load_model(cls, model_name: str, model_factory: Callable[[str], Any] | None) -> Any | None:
        if model_name in cls._models:
            return cls._models[model_name]

        with cls._model_lock:
            if model_name in cls._models:
                return cls._models[model_name]

            try:
                factory = model_factory
                if factory is None:
                    from sentence_transformers import SentenceTransformer

                    factory = SentenceTransformer
                model = factory(model_name)
            except Exception:
                return None

            cls._models[model_name] = model
            return model

    @staticmethod
    def _build_feature_dictionary() -> list[_FeatureRule]:
        return [
            _FeatureRule(
                name="http",
                intent="HTTP/Network request",
                tags=("network", "http", "api"),
                import_names=("requests", "httpx", "aiohttp", "urllib"),
                patterns=(
                    r"\brequests\.",
                    r"\bhttpx\.",
                    r"\baiohttp\.",
                    r"\burllib\.",
                    r"\burlopen\(",
                    r"\bget\(.+https?://",
                    r"\bpost\(.+https?://",
                ),
            ),
            _FeatureRule(
                name="json",
                intent="JSON parsing/serialization",
                tags=("json",),
                import_names=("json",),
                patterns=(r"\.json\(", r"\bjson\.", r"application/json"),
            ),
            _FeatureRule(
                name="dataframe",
                intent="Data Processing",
                tags=("data", "dataframe", "etl"),
                import_names=("pandas", "numpy", "polars", "duckdb"),
                patterns=(r"\bpd\.", r"\bDataFrame\(", r"\bread_csv\(", r"\bnp\."),
            ),
            _FeatureRule(
                name="vision",
                intent="Vision / Image Processing",
                tags=("vision", "image", "opencv"),
                import_names=("cv2", "PIL", "skimage"),
                patterns=(r"\bcv2\.", r"\bImage\.open\(", r"\bimread\(", r"\bVideoCapture\("),
            ),
            _FeatureRule(
                name="hardware",
                intent="Hardware I/O control",
                tags=("hardware", "io", "device"),
                import_names=("serial", "RPi", "gpiozero", "smbus", "pyvisa"),
                patterns=(r"\bGPIO\.", r"\bSerial\(", r"\bsmbus\.", r"\bpyvisa\."),
            ),
        ]

    @staticmethod
    def _build_intent_prototypes() -> list[_IntentPrototype]:
        return [
            _IntentPrototype(
                intent="HTTP + JSON fetch",
                tags=("network", "json", "api"),
                example="response = client.get(url); payload = response.json()",
            ),
            _IntentPrototype(
                intent="Tabular data transformation",
                tags=("data", "dataframe", "etl"),
                example="table = read rows, filter columns, group records, aggregate metrics",
            ),
            _IntentPrototype(
                intent="Vision frame processing",
                tags=("vision", "image", "opencv"),
                example="load image frame, resize, threshold pixels, detect contours",
            ),
            _IntentPrototype(
                intent="Hardware I/O control",
                tags=("hardware", "io", "device"),
                example="open serial port, write command bytes, read sensor response",
            ),
            _IntentPrototype(
                intent="File parsing and serialization",
                tags=("file", "parse", "serialization"),
                example="open local file, parse text content, write structured output",
            ),
        ]

    @staticmethod
    def _extract_import_roots(code: str) -> set[str]:
        try:
            tree = ast.parse(code)
        except SyntaxError:
            return set()

        roots: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                roots.update(alias.name.split(".", maxsplit=1)[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                roots.add(node.module.split(".", maxsplit=1)[0])
        return roots

    @staticmethod
    def _normalize_result(result: CodeSemanticResult) -> CodeSemanticResult:
        intent = str(result.get("intent") or "unknown")
        tags = result.get("tags") or []
        return {"intent": intent, "tags": CodeSemanticRouter._unique(str(tag) for tag in tags)}

    @staticmethod
    def _cosine_similarity(left: Any, right: Any) -> float:
        left_values = CodeSemanticRouter._as_float_list(left)
        right_values = CodeSemanticRouter._as_float_list(right)
        if not left_values or not right_values or len(left_values) != len(right_values):
            return -1.0

        dot = sum(a * b for a, b in zip(left_values, right_values, strict=True))
        left_norm = math.sqrt(sum(value * value for value in left_values))
        right_norm = math.sqrt(sum(value * value for value in right_values))
        if left_norm == 0 or right_norm == 0:
            return -1.0
        return dot / (left_norm * right_norm)

    @staticmethod
    def _as_float_list(value: Any) -> list[float]:
        if hasattr(value, "tolist"):
            value = value.tolist()
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            return [float(item) for item in value]
        return []

    @staticmethod
    def _unique(values: Iterable[str]) -> list[str]:
        seen: set[str] = set()
        result: list[str] = []
        for value in values:
            if value not in seen:
                seen.add(value)
                result.append(value)
        return result
