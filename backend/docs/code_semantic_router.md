# CodeSemanticRouter

`CodeSemanticRouter` extracts an intent and metadata tags from Python code snippets.

## Install

```bash
pip install sentence-transformers
```

The default embedding model is `all-MiniLM-L6-v2`, loaded through:

```python
from sentence_transformers import SentenceTransformer
```

The model is initialized in `CodeSemanticRouter.__init__` and cached as a class-level singleton per model name, so multiple router instances reuse the same loaded Transformer.

## Usage

```python
from deerflow.utils.code_semantic_router import CodeSemanticRouter

router = CodeSemanticRouter()

result = router.route("""
import requests

payload = requests.get(url).json()
""")

assert result == {
    "intent": "HTTP + JSON fetch",
    "tags": ["network", "http", "api", "json"],
}
```

## Fallback Strategy

1. Rule-based engine: matches a lightweight internal feature dictionary for libraries and call patterns such as `requests`, `pandas`, `cv2`, and hardware I/O packages.
2. Embedding fallback: when rules miss, embeds the code snippet with `sentence-transformers` and retrieves the nearest semantic prototype.
3. Optional local LLM renderer: pass `llm_generator` to turn the retrieved prototype into a custom `{"intent": ..., "tags": [...]}` payload using a local/open-source model.
4. Unknown fallback: returns `{"intent": "unknown", "tags": []}` if neither rules nor embeddings are available.
