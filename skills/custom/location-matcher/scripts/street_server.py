"""Client for the model service running on localhost:3130 via SSH tunnel.

Provides typed wrappers for all REST endpoints:
  - health()       → GET  /health
  - list_models()  → GET  /models
  - embed()        → POST /embed
  - rerank()       → POST /rerank
  - batch()        → POST /batch

Service Configuration:
  - URL: http://localhost:3130
  - Timeout: 120 seconds (configurable via MODEL_SERVICE_TIMEOUT env var)

Available Models:
  Embedding Models:
    - Qwen3-VL-Embedding-2B (2048 dims, supports instruction)
    - ImAge4VPR (6144 dims, no instruction support, resizes to 322×322)
  
  Reranker Models:
    - Qwen3-VL-Reranker-2B

Instruction Keys:
  - urban_governance: Urban governance issues (environment, traffic, safety, sanitation)
  - traffic_order: Traffic order (illegal parking, road obstruction)
  - safety_hazard: Safety hazards (damaged facilities, road surfaces, obstacles)
  - sanitation: Environmental sanitation (garbage, dumping, sewage)
  - street_appearance: Street appearance (unauthorized business, postings, ads)
  - municipal_facility: Municipal facilities (streetlights, signage, guardrails)

Usage Examples:
  # Health check
  from street_server import health
  status = health()
  
  # Text embedding
  from street_server import embed_text
  embeddings = embed_text(["text1", "text2"], instruction_key="urban_governance")
  
  # Image embedding
  from street_server import embed_image
  embeddings = embed_image("/path/to/image.jpg", model_name="ImAge4VPR")
"""

from __future__ import annotations

import base64
import mimetypes
import os
from typing import Any

import requests

_BASE_URL = os.getenv("MODEL_SERVICE_URL", "http://localhost:3130")
_DEFAULT_TIMEOUT = int(os.getenv("MODEL_SERVICE_TIMEOUT", "120"))
MultiModalItem = dict[str, Any]


_INSTRUCTION_MAP = {
    "urban_governance": "Represent this street-view image as a semantic embedding for urban governance retrieval, covering issues related to street environment, traffic order, public safety, environmental sanitation, and municipal infrastructure.",
    "traffic_order": "Represent this street-view image as a semantic embedding for traffic order issue retrieval, focusing on illegal parking, road obstruction, vehicles encroaching on sidewalks or bicycle lanes, and other phenomena disrupting road traffic flow.",
    "safety_hazard": "Represent this street-view image as a semantic embedding for safety hazard retrieval, focusing on risks that may cause personal injury or vehicle accidents, such as damaged facilities, abnormal road surfaces, obstacles, missing barriers, and exposed hazardous points.",
    "sanitation": "Represent this street-view image as a semantic embedding for environmental sanitation issue retrieval, focusing on exposed garbage, illegal dumping, sewage or stains, overflowing waste containers, and other phenomena affecting cleanliness.",
    "street_appearance": "Represent this street-view image as a semantic embedding for street appearance issue retrieval, focusing on unauthorized outdoor business operations, road encroachments, illegal postings or banners, unauthorized advertisements, and phenomena affecting street tidiness and order.",
    "municipal_facility": "Represent this street-view image as a semantic embedding for municipal facility issue retrieval, focusing on the absence, damage, tilting, or abnormal condition of public infrastructure such as streetlights, signage, guardrails, bus shelters, and manhole covers.",
}


def _url(path: str) -> str:
    return f"{_BASE_URL.rstrip('/')}/{path.lstrip('/')}"


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------


def health(timeout: int = _DEFAULT_TIMEOUT) -> dict[str, Any]:
    """Check service health and loaded models.

    Returns:
        {
            "status": "ok",
            "loaded_embed_models": [...],
            "loaded_rerank_models": [...],
        }

    Raises:
        requests.HTTPError: on non-2xx response.
    """
    resp = requests.get(_url("/health"), timeout=timeout)
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# GET /models
# ---------------------------------------------------------------------------


def list_models(timeout: int = _DEFAULT_TIMEOUT) -> list[dict[str, Any]]:
    """List all models supported by the service with their load status.

    Returns:
        List of model dicts with keys: name, kind, path, exists, loaded.

    Raises:
        requests.HTTPError: on non-2xx response.
    """
    resp = requests.get(_url("/models"), timeout=timeout)
    resp.raise_for_status()
    return resp.json().get("models", [])


# ---------------------------------------------------------------------------
# POST /embed
# ---------------------------------------------------------------------------


def embed(
    image_paths: list[str] | None = None,
    model_name: str = "ImAge4VPR",
    instruction: str | None = None,
    batch_size: int = 16,
    timeout: int = _DEFAULT_TIMEOUT,
    items: list[MultiModalItem] | None = None,
) -> list[list[float]]:
    """Extract L2-normalized embedding vectors for multimodal inputs.

    The first call may take tens of seconds while the model loads;
    subsequent calls reuse the already-loaded model.

    Args:
        image_paths:  Legacy image-only input field (optional).
        items:        Unified multimodal input field. Each item typically uses:
                      {"type": "image|text|video|document", "uri": "...", "content": "..."}.
                      This field has higher priority than image_paths on server side.
        model_name:   Model to use. Supported values:
                        - "Qwen3-VL-Embedding-2B" (dim=2048, supports instruction)
                        - "ImAge4VPR" (dim=6144, instruction ignored, resizes to 322×322)
        instruction:  Optional task-description prefix (ignored by ImAge4VPR).
        batch_size:   Internal inference batch size (1–128).
        timeout:      HTTP request timeout in seconds.

    Returns:
        List of float32 vectors, shape [N, D].

    Raises:
        requests.HTTPError: on non-2xx response.
        ValueError: if neither items nor image_paths is provided.
    """
    if not items and not image_paths:
        raise ValueError("Either items or image_paths must be provided")

    payload: dict[str, Any] = {
        "model_name": model_name,
        "batch_size": batch_size,
    }
    if items:
        payload["items"] = items
    if image_paths:
        payload["image_paths"] = image_paths
    if instruction is not None:
        payload["instruction"] = instruction

    resp = requests.post(_url("/embed"), json=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()["embeddings"]


def embed_image(
    image_path: str,
    model_name: str = "Qwen3-VL-Embedding-2B",
    instruction_key: str | None = None,
    timeout: int = _DEFAULT_TIMEOUT,
) -> list[list[float]]:
    """Convenience wrapper: embed a single image with an instruction key.

    Args:
        image_path:      Server-local absolute path to the image file.
        model_name:      Model to use (see embed() for supported values).
        instruction_key: Optional key to look up in _INSTRUCTION_MAP for the instruction text.
        timeout:         HTTP request timeout in seconds.

    Returns:
        A single float32 vector of length D.
    """
    instruction = _INSTRUCTION_MAP.get(instruction_key) if instruction_key else None
    
    # Read image file and encode to base64
    with open(image_path, "rb") as f:
        image_data = f.read()
    image_base64 = base64.b64encode(image_data).decode("utf-8")
    
    # Infer MIME type from file extension
    mime_type, _ = mimetypes.guess_type(image_path)
    if mime_type is None:
        mime_type = "image/jpeg"  # Default to JPEG if unknown
    
    data_url = f"data:{mime_type};base64,{image_base64}"
    items = [{"type": "image_base64", "data": data_url, "encoding": "base64", "media_type": mime_type}]
    
    embeddings = embed(items=items, 
                       model_name=model_name, 
                       instruction=instruction, 
                       timeout=timeout)
    return embeddings 
    
    
def embed_text(
    texts: list[str],
    model_name: str = "Qwen3-VL-Embedding-2B",
    instruction_key: str | None = None,
    timeout: int = _DEFAULT_TIMEOUT,
) -> list[list[float]]:
    """Convenience wrapper: embed multiple images with an instruction key.

    Args:
        texts:      List of text strings to embed.
        model_name:      Model to use (see embed() for supported values).
        instruction_key: Optional key to look up in _INSTRUCTION_MAP for the instruction text.
        timeout:         HTTP request timeout in seconds.

    Returns:
        List of float32 vectors, shape [N, D].
    """
    instruction = _INSTRUCTION_MAP.get(instruction_key) if instruction_key else None
    items = [{"type": "text", "content": text} for text in texts]
    embeddings = embed(items=items, 
                       model_name=model_name, 
                       instruction=instruction, 
                       timeout=timeout)
    return embeddings


# ---------------------------------------------------------------------------
# POST /rerank
# ---------------------------------------------------------------------------


def rerank(
    query_images: list[str] | None = None,
    candidate_images: list[str] | None = None,
    model_name: str = "Qwen3-VL-Reranker-2B",
    instruction: str | None = None,
    timeout: int = _DEFAULT_TIMEOUT,
    query_items: list[MultiModalItem] | None = None,
    candidate_items: list[MultiModalItem] | None = None,
) -> list[list[float]]:
    """Compute relevance scores between multimodal queries and candidates.

    Scores are Sigmoid-normalized to [0, 1]; higher means more similar.

    Args:
        query_items:       Unified multimodal query inputs (preferred).
        candidate_items:   Unified multimodal candidate inputs (preferred).
        query_images:      Legacy image-only query field (optional).
        candidate_images:  Legacy image-only candidate field (optional).
        model_name:        Reranker model name (currently only "Qwen3-VL-Reranker-2B").
        instruction:       Optional task-description prefix.
        timeout:           HTTP request timeout in seconds.

    Returns:
        scores[i][j]: relevance of query i vs. candidate j, shape [Q, C].

    Raises:
        requests.HTTPError: on non-2xx response.
        ValueError: if query side or candidate side input is missing.
    """
    if not query_items and not query_images:
        raise ValueError("Either query_items or query_images must be provided")
    if not candidate_items and not candidate_images:
        raise ValueError("Either candidate_items or candidate_images must be provided")

    payload: dict[str, Any] = {"model_name": model_name}
    if query_items:
        payload["query_items"] = query_items
    if candidate_items:
        payload["candidate_items"] = candidate_items
    if query_images:
        payload["query_images"] = query_images
    if candidate_images:
        payload["candidate_images"] = candidate_images
    if instruction is not None:
        payload["instruction"] = instruction

    resp = requests.post(_url("/rerank"), json=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()["scores"]

def rerank_text_image(
    query_texts: list[str],
    candidate_images: list[str],
    model_name: str = "Qwen3-VL-Reranker-2B",
    instruction_key: str | None = None,
    timeout: int = _DEFAULT_TIMEOUT,
) -> list[list[float]]:
    """Convenience wrapper: rerank text queries against image candidates with an instruction key."""
    instruction = _INSTRUCTION_MAP.get(instruction_key) if instruction_key else None
    query_items = [{"type": "text", "content": text} for text in query_texts]
    candidate_items = [{"type": "image", "uri": image_path} for image_path in candidate_images]
    scores = rerank(query_items=query_items, 
                    candidate_items=candidate_items, 
                    model_name=model_name, 
                    instruction=instruction, 
                    timeout=timeout)
    return scores

# ---------------------------------------------------------------------------
# POST /batch
# ---------------------------------------------------------------------------


def batch(
    jsonl_path: str,
    model_name: str,
    instruction: str | None = None,
    output_dir: str | None = None,
    batch_size: int = 16,
    image_key: str = "image_path",
    id_key: str = "id",
    timeout: int = 3600,
) -> dict[str, Any]:
    """Batch-embed images listed in a server-local JSONL file.

    This is a synchronous blocking call; the server processes the entire file
    before responding.  Set a generous timeout for large datasets.

    Input JSONL format (one JSON object per line):
        {"id": "img_001", "image_path": "/data/images/001.jpg"}

    Output JSONL written by the server:
        Line 0: {"meta": {"model_name": ..., "instruction": ...}}
        Line N: {"id": "img_001", "embedding_vector": [...]}

    Args:
        jsonl_path:  Server-local absolute path to the input JSONL file.
        model_name:  Model name (see embed() for supported values).
        instruction: Optional task-description prefix.
        output_dir:  Server-local directory for the output file (uses
                     service default when None).
        batch_size:  Internal inference batch size (1–128).
        image_key:   Field name for image paths in each JSONL record.
        id_key:      Field name for sample IDs in each JSONL record.
        timeout:     HTTP request timeout in seconds (default 3600 for large jobs).

    Returns:
        {
            "output_path": "<server-local path to output JSONL>",
            "processed_count": <number of images processed>,
        }

    Raises:
        requests.HTTPError: on non-2xx response.
    """
    payload: dict[str, Any] = {
        "jsonl_path": jsonl_path,
        "model_name": model_name,
        "batch_size": batch_size,
        "image_key": image_key,
        "id_key": id_key,
    }
    if instruction is not None:
        payload["instruction"] = instruction
    if output_dir is not None:
        payload["output_dir"] = output_dir

    resp = requests.post(_url("/batch"), json=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


