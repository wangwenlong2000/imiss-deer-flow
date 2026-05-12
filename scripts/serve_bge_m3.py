#!/usr/bin/env python3
"""Local BGE-M3 embedding service with OpenAI-compatible API."""

import os
from sentence_transformers import SentenceTransformer
from fastapi import FastAPI
from pydantic import BaseModel

MODEL_PATH = os.environ.get(
    "BGE_M3_MODEL_PATH",
    "/home/wwl/imiss-deer-flow-main/.models/bge-m3",
)
PORT = int(os.environ.get("BGE_M3_PORT", "7799"))
HOST = os.environ.get("BGE_M3_HOST", "0.0.0.0")

app = FastAPI(title="BGE-M3 Embedding Service")
model = SentenceTransformer(MODEL_PATH)


class EmbeddingInput(BaseModel):
    model: str = "BAAI/bge-m3"
    input: str | list[str]


class EmbeddingResponse(BaseModel):
    object: str = "list"
    data: list[dict]
    model: str = "BAAI/bge-m3"
    usage: dict


@app.post("/v1/embeddings")
async def embeddings(req: EmbeddingInput):
    texts = [req.input] if isinstance(req.input, str) else req.input
    vectors = model.encode(texts, normalize_embeddings=True, show_progress_bar=False)
    return EmbeddingResponse(
        data=[
            {"object": "embedding", "embedding": v.tolist(), "index": i}
            for i, v in enumerate(vectors)
        ],
        usage={"prompt_tokens": len(texts), "total_tokens": len(texts)},
    )


@app.get("/health")
async def health():
    return {"status": "ok", "model": MODEL_PATH}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=HOST, port=PORT)
