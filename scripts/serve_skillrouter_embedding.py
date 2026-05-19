#!/usr/bin/env python3
"""Local SkillRouter embedding service with OpenAI-compatible API."""

import os
from pathlib import Path

import torch
import torch.nn.functional as F
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from transformers import AutoModel, AutoTokenizer

REPO_ROOT = Path(__file__).resolve().parents[1]

MODEL_ID = os.environ.get(
    "SKILLROUTER_EMBEDDING_MODEL_ID",
    "pipizhao/SkillRouter-Embedding-0.6B",
)
MODEL_PATH = os.environ.get(
    "SKILLROUTER_EMBEDDING_MODEL_PATH",
    str(REPO_ROOT / ".models" / "skillrouter-embedding-0.6b"),
)
PORT = int(os.environ.get("SKILLROUTER_EMBEDDING_PORT", "7800"))
HOST = os.environ.get("SKILLROUTER_EMBEDDING_HOST", "0.0.0.0")
MAX_LENGTH = int(os.environ.get("SKILLROUTER_EMBEDDING_MAX_LENGTH", "4096"))
QUERY_INSTRUCTION = (
    "Instruct: Given a task description, retrieve the most relevant "
    "skill document that would help an agent complete the task\nQuery:"
)

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
DTYPE = torch.bfloat16 if torch.cuda.is_available() else torch.float32

app = FastAPI(title="SkillRouter Embedding Service")
tokenizer = AutoTokenizer.from_pretrained(
    MODEL_PATH,
    trust_remote_code=True,
    padding_side="left",
)
if tokenizer.pad_token is None:
    tokenizer.pad_token = tokenizer.eos_token or tokenizer.unk_token

model = AutoModel.from_pretrained(
    MODEL_PATH,
    trust_remote_code=True,
    torch_dtype=DTYPE,
)
model = model.eval().to(DEVICE)


class EmbeddingInput(BaseModel):
    model: str = MODEL_ID
    input: str | list[str]
    mode: str = "document"


class EmbeddingData(BaseModel):
    object: str = "embedding"
    embedding: list[float]
    index: int


class EmbeddingResponse(BaseModel):
    object: str = "list"
    data: list[EmbeddingData]
    model: str = MODEL_ID
    usage: dict[str, int]


def last_token_pool(last_hidden_states: torch.Tensor, attention_mask: torch.Tensor) -> torch.Tensor:
    left_padding = attention_mask[:, -1].sum() == attention_mask.shape[0]
    if left_padding:
        return last_hidden_states[:, -1]
    sequence_lengths = attention_mask.sum(dim=1) - 1
    batch = last_hidden_states.shape[0]
    return last_hidden_states[torch.arange(batch, device=last_hidden_states.device), sequence_lengths]


def prepare_texts(texts: list[str], mode: str) -> list[str]:
    if mode == "document":
        return texts
    if mode == "query":
        return [f"{QUERY_INSTRUCTION} {text}" for text in texts]
    raise HTTPException(status_code=400, detail="mode must be 'document' or 'query'")


def encode(texts: list[str]) -> torch.Tensor:
    encoded = tokenizer(
        texts,
        padding=True,
        truncation=True,
        max_length=MAX_LENGTH,
        return_tensors="pt",
    )
    encoded = {key: value.to(DEVICE) for key, value in encoded.items()}
    with torch.no_grad():
        outputs = model(**encoded)
        embeddings = last_token_pool(outputs.last_hidden_state, encoded["attention_mask"])
        embeddings = F.normalize(embeddings, p=2, dim=1)
    return embeddings


@app.post("/v1/embeddings")
async def embeddings(req: EmbeddingInput):
    texts = [req.input] if isinstance(req.input, str) else req.input
    prepared_texts = prepare_texts(texts, req.mode)
    vectors = encode(prepared_texts)
    return EmbeddingResponse(
        data=[
            {"object": "embedding", "embedding": vector.tolist(), "index": index}
            for index, vector in enumerate(vectors)
        ],
        usage={"prompt_tokens": len(texts), "total_tokens": len(texts)},
    )


@app.get("/health")
async def health():
    return {"status": "ok", "model": MODEL_PATH}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=HOST, port=PORT)