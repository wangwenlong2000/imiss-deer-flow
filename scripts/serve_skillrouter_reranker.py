#!/usr/bin/env python3
"""Local SkillRouter reranker service with a simple HTTP API."""

import os
from pathlib import Path
from typing import Any

import torch
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from transformers import AutoModelForCausalLM, AutoTokenizer

REPO_ROOT = Path(__file__).resolve().parents[1]

MODEL_ID = os.environ.get(
    "SKILLROUTER_RERANKER_MODEL_ID",
    "pipizhao/SkillRouter-Reranker-0.6B",
)
MODEL_PATH = os.environ.get(
    "SKILLROUTER_RERANKER_MODEL_PATH",
    str(REPO_ROOT / ".models" / "skillrouter-reranker-0.6b"),
)
PORT = int(os.environ.get("SKILLROUTER_RERANKER_PORT", "7801"))
HOST = os.environ.get("SKILLROUTER_RERANKER_HOST", "0.0.0.0")
MAX_LENGTH = int(os.environ.get("SKILLROUTER_RERANKER_MAX_LENGTH", "4096"))

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
DTYPE = torch.bfloat16 if torch.cuda.is_available() else torch.float32

app = FastAPI(title="SkillRouter Reranker Service")
tokenizer = AutoTokenizer.from_pretrained(
    MODEL_PATH,
    padding_side="left",
)
if tokenizer.pad_token is None:
    tokenizer.pad_token = tokenizer.eos_token or tokenizer.unk_token

model = AutoModelForCausalLM.from_pretrained(
    MODEL_PATH,
    torch_dtype=DTYPE,
)
model = model.eval().to(DEVICE)

TOKEN_YES = tokenizer.convert_tokens_to_ids("yes")
TOKEN_NO = tokenizer.convert_tokens_to_ids("no")


class RerankInput(BaseModel):
    model: str = MODEL_ID
    query: str
    documents: list[Any]
    top_n: int | None = None


class RerankResult(BaseModel):
    index: int
    relevance_score: float
    document: str


class RerankResponse(BaseModel):
    object: str = "list"
    model: str = MODEL_ID
    results: list[RerankResult]
    usage: dict[str, int]


def format_document(document: Any) -> str:
    if isinstance(document, str):
        return document
    if isinstance(document, dict):
        name = str(document.get("name", document.get("title", ""))).strip()
        description = str(
            document.get("desc", document.get("description", ""))
        ).strip()
        body = str(document.get("body", document.get("text", ""))).strip()
        parts = [part for part in [name, description, body] if part]
        return " | ".join(parts)
    return str(document)


def format_prompt(query_text: str, document_text: str) -> str:
    instruction = (
        "Given a task description, judge whether the skill document is "
        "relevant and useful for completing the task"
    )
    return (
        f"<Instruct>: {instruction}\n\n"
        f"<Query>: {query_text}\n\n"
        f"<Document>: {document_text}"
    )


def build_inputs(prompts: list[str]) -> dict[str, torch.Tensor]:
    prefix = (
        '<|im_start|>system\nJudge whether the Document meets the requirements '
        'based on the Query and the Instruct provided. Note that the answer can '
        'only be "yes" or "no".<|im_end|>\n<|im_start|>user\n'
    )
    suffix = '<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n'
    prefix_tokens = tokenizer.encode(prefix, add_special_tokens=False)
    suffix_tokens = tokenizer.encode(suffix, add_special_tokens=False)
    prompt_limit = max(1, MAX_LENGTH - len(prefix_tokens) - len(suffix_tokens))
    prompt_tokens = tokenizer(
        prompts,
        padding=False,
        truncation=True,
        max_length=prompt_limit,
        return_attention_mask=False,
    )["input_ids"]
    sequences = [prefix_tokens + tokens + suffix_tokens for tokens in prompt_tokens]
    return tokenizer.pad(
        {"input_ids": sequences},
        padding=True,
        return_attention_mask=True,
        return_tensors="pt",
    )


def score_documents(query_text: str, documents: list[Any]) -> list[float]:
    if not documents:
        return []
    prompts = [format_prompt(query_text, format_document(document)) for document in documents]
    encoded = build_inputs(prompts)
    input_ids = encoded["input_ids"].to(DEVICE)
    attention_mask = encoded["attention_mask"].to(DEVICE)
    with torch.no_grad():
        outputs = model(input_ids=input_ids, attention_mask=attention_mask)
        last_positions = attention_mask.sum(dim=1) - 1
        batch_indices = torch.arange(input_ids.shape[0], device=DEVICE)
        last_logits = outputs.logits[batch_indices, last_positions]
    return (last_logits[:, TOKEN_YES] - last_logits[:, TOKEN_NO]).tolist()


@app.post("/v1/rerank")
async def rerank(req: RerankInput):
    if req.top_n is not None and req.top_n < 1:
        raise HTTPException(status_code=400, detail="top_n must be >= 1")
    scores = score_documents(req.query, req.documents)
    ranked = sorted(enumerate(scores), key=lambda item: item[1], reverse=True)
    if req.top_n is not None:
        ranked = ranked[: req.top_n]
    return RerankResponse(
        results=[
            {
                "index": index,
                "relevance_score": float(score),
                "document": format_document(req.documents[index]),
            }
            for index, score in ranked
        ],
        usage={"prompt_tokens": len(req.documents), "total_tokens": len(req.documents)},
    )


@app.get("/health")
async def health():
    return {"status": "ok", "model": MODEL_PATH}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=HOST, port=PORT)