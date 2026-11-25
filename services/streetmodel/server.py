"""Local implementation of the street model service's existing REST contract."""
from __future__ import annotations

import argparse
import base64
import binascii
import importlib.util
import io
import json
import logging
import os
from pathlib import Path
import sys
import threading
from typing import Literal
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from PIL import Image, UnidentifiedImageError
from pydantic import BaseModel, Field

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / '.runtime/streetmodel'
MODEL_ROOT = ROOT / 'models/streetmodel'
IMAGE_MODEL = 'ImAge4VPR'
EMBED_MODEL = 'Qwen3-VL-Embedding-2B'
RERANK_MODEL = 'Qwen3-VL-Reranker-2B'
LOG = logging.getLogger('streetmodel')
DATA_ROOTS = [ROOT / 'datasets', ROOT / '多场景测试', RUNTIME / 'io',
              ROOT / 'docs/unverified-scenarios-eval-20261028/fixtures',
              Path('/mnt/nas/streetview_meta')]
DATA_ROOTS += [Path(p).resolve() for p in os.environ.get('STREET_DATA_ROOTS', '').split(os.pathsep) if p]


class Item(BaseModel):
    type: Literal['text', 'image', 'image_base64', 'video', 'document']
    uri: str | None = None
    content: str | None = None
    data: str | None = Field(default=None, max_length=64 * 1024 * 1024)
    encoding: str | None = None
    media_type: str | None = None


class EmbedRequest(BaseModel):
    model_name: str = IMAGE_MODEL
    items: list[Item] | None = Field(default=None, max_length=128)
    image_paths: list[str] | None = Field(default=None, max_length=128)
    instruction: str | None = None
    batch_size: int = Field(default=16, ge=1, le=128)


class RerankRequest(BaseModel):
    model_name: str = RERANK_MODEL
    query_items: list[Item] | None = Field(default=None, max_length=128)
    candidate_items: list[Item] | None = Field(default=None, max_length=128)
    query_images: list[str] | None = Field(default=None, max_length=128)
    candidate_images: list[str] | None = Field(default=None, max_length=128)
    instruction: str | None = None


class BatchRequest(BaseModel):
    jsonl_path: str
    model_name: str
    instruction: str | None = None
    output_dir: str | None = None
    batch_size: int = Field(default=16, ge=1, le=128)
    image_key: str = 'image_path'
    id_key: str = 'id'


def local_file(raw: str) -> Path:
    try:
        path = Path(raw.removeprefix('file://')).resolve()
    except OSError as exc:
        raise HTTPException(400, 'Local input filesystem is unavailable') from exc
    if not any(path.is_relative_to(root.resolve()) for root in DATA_ROOTS):
        raise HTTPException(403, 'File must be within a configured street data root')
    if not path.is_file():
        raise HTTPException(400, f'Local input file is unavailable: {path}')
    return path


def parse_item(item: Item) -> dict:
    if item.type in {'image', 'image_base64'}:
        try:
            if item.data:
                encoded = item.data.split(',', 1)[1] if item.data.startswith('data:') else item.data
                image = Image.open(io.BytesIO(base64.b64decode(encoded, validate=True)))
            elif item.type == 'image' and item.uri:
                image = Image.open(local_file(item.uri))
            else:
                raise HTTPException(400, 'An image requires inline data or a local image URI')
            image.load()
            return {'image': image.convert('RGB')}
        except (binascii.Error, ValueError, UnidentifiedImageError, OSError) as exc:
            raise HTTPException(400, 'Invalid or unreadable image input') from exc
    if item.type == 'text':
        if not item.content:
            raise HTTPException(400, 'Text input requires content')
        return {'text': item.content}
    if not item.uri:
        raise HTTPException(400, f'{item.type} input requires a local URI')
    path = local_file(item.uri)
    if item.type == 'document':
        if path.suffix.lower() not in {'.txt', '.md', '.csv', '.json', '.jsonl'}:
            raise HTTPException(400, 'Document input supports UTF-8 text files only')
        try:
            return {'text': path.read_text(encoding='utf-8')}
        except UnicodeError as exc:
            raise HTTPException(400, 'Document is not UTF-8 text') from exc
    return {'video': str(path)}


def inputs(items: list[Item] | None, paths: list[str] | None) -> list[dict]:
    selected = items or [Item(type='image', uri=path) for path in paths or []]
    if not selected:
        raise HTTPException(400, 'Provide nonempty items or image paths')
    return [parse_item(item) for item in selected]


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class ModelRegistry:
    def __init__(self):
        self.lock = threading.RLock()
        self.loaded = {}
        self.checkpoint = os.environ.get('STREET_IMAGE_CHECKPOINT', 'ImAge_Merged.pth')
        if self.checkpoint not in {'ImAge_Merged.pth', 'ImAge_GSV.pth'}:
            raise ValueError('Unsupported ImAge checkpoint')

    def path(self, name):
        return MODEL_ROOT / name

    def get(self, name):
        if name not in {IMAGE_MODEL, EMBED_MODEL, RERANK_MODEL}:
            raise HTTPException(400, f'Unknown model: {name}')
        import torch
        if name in self.loaded:
            return self.loaded[name]
        if not torch.cuda.is_available():
            raise HTTPException(503, 'CUDA device is unavailable')
        LOG.info('Loading %s from local weights', name)
        if name == IMAGE_MODEL:
            sys.path.insert(0, str(RUNTIME / 'ImAge'))
            from hubconf import SimpleArgs
            from network import VPRmodel
            model = VPRmodel(SimpleArgs())
            import numpy as np
            # Upstream training checkpoints include NumPy RNG/scalar metadata.
            # Keep the restricted loader and allow only the required NumPy types.
            safe = [np.core.multiarray._reconstruct, np.core.multiarray.scalar, np.ndarray, np.dtype,
                    type(np.dtype('float32')), type(np.dtype('float64')),
                    type(np.dtype('uint32')), type(np.dtype('int64'))]
            with torch.serialization.safe_globals(safe):
                checkpoint = torch.load(self.path(name) / self.checkpoint, map_location='cpu', weights_only=True)
            state = checkpoint['model_state_dict']
            model.load_state_dict({key.removeprefix('module.'): value for key, value in state.items()}, strict=True)
            model = model.eval().cuda()
        else:
            file = 'qwen3_vl_embedding.py' if name == EMBED_MODEL else 'qwen3_vl_reranker.py'
            source = RUNTIME / 'Qwen3-VL-Embedding/src/models' / file
            module = load_module('_street_' + file.removesuffix('.py'), source)
            cls = module.Qwen3VLEmbedder if name == EMBED_MODEL else module.Qwen3VLReranker
            model = cls(str(self.path(name)), torch_dtype=torch.bfloat16,
                        attn_implementation='sdpa', local_files_only=True)
        self.loaded[name] = model
        return model

    def embed(self, name: str, records: list[dict], instruction: str | None, batch_size: int):
        if name not in {IMAGE_MODEL, EMBED_MODEL}:
            raise HTTPException(400, f'Model does not support embedding: {name}')
        if name == IMAGE_MODEL and any(set(record) != {'image'} for record in records):
            raise HTTPException(400, 'ImAge4VPR supports image inputs only')
        import torch
        from torchvision import transforms
        vectors = []
        with self.lock, torch.inference_mode():
            model = self.get(name)
            for start in range(0, len(records), batch_size):
                chunk = records[start:start + batch_size]
                if name == IMAGE_MODEL:
                    transform = transforms.Compose([
                        transforms.Resize((322, 322), interpolation=transforms.InterpolationMode.BILINEAR),
                        transforms.ToTensor(),
                        transforms.Normalize([.485, .456, .406], [.229, .224, .225]),
                    ])
                    batch = torch.stack([transform(record['image']) for record in chunk]).cuda()
                    output = model(batch)
                else:
                    prepared = [{**record, **({'instruction': instruction} if instruction else {})} for record in chunk]
                    output = model.process(prepared, normalize=True)
                vectors.extend(torch.nn.functional.normalize(output.float(), dim=-1).cpu().tolist())
        return vectors


registry = ModelRegistry()
app = FastAPI(title='Street Model Service', version='1.0.0')


@app.get('/health')
def health():
    return {'status': 'ok', 'loaded_embed_models': [name for name in registry.loaded if name != RERANK_MODEL],
            'loaded_rerank_models': [name for name in registry.loaded if name == RERANK_MODEL]}


@app.get('/models')
def models():
    return {'models': [{'name': name, 'kind': 'reranker' if name == RERANK_MODEL else 'embedding',
                        'path': str(registry.path(name)), 'exists': (registry.path(name) / (registry.checkpoint if name == IMAGE_MODEL else 'model.safetensors')).is_file(),
                        'loaded': name in registry.loaded,
                        **({'checkpoint': registry.checkpoint, 'dimensions': 6144} if name == IMAGE_MODEL else {'dimensions': 2048} if name == EMBED_MODEL else {})}
                       for name in [EMBED_MODEL, RERANK_MODEL, IMAGE_MODEL]]}


@app.post('/embed')
def embed(request: EmbedRequest):
    records = inputs(request.items, request.image_paths)
    vectors = registry.embed(request.model_name, records, request.instruction, request.batch_size)
    return {'model_name': request.model_name, 'embeddings': vectors}


@app.post('/rerank')
def rerank(request: RerankRequest):
    if request.model_name != RERANK_MODEL:
        raise HTTPException(400, 'Only Qwen3-VL-Reranker-2B supports reranking')
    queries = inputs(request.query_items, request.query_images)
    candidates = inputs(request.candidate_items, request.candidate_images)
    import torch
    with registry.lock, torch.inference_mode():
        model = registry.get(RERANK_MODEL)
        scores = [model.process({'query': query, 'documents': candidates,
                                 **({'instruction': request.instruction} if request.instruction else {})}) for query in queries]
    return {'scores': scores}


@app.post('/batch')
def batch(request: BatchRequest):
    if request.model_name not in {IMAGE_MODEL, EMBED_MODEL}:
        raise HTTPException(400, 'Batch requires a supported embedding model')
    source = local_file(request.jsonl_path)
    output_dir = Path(request.output_dir or RUNTIME / 'io/batch').resolve()
    if not output_dir.is_relative_to((RUNTIME / 'io').resolve()):
        raise HTTPException(403, 'Batch outputs must be within the configured runtime io directory')
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / f'{request.model_name}_{uuid4().hex[:12]}.jsonl'
    temporary = target.with_suffix('.jsonl.part')
    processed = 0
    try:
        with source.open(encoding='utf-8') as handle, temporary.open('w', encoding='utf-8') as out:
            out.write(json.dumps({'meta': {'model_name': request.model_name, 'instruction': request.instruction}}) + '\n')
            chunk = []
            for line in handle:
                if not line.strip():
                    continue
                row = json.loads(line)
                if not isinstance(row, dict) or request.image_key not in row or request.id_key not in row:
                    raise HTTPException(400, 'Batch row lacks image or ID field')
                chunk.append(row)
                if len(chunk) >= request.batch_size:
                    processed += write_batch(chunk, request, out)
                    chunk = []
            if chunk:
                processed += write_batch(chunk, request, out)
        temporary.replace(target)
    except (ValueError, UnicodeError) as exc:
        temporary.unlink(missing_ok=True)
        raise HTTPException(400, 'Invalid JSONL batch input') from exc
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return {'output_path': str(target), 'processed_count': processed}


def write_batch(rows, request, out):
    records = [parse_item(Item(type='image', uri=row[request.image_key])) for row in rows]
    vectors = registry.embed(request.model_name, records, request.instruction, request.batch_size)
    for row, vector in zip(rows, vectors, strict=True):
        out.write(json.dumps({'id': row[request.id_key], 'embedding_vector': vector}) + '\n')
    return len(rows)


if __name__ == '__main__':
    import uvicorn
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='127.0.0.1,172.17.0.1', help='Comma-separated local interfaces')
    parser.add_argument('--port', type=int, default=3130)
    args = parser.parse_args()
    import socket
    sockets = []
    try:
        for host in args.host.split(','):
            sockets.append(socket.create_server((host, args.port)))
        uvicorn.Server(uvicorn.Config(app, workers=1)).run(sockets=sockets)
    finally:
        for listener in sockets:
            listener.close()
