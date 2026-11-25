# Local street embedding service

Reimplements the existing street-model REST contract using pretrained local models:

- `ImAge4VPR`: 6144-dimensional image vectors, RGB → 322×322 bilinear resize → ImageNet normalization → ImAge → L2 normalization. The existing Elasticsearch client retains its original first-4096-dimensions behavior.
- `Qwen3-VL-Embedding-2B`: 2048-dimensional multimodal vectors, with the caller's instruction passed to the official embedder.
- `Qwen3-VL-Reranker-2B`: query/candidate relevance scores.

The exact old server checkpoint and preprocessing have not been recovered. Public `ImAge_Merged.pth` is the explicit default; `STREET_IMAGE_CHECKPOINT=ImAge_GSV.pth` selects the other downloaded checkpoint. Matching vector dimensions alone does not establish compatibility with an existing index.

## Install and run

Requires Linux, Python 3.10+, a compatible NVIDIA driver and free GPU memory. All weights, dependencies, logs and PIDs stay in this project. No container GPU runtime is required.

```bash
bash services/streetmodel/setup.sh
python scripts/streetmodel_service.py start
python scripts/streetmodel_service.py status
python scripts/streetmodel_service.py stop
```

The default listeners are `127.0.0.1:3130` and `172.17.0.1:3130`, served by a single process sharing the loaded models. `STREET_BIND_HOST` and `STREET_BIND_PORT` override these bindings. This avoids exposing the endpoint on every host network interface. Use the bridge address in `sandbox.environment.STREETMODEL_BASE_URL` for Docker sandboxes.

Models load lazily on their first request and run on CUDA. `/health` reports loaded names; `/models` additionally reports whether local weight files exist and the selected ImAge checkpoint. The service does not call a paid conversation API. Logs: `.runtime/streetmodel/server.log`. It stays running after the start command exits; run the start command again after a host reboot.

The Python environment is isolated at `.runtime/streetmodel/venv`. `models.json` pins public model revisions; the installer verifies published LFS SHA256 hashes and records `.runtime/streetmodel/downloaded-models.json`. Upstream source revisions are pinned in `setup.sh`; their licenses are retained in the downloaded repositories. `requirements.txt` pins direct runtime dependencies; the deployment evidence records the full installed package list.

## API compatibility

- `GET /health`, `GET /models`, `GET /docs`
- `POST /embed`: `model_name`, `items` or legacy `image_paths`, `instruction`, `batch_size`; returns `embeddings`.
- `POST /rerank`: `query_items`/`candidate_items` or legacy image lists; returns a query×candidate `scores` matrix.
- `POST /batch`: local JSONL image records, optional ID/image keys, instruction and batch size; returns `output_path` and `processed_count`.

Images can be submitted as inline base64/data URLs. Qwen additionally supports text, local video and UTF-8 text documents. Binary PDF/DOCX parsing is not claimed. File inputs are restricted to configured dataset roots; set `STREET_DATA_ROOTS` to add roots. Batch outputs go below `.runtime/streetmodel/io`; partial failed batches are not published. Original NAS paths still require a functioning shared filesystem.

## Verification

Deployment results and limitations are summarized in [the release notes](../../docs/releases/20261125-street-workflow.md).

The NAS subsequently recovered. Read-only mounts into the graph and sandbox are required to inspect retrieved originals; six known-index originals passed same-image Top-1 retrieval and old-vector comparison in [the release notes](../../docs/releases/20261125-street-workflow.md). Existing sandboxes must be recreated to gain new mounts.

```bash
.runtime/streetmodel/venv/bin/python -m pytest -q services/streetmodel/tests
```

These tests cover the REST contract and invalid input behavior. Deployment evidence separately tests actual CUDA models, vector norms/dimensions, the existing client, Elasticsearch queries and real sandbox reachability. Do not describe a successful query against the old index as proven retrieval accuracy without comparing original-image vectors.
