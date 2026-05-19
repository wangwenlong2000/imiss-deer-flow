# Workflow Routing

This reference defines the high-level route selection for the network-traffic-analysis skill.

Progressive disclosure rule:

- Default to the smallest route that can answer the user's question.
- Do not reveal or execute the RAG build chain unless the selected route actually needs retrieval or rebuild.
- Reuse existing processed and indexed artifacts before rebuilding.
- In a fresh runtime, run `analyze.py --self-check --format skill-result-json` before the
  first analysis command. If it fails, stop and report the failed runtime check
  instead of installing packages, probing `/mnt`, or creating config/schema files.

## Route 1: `analysis-only`

Use this route when the user wants:

- direct analysis of the current uploaded file
- analysis of an already prepared `flow.csv` or `packet.csv`
- direct statistics, protocol review, anomaly review, or export on one known dataset

Primary scripts:

- `prepare_pcap.py` only if the input is raw and preprocessing is actually needed
- `analyze.py` as the main script

Execution boundary:

- Do not introduce `build_rag_docs.py`, `embed_rag_docs.py`, `index_rag_docs.py`, or `build_full_rag_index.py`
- Keep the workflow on direct preprocessing plus structured analysis

## Route 2: `rag-only`

Use this route when the user wants:

- retrieval from indexed historical data
- cross-dataset evidence recall
- answers explicitly based on the shared Elasticsearch index
- historical summaries without reprocessing or re-analysis

Primary script:

- `rag_search.py`

Expansion rule:

- If the needed dataset is not yet indexed, expand only as far as necessary:
  1. reuse existing `flow.csv` when present
  2. build `rag_docs.jsonl` only if missing or stale
  3. build `rag_embeddings.jsonl` only if missing or stale
  4. index into Elasticsearch only if the index entry is missing or stale

## Route 3: `rag-plus-analysis`

Use this route when the user wants:

- indexed evidence first, then measured validation
- historical context followed by current file verification
- retrieval-guided investigation with a structured final result

Primary execution order:

1. `rag_search.py`
2. `analyze.py`

Expansion rule:

- Prefer retrieval first only when usable indexed artifacts already exist
- If indexed artifacts do not exist, build only the missing steps in the RAG chain before retrieval
- Keep `analyze.py` as the final validation step on the current dataset

Interpretation rule:

- Use `analyze.py` as the final measured conclusion.
- Use RAG as evidence recall and context, not as the primary conclusion source.

## Preprocessing reuse rule

For local datasets:

- Prefer existing processed artifacts under `processed/<dataset>/...` whenever available.
- Do not re-run `prepare_pcap.py` on local raw `pcap` only because the dataset name ends with `.pcap`.
- Reprocess only when:
  - processed artifacts do not exist
  - the user explicitly requests rebuild or reprocess

For uploaded raw captures:

- Always preprocess first.

## RAG rebuild rule

Use full index rebuild workflows when:

- the preprocessing semantics changed
- field mappings changed
- RAG document structure changed
- embedding model changed
- Elasticsearch index was removed and must be rebuilt from local artifacts or raw pcaps

Do not use a full rebuild when:

- one dataset already has valid `rag` artifacts
- the user only asked for one-off current-dataset analysis
- a partial artifact rebuild is sufficient
