# Workflow Routing

This reference defines the high-level route selection for the network-traffic-analysis skill.

## Route 1: `analysis-only`

Use this route when the user wants:

- direct analysis of the current uploaded file
- analysis of an already prepared `flow.csv` or `packet.csv`
- direct statistics, protocol review, anomaly review, or export on one known dataset

Primary scripts:

- `prepare_pcap.py` only if the input is raw and preprocessing is actually needed
- `analyze.py` as the main script

## Route 2: `rag-only`

Use this route when the user wants:

- retrieval from indexed historical data
- cross-dataset evidence recall
- answers explicitly based on the shared Elasticsearch index
- historical summaries without reprocessing or re-analysis

Primary script:

- `rag_search.py`

## Route 3: `rag-plus-analysis`

Use this route when the user wants:

- indexed evidence first, then measured validation
- historical context followed by current file verification
- retrieval-guided investigation with a structured final result

Primary execution order:

1. `rag_search.py`
2. `analyze.py`

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
