---
name: network-traffic-analysis
description: Use this skill when the user wants to investigate network traffic with a strict, enterprise-style workflow. Supports uploaded files and server-side local datasets, including raw packet captures (.pcap/.pcapng/.cap) and structured flow logs stored as CSV, Parquet, Excel, JSON, or JSONL. Supports preprocessing PCAP into packet.csv and flow.csv, then running inspect, summary, overview-report, scan-review, session-review, protocol-review, packet-review, top-N, distribution, timeseries, filtering, aggregation, anomaly screening, SQL investigation, and export.
metadata:
  short-description: Investigate network traffic with a strict script-driven workflow using read_file and bash.
---

# Network Traffic Analysis Skill

This skill is a strict, script-driven network traffic investigation workflow.

Use it for enterprise-style traffic review, security investigation support, anomaly screening, communication analysis, and evidence export.

## Hard rules

These rules are mandatory for this skill:

- Always use this skill's workflow when the task is network traffic analysis
- Always run the provided scripts through `bash`
- Never replace this workflow with ad hoc Python, JavaScript, SQL generators, or generic one-off code
- Never rely on backend custom tools for this skill
- Never read full network traffic data files into model context with `read_file`
- Never skip `inspect` when the current thread has not already confirmed the schema
- Never silently switch to unrelated paths or generic directory probing
- Never guess missing fields, data source meaning, or protocol semantics
- If the input cannot be resolved cleanly, stop and report the exact reason instead of improvising

## Input resolution order

Resolve the input in this exact order:

1. Uploaded files under `/mnt/user-data/uploads`
2. Local datasets under:
   - `/mnt/datasets/network-traffic/processed`
   - `/mnt/datasets/network-traffic/raw`

Rules:

- If an exact uploaded file match exists in `<uploaded_files>`, use it
- If there is no uploaded match, resolve the file under `/mnt/datasets/network-traffic/...`
- If both uploaded and local files match the same name, use the uploaded file unless the user explicitly says to use the local dataset
- For local datasets, prefer an existing `processed/<dataset>/...flow.csv` or `processed/<dataset>/...packet.csv` over reusing the raw `pcap`
- Only fall back to local raw `pcap` when no suitable processed artifact exists, or when the user explicitly asks to rebuild/reprocess
- Do not browse unrelated directories before checking these known roots
- Do not invent alternate paths

## File type handling

Classify the input immediately:

- Raw capture: `.pcap`, `.pcapng`, `.cap`
- Structured traffic data: `.csv`, `.parquet`, `.json`, `.jsonl`, `.xlsx`, `.xls`

Rules:

- Raw capture must go through `prepare_pcap.py` first
- Structured traffic data must go through `analyze.py`
- Uploaded raw `pcap` must always be preprocessed before analysis
- Local raw `pcap` should be preprocessed only when there is no suitable processed artifact, or when the user explicitly asks to rebuild/reprocess
- Do not treat structured traffic data as plain chat text

## Allowed tools and assets

Use only these assets for execution:

- `read_file` for:
  - this `SKILL.md`
  - referenced skill files
  - small metadata or reference files
  - short, line-limited previews when strictly necessary
- `bash` for:
  - `/mnt/skills/custom/network-traffic-analysis/scripts/prepare_pcap.py`
  - `/mnt/skills/custom/network-traffic-analysis/scripts/analyze.py`
  - `/mnt/skills/custom/network-traffic-analysis/scripts/build_rag_docs.py`
  - `/mnt/skills/custom/network-traffic-analysis/scripts/embed_rag_docs.py`
  - `/mnt/skills/custom/network-traffic-analysis/scripts/index_rag_docs.py`
  - `/mnt/skills/custom/network-traffic-analysis/scripts/build_full_rag_index.py`
- `/mnt/skills/custom/network-traffic-analysis/scripts/rag_search.py`

Do not use `read_file` to load full CSV, JSON, Excel, Parquet, or PCAP content into the model.

## RAG document build

Use `build_rag_docs.py` when the user wants retrieval-ready network traffic documents from an existing `*.flow.csv`.

Rules:

- Build RAG docs only from `flow.csv` in the first version
- Do not run this step on raw `pcap` directly
- Keep `prepare_pcap.py` for preprocessing and `analyze.py` for measured analysis
- Use the generated `rag_docs.jsonl` as the handoff artifact for later embedding and Elasticsearch indexing

Command:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/build_rag_docs.py --files <resolved-flow-csv-path> --format json
```

## RAG embedding

Use `embed_rag_docs.py` when `rag_docs.jsonl` has already been created and the next step is generating local embedding outputs for later Elasticsearch ingestion.

Rules:

- Embedding input must be `rag_docs.jsonl`
- Embedding configuration should come from `config.yaml`
- The configured provider may be `dashscope`, `openai`, or another OpenAI-compatible endpoint
- Default model comes from `config.yaml` `embedding.model`
- Use a small batch size for embedding providers with strict request limits; the current default is `10`
- Output is a local `rag_embeddings.jsonl`, not a vector database write

Command:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/embed_rag_docs.py --files <resolved-rag-docs-path> --format json
```

## RAG Elasticsearch indexing

Use `index_rag_docs.py` when `rag_embeddings.jsonl` has already been created and the next step is writing indexed documents into Elasticsearch.

Rules:

- Elasticsearch input must be `rag_embeddings.jsonl`
- Elasticsearch connection and index settings should come from `config.yaml`
- Default index name is `network-traffic-rag`
- Do not skip local `rag_docs.jsonl` and `rag_embeddings.jsonl`; Elasticsearch is the downstream indexed store, not the only artifact
- This step writes vectors into Elasticsearch, but it does not yet perform retrieval-time search orchestration

Command:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/index_rag_docs.py --files <resolved-rag-embeddings-path> --format json
```

## Full RAG index build

Use `build_full_rag_index.py` when the goal is to preprocess all raw pcap files under the network-traffic raw directory, build per-file RAG artifacts, generate embeddings, and append them into one shared Elasticsearch index.

Rules:

- This workflow is recommended for server-side batch indexing
- Each pcap is processed independently
- The shared Elasticsearch index is still unified
- Existing indexed datasets are skipped by default unless `--rebuild-existing` is specified

Command:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/build_full_rag_index.py --raw-dir /mnt/datasets/network-traffic/raw --index-name network-traffic-rag --format json
```

## RAG search

Use `rag_search.py` when the user is asking about already indexed historical traffic, wants retrieval from the shared Elasticsearch index, or wants cross-dataset evidence recall.

Rules:

- Treat `rag_search.py` as a retrieval tool, not as the high-level workflow router
- Do not default to `--verbose` in front-end or skill execution
- Prefer dataset-level filtering when the user names a concrete dataset or pcap
- Let `rag_search.py` handle metadata filtering, text retrieval, vector retrieval, fusion, and doc_type soft preference
- Do not use `--doc-type` unless the user explicitly requests a specific document type
- Prefer `--dataset-name <dataset>` when the user names a specific indexed dataset; `Virut.pcap` should be normalized to `Virut`

Command:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/rag_search.py --query "<user-question>" --format json
```

## Lead-Agent routing

The lead agent should decide the high-level path before selecting a script.

### `analysis-only`

Use this path when the user is asking about:

- the current uploaded file
- the currently prepared `flow.csv` or `packet.csv`
- direct statistics, protocol analysis, anomaly analysis, or investigation on the current dataset

Execution rule:

- Prefer `analyze.py`
- Use `prepare_pcap.py` first only when the source is raw `pcap` and there is no reusable processed artifact, or when the user explicitly asks to rebuild/reprocess
- If the requested anomaly heuristic or review path is unclear, first inspect capabilities with `python3 scripts/analyze.py --action list-capabilities`
- Never invent or guess a `detect-anomaly --rule <name>` value
- Only use `detect-anomaly` rules that appear under `detect_anomaly_rules.supported` from `python3 scripts/analyze.py --action list-capabilities`
- If the requested heuristic is not listed there, do not call `detect-anomaly`; map the request to the nearest structured action (`session-review`, `scan-review`, `protocol-review`, `packet-review`) or use `query` for explicit thresholds and analyst-defined logic
- Prefer `short-connection-review` for formal short-connection analysis on flow data; use `query` only when the user needs custom thresholds or raw samples
- When the user names a local dataset that already has processed artifacts, prefer those processed files instead of re-running `prepare_pcap.py`

### `rag-only`

Use this path when the user is asking about:

- already indexed historical data
- the shared Elasticsearch index
- retrieval across multiple datasets
- finding similar communication or similar anomalies from indexed data

Execution rule:

- Prefer `rag_search.py`
- If the user explicitly says "from the index", "based on indexed data", or "do not reprocess/do not run analyze.py", keep the request on the RAG path even if the dataset name looks like a raw `pcap`

### `rag-plus-analysis`

Use this path when the user is asking to:

- retrieve historical evidence first, then verify the current file
- compare indexed evidence with a current uploaded file
- use retrieved context to guide a more precise local analysis

Execution rule:

- Run `rag_search.py` first
- Then run `analyze.py` on the target local or uploaded dataset
- Keep `analyze.py` as the final structured validation step
- In the merged answer, treat `analyze.py` as the primary conclusion and use RAG as evidence recall and context

## Flow vs packet selection

Use these rules to choose the analysis view:

- If the user explicitly names `*.flow.csv`, use the `flow` view
- If the user explicitly names `*.packet.csv`, use the `packet` view
- If the user does not specify, use `--view auto`
- In `auto` mode:
  - Prefer `flow` for overview, ranking, distribution, trends, asset relations, and most anomaly triage
  - Prefer `packet` for TCP flags, SYN-only behavior, handshake quality, RST-heavy traffic, ICMP probing, and packet-level burst questions
- When the investigation starts broad and then needs protocol-detail validation, first use `flow`, then drill down into `packet`

## Mandatory execution workflow

### Step 1. Resolve the file path

Use the exact resolved path from:

- `/mnt/user-data/uploads/<filename>`
- or `/mnt/datasets/network-traffic/raw/...`
- or `/mnt/datasets/network-traffic/processed/...`

### Step 2. Classify the file

- If raw capture from uploads: preprocess first
- If local raw capture: first check whether matching processed artifacts already exist; if they do, use the processed files unless the user explicitly asks to rebuild/reprocess
- If raw capture has no reusable processed artifact: preprocess first
- If tabular traffic data: inspect first, then analyze

### Step 3. Execute the correct script

For raw capture:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/prepare_pcap.py --files <resolved-input-path> --format json
```

Then take the generated `flow.csv` path and continue with `analyze.py`.

Before running `prepare_pcap.py` on a local raw dataset, check whether a matching processed artifact already exists:

- Preferred reusable paths:
  - `/mnt/datasets/network-traffic/processed/<dataset-name>/<dataset-name>.flow.csv`
  - `/mnt/datasets/network-traffic/processed/<dataset-name>/<dataset-name>.packet.csv`
- If either of these exists and the user did not explicitly request rebuild/reprocess, use the processed artifact instead of re-running preprocessing
- Do not re-run preprocessing on local raw `pcap` only because the dataset name ends with `.pcap`

Output location rules:

- If the input came from `/mnt/user-data/uploads/...`, the generated outputs should stay under `/mnt/user-data/workspace/network-traffic/<dataset-name>/...`
- If the input came from `/mnt/datasets/network-traffic/raw/...`, the generated outputs should go under `/mnt/datasets/network-traffic/processed/<dataset-name>/...`
- Always use the `flow_csv` path returned by `prepare_pcap.py` for the next step
- Do not guess or reconstruct the processed output path by hand

For tabular traffic data:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action inspect
```

### Step 4. Inspect before deep analysis

Unless schema was already confirmed in the current thread, run:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action inspect
```

### Step 5. Choose the correct analysis action

Use the smallest correct action from the script:

- `summary`
- `overview-report`
- `scan-review`
- `session-review`
- `short-connection-review`
- `protocol-review`
- `packet-review`
- `topn`
- `distribution`
- `timeseries`
- `filter`
- `aggregate`
- `detect-anomaly`
- `query`
- `export`

Do not substitute these with self-written code.

### Action mapping discipline

Use these intent-to-action mappings before considering lower-level actions:

- "overall overview", "overall profile", "enterprise overview", "traffic summary" -> `overview-report`
- "scan", "probe", "port sweep", "broad destination spread" -> `scan-review`
- "session quality", "failure-heavy", "reset-heavy", "handshake abnormal", "handshake anomaly" -> `session-review`
- "short connection", "short-lived flow", "brief connection", "low-byte short session" -> `short-connection-review`
- "protocol activity", "DNS/TLS/HTTP investigation", "application protocol mix" -> `protocol-review`
- "packet-level review", "TCP flags", "SYN-only", "RST-heavy", "ICMP probing", "small-packet burst" -> `packet-review` or `detect-anomaly`

Only use `topn`, `distribution`, `aggregate`, or `query` when:

- the user explicitly asks for a lower-level breakdown,
- the higher-level action does not expose the needed section,
- or a precise follow-up drill-down is required after a higher-level action has already run.

Do not replace `overview-report`, `scan-review`, `session-review`, `protocol-review`, or `packet-review` with self-written Python, shell, awk, or one-off SQL if one of those actions already covers the request.

## Enterprise capability coverage

### 1. Overview and inventory

Use for:

- Dataset sanity check
- Record, byte, packet, and time-range summary
- Unique source and destination counts
- High-level protocol and service inventory

Primary actions:

- `inspect`
- `summary`
- `overview-report`
- `distribution`

### 2. Heavy hitters and communication concentration

Use for:

- Top source IPs
- Top destination IPs
- Top destination ports
- Top services
- Top application protocols
- Traffic concentration by bytes, packets, or flow count

Primary actions:

- `topn`
- `aggregate`
- `query`

### 3. Distribution and protocol mix

Use for:

- Protocol mix
- Port mix
- App-protocol mix
- Service mix
- Direction mix
- Action mix
- Traffic-family mix

Primary actions:

- `distribution`
- `aggregate`
- `protocol-review`

### 4. Time-series and burst analysis

Use for:

- Hourly traffic profile
- Daily traffic profile
- Burst windows
- Byte spikes
- Activity concentration by time bucket

Primary actions:

- `timeseries`
- `detect-anomaly --rule volume-spike`
- `query`

### 5. Asset, endpoint, and peer analysis

Use for:

- Which hosts contact the most peers
- Which destinations are contacted by the most unique sources
- Which assets, users, devices, or sensors are most active
- Which communication relationships are unusually broad

Primary actions:

- `aggregate`
- `topn`
- `query`

Relevant fields when available:

- `src_ip`
- `dst_ip`
- `asset_id`
- `device_id`
- `user_id`
- `sensor_id`
- `direction`

### 6. Session quality and connection outcome analysis

Use for:

- Allowed vs denied traffic
- Session-state distribution
- Reset-heavy traffic
- Failed or abnormal connection concentration
- Short-lived low-byte connection patterns

Primary actions:

- `distribution`
- `session-review`
- `short-connection-review`
- `aggregate`
- `query`
- `detect-anomaly --rule failure-rate`

Relevant fields when available:

- `action`
- `session_state`
- `tcp_flags`
- `duration_ms`
- `flow_duration`
- `bytes`
- `packets`

### 7. Protocol field investigation

Use for:

- DNS query analysis
- TLS SNI analysis
- HTTP host analysis
- Rule-name and action review
- TCP flag concentration

Primary actions:

- `distribution`
- `protocol-review`
- `topn`
- `query`
- `filter`

Relevant fields when available:

- `dns_query`
- `tls_sni`
- `http_host`
- `rule_name`
- `tcp_flags`
- `action`

### 8. Rule-based anomaly screening

Current anomaly handling is investigation-grade and rule-based.

Use it for:

- Wide destination spread from one source
- Wide destination port spread from one source
- Rare destination ports
- Volume spikes
- Failure-rate concentration

Primary actions:

- `detect-anomaly`
- `query`

Interpretation rule:

- Treat anomaly results as investigation leads, not final attack attribution

### 9. Packet-level review and handshake analysis

Use for:

- TCP flags distribution
- SYN-only behavior
- RST-heavy traffic
- Handshake-quality review
- Handshake anomaly review
- Large reset ratio review
- ICMP probing
- Small-packet burst patterns
- Protocol-detail validation after a broad flow-level screen

Primary actions:

- `packet-review`
- `protocol-review --view packet`
- `session-review --view packet`
- `detect-anomaly --rule syn-scan`
- `detect-anomaly --rule rst-heavy`
- `detect-anomaly --rule handshake-failure`
- `detect-anomaly --rule icmp-probe`
- `detect-anomaly --rule small-packet-burst`

## Standard investigation playbooks

### Playbook A. Executive traffic overview

Run in this order:

1. `inspect`
2. `overview-report --view auto`
3. If needed, `protocol-review --view auto`

### Playbook B. Host and peer investigation

Run in this order:

1. `inspect`
2. `summary`
3. `topn` on `src_ip`
4. `query` or `aggregate` for unique destination spread

### Playbook C. Rare-port and long-tail screening

Run in this order:

1. `inspect`
2. `detect-anomaly --rule rare-port`
3. `query` for port-level detail and bytes

### Playbook D. Volume and timing anomaly screening

Run in this order:

1. `inspect`
2. `timeseries`
3. `detect-anomaly --rule volume-spike`

### Playbook E. Failure and reset investigation

Run in this order:

1. `inspect`
2. `session-review --view auto`
3. `detect-anomaly --rule failure-rate`
4. `query` for affected hosts

### Playbook F. Protocol field investigation

Run in this order:

1. `inspect`
2. `protocol-review --view auto`
3. `query` or `distribution` on one of:
   - `dns_query`
   - `tls_sni`
   - `http_host`
   - `rule_name`

### Playbook G. Packet-level scan and handshake investigation

Run in this order:

1. `inspect`
2. `packet-review --view packet`
3. `detect-anomaly --rule syn-scan --view packet`
4. If needed, `session-review --view packet`

## Standard command patterns

Summary:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action summary
```

Enterprise overview report:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action overview-report --view auto
```

Scan review:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action scan-review --view auto --limit 20
```

Session review:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action session-review --view auto --limit 20
```

Short-connection review:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action short-connection-review --view flow --limit 20
```

Protocol review:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action protocol-review --view auto --limit 20
```

Packet review:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action packet-review --view packet --limit 20
```

Top source IPs by bytes:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action topn --dimension src_ip --metric bytes --limit 10
```

Destination port distribution:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action distribution --dimension dst_port --limit 10
```

Hourly trend:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action timeseries --interval hour
```

Rare-port screening:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action detect-anomaly --rule rare-port
```

Failure-rate screening:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action detect-anomaly --rule failure-rate
```

Volume-spike screening:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action detect-anomaly --rule volume-spike
```

Build first-version RAG docs:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/build_rag_docs.py --files <resolved-flow-csv-path> --format json
```

Embed first-version RAG docs:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/embed_rag_docs.py --files <resolved-rag-docs-path> --format json
```

Index first-version RAG docs into Elasticsearch:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/index_rag_docs.py --files <resolved-rag-embeddings-path> --format json
```

Batch-build a unified RAG index from all raw pcaps:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/build_full_rag_index.py --raw-dir /mnt/datasets/network-traffic/raw --index-name network-traffic-rag --format json
```

Packet-level SYN scan screening:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action detect-anomaly --rule syn-scan --view packet
```

Packet-level reset-heavy screening:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action detect-anomaly --rule rst-heavy --view packet
```

Packet-level handshake-failure screening:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action detect-anomaly --rule handshake-failure --view packet
```

Custom SQL:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action query --sql "<enterprise-investigation-sql>"
```

Export:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action export --filters "<json-filters>" --output-file /mnt/user-data/outputs/<result-file>.csv
```

## Output discipline

When replying:

- State exactly which file was analyzed
- State whether the source came from uploads or local datasets
- State which script and which action or SQL was used
- Present measured findings first
- Separate findings from interpretation
- Keep interpretation conservative
- For anomaly analysis, use language such as:
  - suspicious
  - notable
  - rare
  - concentrated
  - bursty
  - investigation-worthy
- Do not claim malware family, attack phase, product identity, or threat intent unless the data directly supports it

## Non-negotiable boundaries

- Uploaded files are the primary input
- Local datasets are secondary inputs
- Execution must go through the provided scripts
- Anomaly detection is rule-based, not model-based detection
- RAG document construction, embedding, and Elasticsearch indexing are allowed as downstream steps in this workflow
- Retrieval-time RAG answering still requires a downstream search step such as `rag_search.py` or equivalent workflow integration
- No backend-tool path is part of this workflow
