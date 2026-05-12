---
name: network-traffic-analysis
description: Use this skill to investigate network traffic in the sandbox using strict script-driven workflows. Handles PCAP preprocessing, structured flow analysis, report export, and optional RAG retrieval over indexed traffic evidence.
metadata:
  short-description: Investigate network traffic using strict script-driven workflows.
---

# Network Traffic Analysis Skill

This skill is a strict, script-driven network traffic investigation workflow for enterprise-style traffic review, anomaly screening, and evidence export.

## Hard rules

- Always run the provided scripts through `bash`; never replace this workflow with ad hoc code
- Never read full network traffic data files into model context with `read_file`
- Never skip `inspect` before deep analysis on an unconfirmed schema
- Never guess missing fields, data source meaning, or protocol semantics
- Never jump into the RAG chain unless the route selection rules below say it is needed
- During analysis tasks, never run `pip install`, broad `find /mnt`, `grep -r /mnt`, or edit skill/schema/config files unless the user explicitly asks for skill development
- Never write final reports with heredocs or `cat >`; use `generate_incident_report.py` only
- If the input cannot be resolved cleanly, stop and report the exact reason

## Path conventions

| Root | Purpose |
| --- | --- |
| `/mnt/skills/custom/network-traffic-analysis` | Skill root |
| `/mnt/user-data/uploads` | Uploaded files |
| `/mnt/user-data/outputs` | Report and export artifacts |
| `/mnt/datasets/network-traffic` | Platform datasets |

Do not use `/mnt/skills/custom/datasets` or `/mnt/skills/custom/network-traffic-analysis/datasets` as dataset roots.

The field mapping is resolved: explicit CLI → `$NETWORK_TRAFFIC_FIELD_MAPPING` → dataset `schema/field_mapping.yaml` → skill `config/field_mapping.yaml` → repo fallback.

Environment overrides: `NETWORK_TRAFFIC_DATASET_ROOT`, `NETWORK_TRAFFIC_UPLOADS_ROOT`, `NETWORK_TRAFFIC_WORKSPACE_ROOT`, `NETWORK_TRAFFIC_OUTPUTS_ROOT`, `NETWORK_TRAFFIC_FIELD_MAPPING`, `NETWORK_TRAFFIC_PROCESSED_ROOT`.

## Input resolution order

1. Explicit path from `<selected_data_sources>` in the current turn
2. Uploaded files under `/mnt/user-data/uploads`
3. Platform datasets under `/mnt/datasets/network-traffic/raw` or `processed`

When `<selected_data_sources>` points to a dataset root, prefer in order: `*.flow.csv` → `*.packet.csv` → `rag/rag_docs.jsonl` → raw `*.pcap`/`*.pcapng`/`*.cap`. If both uploaded and local files match the same name, use the uploaded file unless the user explicitly says otherwise.

## File type handling

- Raw capture (`.pcap`, `.pcapng`, `.cap`): must go through `prepare_pcap.py` first
- Structured traffic data (`.csv`, `.parquet`, `.json`, `.jsonl`, `.xlsx`, `.xls`): must go through `analyze.py`

`prepare_pcap.py` prefers `tshark`, falls back to `scapy`, and when `zeek` is available emits protocol-semantic logs under a `zeek/` subdirectory.

## Allowed tools

- File reading: only for this SKILL.md, references, small metadata, and short previews
- Shell execution: only through the provided scripts — `prepare_pcap.py`, `analyze.py`, `build_rag_docs.py`, `embed_rag_docs.py`, `index_rag_docs.py`, `build_full_rag_index.py`, `rag_search.py`, `generate_incident_report.py`

Do not call helper modules directly unless debugging implementation internals.

## Reference use rules

Read references only when the current task requires them. Do not bulk-read all references.

| Trigger | Required reference |
| --- | --- |
| Route choice is unclear, or user mixes retrieval with current-file analysis | `references/workflow-routing.md` |
| User asks what actions or capabilities are available | `references/capability-catalog.md` |
| User asks for an investigation plan, incident workflow, triage sequence, or "what should I check next" | `references/playbooks.md` |
| User asks about advanced analytics, ML, graph, QoS, threat intel, forecasting, or drift | `references/advanced-actions.md` |
| User asks for JSON, SkillResult, or downstream integration format | `references/input-output-contract.md` |
| User asks to build, rebuild, index, validate, or troubleshoot RAG | `references/rag-indexing-and-rebuild.md` |
| User asks about field meaning, canonical schema, timestamps, durations, sessionization, or PCAP-to-flow semantics | `references/field-dictionary.md` and `references/preprocessing-and-time-semantics.md` |
| User asks for exact command examples | `references/command-patterns.md` |

## Route selection

### Route 1: `analysis-only` (default)

Direct analysis of one file or dataset. Use `prepare_pcap.py` + `analyze.py` only. Do not introduce RAG.

### Route 2: `rag-only`

Retrieval from indexed historical data. Start with `rag_search.py`. Only expand into build chain if required artifacts or index do not exist.

### Route 3: `rag-plus-analysis`

Indexed evidence first (`rag_search.py`), then measured validation (`analyze.py` on current dataset). Only rebuild RAG if indexed artifacts are missing or stale.

## Flow vs packet selection

- User names `*.flow.csv` → `flow` view; `*.packet.csv` → `packet` view
- No specification → `--view auto` (prefer `flow` for overview/trends/anomaly; prefer `packet` for TCP flags/handshake/ICMP/packet-level burst)

## Mandatory execution workflow

### Step 0. Runtime preflight

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --self-check --format skill-result-json
```

If self-check fails, report the failure and stop immediately.

### Step 1. Resolve the file path

Use the resolved path from `/mnt/user-data/uploads/`, `/mnt/datasets/network-traffic/raw/`, or `/mnt/datasets/network-traffic/processed/`. When you know the filename but not the full path, use scoped lookup only:

```bash
find /mnt/datasets/network-traffic/raw -name "<filename>" 2>/dev/null
find /mnt/datasets/network-traffic/processed -name "<filename>" 2>/dev/null
```

Do not run broad `find /mnt`.

### Step 2. Classify the file

Raw capture → preprocess first. Tabular traffic data → inspect first, then analyze. RAG routes → check for reusable `rag` artifacts before rebuilding.

### Step 3. Execute the correct script

Raw capture preprocessing:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/prepare_pcap.py --files <resolved-input-path> --format json
```

Always continue analysis from the returned `flow_csv`; use `packet_csv` only for packet-specific questions. Preprocessed datasets with `zeek_enabled=true` contain `conn.log`, `dns.log`, `http.log`, `ssl.log`, and `weird.log` as supporting evidence.

Tabular data inspection:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action inspect
```

### Step 4. Choose the analysis action

If the task cannot be answered by the common starting actions listed below, run `--action list-capabilities` and read `references/capability-catalog.md` before choosing an action.

Common starting actions: `summary`, `overview-report`, `topn`, `distribution`, `timeseries`, `detect-anomaly`, `query`.

Advanced actions: `encrypted-flow-analysis`, `device-identification`, `behavior-analysis`, `graph-analysis`, `qos-analysis`, `root-cause-analysis`, `threat-intel-match`, `forecast-traffic`, `detect-concept-drift` (see `references/advanced-actions.md`).

Contextual actions: `signature-review` (50+ Aho-Corasick IOC rules), `zeek-review` (protocol semantics), `risk-fusion-review` (final risk ranking), `short-connection-review`, `dns-tunnel-review`, `data-exfiltration-review`, `lateral-movement-review`, `scan-review`, `session-review`, `protocol-review`, `packet-review`, `periodicity-review`.

Anomaly engines: `hybrid` (default), `iforest`, `lof`, `rcf`, `rule`. Use `--anomaly-engine hybrid` for normal investigations.

### Step 5. Optional RAG chain

Before any RAG build, rebuild, indexing, deletion, or troubleshooting operation, read `references/rag-indexing-and-rebuild.md`.

Artifact chain: `flow.csv` → `rag/rag_docs.jsonl` → `rag/rag_embeddings.jsonl` → Elasticsearch index.

Reuse existing `rag` artifacts whenever present. Do not rebuild by default.

Full rebuild (preferred for shared-index scenarios):

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/build_full_rag_index.py \
  --files <resolved-pcap-or-flow-csv> \
  --dataset-name <dataset-name> \
  --replace-source \
  --verify-search \
  --format json
```

Dry-run before rebuild:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/build_full_rag_index.py \
  --files <resolved-pcap-or-flow-csv> \
  --dataset-name <dataset-name> \
  --replace-source \
  --dry-run \
  --format json
```

Do not use full rebuild for one-off analysis of a single already-processed dataset.

### Elasticsearch shared index policy

Connection info from `config.yaml` referencing `$NETWORK_TRAFFIC_ES_*` env vars. CLI overrides: `--es-host`, `--es-username`, `--es-password`, `--es-api-key`.

Index names: `network-traffic-rag-smoke` (smoke test), `network-traffic-rag-dev` (development), `network-traffic-rag` (production). Never use or delete the `street` index.

Shared-index isolation uses `dataset_name`, `source_file`, and `schema_version`. Use `--replace-source` when reindexing the same dataset/source.

### RAG runtime prerequisites

`rag_search.py` requires both Elasticsearch and an embedding service to compute query vectors.

| Service | Required env vars | How it works |
| --- | --- | --- |
| Elasticsearch | `NETWORK_TRAFFIC_ES_HOST`, `NETWORK_TRAFFIC_ES_INDEX`, `NETWORK_TRAFFIC_ES_USERNAME`, `NETWORK_TRAFFIC_ES_PASSWORD` | Injected into sandbox via `sandbox.environment` in `config.yaml` |
| Embedding | `NETWORK_TRAFFIC_EMBEDDING_API_KEY`, `NETWORK_TRAFFIC_EMBEDDING_BASE_URL` | Local bge-m3 service (default); does not validate the API key. Switch to cloud providers (OpenAI, DashScope) by changing the base URL and setting a real key |

Inside sandbox containers, `config.yaml` is not mounted. All configuration resolves from environment variables directly — scripts fall back to `NETWORK_TRAFFIC_ES_*` and `NETWORK_TRAFFIC_EMBEDDING_*` env vars when config.yaml is unavailable.

If `rag_search.py` fails with "No embedding API key resolved", check that `NETWORK_TRAFFIC_EMBEDDING_API_KEY` is set (use `"unused"` for local bge-m3 service) and that `sandbox.environment` in `config.yaml` includes `NETWORK_TRAFFIC_EMBEDDING_API_KEY: $NETWORK_TRAFFIC_EMBEDDING_API_KEY`.

## RAG v2 document model

New RAG documents must use `schema_version: rag_doc_v2`. Do not generate or recommend `rag_doc_v1_compat`.

| Type | Source | Purpose |
| --- | --- | --- |
| `flow_summary` | flow.csv | One document per flow row |
| `endpoint_summary` | flow.csv | Source endpoint aggregate |
| `port_summary` | flow.csv | Destination port/protocol aggregate |
| `protocol_summary` | flow.csv | DNS/TLS/HTTP/service feature aggregate |
| `behavior_summary` | flow.csv | Behavioral aggregate summary |
| `anomaly_summary` | flow.csv | High-level anomaly summary |
| `action_finding` | analysis actions | One document per structured finding |
| `action_evidence` | analysis actions | Evidence tables, entities, metrics |
| `action_diagnostic` | analysis actions | Per-action diagnostics and warnings |
| `dataset_profile` | flow.csv | Dataset-level profile |

Provenance rules:
- `source_file` is the direct structured input, normally the generated `flow.csv`
- `raw_source_file` is the original PCAP when the data came from packet capture preprocessing
- Do not treat `raw_source_file` as the delete/replacement key for shared-index cleanup

Linked-flow retrieval: `action_finding` and `action_evidence` carry `payload.flow_filter` with dataset, source, and entity filters. `rag_search.py` uses this for second-hop lookup against `flow_summary` documents. See `references/workflow-routing.md` for two-hop retrieval details.

For suspicious activity retrieval, prefer `--doc-type action_finding` or high-level search first, then inspect linked `flow_summary` results:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/rag_search.py \
  --query "<question>" \
  --doc-type action_finding \
  --risk-level high \
  --format json
```

## Standard command patterns

When the user asks for exact commands or when you need a command not shown in this file, read `references/command-patterns.md`.

## Output discipline

- State which file was analyzed, its source (uploads vs local datasets), and which script/action was used
- Present measured findings first; separate findings from interpretation
- Keep interpretation conservative; do not claim malware family, attack phase, or threat intent unless data directly supports it
- Separate Zeek semantic evidence from flow/packet statistical evidence when applicable
- For anomaly analysis use language such as: suspicious, notable, rare, concentrated, bursty, investigation-worthy
- For final Markdown reports, use `generate_incident_report.py`; do not compose final reports manually

## Non-negotiable boundaries

- Uploaded files are the primary input; local datasets are secondary
- Execution must go through the provided scripts
- Anomaly detection uses rule + statistical + ML scoring (not pure legacy rule detection)
- RAG and vector retrieval are optional routes, not the default
- No backend-tool path is part of this workflow

## Investigation playbooks

When the user asks for an investigation plan, incident workflow, triage sequence, or "what should I check next", read `references/playbooks.md` before proposing steps.

`references/playbooks.md` contains 13 structured investigation playbooks (A-M) covering executive overview through advanced threat intelligence and forecasting.
