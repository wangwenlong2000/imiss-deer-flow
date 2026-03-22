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
   - `/mnt/datasets/network-traffic/raw`
   - `/mnt/datasets/network-traffic/processed`

Rules:

- If an exact uploaded file match exists in `<uploaded_files>`, use it
- If there is no uploaded match, resolve the file under `/mnt/datasets/network-traffic/...`
- If both uploaded and local files match the same name, use the uploaded file unless the user explicitly says to use the local dataset
- Do not browse unrelated directories before checking these known roots
- Do not invent alternate paths

## File type handling

Classify the input immediately:

- Raw capture: `.pcap`, `.pcapng`, `.cap`
- Structured traffic data: `.csv`, `.parquet`, `.json`, `.jsonl`, `.xlsx`, `.xls`

Rules:

- Raw capture must go through `prepare_pcap.py` first
- Structured traffic data must go through `analyze.py`
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

Do not use `read_file` to load full CSV, JSON, Excel, Parquet, or PCAP content into the model.

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

- If raw capture: preprocess first
- If tabular traffic data: inspect first, then analyze

### Step 3. Execute the correct script

For raw capture:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/prepare_pcap.py --files <resolved-input-path> --format json
```

Then take the generated `flow.csv` path and continue with `analyze.py`.

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
- RAG and vector retrieval are out of scope for this skill
- No backend-tool path is part of this workflow
