---
name: network-traffic-analysis
description: Use this skill when the user wants to analyze network traffic datasets, including raw packet captures (.pcap/.pcapng/.cap) and tabular flow logs stored as CSV, Parquet, Excel, JSON, or JSONL files. Supports host-side PCAP preprocessing, schema inspection, protocol and port distribution, source and destination IP top-N analysis, time-series aggregation, filtering, grouping, anomaly checks, SQL queries, and result export for datasets under datasets/network-traffic or user-provided paths.
metadata:
  short-description: Preprocess PCAP files or analyze tabular network traffic logs with host-side tools.
---

# Network Traffic Analysis Skill

Use this skill for both raw packet captures and structured network traffic datasets. The workflow is intentionally strict so the agent answers with repeatable analysis instead of improvising.

This skill is designed to work against datasets already present on the server under `datasets/network-traffic/`.
Do not require upload or `/mnt/user-data/...` access unless the user explicitly says the file only exists in the sandbox workspace.

There are now two backend tools for this skill:

- `network_traffic_prepare`: preprocess `.pcap` and `.pcapng` files into standardized `packet.csv` and `flow.csv`
- `network_traffic_analyze`: analyze one or more tabular datasets that already exist on the host

## Workflow

### 1. Locate the files

First classify the user input:

- If the user references `.pcap`, `.pcapng`, or asks to analyze raw packet captures, call `network_traffic_prepare` first.
- If the user references `.csv`, `.parquet`, `.json`, `.jsonl`, `.xlsx`, `.xls`, or a prepared dataset name, call `network_traffic_analyze` directly.
- If the user asks to compare multiple files together, pass multiple references in one tool call rather than analyzing only one file.
- Treat a bare filename such as `Geodo.pcap`, `Gmail.pcap`, or `Gmail.flow.csv` as a complete and valid first reference. Do not require a directory path before the first tool call.

Execution discipline:

- Do not call `ls` or browse directories first when the user already named the target dataset.
- Do not call `web_search` to find a local dataset that should live under `datasets/network-traffic/`.
- Do not call `ask_clarification` before you have actually tried `network_traffic_prepare` or `network_traffic_analyze` with the user-provided dataset reference.
- Do not try to rediscover the processed output path after `network_traffic_prepare` if the tool already returned `flow.csv`.
- Use the returned `flow.csv` path from `network_traffic_prepare` directly as the next `network_traffic_analyze` reference.
- Prefer filename, relative dataset path, or the tool-returned path. Only escalate to manual path questions if the tool itself reports ambiguity or not-found.

Always resolve local dataset references before asking the user for upload or an absolute path.

Prefer the dedicated backend tool first:

- `network_traffic_prepare`
- `network_traffic_analyze`

These tools run on the host and can access the repository datasets directly. Use them before generic file tools, generic bash steps, or any `/mnt/user-data/...` path probing.

Hard requirement for named local datasets:

- If the user names `Geodo.pcap`, `Gmail.pcap`, `Gmail.flow.csv`, or any similar dataset reference, call the dedicated network traffic tool first.
- Do not inspect `/mnt/user-data/uploads`, `/mnt/user-data/workspace`, or other sandbox directories before trying the dedicated tool.
- Do not claim a dataset is missing until `network_traffic_prepare` or `network_traffic_analyze` returns an actual not-found or ambiguous result.
- Do not ask the user for an absolute path on the first turn when they already provided a concrete filename.
- Only ask for a more specific path after the dedicated tool explicitly reports `ambiguous` or `not_found`.

Prefer files under:

- `datasets/network-traffic/raw/`
- `datasets/network-traffic/processed/`

If the user provided explicit file paths, use those instead.

The script also accepts shorthand file references:

- full relative path, for example `datasets/network-traffic/processed/ustc_tfc2016/flow/Gmail.flow.csv`
- dataset-relative path suffix, for example `ustc_tfc2016/flow/Gmail.flow.csv`
- exact filename, for example `Gmail.flow.csv`

If a shorthand matches multiple files, stop and ask the user to disambiguate instead of guessing.

Important local-file rule:

- If the user names a file that should exist under `datasets/network-traffic/`, do not ask the user to upload it first.
- Do not require a public URL for files that are expected to be on the server already.
- First try the filename or relative path directly with `--files`, because the script resolves shorthand recursively under both default roots.
- For example, if the user says `Gmail.flow.csv`, call the script with `--files Gmail.flow.csv` before asking any follow-up question.
- For example, if the user says `Geodo.pcap`, call `network_traffic_prepare(references=["Geodo.pcap"])` before asking for any directory or path detail.
- Only ask for upload or an explicit path if the shorthand resolution actually fails or matches multiple files.

Preferred order when the user asks about a known local dataset:

1. call the dedicated tool with the exact user-provided filename or reference
2. if resolved, pass the resolved match into `analyze.py`
3. if ambiguous, ask the user to disambiguate between the returned candidates
4. if not found, ask for a more specific local path
5. only suggest upload if local resolution truly fails and the user confirms the file is not already on the server

Preferred tool call:

```text
network_traffic_analyze(
  reference="Gmail.flow.csv",
  action="summary"
)
```

Preferred PCAP preparation call:

```text
network_traffic_prepare(
  references=["corp-day1.pcap", "corp-day2.pcap"],
  dataset_name="corp-day1-day2"
)
```

Then analyze the resulting `flow.csv`:

```text
network_traffic_analyze(
  reference="corp-day1-day2.flow.csv",
  action="summary"
)
```

If `network_traffic_prepare` returns an explicit `flow.csv` path, use that exact path in the next step instead of probing the filesystem.

Example for a named local PCAP:

```text
network_traffic_prepare(
  references=["Geodo.pcap"]
)
```

Then immediately:

```text
network_traffic_analyze(
  reference="<flow.csv returned by network_traffic_prepare>",
  action="distribution",
  dimension="dst_port",
  limit=10
)
```

The resolver is generic for future datasets. It searches recursively under both:

- `datasets/network-traffic/processed/`
- `datasets/network-traffic/raw/`

and supports:

- exact filenames
- dataset-relative suffixes
- normalized name matches for similar naming conventions

For example, if the resolver returns one local path, immediately use that path in the analysis command rather than asking for upload.

Host execution rule:

- Use `network_traffic_analyze` as the primary execution path.
- Use `network_traffic_prepare` first only for PCAP input, then `network_traffic_analyze` on the generated `flow.csv`.
- If the user already provided `csv`, `parquet`, `json`, `jsonl`, `xlsx`, or `xls`, do not preprocess; analyze directly.
- Only fall back to manual host-side script execution if the tools themselves fail unexpectedly.
- Do not inspect `/mnt/user-data/datasets` or require sandbox upload when the dataset is expected to already exist under `datasets/network-traffic/`.
- Treat `datasets/network-traffic/` as the authoritative local dataset root for this skill.

### 2. Inspect first

For tabular datasets, always start with `inspect` unless the schema was already confirmed in the current thread.

For PCAP datasets:

1. preprocess first with `network_traffic_prepare`
2. use the generated `flow.csv` returned by the tool as the default analysis input
3. then inspect or summarize that generated flow file

```text
network_traffic_analyze(reference="flows.csv", action="inspect")
```

### 3. Choose the smallest useful action

- Use `summary` for overall traffic statistics and protocol mix
- Use `topn` for most active IPs, ports, protocols, or destinations
- Use `timeseries` for minute, hour, or day buckets
- Use `distribution` for categorical breakdowns
- Use `filter` for narrowed rows before export or review
- Use `aggregate` for grouped metrics
- Use `detect-anomaly` for simple rule-based anomaly discovery
- Use `query` when the user explicitly asks for SQL or you need a custom computation
- Use `export` when the result set is too large for chat or the user asks for a file

### 4. Prefer the unified `flows` view

The script standardizes common fields into a single DuckDB view named `flows`.

Canonical fields:

- `timestamp`
- `src_ip`
- `dst_ip`
- `src_port`
- `dst_port`
- `protocol`
- `bytes`
- `packets`
- `flow_duration`
- `direction`
- `action`
- `source_table`
- `source_file`

### 5. Common command patterns

Summary:

```text
network_traffic_analyze(reference="flows.csv", action="summary")
```

Top source IPs by bytes:

```text
network_traffic_analyze(
  reference="flows.csv",
  action="topn",
  dimension="src_ip",
  metric="bytes",
  limit=10
)
```

Hourly traffic trend:

```text
network_traffic_analyze(
  reference="flows.csv",
  action="timeseries",
  interval="hour"
)
```

Filtered export:

```text
network_traffic_analyze(
  reference="flows.csv",
  action="export",
  filters="[{\"field\":\"dst_port\",\"op\":\"in\",\"value\":[80,443]},{\"field\":\"timestamp\",\"op\":\"gte\",\"value\":\"2026-03-16T00:00:00\"}]",
  output_file="datasets/network-traffic/outputs/web-traffic.csv"
)
```

Custom SQL:

```text
network_traffic_analyze(
  reference="flows.csv",
  action="query",
  sql="SELECT src_ip, COUNT(*) AS flows, SUM(bytes) AS total_bytes FROM flows GROUP BY src_ip ORDER BY total_bytes DESC LIMIT 10"
)
```

### 6. Filters

Pass filters as JSON. Supported operators:

- `eq`
- `neq`
- `gt`
- `gte`
- `lt`
- `lte`
- `in`
- `contains`
- `startswith`
- `endswith`
- `in_cidr`

### 7. Answering rules

When you answer the user:

- name the files used
- mention important filters and time windows
- summarize the main finding first
- mention export paths when files were written
- say when a result is based on simple anomaly heuristics rather than a trained detector
- do not claim a local dataset is inaccessible unless the script returned a real resolution or file error
- do not propose synthetic sample generation when the user already named a concrete local dataset such as `Gmail.flow.csv`
- do not ask for an absolute path before trying the resolver on the local dataset reference
- do not ask for `/mnt/user-data/...` paths when the user is clearly referring to a server-side dataset managed under `datasets/network-traffic/`
- do not claim the server dataset is inaccessible unless `network_traffic_analyze` itself returns a concrete file-resolution or execution error

### 8. Boundaries

Current boundaries:

- Tabular datasets are analyzed directly with `network_traffic_analyze`
- PCAP input is supported only through `network_traffic_prepare`, which uses host-side Python and `scapy`, then hands the generated `flow.csv` to `network_traffic_analyze`
- Do not treat PCAP input as directly queryable tabular data before preprocessing
- vector retrieval or RAG is still out of scope
- MCP-backed live data access is still out of scope
