# Network Traffic Input / Output Contract

This file maps the common custom skill standard to `network-traffic-analysis`.

Use this reference only when the agent needs structured integration output, when a downstream system needs machine-readable results, or when explaining how this skill maps to the shared `SkillResult` format.

## Input Envelope Mapping

Common invocation shape:

```json
{
  "schema_version": "1.0",
  "request_id": "uuid",
  "skill_name": "network-traffic-analysis",
  "scenario": "network_traffic",
  "capability": "encrypted-flow-analysis",
  "input": {
    "data_sources": [],
    "parameters": {},
    "filters": {},
    "context": {}
  }
}
```

### data_sources

Supported `data_sources[].data_type` values:

- `pcap`
- `pcapng`
- `cap`
- `flow_csv`
- `packet_csv`
- `csv`
- `parquet`
- `json`
- `jsonl`
- `excel`
- `rag_docs`

Mapping rules:

- `data_sources[].uri` maps to `--files <uri>`.
- If `data_type` is `pcap`, `pcapng`, or `cap`, run `prepare_pcap.py` first, then use the returned `flow_csv`.
- If `data_type` is tabular, run `analyze.py` directly.
- If multiple data sources are provided, pass all resolved tabular files to `--files`.

Example:

```json
{
  "source_id": "mta-2018-10-10",
  "source_type": "local_file",
  "uri": "/mnt/datasets/network-traffic/processed/MTA-2018-10-10-fake-updater/MTA-2018-10-10-fake-updater.flow.csv",
  "media_type": "text/csv",
  "data_type": "flow_csv",
  "role": "primary",
  "metadata": {
    "dataset_name": "MTA-2018-10-10-fake-updater"
  }
}
```

### parameters

Common parameters:

- `limit` maps to `--limit`.
- `view` maps to `--view`.
- `dimension` maps to `--dimension`.
- `metric` maps to `--metric`.
- `interval` maps to `--interval`.
- `sql` maps to `--sql`.
- `rule` maps to `--rule`.
- `anomaly_engine` maps to `--anomaly-engine`.
- `source_ip` maps to `--source-ip`.
- `target_ip` maps to `--target-ip`.
- `horizon` maps to `--horizon`.
- `drift_metric` maps to `--drift-metric`.
- `drift_order_by` maps to `--drift-order-by`.
- `output_file` maps to `--output-file`.

### filters

Mapping rules:

- `filters.time_range.start` maps to `--start-time`.
- `filters.time_range.end` maps to `--end-time`.
- `filters.where` maps to `--filters`.

Use JSON filters for structured field filters.

## Capability Mapping

The action surface below covers every entry in `scripts/capability_catalog.py`
(`ACTION_DESCRIPTIONS`).

| Capability | CLI action | Structured output status | Normal output sections |
|---|---|---|---|
| `list-capabilities` | `--action list-capabilities` | catalog (JSON catalog) | JSON capability catalog |
| `inspect` | `--action inspect` | native structured | `summary`, `findings`, `evidence`, `diagnostics` |
| `summary` | `--action summary` | native structured | `summary`, `findings`, `evidence`, `diagnostics` |
| `topn` | `--action topn` | native structured | `summary`, `findings`, `evidence`, `diagnostics` |
| `distribution` | `--action distribution` | native structured | `summary`, `findings`, `evidence`, `diagnostics` |
| `timeseries` | `--action timeseries` | native structured | `summary`, `findings`, `evidence`, `artifacts`, `diagnostics` |
| `aggregate` | `--action aggregate` | native structured | `summary`, `findings`, `evidence`, `diagnostics` |
| `detect-anomaly` | `--action detect-anomaly` | native structured | `summary`, `findings`, `evidence`, `diagnostics` |
| `overview-report` | `--action overview-report` | wrapped text | `summary`, `findings`, `evidence`, `diagnostics` |
| `protocol-review` | `--action protocol-review` | native structured | `summary`, `findings`, `evidence`, `diagnostics` |
| `packet-review` | `--action packet-review` | native structured | `summary`, `findings`, `evidence`, `diagnostics` |
| `scan-review` | `--action scan-review` | native structured | `summary`, `findings`, `evidence`, `diagnostics` |
| `session-review` | `--action session-review` | native structured | `summary`, `findings`, `evidence`, `diagnostics` |
| `short-connection-review` | `--action short-connection-review` | native structured | `summary`, `findings`, `evidence`, `diagnostics` |
| `dns-tunnel-review` | `--action dns-tunnel-review` | native structured | `summary`, `findings`, `evidence`, `diagnostics` |
| `data-exfiltration-review` | `--action data-exfiltration-review` | native structured | `summary`, `findings`, `evidence`, `diagnostics` |
| `lateral-movement-review` | `--action lateral-movement-review` | native structured | `summary`, `findings`, `evidence`, `diagnostics` |
| `zeek-review` | `--action zeek-review` | native structured | `summary`, `findings`, `evidence`, `diagnostics` |
| `signature-review` | `--action signature-review` | native structured | `summary`, `findings`, `evidence`, `diagnostics` |
| `periodicity-review` | `--action periodicity-review` | native structured | `summary`, `findings`, `evidence`, `diagnostics` |
| `risk-fusion-review` | `--action risk-fusion-review` | native structured | `summary`, `findings`, `evidence`, `diagnostics` |
| `encrypted-flow-analysis` | `--action encrypted-flow-analysis` | native structured | `summary`, `findings`, `evidence`, `diagnostics` |
| `device-identification` | `--action device-identification` | wrapped text | `summary`, `findings`, `evidence`, `diagnostics` |
| `behavior-analysis` | `--action behavior-analysis` | wrapped text | `summary`, `findings`, `evidence`, `diagnostics` |
| `graph-analysis` | `--action graph-analysis` | wrapped text | `summary`, `findings`, `evidence`, `diagnostics` |
| `qos-analysis` | `--action qos-analysis` | wrapped text | `summary`, `findings`, `evidence`, `diagnostics` |
| `root-cause-analysis` | `--action root-cause-analysis` | wrapped text | `summary`, `findings`, `evidence`, `diagnostics` |
| `threat-intel-match` | `--action threat-intel-match` | native structured | `summary`, `findings`, `evidence`, `diagnostics` |
| `forecast-traffic` | `--action forecast-traffic` | wrapped text | `summary`, `findings`, `evidence`, `diagnostics` |
| `detect-concept-drift` | `--action detect-concept-drift` | wrapped text | `summary`, `findings`, `evidence`, `diagnostics` |
| `filter` | `--action filter` | wrapped text/table | `summary`, `evidence`, `diagnostics` |
| `query` | `--action query` | wrapped text/table | `summary`, `evidence`, `diagnostics` |
| `export` | `--action export` | artifact/export | `summary`, `artifacts`, `diagnostics` |

### Primitive Actions (Native Structured)

These 7 actions have native structured evidence via `build_skill_result_parts`:

- `inspect`
- `summary`
- `topn`
- `distribution`
- `timeseries`
- `aggregate`
- `detect-anomaly`

They emit machine-readable table evidence using `columns` + `rows`, metric
evidence using `metrics`, and ingestion diagnostics under
`diagnostics.data_quality.ingestion`.

## SkillResult Output

When structured output is required, call:

```bash
python3 scripts/analyze.py --files <file> --action <action> --format skill-result-json
```

The command wraps the existing action output in the shared `SkillResult` envelope.

Top-level shape:

```json
{
  "schema_version": "1.0",
  "request_id": "generated-uuid",
  "skill_name": "network-traffic-analysis",
  "scenario": "network_traffic",
  "capability": "encrypted-flow-analysis",
  "status": "success",
  "result": {
    "summary": {},
    "findings": [],
    "evidence": [],
    "artifacts": []
  },
  "diagnostics": {},
  "errors": []
}
```

## Output Section Rules

### summary

Required fields:

- `title`
- `overview`

Recommended fields:

- `severity`
- `confidence`
- `key_metrics`

For wrapper output, `title` is derived from the action name and `overview` is derived from the first meaningful line of the action report.

### findings

Use findings for analyst-facing conclusions, such as:

- JA3 match
- threat-intel match
- anomalous host
- suspicious scan source
- QoS degradation
- high-risk graph node

Each finding should include:

- `finding_id`
- `type`
- `severity`
- `confidence`
- `title`
- `description`
- `entities`
- `evidence_refs`

If the current action only returns a text report, the wrapper may leave `findings` empty and preserve the report under `evidence`.

### evidence

Network traffic normally uses these evidence types:

- `table`
- `metric`
- `timeseries`
- `graph`
- `text`
- `file`

The wrapper places the original action report in a `text` evidence item:

```json
{
  "evidence_id": "e-001",
  "type": "text",
  "title": "Raw Action Output",
  "content": "<original report text>"
}
```

Actions can later be upgraded to emit richer `table`, `timeseries`, or `graph` evidence directly.

Current second-level structured actions:

- `encrypted-flow-analysis` emits structured `summary`, deduplicated `findings`, metric evidence, JA3 table evidence, risk table evidence, application-classification table evidence, tunnel-indicator table evidence, and the raw text report.
- `threat-intel-match` emits structured `summary`, deduplicated IOC-match `findings`, metric evidence, coverage-status evidence, match table evidence, coverage warnings, and the raw text report.
- `risk-fusion-review` emits structured `summary`, fused source-risk `findings`, coverage evidence, final risk table evidence, evidence-mix table evidence, fusion notes, and the raw text report.

### artifacts

Use artifacts for generated files:

- exported CSV
- exported JSON
- markdown report
- RAG index manifest
- generated chart/image

### diagnostics

Recommended diagnostics:

- `warnings`
- `data_quality.ingestion` — list of per-table ingestion quality objects, each with:
  - `table`
  - `file`
  - `ingestion_mode`
  - `rows_loaded`
  - `physical_data_rows`
  - `approx_dropped_rows` — estimated lines dropped during CSV load
    (`physical_data_rows - rows_loaded`); may include comments or blank lines.
  - `approx_null_key_rows` — rows where `src_ip IS NULL OR dst_ip IS NULL`.
    Treat this as a data-quality proxy signal, not proof of null padding.
    Legitimate captures (ARP, DHCP, broadcast) can also produce NULL IPs.
- `provenance`
- `runtime`

When an action uses external or local intelligence sources, include source provenance in diagnostics.

### errors

Use `errors` for both failed and partial-success cases.

Example:

```json
{
  "code": "MISSING_REQUIRED_FIELD",
  "message": "encrypted-flow-analysis requires dst_port.",
  "severity": "error",
  "recoverable": true,
  "details": {
    "missing_fields": ["dst_port"]
  }
}
```

## Status Rules

- `success`: Main task completed.
- `partial_success`: Main task completed but some inputs failed, fields were missing, or evidence quality was reduced.
- `failed`: Main task could not complete.

## Migration Notes

Current actions still support human-readable text output by default.

For integration:

1. Use `--format skill-result-json`.
2. Consume `result.summary` for cards and high-level reports.
3. Consume `result.evidence` for detailed rendering.
4. Treat `diagnostics.warnings` and `errors` as first-class output, not prose-only notes.

Future improvements can upgrade high-value actions to populate structured `findings` and typed `evidence` directly instead of only wrapping text output.
