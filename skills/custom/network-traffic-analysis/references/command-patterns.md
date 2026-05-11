# Standard Command Patterns

Referenced from `SKILL.md`. All command patterns for analysis, reporting, and export.

## Core Analysis

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

Top-N (source IPs, destination IPs, ports, services):

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action topn --dimension src_ip --metric bytes --limit 10
```

Hourly trend:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action timeseries --interval hour
```

Distribution:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action distribution --dimension protocol
```

Custom SQL:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action query --sql "<enterprise-investigation-sql>"
```

Capability list:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action list-capabilities
```

## Machine-Readable Output

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action <action> --format skill-result-json
```

## Anomaly Detection

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action detect-anomaly --anomaly-engine hybrid
```

Engines: `rule`, `iforest`, `lof`, `rcf`, `hybrid` (default and preferred).

## Specialized Review Actions

Short-connection review:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action short-connection-review --limit 20
```

DNS tunnel review:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action dns-tunnel-review --limit 20
```

Data exfiltration review:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action data-exfiltration-review --limit 20
```

Lateral movement review:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action lateral-movement-review --limit 20
```

## Filtering and Aggregation

Quick filtering:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action filter --filters '<json-filters>' --limit 50
```

Grouped aggregation:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action aggregate --group-by src_ip,dst_port --metrics count,sum:bytes
```

Export:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <resolved-input-path> --action export --output-file <destination-file> --limit 100
```

## RAG Chain

Build documents:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/build_rag_docs.py --files <resolved-flow-csv> --format json
```

Build embeddings:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/embed_rag_docs.py --files <resolved-rag-docs-jsonl> --format json
```

Index into Elasticsearch:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/index_rag_docs.py --files <resolved-rag-embeddings-jsonl> --format json
```

Search:

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/rag_search.py --query "<user-question>" --format json
```

Full rebuild (shared-index scenario):

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/build_full_rag_index.py \
  --files <resolved-pcap-or-flow-csv> \
  --dataset-name <dataset-name> \
  --replace-source \
  --verify-search \
  --format json
```

Force full rebuild (docs + embeddings + index):

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/build_full_rag_index.py \
  --files <resolved-pcap-or-flow-csv> \
  --dataset-name <dataset-name> \
  --force-docs \
  --force-embeddings \
  --force-index \
  --replace-source \
  --verify-search \
  --format json
```

Dry-run (preview what would be processed):

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/build_full_rag_index.py \
  --files <resolved-pcap-or-flow-csv> \
  --dataset-name <dataset-name> \
  --replace-source \
  --dry-run \
  --format json
```

## Final Incident Report

```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/generate_incident_report.py --files <resolved-input-path> --output-file /mnt/user-data/outputs/<report-name>.md --format json
```

After generating a report, call `present_files` with the generated `/mnt/user-data/outputs/...` path.
