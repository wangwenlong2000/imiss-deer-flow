# Advanced Analysis Actions

Referenced from `SKILL.md`. Use these actions for specialized analysis scenarios beyond basic inspection.

## Phase 1: Enhanced Security Capabilities

### Encrypted Traffic Analysis
**Action**: `encrypted-flow-analysis`

Use for:
- JA3/JA3S TLS fingerprinting from real TLS handshake metadata when preprocessing has ClientHello/ServerHello evidence
- JA3 match review against the local versioned fingerprint database under `data/ja3_fingerprints.json`
- Encrypted application classification with explicit evidence levels (`ja3_match` is strong; port/flow inference is weak)
- Encrypted tunnel detection (VPN, SSH, C2 beacons)
- TLS behavior anomalies (weak ciphers, expired certs, DGA domains)

If `tls_metadata_source` is `missing`, treat application labels as weak inference only. Do not present port-based HTTPS classification as a JA3 or application fingerprint match.

Command:
```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <flow.csv> --action encrypted-flow-analysis --limit 20
```

Local JA3 database maintenance:
```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/update_ja3_fingerprints.py --input <ja3-records.csv-or-json> --source <provider-name> --dry-run
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/update_ja3_fingerprints.py --input <ja3-records.csv-or-json> --source <provider-name>
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/update_ja3_fingerprints.py --sync-sslbl --dry-run
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/update_ja3_fingerprints.py --sync-sslbl
```

Accepted input fields include `fingerprint`, `ja3`, or `ja3_hash`, plus optional `application`, `category`, `risk_level`, `confidence`, `source`, `source_url`, and `description`. Imported records are merged into `data/ja3_fingerprints.json` by fingerprint value.

### Device Identification
**Action**: `device-identification`

Use for:
- Identifying device types (IoT, mobile, server, desktop, network equipment)
- Fingerprinting based on TLS/HTTP/DHCP characteristics

Command:
```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <flow.csv> --action device-identification --limit 50
```

### Behavior Analysis
**Action**: `behavior-analysis`

Use for:
- Building behavior baselines for hosts/users
- Detecting behavioral shifts (volume spikes, destination spread, time shifts)
- Profiling entity behavior (bytes, packets, peers, ports, protocols)

Command:
```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <flow.csv> --action behavior-analysis --limit 20
```

For verified baseline/current comparisons, pass explicit time windows:
```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <flow.csv> --action behavior-analysis --baseline-start 2026-04-29T00:00:00Z --baseline-end 2026-04-29T12:00:00Z --current-start 2026-04-29T12:00:00Z --current-end 2026-04-29T18:00:00Z --limit 20
```

### QoS and Service Quality Analysis
**Action**: `qos-analysis`

Use for:
- Service-quality triage using throughput and sustained low-rate flow evidence
- Loss, retransmission, and session-failure screening with explicit direct-vs-proxy measurement reporting
- Packet-derived RTT/retransmission evidence when preprocessing used `tshark` TCP analysis enrichment
- Timing-instability hotspot review using ordered flow timestamps
- QoS/QoE-oriented ranking before packet-level troubleshooting

Command:
```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <flow.csv> --action qos-analysis --limit 20
```

### Graph Analysis
**Action**: `graph-analysis`

Use for:
- Network communication graph construction
- Community detection (Louvain algorithm) - finding groups of related hosts
- Centrality analysis (PageRank, Betweenness) - identifying critical nodes
- Attack path discovery between source and target IPs

Command:
```bash
# Basic graph analysis
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <flow.csv> --action graph-analysis --limit 100

# Attack path discovery
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <flow.csv> --action graph-analysis --source-ip 10.0.0.1 --target-ip 10.0.0.50
```

## Phase 2: Advanced Analytics

### Root Cause Analysis (heuristic feature contribution)
**Action**: `root-cause-analysis`

Use for:
- Explaining which fields most contributed to heuristic anomaly scores
- Feature contribution ranking (which fields caused the anomaly score)
- Automated explanation generation and recommended investigation actions

Command:
```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <flow.csv> --action root-cause-analysis --limit 10
```

### Threat Intelligence Matching
**Action**: `threat-intel-match`

Use for:
- Matching indicators (IPs, domains, URLs) against threat intelligence databases
- MITRE ATT&CK technique mapping
- Campaign clustering
- Known C2, malware, phishing, and cryptomining indicator detection

Command:
```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <flow.csv> --action threat-intel-match --limit 50
```

### Traffic Forecasting
**Action**: `forecast-traffic`

Use for:
- Predicting future traffic volumes (Holt-Winters, linear regression)
- Capacity risk estimation
- Trend shift detection
- Anomaly prediction for future periods

Command:
```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <flow.csv> --action forecast-traffic --horizon 24
```

### Concept Drift Detection
**Action**: `detect-concept-drift`

Use for:
- Detecting concept drift in data streams with ADWIN-like mean-shift screening
- Identifying when the underlying traffic patterns have changed
- Model adaptation recommendations

Command:
```bash
cd /mnt/skills/custom/network-traffic-analysis && python3 scripts/analyze.py --files <flow.csv> --action detect-concept-drift
```
