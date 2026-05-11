# Capability Catalog

Referenced from `SKILL.md`. Two parts: capability matrix (when to use what), and the full action index.

## Capability Matrix

| Capability | Use when | Primary actions |
| --- | --- | --- |
| Overview and inventory | Dataset sanity check, records/bytes/packets/time range | `inspect`, `summary`, `overview-report`, `distribution` |
| Heavy hitters and concentration | Top IPs, ports, services, traffic concentration | `topn`, `aggregate`, `query` |
| Distribution and protocol mix | Protocol/port/service/direction mix | `distribution`, `aggregate`, `protocol-review` |
| Time-series and burst analysis | Hourly/daily profiles, burst windows, volume spikes | `timeseries`, `periodicity-review`, `detect-anomaly`, `forecast-traffic` |
| Asset and endpoint analysis | Which hosts contact the most peers, broad relationships | `aggregate`, `topn`, `query`, `graph-analysis` |
| Session quality and outcome | Allowed vs denied, reset-heavy, failed connections | `session-review`, `distribution`, `detect-anomaly`, `query` |
| Protocol field investigation | DNS/TLS/HTTP field review, TCP flags | `distribution`, `protocol-review`, `topn`, `query`, `filter` |
| Anomaly screening | Scan-like sources, rare ports, volume spikes, failure-heavy | `detect-anomaly` (hybrid engine), `query` |
| Packet-level review | TCP flags, SYN-only, RST-heavy, handshake, ICMP | `packet-review`, `session-review`, `detect-anomaly` rules |
| Encrypted traffic analysis | JA3 fingerprinting, TLS anomalies, tunnel detection | `encrypted-flow-analysis`, `signature-review` |
| Device identification | IoT/mobile/server/desktop fingerprinting | `device-identification` |
| Behavior analysis | Baseline building, behavioral shifts, drift | `behavior-analysis`, `detect-concept-drift` |
| Graph analysis | Communication graph, community detection, attack paths | `graph-analysis` |
| Root cause analysis | Explain anomaly candidates, feature contribution | `root-cause-analysis` |
| Threat intelligence | IOC matching, MITRE ATT&CK mapping, campaign clustering | `threat-intel-match` |
| Forecasting | Traffic volume prediction, capacity risk, trend shifts | `forecast-traffic`, `detect-concept-drift` |
| QoS analysis | Service-quality triage, throughput/loss/retransmission | `qos-analysis`, `session-review`, `packet-review` |

## Action Index

| Action | Purpose | Typical view/input |
| --- | --- | --- |
| `inspect` | Schema validation, canonical field mapping, time semantics | flow.csv or packet.csv |
| `summary` | Dataset sanity check, record/byte/packet/time totals | flow.csv or packet.csv |
| `topn` | Top source IPs, destination IPs, ports, services | flow.csv or packet.csv |
| `distribution` | Protocol, port, service, direction, action distribution | flow.csv or packet.csv |
| `timeseries` | Hourly/daily traffic profiles and burst windows | flow.csv or packet.csv |
| `aggregate` | Grouped metrics (count, sum:bytes, etc.) | flow.csv or packet.csv |
| `detect-anomaly` | Rule + statistical + ML anomaly scoring | flow.csv or packet.csv |
| `overview-report` | Enterprise overview report | flow.csv or packet.csv |
| `protocol-review` | Protocol field inventory and review | flow.csv or packet.csv |
| `packet-review` | TCP flags, SYN/RST/ICMP behavior, packet-level review | packet.csv |
| `scan-review` | Scan-like source concentration | flow.csv |
| `session-review` | Session outcome, state distribution | flow.csv or packet.csv |
| `short-connection-review` | Wide/narrow short-lived connection patterns | flow.csv |
| `dns-tunnel-review` | DNS tunnel indicators | flow.csv |
| `data-exfiltration-review` | Data exfiltration indicators | flow.csv |
| `lateral-movement-review` | Lateral movement indicators | flow.csv |
| `zeek-review` | Zeek semantic evidence (conn.log, dns.log, http.log, ssl.log) | zeek/ logs |
| `signature-review` | 50+ Aho-Corasick IOC rule matching | flow.csv |
| `periodicity-review` | Periodic behavior detection | flow.csv |
| `risk-fusion-review` | Final risk ranking fusing anomalies, Zeek, signatures | flow.csv |
| `encrypted-flow-analysis` | JA3 fingerprinting, TLS behavior, tunnel detection | flow.csv |
| `device-identification` | Device type classification | flow.csv |
| `behavior-analysis` | Behavior baselines and shift detection | flow.csv |
| `graph-analysis` | Network graph, communities, centrality, attack paths | flow.csv |
| `qos-analysis` | Service-quality triage | flow.csv |
| `root-cause-analysis` | Feature-contribution explanation of anomalies | flow.csv |
| `threat-intel-match` | IOC matching against threat intelligence | flow.csv |
| `forecast-traffic` | Volume prediction and trend forecasting | flow.csv |
| `detect-concept-drift` | Stream drift screening with ADWIN-like mean-shift | flow.csv |
| `filter` | Quick field-based filtering | flow.csv or packet.csv |
| `query` | Custom SQL investigation | flow.csv or packet.csv |
| `export` | Export selected results to CSV/JSON | flow.csv or packet.csv |
| `list-capabilities` | Show all supported actions | — |