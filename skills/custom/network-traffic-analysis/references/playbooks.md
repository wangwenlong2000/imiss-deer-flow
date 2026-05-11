# Investigation Playbooks

Referenced from `SKILL.md`. Complete list of standard investigation playbooks (A-M).

## Basic Investigation Playbooks

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
3. `periodicity-review`
4. `detect-anomaly --rule volume-spike --anomaly-engine hybrid`

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
3. If Zeek logs exist, `zeek-review`
4. `query` or `distribution` on one of:
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

## Advanced Investigation Playbooks

### Playbook H. Encrypted traffic investigation
Run in this order:

1. `inspect`
2. `encrypted-flow-analysis --limit 20`
3. If high-risk TLS indicators found, `root-cause-analysis`
4. `threat-intel-match` to check for known C2/malware signatures

### Playbook I. Behavior anomaly investigation
Run in this order:

1. `inspect`
2. `behavior-analysis --limit 20`
3. If deviation detected, `detect-concept-drift`
4. `root-cause-analysis` to explain the shift

### Playbook J. Network graph investigation
Run in this order:

1. `inspect`
2. `graph-analysis --limit 100`
3. `analyze-communities` to find related hosts
4. If attack path found, `threat-intel-match` on involved IPs

### Playbook K. Threat intelligence enrichment
Run in this order:

1. `inspect`
2. `overview-report`
3. `threat-intel-match --limit 50`
4. If matches found, `risk-fusion-review`

### Playbook L. Capacity and trend forecasting
Run in this order:

1. `inspect`
2. `timeseries --interval hour`
3. `forecast-traffic --horizon 24`
4. `detect-concept-drift` to check for trend shifts

### Playbook M. QoS and service health investigation
Run in this order:

1. `inspect`
2. `qos-analysis --limit 20`
3. If degraded paths are found, `session-review --view auto`
4. If retransmission or reset pressure is suspected, `packet-review --view packet`

## Example User Questions

These are representative question shapes mapped to playbooks:

### Overview
- "Read `X.flow.csv` and provide a traffic overview with records, bytes, time range, protocol distribution."
- "Preprocess `X.pcap` then generate an enterprise overview report."

### Scan and anomaly
- "Review scan-like source behavior in `X.flow.csv`."
- "Are there suspicious burst windows, volume spikes, or rare destination ports?"

### Protocol and packet
- "List the top TLS SNI values by flow count."
- "Analyze `X.packet.csv` TCP flags distribution and handshake-failure patterns."

### RAG retrieval
- "Search the shared traffic index for scan-like evidence related to `X`."
- "Retrieve historical protocol and anomaly evidence for `X` without re-running analysis."

### RAG plus analysis
- "Retrieve indexed evidence for `X` first, then validate with formal analysis on the current `X.flow.csv`."

### Time and export
- "Show hourly traffic volume using absolute timestamps."
- "Export only flows where `dst_port` is 80 or 443."
