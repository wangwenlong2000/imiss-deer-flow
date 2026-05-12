# Analysis Patterns

Use these patterns only after the script has confirmed the available canonical fields.

## Hard usage rules

- Start with `inspect` unless schema has already been confirmed in the current thread
- Use the script action that already fits the investigation goal
- Do not substitute script actions with ad hoc code
- Use SQL only when the built-in actions are not specific enough
- Treat anomaly outputs as leads, not conclusions

## Standard enterprise analysis layers

### 1. Dataset sanity and overview

Questions this layer answers:

- What is the scope of this traffic set
- How many records, bytes, and packets are present
- What is the time range
- How many unique sources and destinations exist
- What is the high-level protocol mix

Preferred actions:

- `inspect`
- `summary`
- `overview-report`
- `distribution`

### 2. Heavy hitters and concentration

Questions this layer answers:

- Which sources dominate bytes or packets
- Which destinations dominate flows
- Which ports dominate communication
- Which protocols or services dominate the dataset

Preferred actions:

- `topn`
- `aggregate`
- `query`

### 3. Distribution and inventory

Questions this layer answers:

- What does the port landscape look like
- What is the protocol and service mix
- How does `action`, `direction`, or `traffic_family` distribute

Preferred actions:

- `distribution`
- `aggregate`

### 4. Time and burst behavior

Questions this layer answers:

- Are there spikes
- Which hour or day is most active
- Is activity smooth or bursty

Preferred actions:

- `timeseries`
- `detect-anomaly --rule volume-spike`
- `query`

### 5. Asset and communication analysis

Questions this layer answers:

- Which hosts contact the most peers
- Which destinations receive the widest spread of traffic
- Which assets, devices, users, or sensors stand out

Preferred actions:

- `aggregate`
- `topn`
- `query`

### 6. Session quality and outcome analysis

Questions this layer answers:

- How much traffic is allowed, denied, blocked, or reset
- Are there abnormal session outcomes
- Are there many short or low-byte connections

Preferred actions:

- `distribution`
- `session-review`
- `aggregate`
- `detect-anomaly --rule failure-rate`
- `query`

### 7. Protocol field investigation

Questions this layer answers:

- Which DNS names are most common
- Which TLS SNIs appear
- Which HTTP hosts appear
- Which rule names or TCP flag patterns stand out

Preferred actions:

- `distribution`
- `protocol-review`
- `topn`
- `query`
- `filter`

### 8. Packet-level review and handshake analysis

Questions this layer answers:

- Are packet-level flags dominated by SYN, RST, or other unusual combinations
- Are there handshake-failure or SYN-only patterns
- Are there ICMP probes or packet-size anomalies
- Do packet-level findings confirm a flow-level suspicion

Preferred actions:

- `packet-review`
- `protocol-review --view packet`
- `session-review --view packet`
- `detect-anomaly --rule syn-scan`
- `detect-anomaly --rule rst-heavy`
- `detect-anomaly --rule handshake-failure`
- `detect-anomaly --rule icmp-probe`
- `detect-anomaly --rule small-packet-burst`

### 9. Rule-based anomaly screening

Questions this layer answers:

- Are there scan-like sources
- Are there rare destination ports
- Are there failure-heavy traffic patterns
- Are there suspicious spikes

Preferred actions:

- `detect-anomaly`
- `query`

## Recommended investigation order

### Short triage

1. `inspect`
2. `overview-report`
3. One targeted action:
   - `topn`
   - `distribution`
   - `timeseries`
   - `detect-anomaly`

### Full enterprise review

1. `inspect`
2. `overview-report`
3. `topn` or `scan-review`
4. `protocol-review`
5. `session-review`
6. `timeseries`
7. `detect-anomaly`
8. `query`
9. `export` only if needed

## Interpretation discipline

- Explain what the data shows before saying what it may mean
- Avoid malware or product attribution from ports alone
- Avoid claiming normality or maliciousness from one weak signal
- Call out uncertainty explicitly when interpretation is inferred rather than measured
