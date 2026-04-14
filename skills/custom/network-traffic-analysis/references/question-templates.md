# Question Templates

Use these templates as canonical task shapes for this skill.

## Analysis-only

### Overview

- Read `Gmail.flow.csv` and provide a traffic overview with records, bytes, packets, time range, unique source IPs, unique destination IPs, and protocol distribution.
- Analyze `Virut.flow.csv` and generate an enterprise overview report.
- Analyze `Zeus.pcap`, preprocess it if needed, then generate an enterprise overview report.

### Scan and anomaly review

- Read `Geodo.flow.csv` and review scan-like source behavior.
- Read `Virut.flow.csv` and identify suspicious burst windows or volume spikes.
- Read `FTP.flow.csv` and check whether there are rare destination ports.

### Short-connection review

- Read `BitTorrent.flow.csv` and run a formal short-connection review.
- Read `Virut.flow.csv` and summarize wide short and narrow short connection behavior.
- Read `Zeus.flow.csv` and identify low-byte short-lived flow concentration.

### Protocol field analysis

- Read `Outlook.flow.csv` and list the top TLS SNI values by flow count.
- Read `Virut.flow.csv` and run a protocol review for protocol, app protocol, DNS query, TLS SNI, and HTTP host fields.
- Read `Zeus.flow.csv` and inspect DNS and HTTP communication hints.

### Packet-level review

- Read `Virut.packet.csv` and analyze TCP flags distribution.
- Read `Geodo.packet.csv` and review packet-level scan behavior.
- Read `Virut.packet.csv` and identify handshake-failure patterns.
- Read `Zeus.packet.csv` and run a packet review focused on TCP flags and packet-size behavior.

## RAG-only

- From the indexed data, retrieve the historical communication profile for `Virut`.
- Search the shared traffic index for scan-like evidence related to `Zeus`.
- Search indexed historical data for short-connection evidence in `BitTorrent`.
- From the Elasticsearch index, retrieve protocol and anomaly evidence for `Virut` without re-running local analysis.

## RAG plus analysis

- Retrieve historical evidence for `Virut` from the index first, then validate the current `Virut.flow.csv` with formal analysis.
- Use indexed `Zeus` evidence as context, then analyze the local `Zeus.flow.csv` and report the measured conclusion.
- Search the index for `BitTorrent` communication patterns first, then verify the local processed file and give a final answer based on the measured analysis.

## Time analysis

- Read `BitTorrent.flow.csv` and show relative-time traffic volume by hour.
- Read `Zeus.flow.csv` and show hourly traffic volume using absolute timestamps.
- Read `Outlook.flow.csv` and identify whether there are obvious traffic spikes by hour.

## Export and drill-down

- Read `Outlook.flow.csv` and export only rows where `dst_port` is 80 or 443.
- Read `Gmail.flow.csv` and export only flows where `dns_query` is not null.
- Read `Virut.flow.csv` and run SQL to inspect IRC communication on port 6667.
