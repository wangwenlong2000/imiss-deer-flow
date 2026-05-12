# Question Templates

Use these templates as canonical task shapes for this skill.

## Overview

- Read `Gmail.flow.csv` and provide a traffic overview with records, bytes, packets, time range, unique source IPs, unique destination IPs, and protocol distribution.
- Analyze `Outlook.pcap`, preprocess it first, then summarize the traffic at a high level.
- Read `Virut.flow.csv` and generate an enterprise overview report.
- Analyze `Virut.pcap`, preprocess it first, then generate an enterprise overview report.

## Heavy hitters

- Read `Gmail.flow.csv` and show the top 10 source IPs by total bytes.
- Read `Outlook.flow.csv` and show the top 10 destination ports by bytes.
- Read `Skype.flow.csv` and show the top 20 destination IPs by flow count.

## Distribution

- Read `Outlook.flow.csv` and show the destination port distribution.
- Read `Skype.flow.csv` and show the protocol distribution with records and total bytes.
- Read `Gmail.flow.csv` and show the application protocol distribution.

## Time analysis

- Read `BitTorrent.flow.csv` and show hourly traffic volume.
- Read `Outlook.flow.csv` and identify whether there are obvious traffic spikes by hour.

## Asset and communication analysis

- Read `Gmail.flow.csv` and find the top 20 source IPs by number of unique destination IPs.
- Read `Outlook.flow.csv` and find which destination IPs are contacted by the highest number of unique source IPs.

## Protocol field analysis

- Read `Gmail.flow.csv` and run SQL: `SELECT dns_query, COUNT(*) AS flows FROM flows WHERE dns_query IS NOT NULL GROUP BY dns_query ORDER BY flows DESC LIMIT 20`.
- Read `Outlook.flow.csv` and list the top TLS SNI values by flow count.
- Read `Outlook.flow.csv` and show HTTP host values when present.
- Read `Virut.flow.csv` and run a protocol review for protocol, app protocol, DNS query, TLS SNI, and HTTP host fields.

## Session quality and outcome analysis

- Read `Outlook.flow.csv` and show the distribution of `session_state`.
- Read `Outlook.flow.csv` and analyze the distribution of `action`.
- Read `Outlook.flow.csv` and find reset-heavy or failure-heavy traffic patterns.
- Read `Zeus.flow.csv` and generate a session review report for action, session state, and failure-heavy sources.

## Rule-based anomaly screening

- Read `FTP.flow.csv` and check whether there are rare destination ports.
- Read `Geodo.flow.csv` and check whether there are scan-like source IPs.
- Read `Outlook.flow.csv` and identify suspicious burst windows or volume spikes.
- Read `Zeus.packet.csv` and check whether there are SYN-only scan patterns.
- Read `Virut.packet.csv` and check whether there are reset-heavy sources.
- Read `Virut.packet.csv` and identify handshake-failure patterns.
- Read `Virut.packet.csv` and identify ICMP probing behavior.
- Read `Virut.packet.csv` and identify small-packet burst sources.

## Packet-level review

- Read `Virut.packet.csv` and analyze TCP flags distribution.
- Read `Geodo.packet.csv` and review packet-level scan behavior.
- Read `Outlook.packet.csv` and inspect reset-heavy or handshake-failure patterns.
- Read `Virut.packet.csv` and generate a packet review that includes protocol mix, TCP flags, packet-size profile, and top packet talkers.
- Read `Zeus.packet.csv` and run a protocol review in packet view.
- Read `Geodo.packet.csv` and run a session review in packet view.

## Export

- Read `Outlook.flow.csv` and export only rows where `dst_port` is 80 or 443.
- Read `Gmail.flow.csv` and export only flows where `dns_query` is not null.

## PCAP workflow

- Analyze `Geodo.pcap`, preprocess it first, then summarize the traffic.
- Analyze `Outlook.pcap`, preprocess it first, then analyze destination port distribution.
- Analyze `Geodo.pcap`, preprocess it first, then screen for rare ports and scan-like behavior.
- Analyze `Virut.pcap`, preprocess it first, then run `overview-report` in auto view.
- Analyze `Zeus.pcap`, preprocess it first, then run `packet-review` or packet-focused anomaly checks when the question is about flags, SYN-only, RST, ICMP, or handshake quality.
