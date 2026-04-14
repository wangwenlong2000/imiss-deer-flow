# Preprocessing And Time Semantics

This reference defines how `prepare_pcap.py` should be interpreted.

## Flow semantics

Current `flow.csv` output is session-oriented, not whole-file conversation aggregation.

### TCP sessionization

- `SYN` preferentially starts a new session
- `FIN` or `RST` closes a session
- idle timeout also closes a session
- current default TCP idle timeout is `60s`

### UDP, ICMP, and other protocols

- flow/session grouping uses idle timeout
- current default non-TCP idle timeout is `30s`

### Resulting expectations

- one endpoint pair may produce multiple flow rows
- `duration_ms`, `src_bytes`, `dst_bytes`, `src_packets`, `dst_packets`, and `direction` should be interpreted at session level
- `flow_start_reason` and `flow_end_reason` explain why a session began and ended

## Session state expectations

Current preprocessing may emit states such as:

- `SYN_ONLY`
- `SYN`
- `SYN_ACK`
- `ACK`
- `FIN`
- `RST`
- `ESTABLISHED`

Not every protocol or capture quality level will populate a session state.

## Time semantics

Two time modes are supported.

### Absolute time

Use when packet times are real wall-clock timestamps.

Expected fields:

- `timestamp`
- `end_time`
- `time_is_relative = false`

### Relative time

Use when packet times are offsets from capture start.

Expected fields:

- packet level:
  - `relative_time_s`
  - `time_is_relative = true`
- flow level:
  - `start_relative_time_s`
  - `end_relative_time_s`
  - `time_is_relative = true`

Rules:

- Do not reinterpret relative time as `1970-01-01...`
- Use relative buckets such as `t+0s` for analysis and RAG summaries

## Duration semantics

- `flow_duration` is stored in seconds
- `duration_ms` is the millisecond convenience field
- nonzero microsecond-scale flows should not collapse to `0ms`

## Why this matters

These preprocessing semantics affect:

- `analyze.py`
- `build_rag_docs.py`
- `rag_search.py`
- all downstream indexing and evaluation

Any semantic change here requires downstream rebuild.
