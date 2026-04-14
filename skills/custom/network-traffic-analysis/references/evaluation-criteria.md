# Evaluation Criteria

This reference defines the current analysis and RAG evaluation standards.

## Analysis standards

### Short connections

Wide short:

- `duration_ms < 1000`

Narrow short:

- `duration_ms < 1000`
- `bytes < 500`
- `packets <= 3`

### Scan-like source posture

Typical rule-based thresholds:

- wide destination spread
- wide destination port spread
- high unique destination count

Interpret scan outputs as suspicious or notable behavior, not final attribution.

### Volume spikes

Interpretation standard:

- bucket activity significantly above average
- confirmed using `timeseries` and `detect-anomaly --rule volume-spike`

### Failure-heavy traffic

Interpretation standard:

- concentrated resets, denies, drops, or other failure-like outcomes
- confirmed using `session-review` and `detect-anomaly --rule failure-rate`

## RAG evaluation standards

Recommended metrics:

- `Hit@K`
- `Recall@K`
- `Type Match@K`

## Ground-truth construction

Recommended approach:

1. use `analyze.py` outputs as the fact source
2. convert facts into:
   - expected document types
   - expected entities
   - expected evidence phrases
3. evaluate retrieval results against those labels

## Core document-type expectations

- communication profile questions:
  - `endpoint_summary`
  - `port_summary`
  - `protocol_summary`
- anomaly questions:
  - `anomaly_summary`
  - `endpoint_summary`
- short-connection questions:
  - `anomaly_summary`
  - `endpoint_summary`
  - `port_summary`

## Minimum validation practice

For every major preprocessing or summary change, re-check:

- one relative-time dataset
- one absolute-time dataset
- one scan-heavy or anomaly-heavy dataset
- one retrieval query for each major question type
