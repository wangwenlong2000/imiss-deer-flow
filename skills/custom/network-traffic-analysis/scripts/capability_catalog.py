from __future__ import annotations

import json
from typing import Any

from constants import CAPABILITY_GUIDANCE, SUPPORTED_ANOMALY_RULES


ACTION_DESCRIPTIONS = {
    "list-capabilities": "Return the supported action catalog and workflow recommendations.",
    "inspect": "Inspect schema, canonical mappings, and table structure.",
    "summary": "Return high-level record, time-range, and protocol totals.",
    "overview-report": CAPABILITY_GUIDANCE["overview-report"],
    "scan-review": CAPABILITY_GUIDANCE["scan-review"],
    "session-review": CAPABILITY_GUIDANCE["session-review"],
    "short-connection-review": CAPABILITY_GUIDANCE["short-connection-review"],
    "protocol-review": CAPABILITY_GUIDANCE["protocol-review"],
    "dns-tunnel-review": CAPABILITY_GUIDANCE["dns-tunnel-review"],
    "data-exfiltration-review": CAPABILITY_GUIDANCE["data-exfiltration-review"],
    "lateral-movement-review": CAPABILITY_GUIDANCE["lateral-movement-review"],
    "zeek-review": CAPABILITY_GUIDANCE["zeek-review"],
    "packet-review": CAPABILITY_GUIDANCE["packet-review"],
    "signature-review": CAPABILITY_GUIDANCE["signature-review"],
    "periodicity-review": CAPABILITY_GUIDANCE["periodicity-review"],
    "risk-fusion-review": CAPABILITY_GUIDANCE["risk-fusion-review"],
    "query": CAPABILITY_GUIDANCE["query"],
    "topn": "Rank a dimension by bytes, packets, flow count, destinations, or ports.",
    "timeseries": "Aggregate records, bytes, and packets over time buckets.",
    "distribution": "Show categorical or numeric distribution for one dimension.",
    "filter": "Return filtered rows for quick triage.",
    "aggregate": "Run grouped aggregations with analyst-selected metrics.",
    "detect-anomaly": CAPABILITY_GUIDANCE["detect-anomaly"],
    "export": "Export a result set to CSV, JSON, or Markdown.",
    "encrypted-flow-analysis": CAPABILITY_GUIDANCE["encrypted-flow-analysis"],
    "device-identification": CAPABILITY_GUIDANCE["device-identification"],
    "behavior-analysis": CAPABILITY_GUIDANCE["behavior-analysis"],
    "graph-analysis": CAPABILITY_GUIDANCE["graph-analysis"],
    "qos-analysis": CAPABILITY_GUIDANCE["qos-analysis"],
    "root-cause-analysis": CAPABILITY_GUIDANCE["root-cause-analysis"],
    "threat-intel-match": CAPABILITY_GUIDANCE["threat-intel-match"],
    "forecast-traffic": CAPABILITY_GUIDANCE["forecast-traffic"],
    "detect-concept-drift": CAPABILITY_GUIDANCE["detect-concept-drift"],
}


def build_capability_catalog() -> dict[str, Any]:
    return {
        "actions": ACTION_DESCRIPTIONS,
        "detect_anomaly_rules": {
            "supported": SUPPORTED_ANOMALY_RULES,
            "rule_guidance": {
                "scan-source": "Broad-destination or broad-port source behavior.",
                "volume-spike": "Hourly traffic spikes relative to the average bucket.",
                "rare-port": "Low-frequency destination ports that may merit review.",
                "failure-rate": "High proportions of failed or blocked actions.",
                "syn-scan": "SYN-heavy probing patterns and broad target coverage.",
                "rst-heavy": "RST-dominant traffic that suggests rejection or abrupt termination.",
                "handshake-failure": "SYN without SYN-ACK and failed TCP setup patterns.",
                "icmp-probe": "ICMP probing across many destinations or message types.",
                "small-packet-burst": "High-volume low-payload burst behavior.",
            },
        },
        "workflow_recommendations": {
            "current-dataset-overview": ["overview-report", "protocol-review"],
            "scan-investigation": ["scan-review", "detect-anomaly:scan-source", "query"],
            "session-quality-or-short-lived-flows": ["session-review", "short-connection-review", "query"],
            "packet-evidence": ["packet-review", "detect-anomaly:syn-scan", "detect-anomaly:rst-heavy"],
            "c2-or-beaconing-hypotheses": ["periodicity-review", "timeseries", "protocol-review"],
            "zeek-semantic-evidence": ["zeek-review", "dns-tunnel-review", "signature-review", "protocol-review", "packet-review"],
            "dns-tunneling-hypotheses": ["dns-tunnel-review", "zeek-review", "protocol-review", "query"],
            "data-exfiltration-hypotheses": ["data-exfiltration-review", "risk-fusion-review", "query"],
            "lateral-movement-hypotheses": ["lateral-movement-review", "scan-review", "session-review", "query"],
            "performance-and-quality-triage": ["qos-analysis", "timeseries", "session-review", "packet-review"],
            "final-risk-ranking": ["risk-fusion-review", "zeek-review", "signature-review", "detect-anomaly:scan-source"],
            "custom-thresholds-or-ad-hoc-hypotheses": ["query"],
        },
        "notes": [
            "If a requested heuristic is not listed under detect_anomaly_rules.supported, do not invent a new rule name.",
            "Use session-review, scan-review, protocol-review, packet-review, or query as the nearest structured fallback.",
            "For explicit thresholds or analyst-defined logic, prefer --action query over unsupported anomaly rules.",
        ],
    }


def render_capability_catalog() -> str:
    return json.dumps(build_capability_catalog(), ensure_ascii=False, indent=2)
