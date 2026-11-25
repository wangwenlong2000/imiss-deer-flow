"""Purpose-separated credentials for Gateway-to-graph requests.

These headers are server credentials and must never be returned to browsers.
The public reverse proxy strips them; direct graph access still verifies them.
"""

import hashlib
import hmac
import os

INTERNAL_GRAPH_HEADER = "x-deerflow-internal-token"


def internal_graph_headers() -> dict[str, str]:
    key = os.getenv("DEER_FLOW_ADMISSION_SIGNING_KEY", "").encode()
    if len(key) < 32:
        return {}
    credential = hmac.new(key, b"deerflow-internal-graph-v1", hashlib.sha256).hexdigest()
    return {INTERNAL_GRAPH_HEADER: credential}


def verify_internal_graph_header(value: str | None) -> bool:
    expected = internal_graph_headers().get(INTERNAL_GRAPH_HEADER)
    return bool(expected and value and hmac.compare_digest(expected.encode(), value.encode()))
