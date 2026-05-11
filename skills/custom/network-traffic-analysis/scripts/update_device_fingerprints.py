#!/usr/bin/env python3
"""
Refresh external device fingerprint reference files.

The analysis actions stay offline and read only files under data/external.
Run this script manually when the local fingerprint corpus should be refreshed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = SCRIPT_DIR.parent / "data" / "external"

SOURCES = [
    {
        "id": "fingerbank_dhcp_legacy",
        "url": "https://raw.githubusercontent.com/karottc/fingerbank/master/dhcp_fingerprints.conf",
        "filename": "fingerbank_dhcp_fingerprints.conf",
        "parser_hint": "dhcp_fingerprint_rules",
        "description": "Legacy public Fingerbank DHCP fingerprint sample repository.",
        "caveat": "Fingerbank's current full database is API/licensed; this file is an older public corpus.",
    },
    {
        "id": "cert_p0f_syn",
        "url": "https://tools.netsa.cert.org/p0f/p0f.fp.2012032901",
        "filename": "p0f.fp.2012032901",
        "parser_hint": "p0f_syn_fingerprints",
        "description": "CERT NetSA p0f SYN passive OS fingerprint signatures.",
        "caveat": "Compatible with p0f 2.0.x style SYN fingerprints; used as optional OS evidence.",
    },
    {
        "id": "wireshark_manuf",
        "url": "https://www.wireshark.org/download/automated/data/manuf",
        "filename": "wireshark_manuf.txt",
        "parser_hint": "mac_oui_vendor_prefixes",
        "description": "Wireshark generated manufacturer prefix file from public registries.",
        "caveat": "OUI identifies vendor/manufacturer, not exact device model; confidence is intentionally capped.",
    },
]


def fetch(url: str, timeout: int) -> bytes:
    request = Request(url, headers={"User-Agent": "network-traffic-analysis-skill/1.0"})
    with urlopen(request, timeout=timeout) as response:
        return response.read()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description="Download external device fingerprint reference files.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Directory for downloaded reference files.")
    parser.add_argument("--timeout", type=int, default=45, help="HTTP timeout in seconds.")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest: dict[str, object] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": [],
    }
    for source in SOURCES:
        data = fetch(str(source["url"]), args.timeout)
        target = output_dir / str(source["filename"])
        target.write_bytes(data)
        manifest["sources"].append(
            {
                **source,
                "path": str(target),
                "bytes": len(data),
                "sha256": sha256_bytes(data),
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    manifest_path = output_dir / "source_manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
