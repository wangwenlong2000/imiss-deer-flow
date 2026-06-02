#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import json
import urllib.request
from datetime import date
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_DATABASE = SCRIPT_DIR.parent / "data" / "ja3_fingerprints.json"
SSLBL_JA3_CSV_URL = "https://sslbl.abuse.ch/blacklist/ja3_fingerprints.csv"

FIELD_ALIASES = {
    "fingerprint": ("fingerprint", "ja3", "ja3_hash", "md5"),
    "type": ("type", "fingerprint_type"),
    "application": ("application", "app", "malware", "family", "label"),
    "category": ("category", "classification"),
    "risk_level": ("risk_level", "severity", "risk"),
    "confidence": ("confidence", "score"),
    "source": ("source", "provider"),
    "source_url": ("source_url", "url", "reference"),
    "description": ("description", "notes", "comment"),
}


def fetch_text(url: str, timeout: int = 60) -> str:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "network-traffic-analysis-ja3-sync/1.0",
            "Accept": "text/csv,text/plain,*/*",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        content_type = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(content_type, errors="replace")


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "version": str(date.today()),
            "description": "Local JA3 fingerprint records loaded by encrypted-flow-analysis.",
            "records": [],
        }
    return json.loads(path.read_text(encoding="utf-8"))


def records_from_json(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return [record for record in payload if isinstance(record, dict)]
    if isinstance(payload, dict):
        records = payload.get("records", [])
        if isinstance(records, list):
            return [record for record in records if isinstance(record, dict)]
        if payload.get("fingerprint") or payload.get("ja3") or payload.get("ja3_hash"):
            return [payload]
    raise ValueError(f"Unsupported JSON shape in {path}")


def records_from_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def records_from_csv_text(text: str) -> list[dict[str, Any]]:
    lines = [line for line in text.splitlines() if line.strip() and not line.lstrip().startswith("#")]
    if not lines:
        return []
    sample = "\n".join(lines[:5])
    if "ja3" in sample.lower() or "fingerprint" in sample.lower():
        return list(csv.DictReader(io.StringIO("\n".join(lines))))
    reader = csv.reader(io.StringIO("\n".join(lines)))
    records: list[dict[str, Any]] = []
    for row in reader:
        if not row:
            continue
        records.append(
            {
                "ja3_hash": row[0] if len(row) > 0 else "",
                "first_seen": row[1] if len(row) > 1 else "",
                "last_seen": row[2] if len(row) > 2 else "",
                "listing_reason": row[3] if len(row) > 3 else "",
            }
        )
    return records


def records_from_sslbl_csv_text(text: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for row in records_from_csv_text(text):
        fingerprint = (
            row.get("ja3_md5")
            or row.get("ja3_hash")
            or row.get("JA3 Fingerprint")
            or row.get("fingerprint")
            or row.get("ja3")
            or ""
        )
        reason = row.get("listing_reason") or row.get("Listingreason") or row.get("Listing Reason") or row.get("reason") or ""
        first_seen = row.get("first_seen") or row.get("Firstseen") or row.get("First seen (UTC)") or ""
        last_seen = row.get("last_seen") or row.get("Lastseen") or row.get("Last seen (UTC)") or ""
        if not fingerprint:
            continue
        app = str(reason).strip() or "sslbl-blacklisted-ja3"
        records.append(
            {
                "fingerprint": fingerprint,
                "type": "ja3",
                "application": app,
                "category": "malware_c2",
                "risk_level": "high",
                "confidence": 0.8,
                "source": "abuse.ch SSLBL",
                "source_url": SSLBL_JA3_CSV_URL,
                "description": (
                    "SSLBL JA3 blacklist record. "
                    f"First seen: {first_seen or 'unknown'}; last seen: {last_seen or 'unknown'}. "
                    "Verify with SNI, IP/domain reputation, and behavior evidence because this feed may produce false positives."
                ),
            }
        )
    return records


def pick(row: dict[str, Any], canonical: str) -> Any:
    for field in FIELD_ALIASES[canonical]:
        if field in row and row[field] not in (None, ""):
            return row[field]
    return ""


def normalize_record(row: dict[str, Any], default_source: str = "") -> dict[str, Any] | None:
    fingerprint = str(pick(row, "fingerprint")).strip().lower()
    if not fingerprint:
        return None
    if len(fingerprint) != 32 or any(char not in "0123456789abcdef" for char in fingerprint):
        raise ValueError(f"Invalid JA3 fingerprint '{fingerprint}'. Expected 32 lowercase hex characters.")

    confidence_raw = pick(row, "confidence")
    try:
        confidence = float(confidence_raw) if confidence_raw not in (None, "") else 0.6
    except (TypeError, ValueError):
        confidence = 0.6

    return {
        "fingerprint": fingerprint,
        "type": str(pick(row, "type") or "ja3").strip().lower(),
        "application": str(pick(row, "application") or "unknown").strip(),
        "category": str(pick(row, "category") or "unknown").strip(),
        "risk_level": str(pick(row, "risk_level") or "info").strip().lower(),
        "confidence": max(0.0, min(1.0, confidence)),
        "source": str(pick(row, "source") or default_source or "local-import").strip(),
        "source_url": str(pick(row, "source_url") or "").strip(),
        "description": str(pick(row, "description") or "Imported local JA3 record. Verify with SNI, reputation, and behavior evidence.").strip(),
    }


def load_input_records(path: Path) -> list[dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".json":
        return records_from_json(path)
    if suffix == ".csv":
        return records_from_csv(path)
    raise ValueError(f"Unsupported input type '{path.suffix}'. Use .json or .csv.")


def merge_records(existing: list[dict[str, Any]], incoming: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_fingerprint: dict[str, dict[str, Any]] = {}
    for record in existing + incoming:
        fingerprint = str(record.get("fingerprint") or record.get("ja3") or "").strip().lower()
        if not fingerprint:
            continue
        normalized = dict(record)
        normalized["fingerprint"] = fingerprint
        by_fingerprint[fingerprint] = normalized
    return [by_fingerprint[key] for key in sorted(by_fingerprint)]


def main() -> int:
    parser = argparse.ArgumentParser(description="Merge JA3 fingerprint records into data/ja3_fingerprints.json.")
    parser.add_argument("--input", default="", help="Input .json or .csv containing JA3 records.")
    parser.add_argument("--url", default="", help="Remote .csv feed URL to download and merge.")
    parser.add_argument(
        "--provider",
        choices=["generic", "sslbl"],
        default="generic",
        help="Input/feed parser. Use sslbl for https://sslbl.abuse.ch/blacklist/ja3_fingerprints.csv.",
    )
    parser.add_argument("--sync-sslbl", action="store_true", help="Download and merge the abuse.ch SSLBL JA3 CSV feed.")
    parser.add_argument("--database", default=str(DEFAULT_DATABASE), help="Target ja3_fingerprints.json path.")
    parser.add_argument("--source", default="", help="Default source label for imported rows without a source field.")
    parser.add_argument("--dry-run", action="store_true", help="Validate and report counts without writing.")
    args = parser.parse_args()

    if not args.input and not args.url and not args.sync_sslbl:
        parser.error("Provide --input, --url, or --sync-sslbl.")

    database_path = Path(args.database)
    database = load_json(database_path)
    existing = database.get("records", [])
    if not isinstance(existing, list):
        raise ValueError(f"Database {database_path} has a non-list records field.")

    source_label = args.source
    input_ref = args.input
    if args.sync_sslbl:
        args.url = SSLBL_JA3_CSV_URL
        args.provider = "sslbl"
        source_label = source_label or "abuse.ch SSLBL"

    if args.url:
        input_ref = args.url
        feed_text = fetch_text(args.url)
        if args.provider == "sslbl":
            raw_incoming = records_from_sslbl_csv_text(feed_text)
        else:
            raw_incoming = records_from_csv_text(feed_text)
    else:
        input_path = Path(args.input)
        raw_incoming = load_input_records(input_path)

    incoming = [
        record
        for row in raw_incoming
        if (record := normalize_record(row, default_source=source_label)) is not None
    ]
    merged = merge_records(existing, incoming)

    print(
        json.dumps(
            {
                "database": str(database_path),
                "input": input_ref,
                "provider": args.provider,
                "existing_records": len(existing),
                "incoming_records": len(incoming),
                "merged_records": len(merged),
                "dry_run": args.dry_run,
            },
            ensure_ascii=True,
            indent=2,
        )
    )

    if args.dry_run:
        return 0

    database["version"] = str(date.today())
    database["records"] = merged
    database_path.parent.mkdir(parents=True, exist_ok=True)
    database_path.write_text(json.dumps(database, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
