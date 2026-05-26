#!/usr/bin/env python3
"""Simple geocoding placeholder script.

This script accepts an address input but always returns a fixed latitude/longitude.
Currently configured for Shibuya, Tokyo (涩谷, 东京).

Usage:
  python geocoding.py --address "任何地址"

Output:
  Always returns fixed coordinates:
  - Latitude: 35.654008
  - Longitude: 139.705398

Note: This is a placeholder. Replace with real geocoding service when ready.
"""

from __future__ import annotations

import argparse
import json


FIXED_LATITUDE = 35.659345
FIXED_LONGITUDE = 139.700177

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Geocode address to fixed coordinates")
    parser.add_argument("--address", required=True, help="Address text to geocode")
    return parser.parse_args()


def main() -> None:
    _ = parse_args()
    result = {
        "latitude": FIXED_LATITUDE,
        "longitude": FIXED_LONGITUDE,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
