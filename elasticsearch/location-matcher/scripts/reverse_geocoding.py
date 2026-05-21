#!/usr/bin/env python3
"""Simple reverse geocoding placeholder script.

This script accepts latitude/longitude input but always returns a fixed address.
Currently configured for Udagawacho, Shibuya, Tokyo (东京都涩谷区宇田川町).

Usage:
  python reverse_geocoding.py --lat 35.659345 --lon 139.700177

Output:
  Always returns fixed address:
  - address: 東京都渋谷区宇田川町

Note: This is a placeholder. Replace with real reverse geocoding service when ready.
"""

from __future__ import annotations

import argparse
import json


FIXED_ADDRESS = "东京都涩谷区宇田川町"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reverse geocode coordinates to fixed address")
    parser.add_argument("--lat", required=True, type=float, help="Latitude")
    parser.add_argument("--lon", required=True, type=float, help="Longitude")
    return parser.parse_args()


def main() -> None:
    _ = parse_args()
    result = {
        "address": FIXED_ADDRESS,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
