#!/usr/bin/env python3
"""Resolve an address to WGS84 coordinates; never substitute a fixed point."""
import argparse
import json
from geocoding_client import GeocodingError, failure, geocode

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--address', required=True)
    parser.add_argument('--countrycode', help='Optional ISO country filter, e.g. JP')
    args = parser.parse_args()
    try:
        result = geocode(args.address, args.countrycode)
    except GeocodingError as error:
        print(json.dumps(failure(error), ensure_ascii=False))
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
