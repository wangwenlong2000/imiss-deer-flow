#!/usr/bin/env python3
"""Resolve WGS84 coordinates to a nearby mapped address."""
import argparse
import json
from geocoding_client import GeocodingError, failure, reverse_geocode

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--lat', '--latitude', dest='latitude', required=True, type=float)
    parser.add_argument('--lon', '--longitude', dest='longitude', required=True, type=float)
    args = parser.parse_args()
    try:
        result = reverse_geocode(args.latitude, args.longitude)
    except GeocodingError as error:
        print(json.dumps(failure(error), ensure_ascii=False))
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
