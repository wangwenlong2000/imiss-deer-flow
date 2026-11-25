#!/usr/bin/env python3
"""Finalize the visual match after viewing every exported candidate image."""
import argparse
import json
from street_evidence import finalize


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', required=True)
    choice = parser.add_mutually_exclusive_group(required=True)
    choice.add_argument('--matched-id')
    choice.add_argument('--no-match', action='store_true')
    parser.add_argument('--reason', required=True)
    args = parser.parse_args()
    try:
        result = finalize(args.manifest, args.matched_id, args.reason)
    except (OSError, ValueError, TypeError, KeyError) as error:
        print(json.dumps({'skill': 'location-matcher', 'status': 'failed', 'message': str(error)}, ensure_ascii=False))
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
