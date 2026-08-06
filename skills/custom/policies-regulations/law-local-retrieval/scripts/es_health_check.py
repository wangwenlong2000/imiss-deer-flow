#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import sys

import requests
from requests.auth import HTTPBasicAuth


ES_URL = os.getenv("POLICY_ES_URL", "http://172.17.0.1:3128").rstrip("/")
ES_USER = os.getenv("POLICY_ES_USER", "citybrain-street")
ES_PASSWORD = os.getenv("POLICY_ES_PASSWORD", "123456")


def main():
    url = f"{ES_URL}/_cluster/health"
    try:
        resp = requests.get(
            url,
            auth=HTTPBasicAuth(ES_USER, ES_PASSWORD),
            timeout=10,
        )
        print(json.dumps(resp.json(), ensure_ascii=False, indent=2))
        if resp.status_code >= 400:
            sys.exit(1)
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False, indent=2))
        sys.exit(1)


if __name__ == "__main__":
    main()