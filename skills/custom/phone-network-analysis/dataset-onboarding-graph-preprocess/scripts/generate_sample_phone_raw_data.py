#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate a small synthetic raw phone-network dataset for onboarding tests."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path
import random
import pandas as pd


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    random.seed(args.seed)
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    phones = [f"1380000{i:04d}" for i in range(1, 16)]
    peers = [f"1391000{i:04d}" for i in range(1, 31)]
    devices = [f"IMEI-DEMO-{i:04d}" for i in range(1, 8)]

    users = []
    for i, p in enumerate(phones):
        users.append({
            "phone_number": p,
            "province": "demo_province",
            "label": 1 if i in {0, 1, 2, 7} else 0,
            "sub_label": "risk" if i in {0, 1, 2, 7} else "normal",
            "age": random.choice([22, 28, 35, 46, ""]),
            "open_card_time": "2023-01-01",
        })
    pd.DataFrame(users).to_csv(out / "raw_user_labels.csv", index=False, encoding="utf-8-sig")

    rows = []
    start = datetime(2024, 1, 1, 8, 0, 0)
    for day in range(14):
        for _ in range(random.randint(18, 34)):
            p = random.choice(phones)
            peer = random.choice(peers)
            device = random.choice(devices[:3] if p in phones[:5] else devices)
            hour = random.choice([1, 2, 3, 9, 10, 14, 16, 21, 22])
            t = start + timedelta(days=day, hours=hour, minutes=random.randint(0, 59))
            rows.append({
                "caller": p,
                "callee": peer,
                "call_time": t.strftime("%Y-%m-%d %H:%M:%S"),
                "duration_sec": random.randint(10, 600),
                "imei": device,
                "province": "demo_province",
                "city": "demo_city",
                "call_type": random.choice(["out", "in"]),
            })
    pd.DataFrame(rows).to_csv(out / "raw_call_records.csv", index=False, encoding="utf-8-sig")

    dev_rows = []
    for i, p in enumerate(phones):
        # First five phones intentionally share devices for downstream tests.
        candidate_devices = devices[:3] if i < 5 else devices[3:]
        for d in random.sample(candidate_devices, k=1 if i >= 5 else 2):
            dev_rows.append({"mobile": p, "device_id": d, "province": "demo_province"})
    pd.DataFrame(dev_rows).to_csv(out / "raw_device_bindings.csv", index=False, encoding="utf-8-sig")

    print(f"sample raw data written to {out}")


if __name__ == "__main__":
    main()
