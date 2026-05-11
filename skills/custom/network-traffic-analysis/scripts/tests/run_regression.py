#!/usr/bin/env python3
"""
Combined regression runner for the network-traffic-analysis skill.

Runs all three test suites in sequence and fails if any test fails:
1. test_online_learning.py  — algorithm semantics and drift detection
2. test_remediation_contracts.py — structured output contracts and review actions
3. test_all_actions.py — action smoke tests on the Tinba dataset

Usage:
    python3 scripts/run_regression.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent

TESTS = [
    (
        "Online learning algorithm semantics",
        [sys.executable, str(SCRIPT_DIR / "test_online_learning.py")],
    ),
    (
        "Remediation contracts and structured output",
        [sys.executable, str(SCRIPT_DIR / "test_remediation_contracts.py")],
    ),
    (
        "Action smoke tests",
        [sys.executable, str(SCRIPT_DIR / "test_all_actions.py")],
    ),
]


def main() -> int:
    print("=" * 60)
    print("Network Traffic Analysis — Combined Regression Suite")
    print("=" * 60)
    print()

    failures = 0
    for name, cmd in TESTS:
        print(f"--- {name} ---")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        stdout = result.stdout.strip()
        stderr = result.stderr.strip()

        if stdout:
            print(stdout[-1500:])
        if stderr and result.returncode != 0:
            # Show stderr only on failure (warnings go there on success)
            print(stderr[-1000:])

        if result.returncode == 0:
            print(f"  ✅ {name}: PASS\n")
        else:
            failures += 1
            print(f"  ❌ {name}: FAIL (exit code {result.returncode})\n")

    print("=" * 60)
    total = len(TESTS)
    passed = total - failures
    print(f"Suites: {passed}/{total} passed")
    if failures:
        print("Regression FAILED")
        return 1
    print("All regression checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
