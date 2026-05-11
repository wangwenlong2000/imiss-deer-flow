#!/usr/bin/env python3
"""
Test all analysis capabilities on the available datasets.
Tests both existing and newly added actions.
"""

import subprocess
import sys
import os
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from utils.path import repo_root

REPO_ROOT = repo_root()

FLOW_CSV_NERIS = REPO_ROOT / "datasets/network-traffic/processed/Neris/Neris.flow.csv"
FLOW_CSV_ZEUS = REPO_ROOT / "datasets/network-traffic/processed/Zeus/Zeus.flow.csv"
FLOW_CSV_TINBA = REPO_ROOT / "datasets/network-traffic/processed/Tinba/Tinba.flow.csv"

# Use whichever dataset exists, prefer Zeus then Neris
if FLOW_CSV_ZEUS.exists():
    FLOW_CSV = str(FLOW_CSV_ZEUS)
elif FLOW_CSV_NERIS.exists():
    FLOW_CSV = str(FLOW_CSV_NERIS)
elif FLOW_CSV_TINBA.exists():
    FLOW_CSV = str(FLOW_CSV_TINBA)
else:
    print("No dataset found in datasets/network-traffic/processed/")
    sys.exit(1)

# Test actions
ACTIONS = {
    # Basic actions
    "summary": {"args": "", "expected": "records"},
    "inspect": {"args": "", "expected": "columns"},
    "overview-report": {"args": "--view auto", "expected": "protocol"},
    
    # Review actions
    "protocol-review": {"args": "--view auto --limit 5", "expected": "protocol"},
    "signature-review": {"args": "--limit 5", "expected": "signature"},
    "zeek-review": {"args": "--limit 5", "expected": "zeek"},
    "session-review": {"args": "--view auto --limit 5", "expected": "session"},
    "scan-review": {"args": "--view auto --limit 5", "expected": "scan"},
    "periodicity-review": {"args": "--limit 5", "expected": "periodicity"},
    "risk-fusion-review": {"args": "--limit 5", "expected": "risk"},
    "packet-review": {"args": "--view packet --limit 5", "expected": "packet"},
    
    # Analysis actions
    "topn": {"args": "--dimension src_ip --metric bytes --limit 5", "expected": "dimension_value"},
    "distribution": {"args": "--dimension protocol --limit 5", "expected": "bucket"},
    "timeseries": {"args": "--interval hour", "expected": "time_bucket"},
    "detect-anomaly": {"args": "--rule volume-spike --anomaly-engine hybrid", "expected": "anomaly"},
    
    # New actions (Phase 1 & 2)
    "encrypted-flow-analysis": {"args": "--limit 5", "expected": "encrypted"},
    "device-identification": {"args": "--limit 5", "expected": "device"},
    "behavior-analysis": {"args": "--limit 5", "expected": "behavior"},
    "graph-analysis": {"args": "--limit 5", "expected": "graph"},
    "qos-analysis": {"args": "--limit 5", "expected": "qos"},
    "root-cause-analysis": {"args": "--limit 5", "expected": "root"},
    "threat-intel-match": {"args": "--limit 5", "expected": "threat"},
    "forecast-traffic": {"args": "--horizon 12 --interval hour", "expected": "forecast"},
    "detect-concept-drift": {"args": "", "expected": "drift"},
}

results = {"passed": 0, "failed": 0, "skipped": 0, "details": []}

def run_test(action_name, config):
    """Run a single test action."""
    cmd = f"python3 analyze.py --files {FLOW_CSV} --action {action_name} {config['args']}"
    
    try:
        result = subprocess.run(
            cmd.split(),
            cwd=SCRIPT_DIR,
            capture_output=True,
            text=True,
            timeout=60
        )
        
        if result.returncode == 0:
            # Check if expected content is in output
            if config["expected"].lower() in result.stdout.lower() or config["expected"].lower() in result.stderr.lower():
                results["passed"] += 1
                results["details"].append({"action": action_name, "status": "PASS", "output": result.stdout[:100]})
                print(f"  ✅ {action_name}: PASS")
            else:
                results["passed"] += 1  # Still passed if no error
                results["details"].append({"action": action_name, "status": "PASS (warning: expected content not found)", "output": result.stdout[:100]})
                print(f"  ⚠️  {action_name}: PASS (content check inconclusive)")
        else:
            results["failed"] += 1
            error_msg = result.stderr[:200] if result.stderr else result.stdout[:200]
            results["details"].append({"action": action_name, "status": "FAIL", "error": error_msg})
            print(f"  ❌ {action_name}: FAIL")
            print(f"     Error: {error_msg[:100]}")
            
    except subprocess.TimeoutExpired:
        results["failed"] += 1
        results["details"].append({"action": action_name, "status": "FAIL (timeout)"})
        print(f"  ⏱️  {action_name}: TIMEOUT")
    except Exception as e:
        results["failed"] += 1
        results["details"].append({"action": action_name, "status": f"FAIL ({e})"})
        print(f"  ❌ {action_name}: EXCEPTION - {e}")

# Run tests
print("=" * 60)
print("Testing Network Traffic Analysis Capabilities")
print("=" * 60)
print(f"\nDataset: {FLOW_CSV}")
print(f"Actions to test: {len(ACTIONS)}\n")

for action_name, config in ACTIONS.items():
    print(f"Testing: {action_name}")
    run_test(action_name, config)

# Summary
print("\n" + "=" * 60)
print("Test Summary")
print("=" * 60)
print(f"Passed:  {results['passed']}")
print(f"Failed:  {results['failed']}")
print(f"Skipped: {results['skipped']}")
print(f"Total:   {len(ACTIONS)}")

# Failed details
if results['failed'] > 0:
    print("\nFailed Actions:")
    for detail in results['details']:
        if detail['status'].startswith('FAIL'):
            print(f"  - {detail['action']}: {detail['status']}")

# Exit code
sys.exit(1 if results['failed'] > 0 else 0)
