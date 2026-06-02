#!/usr/bin/env python3
"""Focused tests for threat intelligence MITRE ATT&CK contract.

Tests the three P1-2 remaining refinements:
1. MITRE database file missing → FileNotFoundError
2. MITRE database parse failure → RuntimeError
3. build_skill_result_parts reports coverage_mode
"""

import json
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from actions.threat_intel_match_action import build_skill_result_parts


class TestThreatIntelContract(unittest.TestCase):
    """Contract tests for threat-intel-match MITRE and coverage behavior."""

    def test_mitre_database_missing_fails_with_guidance(self):
        """Missing MITRE data file should raise FileNotFoundError with guidance."""
        from analysis.threat_intel import ThreatIntelMatcher

        with unittest.mock.patch.object(Path, "exists", return_value=False):
            matcher = ThreatIntelMatcher.__new__(ThreatIntelMatcher)
            matcher.data_directory = Path("/nonexistent")
            try:
                matcher._load_external_mitre_database()
                self.fail("Expected FileNotFoundError")
            except FileNotFoundError as exc:
                self.assertIn("MITRE ATT&CK data file not found", str(exc))
                self.assertIn("data/external/mitre_attack_techniques.json", str(exc))

    def test_mitre_database_parse_failure_fails_fast(self):
        """Invalid JSON in MITRE data file should raise RuntimeError."""
        from analysis.threat_intel import ThreatIntelMatcher

        with unittest.mock.patch.object(Path, "exists", return_value=True):
            with unittest.mock.patch.object(Path, "read_text", return_value="not valid json {{{"):
                matcher = ThreatIntelMatcher.__new__(ThreatIntelMatcher)
                matcher.data_directory = Path("/fake")
                try:
                    matcher._load_external_mitre_database()
                    self.fail("Expected RuntimeError")
                except RuntimeError as exc:
                    self.assertIn("Failed to parse MITRE ATT&CK data", str(exc))

    def test_threat_intel_skill_result_reports_coverage_mode(self):
        """build_skill_result_parts must expose coverage_mode in evidence, diagnostics.data_quality, and diagnostics.threat_intel."""
        matcher_status = {
            "coverage_mode": "heuristic_only",
            "loaded_feed_count": 0,
            "total_loaded_indicators": 0,
            "feed_directory": "",
        }
        results = {
            "matches": [],
            "summary": {
                "total_entities_checked": 10,
                "threat_matches_found": 0,
                "critical_threats": 0,
                "high_threats": 0,
                "mitre_techniques_mapped": 0,
                "campaigns_identified": 0,
            },
            "matcher_status": matcher_status,
        }
        skill_result = build_skill_result_parts(results, raw_output="")

        # coverage_mode in evidence
        coverage_evidence = next(
            (e for e in skill_result["evidence"] if e["evidence_id"] == "e-threat-intel-coverage"),
            None,
        )
        self.assertIsNotNone(coverage_evidence, "coverage evidence missing")
        self.assertEqual(coverage_evidence["content"]["coverage_mode"], "heuristic_only")

        # coverage_mode in diagnostics.data_quality
        self.assertEqual(
            skill_result["diagnostics"]["data_quality"]["coverage_mode"],
            "heuristic_only",
        )

        # coverage_mode in diagnostics.threat_intel
        self.assertEqual(
            skill_result["diagnostics"]["threat_intel"]["coverage_mode"],
            "heuristic_only",
        )


if __name__ == "__main__":
    unittest.main()
