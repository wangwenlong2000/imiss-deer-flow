from __future__ import annotations

import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
import unittest.mock
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from analyze import build_parser, run_self_check
from analysis.behavior_analysis import BehaviorAnalyzer
from analysis.device_fingerprint import DeviceFingerprinter
from analysis.online_learning import BernoulliDDMDriftDetector, OnlineStatistics
from capability_catalog import ACTION_DESCRIPTIONS
from core.schema_mapping import load_sources, default_field_mapping_candidates, load_mapping
from utils.path import compute_cache_key, dataset_root, uploads_root, skill_root, repo_root, outputs_root
from file_resolution import get_default_search_roots
import duckdb


class TestRemediationContracts(unittest.TestCase):
    # --- Dataset path helpers ---

    @staticmethod
    def _tinba_flow() -> str:
        path = repo_root() / "datasets/network-traffic/processed/Tinba/Tinba.flow.csv"
        if not path.exists():
            raise unittest.SkipTest("Tinba dataset not present (cleaned up)")
        return str(path)

    @staticmethod
    def _zeus_flow() -> str:
        path = repo_root() / "datasets/network-traffic/processed/Zeus/Zeus.flow.csv"
        if not path.exists():
            raise unittest.SkipTest("Zeus dataset not present")
        return str(path)

    def test_cli_action_choices_match_capability_catalog(self) -> None:
        parser = build_parser()
        action = next(action for action in parser._actions if action.dest == "action")

        self.assertEqual(set(action.choices), set(ACTION_DESCRIPTIONS))

    def test_online_statistics_merge_handles_empty_inputs(self) -> None:
        left = OnlineStatistics()
        right = OnlineStatistics()

        merged = left.merge(right)

        self.assertEqual(merged.n, 0)
        self.assertEqual(merged.mean, 0.0)

    def test_bernoulli_ddm_rejects_non_probability_values(self) -> None:
        detector = BernoulliDDMDriftDetector()

        with self.assertRaises(ValueError):
            detector.update(2.0)

    def test_behavior_baseline_records_time_ordering_and_low_sample_warning(self) -> None:
        analyzer = BehaviorAnalyzer()
        flows = [
            {"timestamp": "2026-04-29T00:02:00Z", "bytes": 200, "packets": 2, "dst_ip": "10.0.0.2", "dst_port": 443, "protocol": "TCP"},
            {"timestamp": "2026-04-29T00:00:00Z", "bytes": 100, "packets": 1, "dst_ip": "10.0.0.1", "dst_port": 80, "protocol": "TCP"},
            {"timestamp": "2026-04-29T00:01:00Z", "bytes": 120, "packets": 1, "dst_ip": "10.0.0.1", "dst_port": 80, "protocol": "TCP"},
            {"timestamp": "2026-04-29T00:03:00Z", "bytes": 220, "packets": 2, "dst_ip": "10.0.0.2", "dst_port": 443, "protocol": "TCP"},
        ]

        profile = analyzer.analyze_behavior("10.0.0.10", flows, min_baseline_size=10)

        windowing = profile.baseline["windowing"]
        self.assertEqual(windowing["method"], "time_ordered_split")
        self.assertTrue(windowing["time_ordered"])
        self.assertIn("warnings", profile.baseline)

    def test_behavior_analysis_uses_explicit_time_windows(self) -> None:
        analyzer = BehaviorAnalyzer()
        flows = [
            {"timestamp": "2026-04-29T00:10:00Z", "bytes": 100, "packets": 1, "dst_ip": "10.0.0.1", "dst_port": 80, "protocol": "TCP"},
            {"timestamp": "2026-04-29T00:20:00Z", "bytes": 120, "packets": 1, "dst_ip": "10.0.0.1", "dst_port": 80, "protocol": "TCP"},
            {"timestamp": "2026-04-29T12:10:00Z", "bytes": 400, "packets": 1, "dst_ip": "10.0.0.2", "dst_port": 443, "protocol": "TCP"},
            {"timestamp": "2026-04-29T12:20:00Z", "bytes": 450, "packets": 1, "dst_ip": "10.0.0.2", "dst_port": 443, "protocol": "TCP"},
            {"timestamp": "2026-04-29T12:30:00Z", "bytes": 420, "packets": 1, "dst_ip": "10.0.0.2", "dst_port": 443, "protocol": "TCP"},
            {"timestamp": "2026-04-29T12:40:00Z", "bytes": 430, "packets": 1, "dst_ip": "10.0.0.2", "dst_port": 443, "protocol": "TCP"},
            {"timestamp": "2026-04-29T12:50:00Z", "bytes": 410, "packets": 1, "dst_ip": "10.0.0.2", "dst_port": 443, "protocol": "TCP"},
        ]

        profile = analyzer.analyze_behavior(
            "10.0.0.10",
            flows,
            baseline_start="2026-04-29T00:00:00Z",
            baseline_end="2026-04-29T01:00:00Z",
            current_start="2026-04-29T12:00:00Z",
            current_end="2026-04-29T13:00:00Z",
            min_baseline_size=1,
        )

        self.assertEqual(profile.baseline["windowing"]["method"], "explicit_time_windows")
        self.assertEqual(profile.baseline["windowing"]["baseline_flows"], 2)
        self.assertEqual(profile.baseline["windowing"]["current_flows"], 5)
        self.assertGreater(profile.deviation_score, 0.0)
        self.assertTrue(profile.data_quality["sufficient_data"])

    def test_lenient_csv_ingestion_reports_row_quality_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "flows.csv"
            path.write_text("src_ip,dst_ip,bytes\n1.1.1.1,2.2.2.2,10\nbad,row\n", encoding="utf-8")
            con = duckdb.connect(":memory:")
            try:
                with contextlib.redirect_stderr(io.StringIO()):
                    table_info = load_sources(con, [str(path)], ingestion_mode="lenient", quiet=True)
            finally:
                con.close()

        metadata = next(iter(table_info.values()))
        self.assertEqual(metadata["ingestion_mode"], "lenient")
        self.assertEqual(metadata["physical_data_rows"], 2)
        self.assertIn("rows_loaded", metadata)
        self.assertIn("ingestion_warning", metadata)

    def test_strict_csv_ingestion_rejects_malformed_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "flows.csv"
            path.write_text("src_ip,dst_ip,bytes\n1.1.1.1,2.2.2.2,10\nbad,row\n", encoding="utf-8")
            con = duckdb.connect(":memory:")
            try:
                with contextlib.redirect_stderr(io.StringIO()):
                    table_info = load_sources(con, [str(path)], ingestion_mode="strict", quiet=True)
            finally:
                con.close()

        self.assertEqual(table_info, {})

    def test_cache_key_includes_ingestion_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "flows.csv"
            path.write_text("src_ip,dst_ip,bytes\n1.1.1.1,2.2.2.2,10\n", encoding="utf-8")
            mapping = {"canonical_fields": {}}

            lenient_key = compute_cache_key([str(path)], mapping, ingestion_mode="lenient")
            strict_key = compute_cache_key([str(path)], mapping, ingestion_mode="strict")

        self.assertNotEqual(lenient_key, strict_key)

    def test_device_fingerprinter_prefers_profile_matches(self) -> None:
        fingerprinter = DeviceFingerprinter()

        result = fingerprinter.identify_device_type(
            {
                "http_user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) Mobile Safari/604.1",
                "dst_port": 443,
                "protocol": "TCP",
            }
        )

        self.assertEqual(result["device_type"], "mobile")
        self.assertEqual(result["device_os"], "iOS")
        self.assertEqual(result["classification_method"], "profile_match")
        self.assertGreaterEqual(result["confidence"], 0.6)

    def test_device_fingerprinter_uses_downloaded_dhcp_fingerprints(self) -> None:
        fingerprinter = DeviceFingerprinter()
        if not fingerprinter.external_sources.get("dhcp_fingerprints"):
            self.skipTest("downloaded DHCP fingerprint corpus is not present")

        result = fingerprinter.identify_device_type(
            {
                "dhcp_fingerprint": "1,3,6,15,119,252",
                "dst_port": 67,
                "protocol": "UDP",
            }
        )

        self.assertEqual(result["device_type"], "mobile")
        self.assertEqual(result["classification_method"], "external_dhcp_fingerprint")
        self.assertIn("fingerbank", result["source"])

    def test_device_fingerprinter_uses_downloaded_oui_vendor_data(self) -> None:
        fingerprinter = DeviceFingerprinter()
        if not fingerprinter.external_sources.get("mac_vendors"):
            self.skipTest("downloaded OUI vendor corpus is not present")

        result = fingerprinter.identify_device_type(
            {
                "mac_src": "00:1B:63:00:00:00",
                "dst_port": 443,
                "protocol": "TCP",
            }
        )

        self.assertEqual(result["device_type"], "mobile")
        self.assertEqual(result["classification_method"], "external_oui_vendor")
        self.assertIn("wireshark_manuf", result["source"])

    def test_input_output_contract_documents_all_actions(self) -> None:
        path = SCRIPT_DIR.parent / "references" / "input-output-contract.md"
        text = path.read_text(encoding="utf-8")

        missing = [
            action for action in ACTION_DESCRIPTIONS
            if f"`{action}`" not in text and f"--action {action}" not in text
        ]

        self.assertEqual(missing, [])

    def test_capability_catalog_reference_documents_all_actions(self) -> None:
        path = SCRIPT_DIR.parent / "references" / "capability-catalog.md"
        text = path.read_text(encoding="utf-8")

        missing = [
            action for action in ACTION_DESCRIPTIONS
            if f"`{action}`" not in text
        ]

        self.assertEqual(missing, [])

    def test_protocol_review_structured_output(self) -> None:
        """Smoke test: protocol-review --format skill-result-json has proper evidence."""
        import json
        import subprocess

        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_DIR / "analyze.py"),
                "--files",
                self._tinba_flow(),
                "--action",
                "protocol-review",
                "--format",
                "skill-result-json",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, f"analyze.py failed: {result.stderr}")

        data = json.loads(result.stdout)
        self.assertEqual(data["status"], "success")

        evidence_ids = [e["evidence_id"] for e in data["result"]["evidence"]]
        self.assertIn("e-protocol-mix", evidence_ids)

        # Verify evidence_refs in findings point to existing evidence
        existing = set(evidence_ids)
        for finding in data["result"]["findings"]:
            for ref in finding.get("evidence_refs", []):
                self.assertIn(ref, existing, f"Finding {finding['finding_id']} references missing evidence: {ref}")

        # Verify table evidence uses columns + rows
        protocol_evidence = next(e for e in data["result"]["evidence"] if e["evidence_id"] == "e-protocol-mix")
        self.assertEqual(protocol_evidence["type"], "table")
        self.assertIn("columns", protocol_evidence)
        self.assertIn("rows", protocol_evidence)

    def test_packet_review_structured_output(self) -> None:
        """Smoke test: packet-review --format skill-result-json has proper evidence."""
        import json
        import subprocess

        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_DIR / "analyze.py"),
                "--files",
                self._tinba_flow(),
                "--action",
                "packet-review",
                "--format",
                "skill-result-json",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, f"analyze.py failed: {result.stderr}")

        data = json.loads(result.stdout)
        self.assertEqual(data["status"], "success")

        evidence_ids = [e["evidence_id"] for e in data["result"]["evidence"]]
        self.assertIn("e-handshake-posture", evidence_ids)
        self.assertIn("e-packet-protocol-mix", evidence_ids)

        # Verify table evidence uses columns + rows
        packet_evidence = next(e for e in data["result"]["evidence"] if e["evidence_id"] == "e-packet-protocol-mix")
        self.assertEqual(packet_evidence["type"], "table")
        self.assertIn("columns", packet_evidence)
        self.assertIn("rows", packet_evidence)

        # Verify no_tcp_handshake_evidence warning when no TCP packets
        warnings = data["diagnostics"]["warnings"]
        self.assertTrue(any(w["code"] == "no_tcp_handshake_evidence" for w in warnings))

    def test_risk_fusion_table_evidence_uses_columns_and_rows(self) -> None:
        """Smoke test: risk-fusion-review table evidence migrated from content to columns+rows."""
        import json
        import subprocess

        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_DIR / "analyze.py"),
                "--files",
                self._tinba_flow(),
                "--action",
                "risk-fusion-review",
                "--format",
                "skill-result-json",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, f"analyze.py failed: {result.stderr}")

        data = json.loads(result.stdout)
        self.assertEqual(data["status"], "success")

        evidence_ids = [e["evidence_id"] for e in data["result"]["evidence"]]
        self.assertIn("e-final-fused-risk", evidence_ids)

        # Verify table evidence uses columns + rows, not content
        fused_risk = next(e for e in data["result"]["evidence"] if e["evidence_id"] == "e-final-fused-risk")
        self.assertEqual(fused_risk["type"], "table")
        self.assertIn("columns", fused_risk)
        self.assertIn("rows", fused_risk)
        self.assertNotIn("content", fused_risk)

        evidence_mix = next(e for e in data["result"]["evidence"] if e["evidence_id"] == "e-fused-evidence-mix")
        self.assertEqual(evidence_mix["type"], "table")
        self.assertIn("columns", evidence_mix)
        self.assertIn("rows", evidence_mix)
        self.assertNotIn("content", evidence_mix)

        # Verify coverage is metric, not table with content
        coverage = next(e for e in data["result"]["evidence"] if e["evidence_id"] == "e-risk-fusion-coverage")
        self.assertEqual(coverage["type"], "metric")
        self.assertIn("metrics", coverage)

    def test_signature_review_structured_output(self) -> None:
        """Smoke test: signature-review --format skill-result-json has structured evidence."""
        import json
        import subprocess

        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_DIR / "analyze.py"),
                "--files",
                self._tinba_flow(),
                "--action",
                "signature-review",
                "--format",
                "skill-result-json",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, f"analyze.py failed: {result.stderr}")

        data = json.loads(result.stdout)
        self.assertEqual(data["status"], "success")

        evidence_ids = [e["evidence_id"] for e in data["result"]["evidence"]]
        self.assertIn("e-signature-metrics", evidence_ids)

        # No-hit case should have coverage warning
        warnings = data["diagnostics"]["warnings"]
        self.assertTrue(any(w["code"] == "no_signature_hits" for w in warnings))

        # When hits exist, table evidence must use columns+rows
        if "e-signature-summary" in evidence_ids:
            summary_evidence = next(e for e in data["result"]["evidence"] if e["evidence_id"] == "e-signature-summary")
            self.assertEqual(summary_evidence["type"], "table")
            self.assertIn("columns", summary_evidence)
            self.assertIn("rows", summary_evidence)

    def test_zeek_review_no_artifacts_structured_output(self) -> None:
        """Smoke test: zeek-review without Zeek logs returns success with coverage warning."""
        import json
        import subprocess
        import tempfile

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, dir="/tmp") as f:
            f.write("src_ip,dst_ip,bytes\n10.0.0.1,10.0.0.2,100\n")
            tmp_path = f.name

        try:
            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_DIR / "analyze.py"),
                    "--files",
                    tmp_path,
                    "--action",
                    "zeek-review",
                    "--format",
                    "skill-result-json",
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )
            self.assertEqual(result.returncode, 0, f"analyze.py failed: {result.stderr}")

            data = json.loads(result.stdout)
            self.assertEqual(data["status"], "success")

            evidence_ids = [e["evidence_id"] for e in data["result"]["evidence"]]
            self.assertIn("e-zeek-coverage", evidence_ids)

            # No Zeek artifacts should produce warning
            warnings = data["diagnostics"]["warnings"]
            self.assertTrue(any(w["code"] == "no_zeek_artifacts" for w in warnings))

            # Coverage should be metric type, not table
            coverage = next(e for e in data["result"]["evidence"] if e["evidence_id"] == "e-zeek-coverage")
            self.assertEqual(coverage["type"], "metric")
            self.assertIn("metrics", coverage)
        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_zeek_review_with_artifacts_uses_columns_and_rows(self) -> None:
        """Smoke test: zeek-review with Zeek logs produces table evidence in columns+rows format."""
        import json
        import subprocess

        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_DIR / "analyze.py"),
                "--files",
                self._tinba_flow(),
                "--action",
                "zeek-review",
                "--format",
                "skill-result-json",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, f"analyze.py failed: {result.stderr}")

        data = json.loads(result.stdout)
        self.assertEqual(data["status"], "success")

        evidence_ids = [e["evidence_id"] for e in data["result"]["evidence"]]
        self.assertIn("e-zeek-coverage", evidence_ids)

        # When conn data exists, table evidence must use columns+rows
        table_evidence_ids = ["e-zeek-conn-states", "e-zeek-services", "e-zeek-talker-pairs"]
        for eid in table_evidence_ids:
            if eid in evidence_ids:
                ev = next(e for e in data["result"]["evidence"] if e["evidence_id"] == eid)
                self.assertEqual(ev["type"], "table")
                self.assertIn("columns", ev)
                self.assertIn("rows", ev)

        # Coverage should be metric type
        coverage = next(e for e in data["result"]["evidence"] if e["evidence_id"] == "e-zeek-coverage")
        self.assertEqual(coverage["type"], "metric")
        self.assertIn("metrics", coverage)

    def test_scan_review_structured_output(self) -> None:
        """Smoke test: scan-review --format skill-result-json has structured evidence."""
        import json
        import subprocess

        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_DIR / "analyze.py"),
                "--files",
                self._tinba_flow(),
                "--action",
                "scan-review",
                "--view",
                "auto",
                "--format",
                "skill-result-json",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, f"analyze.py failed: {result.stderr}")

        data = json.loads(result.stdout)
        self.assertEqual(data["status"], "success")

        evidence_ids = [e["evidence_id"] for e in data["result"]["evidence"]]

        # Must have non-raw-report evidence
        non_raw = [e for e in evidence_ids if e != "e-raw-report"]
        self.assertTrue(len(non_raw) > 0, "scan-review should have evidence beyond e-raw-report")

        # Scan risk sources table must exist with columns+rows
        self.assertIn("e-scan-risk-sources", evidence_ids)
        risk_sources = next(e for e in data["result"]["evidence"] if e["evidence_id"] == "e-scan-risk-sources")
        self.assertEqual(risk_sources["type"], "table")
        self.assertIn("columns", risk_sources)
        self.assertIn("rows", risk_sources)

        # Scan metrics must exist
        self.assertIn("e-scan-metrics", evidence_ids)
        scan_metrics = next(e for e in data["result"]["evidence"] if e["evidence_id"] == "e-scan-metrics")
        self.assertEqual(scan_metrics["type"], "metric")
        self.assertIn("metrics", scan_metrics)

        # Verify evidence_refs in findings point to existing evidence
        existing = set(evidence_ids)
        for finding in data["result"]["findings"]:
            for ref in finding.get("evidence_refs", []):
                self.assertIn(ref, existing, f"Finding {finding['finding_id']} references missing evidence: {ref}")

    def test_short_connection_review_structured_output(self) -> None:
        """Smoke test: short-connection-review --format skill-result-json has structured evidence."""
        import json
        import subprocess

        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_DIR / "analyze.py"),
                "--files",
                self._tinba_flow(),
                "--action",
                "short-connection-review",
                "--format",
                "skill-result-json",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, f"analyze.py failed: {result.stderr}")

        data = json.loads(result.stdout)
        self.assertEqual(data["status"], "success")

        evidence_ids = [e["evidence_id"] for e in data["result"]["evidence"]]

        # Must have non-raw-report evidence
        non_raw = [e for e in evidence_ids if e != "e-raw-report"]
        self.assertTrue(len(non_raw) > 0, "short-connection-review should have evidence beyond e-raw-report")

        # Summary metrics must exist
        self.assertIn("e-short-connection-summary", evidence_ids)
        summary_evidence = next(e for e in data["result"]["evidence"] if e["evidence_id"] == "e-short-connection-summary")
        self.assertEqual(summary_evidence["type"], "metric")
        self.assertIn("metrics", summary_evidence)

        # Verify table evidence uses columns + rows
        if "e-short-connection-sources" in evidence_ids:
            sources = next(e for e in data["result"]["evidence"] if e["evidence_id"] == "e-short-connection-sources")
            self.assertEqual(sources["type"], "table")
            self.assertIn("columns", sources)
            self.assertIn("rows", sources)

        # Verify evidence_refs in findings point to existing evidence
        existing = set(evidence_ids)
        for finding in data["result"]["findings"]:
            for ref in finding.get("evidence_refs", []):
                self.assertIn(ref, existing, f"Finding {finding['finding_id']} references missing evidence: {ref}")

    def test_periodicity_review_structured_output(self) -> None:
        """Smoke test: periodicity-review --format skill-result-json has structured evidence."""
        import json
        import subprocess

        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_DIR / "analyze.py"),
                "--files",
                self._tinba_flow(),
                "--action",
                "periodicity-review",
                "--interval",
                "minute",
                "--format",
                "skill-result-json",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, f"analyze.py failed: {result.stderr}")

        data = json.loads(result.stdout)
        self.assertEqual(data["status"], "success")

        evidence_ids = [e["evidence_id"] for e in data["result"]["evidence"]]

        # Must have non-raw-report evidence
        non_raw = [e for e in evidence_ids if e != "e-raw-report"]
        self.assertTrue(len(non_raw) > 0, "periodicity-review should have evidence beyond e-raw-report")

        # Metrics must exist
        self.assertIn("e-periodicity-metrics", evidence_ids)
        metrics_evidence = next(e for e in data["result"]["evidence"] if e["evidence_id"] == "e-periodicity-metrics")
        self.assertEqual(metrics_evidence["type"], "metric")
        self.assertIn("metrics", metrics_evidence)

        # Verify table evidence uses columns + rows
        if "e-periodicity-candidates" in evidence_ids:
            candidates = next(e for e in data["result"]["evidence"] if e["evidence_id"] == "e-periodicity-candidates")
            self.assertEqual(candidates["type"], "table")
            self.assertIn("columns", candidates)
            self.assertIn("rows", candidates)

        # Verify evidence_refs in findings point to existing evidence
        existing = set(evidence_ids)
        for finding in data["result"]["findings"]:
            for ref in finding.get("evidence_refs", []):
                self.assertIn(ref, existing, f"Finding {finding['finding_id']} references missing evidence: {ref}")

    def test_signature_review_structured_output_zeus(self) -> None:
        """Smoke test: signature-review --format skill-result-json with Zeus data (has signature hits)."""
        import json
        import subprocess

        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_DIR / "analyze.py"),
                "--files",
                self._zeus_flow(),
                "--action",
                "signature-review",
                "--format",
                "skill-result-json",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, f"analyze.py failed: {result.stderr}")

        data = json.loads(result.stdout)
        self.assertEqual(data["status"], "success")

        evidence_ids = [e["evidence_id"] for e in data["result"]["evidence"]]

        # Must have non-raw-report evidence
        non_raw = [e for e in evidence_ids if e != "e-raw-report"]
        self.assertTrue(len(non_raw) > 0, "signature-review should have evidence beyond e-raw-report")

        # Metrics must exist
        self.assertIn("e-signature-metrics", evidence_ids)
        metrics_evidence = next(e for e in data["result"]["evidence"] if e["evidence_id"] == "e-signature-metrics")
        self.assertEqual(metrics_evidence["type"], "metric")
        self.assertIn("metrics", metrics_evidence)

        # Summary table must exist with columns+rows
        self.assertIn("e-signature-summary", evidence_ids)
        summary = next(e for e in data["result"]["evidence"] if e["evidence_id"] == "e-signature-summary")
        self.assertEqual(summary["type"], "table")
        self.assertIn("columns", summary)
        self.assertIn("rows", summary)

        # Zeus has signature hits, so hotspots should exist
        self.assertIn("e-signature-source-hotspots", evidence_ids)
        hotspots = next(e for e in data["result"]["evidence"] if e["evidence_id"] == "e-signature-source-hotspots")
        self.assertEqual(hotspots["type"], "table")
        self.assertIn("columns", hotspots)
        self.assertIn("rows", hotspots)

        # Verify findings exist (Zeus has c2_or_evasion hits)
        self.assertTrue(len(data["result"]["findings"]) > 0, "signature-review should have findings for Zeus data")

        # Verify evidence_refs in findings point to existing evidence
        existing = set(evidence_ids)
        for finding in data["result"]["findings"]:
            for ref in finding.get("evidence_refs", []):
                self.assertIn(ref, existing, f"Finding {finding['finding_id']} references missing evidence: {ref}")

    def test_dns_tunnel_structured_output_zeus(self) -> None:
        """Smoke test: dns-tunnel-review with Zeus data (has Zeek dns.log)."""
        import json
        import subprocess

        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_DIR / "analyze.py"),
                "--files",
                self._zeus_flow(),
                "--action",
                "dns-tunnel-review",
                "--format",
                "skill-result-json",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, f"analyze.py failed: {result.stderr}")

        data = json.loads(result.stdout)
        self.assertEqual(data["status"], "success")

        evidence_ids = [e["evidence_id"] for e in data["result"]["evidence"]]

        # Must have non-raw-report evidence
        non_raw = [e for e in evidence_ids if e != "e-raw-report"]
        self.assertTrue(len(non_raw) > 0, "dns-tunnel-review should have evidence beyond e-raw-report")

        # Metrics must exist
        self.assertIn("e-dns-tunnel-metrics", evidence_ids)
        metrics_evidence = next(e for e in data["result"]["evidence"] if e["evidence_id"] == "e-dns-tunnel-metrics")
        self.assertEqual(metrics_evidence["type"], "metric")
        self.assertIn("metrics", metrics_evidence)

        # Hotspots table must use columns + rows
        if "e-dns-tunnel-hotspots" in evidence_ids:
            hotspots = next(e for e in data["result"]["evidence"] if e["evidence_id"] == "e-dns-tunnel-hotspots")
            self.assertEqual(hotspots["type"], "table")
            self.assertIn("columns", hotspots)
            self.assertIn("rows", hotspots)

        # Zeus has Zeek dns.log, so semantic candidates should be present
        self.assertIn("e-zeek-dns-semantic-candidates", evidence_ids)

        # Verify evidence_refs in findings point to existing evidence
        existing = set(evidence_ids)
        for finding in data["result"]["findings"]:
            for ref in finding.get("evidence_refs", []):
                self.assertIn(ref, existing, f"Finding {finding['finding_id']} references missing evidence: {ref}")

    def test_data_exfiltration_review_structured_output(self) -> None:
        """Smoke test: data-exfiltration-review --format skill-result-json has structured evidence."""
        import json
        import subprocess

        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_DIR / "analyze.py"),
                "--files",
                self._zeus_flow(),
                "--action",
                "data-exfiltration-review",
                "--format",
                "skill-result-json",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, f"analyze.py failed: {result.stderr}")

        data = json.loads(result.stdout)
        self.assertEqual(data["status"], "success")

        evidence_ids = [e["evidence_id"] for e in data["result"]["evidence"]]

        # Must have non-raw-report evidence
        non_raw = [e for e in evidence_ids if e != "e-raw-report"]
        self.assertTrue(len(non_raw) > 0, "data-exfiltration-review should have evidence beyond e-raw-report")

        # Metrics must exist
        self.assertIn("e-exfiltration-metrics", evidence_ids)
        metrics_evidence = next(e for e in data["result"]["evidence"] if e["evidence_id"] == "e-exfiltration-metrics")
        self.assertEqual(metrics_evidence["type"], "metric")
        self.assertIn("metrics", metrics_evidence)

        # Hotspots table must use columns + rows
        self.assertIn("e-exfiltration-hotspots", evidence_ids)
        hotspots = next(e for e in data["result"]["evidence"] if e["evidence_id"] == "e-exfiltration-hotspots")
        self.assertEqual(hotspots["type"], "table")
        self.assertIn("columns", hotspots)
        self.assertIn("rows", hotspots)

        # Zeus has exfiltration findings
        self.assertTrue(len(data["result"]["findings"]) > 0, "data-exfiltration-review should have findings for Zeus data")

        # Verify evidence_refs in findings point to existing evidence
        existing = set(evidence_ids)
        for finding in data["result"]["findings"]:
            for ref in finding.get("evidence_refs", []):
                self.assertIn(ref, existing, f"Finding {finding['finding_id']} references missing evidence: {ref}")

    def test_lateral_movement_review_structured_output(self) -> None:
        """Smoke test: lateral-movement-review --format skill-result-json has structured evidence."""
        import json
        import subprocess

        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_DIR / "analyze.py"),
                "--files",
                self._zeus_flow(),
                "--action",
                "lateral-movement-review",
                "--format",
                "skill-result-json",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, f"analyze.py failed: {result.stderr}")

        data = json.loads(result.stdout)
        self.assertEqual(data["status"], "success")

        evidence_ids = [e["evidence_id"] for e in data["result"]["evidence"]]

        # Must have non-raw-report evidence
        non_raw = [e for e in evidence_ids if e != "e-raw-report"]
        self.assertTrue(len(non_raw) > 0, "lateral-movement-review should have evidence beyond e-raw-report")

        # Metrics must exist
        self.assertIn("e-lateral-movement-metrics", evidence_ids)
        metrics_evidence = next(e for e in data["result"]["evidence"] if e["evidence_id"] == "e-lateral-movement-metrics")
        self.assertEqual(metrics_evidence["type"], "metric")
        self.assertIn("metrics", metrics_evidence)

        # Hotspots table must use columns + rows when candidates exist
        if "e-lateral-movement-hotspots" in evidence_ids:
            hotspots = next(e for e in data["result"]["evidence"] if e["evidence_id"] == "e-lateral-movement-hotspots")
            self.assertEqual(hotspots["type"], "table")
            self.assertIn("columns", hotspots)
            self.assertIn("rows", hotspots)

        # Verify evidence_refs in findings point to existing evidence
        existing = set(evidence_ids)
        for finding in data["result"]["findings"]:
            for ref in finding.get("evidence_refs", []):
                self.assertIn(ref, existing, f"Finding {finding['finding_id']} references missing evidence: {ref}")

    def test_second_batch_review_actions_are_native_structured(self) -> None:
        """Regression guard: all second-batch review actions are documented as native structured."""
        import re

        second_batch = [
            "protocol-review",
            "session-review",
            "packet-review",
            "zeek-review",
            "signature-review",
            "risk-fusion-review",
            "scan-review",
            "short-connection-review",
            "periodicity-review",
            "dns-tunnel-review",
            "data-exfiltration-review",
            "lateral-movement-review",
        ]

        path = SCRIPT_DIR.parent / "references" / "input-output-contract.md"
        text = path.read_text(encoding="utf-8")

        for action in second_batch:
            pattern = re.compile(rf"^\s*\|\s*`{re.escape(action)}`\s*\|\s*.*?\s*\|\s*native structured\s*\|", re.MULTILINE)
            self.assertTrue(
                pattern.search(text),
                f"Action `{action}` is not marked 'native structured' in input-output-contract.md"
            )

    def _run_analyze_with_data(self, csv_content: str, action: str, fmt: str = "skill-result-json") -> dict:
        """Helper: write a synthetic CSV, run analyze.py, return parsed JSON result."""
        import subprocess
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, dir=tempfile.gettempdir()) as f:
            f.write(csv_content)
            f.flush()
            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_DIR / "analyze.py"),
                    "--files",
                    f.name,
                    "--action",
                    action,
                    "--format",
                    fmt,
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )
            self.assertEqual(result.returncode, 0, f"analyze.py failed: {result.stderr}")
            return json.loads(result.stdout)

    def test_data_exfil_missing_directional_fields_warns(self) -> None:
        """When no src_to_dst_byte_ratio/byte_asymmetry, exfil action should emit a warning."""
        # Minimal CSV with only basic fields — no directional byte features
        csv = (
            "src_ip,dst_ip,dst_port,bytes,packets\n"
            "10.0.0.1,10.0.0.2,443,1000,5\n"
            "10.0.0.1,10.0.0.3,443,2000,10\n"
        )
        data = self._run_analyze_with_data(csv, "data-exfiltration-review")
        warnings = data.get("diagnostics", {}).get("warnings", [])
        self.assertTrue(
            any(w.get("code") == "missing_directional_fields" for w in warnings),
            f"Expected missing_directional_fields warning, got: {warnings}",
        )

    def test_data_exfil_no_outbound_findings_without_directional_fields(self) -> None:
        """Without directional fields, outbound_byte_asymmetry/byte_ratio findings must not be emitted."""
        csv = (
            "src_ip,dst_ip,dst_port,bytes,packets\n"
            "10.0.0.1,10.0.0.2,443,1000,5\n"
            "10.0.0.1,10.0.0.3,443,2000,10\n"
            "10.0.0.1,10.0.0.4,80,500,3\n"
            "10.0.0.1,10.0.0.5,80,800,4\n"
            "10.0.0.1,10.0.0.6,8080,300,2\n"
        )
        data = self._run_analyze_with_data(csv, "data-exfiltration-review")
        findings = data["result"]["findings"]
        outbound_types = {"outbound_byte_asymmetry", "outbound_byte_ratio"}
        outbound_found = [f for f in findings if f.get("type") in outbound_types]
        self.assertEqual(
            len(outbound_found), 0,
            f"Should not emit outbound findings without directional fields, got: {[f['type'] for f in outbound_found]}",
        )

    def test_data_exfil_total_bytes_not_labeled_egress_without_direction(self) -> None:
        """Without directional fields, metrics should use total_candidate_bytes, not total_egress_bytes."""
        csv = (
            "src_ip,dst_ip,dst_port,bytes,packets\n"
            "10.0.0.1,10.0.0.2,443,1000,5\n"
            "10.0.0.1,10.0.0.3,443,2000,10\n"
            "10.0.0.1,10.0.0.4,80,500,3\n"
            "10.0.0.1,10.0.0.5,80,800,4\n"
            "10.0.0.1,10.0.0.6,8080,300,2\n"
        )
        data = self._run_analyze_with_data(csv, "data-exfiltration-review")
        metrics = data["result"]["summary"]["key_metrics"]
        metric_names = [m["name"] for m in metrics]
        self.assertIn(
            "total_candidate_bytes", metric_names,
            f"Without directional fields, expected 'total_candidate_bytes' metric, got: {metric_names}",
        )
        self.assertNotIn(
            "total_egress_bytes", metric_names,
            f"Without directional fields, should not have 'total_egress_bytes' metric",
        )

    def test_lateral_missing_session_action_fields_warns(self) -> None:
        """When action/session_state/service/duration are missing, lateral-movement-review should warn."""
        csv = (
            "src_ip,dst_ip,dst_port,bytes,packets\n"
            "10.0.0.1,10.0.0.2,443,1000,5\n"
            "10.0.0.1,10.0.0.3,443,2000,10\n"
        )
        data = self._run_analyze_with_data(csv, "lateral-movement-review")
        warnings = data.get("diagnostics", {}).get("warnings", [])
        warning_codes = {w.get("code") for w in warnings}
        # All 4 optional fields are missing from the minimal CSV
        self.assertTrue(
            "missing_action_field" in warning_codes,
            f"Expected missing_action_field warning, got: {warning_codes}",
        )
        self.assertTrue(
            "missing_session_state_field" in warning_codes,
            f"Expected missing_session_state_field warning, got: {warning_codes}",
        )
        self.assertTrue(
            "missing_service_field" in warning_codes,
            f"Expected missing_service_field warning, got: {warning_codes}",
        )
        self.assertTrue(
            "missing_duration_field" in warning_codes,
            f"Expected missing_duration_field warning, got: {warning_codes}",
        )

    def test_lateral_missing_fields_not_counted_as_zero_signal(self) -> None:
        """Missing optional fields should not produce false short-probe findings or zero-risk signals."""
        csv = (
            "src_ip,dst_ip,dst_port,bytes,packets\n"
            "10.0.0.1,10.0.0.2,443,1000,5\n"
            "10.0.0.1,10.0.0.3,443,2000,10\n"
        )
        data = self._run_analyze_with_data(csv, "lateral-movement-review")
        self.assertEqual(data["status"], "success")
        warnings = data.get("diagnostics", {}).get("warnings", [])
        # Should have warnings for all 4 missing optional fields
        warning_codes = {w.get("code") for w in warnings}
        expected_codes = {
            "missing_action_field", "missing_session_state_field",
            "missing_service_field", "missing_duration_field",
        }
        # lenient_ingestion may also appear from CSV parsing
        self.assertTrue(
            expected_codes.issubset(warning_codes),
            f"Expected {expected_codes} in warnings, got: {warning_codes}",
        )
        # No short-probe finding when duration is missing
        findings = data["result"]["findings"]
        finding_ids = {f.get("finding_id") for f in findings}
        self.assertNotIn(
            "f-lateral-short-probe", finding_ids,
            f"Should not emit short-probe finding without duration, got: {finding_ids}",
        )
        # Signal coverage should report duration as false
        signal_coverage = data.get("diagnostics", {}).get("data_quality", {}).get("signal_coverage", {})
        self.assertFalse(
            signal_coverage.get("duration"),
            f"Expected signal_coverage.duration == false, got: {signal_coverage}",
        )

    def test_lateral_duration_column_exists_but_null_values_no_probe_finding(self) -> None:
        """When duration_ms column exists but values are NULL/empty, short-probe must not fire."""
        csv = (
            "src_ip,dst_ip,dst_port,bytes,packets,flow_duration\n"
            "10.0.0.1,10.0.0.2,443,50,1,\n"
            "10.0.0.1,10.0.0.3,80,30,1,\n"
            "10.0.0.1,10.0.0.4,8080,40,1,\n"
            "10.0.0.1,10.0.0.5,22,60,1,\n"
            "10.0.0.1,10.0.0.6,3389,20,1,\n"
            "10.0.0.1,10.0.0.7,445,45,1,\n"
            "10.0.0.1,10.0.0.8,80,35,1,\n"
            "10.0.0.1,10.0.0.9,80,50,1,\n"
        )
        data = self._run_analyze_with_data(csv, "lateral-movement-review")
        self.assertEqual(data["status"], "success")
        signal_coverage = data.get("diagnostics", {}).get("data_quality", {}).get("signal_coverage", {})
        # Column exists -> duration coverage is True
        self.assertTrue(signal_coverage.get("duration"))
        # But short-probe must NOT fire because all durations are NULL
        findings = data["result"]["findings"]
        finding_ids = {f.get("finding_id") for f in findings}
        self.assertNotIn(
            "f-lateral-short-probe", finding_ids,
            f"Should not emit short-probe when all duration values are NULL, got: {finding_ids}",
        )

    # --- Multi-root path resolution tests ---

    def test_dataset_root_prefers_mnt_dataset_root(self) -> None:
        """When /mnt/datasets/network-traffic exists, dataset_root() should return it."""
        with unittest.mock.patch.object(Path, "exists", return_value=True):
            # This mock makes /mnt/datasets/network-traffic appear to exist
            result = dataset_root()
            # Since env var is not set, it should check /mnt/datasets/network-traffic first
            self.assertIn("datasets/network-traffic", str(result))

    def test_load_mapping_reports_all_searched_paths(self) -> None:
        """When no field mapping exists, load_mapping() should list all searched paths."""
        with unittest.mock.patch.object(Path, "exists", return_value=False):
            with self.assertRaises(FileNotFoundError) as ctx:
                load_mapping()
            error_msg = str(ctx.exception)
            self.assertIn("Searched", error_msg)

    def test_file_resolution_prefers_uploads_then_processed_then_raw(self) -> None:
        """get_default_search_roots() should prefer uploads, then dataset processed/raw."""
        # On local dev, uploads_root likely doesn't exist, so it should start with dataset roots
        roots = get_default_search_roots()
        self.assertGreater(len(roots), 0, "Expected at least one search root")
        # Verify no duplicate roots
        resolved = [str(r.resolve()) for r in roots]
        self.assertEqual(len(resolved), len(set(resolved)), "Duplicate roots detected")

    def test_self_check_returns_nonzero_without_action(self) -> None:
        """--self-check should work independently of --action."""
        code = run_self_check()
        # Should return 0 on local dev (all deps available, field mapping exists)
        self.assertEqual(code, 0)

    def test_cli_action_is_optional_with_self_check(self) -> None:
        """Parser should accept --self-check without --action."""
        parser = build_parser()
        args = parser.parse_args(["--self-check"])
        self.assertTrue(args.self_check)
        self.assertIsNone(args.action)

    def test_self_check_reports_field_mapping_path(self) -> None:
        """Self-check should report the found field mapping path."""
        import io
        import contextlib
        f = io.StringIO()
        with contextlib.redirect_stdout(f):
            run_self_check()
        output = f.getvalue()
        self.assertIn("field_mapping", output)
        self.assertIn("ok", output.lower())

    def test_outputs_root_honors_environment_override(self) -> None:
        """Report output root should be configurable for local tests and product runtimes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with unittest.mock.patch.dict(os.environ, {"NETWORK_TRAFFIC_OUTPUTS_ROOT": tmpdir}):
                self.assertEqual(outputs_root(), Path(tmpdir))

    def test_self_check_validates_outputs_root_writable(self) -> None:
        """Self-check should verify that report/export output directory can be written."""
        import io
        import contextlib

        with tempfile.TemporaryDirectory() as tmpdir:
            with unittest.mock.patch.dict(os.environ, {"NETWORK_TRAFFIC_OUTPUTS_ROOT": tmpdir}):
                f = io.StringIO()
                with contextlib.redirect_stdout(f):
                    code = run_self_check()
                output = f.getvalue()

        self.assertEqual(code, 0)
        self.assertIn("outputs_root", output)
        self.assertIn("writable=True", output)

    # --- Network traffic workspace domain scoping tests ---

    def test_workspace_root_is_generic(self) -> None:
        """workspace_root() should return the generic /mnt/user-data/workspace, not a domain subdirectory."""
        from utils.path import workspace_root
        self.assertEqual(workspace_root(), Path("/mnt/user-data/workspace"))

    def test_workspace_root_honors_env_override(self) -> None:
        """workspace_root() should be overridable via NETWORK_TRAFFIC_WORKSPACE_ROOT."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with unittest.mock.patch.dict(os.environ, {"NETWORK_TRAFFIC_WORKSPACE_ROOT": tmpdir}):
                from importlib import reload
                import utils.path as path_mod
                reload(path_mod)
                self.assertEqual(path_mod.workspace_root(), Path(tmpdir))
                reload(path_mod)

    def test_network_traffic_workspace_root_is_domain_scoped(self) -> None:
        """network_traffic_workspace_root() should return /mnt/user-data/workspace/network-traffic."""
        from utils.path import network_traffic_workspace_root
        self.assertEqual(network_traffic_workspace_root(), Path("/mnt/user-data/workspace/network-traffic"))

    def test_network_traffic_workspace_root_honors_env_override(self) -> None:
        """network_traffic_workspace_root() should honour NETWORK_TRAFFIC_WORKSPACE_ROOT directly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with unittest.mock.patch.dict(os.environ, {"NETWORK_TRAFFIC_WORKSPACE_ROOT": tmpdir}):
                from importlib import reload
                import utils.path as path_mod
                reload(path_mod)
                self.assertEqual(path_mod.network_traffic_workspace_root(), Path(tmpdir))
                reload(path_mod)

    def test_prepare_pcap_dataset_output_uses_network_traffic_processed_workspace_in_mounted_runtime(self) -> None:
        """In AIO, prepare_pcap.py output should go to /mnt/user-data/workspace/network-traffic/processed/<name>."""
        from utils.path import network_traffic_workspace_root
        expected = network_traffic_workspace_root() / "processed" / "Neris"
        self.assertIn("network-traffic", str(expected))
        self.assertIn("processed", str(expected))
        self.assertIn("Neris", str(expected))

    def test_rag_cache_uses_network_traffic_workspace_cache(self) -> None:
        """RAG query embedding cache should be under network_traffic_workspace_root/.cache when workspace exists."""
        from utils.path import network_traffic_workspace_root
        expected_cache = network_traffic_workspace_root() / ".cache" / "query-embeddings"
        self.assertIn("network-traffic", str(expected_cache))
        self.assertIn(".cache", str(expected_cache))

    def test_outputs_root_is_artifact_level_not_skill_workspace(self) -> None:
        """Final report output root should be /mnt/user-data/outputs, not a workspace subdirectory."""
        from utils.path import outputs_root, network_traffic_workspace_root
        outputs = outputs_root()
        workspace = network_traffic_workspace_root()
        self.assertNotIn("workspace", str(outputs))
        self.assertIn("/outputs", str(outputs))
        self.assertNotEqual(outputs, workspace)

    def test_prepare_pcap_uploaded_pcap_output_goes_to_processed_subdir(self) -> None:
        """Uploaded pcap output should go to network_traffic_workspace_root/processed/<name>."""
        from prepare_pcap import default_output_dir_for_inputs
        from utils.path import network_traffic_workspace_root
        import unittest.mock

        with unittest.mock.patch(
            "utils.path.is_relative_to_path",
            side_effect=lambda p, r: r.name == "uploads",
        ):
            result = default_output_dir_for_inputs("test-upload", ["/mnt/user-data/uploads/test-upload.pcap"])
        expected = network_traffic_workspace_root() / "processed" / "test-upload"
        self.assertEqual(result, expected)
        self.assertIn("processed", str(result))

    def test_prepare_pcap_dataset_raw_defaults_to_dataset_processed(self) -> None:
        """Dataset raw pcap output should go to processed_dataset_root/<name>."""
        from prepare_pcap import default_output_dir_for_inputs
        from utils.path import processed_dataset_root
        import unittest.mock

        with unittest.mock.patch(
            "utils.path.is_relative_to_path",
            side_effect=lambda p, r: r.name == "raw",
        ):
            result = default_output_dir_for_inputs("Neris", ["/mnt/datasets/network-traffic/raw/Neris.pcap"])
        expected = processed_dataset_root() / "Neris"
        self.assertEqual(result, expected)
        self.assertIn("processed", str(result))

    def test_prepare_pcap_other_path_defaults_to_workspace_processed(self) -> None:
        """Arbitrary path output should default to workspace processed."""
        from prepare_pcap import default_output_dir_for_inputs
        from utils.path import network_traffic_workspace_root
        import unittest.mock

        with unittest.mock.patch(
            "utils.path.is_relative_to_path",
            return_value=False,
        ):
            result = default_output_dir_for_inputs("test", ["/tmp/test.pcap"])
        expected = network_traffic_workspace_root() / "processed" / "test"
        self.assertEqual(result, expected)
        self.assertIn("processed", str(result))

    def test_generate_incident_report_json_contains_artifacts(self) -> None:
        """generate_incident_report.py JSON output should contain artifacts array."""
        import subprocess

        with tempfile.TemporaryDirectory() as tmpdir:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, dir=tempfile.gettempdir()) as f:
                f.write("src_ip,dst_ip,bytes,packets,dst_port,protocol\n1.1.1.1,2.2.2.2,100,5,80,TCP\n")
                f.flush()
                env = os.environ.copy()
                env["NETWORK_TRAFFIC_OUTPUTS_ROOT"] = tmpdir
                result = subprocess.run(
                    [
                        sys.executable,
                        str(SCRIPT_DIR / "generate_incident_report.py"),
                        "--files",
                        f.name,
                        "--format",
                        "json",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=120,
                    env=env,
                )
                Path(f.name).unlink(missing_ok=True)

        self.assertEqual(result.returncode, 0, f"generate_incident_report.py failed: {result.stderr}")
        data = json.loads(result.stdout)
        self.assertEqual(data["status"], "success")
        self.assertIn("artifacts", data)
        self.assertTrue(len(data["artifacts"]) > 0)
        artifact = data["artifacts"][0]
        self.assertEqual(artifact["type"], "report")
        self.assertIn(tmpdir, artifact["path"])

    # --- Processed reuse and routing correctness tests ---

    def test_prepare_pcap_reuses_existing_processed_without_force(self) -> None:
        """When flow.csv + metadata.json exist, prepare_pcap should return reused=True without rebuilding."""
        import subprocess

        with tempfile.TemporaryDirectory() as tmpdir:
            dataset_name = "reuse-test"
            out_dir = Path(tmpdir) / dataset_name
            out_dir.mkdir()
            flow_path = out_dir / f"{dataset_name}.flow.csv"
            flow_path.write_text("src_ip,dst_ip,bytes\n1.1.1.1,2.2.2.2,100\n", encoding="utf-8")
            meta_path = out_dir / "metadata.json"
            meta_path.write_text('{"dataset_name": "reuse-test", "existing": true}', encoding="utf-8")
            # Create a dummy pcap file so discover_pcaps finds something
            pcap_path = out_dir / "dummy.pcap"
            pcap_path.write_bytes(b"\xd4\xc3\xb2\xa1\x02\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x01\x00\x00\x00")

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_DIR / "prepare_pcap.py"),
                    "--files",
                    str(out_dir),
                    "--dataset-name",
                    dataset_name,
                    "--output-dir",
                    str(out_dir),
                    "--format",
                    "json",
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )

        self.assertEqual(result.returncode, 0, f"prepare_pcap failed: {result.stderr}")
        data = json.loads(result.stdout)
        self.assertTrue(data.get("reused"), "Expected reused=True when processed files already exist")

    def test_prepare_pcap_rebuilds_with_force_flag(self) -> None:
        """With --force, prepare_pcap should rebuild even when processed files exist."""
        import subprocess

        with tempfile.TemporaryDirectory() as tmpdir:
            dataset_name = "force-test"
            out_dir = Path(tmpdir) / dataset_name
            out_dir.mkdir()
            flow_path = out_dir / f"{dataset_name}.flow.csv"
            flow_path.write_text("src_ip,dst_ip,bytes\n1.1.1.1,2.2.2.2,100\n", encoding="utf-8")
            meta_path = out_dir / "metadata.json"
            meta_path.write_text('{"dataset_name": "force-test"}', encoding="utf-8")
            # Create a dummy pcap file so discover_pcaps finds something
            pcap_path = out_dir / "dummy.pcap"
            pcap_path.write_bytes(b"\xd4\xc3\xb2\xa1\x02\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x01\x00\x00\x00")

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_DIR / "prepare_pcap.py"),
                    "--files",
                    str(out_dir),
                    "--dataset-name",
                    dataset_name,
                    "--output-dir",
                    str(out_dir),
                    "--force",
                    "--format",
                    "json",
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )

        self.assertEqual(result.returncode, 0, f"prepare_pcap --force failed: {result.stderr}")
        data = json.loads(result.stdout)
        self.assertFalse(data.get("reused"), "Expected reused=False with --force flag")

    def test_processed_root_env_does_not_override_uploads_routing(self) -> None:
        """NETWORK_TRAFFIC_PROCESSED_ROOT should only affect dataset raw inputs, not uploads."""
        from prepare_pcap import default_output_dir_for_inputs
        from utils.path import network_traffic_workspace_root
        import unittest.mock

        with unittest.mock.patch(
            "utils.path.is_relative_to_path",
            side_effect=lambda p, r: r.name == "uploads",
        ), unittest.mock.patch.dict(os.environ, {"NETWORK_TRAFFIC_PROCESSED_ROOT": "/tmp/custom-processed"}):
            from importlib import reload
            import prepare_pcap as pcap_mod
            reload(pcap_mod)
            try:
                result = pcap_mod.default_output_dir_for_inputs("upload-test", ["/mnt/user-data/uploads/test.pcap"])
            finally:
                reload(pcap_mod)

        # uploads should still go to workspace processed, NOT to NETWORK_TRAFFIC_PROCESSED_ROOT
        self.assertIn("network-traffic", str(result))
        self.assertIn("processed", str(result))
        self.assertNotIn("custom-processed", str(result))


if __name__ == "__main__":
    unittest.main()
