import unittest
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from utils.path import repo_root


def _zeus_flow() -> str:
    path = repo_root() / "datasets/network-traffic/processed/Zeus/Zeus.flow.csv"
    if not path.exists():
        raise unittest.SkipTest("Zeus dataset not present")
    return str(path)


class TestDnsTunnelStructuredOutput(unittest.TestCase):
    """Smoke test: dns-tunnel-review --format skill-result-json has structured evidence."""

    def test_dns_tunnel_structured_output_tinba(self) -> None:
        import json
        import subprocess

        path = repo_root() / "datasets/network-traffic/processed/Tinba/Tinba.flow.csv"
        if not path.exists():
            self.skipTest("Tinba dataset not present (cleaned up)")

        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_DIR / "analyze.py"),
                "--files",
                str(path),
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
                _zeus_flow(),
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


if __name__ == "__main__":
    unittest.main()
