from __future__ import annotations

import sys
import unittest
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from analysis.anomaly_models import score_generic_candidates, score_scan_candidates, score_short_connection_candidates


class TestAnomalyRcf(unittest.TestCase):
    def test_generic_candidates_support_true_rcf_engine(self) -> None:
        rows = [
            {"dst_port": "53", "records": 100, "unique_src_ip": 1, "unique_dst_ip": 1, "total_bytes": 1000, "unique_protocols": 1, "unique_app_protocols": 1},
            {"dst_port": "0", "records": 2, "unique_src_ip": 3, "unique_dst_ip": 2, "total_bytes": 10, "unique_protocols": 2, "unique_app_protocols": 1},
            {"dst_port": "443", "records": 10, "unique_src_ip": 1, "unique_dst_ip": 1, "total_bytes": 500, "unique_protocols": 1, "unique_app_protocols": 1},
        ]
        ranked = score_generic_candidates(
            rows,
            numeric_fields=["records", "unique_src_ip", "unique_dst_ip", "total_bytes", "unique_protocols", "unique_app_protocols"],
            categorical_fields=["dst_port"],
            rule_score_fn=lambda row: 0.5 if float(row.get("records") or 0) <= 3 else 0.1,
            reason_fn=lambda row, final, rule_score: "test_reason",
            output_field="risk_score",
            contamination=0.2,
            engine="rcf",
        )

        self.assertTrue(ranked)
        self.assertIn("rcf_score", ranked[0])
        self.assertEqual(ranked[0]["risk_score"], ranked[0]["rcf_score"])

    def test_scan_candidates_support_true_rcf_engine(self) -> None:
        rows = [
            {"src_ip": "a", "flows": 100, "unique_dst_ip": 2, "unique_dst_port": 2, "total_bytes": 1000, "avg_bytes": 10, "unique_protocols": 1, "unique_app_protocols": 1, "syn_only_packets": 0, "rst_packets": 0, "syn_only_pct": 0, "rst_pct": 0},
            {"src_ip": "b", "flows": 5, "unique_dst_ip": 50, "unique_dst_port": 60, "total_bytes": 50, "avg_bytes": 10, "unique_protocols": 2, "unique_app_protocols": 1, "syn_only_packets": 20, "rst_packets": 0, "syn_only_pct": 80, "rst_pct": 0},
            {"src_ip": "c", "flows": 10, "unique_dst_ip": 3, "unique_dst_port": 4, "total_bytes": 100, "avg_bytes": 10, "unique_protocols": 1, "unique_app_protocols": 1, "syn_only_packets": 0, "rst_packets": 10, "syn_only_pct": 0, "rst_pct": 100},
        ]
        ranked = score_scan_candidates(rows, packet_view=False, engine="rcf")

        self.assertTrue(ranked)
        self.assertIn("rcf_score", ranked[0])
        self.assertEqual(ranked[0]["scan_risk_score"], ranked[0]["rcf_score"])

    def test_short_connection_candidates_no_longer_raise_name_error(self) -> None:
        rows = [
            {
                "bytes": 1,
                "packets": 1,
                "duration_ms": 1,
                "payload_bytes": 0,
                "src_flow_count": 1,
                "src_unique_dst_ip": 1,
                "src_unique_dst_port": 1,
                "dst_flow_count": 1,
                "dst_unique_src_ip": 1,
                "session_state": "SYN",
                "protocol": "TCP",
            }
        ]
        ranked = score_short_connection_candidates(rows)

        self.assertEqual(len(ranked), 1)
        self.assertIn("anomaly_score", ranked[0])


if __name__ == "__main__":
    unittest.main()
