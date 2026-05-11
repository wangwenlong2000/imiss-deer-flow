"""Unit tests for rag_search.py linked-flow retrieval helpers."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPT_DIR))

from rag_search import (
    _build_linked_flow_query,
    _normalize_service_for_match,
    _parent_risk_for,
    _risk_of,
    format_hits,
)
from pathlib import Path


class TestRiskOf(unittest.TestCase):
    def test_risk_level_direct(self):
        """Hit with top-level risk_level returns it."""
        hit = {"doc_type": "action_finding", "risk_level": "high"}
        self.assertEqual(_risk_of(hit), "high")

    def test_linked_parent_risk(self):
        """Linked flow without risk_level falls back to linked_parent_risk_level."""
        hit = {
            "doc_type": "flow_summary",
            "linked_to_action": "zeek-review",
            "linked_parent_risk_level": "critical",
        }
        self.assertEqual(_risk_of(hit), "critical")

    def test_severity_fallback(self):
        """Hit with only severity uses that."""
        hit = {"doc_type": "action_finding", "severity": "medium"}
        self.assertEqual(_risk_of(hit), "medium")

    def test_default_low(self):
        """Hit with no risk info defaults to 'low'."""
        hit = {"doc_type": "flow_summary"}
        self.assertEqual(_risk_of(hit), "low")


class TestParentRiskFor(unittest.TestCase):
    def setUp(self):
        self.action_hits = [
            {
                "_source": {
                    "doc_id": "abc",
                    "doc_type": "action_finding",
                    "action_name": "zeek-review",
                    "finding_id": "f-zeek-001",
                    "severity": "high",
                }
            },
            {
                "_source": {
                    "doc_id": "def",
                    "doc_type": "action_evidence",
                    "action_name": "signature-review",
                    "finding_id": "f-sig-002",
                    "risk_level": "critical",
                }
            },
        ]

    def test_match_by_action(self):
        """Linked flow matching parent action gets its severity."""
        lf = {"linked_to_action": "zeek-review", "linked_to_finding": ""}
        self.assertEqual(_parent_risk_for(lf, self.action_hits), "high")

    def test_match_by_finding(self):
        """Linked flow matching finding_id gets its severity."""
        lf = {"linked_to_action": "", "linked_to_finding": "f-sig-002"}
        self.assertEqual(_parent_risk_for(lf, self.action_hits), "critical")

    def test_no_match(self):
        """Unmatched linked flow returns 'info'."""
        lf = {"linked_to_action": "unknown", "linked_to_finding": ""}
        self.assertEqual(_parent_risk_for(lf, self.action_hits), "info")


class TestFormatHitsLinkedFields(unittest.TestCase):
    def test_linked_fields_in_output(self):
        """format_hits includes linked_to_action/linked_to_finding/dst_port/etc."""
        item = {
            "_source": {
                "doc_id": "flow-001",
                "doc_type": "flow_summary",
                "schema_version": "rag_doc_v2",
                "title": "test",
                "summary": "test summary",
                "dataset_name": "Neris",
                "source_file": "test.csv",
                "keywords": [],
                "metadata": {},
                "dst_ips": ["8.8.8.8"],
                "dst_port": 443,
                "service": "ssl",
                "timestamp": "2026-05-10T00:00:00Z",
                "linked_to_action": "zeek-review",
                "linked_to_finding": "f-zeek-001",
            },
            "_score": 1.5,
        }
        result = format_hits([item])
        hit = result[0]
        self.assertEqual(hit["linked_to_action"], "zeek-review")
        self.assertEqual(hit["linked_to_finding"], "f-zeek-001")
        self.assertEqual(hit["dst_port"], 443)
        self.assertEqual(hit["service"], "ssl")
        self.assertIn("8.8.8.8", hit["dst_ips"])
        self.assertEqual(hit["timestamp"], "2026-05-10T00:00:00Z")

    def test_linked_fields_empty_values_stripped(self):
        """Empty linked fields are not included in output."""
        item = {
            "_source": {
                "doc_id": "flow-001",
                "doc_type": "flow_summary",
                "schema_version": "rag_doc_v2",
                "title": "test",
                "summary": "test summary",
                "dataset_name": "Neris",
                "source_file": "test.csv",
                "keywords": [],
                "metadata": {},
                "linked_to_action": "",
                "linked_to_finding": "",
            },
            "_score": 1.0,
        }
        result = format_hits([item])
        hit = result[0]
        self.assertNotIn("linked_to_action", hit)
        self.assertNotIn("linked_to_finding", hit)


class TestRiskFilterNotOverwritten(unittest.TestCase):
    def test_risk_filter_preserves_linked_flows_with_parent_risk(self):
        """
        After risk filter is applied, linked flows that inherit parent's
        high risk should NOT be filtered out when --risk-level high is set.
        This is a pure-function simulation of the full pipeline.
        """
        from rag_search import format_hits

        # Simulate: 1 action_finding (high), 1 linked flow (inherited high)
        fused = [
            {
                "_source": {
                    "doc_id": "act-001",
                    "doc_type": "action_finding",
                    "schema_version": "rag_doc_v2",
                    "title": "Finding",
                    "summary": "High severity finding",
                    "dataset_name": "Neris",
                    "source_file": "test.csv",
                    "keywords": [],
                    "metadata": {},
                    "severity": "high",
                },
                "_score": 2.0,
            },
            {
                "_source": {
                    "doc_id": "flow-001",
                    "doc_type": "flow_summary",
                    "schema_version": "rag_doc_v2",
                    "title": "Related flow",
                    "summary": "Linked flow",
                    "dataset_name": "Neris",
                    "source_file": "test.csv",
                    "keywords": [],
                    "metadata": {},
                    "linked_to_action": "some-action",
                    "linked_to_finding": "act-001",
                    "linked_parent_risk_level": "high",
                },
                "_score": 0.0,
            },
        ]

        all_formatted = format_hits(fused)

        # Apply risk filter (simulating --risk-level high)
        risk_levels = ["high"]
        risk_level_order = {"low": 0, "medium": 1, "high": 2, "critical": 3}
        min_score = min(risk_level_order.get(r, 0) for r in risk_levels)
        filtered = []
        for hit in all_formatted:
            rl = _risk_of(hit)
            if risk_level_order.get(rl, 0) >= min_score:
                filtered.append(hit)

        # Both the action_finding and linked flow should survive
        self.assertEqual(len(filtered), 2)
        types = {h.get("doc_type") for h in filtered}
        self.assertIn("action_finding", types)
        self.assertIn("flow_summary", types)


class TestBuildLinkedFlowQueryFieldPaths(unittest.TestCase):
    def test_source_includes_metadata(self):
        """_source whitelist must include 'metadata' for flow_summary field lookup."""
        flow_filter = {
            "dataset_name": "Neris",
            "primary_entity": {"type": "src_ip", "value": "10.0.0.1"},
        }
        query = _build_linked_flow_query(flow_filter)
        self.assertIn("metadata", query["_source"])

    def test_src_ip_field_path(self):
        """primary_entity src_ip maps to metadata.src_ip term query."""
        flow_filter = {
            "dataset_name": "Neris",
            "primary_entity": {"type": "src_ip", "value": "10.0.0.1"},
        }
        query = _build_linked_flow_query(flow_filter)
        bool_query = query["query"]["bool"]
        filter_clauses = bool_query["must"][-1]["bool"]["should"]
        self.assertIn({"term": {"metadata.src_ip": "10.0.0.1"}}, filter_clauses)

    def test_src_ips_values_field_path(self):
        """primary_entity src_ips values maps to metadata.src_ip terms query."""
        flow_filter = {
            "dataset_name": "Neris",
            "primary_entity": {"type": "src_ips", "values": ["10.0.0.1", "10.0.0.2"]},
        }
        query = _build_linked_flow_query(flow_filter)
        bool_query = query["query"]["bool"]
        filter_clauses = bool_query["must"][-1]["bool"]["should"]
        self.assertIn({"terms": {"metadata.src_ip": ["10.0.0.1", "10.0.0.2"]}}, filter_clauses)

    def test_dst_ips_field_path(self):
        """dst_ips maps to metadata.dst_ip term queries."""
        flow_filter = {
            "dataset_name": "Neris",
            "dst_ips": ["8.8.8.8", "1.1.1.1"],
        }
        query = _build_linked_flow_query(flow_filter)
        bool_query = query["query"]["bool"]
        filter_clauses = bool_query["must"][-1]["bool"]["should"]
        self.assertIn({"term": {"metadata.dst_ip": "8.8.8.8"}}, filter_clauses)
        self.assertIn({"term": {"metadata.dst_ip": "1.1.1.1"}}, filter_clauses)

    def test_ports_field_path(self):
        """ports maps to metadata.dst_port term queries with int conversion."""
        flow_filter = {
            "dataset_name": "Neris",
            "ports": [443, 80],
        }
        query = _build_linked_flow_query(flow_filter)
        bool_query = query["query"]["bool"]
        filter_clauses = bool_query["must"][-1]["bool"]["should"]
        self.assertIn({"term": {"metadata.dst_port": 443}}, filter_clauses)
        self.assertIn({"term": {"metadata.dst_port": 80}}, filter_clauses)

    def test_services_field_path(self):
        """services maps to metadata.app_protocol terms queries."""
        flow_filter = {
            "dataset_name": "Neris",
            "services": ["ssl", "http"],
        }
        query = _build_linked_flow_query(flow_filter)
        bool_query = query["query"]["bool"]
        filter_clauses = bool_query["must"][-1]["bool"]["should"]
        self.assertIn({"terms": {"metadata.app_protocol": ["ssl", "SSL"]}}, filter_clauses)
        self.assertIn({"terms": {"metadata.app_protocol": ["http", "HTTP"]}}, filter_clauses)

    def test_domains_field_path(self):
        """domains maps to top-level domains term queries (not in metadata)."""
        flow_filter = {
            "dataset_name": "Neris",
            "domains": ["example.com"],
        }
        query = _build_linked_flow_query(flow_filter)
        bool_query = query["query"]["bool"]
        filter_clauses = bool_query["must"][-1]["bool"]["should"]
        self.assertIn({"term": {"domains": "example.com"}}, filter_clauses)

    def test_dataset_name_filter(self):
        """dataset_name adds a must term for dataset scoping."""
        flow_filter = {"dataset_name": "Neris"}
        query = _build_linked_flow_query(flow_filter)
        must_clauses = query["query"]["bool"]["must"]
        self.assertIn({"term": {"doc_type": "flow_summary"}}, must_clauses)
        self.assertIn({"term": {"dataset_name": "Neris"}}, must_clauses)

    def test_no_filter_clauses_without_entities(self):
        """Empty flow_filter produces only doc_type must clause."""
        flow_filter = {}
        query = _build_linked_flow_query(flow_filter)
        must_clauses = query["query"]["bool"]["must"]
        # Only the doc_type term clause, no entity filter clauses
        self.assertEqual(len(must_clauses), 1)
        self.assertIn({"term": {"doc_type": "flow_summary"}}, must_clauses)

    def test_port_truncation_limit(self):
        """ports are truncated to first 10 values."""
        flow_filter = {"ports": list(range(15))}
        query = _build_linked_flow_query(flow_filter)
        bool_query = query["query"]["bool"]
        filter_clauses = bool_query["must"][-1]["bool"]["should"]
        port_clauses = [c for c in filter_clauses if "metadata.dst_port" in c.get("term", {})]
        self.assertEqual(len(port_clauses), 10)


class TestBuildLinkedFlowQueryPortTolerance(unittest.TestCase):
    """Port tolerance tests: non-integer port values should not raise exceptions."""

    def test_empty_string_port_skipped(self):
        """Empty string in ports list is skipped rather than raising ValueError."""
        flow_filter = {"ports": ["", "443"]}
        # Should not raise - empty strings are silently skipped
        query = _build_linked_flow_query(flow_filter)
        filter_clauses = query["query"]["bool"]["must"][-1]["bool"]["should"]
        # Only 443 should produce a clause
        port_clauses = [c for c in filter_clauses if "metadata.dst_port" in c.get("term", {})]
        self.assertEqual(len(port_clauses), 1)
        self.assertIn({"term": {"metadata.dst_port": 443}}, port_clauses)

    def test_non_numeric_string_port_skipped(self):
        """Non-numeric string like 'unknown' in ports list is skipped."""
        flow_filter = {"ports": ["unknown", "443", "-"]}
        query = _build_linked_flow_query(flow_filter)
        filter_clauses = query["query"]["bool"]["must"][-1]["bool"]["should"]
        port_clauses = [c for c in filter_clauses if "metadata.dst_port" in c.get("term", {})]
        self.assertEqual(len(port_clauses), 1)
        self.assertIn({"term": {"metadata.dst_port": 443}}, port_clauses)

    def test_dash_port_skipped(self):
        """Dash '-' in ports list is skipped."""
        flow_filter = {"ports": ["-", "80"]}
        query = _build_linked_flow_query(flow_filter)
        filter_clauses = query["query"]["bool"]["must"][-1]["bool"]["should"]
        port_clauses = [c for c in filter_clauses if "metadata.dst_port" in c.get("term", {})]
        self.assertEqual(len(port_clauses), 1)
        self.assertIn({"term": {"metadata.dst_port": 80}}, port_clauses)

    def test_all_invalid_ports_produces_no_port_clause(self):
        """All invalid ports produces no port filter clause at all."""
        flow_filter = {"ports": ["", "-", "unknown", "abc"]}
        query = _build_linked_flow_query(flow_filter)
        # No port clauses, query is just doc_type must clause
        must_clauses = query["query"]["bool"]["must"]
        self.assertEqual(len(must_clauses), 1)
        self.assertIn({"term": {"doc_type": "flow_summary"}}, must_clauses)
        # No should/filter clause added since all ports were invalid
        self.assertNotIn("should", query["query"]["bool"])

    def test_float_port_converted_to_int(self):
        """Float port values are converted to int by the ES query."""
        flow_filter = {"ports": [443.0]}
        query = _build_linked_flow_query(flow_filter)
        filter_clauses = query["query"]["bool"]["must"][-1]["bool"]["should"]
        self.assertIn({"term": {"metadata.dst_port": 443}}, filter_clauses)


class TestNormalizeServiceForMatch(unittest.TestCase):
    def test_sslv3_expands_to_ssl_tls_aliases(self):
        self.assertEqual(_normalize_service_for_match("SSLv3"), ["SSL", "TLS", "SSLV3"])

    def test_dns_query_maps_to_dns(self):
        self.assertEqual(_normalize_service_for_match("DNS Query"), ["DNS"])

    def test_unknown_service_preserves_original_and_uppercase(self):
        self.assertEqual(_normalize_service_for_match("customSvc"), ["customSvc", "CUSTOMSVC"])


if __name__ == "__main__":
    unittest.main()
