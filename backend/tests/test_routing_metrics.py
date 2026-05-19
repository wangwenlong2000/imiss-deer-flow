"""Tests for deerflow.routing.metrics module.

Run with:
    PYTHONPATH=backend/packages/harness python3 backend/tests/test_routing_metrics.py
"""

from __future__ import annotations

import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "packages" / "harness"))

from deerflow.routing import metrics


class TestRecordRequest:
    def test_trigger_true(self):
        metrics.reset_metrics()
        metrics.record_request(trigger=True, latency_ms=100.0)
        snap = metrics.get_metrics_snapshot()
        assert snap["total_requests"] == 1
        assert snap["trigger_true_count"] == 1
        assert snap["trigger_false_count"] == 0
        assert snap["avg_latency_ms"] == 100.0

    def test_trigger_false(self):
        metrics.reset_metrics()
        metrics.record_request(trigger=False, latency_ms=50.0)
        snap = metrics.get_metrics_snapshot()
        assert snap["total_requests"] == 1
        assert snap["trigger_false_count"] == 1
        assert snap["trigger_true_count"] == 0

    def test_average_latency(self):
        metrics.reset_metrics()
        metrics.record_request(trigger=True, latency_ms=100.0)
        metrics.record_request(trigger=True, latency_ms=200.0)
        snap = metrics.get_metrics_snapshot()
        assert snap["avg_latency_ms"] == 150.0


class TestRecordSkillHit:
    def test_single_hit(self):
        metrics.reset_metrics()
        metrics.record_skill_hit("skill-a")
        snap = metrics.get_metrics_snapshot()
        assert snap["skill_hit_counts"]["skill-a"] == 1

    def test_multiple_hits_same_skill(self):
        metrics.reset_metrics()
        metrics.record_skill_hit("skill-a")
        metrics.record_skill_hit("skill-a")
        snap = metrics.get_metrics_snapshot()
        assert snap["skill_hit_counts"]["skill-a"] == 2

    def test_multiple_skills(self):
        metrics.reset_metrics()
        metrics.record_skill_hit("skill-a")
        metrics.record_skill_hit("skill-b")
        snap = metrics.get_metrics_snapshot()
        assert snap["skill_hit_counts"]["skill-a"] == 1
        assert snap["skill_hit_counts"]["skill-b"] == 1


class TestErrorCounters:
    def test_es_error(self):
        metrics.reset_metrics()
        metrics.record_es_error()
        snap = metrics.get_metrics_snapshot()
        assert snap["es_errors"] == 1

    def test_embedding_error(self):
        metrics.reset_metrics()
        metrics.record_embedding_error()
        snap = metrics.get_metrics_snapshot()
        assert snap["embedding_errors"] == 1

    def test_reranker_error(self):
        metrics.reset_metrics()
        metrics.record_reranker_error()
        snap = metrics.get_metrics_snapshot()
        assert snap["reranker_errors"] == 1


class TestTriggerRatio:
    def test_all_true(self):
        metrics.reset_metrics()
        metrics.record_request(trigger=True, latency_ms=10.0)
        metrics.record_request(trigger=True, latency_ms=20.0)
        snap = metrics.get_metrics_snapshot()
        assert snap["trigger_ratio"] == 1.0

    def test_mixed(self):
        metrics.reset_metrics()
        metrics.record_request(trigger=True, latency_ms=10.0)
        metrics.record_request(trigger=False, latency_ms=5.0)
        snap = metrics.get_metrics_snapshot()
        assert snap["trigger_ratio"] == 0.5

    def test_no_requests(self):
        metrics.reset_metrics()
        snap = metrics.get_metrics_snapshot()
        assert snap["trigger_ratio"] == 0.0


class TestResetMetrics:
    def test_reset_clears_all(self):
        metrics.reset_metrics()
        metrics.record_request(trigger=True, latency_ms=100.0)
        metrics.record_skill_hit("test")
        metrics.record_es_error()
        metrics.reset_metrics()
        snap = metrics.get_metrics_snapshot()
        assert snap["total_requests"] == 0
        assert snap["skill_hit_counts"] == {}
        assert snap["es_errors"] == 0
        assert snap["avg_latency_ms"] == 0.0


class TestThreadSafety:
    def test_concurrent_increments(self):
        metrics.reset_metrics()

        def increment(n):
            for _ in range(n):
                metrics.record_request(trigger=True, latency_ms=1.0)

        threads = [threading.Thread(target=increment, args=(100,)) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        snap = metrics.get_metrics_snapshot()
        assert snap["total_requests"] == 1000
