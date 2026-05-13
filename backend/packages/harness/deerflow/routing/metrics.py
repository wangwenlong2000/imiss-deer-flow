"""SkillRouter routing metrics collection.

Thread-safe counters tracking routing behavior across requests.
Accessible via GET /api/skill-router/metrics.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field


@dataclass
class RoutingMetrics:
    total_requests: int = 0
    trigger_true_count: int = 0
    trigger_false_count: int = 0
    skill_hit_counts: dict = field(default_factory=dict)
    total_latency_ms: float = 0.0
    latency_samples: int = 0
    es_errors: int = 0
    embedding_errors: int = 0
    reranker_errors: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)


_metrics = RoutingMetrics()


def record_request(trigger: bool, latency_ms: float) -> None:
    """Record a routing request outcome."""
    with _metrics._lock:
        _metrics.total_requests += 1
        if trigger:
            _metrics.trigger_true_count += 1
        else:
            _metrics.trigger_false_count += 1
        _metrics.total_latency_ms += latency_ms
        _metrics.latency_samples += 1


def record_skill_hit(skill_id: str) -> None:
    """Increment hit counter for a skill."""
    with _metrics._lock:
        _metrics.skill_hit_counts[skill_id] = _metrics.skill_hit_counts.get(skill_id, 0) + 1


def record_es_error() -> None:
    """Record an Elasticsearch error."""
    with _metrics._lock:
        _metrics.es_errors += 1


def record_embedding_error() -> None:
    """Record an embedding API error."""
    with _metrics._lock:
        _metrics.embedding_errors += 1


def record_reranker_error() -> None:
    """Record a reranker API error."""
    with _metrics._lock:
        _metrics.reranker_errors += 1


def get_metrics_snapshot() -> dict:
    """Return a serializable dict of current metrics."""
    with _metrics._lock:
        avg_latency = 0.0
        if _metrics.latency_samples > 0:
            avg_latency = round(_metrics.total_latency_ms / _metrics.latency_samples, 2)
        return {
            "total_requests": _metrics.total_requests,
            "trigger_true_count": _metrics.trigger_true_count,
            "trigger_false_count": _metrics.trigger_false_count,
            "trigger_ratio": round(
                _metrics.trigger_true_count / _metrics.total_requests, 4
            ) if _metrics.total_requests > 0 else 0.0,
            "skill_hit_counts": dict(_metrics.skill_hit_counts),
            "avg_latency_ms": avg_latency,
            "es_errors": _metrics.es_errors,
            "embedding_errors": _metrics.embedding_errors,
            "reranker_errors": _metrics.reranker_errors,
        }


def reset_metrics() -> None:
    """Reset all counters to zero."""
    with _metrics._lock:
        _metrics.total_requests = 0
        _metrics.trigger_true_count = 0
        _metrics.trigger_false_count = 0
        _metrics.skill_hit_counts.clear()
        _metrics.total_latency_ms = 0.0
        _metrics.latency_samples = 0
        _metrics.es_errors = 0
        _metrics.embedding_errors = 0
        _metrics.reranker_errors = 0
