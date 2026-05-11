from __future__ import annotations

import math
import statistics
from collections import defaultdict
from contextlib import suppress
from typing import Any


def _safe_float_local(value: Any) -> float:
    try:
        if value in (None, ""):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _safe_text(value: Any) -> str:
    return "UNKNOWN" if value in (None, "") else str(value)


def _safe_ratio_local(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)


def _private_ip_predicate(field_name: str) -> str:
    cidrs = [
        "10.0.0.0/8",
        "172.16.0.0/12",
        "192.168.0.0/16",
        "100.64.0.0/10",
        "fc00::/7",
    ]
    return "(" + " OR ".join(f"ip_in_cidr(CAST({field_name} AS VARCHAR), '{cidr}')" for cidr in cidrs) + ")"


def _text_entropy_local(value: str) -> float:
    text = str(value or "").strip().lower()
    if not text:
        return 0.0
    counts = defaultdict(int)
    for char in text:
        counts[char] += 1
    return _shannon_entropy([float(item) for item in counts.values()])


def _coerce_event_seconds(value: Any) -> float | None:
    if value in (None, ""):
        return None
    with suppress(TypeError, ValueError):
        return float(value)
    return None


def _mean_std(values: list[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    if len(values) == 1:
        return values[0], 0.0
    return statistics.fmean(values), statistics.pstdev(values)


def _zscore(value: float, mean: float, std: float) -> float:
    if std <= 1e-9:
        return 0.0
    return (value - mean) / std


def _shannon_entropy(counts: list[float]) -> float:
    total = sum(count for count in counts if count > 0)
    if total <= 0:
        return 0.0
    entropy = 0.0
    for count in counts:
        if count <= 0:
            continue
        p = count / total
        entropy -= p * math.log2(p)
    return entropy


def _lag_autocorrelation(values: list[float], lag: int) -> float:
    if lag <= 0 or len(values) <= lag:
        return 0.0
    lead = values[:-lag]
    lagged = values[lag:]
    if len(lead) < 2:
        return 0.0
    mean_lead = statistics.fmean(lead)
    mean_lagged = statistics.fmean(lagged)
    num = sum((a - mean_lead) * (b - mean_lagged) for a, b in zip(lead, lagged))
    den_left = sum((a - mean_lead) ** 2 for a in lead)
    den_right = sum((b - mean_lagged) ** 2 for b in lagged)
    denom = math.sqrt(den_left * den_right)
    if denom <= 1e-9:
        return 0.0
    return num / denom


def _dominant_periodicity(values: list[float], max_lag: int = 12) -> tuple[int | None, float]:
    best_lag: int | None = None
    best_score = -1.0
    upper = min(max_lag, max(1, len(values) // 2))
    for lag in range(1, upper + 1):
        score = abs(_lag_autocorrelation(values, lag))
        if score > best_score:
            best_score = score
            best_lag = lag
    return best_lag, max(best_score, 0.0)
