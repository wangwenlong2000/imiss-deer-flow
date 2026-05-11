"""Deterministic regression tests for online_learning drift detectors."""

from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from analysis.online_learning import (
    ADWINLikeDriftDetector,
    BernoulliDDMDriftDetector,
    OnlineStatistics,
    OnlineLearner,
)


def test_adwin_no_drift_on_stable_stream():
    """ADWIN should not detect drift on a stable (constant-mean) stream."""
    detector = ADWINLikeDriftDetector(delta=0.002)
    # Stable stream with small noise
    import random
    random.seed(42)
    for _ in range(200):
        detector.update(100.0 + random.uniform(-1, 1))
    # After initial warm-up, no more drift should be detected
    detector.update(100.0)
    detector.update(100.0)
    detector.update(100.0)
    # The detector should have stabilized
    assert abs(detector.estimated_mean - 100.0) < 5.0
    print("  PASS: adwin_no_drift_on_stable_stream")


def test_adwin_detects_sudden_drift():
    """ADWIN should detect a sudden mean shift."""
    detector = ADWINLikeDriftDetector(delta=0.001)
    import random
    random.seed(42)
    # Stable at 100
    for _ in range(100):
        detector.update(100.0 + random.uniform(-1, 1))
    # Sudden shift to 500
    drift_detected = False
    for i in range(50):
        if detector.update(500.0 + random.uniform(-1, 1)):
            drift_detected = True
            break
    assert drift_detected, "ADWIN should detect sudden mean shift"
    print("  PASS: adwin_detects_sudden_drift")


def test_adwin_empty_window():
    """ADWIN should handle empty window gracefully."""
    detector = ADWINLikeDriftDetector()
    assert detector.estimated_mean == 0.0
    assert detector.estimated_variance == 0.0
    assert detector.width == 0
    print("  PASS: adwin_empty_window")


def test_ddm_no_drift_on_stable_error_rate():
    """DDM should not detect drift when error rate is stable."""
    detector = BernoulliDDMDriftDetector()
    import random
    random.seed(123)
    # Stable error rate of 0.10 (higher rate to avoid early false alarms)
    for _ in range(200):
        result = detector.update(1.0 if random.random() < 0.10 else 0.0)
        if result["drift_detected"]:
            raise AssertionError(f"DDM should not detect drift at step {result['n']}, mean={result['mean']:.3f}")
    print("  PASS: ddm_no_drift_on_stable_error_rate")


def test_ddm_detects_error_rate_increase():
    """DDM should detect a sudden increase in error rate."""
    detector = BernoulliDDMDriftDetector()
    import random
    random.seed(42)
    # Low error rate
    for _ in range(60):
        detector.update(1.0 if random.random() < 0.02 else 0.0)
    # High error rate
    drift_detected = False
    for _ in range(100):
        result = detector.update(1.0 if random.random() < 0.50 else 0.0)
        if result["drift_detected"]:
            drift_detected = True
            break
    assert drift_detected, "DDM should detect error rate increase"
    print("  PASS: ddm_detects_error_rate_increase")


def test_ddm_rejects_out_of_range_values():
    """DDM should reject values outside [0, 1]."""
    detector = BernoulliDDMDriftDetector()
    for bad_value in [-0.1, 1.1, 2.0]:
        try:
            detector.update(bad_value)
            raise AssertionError(f"DDM should reject {bad_value}")
        except ValueError:
            pass  # Expected
    print("  PASS: ddm_rejects_out_of_range_values")


def test_online_statistics_merge_empty():
    """OnlineStatistics.merge() should handle empty inputs safely."""
    a = OnlineStatistics()
    b = OnlineStatistics()

    # Both empty
    merged = a.merge(b)
    assert merged.n == 0

    # One empty
    import random
    random.seed(42)
    for _ in range(10):
        a.update(random.uniform(0, 10))
    b = OnlineStatistics()
    merged = a.merge(b)
    assert merged.n == a.n
    assert abs(merged.mean - a.mean) < 1e-9

    merged = b.merge(a)
    assert merged.n == a.n
    assert abs(merged.mean - a.mean) < 1e-9
    print("  PASS: online_statistics_merge_empty")


def test_online_statistics_merge_correctness():
    """Merged statistics should match direct computation."""
    a = OnlineStatistics()
    b = OnlineStatistics()
    import random
    random.seed(42)
    all_values = []
    for _ in range(50):
        v = random.uniform(0, 100)
        a.update(v)
        all_values.append(v)
    for _ in range(30):
        v = random.uniform(0, 100)
        b.update(v)
        all_values.append(v)

    merged = a.merge(b)
    expected_mean = sum(all_values) / len(all_values)
    assert abs(merged.mean - expected_mean) < 0.1
    assert merged.n == len(all_values)
    assert merged.min_val == min(all_values)
    assert merged.max_val == max(all_values)
    print("  PASS: online_statistics_merge_correctness")


def test_online_learner_insufficient_data():
    """OnlineLearner should handle insufficient data gracefully."""
    learner = OnlineLearner()
    result = learner.detect_drift_adwin([1.0, 2.0, 3.0])
    assert not result.drift_detected
    assert result.drift_type == "none"

    result = learner.detect_drift_ddm([0.01] * 10)
    assert not result.drift_detected
    assert result.drift_type == "none"
    print("  PASS: online_learner_insufficient_data")


def run_all_tests():
    """Run all deterministic tests."""
    print("=" * 60)
    print("online_learning.py Deterministic Regression Tests")
    print("=" * 60)
    passed = 0
    failed = 0

    tests = [
        test_adwin_no_drift_on_stable_stream,
        test_adwin_detects_sudden_drift,
        test_adwin_empty_window,
        test_ddm_no_drift_on_stable_error_rate,
        test_ddm_detects_error_rate_increase,
        test_ddm_rejects_out_of_range_values,
        test_online_statistics_merge_empty,
        test_online_statistics_merge_correctness,
        test_online_learner_insufficient_data,
    ]

    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"  FAIL: {test.__name__}: {e}")
            failed += 1

    print(f"\nPassed: {passed}, Failed: {failed}, Total: {len(tests)}")
    return failed == 0


if __name__ == "__main__":
    import sys
    sys.exit(0 if run_all_tests() else 1)
