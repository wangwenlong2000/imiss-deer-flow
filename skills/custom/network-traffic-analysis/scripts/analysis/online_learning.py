"""
Online Learning and Adaptive Models Module

Implemented detectors:
- ADWIN-like mean-shift screening over a variable recent window
- DDM-compatible Bernoulli/error-stream screening
- Online statistics updating with Welford's algorithm
- Active learning via uncertainty sampling

Algorithm references that informed the simplified detectors:
- Bifet, A., & Gavaldà, R. (2007). "Learning from time-changing data with adaptive sliding windows"
- Gama, J., et al. (2004). "Learning with drift detection"
- Welford, B. P. (1962). "Note on a method for calculating corrected sums of squares and products"
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any


@dataclass
class DriftDetectionResult:
    """Drift detection result."""
    drift_detected: bool
    drift_type: str
    drift_severity: str
    drift_point: int
    confidence: float
    recommendation: str


class ADWINLikeDriftDetector:
    """
    ADWIN-like mean-shift detector for concept drift screening.

    This class intentionally does not claim full ADWIN bucket compression.
    It keeps a recent window, tests possible splits with a Hoeffding-style
    epsilon bound, and shrinks the window when a large mean shift is found.

    The algorithm:
    1. Maintain a window W of recent data points
    2. For each new data point, add it to W
    3. Check all possible splits of W into W_0 and W_1
    4. If |μ(W_0) - μ(W_1)| > ε (epsilon-cut bound), shrink W from the left
    5. Return True if drift detected (window was cut)

    The epsilon-cut bound ensures:
        ε = sqrt(0.5 * ln(2/δ) / m)

    where:
    - δ is the confidence parameter
    - m is the harmonic mean of window sizes

    Time complexity: O(W^2) per checked element because split means are
    recomputed over the explicit window.
    Space complexity: O(W) where W is window size

    Reference: Bifet & Gavaldà, SDM 2007
    """

    def __init__(self, delta: float = 0.002):
        """
        Initialize ADWIN detector.

        Args:
            delta: Confidence parameter (smaller = more sensitive, fewer false positives)
        """
        self.delta = delta

        # ADWIN maintains a window of recent observations
        self.window: list[float] = []

        # Track statistics for efficient computation
        self.window_sum = 0.0
        self.window_count = 0

        # Drift tracking
        self._drift_detected = False
        self._last_drift_point = -1
        self._total_detections = 0

    def update(self, value: float) -> bool:
        """
        Process a new data point and detect drift.

        This is the core ADWIN update algorithm:
        1. Add new value to window
        2. Check for drift using epsilon-cut test
        3. If drift detected, shrink window from left

        Args:
            value: New data point

        Returns:
            True if drift detected, False otherwise
        """
        # Add to window
        self.window.append(value)
        self.window_sum += value
        self.window_count += 1

        # Check for drift
        drift_detected = self._check_drift()

        if drift_detected:
            self._drift_detected = True
            self._last_drift_point = self.window_count
            self._total_detections += 1

        return drift_detected

    def _check_drift(self) -> bool:
        """
        Check for concept drift using epsilon-cut bound.

        Tests all possible splits of the current window into two sub-windows
        and checks if the difference in means exceeds the epsilon-cut bound.

        Returns:
            True if drift detected (window was cut)
        """
        n = len(self.window)
        if n < 10:
            return False

        # Track maximum test statistic
        max_epsilon_cut = 0.0
        best_split = -1

        # Test all possible splits
        # Split point i means: W_0 = window[0:i], W_1 = window[i:n]
        for i in range(1, n):
            # Compute statistics for W_0
            n0 = i
            sum0 = sum(self.window[:i])
            mean0 = sum0 / n0

            # Compute statistics for W_1
            n1 = n - i
            sum1 = self.window_sum - sum0
            mean1 = sum1 / n1

            # Compute absolute difference
            abs_diff = abs(mean0 - mean1)

            # Compute epsilon-cut bound
            # ε = sqrt(0.5 * ln(2/δ) / m)
            # where m is the harmonic mean: 1/m = 1/n0 + 1/n1
            m = 1.0 / (1.0 / n0 + 1.0 / n1)
            epsilon = math.sqrt(0.5 * math.log(2.0 / self.delta) / m)

            # Normalized difference
            epsilon_cut = abs_diff / epsilon if epsilon > 0 else 0.0

            if epsilon_cut > max_epsilon_cut:
                max_epsilon_cut = epsilon_cut
                best_split = i

        # Check if drift detected
        if max_epsilon_cut > 1.0 and best_split > 0:
            # Drift detected: shrink window from the left
            # Keep only the more recent window W_1
            removed_sum = sum(self.window[:best_split])
            self.window = self.window[best_split:]
            self.window_sum -= removed_sum
            self.window_count = len(self.window)
            return True

        return False

    @property
    def estimated_mean(self) -> float:
        """Current estimated mean of the window."""
        if not self.window:
            return 0.0
        return self.window_sum / len(self.window)

    @property
    def estimated_variance(self) -> float:
        """Current estimated variance of the window."""
        if len(self.window) < 2:
            return 0.0

        mean = self.estimated_mean
        return sum((x - mean) ** 2 for x in self.window) / (len(self.window) - 1)

    @property
    def width(self) -> int:
        """Current window width."""
        return len(self.window)


class BernoulliDDMDriftDetector:
    """
    DDM-compatible detector for Bernoulli/error indicator streams.

    Use this only for values that represent binary or probability-like error
    indicators in the [0, 1] range. Numeric traffic metrics should use the
    ADWIN-like detector instead.

    The algorithm tracks:
    - p_i: error rate at step i
    - s_i: standard deviation at step i
    - p_min: minimum error rate seen so far
    - s_min: minimum standard deviation seen so far

    Drift is detected when: p_i + s_i > p_min + 3 * s_min
    Warning is detected when: p_i + s_i > p_min + 2 * s_min

    Time complexity: O(1) per element
    Space complexity: O(1)

    Reference: Gama et al., SBIA 2004
    """

    def __init__(self, warning_level: float = 2.0, drift_level: float = 3.0):
        """
        Initialize DDM detector.

        Args:
            warning_level: Warning threshold (in standard deviations)
            drift_level: Drift threshold (in standard deviations)
        """
        self.warning_level = warning_level
        self.drift_level = drift_level

        # Running statistics (Welford's online algorithm)
        self.n = 0
        self.mean = 0.0
        self.m2 = 0.0  # Sum of squares of differences from the mean

        # Minimum statistics
        self.p_min = float('inf')
        self.s_min = float('inf')

        # State tracking
        self._in_warning_zone = False
        self._warning_start = -1
        self._drift_detected = False
        self._last_drift_point = -1

    def update(self, value: float) -> dict[str, Any]:
        """
        Update DDM with new value (typically an error indicator).

        Uses Welford's online algorithm for numerically stable
        computation of running mean and variance.

        Welford's algorithm:
            n += 1
            delta = x - mean
            mean += delta / n
            delta2 = x - mean
            m2 += delta * delta2

        Args:
        value: New value (error indicator or probability in the [0, 1] range)

        Returns:
            Dictionary with update status
        """
        if value < 0.0 or value > 1.0:
            raise ValueError("Bernoulli DDM requires values in the [0, 1] range")

        self.n += 1

        # Welford's online algorithm for running variance
        delta = value - self.mean
        self.mean += delta / self.n
        delta2 = value - self.mean
        self.m2 += delta * delta2

        # Compute current standard deviation
        if self.n >= 2:
            variance = self.m2 / (self.n - 1)
            std = math.sqrt(variance)
        else:
            std = 0.0

        # Current error rate + standard deviation
        p_plus_s = self.mean + std

        # Update minimum statistics
        if p_plus_s < (self.p_min + self.s_min):
            self.p_min = self.mean
            self.s_min = std

        result = {
            "drift_detected": False,
            "warning": False,
            "n": self.n,
            "mean": self.mean,
            "std": std,
            "p_min": self.p_min,
            "s_min": self.s_min
        }

        # Check for drift
        if self.n > 30:  # Need minimum samples
            if p_plus_s > (self.p_min + self.drift_level * self.s_min):
                self._drift_detected = True
                self._last_drift_point = self.n
                result["drift_detected"] = True

                # Reset statistics after drift
                self._reset()

            # Check for warning
            elif p_plus_s > (self.p_min + self.warning_level * self.s_min):
                if not self._in_warning_zone:
                    self._in_warning_zone = True
                    self._warning_start = self.n
                result["warning"] = True

            else:
                self._in_warning_zone = False

        return result

    def _reset(self) -> None:
        """Reset statistics after drift detection."""
        self.n = 0
        self.mean = 0.0
        self.m2 = 0.0
        self.p_min = float('inf')
        self.s_min = float('inf')
        self._in_warning_zone = False
        self._warning_start = -1


class OnlineStatistics:
    """
    Online statistics computation using Welford's algorithm.

    Maintains running mean, variance, and higher-order statistics
    without storing all data points.
    """

    def __init__(self):
        """Initialize online statistics."""
        self.n = 0
        self.mean = 0.0
        self.m2 = 0.0
        self.min_val = float('inf')
        self.max_val = float('-inf')

    def update(self, value: float) -> None:
        """
        Update statistics with new value using Welford's algorithm.

        Args:
            value: New data point
        """
        self.n += 1

        delta = value - self.mean
        self.mean += delta / self.n
        delta2 = value - self.mean
        self.m2 += delta * delta2

        self.min_val = min(self.min_val, value)
        self.max_val = max(self.max_val, value)

    @property
    def variance(self) -> float:
        """Sample variance."""
        if self.n < 2:
            return 0.0
        return self.m2 / (self.n - 1)

    @property
    def std(self) -> float:
        """Standard deviation."""
        return math.sqrt(self.variance)

    def merge(self, other: 'OnlineStatistics') -> 'OnlineStatistics':
        """
        Merge with another OnlineStatistics instance.

        Uses parallel algorithm for combining running statistics.

        Args:
            other: Another OnlineStatistics instance

        Returns:
            Merged OnlineStatistics
        """
        merged = OnlineStatistics()
        merged.n = self.n + other.n
        if merged.n == 0:
            return merged
        if self.n == 0:
            return other
        if other.n == 0:
            return self

        delta = other.mean - self.mean
        merged.mean = (self.n * self.mean + other.n * other.mean) / merged.n

        merged.m2 = (
            self.m2 + other.m2 +
            delta ** 2 * self.n * other.n / merged.n
        )

        merged.min_val = min(self.min_val, other.min_val)
        merged.max_val = max(self.max_val, other.max_val)

        return merged


class OnlineLearner:
    """
    Online learning manager combining multiple drift detection algorithms.

    Provides:
    - ADWIN-like mean-shift drift screening for numeric streams
    - DDM-compatible screening for Bernoulli/error streams
    - Online statistics updating
    - Active learning sample selection
    - Feedback loop management
    """

    def __init__(self, delta: float = 0.002):
        """
        Initialize online learner.

        Args:
            delta: ADWIN-like confidence parameter
        """
        self.delta = delta
        self.adwin = ADWINLikeDriftDetector(delta=delta)
        self.ddm = BernoulliDDMDriftDetector()
        self.stats = OnlineStatistics()

    def detect_drift_adwin(self, stream: list[float]) -> DriftDetectionResult:
        """
        Detect drift using the ADWIN-like mean-shift detector on a historical stream.

        Args:
            stream: Historical data stream

        Returns:
            DriftDetectionResult
        """
        if len(stream) < 10:
            return DriftDetectionResult(
                drift_detected=False,
                drift_type="none",
                drift_severity="info",
                drift_point=-1,
                confidence=0.0,
                recommendation="Insufficient data for drift detection"
            )

        # Reset detector state for reproducible historical evaluation.
        self.adwin = ADWINLikeDriftDetector(delta=self.delta)

        drift_detected = False
        drift_point = -1
        drift_count = 0

        for i, value in enumerate(stream):
            if self.adwin.update(value):
                drift_detected = True
                drift_point = i
                drift_count += 1

        if drift_detected:
            severity = "high" if drift_count > 2 else "medium"
            return DriftDetectionResult(
                drift_detected=True,
                drift_type="sudden" if drift_count <= 2 else "gradual",
                drift_severity=severity,
                drift_point=drift_point,
                confidence=min(1.0, drift_count * 0.3),
                recommendation=f"ADWIN-like detector found {drift_count} drift(s). Last at point {drift_point}. Consider validating the shifted window."
            )

        return DriftDetectionResult(
            drift_detected=False,
            drift_type="none",
            drift_severity="info",
            drift_point=-1,
            confidence=0.0,
            recommendation="No drift detected by ADWIN-like mean-shift screening"
        )

    def detect_drift_ddm(self, stream: list[float]) -> DriftDetectionResult:
        """
        Detect drift using DDM-compatible logic on a Bernoulli/error stream.

        Args:
            stream: Historical data stream

        Returns:
            DriftDetectionResult
        """
        if len(stream) < 30:
            return DriftDetectionResult(
                drift_detected=False,
                drift_type="none",
                drift_severity="info",
                drift_point=-1,
                confidence=0.0,
                recommendation="Insufficient data for Bernoulli DDM screening (need at least 30 points)"
            )

        # Reset detector state for reproducible historical evaluation.
        self.ddm = BernoulliDDMDriftDetector()

        drift_detected = False
        drift_point = -1
        warning_detected = False

        for i, value in enumerate(stream):
            result = self.ddm.update(value)

            if result["drift_detected"]:
                drift_detected = True
                drift_point = i
                break

            if result["warning"]:
                warning_detected = True

        if drift_detected:
            return DriftDetectionResult(
                drift_detected=True,
                drift_type="incremental",
                drift_severity="high",
                drift_point=drift_point,
                confidence=0.85,
                recommendation=f"Bernoulli DDM-compatible drift detected at point {drift_point}. Model retraining may be needed."
            )

        if warning_detected:
            return DriftDetectionResult(
                drift_detected=False,
                drift_type="warning",
                drift_severity="low",
                drift_point=-1,
                confidence=0.3,
                recommendation="Bernoulli DDM-compatible warning zone entered. Monitor closely for potential drift."
            )

        return DriftDetectionResult(
            drift_detected=False,
            drift_type="none",
            drift_severity="info",
            drift_point=-1,
            confidence=0.0,
            recommendation="No drift or warning detected by Bernoulli DDM-compatible screening"
        )

    def request_labels_uncertain_samples(self, predictions: list[dict[str, Any]],
                                        threshold: float = 0.5) -> list[dict[str, Any]]:
        """
        Identify uncertain samples for active learning.

        Uses uncertainty sampling: select samples with lowest prediction confidence.

        Args:
            predictions: List of prediction results with confidence scores
            threshold: Confidence threshold for uncertainty

        Returns:
            List of samples needing labels, sorted by priority
        """
        uncertain_samples = []

        for pred in predictions:
            confidence = pred.get("confidence", 1.0)

            if confidence < threshold:
                uncertain_samples.append({
                    "sample_id": pred.get("sample_id", "unknown"),
                    "prediction": pred.get("prediction", "unknown"),
                    "confidence": confidence,
                    "features": pred.get("features", {}),
                    "priority": 1.0 - confidence,
                    "reason": "Low prediction confidence"
                })

        # Sort by priority (most uncertain first)
        uncertain_samples.sort(key=lambda x: x["priority"], reverse=True)

        return uncertain_samples[:20]

    def manage_feedback_loop(self, feedback_data: list[dict[str, Any]]) -> dict[str, Any]:
        """
        Manage feedback loop for continuous learning.

        Args:
            feedback_data: User feedback on predictions

        Returns:
            Feedback analysis
        """
        if not feedback_data:
            return {
                "feedback_count": 0,
                "accuracy": 0.0,
                "error_patterns": {},
                "recommendations": ["Collect user feedback to improve model"]
            }

        # Analyze feedback
        correct = 0
        total = len(feedback_data)
        error_patterns: dict[str, int] = {}

        for feedback in feedback_data:
            predicted = feedback.get("predicted_label", "")
            actual = feedback.get("actual_label", "")

            if predicted == actual:
                correct += 1
            else:
                # Track error patterns
                error_patterns[predicted] = error_patterns.get(predicted, 0) + 1

        accuracy = correct / total if total > 0 else 0.0

        recommendations = [
            f"Feedback accuracy: {accuracy:.1%}",
            f"Total feedback samples: {total}"
        ]

        if accuracy < 0.7:
            recommendations.append("Model accuracy below threshold - consider retraining")

        # Identify common error patterns
        for pattern, count in error_patterns.items():
            if count >= 3:
                recommendations.append(
                    f"Common error: '{pattern}' misclassified {count} times"
                )

        return {
            "feedback_count": total,
            "correct_predictions": correct,
            "incorrect_predictions": total - correct,
            "accuracy": accuracy,
            "error_patterns": error_patterns,
            "recommendations": recommendations
        }


# Backward-compatible aliases for older imports.
ADWINDriftDetector = ADWINLikeDriftDetector
DDMDriftDetector = BernoulliDDMDriftDetector
