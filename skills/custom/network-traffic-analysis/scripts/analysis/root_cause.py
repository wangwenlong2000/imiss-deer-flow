"""Heuristic root-cause attribution for network traffic anomalies.

This module ranks feature contributions against a deterministic heuristic
anomaly scorer. The output is an investigation aid for prioritizing fields to
inspect next, not a causal proof.
"""

from __future__ import annotations

import math
import itertools
from dataclasses import dataclass
from typing import Any


@dataclass
class FeatureAttributionResult:
    """Feature contribution computation result."""
    feature_names: list[str]
    feature_contributions: dict[str, float]
    base_value: float
    model_output: float
    explanation: str


@dataclass
class RootCauseResult:
    """Root cause analysis result."""
    entity_id: str
    anomaly_score: float
    attribution_result: FeatureAttributionResult | None
    root_causes: list[dict[str, Any]]
    contributing_factors: list[dict[str, Any]]
    explanation: str
    confidence: float
    recommended_actions: list[str]
    # Explicit field inventory
    fields_used: list[str]
    fields_missing: list[str]
    # Layered cause classification
    direct_triggers: list[dict[str, Any]]
    supporting_factors: list[dict[str, Any]]
    fallback_items: list[dict[str, Any]]
    causal_notes: list[str]
    # New: converged output (1 primary + 2 supporting)
    primary_cause: dict[str, Any] | None
    supporting_causes: list[dict[str, Any]]
    uncertain_notes: list[str]
    # New: conclusion grade
    conclusion_grade: str  # confirmed / likely / possible / insufficient_evidence


class HeuristicFeatureAttributor:
    """Estimate feature contributions to the local heuristic anomaly score."""

    def __init__(self, background_dataset: list[dict[str, Any]]):
        """
        Initialize HeuristicFeatureAttributor with background dataset.

        Args:
            background_dataset: List of feature dictionaries representing
                              normal/baseline behavior
        """
        self.background_dataset = background_dataset
        self.background_size = len(background_dataset)

        # Compute base value (expected model output)
        if background_dataset:
            self.base_value = self._compute_mean_anomaly_score(background_dataset)
        else:
            self.base_value = 0.0

    def _compute_mean_anomaly_score(self, samples: list[dict[str, Any]]) -> float:
        """
        Compute mean anomaly score for a set of samples.

        This serves as the model function f(x) for heuristic attribution computation.
        Uses a heuristic anomaly scoring based on feature deviations.

        Args:
            samples: List of feature dictionaries

        Returns:
            Mean anomaly score
        """
        if not samples:
            return 0.0

        scores = []
        for sample in samples:
            score = self._compute_anomaly_score(sample)
            scores.append(score)

        return sum(scores) / len(scores)

    def _compute_anomaly_score(self, features: dict[str, Any]) -> float:
        """
        Compute anomaly score for a single sample.

        This is the model function f(x) used in heuristic attribution.
        It computes a normalized anomaly score based on feature deviations.

        Args:
            features: Dictionary of feature values

        Returns:
            Anomaly score in [0, 1]
        """
        score = 0.0
        weight_sum = 0.0

        # Feature weights based on importance in anomaly detection
        feature_weights = {
            'bytes': 0.15,
            'packets': 0.10,
            'duration_ms': 0.08,
            'dst_port': 0.12,
            'byte_asymmetry': 0.18,
            'packet_asymmetry': 0.15,
            'dns_query_entropy': 0.12,
            'ttl_avg': 0.05,
            'src_to_dst_byte_ratio': 0.05,
        }

        # Expected normal ranges (from RFC/empirical data)
        normal_ranges = {
            'bytes': (100, 100000),
            'packets': (1, 1000),
            'duration_ms': (10, 30000),
            'dst_port': (80, 443),
            'byte_asymmetry': (0.0, 0.3),
            'packet_asymmetry': (0.0, 0.3),
            'dns_query_entropy': (2.0, 4.0),
            'ttl_avg': (32, 128),
            'src_to_dst_byte_ratio': (0.3, 3.0),
        }

        for feature, weight in feature_weights.items():
            if feature not in features:
                continue

            value = features[feature]
            if not isinstance(value, (int, float)):
                continue

            if feature in normal_ranges:
                low, high = normal_ranges[feature]
                mid = (low + high) / 2
                half_range = (high - low) / 2

                if half_range > 0:
                    # Normalized deviation from normal range
                    if value < low:
                        deviation = (low - value) / half_range
                    elif value > high:
                        deviation = (value - high) / half_range
                    else:
                        deviation = 0.0

                    # Sigmoid scaling
                    scaled_deviation = 1.0 / (1.0 + math.exp(-deviation * 2))
                    score += weight * scaled_deviation
                    weight_sum += weight

        if weight_sum > 0:
            score /= weight_sum

        return min(1.0, score)

    def _coalition_weight(self, coalition_size: int, num_features: int) -> float:
        """Compute a deterministic heuristic weight for a feature coalition."""
        M = num_features
        m = coalition_size

        # Handle edge cases
        if m == 0 or m == M:
            return 1e10  # Very large weight for edge coalitions

        # Binomial coefficient C(M, m)
        try:
            binomial_coef = math.comb(M, m)
        except (ValueError, OverflowError):
            return 1.0

        weight = (M - 1) / (binomial_coef * m * (M - m))

        return max(weight, 1e-6)  # Avoid division by zero

    def _create_coalition_sample(self, features: list[str], coalition_size: int,
                                 sample_count: int = 10) -> list[list[str]]:
        """
        Create sampled coalitions for efficient heuristic attribution computation.

        For large feature sets, exact computation requires 2^M coalitions.
        We use Monte Carlo sampling to approximate feature contribution scores.

        Args:
            features: List of all feature names
            coalition_size: Target coalition size
            sample_count: Number of samples to generate

        Returns:
            List of coalitions (each is a list of feature names)
        """
        M = len(features)

        if M <= 15:
            # Small feature set: enumerate all coalitions of this size
            if coalition_size <= M:
                return [list(c) for c in itertools.combinations(features, coalition_size)]

        # Large feature set: Monte Carlo sampling
        import random
        coalitions = []
        for _ in range(sample_count):
            # Random sample of coalition_size features
            coalition = random.sample(features, min(coalition_size, M))
            coalitions.append(coalition)

        return coalitions

    def _evaluate_model_with_coalition(self, coalition: list[str],
                                       sample: dict[str, Any]) -> float:
        """
        Evaluate model output when only features in coalition are present.

        Features not in coalition are replaced with background values.
        This implements h_x(z') where z' is the binary coalition vector.

        Args:
            coalition: List of features present in coalition
            sample: Original sample to evaluate

        Returns:
            Model output with coalition
        """
        # Create modified sample with coalition features
        modified_sample = {}

        for feature_name, value in sample.items():
            if feature_name in coalition:
                # Feature is present: use original value
                modified_sample[feature_name] = value
            else:
                # Feature is absent: use background mean
                if self.background_dataset:
                    background_values = [
                        bg.get(feature_name, 0)
                        for bg in self.background_dataset
                        if isinstance(bg.get(feature_name), (int, float))
                    ]
                    if background_values:
                        modified_sample[feature_name] = sum(background_values) / len(background_values)
                    else:
                        modified_sample[feature_name] = 0
                else:
                    modified_sample[feature_name] = 0

        return self._compute_anomaly_score(modified_sample)

    def compute_feature_contributions(self, sample: dict[str, Any],
                           feature_names: list[str] | None = None,
                           max_coalitions: int = 1000) -> FeatureAttributionResult:
        """
        Compute feature contribution scores for a sample using HeuristicFeatureAttributor algorithm.

        This implements a deterministic heuristic attribution algorithm:
        1. Generate coalitions (feature subsets)
        2. Evaluate model for each coalition
        3. Accumulate weighted marginal score changes
        4. Return feature contribution scores

        Args:
            sample: Sample to explain (feature dictionary)
            feature_names: Features to include in explanation
            max_coalitions: Maximum number of coalitions to evaluate

        Returns:
            FeatureAttributionResult with feature contribution scores and explanation
        """
        if feature_names is None:
            feature_names = [
                k for k, v in sample.items()
                if isinstance(v, (int, float))
            ]

        M = len(feature_names)

        if M == 0:
            return FeatureAttributionResult(
                feature_names=[],
                feature_contributions={},
                base_value=self.base_value,
                model_output=self._compute_anomaly_score(sample),
                explanation="No numeric features available for heuristic attribution analysis"
            )

        # Compute model output for original sample
        model_output = self._compute_anomaly_score(sample)

        if M <= 12:
            # Exact heuristic attribution: enumerate all 2^M coalitions
            feature_contributions = self._exact_feature_contributions(sample, feature_names)
        else:
            # Approximate heuristic attribution: Monte Carlo sampling
            feature_contributions = self._approximate_feature_contributions(
                sample, feature_names, max_coalitions
            )

        # Verify additivity property: sum(feature contribution scores) + base_value ≈ model_output
        contribution_sum = sum(feature_contributions.values())
        reconstructed = self.base_value + contribution_sum
        reconstruction_error = abs(reconstructed - model_output)

        explanation = self._generate_contribution_explanation(
            feature_names, feature_contributions, model_output, reconstruction_error
        )

        return FeatureAttributionResult(
            feature_names=feature_names,
            feature_contributions=feature_contributions,
            base_value=self.base_value,
            model_output=model_output,
            explanation=explanation
        )

    def _exact_feature_contributions(self, sample: dict[str, Any],
                          feature_names: list[str]) -> dict[str, float]:
        """
        Compute exact feature contribution scores via enumeration (for small M).

        Time complexity: O(2^M * N) where M is features, N is background size

        Args:
            sample: Sample to explain
            feature_names: Features to include

        Returns:
            Dictionary mapping feature names to feature contribution scores
        """
        M = len(feature_names)
        feature_contributions = {f: 0.0 for f in feature_names}

        # Enumerate all coalitions (subsets)
        for coalition_size in range(M + 1):
            # Generate all coalitions of this size
            coalitions = list(itertools.combinations(feature_names, coalition_size))

            for coalition in coalitions:
                coalition_set = set(coalition)
                coalition_weight = self._coalition_weight(
                    len(coalition_set), M
                )

                # For each feature not in coalition, compute marginal contribution
                for feature_i in feature_names:
                    if feature_i in coalition_set:
                        continue

                    # f(S ∪ {i}) - f(S)
                    coalition_with_i = coalition_set | {feature_i}

                    f_S_union_i = self._evaluate_model_with_coalition(
                        list(coalition_with_i), sample
                    )
                    f_S = self._evaluate_model_with_coalition(
                        list(coalition_set), sample
                    )

                    marginal_contribution = f_S_union_i - f_S

                    # Weighted accumulation
                    feature_contributions[feature_i] += (
                        coalition_weight * marginal_contribution
                    )

        # Normalize by number of coalitions
        total_weight = sum(
            self._coalition_weight(s, M)
            for s in range(M + 1)
        )

        if total_weight > 0:
            for feature in feature_contributions:
                feature_contributions[feature] /= (total_weight / M)

        return feature_contributions

    def _approximate_feature_contributions(self, sample: dict[str, Any],
                                feature_names: list[str],
                                max_coalitions: int) -> dict[str, float]:
        """
        Approximate feature contribution scores via Monte Carlo sampling (for large M).

        Uses permutation sampling to estimate feature values.

        Algorithm:
        1. Generate random permutations of features
        2. For each permutation, compute marginal contributions
        3. Average marginal contributions across permutations

        Time complexity: O(K * M * N) where K is samples, M is features

        Args:
            sample: Sample to explain
            feature_names: Features to include
            max_coalitions: Maximum number of permutation samples

        Returns:
            Dictionary mapping feature names to feature contribution scores
        """
        import random

        M = len(feature_names)
        feature_contributions = {f: 0.0 for f in feature_names}
        contribution_counts = {f: 0 for f in feature_names}

        num_permutations = min(max_coalitions // M, 500)

        for _ in range(num_permutations):
            # Random permutation of features
            permutation = list(feature_names)
            random.shuffle(permutation)

            # Build coalition incrementally
            coalition = set()
            f_prev = self._evaluate_model_with_coalition([], sample)

            for i, feature_i in enumerate(permutation):
                coalition.add(feature_i)

                # f(S ∪ {i})
                f_new = self._evaluate_model_with_coalition(
                    list(coalition), sample
                )

                # Marginal contribution
                marginal = f_new - f_prev
                feature_contributions[feature_i] += marginal
                contribution_counts[feature_i] += 1

                f_prev = f_new

        # Average contributions
        for feature in feature_names:
            if contribution_counts[feature] > 0:
                feature_contributions[feature] /= contribution_counts[feature]

        return feature_contributions

    def _generate_contribution_explanation(self, feature_names: list[str],
                                           feature_contributions: dict[str, float],
                                           model_output: float,
                                           reconstruction_error: float) -> str:
        """
        Generate human-readable explanation from feature contribution scores.

        Args:
            feature_names: Feature names
            feature_contributions: feature contribution scores
            model_output: Model output for sample
            reconstruction_error: heuristic attribution additivity error

        Returns:
            Explanation string
        """
        # Sort features by absolute feature contribution score
        sorted_features = sorted(
            feature_contributions.items(),
            key=lambda x: abs(x[1]),
            reverse=True
        )

        if not sorted_features:
            return "No significant feature contributions detected"

        # Build explanation
        parts = []
        parts.append(f"Anomaly score: {model_output:.3f} (base: {self.base_value:.3f})")

        # Top contributors
        top_positive = [(f, v) for f, v in sorted_features if v > 0.01][:3]
        top_negative = [(f, v) for f, v in sorted_features if v < -0.01][:3]

        if top_positive:
            pos_strs = [f"{f} (+{v:.3f})" for f, v in top_positive]
            parts.append(f"Increasing anomaly: {', '.join(pos_strs)}")

        if top_negative:
            neg_strs = [f"{f} ({v:.3f})" for f, v in top_negative]
            parts.append(f"Decreasing anomaly: {', '.join(neg_strs)}")

        # Reconstruction error
        if reconstruction_error > 0.01:
            parts.append(f"heuristic attribution residual: {reconstruction_error:.3f}")
        else:
            parts.append("heuristic attribution residual within tolerance")

        return " | ".join(parts)


class RootCauseAnalyzer:
    """
    Analyzes root causes of network anomalies using feature contribution scores.

    Uses HeuristicFeatureAttributor to rank feature contributions to anomaly
    scores. Results are heuristic and should be validated with the underlying
    flows or packet evidence.
    """

    def __init__(self, background_dataset: list[dict[str, Any]] | None = None):
        """
        Initialize root cause analyzer.

        Args:
            background_dataset: Normal behavior samples for heuristic attribution baseline
        """
        self.background_dataset = background_dataset or []
        self.attributor = HeuristicFeatureAttributor(self.background_dataset)

    def compute_feature_contributions(self, anomaly_record: dict[str, Any],
                                     feature_names: list[str] | None = None) -> FeatureAttributionResult:
        """
        Compute heuristic attribution-based feature contributions.

        Args:
            anomaly_record: Anomaly record to explain
            feature_names: Features to include (None = all numeric)

        Returns:
            FeatureAttributionResult with heuristic feature contribution scores
        """
        return self.attributor.compute_feature_contributions(
            anomaly_record, feature_names
        )

    def analyze_root_cause(self, anomaly_record: dict[str, Any],
                          context: dict[str, Any] | None = None) -> RootCauseResult:
        """
        Comprehensive root cause analysis with heuristic attribution.

        Args:
            anomaly_record: Anomaly record to analyze
            context: Investigation context (behavior findings, anomaly tags, etc.)

        Returns:
            RootCauseResult with layered cause classification
        """
        entity_id = anomaly_record.get("src_ip", anomaly_record.get("dst_ip", "unknown"))
        anomaly_score = anomaly_record.get("anomaly_score", 0.0)
        context = context or {}

        # Compute feature contribution scores
        attribution_result = self.attributor.compute_feature_contributions(anomaly_record)

        # Track which fields were available vs missing
        expected_fields = [
            'bytes', 'packets', 'duration_ms', 'dst_port',
            'byte_asymmetry', 'packet_asymmetry', 'dns_query_entropy',
            'ttl_avg', 'src_to_dst_byte_ratio',
        ]
        fields_used = [f for f in expected_fields if f in attribution_result.feature_contributions]
        fields_missing = [f for f in expected_fields if f not in attribution_result.feature_names]

        # Build contributing factors
        contributing_factors = []
        for feature, contribution in attribution_result.feature_contributions.items():
            current_value = anomaly_record.get(feature, 0)
            if abs(contribution) > 0.001:
                direction = "increased" if contribution > 0 else "decreased"
                contributing_factors.append({
                    "feature": feature,
                    "current_value": current_value,
                    "contribution_score": round(contribution, 4),
                    "contribution_direction": direction,
                    "absolute_contribution": abs(contribution)
                })
        contributing_factors.sort(key=lambda x: x["absolute_contribution"], reverse=True)

        # Layer 1: Direct triggers (high contribution + strong anomaly signal)
        # These are fields whose deviation alone explains a large portion of the anomaly
        direct_triggers = []
        for factor in contributing_factors:
            score = factor["absolute_contribution"]
            if score > 0.15:
                field = factor["feature"]
                value = factor["current_value"]
                normal_range = self._get_normal_range(field)
                is_outside_normal = self._is_outside_range(field, value)
                direct_triggers.append({
                    "feature": field,
                    "current_value": value,
                    "contribution_score": factor["contribution_score"],
                    "outside_normal_range": is_outside_normal,
                    "normal_range": normal_range,
                    "description": self._build_trigger_description(field, value, is_outside_normal),
                    "relationship": "direct" if is_outside_normal else "correlated",
                })

        # Layer 2: Supporting factors (moderate contribution, contextual)
        supporting_factors = []
        for factor in contributing_factors:
            score = factor["absolute_contribution"]
            if 0.05 < score <= 0.15:
                field = factor["feature"]
                value = factor["current_value"]
                supporting_factors.append({
                    "feature": field,
                    "current_value": value,
                    "contribution_score": factor["contribution_score"],
                    "description": f"{field} contributed {score:.3f} to the anomaly score",
                    "relationship": "supporting",
                })

        # Layer 3: Fallback items (low contribution, correlation-only, or missing data notes)
        fallback_items = []
        for factor in contributing_factors:
            score = factor["absolute_contribution"]
            if score <= 0.05 and score > 0.001:
                fallback_items.append({
                    "feature": factor["feature"],
                    "current_value": factor["current_value"],
                    "contribution_score": factor["contribution_score"],
                    "description": f"{factor['feature']} has minimal contribution ({score:.3f}); correlation, not causal",
                    "relationship": "correlation_only",
                })
        # Add missing field notes
        for missing_field in fields_missing:
            fallback_items.append({
                "feature": missing_field,
                "current_value": "N/A",
                "contribution_score": 0.0,
                "description": f"Field '{missing_field}' not available in data; could not evaluate its contribution",
                "relationship": "insufficient_data",
            })

        # Correlation vs causation notes
        causal_notes = self._build_causal_notes(direct_triggers, supporting_factors, anomaly_record, context)

        # Build root causes (top 5 from direct + supporting)
        root_causes = []
        for trigger in direct_triggers[:3]:
            root_causes.append({
                "type": "direct_trigger",
                "feature": trigger["feature"],
                "contribution_score": trigger["contribution_score"],
                "severity": "high",
                "description": trigger["description"],
                "relationship": trigger["relationship"],
            })
        for supp in supporting_factors[:2]:
            root_causes.append({
                "type": "supporting_factor",
                "feature": supp["feature"],
                "contribution_score": supp["contribution_score"],
                "severity": "medium",
                "description": supp["description"],
                "relationship": "supporting",
            })

        # Recommended actions
        actions = self._recommend_actions(contributing_factors, anomaly_record, context)

        # Confidence: reduced if many fields are missing
        field_completeness = len(fields_used) / max(len(expected_fields), 1)
        confidence = 0.5 + 0.35 * field_completeness
        if direct_triggers:
            confidence += 0.1
        confidence = min(0.95, confidence)

        # Converge to 1 primary + 2 supporting causes
        primary_cause = None
        if direct_triggers:
            primary_cause = {
                "feature": direct_triggers[0]["feature"],
                "contribution_score": direct_triggers[0]["contribution_score"],
                "current_value": direct_triggers[0]["current_value"],
                "outside_normal_range": direct_triggers[0].get("outside_normal_range"),
                "description": direct_triggers[0]["description"],
            }

        supporting_causes = []
        remaining_direct = direct_triggers[1:] if len(direct_triggers) > 1 else []
        for t in remaining_direct[:1]:
            supporting_causes.append({
                "feature": t["feature"],
                "contribution_score": t["contribution_score"],
                "current_value": t["current_value"],
                "description": t["description"],
            })
        for s in supporting_factors[:1]:
            supporting_causes.append({
                "feature": s["feature"],
                "contribution_score": s["contribution_score"],
                "current_value": s["current_value"],
                "description": s["description"],
            })

        # Uncertain notes: deduplicate and consolidate
        uncertain_notes = []
        fallback_descriptions = [f["description"] for f in fallback_items if f.get("description")]
        if fallback_descriptions:
            uncertain_notes.append("; ".join(fallback_descriptions[:2]))
        # Add only ONE correlation note, not repeated across layers
        if not direct_triggers and not supporting_factors:
            uncertain_notes.append("Attribution is heuristic; treat as correlation evidence, not causal proof.")

        # Conclusion grade: explicit classification
        conclusion_grade = self._compute_conclusion_grade(
            direct_triggers, supporting_factors, fields_used, fields_missing, context
        )

        return RootCauseResult(
            entity_id=entity_id,
            anomaly_score=anomaly_score,
            attribution_result=attribution_result,
            root_causes=root_causes,
            contributing_factors=contributing_factors,
            explanation=attribution_result.explanation,
            confidence=round(confidence, 4),
            recommended_actions=actions,
            fields_used=fields_used,
            fields_missing=fields_missing,
            direct_triggers=direct_triggers,
            supporting_factors=supporting_factors,
            fallback_items=fallback_items,
            causal_notes=causal_notes,
            primary_cause=primary_cause,
            supporting_causes=supporting_causes,
            uncertain_notes=uncertain_notes,
            conclusion_grade=conclusion_grade,
        )

    @staticmethod
    def _compute_conclusion_grade(
        direct_triggers: list[dict],
        supporting: list[dict],
        fields_used: list[str],
        fields_missing: list[str],
        context: dict[str, Any],
    ) -> str:
        """Determine conclusion grade based on evidence strength."""
        # confirmed: multiple direct triggers outside normal range + behavior alignment
        if len(direct_triggers) >= 2 and all(t.get("outside_normal_range") for t in direct_triggers[:2]):
            behavior_dev = context.get("behavior_deviation_score", 0)
            if behavior_dev and behavior_dev > 0.5:
                return "confirmed"
            if len(direct_triggers) >= 3:
                return "confirmed"
            return "likely"

        # likely: 1-2 direct triggers, some outside normal range
        if direct_triggers and any(t.get("outside_normal_range") for t in direct_triggers):
            return "likely"

        # possible: only supporting factors or no triggers outside normal range
        if supporting or direct_triggers:
            return "possible"

        # insufficient_evidence: no meaningful evidence
        return "insufficient_evidence"

    @staticmethod
    def _get_normal_range(field: str) -> str:
        """Return human-readable normal range for a field."""
        ranges = {
            'bytes': '100-100,000 bytes/flow',
            'packets': '1-1,000 packets/flow',
            'duration_ms': '10-30,000 ms',
            'dst_port': '80 (HTTP), 443 (HTTPS), 53 (DNS), 22 (SSH)',
            'byte_asymmetry': '0.0-0.3 (balanced traffic)',
            'packet_asymmetry': '0.0-0.3 (balanced traffic)',
            'dns_query_entropy': '2.0-4.0 (normal DNS patterns)',
            'ttl_avg': '32-128 (typical TTL)',
            'src_to_dst_byte_ratio': '0.3-3.0 (bidirectional traffic)',
        }
        return ranges.get(field, 'no reference range')

    @staticmethod
    def _is_outside_range(field: str, value: Any) -> bool:
        """Check if a value falls outside the normal range."""
        if not isinstance(value, (int, float)):
            return False
        ranges = {
            'bytes': (100, 100000),
            'packets': (1, 1000),
            'duration_ms': (10, 30000),
            'byte_asymmetry': (0.0, 0.3),
            'packet_asymmetry': (0.0, 0.3),
            'dns_query_entropy': (2.0, 4.0),
            'ttl_avg': (32, 128),
            'src_to_dst_byte_ratio': (0.3, 3.0),
        }
        if field not in ranges:
            return False
        low, high = ranges[field]
        return value < low or value > high

    @staticmethod
    def _build_trigger_description(field: str, value: Any, outside_range: bool) -> str:
        """Build a human-readable description for a trigger."""
        if not isinstance(value, (int, float)):
            return f"{field} has non-numeric value"
        descriptions = {
            'bytes': f"Traffic volume ({value:,.0f} bytes) {'exceeds normal range' if outside_range else 'is elevated'}",
            'packets': f"Packet count ({value:,.0f}) {'exceeds normal range' if outside_range else 'is elevated'}",
            'duration_ms': f"Connection duration ({value/1000:.1f}s) {'exceeds normal range' if outside_range else 'is extended'}",
            'byte_asymmetry': f"Byte asymmetry ({value:.2f}) {'strongly unidirectional' if outside_range else 'moderately asymmetric'}",
            'packet_asymmetry': f"Packet asymmetry ({value:.2f}) {'strongly unidirectional' if outside_range else 'moderately asymmetric'}",
            'dns_query_entropy': f"DNS query entropy ({value:.2f}) {'unusually high, possible DGA/tunneling' if outside_range else 'elevated'}",
            'dst_port': f"Non-standard port ({int(value)}) used",
            'ttl_avg': f"Average TTL ({value:.0f}) {'unusual' if outside_range else 'within range'}",
            'src_to_dst_byte_ratio': f"Src/dst byte ratio ({value:.1f}) {'highly unidirectional' if outside_range else 'moderately asymmetric'}",
        }
        return descriptions.get(field, f"{field} value: {value}")

    def _build_causal_notes(
        self,
        direct_triggers: list[dict],
        supporting: list[dict],
        record: dict[str, Any],
        context: dict[str, Any],
    ) -> list[str]:
        """Build notes about correlation vs causation confidence."""
        notes = []

        # Check if the anomaly is driven by a single dominant field
        if direct_triggers:
            top_score = max(abs(t["contribution_score"]) for t in direct_triggers)
            total_score = sum(abs(t["contribution_score"]) for t in direct_triggers)
            if total_score > 0 and top_score / total_score > 0.6:
                notes.append(
                    f"Dominant driver: '{direct_triggers[0]['feature']}' accounts for "
                    f"{top_score/total_score:.0%} of the top trigger contributions. "
                    f"This is a strong attribution signal, not a coincidental correlation."
                )
            else:
                features = [t["feature"] for t in direct_triggers[:3]]
                notes.append(
                    f"Multi-factor anomaly: {', '.join(features)} jointly drive the score. "
                    f"Each contributes independently; no single dominant cause."
                )

        # Check behavioral context consistency
        behavior_tags = context.get("behavior_tags")
        deviation_score = context.get("behavior_deviation_score")
        if behavior_tags and deviation_score and deviation_score > 0.5:
            notes.append(
                f"Behavior analysis flagged this entity (deviation={deviation_score:.2f}, tags={behavior_tags}). "
                f"RCA attribution aligns with the observed behavior shift."
            )
        elif behavior_tags and deviation_score and deviation_score <= 0.5:
            notes.append(
                f"Behavior analysis did not flag a significant shift (deviation={deviation_score:.2f}). "
                f"RCA finding is based on flow-level heuristics only; validate with session evidence."
            )

        # Missing fields warning
        expected_count = 9
        used_count = len(set(f["feature"] for f in direct_triggers + supporting))
        if used_count < 4:
            notes.append(
                f"Only {used_count} of {expected_count} expected fields drove the attribution. "
                f"Conclusion is sensitive to which fields are available."
            )

        # Default correlation note
        if not notes:
            notes.append(
                "Attribution is heuristic: features ranked by coalition-weighted marginal impact. "
                "Treat as correlation evidence, not causal proof. Validate with flow/packet inspection."
            )

        return notes

    def _recommend_actions(self, factors: list[dict], record: dict[str, Any], context: dict[str, Any] | None = None) -> list[str]:
        """Generate investigation recommendations from heuristic attribution factors.

        Recommendations are tied to field combinations, not hardcoded phrases.
        """
        context = context or {}
        actions = []

        top_factors = [f["feature"] for f in factors[:3]]

        if "byte_asymmetry" in top_factors or "packet_asymmetry" in top_factors:
            ratio = record.get("src_to_dst_byte_ratio", 0)
            if ratio > 3:
                actions.append("Src->dst byte ratio is highly asymmetric; investigate potential data exfiltration")
            elif ratio < 0.3:
                actions.append("Dst->src byte ratio is highly asymmetric; investigate potential download or command-and-control response")
            else:
                actions.append("Traffic asymmetry detected; review data transfer patterns")

        if "dns_query_entropy" in top_factors:
            entropy = record.get("dns_query_entropy", 0)
            if entropy > 4.0:
                actions.append("DNS query entropy exceeds normal range; possible DGA domain generation or DNS tunneling")
            else:
                actions.append("DNS query patterns show deviation; analyze DNS query content")

        if "dst_port" in top_factors:
            dst_port = record.get("dst_port", 0)
            if dst_port > 1024 and dst_port not in [8080, 8443, 3306, 5432, 6379, 27017]:
                actions.append(f"Non-standard high port ({int(dst_port)}) in use; identify the service")
            elif dst_port in [4444, 5555, 1337, 31337]:
                actions.append(f"Port {int(dst_port)} is commonly associated with backdoors; investigate immediately")

        if "bytes" in top_factors or "packets" in top_factors:
            bytes_val = record.get("bytes", 0)
            if bytes_val > 1_000_000:
                actions.append(f"Large data transfer ({bytes_val/1_000_000:.1f} MB); review payload content")
            else:
                actions.append("Unusual traffic volume for this entity; compare against baseline")

        if "duration_ms" in top_factors:
            dur = record.get("duration_ms", 0)
            if dur > 60_000:
                actions.append(f"Long-lived connection ({dur/1000:.0f}s); check if this is a persistent tunnel or streaming session")
            else:
                actions.append("Connection duration deviates from baseline; review connection patterns")

        # Cross-reference with behavior analysis if available
        behavior_tags = context.get("behavior_tags", "")
        if behavior_tags:
            if "volume_anomaly" in behavior_tags and "bytes" in top_factors:
                actions.append("Behavior analysis confirms volume anomaly; prioritize payload inspection")
            if "destination_spread" in behavior_tags:
                actions.append("Behavior analysis shows destination expansion; correlate with threat intelligence")

        if not actions:
            actions.append("Review anomalous features identified by heuristic attribution analysis")

        return actions[:5]
