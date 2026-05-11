"""
Behavior Analysis Module

Provides user/host behavior modeling, behavioral shift detection, session
pattern mining, and concept drift detection for network traffic analysis.

This module analyzes:
- Connection frequency patterns
- Data transfer volumes
- Protocol usage patterns
- Time-of-day behavior
- Peer communication patterns
- Behavioral baselines and anomalies
"""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class BehaviorProfile:
    """Represents a behavior profile for a host or user."""
    entity_id: str
    entity_type: str  # host, user, device
    baseline: dict[str, Any]
    current_behavior: dict[str, Any]
    deviation_score: float
    behavior_tags: list[str]
    anomalies: list[dict[str, Any]]
    comparison_details: dict[str, Any] = None  # Detailed comparison breakdown
    data_quality: dict[str, Any] = None  # Data quality assessment


class BehaviorAnalyzer:
    """
    Analyzes network behavior patterns for hosts and users.
    
    Focuses on:
    - Building behavior baselines
    - Detecting behavioral shifts
    - Mining session patterns
    - Markov Chain state transitions
    - Concept drift detection
    """

    @staticmethod
    def _parse_timestamp(value: Any) -> datetime | None:
        if value in (None, ""):
            return None
        # Already a datetime object (e.g., from DuckDB)
        if isinstance(value, datetime):
            return value
        text = str(value).strip()
        if not text:
            return None
        normalized = text.replace("Z", "+00:00")
        for candidate in (normalized, normalized.replace(" ", "T", 1)):
            try:
                return datetime.fromisoformat(candidate)
            except ValueError:
                pass
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%m/%d/%Y %H:%M:%S"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                pass
        return None

    @staticmethod
    def _parse_time_marker(value: Any) -> datetime | float | None:
        dt = BehaviorAnalyzer._parse_timestamp(value)
        if dt is not None:
            return dt
        if value in (None, ""):
            return None
        try:
            return float(str(value).strip())
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _extract_hour(value: Any) -> int | None:
        """Extract hour from timestamp, handling both strings and datetime objects."""
        dt = BehaviorAnalyzer._parse_timestamp(value)
        if dt is not None:
            return dt.hour
        return None

    @staticmethod
    def _flow_timestamp(flow: dict[str, Any]) -> datetime | float | None:
        ts = (
            flow.get("analysis_time_ts")
            or flow.get("timestamp")
            or flow.get("start_relative_time_s")
            or flow.get("end_relative_time_s")
        )
        return BehaviorAnalyzer._parse_time_marker(ts)

    def _time_order_flows(self, flows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        annotated: list[tuple[datetime | float, dict[str, Any]]] = []
        unsupported_timestamps = 0
        for flow in flows:
            parsed = self._flow_timestamp(flow)
            if parsed is None:
                unsupported_timestamps += 1
                continue
            annotated.append((parsed, flow))

        if not annotated:
            return list(flows), {
                "time_ordered": False,
                "time_field": "timestamp",
                "unsupported_timestamps": unsupported_timestamps,
                "warning": "No parseable absolute timestamps were available; baseline/current split used input order.",
            }

        ordered = [flow for _, flow in sorted(annotated, key=lambda item: item[0])]
        if len(ordered) < len(flows):
            ordered.extend(flow for flow in flows if self._flow_timestamp(flow) is None)
        return ordered, {
            "time_ordered": True,
            "time_field": "analysis_time_ts",
            "unsupported_timestamps": unsupported_timestamps,
        }
    
    def build_user_baseline(self, entity_id: str, historical_flows: list[dict[str, Any]]) -> dict[str, Any]:
        """
        Build behavior baseline from historical flow data.
        
        Args:
            entity_id: Host or user identifier
            historical_flows: List of historical flow records
            
        Returns:
            Dictionary with behavior baseline
        """
        if not historical_flows:
            return {"entity_id": entity_id, "baseline": {}, "confidence": 0.0}
        
        # Extract behavior metrics
        bytes_list = []
        packets_list = []
        dst_ips = set()
        dst_ports = set()
        protocols = Counter()
        hours_of_activity = Counter()
        
        for flow in historical_flows:
            # Time-based metrics
            hour = self._extract_hour(
                flow.get("analysis_time_ts") or flow.get("timestamp") or flow.get("start_relative_time_s") or flow.get("end_relative_time_s")
            )
            if hour is not None:
                hours_of_activity[hour] += 1
            
            # Volume metrics
            bytes_list.append(flow.get("bytes", 0))
            packets_list.append(flow.get("packets", 0))
            
            # Connection metrics
            dst_ips.add(flow.get("dst_ip", ""))
            dst_ports.add(flow.get("dst_port", 0))
            protocols[flow.get("protocol", "")] += 1
        
        # Calculate statistics
        baseline = {
            "entity_id": entity_id,
            "total_flows": len(historical_flows),
            "unique_destinations": len(dst_ips),
            "unique_ports": len(dst_ports),
            "avg_bytes": sum(bytes_list) / len(bytes_list) if bytes_list else 0,
            "avg_packets": sum(packets_list) / len(packets_list) if packets_list else 0,
            "max_bytes": max(bytes_list) if bytes_list else 0,
            "max_packets": max(packets_list) if packets_list else 0,
            "protocol_distribution": dict(protocols),
            "hourly_activity": dict(hours_of_activity),
            "peak_hours": [h for h, c in hours_of_activity.most_common(5)],
        }
        
        return baseline
    
    def detect_behavior_shift(self, current_flows: list[dict[str, Any]], 
                             baseline: dict[str, Any]) -> dict[str, Any]:
        """
        Detect behavioral shifts from baseline.
        
        Args:
            current_flows: Recent flow data
            baseline: Historical baseline
            
        Returns:
            Dictionary with shift detection results
        """
        if not current_flows or not baseline:
            return {"shift_detected": False, "deviation_score": 0.0, "anomalies": []}
        
        anomalies = []
        deviation_scores = []
        
        # Check connection volume deviation
        current_avg_bytes = sum(f.get("bytes", 0) for f in current_flows) / len(current_flows)
        baseline_avg_bytes = baseline.get("avg_bytes", 0)
        
        if baseline_avg_bytes > 0:
            bytes_ratio = current_avg_bytes / baseline_avg_bytes
            if bytes_ratio > 3.0 or bytes_ratio < 0.3:
                anomalies.append({
                    "type": "volume_deviation",
                    "severity": "high" if bytes_ratio > 5.0 or bytes_ratio < 0.2 else "medium",
                    "current_value": current_avg_bytes,
                    "baseline_value": baseline_avg_bytes,
                    "ratio": bytes_ratio,
                    "description": f"Traffic volume deviation: {bytes_ratio:.2f}x from baseline"
                })
                deviation_scores.append(min(1.0, abs(bytes_ratio - 1.0) / 5.0))
        
        # Check destination spread
        current_dsts = set(f.get("dst_ip", "") for f in current_flows)
        baseline_dsts = baseline.get("unique_destinations", 0)
        
        if baseline_dsts > 0:
            dst_ratio = len(current_dsts) / baseline_dsts
            if dst_ratio > 2.0:
                anomalies.append({
                    "type": "destination_spread",
                    "severity": "medium",
                    "current_value": len(current_dsts),
                    "baseline_value": baseline_dsts,
                    "ratio": dst_ratio,
                    "description": f"Unusual destination spread: {dst_ratio:.2f}x from baseline"
                })
                deviation_scores.append(min(1.0, (dst_ratio - 1.0) / 3.0))
        
        # Check port usage
        current_ports = set(f.get("dst_port", 0) for f in current_flows)
        baseline_ports = baseline.get("unique_ports", 0)
        
        if baseline_ports > 0:
            port_ratio = len(current_ports) / baseline_ports
            if port_ratio > 2.0:
                anomalies.append({
                    "type": "port_diversity",
                    "severity": "medium",
                    "current_value": len(current_ports),
                    "baseline_value": baseline_ports,
                    "ratio": port_ratio,
                    "description": f"Unusual port diversity: {port_ratio:.2f}x from baseline"
                })
                deviation_scores.append(min(1.0, (port_ratio - 1.0) / 3.0))
        
        # Check time-of-day shift
        current_hours = Counter()
        for flow in current_flows:
            hour = self._extract_hour(
                flow.get("analysis_time_ts") or flow.get("timestamp") or flow.get("start_relative_time_s") or flow.get("end_relative_time_s")
            )
            if hour is not None:
                current_hours[hour] += 1
        
        baseline_peak = set(baseline.get("peak_hours", []))
        current_peak = set(h for h, c in current_hours.most_common(5))
        
        if baseline_peak and current_peak:
            overlap = len(baseline_peak & current_peak) / len(baseline_peak)
            if overlap < 0.5:
                anomalies.append({
                    "type": "time_shift",
                    "severity": "low",
                    "overlap": overlap,
                    "description": f"Activity time shift detected: {overlap:.0%} overlap with baseline"
                })
                deviation_scores.append(1.0 - overlap)
        
        # Calculate overall deviation score
        overall_deviation = max(deviation_scores) if deviation_scores else 0.0
        
        return {
            "shift_detected": len(anomalies) > 0,
            "deviation_score": overall_deviation,
            "anomaly_count": len(anomalies),
            "anomalies": anomalies,
            "severity": "high" if overall_deviation > 0.7 else "medium" if overall_deviation > 0.4 else "low"
        }
    
    def mine_session_patterns(self, session_sequence: list[dict[str, Any]], 
                             min_support: float = 0.1) -> list[dict[str, Any]]:
        """
        Mine frequent session patterns using n-gram analysis.
        
        Args:
            session_sequence: List of session records in order
            min_support: Minimum support threshold
            
        Returns:
            List of frequent patterns
        """
        if len(session_sequence) < 2:
            return []
        
        # Extract session types
        session_types = []
        for session in session_sequence:
            dst_port = session.get("dst_port", 0)
            protocol = session.get("protocol", "")
            bytes_total = session.get("bytes", 0)
            
            # Classify session type
            if dst_port == 80 or dst_port == 443:
                session_types.append("web")
            elif dst_port == 22:
                session_types.append("ssh")
            elif dst_port == 25 or dst_port == 587:
                session_types.append("email")
            elif dst_port == 53:
                session_types.append("dns")
            elif bytes_total > 10_000_000:
                session_types.append("large_transfer")
            else:
                session_types.append("other")
        
        # Find frequent n-grams
        patterns = []
        for n in [2, 3, 4]:  # Check 2-grams, 3-grams, 4-grams
            ngrams = Counter()
            for i in range(len(session_types) - n + 1):
                ngram = tuple(session_types[i:i+n])
                ngrams[ngram] += 1
            
            # Filter by support
            min_count = int(len(session_types) * min_support)
            for ngram, count in ngrams.items():
                if count >= min_count:
                    patterns.append({
                        "pattern": list(ngram),
                        "count": count,
                        "support": count / len(session_types),
                        "length": n
                    })
        
        # Sort by support (most frequent first)
        patterns.sort(key=lambda x: x["support"], reverse=True)
        
        return patterns
    
    def model_markov_transitions(self, state_sequence: list[str]) -> dict[str, Any]:
        """
        Model state transitions using Markov Chain.
        
        Args:
            state_sequence: Sequence of states (e.g., connection types)
            
        Returns:
            Dictionary with Markov model
        """
        if len(state_sequence) < 2:
            return {"transition_matrix": {}, "states": []}
        
        # Count transitions
        transitions: dict[str, Counter] = defaultdict(Counter)
        states = set()
        
        for i in range(len(state_sequence) - 1):
            current_state = state_sequence[i]
            next_state = state_sequence[i + 1]
            states.add(current_state)
            states.add(next_state)
            transitions[current_state][next_state] += 1
        
        # Calculate transition probabilities
        transition_matrix = {}
        for state, next_states in transitions.items():
            total = sum(next_states.values())
            transition_matrix[state] = {
                next_state: count / total
                for next_state, count in next_states.items()
            }
        
        return {
            "transition_matrix": transition_matrix,
            "states": sorted(list(states)),
            "total_transitions": len(state_sequence) - 1
        }
    
    def detect_concept_drift(self, time_series: list[dict[str, Any]], 
                            window_size: int = 100) -> dict[str, Any]:
        """
        Detect concept drift in time series data using ADWIN-like approach.
        
        Args:
            time_series: List of time-ordered data points
            window_size: Window size for drift detection
            
        Returns:
            Dictionary with drift detection results
        """
        if len(time_series) < window_size * 2:
            return {"drift_detected": False, "drift_points": [], "confidence": 0.0}
        
        # Extract numeric values (e.g., bytes per time bucket)
        values = [ts.get("bytes", 0) for ts in time_series]
        
        # Sliding window comparison
        drift_points = []
        for i in range(window_size, len(values) - window_size):
            window_before = values[i - window_size:i]
            window_after = values[i:i + window_size]
            
            # Calculate means
            mean_before = sum(window_before) / len(window_before)
            mean_after = sum(window_after) / len(window_after)
            
            # Calculate variance
            var_before = sum((x - mean_before) ** 2 for x in window_before) / len(window_before)
            var_after = sum((x - mean_after) ** 2 for x in window_after) / len(window_after)
            
            # Check for significant change
            if mean_before > 0:
                change_ratio = abs(mean_after - mean_before) / mean_before
                
                # Drift detection threshold
                if change_ratio > 0.5:  # 50% change
                    drift_points.append({
                        "index": i,
                        "timestamp": time_series[i].get("timestamp", ""),
                        "mean_before": mean_before,
                        "mean_after": mean_after,
                        "change_ratio": change_ratio,
                        "variance_before": var_before,
                        "variance_after": var_after
                    })
        
        return {
            "drift_detected": len(drift_points) > 0,
            "drift_point_count": len(drift_points),
            "drift_points": drift_points[:10],  # Return top 10 drift points
            "confidence": min(1.0, len(drift_points) / 10.0)
        }
    
    def analyze_behavior(
        self,
        entity_id: str,
        flows: list[dict[str, Any]],
        *,
        baseline_start: str | None = None,
        baseline_end: str | None = None,
        current_start: str | None = None,
        current_end: str | None = None,
        baseline_ratio: float = 0.5,
        min_baseline_size: int = 10,
    ) -> BehaviorProfile:
        """
        Comprehensive behavior analysis with explicit time window support.

        Args:
            entity_id: Host or user identifier
            flows: List of flow records
            baseline_start/end: Explicit time bounds for baseline window
            current_start/end: Explicit time bounds for current window
            baseline_ratio: Fallback ratio when no explicit windows given
            min_baseline_size: Minimum flows needed for reliable baseline

        Returns:
            BehaviorProfile result object
        """
        if not flows:
            return BehaviorProfile(
                entity_id=entity_id,
                entity_type="host",
                baseline={},
                current_behavior={},
                deviation_score=0.0,
                behavior_tags=[],
                anomalies=[],
                comparison_details={},
                data_quality={"sufficient_data": False, "issues": ["no_flow_data"]},
            )

        # Data quality check
        quality_issues: list[str] = []
        has_timestamps = sum(1 for f in flows if self._flow_timestamp(f) is not None)
        if has_timestamps == 0:
            quality_issues.append("no_parseable_timestamps")
        elif has_timestamps < len(flows) * 0.5:
            quality_issues.append("partial_timestamps")

        # Determine windowing strategy
        has_explicit_windows = all([baseline_start, baseline_end, current_start, current_end])
        if has_explicit_windows:
            baseline_start_dt = self._parse_time_marker(baseline_start)
            baseline_end_dt = self._parse_time_marker(baseline_end)
            current_start_dt = self._parse_time_marker(current_start)
            current_end_dt = self._parse_time_marker(current_end)
            if not all([baseline_start_dt, baseline_end_dt, current_start_dt, current_end_dt]):
                quality_issues.append("invalid_explicit_window_bounds")
            # Use explicit time-based filtering
            baseline_flows = []
            current_flows = []
            for f in flows:
                ts = self._flow_timestamp(f)
                if ts is None or not all([baseline_start_dt, baseline_end_dt, current_start_dt, current_end_dt]):
                    continue
                if baseline_start_dt <= ts <= baseline_end_dt:
                    baseline_flows.append(f)
                elif current_start_dt <= ts <= current_end_dt:
                    current_flows.append(f)
            windowing_method = "explicit_time_windows"
        else:
            # Fallback: time-ordered split
            ordered_flows, time_metadata = self._time_order_flows(flows)
            split_ratio = min(max(baseline_ratio, 0.1), 0.9)
            mid_point = max(1, min(len(ordered_flows) - 1, int(len(ordered_flows) * split_ratio)))
            baseline_flows = ordered_flows[:mid_point]
            current_flows = ordered_flows[mid_point:]
            windowing_method = "time_ordered_split" if time_metadata.get("time_ordered") else "input_order_split"

        # Build baselines
        baseline = self.build_user_baseline(entity_id, baseline_flows)
        baseline["windowing"] = {
            "method": windowing_method,
            "baseline_ratio": baseline_ratio,
            "baseline_flows": len(baseline_flows),
            "current_flows": len(current_flows),
            "min_baseline_size": min_baseline_size,
            "time_ordered": has_explicit_windows or (not has_explicit_windows and time_metadata.get("time_ordered", False)),
        }
        warnings = []
        if has_explicit_windows and not baseline_flows:
            warnings.append("Explicit baseline window produced no usable flows.")
        if has_explicit_windows and not current_flows:
            warnings.append("Explicit current window produced no usable flows.")
        if len(baseline_flows) < min_baseline_size:
            warnings.append(
                f"Baseline has {len(baseline_flows)} flows, below the recommended minimum of {min_baseline_size}; deviation confidence is reduced."
            )
        if len(current_flows) < 5:
            warnings.append(
                f"Current window has {len(current_flows)} flows; behavior comparison has limited statistical power."
            )
        if warnings:
            baseline["warnings"] = warnings

        # Detect shifts
        shift_detection = self.detect_behavior_shift(current_flows, baseline)

        # Build current behavior metrics
        current_behavior = {
            "total_flows": len(current_flows),
            "unique_destinations": len(set(f.get("dst_ip", "") for f in current_flows)),
            "unique_ports": len(set(f.get("dst_port", 0) for f in current_flows)),
            "avg_bytes": sum(f.get("bytes", 0) for f in current_flows) / len(current_flows) if current_flows else 0,
            "avg_packets": sum(f.get("packets", 0) for f in current_flows) / len(current_flows) if current_flows else 0,
        }

        # Build detailed comparison
        comparison_details = self._build_comparison_details(
            baseline_flows, current_flows, baseline, shift_detection
        )

        # Behavior tags
        behavior_tags = []
        if shift_detection.get("deviation_score", 0) > 0.7:
            behavior_tags.append("high_deviation")
        if shift_detection.get("anomaly_count", 0) > 3:
            behavior_tags.append("multiple_anomalies")
        if any(a.get("type") == "volume_deviation" for a in shift_detection.get("anomalies", [])):
            behavior_tags.append("volume_anomaly")
        if any(a.get("type") == "destination_spread" for a in shift_detection.get("anomalies", [])):
            behavior_tags.append("destination_spread")
        if any(a.get("type") == "protocol_change" for a in shift_detection.get("anomalies", [])):
            behavior_tags.append("protocol_anomaly")
        if any(a.get("type") == "new_service" for a in shift_detection.get("anomalies", [])):
            behavior_tags.append("new_service_detected")

        # Reduce confidence if data quality is poor
        final_deviation = shift_detection.get("deviation_score", 0.0)
        if quality_issues:
            final_deviation = min(final_deviation, 0.4)

        sufficient_data = (
            len(baseline_flows) >= min_baseline_size
            and len(current_flows) >= 5
            and not any("no_parseable_timestamps" in q for q in quality_issues)
        )

        return BehaviorProfile(
            entity_id=entity_id,
            entity_type="host",
            baseline=baseline,
            current_behavior=current_behavior,
            deviation_score=round(final_deviation, 4),
            behavior_tags=behavior_tags,
            anomalies=shift_detection.get("anomalies", []),
            comparison_details=comparison_details,
            data_quality={
                "sufficient_data": sufficient_data,
                "issues": quality_issues,
                "baseline_flows": len(baseline_flows),
                "current_flows": len(current_flows),
                "min_baseline_met": len(baseline_flows) >= min_baseline_size,
            },
        )

    def _build_comparison_details(
        self,
        baseline_flows: list[dict[str, Any]],
        current_flows: list[dict[str, Any]],
        baseline: dict[str, Any],
        shift_detection: dict[str, Any],
    ) -> dict[str, Any]:
        """Build human-readable comparison breakdown."""
        comparison = {
            "comparison_type": "baseline_vs_current",
            "baseline_sample_size": len(baseline_flows),
            "current_sample_size": len(current_flows),
            "metric_changes": [],
            "behavioral_interpretation": [],
        }

        if not baseline_flows or not current_flows:
            comparison["behavioral_interpretation"].append({
                "level": "warning",
                "message": "Insufficient data in one or both windows for meaningful comparison.",
            })
            return comparison

        # Volume comparison
        bl_avg_bytes = baseline.get("avg_bytes", 0)
        cur_avg_bytes = sum(f.get("bytes", 0) for f in current_flows) / len(current_flows)
        if bl_avg_bytes > 0:
            bytes_change = (cur_avg_bytes - bl_avg_bytes) / bl_avg_bytes * 100
            comparison["metric_changes"].append({
                "metric": "avg_bytes_per_flow",
                "baseline_value": round(bl_avg_bytes, 2),
                "current_value": round(cur_avg_bytes, 2),
                "change_pct": round(bytes_change, 1),
                "interpretation": "significant_increase" if bytes_change > 50 else "significant_decrease" if bytes_change < -50 else "normal",
            })

        # Destination comparison
        bl_dsts = baseline.get("unique_destinations", 0)
        cur_dsts = len(set(f.get("dst_ip", "") for f in current_flows))
        if bl_dsts > 0:
            dst_change = (cur_dsts - bl_dsts) / bl_dsts * 100
            comparison["metric_changes"].append({
                "metric": "unique_destinations",
                "baseline_value": bl_dsts,
                "current_value": cur_dsts,
                "change_pct": round(dst_change, 1),
                "interpretation": "destination_expansion" if dst_change > 50 else "destination_contraction" if dst_change < -30 else "normal",
            })

        # Port comparison
        bl_ports = baseline.get("unique_ports", 0)
        cur_ports = len(set(f.get("dst_port", 0) for f in current_flows))
        if bl_ports > 0:
            port_change = (cur_ports - bl_ports) / bl_ports * 100
            comparison["metric_changes"].append({
                "metric": "unique_ports",
                "baseline_value": bl_ports,
                "current_value": cur_ports,
                "change_pct": round(port_change, 1),
                "interpretation": "port_diversity_increase" if port_change > 50 else "normal",
            })

        # Protocol distribution comparison
        bl_protocols = baseline.get("protocol_distribution", {})
        cur_protocols = Counter(f.get("protocol", "") for f in current_flows)
        if bl_protocols:
            bl_proto_set = set(bl_protocols.keys())
            cur_proto_set = set(cur_protocols.keys())
            new_protos = cur_proto_set - bl_proto_set
            lost_protos = bl_proto_set - cur_proto_set
            if new_protos:
                comparison["metric_changes"].append({
                    "metric": "protocol_change",
                    "baseline_value": sorted(bl_proto_set),
                    "current_value": sorted(cur_proto_set),
                    "new_protocols": sorted(new_protos),
                    "lost_protocols": sorted(lost_protos),
                    "interpretation": "protocol_mix_change",
                })

        # Generate behavioral interpretation
        for change in comparison["metric_changes"]:
            interp = change.get("interpretation", "")
            metric = change["metric"]
            if "significant_increase" in interp:
                comparison["behavioral_interpretation"].append({
                    "metric": metric,
                    "meaning": f"Average traffic volume increased by {change['change_pct']:.0f}%. Could indicate data exfiltration, large downloads, or new application usage.",
                    "severity": "medium",
                })
            elif "destination_expansion" in interp:
                comparison["behavioral_interpretation"].append({
                    "metric": metric,
                    "meaning": f"Destination count increased by {change['change_pct']:.0f}%. May indicate reconnaissance, C2 communication, or legitimate service expansion.",
                    "severity": "medium",
                })
            elif "port_diversity_increase" in interp:
                comparison["behavioral_interpretation"].append({
                    "metric": metric,
                    "meaning": f"Port diversity increased by {change['change_pct']:.0f}%. Host is communicating over more services than baseline.",
                    "severity": "low",
                })

        return comparison
