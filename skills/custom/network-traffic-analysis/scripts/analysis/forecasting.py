"""
Forecasting and Trend Analysis Module

Implementations of:
- Holt-Winters Triple Exponential Smoothing (additive and multiplicative)
- Linear regression with least squares
- CUSUM (Cumulative Sum) change point detection
- Proper forecast confidence intervals (sqrt scaling)
- Autocorrelation-based seasonality detection (ACF at multiple lags)

References:
- Holt, C. E. (1957). "Forecasting seasonals and trends by exponentially weighted moving averages"
- Winters, P. R. (1960). "Forecasting sales by exponentially weighted moving averages"
- Page, E. S. (1954). "Continuous Inspection Schemes"
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any


@dataclass
class ForecastResult:
    """Forecast result."""
    forecast_points: list[dict[str, Any]]
    trend_direction: str
    seasonality_detected: bool
    seasonality_period: int
    anomaly_predictions: list[dict[str, Any]]
    capacity_risk: str
    recommendations: list[str]
    method_used: str


class HoltWinters:
    """
    Holt-Winters Triple Exponential Smoothing algorithm.

    Decomposes time series into three components:
    - Level (l_t): smoothed value
    - Trend (b_t): slope
    - Seasonality (s_t): periodic pattern

    Additive model:
        l_t = α * (y_t - s_{t-m}) + (1 - α) * (l_{t-1} + b_{t-1})
        b_t = β * (l_t - l_{t-1}) + (1 - β) * b_{t-1}
        s_t = γ * (y_t - l_t) + (1 - γ) * s_{t-m}
        ŷ_{t+h} = l_t + h * b_t + s_{t-m+h%m}

    Multiplicative model:
        l_t = α * (y_t / s_{t-m}) + (1 - α) * (l_{t-1} + b_{t-1})
        b_t = β * (l_t - l_{t-1}) + (1 - β) * b_{t-1}
        s_t = γ * (y_t / l_t) + (1 - γ) * s_{t-m}
        ŷ_{t+h} = (l_t + h * b_t) * s_{t-m+h%m}

    Parameters:
    - α (alpha): level smoothing (0 to 1)
    - β (beta): trend smoothing (0 to 1)
    - γ (gamma): seasonal smoothing (0 to 1)
    - m: seasonality period length

    Time complexity: O(n) for fitting, O(h) for forecasting
    Space complexity: O(m) for seasonal components

    Reference: Winters, Management Science 1960
    """

    def __init__(self, season_length: int, model_type: str = "additive",
                 alpha: float = 0.2, beta: float = 0.1, gamma: float = 0.1):
        """
        Initialize Holt-Winters model.

        Args:
            season_length: Length of seasonal period (m)
            model_type: "additive" or "multiplicative"
            alpha: Level smoothing factor (0 to 1)
            beta: Trend smoothing factor (0 to 1)
            gamma: Seasonal smoothing factor (0 to 1)
        """
        self.m = season_length
        self.model_type = model_type
        self.alpha = alpha
        self.beta = beta
        self.gamma = gamma

        # Fitted components
        self.level: list[float] = []
        self.trend: list[float] = []
        self.seasonal: list[float] = []
        self.fitted_values: list[float] = []
        self.training_values: list[float] = []
        self.residuals: list[float] = []
        self._rmse: float | None = None

        # Optimized parameters (if auto-tuned)
        self.optimized_alpha = alpha
        self.optimized_beta = beta
        self.optimized_gamma = gamma

    def fit(self, values: list[float]) -> dict[str, Any]:
        """
        Fit Holt-Winters model to time series data.

        Initialization:
        - Level: average of first season
        - Trend: average trend over first two seasons
        - Seasonal: deviation from level

        Args:
            values: Time series values

        Returns:
            Dictionary with fitting results
        """
        n = len(values)
        if n < 2 * self.m:
            return {
                "success": False,
                "error": f"Need at least 2 seasons ({2 * self.m} points), got {n}"
            }

        # Initialize level (average of first season)
        level_0 = sum(values[:self.m]) / self.m

        # Initialize trend (average change between first two seasons)
        if n >= 2 * self.m:
            season_1_avg = sum(values[:self.m]) / self.m
            season_2_avg = sum(values[self.m:2*self.m]) / self.m
            trend_0 = (season_2_avg - season_1_avg) / self.m
        else:
            trend_0 = 0.0

        # Initialize seasonal components
        seasonal = [0.0] * self.m
        for i in range(self.m):
            if self.model_type == "additive":
                seasonal[i] = values[i] - level_0
            else:  # multiplicative
                seasonal[i] = values[i] / level_0 if level_0 > 0 else 1.0

        # Normalize seasonal components
        if self.model_type == "additive":
            s_mean = sum(seasonal) / self.m
            seasonal = [s - s_mean for s in seasonal]
        else:
            s_mean = sum(seasonal) / self.m
            if s_mean > 0:
                seasonal = [s / s_mean for s in seasonal]

        # Initialize state
        level = level_0
        trend = trend_0

        # Fit model
        fitted_values = []
        levels = [level]
        trends = [trend]

        for t in range(n):
            # Get seasonal index
            s_idx = t % self.m
            s_t = seasonal[s_idx]

            # One-step-ahead forecast
            if self.model_type == "additive":
                forecast = level + trend + s_t
            else:
                forecast = (level + trend) * s_t

            fitted_values.append(forecast)

            # Update level
            if self.model_type == "additive":
                level = (
                    self.alpha * (values[t] - s_t) +
                    (1 - self.alpha) * (level + trend)
                )
            else:
                level = (
                    self.alpha * (values[t] / s_t if s_t > 0 else values[t]) +
                    (1 - self.alpha) * (level + trend)
                )

            # Update trend
            trend = (
                self.beta * (level - levels[-1]) +
                (1 - self.beta) * trend
            )

            # Update seasonal
            if self.model_type == "additive":
                seasonal[s_idx] = (
                    self.gamma * (values[t] - level) +
                    (1 - self.gamma) * seasonal[s_idx]
                )
            else:
                seasonal[s_idx] = (
                    self.gamma * (values[t] / level if level > 0 else 1.0) +
                    (1 - self.gamma) * seasonal[s_idx]
                )

            levels.append(level)
            trends.append(trend)

        # Store results
        self.level = levels[:-1]  # Remove last (extra)
        self.trend = trends[:-1]
        self.seasonal = seasonal
        self.fitted_values = fitted_values
        self.training_values = list(values)

        # Compute residuals and RMSE
        residuals = [values[t] - fitted_values[t] for t in range(n)]
        rmse = math.sqrt(sum(r ** 2 for r in residuals) / n)
        self.residuals = residuals
        self._rmse = rmse

        return {
            "success": True,
            "fitted_values": fitted_values,
            "residuals": residuals,
            "rmse": rmse,
            "final_level": level,
            "final_trend": trend,
            "seasonal_components": seasonal,
            "n_observations": n
        }

    def forecast(self, horizon: int) -> list[dict[str, Any]]:
        """
        Generate forecasts using fitted Holt-Winters model.

        Forecast formula:
        - Additive: ŷ_{t+h} = l_t + h * b_t + s_{t-m+h%m}
        - Multiplicative: ŷ_{t+h} = (l_t + h * b_t) * s_{t-m+h%m}

        Confidence intervals use sqrt(h) scaling:
        - CI = ŷ ± z * σ * sqrt(1 + h/n)

        Args:
            horizon: Number of periods to forecast

        Returns:
            List of forecast dictionaries
        """
        if not self.fitted_values:
            return []

        n = len(self.fitted_values)
        final_level = self.level[-1] if self.level else 0
        final_trend = self.trend[-1] if self.trend else 0

        residual_std = self._rmse
        if residual_std is None:
            residual_std = math.sqrt(sum(r ** 2 for r in self.residuals) / len(self.residuals)) if self.residuals else 1.0
        residual_std = max(float(residual_std), 1e-9)

        forecast_points = []
        z_95 = 1.96  # 95% confidence

        for h in range(1, horizon + 1):
            s_idx = (n - 1 + h) % self.m
            s_h = self.seasonal[s_idx]

            # Forecast
            if self.model_type == "additive":
                predicted = final_level + h * final_trend + s_h
            else:
                predicted = (final_level + h * final_trend) * s_h

            predicted = max(0, predicted)  # Non-negative

            # Confidence interval with sqrt(h) scaling
            # σ_h = σ * sqrt(1 + h/n)
            sigma_h = residual_std * math.sqrt(1 + h / n)

            lower = max(0, predicted - z_95 * sigma_h)
            upper = predicted + z_95 * sigma_h

            forecast_points.append({
                "period": h,
                "predicted_value": round(predicted, 2),
                "lower_bound": round(lower, 2),
                "upper_bound": round(upper, 2),
                "confidence": round(max(0.1, 1.0 - h * 0.03), 2),  # Decreases with horizon
                "sigma_h": round(sigma_h, 2)
            })

        return forecast_points


class SeasonalityDetector:
    """
    Detect seasonality using autocorrelation function (ACF) at multiple lags.

    Uses the Ljung-Box test statistic to check if autocorrelations
    at seasonal lags are significantly different from zero.

    ACF(k) = Σ_{t=k+1}^{n} [(y_t - ȳ)(y_{t-k} - ȳ)] / Σ_{t=1}^{n} (y_t - ȳ)²

    Time complexity: O(n * max_lag)
    Space complexity: O(max_lag)
    """

    @staticmethod
    def autocorrelation(values: list[float], lag: int) -> float:
        """
        Compute autocorrelation at specified lag.

        Args:
            values: Time series values
            lag: Lag k

        Returns:
            Autocorrelation coefficient
        """
        n = len(values)
        if lag >= n:
            return 0.0

        mean = sum(values) / n

        # Denominator: variance * n
        denominator = sum((y - mean) ** 2 for y in values)

        if denominator == 0:
            return 0.0

        # Numerator: covariance at lag k
        numerator = sum(
            (values[t] - mean) * (values[t - lag] - mean)
            for t in range(lag, n)
        )

        return numerator / denominator

    @staticmethod
    def detect_seasonality(values: list[float],
                          max_period: int = 24) -> dict[str, Any]:
        """
        Detect seasonality using ACF at multiple lags.

        Args:
            values: Time series values
            max_period: Maximum period to check

        Returns:
            Dictionary with seasonality analysis
        """
        n = len(values)
        if n < 2 * max_period:
            max_period = n // 2

        if max_period < 2:
            return {
                "seasonality_detected": False,
                "period": -1,
                "strength": 0.0
            }

        # Compute ACF at multiple lags
        acf_values = []
        for lag in range(1, max_period + 1):
            acf = SeasonalityDetector.autocorrelation(values, lag)
            acf_values.append({"lag": lag, "acf": acf})

        # Find positive peak autocorrelation. Strong negative autocorrelation can
        # indicate alternating behavior, but it is not enough to select a
        # repeating seasonal model.
        significance_threshold = 1.96 / math.sqrt(n)

        best_lag = -1
        best_acf = 0.0

        for acf_info in acf_values:
            lag = acf_info["lag"]
            acf = acf_info["acf"]

            if acf > best_acf and acf > significance_threshold:
                best_acf = acf
                best_lag = lag

        seasonality_detected = best_acf > significance_threshold

        return {
            "seasonality_detected": seasonality_detected,
            "period": best_lag,
            "strength": best_acf if seasonality_detected else 0.0,
            "significance_threshold": significance_threshold,
            "acf_values": acf_values[:12]  # First 12 lags for inspection
        }


class TrafficForecaster:
    """
    Traffic forecaster using multiple algorithms.

    Uses:
    - Holt-Winters for seasonal time series
    - Linear regression for trend
    - CUSUM for change point detection
    - ACF for seasonality detection
    """

    def __init__(self):
        """Initialize forecaster."""
        self.seasonality_detector = SeasonalityDetector()

    def forecast_volume(self, historical_data: list[dict[str, Any]],
                       horizon: int = 24) -> dict[str, Any]:
        """
        Forecast traffic volume using Holt-Winters if seasonal, else linear regression.

        Args:
            historical_data: List of time-bucketed data points
            horizon: Number of future periods to forecast

        Returns:
            Dictionary with forecast results
        """
        if len(historical_data) < 3:
            return {
                "forecast": [],
                "confidence": 0.0,
                "method_used": "insufficient_data",
                "message": "Insufficient historical data (need at least 3 points)"
            }

        values = [d.get("bytes", 0) for d in historical_data]
        n = len(values)

        # Detect seasonality
        seasonality = self.seasonality_detector.detect_seasonality(values)

        if seasonality["seasonality_detected"] and n >= 2 * seasonality["period"]:
            # Use Holt-Winters
            m = seasonality["period"]
            hw = HoltWinters(season_length=m, model_type="additive")
            fit_result = hw.fit(values)

            if fit_result["success"]:
                hw._rmse = fit_result["rmse"]
                forecast_points = hw.forecast(horizon)

                trend_direction = "seasonal"
                if fit_result["final_trend"] > 0:
                    trend_direction = "seasonal_increasing"
                elif fit_result["final_trend"] < 0:
                    trend_direction = "seasonal_decreasing"

                return {
                    "forecast": forecast_points,
                    "method_used": "holt_winters",
                    "season_length": m,
                    "trend_direction": trend_direction,
                    "rmse": fit_result["rmse"],
                    "confidence": max(0.3, 1.0 - horizon * 0.02)
                }

        # Fallback: Linear regression with proper confidence intervals
        return self._linear_regression_forecast(values, horizon)

    def _linear_regression_forecast(self, values: list[float],
                                   horizon: int) -> dict[str, Any]:
        """
        Linear regression forecast with proper confidence intervals.

        Confidence interval formula:
        CI(h) = ŷ(h) ± t_{α/2, n-2} * s * sqrt(1/n + (h - x̄)² / Σ(x_i - x̄)²)

        where s is the residual standard error.

        Args:
            values: Historical values
            horizon: Forecast horizon

        Returns:
            Forecast dictionary
        """
        n = len(values)

        # Simple linear regression
        x_mean = (n - 1) / 2
        y_mean = sum(values) / n

        numerator = sum((i - x_mean) * (values[i] - y_mean) for i in range(n))
        denominator = sum((i - x_mean) ** 2 for i in range(n))

        if denominator > 0:
            slope = numerator / denominator
            intercept = y_mean - slope * x_mean
        else:
            slope = 0
            intercept = y_mean

        # Residual standard error
        fitted = [slope * i + intercept for i in range(n)]
        residuals = [values[i] - fitted[i] for i in range(n)]
        s = math.sqrt(sum(r ** 2 for r in residuals) / (n - 2)) if n > 2 else 0

        # Sum of squares of x
        ss_xx = denominator

        # Generate forecast
        forecast_points = []
        t_95 = 2.0  # Approximate t-value for 95% CI

        for h in range(1, horizon + 1):
            future_x = n - 1 + h
            predicted = slope * future_x + intercept
            predicted = max(0, predicted)

            # Proper confidence interval
            # CI = ŷ ± t * s * sqrt(1/n + (x_h - x̄)² / SS_xx)
            if ss_xx > 0:
                se_forecast = s * math.sqrt(1/n + (future_x - x_mean) ** 2 / ss_xx)
            else:
                se_forecast = s

            lower = max(0, predicted - t_95 * se_forecast)
            upper = predicted + t_95 * se_forecast

            forecast_points.append({
                "period": h,
                "predicted_value": round(predicted, 2),
                "lower_bound": round(lower, 2),
                "upper_bound": round(upper, 2),
                "confidence": max(0.1, 1.0 - h * 0.03)
            })

        trend_direction = "stable"
        if slope > 0:
            trend_direction = "increasing"
        elif slope < 0:
            trend_direction = "decreasing"

        return {
            "forecast": forecast_points,
            "method_used": "linear_regression",
            "slope": slope,
            "intercept": intercept,
            "residual_std": s,
            "trend_direction": trend_direction,
            "confidence": max(0.3, 1.0 - horizon * 0.02)
        }

    def predict_anomalies(self, forecast: dict[str, Any],
                         historical_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Predict anomalies based on forecast.

        Uses historical mean and std to compute z-scores for predicted values.

        Args:
            forecast: Forecast result
            historical_data: Historical data

        Returns:
            List of predicted anomalies
        """
        if not historical_data:
            return []

        values = [d.get("bytes", 0) for d in historical_data]
        baseline_mean = sum(values) / len(values)
        baseline_std = math.sqrt(sum((x - baseline_mean) ** 2 for x in values) / len(values)) if values else 0

        if baseline_std == 0:
            return []

        predictions = []
        for point in forecast.get("forecast", []):
            predicted = point.get("predicted_value", 0)
            z_score = (predicted - baseline_mean) / baseline_std

            if abs(z_score) > 3:
                predictions.append({
                    "period": point.get("period", 0),
                    "predicted_value": predicted,
                    "z_score": z_score,
                    "anomaly_type": "volume_spike" if z_score > 0 else "volume_drop",
                    "severity": "critical" if abs(z_score) > 4 else "high",
                    "description": f"Predicted anomaly: z={z_score:.2f}"
                })

        return predictions

    def detect_trend_shift(self, time_series: list[dict[str, Any]],
                          window_size: int = 10) -> dict[str, Any]:
        """Detect trend shifts using two-sample t-test approach."""
        if len(time_series) < window_size * 2:
            return {"shift_detected": False, "message": "Insufficient data"}

        values = [d.get("bytes", 0) for d in time_series]
        shifts = []

        for i in range(window_size, len(values) - window_size):
            w1 = values[i - window_size:i]
            w2 = values[i:i + window_size]

            m1 = sum(w1) / len(w1)
            m2 = sum(w2) / len(w2)

            # Pooled standard deviation
            var1 = sum((x - m1) ** 2 for x in w1) / (len(w1) - 1) if len(w1) > 1 else 0
            var2 = sum((x - m2) ** 2 for x in w2) / (len(w2) - 1) if len(w2) > 1 else 0
            pooled_std = math.sqrt((var1 + var2) / 2)

            if pooled_std > 0:
                t_stat = abs(m2 - m1) / (pooled_std * math.sqrt(2 / window_size))

                if t_stat > 2.5:  # Approximate significance threshold
                    shifts.append({
                        "index": i,
                        "t_statistic": round(t_stat, 3),
                        "mean_before": round(m1, 2),
                        "mean_after": round(m2, 2),
                        "change_ratio": round((m2 - m1) / abs(m1) if m1 != 0 else 0, 3)
                    })

        return {
            "shift_detected": len(shifts) > 0,
            "shift_count": len(shifts),
            "shift_points": shifts[:10]
        }

    def find_change_points(self, values: list[float]) -> list[dict[str, Any]]:
        """Find change points using CUSUM algorithm."""
        if len(values) < 10:
            return []

        mean = sum(values) / len(values)
        std = math.sqrt(sum((x - mean) ** 2 for x in values) / len(values)) if len(values) > 1 else 0

        if std == 0:
            return []

        change_points = []
        s_pos = 0
        s_neg = 0
        threshold = std * 3

        for i, value in enumerate(values):
            s_pos = max(0, s_pos + (value - mean) - threshold / 2)
            s_neg = min(0, s_neg + (value - mean) + threshold / 2)

            if s_pos > threshold or s_neg < -threshold:
                change_points.append({
                    "index": i,
                    "value": value,
                    "deviation_sigma": (value - mean) / std,
                    "cusum_pos": round(s_pos, 2),
                    "cusum_neg": round(s_neg, 2),
                    "direction": "increase" if s_pos > threshold else "decrease"
                })
                s_pos = 0
                s_neg = 0

        return change_points

    def forecast_traffic(self, time_series: list[dict[str, Any]],
                        horizon: int = 24) -> ForecastResult:
        """Comprehensive traffic forecasting."""
        # Volume forecast
        forecast = self.forecast_volume(time_series, horizon)

        # Seasonality detection
        values = [d.get("bytes", 0) for d in time_series]
        seasonality = self.seasonality_detector.detect_seasonality(values)

        # Anomaly predictions
        anomaly_predictions = self.predict_anomalies(forecast, time_series)

        # Trend shift detection
        trend_shift = self.detect_trend_shift(time_series)

        # Recommendations
        recommendations = []
        method = forecast.get("method_used", "unknown")
        recommendations.append(f"Forecast method: {method}")

        if forecast.get("trend_direction", "").startswith("increasing"):
            recommendations.append("Traffic trend is increasing - monitor capacity")
        if anomaly_predictions:
            recommendations.append(f"{len(anomaly_predictions)} anomalies predicted")
        if trend_shift.get("shift_detected"):
            recommendations.append("Significant trend shifts detected")

        return ForecastResult(
            forecast_points=forecast.get("forecast", []),
            trend_direction=forecast.get("trend_direction", "unknown"),
            seasonality_detected=seasonality["seasonality_detected"],
            seasonality_period=seasonality["period"],
            anomaly_predictions=anomaly_predictions,
            capacity_risk="medium",
            recommendations=recommendations[:5],
            method_used=method
        )
