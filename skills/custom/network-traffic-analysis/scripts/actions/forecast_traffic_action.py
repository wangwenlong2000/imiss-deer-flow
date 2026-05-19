"""
Traffic Forecasting Action

Action handler for traffic volume forecasting and trend analysis.
"""

from __future__ import annotations

from typing import Any

from actions.advanced_action_common import append_file_errors, fetch_rows, format_dict_rows
from analysis.forecasting import TrafficForecaster
from core.schema_mapping import analysis_time_bucket_expr


def execute_forecast_analysis(
    con: Any,
    mappings: dict[str, dict[str, str]],
    where_clause: str,
    files: list[str],
    **kwargs,
) -> dict:
    horizon = kwargs.get("horizon", 24)
    interval = kwargs.get("interval", "hour")

    results = {
        "action": "forecast-traffic",
        "files_analyzed": [],
        "summary": {
            "total_time_series_analyzed": 0,
            "forecasts_generated": 0,
            "anomalies_predicted": 0,
            "trend_shifts_detected": 0,
        },
        "forecasts": [],
    }

    forecaster = TrafficForecaster()
    file_result = {
        "file": files[0] if files else "selected scope",
        "forecast_summary": "",
        "trend_direction": "unknown",
        "anomaly_predictions": [],
        "recommendations": [],
    }

    try:
        time_bucket = analysis_time_bucket_expr(interval)
        query = f"""
            SELECT
                {time_bucket} AS timestamp,
                SUM(COALESCE(bytes, 0)) AS bytes,
                SUM(COALESCE(packets, 0)) AS packets,
                COUNT(*) AS flow_count
            FROM flows
            {where_clause}
            GROUP BY 1
            HAVING timestamp != 'unknown'
            ORDER BY 1
        """
        time_series = fetch_rows(con, query)
        if not time_series:
            file_result["forecast_summary"] = "No time series data available"
            results["files_analyzed"].append(file_result)
            return results

        results["summary"]["total_time_series_analyzed"] += len(time_series)
        forecast_result = forecaster.forecast_traffic(time_series, horizon)
        trend_shift = forecaster.detect_trend_shift(time_series)
        file_result["trend_direction"] = forecast_result.trend_direction
        file_result["forecast_summary"] = (
            f"Trend: {forecast_result.trend_direction} | "
            f"Seasonality: {'Yes' if forecast_result.seasonality_detected else 'No'} | "
            f"Capacity Risk: {forecast_result.capacity_risk}"
        )
        file_result["forecast_points"] = forecast_result.forecast_points[: min(horizon, 10)]
        file_result["anomaly_predictions"] = forecast_result.anomaly_predictions
        file_result["recommendations"] = forecast_result.recommendations

        results["summary"]["forecasts_generated"] += 1
        results["summary"]["anomalies_predicted"] += len(forecast_result.anomaly_predictions)
        results["summary"]["trend_shifts_detected"] += int(trend_shift.get("shift_count", 0))
        results["files_analyzed"].append(file_result)
        results["forecasts"].append(file_result)
    except Exception as e:
        file_result["error"] = str(e)
        results["files_analyzed"].append(file_result)

    return results


def format_results(results: dict) -> str:
    output = []
    output.append("# Traffic Forecasting Results\n")

    summary = results["summary"]
    output.append("## Summary\n")
    output.append(f"- **Total Time Series Analyzed**: {summary['total_time_series_analyzed']}")
    output.append(f"- **Forecasts Generated**: {summary['forecasts_generated']}")
    output.append(f"- **Anomalies Predicted**: {summary['anomalies_predicted']}")
    output.append(f"- **Trend Shifts Detected**: {summary['trend_shifts_detected']}\n")

    for file_result in results["files_analyzed"]:
        output.append(f"\n## File: {file_result['file']}\n")
        output.append(f"**{file_result['forecast_summary']}**\n")
        if file_result.get("forecast_points"):
            output.append("\n### Forecast Points\n")
            output.append(format_dict_rows(file_result["forecast_points"]))
        if file_result.get("anomaly_predictions"):
            output.append("### Predicted Anomalies\n")
            output.append(format_dict_rows(file_result["anomaly_predictions"]))
        if file_result.get("recommendations"):
            output.append("\n### Recommendations\n")
            for rec in file_result["recommendations"]:
                output.append(f"- {rec}")

    append_file_errors(output, results)
    return "\n".join(output)


def build_skill_result_parts(results: dict, raw_output: str) -> dict[str, Any]:
    summary = results.get("summary", {})
    forecasts = results.get("forecasts", [])
    errors = [
        {"file": item.get("file", "selected scope"), "error": item["error"]}
        for item in results.get("files_analyzed", [])
        if item.get("error")
    ]

    forecast_points = []
    anomaly_predictions = []
    for forecast in forecasts:
        forecast_points.extend(forecast.get("forecast_points") or [])
        anomaly_predictions.extend(forecast.get("anomaly_predictions") or [])

    findings: list[dict[str, Any]] = []
    for forecast_index, forecast in enumerate(forecasts, 1):
        capacity_risk = str(forecast.get("forecast_summary", "")).lower()
        if "high" in capacity_risk or "medium" in capacity_risk or forecast.get("anomaly_predictions"):
            severity = "high" if "high" in capacity_risk else "medium"
            evidence_refs = ["e-forecast-points"] if forecast_points else ["e-raw-report"]
            if anomaly_predictions:
                evidence_refs.append("e-forecast-anomalies")
            findings.append(
                {
                    "finding_id": f"f-forecast-{forecast_index:03d}",
                    "type": "traffic_forecast_risk",
                    "severity": severity,
                    "confidence": 0.7,
                    "title": f"Forecast risk for {forecast.get('file', 'selected scope')}",
                    "description": forecast.get("forecast_summary", "Forecast generated for selected traffic series."),
                    "entities": [{"type": "file", "value": forecast.get("file", "selected scope")}],
                    "evidence_refs": evidence_refs,
                    "recommended_actions": forecast.get("recommendations", [])[:3],
                }
            )

    evidence: list[dict[str, Any]] = []
    if forecast_points:
        evidence.append(
            {
                "evidence_id": "e-forecast-points",
                "type": "timeseries",
                "title": "Forecast Points",
                "content": forecast_points,
            }
        )
    if anomaly_predictions:
        evidence.append(
            {
                "evidence_id": "e-forecast-anomalies",
                "type": "table",
                "title": "Predicted Anomalies",
                "columns": list(anomaly_predictions[0].keys()),
                "rows": anomaly_predictions,
            }
        )
    evidence.append(
        {
            "evidence_id": "e-raw-report",
            "type": "text",
            "title": "Raw Traffic Forecast Report",
            "content": raw_output,
        }
    )

    return {
        "summary": {
            "title": "Traffic Forecast",
            "overview": (
                f"Generated {summary.get('forecasts_generated', 0)} forecast(s) from "
                f"{summary.get('total_time_series_analyzed', 0)} time buckets."
            ),
            "severity": (
                "high"
                if any("high" in str(item.get("forecast_summary", "")).lower() for item in forecasts)
                else "medium"
                if summary.get("anomalies_predicted", 0)
                or any("medium" in str(item.get("forecast_summary", "")).lower() for item in forecasts)
                else "info"
            ),
            "confidence": 0.7,
            "key_metrics": [
                {"name": "time_buckets_analyzed", "value": summary.get("total_time_series_analyzed", 0)},
                {"name": "forecasts_generated", "value": summary.get("forecasts_generated", 0)},
                {"name": "anomalies_predicted", "value": summary.get("anomalies_predicted", 0)},
                {"name": "trend_shifts_detected", "value": summary.get("trend_shifts_detected", 0)},
            ],
        },
        "findings": findings,
        "evidence": evidence,
        "diagnostics": {
            "warnings": [
                "Forecast output is model-based projection over the selected scope; validate with later observations before operational action."
            ],
            "data_quality": {
                "files_with_errors": len(errors),
                "forecast_points_returned": len(forecast_points),
            },
            "errors": errors,
        },
    }
