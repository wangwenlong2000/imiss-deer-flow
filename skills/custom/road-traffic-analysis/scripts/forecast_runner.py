#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any


try:
    import pandas as pd
except ImportError:
    os.system(f"{sys.executable} -m pip install pandas -q")
    import pandas as pd

try:
    from statsforecast import StatsForecast
    from statsforecast.models import AutoARIMA, AutoETS, MSTL, SeasonalNaive
except ImportError:
    os.system(f"{sys.executable} -m pip install statsforecast -q")
    from statsforecast import StatsForecast
    from statsforecast.models import AutoARIMA, AutoETS, MSTL, SeasonalNaive


MODEL_CATALOG: dict[str, dict[str, str]] = {
    "seasonal_naive": {
        "display_name": "SeasonalNaive",
        "best_for": "强周期且稳定的短期基线预测，速度最快。",
        "notes": "作为稳定基线模型，适合先验不充分场景。",
    },
    "autoets": {
        "display_name": "AutoETS",
        "best_for": "趋势+季节性明显、需要可解释指数平滑结构的序列。",
        "notes": "对平滑变化效果好。",
    },
    "autoarima": {
        "display_name": "AutoARIMA",
        "best_for": "自相关结构明显、样本量中等且需要稳健统计建模。",
        "notes": "通常在规则时间序列上表现稳定。",
    },
    "mstl": {
        "display_name": "MSTL",
        "best_for": "存在多季节性或复杂周期模式的序列。",
        "notes": "适合小时/日多层周期特征。",
    },
}

MODEL_TO_COLUMN = {
    "seasonal_naive": "SeasonalNaive",
    "autoets": "AutoETS",
    "autoarima": "AutoARIMA",
    "mstl": "MSTL",
}


def _normalize_mapping_value(value: Any) -> str:
    return str(value or "").strip()


def repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def outputs_dir() -> Path:
    return (repo_root() / "datasets" / "road-traffic" / "outputs").resolve()


def _is_explicit_path_reference(value: str) -> bool:
    reference = value.strip()
    normalized = reference.replace("\\", "/")
    return (
        normalized.startswith("/")
        or normalized.startswith("./")
        or normalized.startswith("../")
        or bool(re.match(r"^[A-Za-z]:[/\\]", reference))
        or "/" in normalized
        or "\\" in reference
    )


def resolve_csv_reference(reference: str) -> Path:
    raw = str(reference or "").strip()
    if not raw:
        raise ValueError("Missing CSV reference.")

    direct = Path(raw).expanduser()
    if direct.exists():
        return direct.resolve()

    if _is_explicit_path_reference(raw):
        raise FileNotFoundError(f"CSV path does not exist: {raw}")

    candidates: list[Path] = []
    roots = [
        Path("/mnt/user-data/uploads"),
        (repo_root() / "datasets" / "road-traffic" / "processed"),
        (repo_root() / "datasets" / "road-traffic" / "raw"),
    ]

    for root in roots:
        if not root.exists():
            continue
        exact = root / raw
        if exact.exists() and exact.is_file() and exact.suffix.lower() == ".csv":
            candidates.append(exact.resolve())
            continue
        for file_path in root.rglob("*.csv"):
            if file_path.name == raw or file_path.as_posix().endswith(raw.replace("\\", "/")):
                candidates.append(file_path.resolve())

    deduped: list[Path] = []
    seen: set[str] = set()
    for item in candidates:
        key = item.as_posix()
        if key not in seen:
            deduped.append(item)
            seen.add(key)

    if not deduped:
        raise FileNotFoundError(
            f"CSV reference '{raw}' was not found under /mnt/user-data/uploads or datasets/road-traffic."
        )
    if len(deduped) > 1:
        sample = "\n".join(f"  - {item.as_posix()}" for item in deduped[:10])
        raise ValueError(
            f"CSV reference '{raw}' matched multiple files. Use a more specific path.\nCandidates:\n{sample}"
        )
    return deduped[0]


def parse_history_values(history: str) -> list[float]:
    if not history.strip():
        raise ValueError("--history is empty.")
    items = [token.strip() for token in re.split(r"[,\s]+", history.strip()) if token.strip()]
    if not items:
        raise ValueError("--history has no numeric values.")
    try:
        values = [float(item) for item in items]
    except ValueError as exc:
        raise ValueError(f"--history contains non-numeric value: {exc}") from exc
    return values


def dataset_preview_from_csv(path: Path, head: int) -> dict[str, Any]:
    frame = pd.read_csv(path)
    summary = {
        "source": path.as_posix(),
        "rows": int(len(frame)),
        "columns": list(frame.columns),
        "dtypes": {col: str(dtype) for col, dtype in frame.dtypes.items()},
        "null_ratio": {
            col: round(float(frame[col].isna().mean()), 6) for col in frame.columns
        },
        "head": frame.head(head).to_dict(orient="records"),
    }
    return summary


def dataset_preview_from_history(values: list[float], head: int, freq: str) -> dict[str, Any]:
    head_values = values[:head]
    summary = {
        "source": "inline-history",
        "rows": len(values),
        "columns": ["ds", "y"],
        "dtypes": {"ds": "datetime64[ns]", "y": "float64"},
        "null_ratio": {"ds": 0.0, "y": 0.0},
        "head": [
            {
                "ds": str(timestamp),
                "y": value,
            }
            for timestamp, value in zip(
                pd.date_range(start="2024-01-01", periods=len(head_values), freq=freq),
                head_values,
            )
        ],
    }
    return summary


def normalize_forecast_frame(
    frame: pd.DataFrame,
    *,
    timestamp_col: str,
    value_col: str,
    series_id_col: str | None,
) -> pd.DataFrame:
    if timestamp_col not in frame.columns:
        raise ValueError(f"Missing timestamp column: {timestamp_col}")
    if value_col not in frame.columns:
        raise ValueError(f"Missing value column: {value_col}")

    normalized = pd.DataFrame()
    normalized["ds"] = pd.to_datetime(frame[timestamp_col], errors="coerce")
    normalized["y"] = pd.to_numeric(frame[value_col], errors="coerce")

    if series_id_col:
        if series_id_col not in frame.columns:
            raise ValueError(f"Missing series id column: {series_id_col}")
        normalized["unique_id"] = frame[series_id_col].astype(str)
    else:
        normalized["unique_id"] = "series_1"

    normalized = normalized.dropna(subset=["ds", "y", "unique_id"]).copy()
    if normalized.empty:
        raise ValueError("No valid rows remained after parsing timestamp/value columns.")

    normalized["ds"] = pd.to_datetime(normalized["ds"]).dt.tz_localize(None)
    normalized = normalized.sort_values(["unique_id", "ds"]).reset_index(drop=True)
    return normalized[["unique_id", "ds", "y"]]


def normalize_forecast_frame_from_mapping(
    frame: pd.DataFrame,
    mapping: dict[str, Any],
) -> tuple[pd.DataFrame, dict[str, str]]:
    timestamp_col = _normalize_mapping_value(mapping.get("timestamp_col"))
    value_col = _normalize_mapping_value(mapping.get("value_col"))
    series_id_col = _normalize_mapping_value(mapping.get("series_id_col"))
    if not timestamp_col or not value_col:
        raise ValueError(
            "CSV forecasting requires explicit mapping for timestamp_col and value_col. "
            "Inspect the CSV first, then pass mapped columns explicitly."
        )

    normalized = normalize_forecast_frame(
        frame,
        timestamp_col=timestamp_col,
        value_col=value_col,
        series_id_col=series_id_col or None,
    )
    mapping_used = {
        "timestamp_col": timestamp_col,
        "value_col": value_col,
        "series_id_col": series_id_col,
    }
    return normalized, mapping_used


def load_and_normalize_csv(
    csv_reference: str,
    mapping: dict[str, Any],
) -> tuple[Path, pd.DataFrame, dict[str, str]]:
    csv_path = resolve_csv_reference(csv_reference)
    raw = pd.read_csv(csv_path)
    normalized, mapping_used = normalize_forecast_frame_from_mapping(raw, mapping)
    return csv_path, normalized, mapping_used


def history_to_forecast_frame(values: list[float], freq: str) -> pd.DataFrame:
    timestamps = pd.date_range(start="2024-01-01", periods=len(values), freq=freq)
    return pd.DataFrame({"unique_id": "series_1", "ds": timestamps, "y": values})


def infer_freq(frame: pd.DataFrame, fallback_freq: str) -> str:
    if fallback_freq.strip():
        return fallback_freq.strip()

    ds = pd.to_datetime(frame["ds"]).sort_values().dropna().drop_duplicates()
    inferred = pd.infer_freq(ds)
    if inferred:
        return inferred

    if len(ds) >= 2:
        delta = ds.iloc[1] - ds.iloc[0]
        if delta.total_seconds() <= 60:
            return "min"
        if delta.total_seconds() <= 3600:
            return "h"
        return "D"

    return "h"


def smape(y_true: pd.Series, y_pred: pd.Series) -> float:
    denominator = (y_true.abs() + y_pred.abs()).replace(0, pd.NA)
    ratio = (2.0 * (y_true - y_pred).abs() / denominator).dropna()
    if ratio.empty:
        return 999.0
    return float(ratio.mean() * 100.0)


def _build_single_model(model_key: str, seasonal_length: int):
    if model_key == "seasonal_naive":
        return SeasonalNaive(season_length=seasonal_length)
    if model_key == "autoets":
        return AutoETS(season_length=seasonal_length)
    if model_key == "autoarima":
        return AutoARIMA(season_length=seasonal_length)
    if model_key == "mstl":
        return MSTL(season_length=[seasonal_length])
    raise ValueError(f"Unsupported model key: {model_key}")


def forecast_with_seasonal_naive(df: pd.DataFrame, *, freq: str, horizon: int, seasonal_length: int) -> pd.DataFrame:
    return run_forecast_with_model(df, "seasonal_naive", freq=freq, horizon=horizon, seasonal_length=seasonal_length)


def forecast_with_autoets(df: pd.DataFrame, *, freq: str, horizon: int, seasonal_length: int) -> pd.DataFrame:
    return run_forecast_with_model(df, "autoets", freq=freq, horizon=horizon, seasonal_length=seasonal_length)


def forecast_with_autoarima(df: pd.DataFrame, *, freq: str, horizon: int, seasonal_length: int) -> pd.DataFrame:
    return run_forecast_with_model(df, "autoarima", freq=freq, horizon=horizon, seasonal_length=seasonal_length)


def forecast_with_mstl(df: pd.DataFrame, *, freq: str, horizon: int, seasonal_length: int) -> pd.DataFrame:
    return run_forecast_with_model(df, "mstl", freq=freq, horizon=horizon, seasonal_length=seasonal_length)


def evaluate_models_for_auto(df: pd.DataFrame, *, freq: str, horizon: int, seasonal_length: int) -> dict[str, float]:
    models = [
        SeasonalNaive(season_length=seasonal_length),
        AutoETS(season_length=seasonal_length),
        AutoARIMA(season_length=seasonal_length),
        MSTL(season_length=[seasonal_length]),
    ]
    sf = StatsForecast(models=models, freq=freq, n_jobs=1)
    cv = sf.cross_validation(df=df, h=horizon, n_windows=1)

    scores: dict[str, float] = {}
    for model_key, prediction_col in MODEL_TO_COLUMN.items():
        if prediction_col not in cv.columns:
            continue
        valid = cv[["y", prediction_col]].dropna()
        if valid.empty:
            continue
        score = smape(valid["y"], valid[prediction_col])
        scores[model_key] = round(score, 6)
    if not scores:
        raise ValueError("Auto model selection failed: no valid cross-validation scores.")
    return scores


def forecast_with_auto(
    df: pd.DataFrame,
    *,
    freq: str,
    horizon: int,
    seasonal_length: int,
) -> tuple[str, dict[str, float], pd.DataFrame]:
    model_scores = evaluate_models_for_auto(
        df,
        freq=freq,
        horizon=horizon,
        seasonal_length=seasonal_length,
    )
    selected_model = min(model_scores.items(), key=lambda kv: kv[1])[0]
    result = run_forecast_with_model(
        df,
        selected_model,
        freq=freq,
        horizon=horizon,
        seasonal_length=seasonal_length,
    )
    return selected_model, model_scores, result


def run_forecast_with_model(
    df: pd.DataFrame,
    model_key: str,
    *,
    freq: str,
    horizon: int,
    seasonal_length: int,
) -> pd.DataFrame:
    model = _build_single_model(model_key, seasonal_length=seasonal_length)
    sf = StatsForecast(models=[model], freq=freq, n_jobs=1)
    predicted = sf.forecast(df=df, h=horizon)
    prediction_col = MODEL_TO_COLUMN[model_key]
    if prediction_col not in predicted.columns:
        raise ValueError(f"Forecast output column not found: {prediction_col}")
    result = predicted[["unique_id", "ds", prediction_col]].copy()
    result = result.rename(columns={prediction_col: "yhat"})
    return result


def validate_data_sufficiency(df: pd.DataFrame, *, horizon: int) -> dict[str, Any]:
    per_series = df.groupby("unique_id", sort=False).size()
    min_points = int(per_series.min())
    max_points = int(per_series.max())
    recommendation = "sufficient"
    if min_points < max(8, horizon * 2):
        recommendation = "insufficient"
    return {
        "series_count": int(per_series.shape[0]),
        "min_points_per_series": min_points,
        "max_points_per_series": max_points,
        "recommendation": recommendation,
        "advice": "建议每个序列至少提供 2*h 且不低于 8 个时间点。",
    }


def build_model_payload() -> dict[str, Any]:
    return {
        "models": [
            {
                "model_key": key,
                **spec,
            }
            for key, spec in MODEL_CATALOG.items()
        ]
    }


def to_text_preview(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)


def build_forecast_payload(
    frame: pd.DataFrame,
    *,
    source: str,
    model_requested: str,
    horizon: int,
    freq_hint: str,
    seasonal_length_requested: int,
    output_file: str = "",
    csv_mapping: dict[str, str] | None = None,
) -> dict[str, Any]:
    freq = infer_freq(frame, freq_hint)
    sufficiency = validate_data_sufficiency(frame, horizon=horizon)
    effective_seasonal_length = max(
        2,
        min(
            int(seasonal_length_requested),
            max(2, int(sufficiency["min_points_per_series"]) - 1),
        ),
    )

    model_scores: dict[str, float] | None = None
    selected_model = model_requested
    auto_selection_note = ""
    if model_requested == "auto":
        try:
            selected_model, model_scores, predicted = forecast_with_auto(
                frame,
                freq=freq,
                horizon=horizon,
                seasonal_length=effective_seasonal_length,
            )
        except Exception as exc:
            selected_model = "seasonal_naive"
            auto_selection_note = (
                "auto model evaluation failed; fallback to seasonal_naive. "
                f"reason: {exc}"
            )
            predicted = run_forecast_with_model(
                frame,
                selected_model,
                freq=freq,
                horizon=horizon,
                seasonal_length=effective_seasonal_length,
            )
    else:
        predicted = run_forecast_with_model(
            frame,
            selected_model,
            freq=freq,
            horizon=horizon,
            seasonal_length=effective_seasonal_length,
        )

    predicted_display = predicted.copy()
    predicted_display["ds"] = pd.to_datetime(predicted_display["ds"]).dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    if output_file:
        target = Path(output_file).expanduser()
        target.parent.mkdir(parents=True, exist_ok=True)
        predicted_display.to_csv(target, index=False)
        output_path = target.resolve().as_posix()
    else:
        outputs_dir().mkdir(parents=True, exist_ok=True)
        default_name = f"traffic_forecast_{selected_model}.csv"
        target = outputs_dir() / default_name
        predicted_display.to_csv(target, index=False)
        output_path = target.resolve().as_posix()

    return {
        "ok": True,
        "action": "forecast",
        "source": source,
        "csv_mapping": csv_mapping,
        "model_requested": model_requested,
        "model_selected": selected_model,
        "model_scores": model_scores,
        "auto_selection_note": auto_selection_note or None,
        "freq": freq,
        "horizon": horizon,
        "seasonal_length_requested": seasonal_length_requested,
        "seasonal_length_effective": effective_seasonal_length,
        "sufficiency": sufficiency,
        "rows_input": int(len(frame)),
        "rows_forecast": int(len(predicted)),
        "time_range": {
            "start": pd.to_datetime(frame["ds"].min()).strftime("%Y-%m-%d %H:%M:%S"),
            "end": pd.to_datetime(frame["ds"].max()).strftime("%Y-%m-%d %H:%M:%S"),
        },
        "preview_forecast": predicted_display.head(20).to_dict(orient="records"),
        "output_file": output_path,
    }


def run_csv_forecast_with_mapping(
    csv_reference: str,
    *,
    mapping: dict[str, Any],
    model: str,
    horizon: int,
    freq: str,
    seasonal_length: int,
    output_file: str = "",
) -> dict[str, Any]:
    csv_path, normalized, mapping_used = load_and_normalize_csv(csv_reference, mapping)
    return build_forecast_payload(
        normalized,
        source=csv_path.as_posix(),
        model_requested=model,
        horizon=horizon,
        freq_hint=freq,
        seasonal_length_requested=seasonal_length,
        output_file=output_file,
        csv_mapping=mapping_used,
    )


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Road traffic forecast runner (single-script version).")
    p.add_argument("--action", required=True, choices=["models", "inspect", "forecast"], help="Action name")
    p.add_argument("--file", default="", help="CSV path/reference")
    p.add_argument("--history", default="", help="Inline numeric history, e.g. '120,130,128,140'")
    p.add_argument("--timestamp-col", default="", help="Timestamp column name for CSV (required for CSV forecast)")
    p.add_argument("--value-col", default="", help="Numeric target column name for CSV (required for CSV forecast)")
    p.add_argument("--series-id-col", default="", help="Series id column for multi-series CSV")
    p.add_argument("--model", default="auto", choices=["auto", "seasonal_naive", "autoets", "autoarima", "mstl"], help="Forecast model")
    p.add_argument("--horizon", type=int, default=12, help="Forecast horizon")
    p.add_argument("--freq", default="", help="Pandas/StatsForecast freq alias, e.g. h/15min/D")
    p.add_argument("--seasonal-length", type=int, default=24, help="Seasonal length")
    p.add_argument("--head", type=int, default=100, help="Preview rows for inspect")
    p.add_argument("--output-file", default="", help="Optional CSV export path")
    p.add_argument("--format", choices=["text", "json"], default="text", help="Output format")
    return p


def main() -> int:
    args = parser().parse_args()

    try:
        if args.action == "models":
            payload = {
                "ok": True,
                "action": "models",
                **build_model_payload(),
            }
            print(to_text_preview(payload) if args.format == "text" else json.dumps(payload, ensure_ascii=False))
            return 0

        if args.action == "inspect":
            if args.file:
                csv_path = resolve_csv_reference(args.file)
                preview = dataset_preview_from_csv(csv_path, head=args.head)
                payload = {
                    "ok": True,
                    "action": "inspect",
                    "mode": "csv",
                    "preview": preview,
                }
            elif args.history:
                freq = args.freq.strip() or "h"
                values = parse_history_values(args.history)
                preview = dataset_preview_from_history(values, head=args.head, freq=freq)
                payload = {
                    "ok": True,
                    "action": "inspect",
                    "mode": "history",
                    "preview": preview,
                }
            else:
                raise ValueError("inspect action requires --file or --history")

            print(to_text_preview(payload) if args.format == "text" else json.dumps(payload, ensure_ascii=False))
            return 0

        if args.action == "forecast":
            if args.file:
                payload = run_csv_forecast_with_mapping(
                    args.file,
                    mapping={
                        "timestamp_col": args.timestamp_col,
                        "value_col": args.value_col,
                        "series_id_col": args.series_id_col,
                    },
                    model=args.model,
                    horizon=args.horizon,
                    freq=args.freq,
                    seasonal_length=args.seasonal_length,
                    output_file=args.output_file,
                )
            elif args.history:
                values = parse_history_values(args.history)
                base_freq = args.freq.strip() or "h"
                frame = history_to_forecast_frame(values, freq=base_freq)
                payload = build_forecast_payload(
                    frame,
                    source="inline-history",
                    model_requested=args.model,
                    horizon=args.horizon,
                    freq_hint=args.freq,
                    seasonal_length_requested=args.seasonal_length,
                    output_file=args.output_file,
                    csv_mapping=None,
                )
            else:
                raise ValueError("forecast action requires --file or --history")

            print(to_text_preview(payload) if args.format == "text" else json.dumps(payload, ensure_ascii=False))
            return 0

        raise ValueError(f"Unsupported action: {args.action}")
    except Exception as exc:
        payload = {
            "ok": False,
            "action": args.action,
            "error": str(exc),
        }
        print(to_text_preview(payload) if args.format == "text" else json.dumps(payload, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
