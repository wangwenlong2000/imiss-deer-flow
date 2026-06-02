#!/usr/bin/env python3
from __future__ import annotations

import argparse
import inspect
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
    from adtk.data import validate_series
    from adtk.detector import (
        LevelShiftAD,
        PcaAD,
        PersistAD,
        SeasonalAD,
        ThresholdAD,
        VolatilityShiftAD,
    )
except ImportError:
    os.system(f"{sys.executable} -m pip install adtk -q")
    from adtk.data import validate_series
    from adtk.detector import (
        LevelShiftAD,
        PcaAD,
        PersistAD,
        SeasonalAD,
        ThresholdAD,
        VolatilityShiftAD,
    )


DETECTOR_CATALOG: dict[str, dict[str, Any]] = {
    "seasonal_ad": {
        "display_name": "SeasonalAD",
        "best_for": "Clear seasonality where outliers deviate from periodic pattern.",
        "notes": "Key params: seasonal_period/freq, c, side.",
        "requires": [],
    },
    "persist_ad": {
        "display_name": "PersistAD",
        "best_for": "Sudden point-level jumps compared with recent window.",
        "notes": "Key params: window, c, side.",
        "requires": [],
    },
    "level_shift_ad": {
        "display_name": "LevelShiftAD",
        "best_for": "Regime shift where baseline moves to a new level.",
        "notes": "Key params: window, c, side.",
        "requires": [],
    },
    "volatility_shift_ad": {
        "display_name": "VolatilityShiftAD",
        "best_for": "Variance/spread shift without obvious mean change.",
        "notes": "Key params: window, c, side.",
        "requires": [],
    },
    "threshold_ad": {
        "display_name": "ThresholdAD",
        "best_for": "Business rule anomalies with explicit numeric boundaries.",
        "notes": "Requires threshold_low and/or threshold_high.",
        "requires": ["threshold_low or threshold_high"],
    },
    "pca_ad": {
        "display_name": "PcaAD",
        "best_for": "Multivariate anomaly detection across aligned series.",
        "notes": "Needs at least 2 aligned series and enough aligned timestamps.",
        "requires": ["multi-series data", "aligned timestamps"],
    },
}

DETECTOR_TO_COLUMN = {
    "seasonal_ad": "seasonal_ad",
    "persist_ad": "persist_ad",
    "level_shift_ad": "level_shift_ad",
    "volatility_shift_ad": "volatility_shift_ad",
    "threshold_ad": "threshold_ad",
    "pca_ad": "pca_ad",
}

DETECTOR_ALIAS = {
    "seasonalad": "seasonal_ad",
    "seasonal_ad": "seasonal_ad",
    "seasonal": "seasonal_ad",
    "persistad": "persist_ad",
    "persist_ad": "persist_ad",
    "persist": "persist_ad",
    "levelshiftad": "level_shift_ad",
    "level_shift_ad": "level_shift_ad",
    "levelshift": "level_shift_ad",
    "level_shift": "level_shift_ad",
    "volatilityshiftad": "volatility_shift_ad",
    "volatility_shift_ad": "volatility_shift_ad",
    "volatilityshift": "volatility_shift_ad",
    "volatility_shift": "volatility_shift_ad",
    "thresholdad": "threshold_ad",
    "threshold_ad": "threshold_ad",
    "threshold": "threshold_ad",
    "pcaad": "pca_ad",
    "pca_ad": "pca_ad",
    "pca": "pca_ad",
}


def repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def outputs_dir() -> Path:
    return (repo_root() / "datasets" / "road-traffic" / "outputs").resolve()


def _safe_slug(text: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", str(text or "").strip())
    slug = slug.strip("_").lower()
    return slug or "anomaly"


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


def _normalize_mapping_value(value: Any) -> str:
    return str(value or "").strip()


def normalize_anomaly_frame(
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


def normalize_anomaly_frame_from_mapping(
    frame: pd.DataFrame,
    mapping: dict[str, Any],
) -> tuple[pd.DataFrame, dict[str, str]]:
    timestamp_col = _normalize_mapping_value(mapping.get("timestamp_col"))
    value_col = _normalize_mapping_value(mapping.get("value_col"))
    series_id_col = _normalize_mapping_value(mapping.get("series_id_col"))
    if not timestamp_col or not value_col:
        raise ValueError(
            "CSV anomaly detection requires explicit mapping for timestamp_col and value_col. "
            "Inspect the CSV first, then pass mapped columns explicitly."
        )

    normalized = normalize_anomaly_frame(
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
    normalized, mapping_used = normalize_anomaly_frame_from_mapping(raw, mapping)
    return csv_path, normalized, mapping_used


def history_to_anomaly_frame(values: list[float], freq: str) -> pd.DataFrame:
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


def _infer_default_seasonal_period(freq: str) -> int:
    text = str(freq or "").strip().lower()
    if text in {"h", "1h", "hour", "hourly"}:
        return 24
    if text in {"d", "1d", "day", "daily"}:
        return 7
    if text in {"w", "1w", "week", "weekly"}:
        return 52
    if text in {"m", "1m", "month", "monthly", "ms"}:
        return 12
    if "min" in text:
        return 60
    return 24


def _to_mask(result, index):
    if isinstance(result, pd.DataFrame):
        mask = result.any(axis=1)
    elif isinstance(result, pd.Series):
        mask = result
    else:
        mask = pd.Series(False, index=index)

    if not isinstance(mask, pd.Series):
        mask = pd.Series(mask, index=index)

    mask = mask.reindex(index)
    mask = mask.fillna(False).astype(bool)
    return mask


def _to_optional_float(value: Any) -> float | None:
    if value in (None, "", "none", "null"):
        return None
    try:
        return float(value)
    except Exception:
        return None


def validate_data_sufficiency(df: pd.DataFrame, *, min_points: int) -> dict[str, Any]:
    per_series = df.groupby("unique_id", sort=False).size()
    min_points_series = int(per_series.min())
    max_points_series = int(per_series.max())
    recommendation = "sufficient"
    if min_points_series < int(min_points):
        recommendation = "insufficient"
    return {
        "series_count": int(per_series.shape[0]),
        "min_points_per_series": min_points_series,
        "max_points_per_series": max_points_series,
        "required_min_points": int(min_points),
        "recommendation": recommendation,
        "advice": "Per series points should be >= min_points before anomaly detection.",
    }


def _parse_side(side: str) -> str:
    text = str(side or "both").strip().lower()
    if text not in {"both", "positive", "negative"}:
        return "both"
    return text


def _normalize_detector_key(token: str) -> str | None:
    raw = str(token or "").strip()
    if not raw:
        return None
    key = raw.replace("-", "_").replace(" ", "").lower()
    return DETECTOR_ALIAS.get(key)


def _select_detectors(
    *,
    detector: str,
    detectors_text: str,
    use_pca: bool,
    threshold_low: float | None,
    threshold_high: float | None,
) -> list[str]:
    selected: list[str] = []

    if detectors_text.strip():
        tokens = [t.strip() for t in re.split(r"[,;|]", detectors_text) if t.strip()]
        for token in tokens:
            key = _normalize_detector_key(token)
            if key and key not in selected:
                selected.append(key)
        if not selected:
            raise ValueError("--detectors provided but no valid detector names were parsed.")
        return selected

    detector_key = _normalize_detector_key(detector)
    if detector_key:
        selected = [detector_key]
    else:
        selected = [
            "seasonal_ad",
            "persist_ad",
            "level_shift_ad",
            "volatility_shift_ad",
        ]
        if threshold_low is not None or threshold_high is not None:
            selected.append("threshold_ad")
        if use_pca:
            selected.append("pca_ad")

    if "threshold_ad" in selected and threshold_low is None and threshold_high is None:
        raise ValueError("ThresholdAD requires --threshold-low or --threshold-high.")

    return selected


def _build_detector_kwargs(detector_key: str, cfg: dict[str, Any]) -> dict[str, Any]:
    side = _parse_side(cfg.get("side"))
    if detector_key == "seasonal_ad":
        return {
            "c": float(cfg["seasonal_c"]),
            "side": side,
            "freq": int(cfg["seasonal_period"]),
        }
    if detector_key == "persist_ad":
        return {
            "c": float(cfg["persist_c"]),
            "side": side,
            "window": int(cfg["persist_window"]),
        }
    if detector_key == "level_shift_ad":
        return {
            "c": float(cfg["levelshift_c"]),
            "side": side,
            "window": int(cfg["levelshift_window"]),
        }
    if detector_key == "volatility_shift_ad":
        return {
            "c": float(cfg["volatility_c"]),
            "side": side,
            "window": int(cfg["volatility_window"]),
        }
    if detector_key == "threshold_ad":
        kwargs: dict[str, float] = {}
        low = cfg.get("threshold_low")
        high = cfg.get("threshold_high")
        if low is not None:
            kwargs["low"] = float(low)
        if high is not None:
            kwargs["high"] = float(high)
        return kwargs
    raise ValueError(f"Unsupported per-series detector key: {detector_key}")


def _build_pca_kwargs(cfg: dict[str, Any]) -> dict[str, Any]:
    return {
        "k": int(cfg["pca_k"]),
        "c": float(cfg["pca_c"]),
        "window": int(cfg["pca_window"]),
    }


def _instantiate_detector(detector_cls: type, kwargs: dict[str, Any]):
    signature = inspect.signature(detector_cls.__init__)
    accepted = {key: value for key, value in kwargs.items() if key in signature.parameters}
    return detector_cls(**accepted), accepted


def _apply_detector(detector: Any, data: Any):
    if hasattr(detector, "fit_detect"):
        return detector.fit_detect(data)
    if hasattr(detector, "fit") and hasattr(detector, "detect"):
        detector.fit(data)
        return detector.detect(data)
    if hasattr(detector, "detect"):
        return detector.detect(data)
    raise ValueError(f"Detector {detector.__class__.__name__} has no usable detect API.")


def _prepare_series_groups(frame: pd.DataFrame, *, min_points: int) -> tuple[list[pd.DataFrame], list[dict[str, Any]]]:
    groups: list[pd.DataFrame] = []
    skipped: list[dict[str, Any]] = []

    for series_id, group in frame.groupby("unique_id", sort=False):
        work = group[["unique_id", "ds", "y"]].copy()
        work["ds"] = pd.to_datetime(work["ds"], errors="coerce")
        work["y"] = pd.to_numeric(work["y"], errors="coerce")
        work = work.dropna(subset=["ds", "y"]).sort_values("ds").drop_duplicates(subset=["ds"], keep="last")

        if len(work) < int(min_points):
            skipped.append(
                {
                    "series_id": str(series_id),
                    "reason": f"insufficient_points(<{int(min_points)})",
                    "points": int(len(work)),
                }
            )
            continue

        series = pd.Series(work["y"].to_numpy(), index=work["ds"].to_numpy())
        try:
            checked = validate_series(series)
        except Exception:
            checked = series

        if isinstance(checked, pd.DataFrame):
            if checked.shape[1] == 1:
                checked = checked.iloc[:, 0]
            else:
                checked = checked.mean(axis=1)

        if not isinstance(checked, pd.Series):
            checked = series

        checked = checked.dropna()
        if checked.empty:
            skipped.append(
                {
                    "series_id": str(series_id),
                    "reason": "empty_after_validation",
                    "points": int(len(work)),
                }
            )
            continue

        normalized = pd.DataFrame(
            {
                "unique_id": str(series_id),
                "ds": pd.to_datetime(checked.index, errors="coerce"),
                "y": pd.to_numeric(checked.values, errors="coerce"),
            }
        )
        normalized = normalized.dropna(subset=["ds", "y"]).sort_values("ds").reset_index(drop=True)
        if len(normalized) < int(min_points):
            skipped.append(
                {
                    "series_id": str(series_id),
                    "reason": "insufficient_points_after_validation",
                    "points": int(len(normalized)),
                }
            )
            continue

        groups.append(normalized)

    if not groups:
        raise ValueError("No available series after anomaly preprocessing.")

    return groups, skipped


def _run_per_series_detectors(
    groups: list[pd.DataFrame],
    detector_keys: list[str],
    cfg: dict[str, Any],
) -> tuple[pd.DataFrame, dict[str, dict[str, Any]], list[dict[str, str]]]:
    rows: list[pd.DataFrame] = []
    detector_params: dict[str, dict[str, Any]] = {}
    detector_warnings: list[dict[str, str]] = []

    for group in groups:
        work = group.copy()
        checked_series = pd.Series(work["y"].to_numpy(), index=work["ds"].to_numpy())

        for detector_key in detector_keys:
            detector_cls = {
                "seasonal_ad": SeasonalAD,
                "persist_ad": PersistAD,
                "level_shift_ad": LevelShiftAD,
                "volatility_shift_ad": VolatilityShiftAD,
                "threshold_ad": ThresholdAD,
            }[detector_key]
            raw_params = _build_detector_kwargs(detector_key, cfg)
            detector, params = _instantiate_detector(detector_cls, raw_params)
            detector_params.setdefault(detector_key, params)

            try:
                result = _apply_detector(detector, checked_series)
                mask = _to_mask(result, checked_series.index)
            except Exception as exc:
                mask = pd.Series(False, index=checked_series.index)
                detector_warnings.append(
                    {
                        "series_id": str(work["unique_id"].iloc[0]),
                        "detector": detector_key,
                        "error": str(exc),
                    }
                )

            work[DETECTOR_TO_COLUMN[detector_key]] = mask.to_numpy()

        rows.append(work)

    detect_frame = pd.concat(rows, ignore_index=True)
    for detector_key in detector_keys:
        col = DETECTOR_TO_COLUMN[detector_key]
        detect_frame[col] = detect_frame[col].fillna(False).astype(bool)

    return detect_frame, detector_params, detector_warnings


def _run_pca_detector(
    frame: pd.DataFrame,
    cfg: dict[str, Any],
) -> tuple[pd.Series, dict[str, Any], dict[str, Any]]:
    min_points = int(cfg["pca_min_points"])
    min_features = int(cfg["pca_min_features"])

    work = frame[["unique_id", "ds", "y"]].copy()
    work["ds"] = pd.to_datetime(work["ds"], errors="coerce")
    work["y"] = pd.to_numeric(work["y"], errors="coerce")
    work = work.dropna(subset=["unique_id", "ds", "y"])
    if work.empty:
        raise ValueError("PcaAD has no valid rows after preprocessing.")

    wide = work.pivot_table(index="ds", columns="unique_id", values="y", aggfunc="mean").sort_index()
    wide = wide.dropna(axis=1, how="all")
    if wide.shape[1] < min_features:
        raise ValueError(f"PcaAD needs at least {min_features} variables, got {wide.shape[1]}.")

    wide = wide.interpolate(method="linear", limit_direction="both").ffill().bfill()
    wide = wide.dropna(axis=0, how="any")
    if len(wide) < min_points:
        raise ValueError(f"PcaAD needs at least {min_points} aligned timestamps, got {len(wide)}.")

    try:
        checked = validate_series(wide)
    except Exception:
        checked = wide

    if isinstance(checked, pd.Series):
        checked = checked.to_frame(name="series_0")

    if not isinstance(checked, pd.DataFrame):
        raise ValueError("PcaAD input validation did not return a DataFrame.")

    checked = checked.dropna(axis=0, how="any")
    if checked.empty:
        raise ValueError("PcaAD got empty matrix after validation.")

    detector_cls = PcaAD
    raw_params = _build_pca_kwargs(cfg)
    detector, params = _instantiate_detector(detector_cls, raw_params)
    result = _apply_detector(detector, checked)

    mask = _to_mask(result, checked.index)
    runtime = {
        "enabled": True,
        "feature_count": int(checked.shape[1]),
        "matrix_rows": int(len(checked)),
        "skip_reason": "",
    }
    return mask, params, runtime


def _render_series_payload(detect_frame: pd.DataFrame, detector_keys: list[str], limit_per_series: int) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []

    for series_id, group in detect_frame.groupby("unique_id", sort=False):
        rows = group.sort_values("ds")
        anomalies = rows[rows["is_anomaly"]]

        anomaly_points: list[dict[str, Any]] = []
        for _, row in anomalies.head(limit_per_series).iterrows():
            hit = [key for key in detector_keys if bool(row[DETECTOR_TO_COLUMN[key]])]
            anomaly_points.append(
                {
                    "ds": pd.Timestamp(row["ds"]).isoformat(),
                    "y": round(float(row["y"]), 6),
                    "detectors": hit,
                }
            )

        total_points = int(len(rows))
        total_anomalies = int(rows["is_anomaly"].sum())
        payload.append(
            {
                "series_id": str(series_id),
                "points": total_points,
                "anomaly_points": total_anomalies,
                "anomaly_rate": round((total_anomalies / total_points) if total_points else 0.0, 6),
                "detector_counts": {
                    key: int(rows[DETECTOR_TO_COLUMN[key]].sum()) for key in detector_keys
                },
                "anomalies": anomaly_points,
                "anomalies_truncated": max(0, total_anomalies - len(anomaly_points)),
            }
        )

    payload.sort(key=lambda item: item["series_id"])
    return payload


def _build_runtime_config(
    *,
    freq: str,
    min_points: int,
    side: str,
    seasonal_period: int,
    seasonal_c: float,
    persist_c: float,
    persist_window: int,
    levelshift_c: float,
    levelshift_window: int,
    volatility_c: float,
    volatility_window: int,
    threshold_low: float | None,
    threshold_high: float | None,
    pca_k: int,
    pca_c: float,
    pca_window: int,
    pca_min_points: int,
    pca_min_features: int,
) -> dict[str, Any]:
    period = max(2, int(seasonal_period) if int(seasonal_period) > 0 else _infer_default_seasonal_period(freq))
    return {
        "freq": freq,
        "min_points": max(4, int(min_points)),
        "side": _parse_side(side),
        "seasonal_period": period,
        "seasonal_c": max(0.1, float(seasonal_c)),
        "persist_c": max(0.1, float(persist_c)),
        "persist_window": max(2, int(persist_window)),
        "levelshift_c": max(0.1, float(levelshift_c)),
        "levelshift_window": max(2, int(levelshift_window)),
        "volatility_c": max(0.1, float(volatility_c)),
        "volatility_window": max(2, int(volatility_window)),
        "threshold_low": threshold_low,
        "threshold_high": threshold_high,
        "pca_k": max(1, int(pca_k)),
        "pca_c": max(0.1, float(pca_c)),
        "pca_window": max(2, int(pca_window)),
        "pca_min_points": max(4, int(pca_min_points)),
        "pca_min_features": max(2, int(pca_min_features)),
    }


def run_detection_with_detectors(
    df: pd.DataFrame,
    *,
    detectors: list[str],
    freq: str,
    min_points: int = 8,
    side: str = "both",
    seasonal_period: int = 24,
    seasonal_c: float = 3.0,
    persist_c: float = 3.0,
    persist_window: int = 6,
    levelshift_c: float = 6.0,
    levelshift_window: int = 12,
    volatility_c: float = 6.0,
    volatility_window: int = 12,
    threshold_low: float | None = None,
    threshold_high: float | None = None,
    pca_k: int = 1,
    pca_c: float = 6.0,
    pca_window: int = 10,
    pca_min_points: int = 8,
    pca_min_features: int = 2,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    selected = []
    for detector in detectors:
        key = _normalize_detector_key(detector)
        if key and key not in selected:
            selected.append(key)

    if not selected:
        raise ValueError("No valid detectors were selected.")

    if "threshold_ad" in selected and threshold_low is None and threshold_high is None:
        raise ValueError("ThresholdAD requires threshold_low or threshold_high.")

    cfg = _build_runtime_config(
        freq=freq,
        min_points=min_points,
        side=side,
        seasonal_period=seasonal_period,
        seasonal_c=seasonal_c,
        persist_c=persist_c,
        persist_window=persist_window,
        levelshift_c=levelshift_c,
        levelshift_window=levelshift_window,
        volatility_c=volatility_c,
        volatility_window=volatility_window,
        threshold_low=threshold_low,
        threshold_high=threshold_high,
        pca_k=pca_k,
        pca_c=pca_c,
        pca_window=pca_window,
        pca_min_points=pca_min_points,
        pca_min_features=pca_min_features,
    )

    groups, skipped_series = _prepare_series_groups(df, min_points=cfg["min_points"])

    per_series_detectors = [k for k in selected if k != "pca_ad"]
    if per_series_detectors:
        detect_frame, detector_params, detector_warnings = _run_per_series_detectors(
            groups=groups,
            detector_keys=per_series_detectors,
            cfg=cfg,
        )
    else:
        detect_frame = pd.concat(groups, ignore_index=True)
        detector_params = {}
        detector_warnings = []

    pca_runtime = {
        "requested": "pca_ad" in selected,
        "enabled": False,
        "feature_count": 0,
        "matrix_rows": 0,
        "skip_reason": "",
    }
    if "pca_ad" in selected:
        try:
            pca_mask, pca_params, runtime = _run_pca_detector(detect_frame, cfg=cfg)
            mask_by_ds = {pd.Timestamp(k): bool(v) for k, v in pca_mask.items()}
            detect_frame["pca_ad"] = detect_frame["ds"].apply(
                lambda value: bool(mask_by_ds.get(pd.Timestamp(value), False))
            )
            detector_params["pca_ad"] = pca_params
            pca_runtime = {
                "requested": True,
                "enabled": bool(runtime.get("enabled", False)),
                "feature_count": int(runtime.get("feature_count", 0)),
                "matrix_rows": int(runtime.get("matrix_rows", 0)),
                "skip_reason": runtime.get("skip_reason") or "",
            }
        except Exception as exc:
            detect_frame["pca_ad"] = False
            detector_warnings.append(
                {
                    "series_id": "__multivariate__",
                    "detector": "pca_ad",
                    "error": str(exc),
                }
            )
            pca_runtime = {
                "requested": True,
                "enabled": False,
                "feature_count": 0,
                "matrix_rows": 0,
                "skip_reason": str(exc),
            }

    for key in selected:
        column = DETECTOR_TO_COLUMN[key]
        if column not in detect_frame.columns:
            detect_frame[column] = False
        detect_frame[column] = detect_frame[column].fillna(False).astype(bool)

    detect_frame["is_anomaly"] = detect_frame[[DETECTOR_TO_COLUMN[k] for k in selected]].any(axis=1)
    detect_frame = detect_frame.sort_values(["unique_id", "ds"]).reset_index(drop=True)

    meta = {
        "selected_detectors": selected,
        "detector_params": detector_params,
        "skipped_series": skipped_series,
        "detector_warnings": detector_warnings,
        "pca_runtime": pca_runtime,
        "runtime_config": cfg,
    }
    return detect_frame, meta


def detect_with_seasonal_ad(
    df: pd.DataFrame,
    *,
    freq: str,
    min_points: int = 8,
    seasonal_period: int = 24,
    c: float = 3.0,
    side: str = "both",
) -> pd.DataFrame:
    out, _ = run_detection_with_detectors(
        df,
        detectors=["seasonal_ad"],
        freq=freq,
        min_points=min_points,
        seasonal_period=seasonal_period,
        seasonal_c=c,
        side=side,
    )
    return out


def detect_with_persist_ad(
    df: pd.DataFrame,
    *,
    freq: str,
    min_points: int = 8,
    window: int = 6,
    c: float = 3.0,
    side: str = "both",
) -> pd.DataFrame:
    out, _ = run_detection_with_detectors(
        df,
        detectors=["persist_ad"],
        freq=freq,
        min_points=min_points,
        persist_window=window,
        persist_c=c,
        side=side,
    )
    return out


def detect_with_level_shift_ad(
    df: pd.DataFrame,
    *,
    freq: str,
    min_points: int = 8,
    window: int = 12,
    c: float = 6.0,
    side: str = "both",
) -> pd.DataFrame:
    out, _ = run_detection_with_detectors(
        df,
        detectors=["level_shift_ad"],
        freq=freq,
        min_points=min_points,
        levelshift_window=window,
        levelshift_c=c,
        side=side,
    )
    return out


def detect_with_volatility_shift_ad(
    df: pd.DataFrame,
    *,
    freq: str,
    min_points: int = 8,
    window: int = 12,
    c: float = 6.0,
    side: str = "both",
) -> pd.DataFrame:
    out, _ = run_detection_with_detectors(
        df,
        detectors=["volatility_shift_ad"],
        freq=freq,
        min_points=min_points,
        volatility_window=window,
        volatility_c=c,
        side=side,
    )
    return out


def detect_with_threshold_ad(
    df: pd.DataFrame,
    *,
    freq: str,
    min_points: int = 8,
    threshold_low: float | None = None,
    threshold_high: float | None = None,
) -> pd.DataFrame:
    out, _ = run_detection_with_detectors(
        df,
        detectors=["threshold_ad"],
        freq=freq,
        min_points=min_points,
        threshold_low=threshold_low,
        threshold_high=threshold_high,
    )
    return out


def detect_with_pca_ad(
    df: pd.DataFrame,
    *,
    freq: str,
    min_points: int = 8,
    pca_k: int = 1,
    pca_c: float = 6.0,
    pca_window: int = 10,
    pca_min_points: int = 8,
    pca_min_features: int = 2,
) -> pd.DataFrame:
    out, _ = run_detection_with_detectors(
        df,
        detectors=["pca_ad"],
        freq=freq,
        min_points=min_points,
        pca_k=pca_k,
        pca_c=pca_c,
        pca_window=pca_window,
        pca_min_points=pca_min_points,
        pca_min_features=pca_min_features,
    )
    return out


def detect_with_auto(
    df: pd.DataFrame,
    *,
    freq: str,
    min_points: int = 8,
    use_pca: bool = False,
    threshold_low: float | None = None,
    threshold_high: float | None = None,
) -> tuple[list[str], pd.DataFrame, dict[str, Any]]:
    selected = [
        "seasonal_ad",
        "persist_ad",
        "level_shift_ad",
        "volatility_shift_ad",
    ]
    if threshold_low is not None or threshold_high is not None:
        selected.append("threshold_ad")
    if use_pca:
        selected.append("pca_ad")

    out, meta = run_detection_with_detectors(
        df,
        detectors=selected,
        freq=freq,
        min_points=min_points,
        threshold_low=threshold_low,
        threshold_high=threshold_high,
    )
    return selected, out, meta


def build_detector_payload() -> dict[str, Any]:
    return {
        "detectors": [
            {
                "detector_key": key,
                **spec,
            }
            for key, spec in DETECTOR_CATALOG.items()
        ]
    }


def to_text_preview(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _build_detect_payload(
    frame: pd.DataFrame,
    *,
    source: str,
    detector_requested: str,
    detectors_text: str,
    freq_hint: str,
    min_points: int,
    side: str,
    seasonal_period: int,
    seasonal_c: float,
    persist_c: float,
    persist_window: int,
    levelshift_c: float,
    levelshift_window: int,
    volatility_c: float,
    volatility_window: int,
    threshold_low: float | None,
    threshold_high: float | None,
    use_pca: bool,
    pca_k: int,
    pca_c: float,
    pca_window: int,
    pca_min_points: int,
    pca_min_features: int,
    max_anomaly_points_per_series: int,
    output_file: str = "",
    csv_mapping: dict[str, str] | None = None,
) -> dict[str, Any]:
    freq = infer_freq(frame, freq_hint)

    selected_detectors = _select_detectors(
        detector=detector_requested,
        detectors_text=detectors_text,
        use_pca=use_pca,
        threshold_low=threshold_low,
        threshold_high=threshold_high,
    )

    detect_frame, meta = run_detection_with_detectors(
        frame,
        detectors=selected_detectors,
        freq=freq,
        min_points=min_points,
        side=side,
        seasonal_period=seasonal_period,
        seasonal_c=seasonal_c,
        persist_c=persist_c,
        persist_window=persist_window,
        levelshift_c=levelshift_c,
        levelshift_window=levelshift_window,
        volatility_c=volatility_c,
        volatility_window=volatility_window,
        threshold_low=threshold_low,
        threshold_high=threshold_high,
        pca_k=pca_k,
        pca_c=pca_c,
        pca_window=pca_window,
        pca_min_points=pca_min_points,
        pca_min_features=pca_min_features,
    )

    sufficiency = validate_data_sufficiency(detect_frame, min_points=min_points)

    detect_display = detect_frame.copy()
    detect_display["ds"] = pd.to_datetime(detect_display["ds"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    if output_file:
        target = Path(output_file).expanduser()
        target.parent.mkdir(parents=True, exist_ok=True)
        detect_display.to_csv(target, index=False)
        output_path = target.resolve().as_posix()
    else:
        outputs_dir().mkdir(parents=True, exist_ok=True)
        if detector_requested == "auto" and not detectors_text.strip():
            name_part = "auto"
        elif detectors_text.strip():
            name_part = _safe_slug(detectors_text)
        else:
            name_part = _safe_slug(detector_requested)
        target = outputs_dir() / f"traffic_anomaly_{name_part}.csv"
        detect_display.to_csv(target, index=False)
        output_path = target.resolve().as_posix()

    anomaly_rows = int(detect_frame["is_anomaly"].sum())
    total_rows = int(len(detect_frame))
    detector_counts = {
        key: int(detect_frame[DETECTOR_TO_COLUMN[key]].sum()) for key in selected_detectors
    }

    series_payload = _render_series_payload(
        detect_frame,
        detector_keys=selected_detectors,
        limit_per_series=max(20, int(max_anomaly_points_per_series)),
    )

    preview_anomalies = detect_display[detect_display["is_anomaly"]].head(20)
    if preview_anomalies.empty:
        preview_anomalies = detect_display.head(20)

    return {
        "ok": True,
        "action": "detect",
        "source": source,
        "csv_mapping": csv_mapping,
        "detector_requested": detector_requested,
        "detectors_selected": selected_detectors,
        "detect_freq": freq,
        "sufficiency": sufficiency,
        "runtime_config": meta.get("runtime_config"),
        "detector_params": meta.get("detector_params"),
        "detector_anomaly_counts": detector_counts,
        "pca_runtime": meta.get("pca_runtime"),
        "skipped_series": meta.get("skipped_series"),
        "detector_warnings": meta.get("detector_warnings"),
        "series_count": int(detect_frame["unique_id"].nunique()),
        "rows_input": int(len(frame)),
        "rows_detection": total_rows,
        "anomaly_rows": anomaly_rows,
        "anomaly_rate": round((anomaly_rows / total_rows) if total_rows else 0.0, 6),
        "time_range": {
            "start": pd.to_datetime(detect_frame["ds"].min()).strftime("%Y-%m-%d %H:%M:%S"),
            "end": pd.to_datetime(detect_frame["ds"].max()).strftime("%Y-%m-%d %H:%M:%S"),
        },
        "preview": preview_anomalies.to_dict(orient="records"),
        "series": series_payload,
        "output_file": output_path,
    }


def run_csv_detection_with_mapping(
    csv_reference: str,
    *,
    mapping: dict[str, Any],
    detector: str,
    detectors: str,
    freq: str,
    min_points: int,
    side: str,
    seasonal_period: int,
    seasonal_c: float,
    persist_c: float,
    persist_window: int,
    levelshift_c: float,
    levelshift_window: int,
    volatility_c: float,
    volatility_window: int,
    threshold_low: float | None,
    threshold_high: float | None,
    use_pca: bool,
    pca_k: int,
    pca_c: float,
    pca_window: int,
    pca_min_points: int,
    pca_min_features: int,
    max_anomaly_points_per_series: int,
    output_file: str = "",
) -> dict[str, Any]:
    csv_path, normalized, mapping_used = load_and_normalize_csv(csv_reference, mapping)
    return _build_detect_payload(
        normalized,
        source=csv_path.as_posix(),
        detector_requested=detector,
        detectors_text=detectors,
        freq_hint=freq,
        min_points=min_points,
        side=side,
        seasonal_period=seasonal_period,
        seasonal_c=seasonal_c,
        persist_c=persist_c,
        persist_window=persist_window,
        levelshift_c=levelshift_c,
        levelshift_window=levelshift_window,
        volatility_c=volatility_c,
        volatility_window=volatility_window,
        threshold_low=threshold_low,
        threshold_high=threshold_high,
        use_pca=use_pca,
        pca_k=pca_k,
        pca_c=pca_c,
        pca_window=pca_window,
        pca_min_points=pca_min_points,
        pca_min_features=pca_min_features,
        max_anomaly_points_per_series=max_anomaly_points_per_series,
        output_file=output_file,
        csv_mapping=mapping_used,
    )


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Road traffic anomaly runner (single-script version).")
    p.add_argument("--action", required=True, choices=["detectors", "inspect", "detect"], help="Action name")
    p.add_argument("--file", default="", help="CSV path/reference")
    p.add_argument("--history", default="", help="Inline numeric history, e.g. '120,130,900,140'")
    p.add_argument("--timestamp-col", default="", help="Timestamp column name for CSV (required for CSV detect)")
    p.add_argument("--value-col", default="", help="Numeric target column name for CSV (required for CSV detect)")
    p.add_argument("--series-id-col", default="", help="Series id column for multi-series CSV")

    p.add_argument(
        "--detector",
        default="auto",
        choices=[
            "auto",
            "seasonal_ad",
            "persist_ad",
            "level_shift_ad",
            "volatility_shift_ad",
            "threshold_ad",
            "pca_ad",
        ],
        help="Primary detector mode",
    )
    p.add_argument("--detectors", default="", help="Comma-separated detector keys; overrides --detector")

    p.add_argument("--freq", default="", help="Pandas/adtk freq alias, e.g. h/15min/D")
    p.add_argument("--min-points", type=int, default=8, help="Minimum points per series")
    p.add_argument("--side", default="both", choices=["both", "positive", "negative"], help="Detector side")

    p.add_argument("--seasonal-period", type=int, default=24, help="Seasonal period for SeasonalAD")
    p.add_argument("--seasonal-c", type=float, default=3.0, help="SeasonalAD c")

    p.add_argument("--persist-c", type=float, default=3.0, help="PersistAD c")
    p.add_argument("--persist-window", type=int, default=6, help="PersistAD window")

    p.add_argument("--levelshift-c", type=float, default=6.0, help="LevelShiftAD c")
    p.add_argument("--levelshift-window", type=int, default=12, help="LevelShiftAD window")

    p.add_argument("--volatility-c", type=float, default=6.0, help="VolatilityShiftAD c")
    p.add_argument("--volatility-window", type=int, default=12, help="VolatilityShiftAD window")

    p.add_argument("--threshold-low", type=float, default=None, help="ThresholdAD low")
    p.add_argument("--threshold-high", type=float, default=None, help="ThresholdAD high")

    p.add_argument("--use-pca", action="store_true", help="Enable pca_ad in auto mode")
    p.add_argument("--pca-k", type=int, default=1, help="PcaAD k")
    p.add_argument("--pca-c", type=float, default=6.0, help="PcaAD c")
    p.add_argument("--pca-window", type=int, default=10, help="PcaAD window")
    p.add_argument("--pca-min-points", type=int, default=8, help="PcaAD minimum aligned rows")
    p.add_argument("--pca-min-features", type=int, default=2, help="PcaAD minimum aligned features")

    p.add_argument("--max-anomaly-points-per-series", type=int, default=200, help="Series anomaly payload cap")
    p.add_argument("--head", type=int, default=100, help="Preview rows for inspect")
    p.add_argument("--output-file", default="", help="Optional CSV export path")
    p.add_argument("--format", choices=["text", "json"], default="text", help="Output format")
    return p


def main() -> int:
    args = parser().parse_args()

    try:
        if args.action == "detectors":
            payload = {
                "ok": True,
                "action": "detectors",
                **build_detector_payload(),
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

        if args.action == "detect":
            if args.file:
                payload = run_csv_detection_with_mapping(
                    args.file,
                    mapping={
                        "timestamp_col": args.timestamp_col,
                        "value_col": args.value_col,
                        "series_id_col": args.series_id_col,
                    },
                    detector=args.detector,
                    detectors=args.detectors,
                    freq=args.freq,
                    min_points=args.min_points,
                    side=args.side,
                    seasonal_period=args.seasonal_period,
                    seasonal_c=args.seasonal_c,
                    persist_c=args.persist_c,
                    persist_window=args.persist_window,
                    levelshift_c=args.levelshift_c,
                    levelshift_window=args.levelshift_window,
                    volatility_c=args.volatility_c,
                    volatility_window=args.volatility_window,
                    threshold_low=_to_optional_float(args.threshold_low),
                    threshold_high=_to_optional_float(args.threshold_high),
                    use_pca=bool(args.use_pca),
                    pca_k=args.pca_k,
                    pca_c=args.pca_c,
                    pca_window=args.pca_window,
                    pca_min_points=args.pca_min_points,
                    pca_min_features=args.pca_min_features,
                    max_anomaly_points_per_series=args.max_anomaly_points_per_series,
                    output_file=args.output_file,
                )
            elif args.history:
                values = parse_history_values(args.history)
                base_freq = args.freq.strip() or "h"
                frame = history_to_anomaly_frame(values, freq=base_freq)
                payload = _build_detect_payload(
                    frame,
                    source="inline-history",
                    detector_requested=args.detector,
                    detectors_text=args.detectors,
                    freq_hint=args.freq,
                    min_points=args.min_points,
                    side=args.side,
                    seasonal_period=args.seasonal_period,
                    seasonal_c=args.seasonal_c,
                    persist_c=args.persist_c,
                    persist_window=args.persist_window,
                    levelshift_c=args.levelshift_c,
                    levelshift_window=args.levelshift_window,
                    volatility_c=args.volatility_c,
                    volatility_window=args.volatility_window,
                    threshold_low=_to_optional_float(args.threshold_low),
                    threshold_high=_to_optional_float(args.threshold_high),
                    use_pca=bool(args.use_pca),
                    pca_k=args.pca_k,
                    pca_c=args.pca_c,
                    pca_window=args.pca_window,
                    pca_min_points=args.pca_min_points,
                    pca_min_features=args.pca_min_features,
                    max_anomaly_points_per_series=args.max_anomaly_points_per_series,
                    output_file=args.output_file,
                    csv_mapping=None,
                )
            else:
                raise ValueError("detect action requires --file or --history")

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
