# ***************************************************************************
#  SAFE — period-over-period drift analysis
#  --------------------------------------------------------------------------
#  Buckets each metric by calendar period (day / week / month / year), then
#  compares consecutive buckets — and the first vs last month — with the
#  shared drift math from safe.stats. Writes one CSV per granularity.
# ***************************************************************************

import logging
import os

import numpy as np
import pandas as pd

from safe.config import HARD_BOUNDS
from safe.loader import load_pivoted_dataframe
from safe.stats import sample_comparison, validate_alpha

logger = logging.getLogger(__name__)

# Rolling consecutive-period comparisons: (output stem, pandas offset alias, label format)
#   D  = calendar day,  W = week (ending Sunday),  MS = month start,  YS = year start
GRANULARITIES = [
    ("day_to_day",     "D",  "%Y-%m-%d"),
    ("week_to_week",   "W",  "%Y-%m-%d"),
    ("month_to_month", "MS", "%Y-%m"),
    ("year_to_year",   "YS", "%Y"),
]

CSV_COLUMNS = [
    "sensor", "metric", "granularity", "old_period", "new_period",
    "old_n", "new_n", "old_n_eff", "new_n_eff",
    "old_mean", "new_mean", "mean_delta", "old_std", "new_std",
    "old_min", "old_max", "new_min", "new_max",
    "cohens_d", "std_ratio", "p_welch", "p_levene", "mean_shift", "variance_shift",
]


def apply_hard_bounds(df, metric_cols):
    """NaN out physically impossible readings so they never enter the stats."""
    for metric in metric_cols:
        df.loc[~np.isfinite(df[metric]), metric] = np.nan
        bounds = HARD_BOUNDS.get(metric)
        if bounds:
            lo, hi = bounds
            df.loc[(df[metric] < lo) | (df[metric] > hi), metric] = np.nan
    return df


def comparison_row(sensor, metric, granularity, old_label, new_label, old_vals, new_vals, result):
    """Assemble one output row from a sample_comparison() result plus min/max."""
    return {
        "sensor": sensor,
        "metric": metric,
        "granularity": granularity,
        "old_period": old_label,
        "new_period": new_label,
        "old_n": result["old_n"],
        "new_n": result["new_n"],
        "old_n_eff": result["old_n_eff"],
        "new_n_eff": result["new_n_eff"],
        "old_mean": round(result["old_mean"], 4),
        "new_mean": round(result["new_mean"], 4),
        "mean_delta": round(result["mean_delta"], 4),
        "old_std": round(result["old_std"], 4),
        "new_std": round(result["new_std"], 4),
        "old_min": round(float(old_vals.min()), 4),
        "old_max": round(float(old_vals.max()), 4),
        "new_min": round(float(new_vals.min()), 4),
        "new_max": round(float(new_vals.max()), 4),
        "cohens_d": round(result["cohens_d"], 4),
        "std_ratio": round(result["std_ratio"], 4),
        "p_welch": format(result["p_welch"], ".2e"),
        "p_levene": format(result["p_levene"], ".2e"),
        "mean_shift": result["mean_shift"],
        "variance_shift": result["variance_shift"],
    }


def bucketize(series, freq, min_samples):
    """Return [(period_timestamp, values_array), ...] for non-empty calendar
    buckets, in time order, keeping only buckets with >= min_samples readings."""
    if isinstance(min_samples, bool) or not isinstance(min_samples, (int, np.integer)) or min_samples < 2:
        raise ValueError("min_samples must be an integer >= 2")
    series = series[np.isfinite(series)]
    if not series.index.is_monotonic_increasing:
        series = series.sort_index(kind='stable')
    buckets = []
    for period, vals in series.resample(freq):
        if len(vals) >= min_samples:
            buckets.append((period, vals.to_numpy()))
    return buckets


def _consecutive_comparisons(df, metric_cols, granularity, freq, label_fmt, p_alpha, min_samples):
    """Compare each non-empty period bucket to the previous one, per sensor and metric.

    Note: comparisons are between consecutive *non-empty* buckets, so a pair may
    span a data gap. The old_period / new_period date columns make gaps visible.
    """
    rows = []
    for sensor, sensor_df in df.groupby("_sensor_name"):
        for metric in metric_cols:
            buckets = bucketize(sensor_df[metric], freq, min_samples)
            for (old_period, old_vals), (new_period, new_vals) in zip(buckets, buckets[1:]):
                result = sample_comparison(old_vals, new_vals, p_alpha, metric=metric)
                rows.append(comparison_row(
                    sensor, metric, granularity,
                    old_period.strftime(label_fmt), new_period.strftime(label_fmt),
                    old_vals, new_vals, result,
                ))
    return rows


def _first_vs_last_month(df, metric_cols, p_alpha, min_samples):
    """Compare the first calendar month of data to the last, per sensor and metric."""
    rows = []
    for sensor, sensor_df in df.groupby("_sensor_name"):
        for metric in metric_cols:
            months = bucketize(sensor_df[metric], "MS", min_samples)
            if len(months) < 2:
                continue
            (first_period, first_vals) = months[0]
            (last_period, last_vals) = months[-1]
            result = sample_comparison(first_vals, last_vals, p_alpha, metric=metric)
            rows.append(comparison_row(
                sensor, metric, "first_vs_last_month",
                first_period.strftime("%Y-%m"), last_period.strftime("%Y-%m"),
                first_vals, last_vals, result,
            ))
    return rows


def run_period_analysis(file_path, output_dir, p_alpha=0.01, min_samples=2, make_plots=True):
    """Run all period comparisons on a CSV and write period_*.csv into output_dir.

    Returns True on success, False when the input CSV could not be loaded."""
    validate_alpha(p_alpha)
    if isinstance(min_samples, bool) or not isinstance(min_samples, (int, np.integer)) or min_samples < 2:
        raise ValueError("min_samples must be an integer >= 2")
    df, metric_cols = load_pivoted_dataframe(file_path)
    if df is None:
        return False

    print(f"Metrics found : {metric_cols}")
    print(f"Date range    : {df.index.min()}  ->  {df.index.max()}")
    print("NOTE: first/last day, week, month and the end years are partial; the old_n/new_n "
          "columns show coverage. Year-to-Year therefore is not a like-for-like comparison.")

    df = apply_hard_bounds(df, metric_cols)
    os.makedirs(output_dir, exist_ok=True)

    # Rolling consecutive-period comparisons (one CSV per granularity)
    for stem, freq, label_fmt in GRANULARITIES:
        rows = _consecutive_comparisons(df, metric_cols, stem, freq, label_fmt, p_alpha, min_samples)
        out_path = os.path.join(output_dir, f"period_{stem}.csv")
        pd.DataFrame(rows, columns=CSV_COLUMNS).to_csv(out_path, index=False)
        print(f"Wrote {os.path.basename(out_path)} ({len(rows)} rows)")

    # First month vs last month
    rows = _first_vs_last_month(df, metric_cols, p_alpha, min_samples)
    out_path = os.path.join(output_dir, "period_first_vs_last_month.csv")
    pd.DataFrame(rows, columns=CSV_COLUMNS).to_csv(out_path, index=False)
    print(f"Wrote {os.path.basename(out_path)} ({len(rows)} rows)")

    print("Period analysis complete.")

    if make_plots:
        try:
            from safe.plotting import run_all_plotting
            plots_dir = os.path.join(output_dir, "plots")
            print("\nAuto-generating period plots...")
            run_all_plotting(output_dir, plots_dir, p_alpha=p_alpha)
        except Exception as plot_err:
            print(f"Failed to auto-generate plots: {plot_err}")
            return False

    return True
