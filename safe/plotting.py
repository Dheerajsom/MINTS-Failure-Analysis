# ***************************************************************************
#  SAFE — period-over-period drift plotting
#  --------------------------------------------------------------------------
#  Reads the period_*.csv files produced by safe.periods and renders, per
#  granularity, four figures over time:
#     *_means.png        - per-metric mean (with min-max band)
#     *_stds.png         - per-metric standard deviation (volatility)
#     *_zscores.png      - per-metric mean standardized against the whole series
#     *_stats_tests.png  - Welch / Levene significance ( -log10(p) ) vs time
#
#  Dense categories (>=3 periods) are drawn as time series; sparse ones
#  (year-to-year, first-vs-last-month) as labelled bar charts.
# ***************************************************************************

import glob
import os

import numpy as np
import pandas as pd
import matplotlib

matplotlib.use("Agg")  # headless / file-only backend
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from safe.stats import validate_alpha

# A clean, report-friendly base style (fall back gracefully on older matplotlib)
try:
    plt.style.use("seaborn-v0_8-whitegrid")
except Exception:
    plt.style.use("default")

plt.rcParams.update({
    "font.size": 10,
    "axes.titlesize": 11,
    "axes.titleweight": "bold",
    "axes.labelsize": 10,
    "axes.edgecolor": "#888888",
    "xtick.labelsize": 9,
    "ytick.labelsize": 9,
    "legend.fontsize": 8.5,
    "legend.framealpha": 0.9,
    "figure.dpi": 130,
    "savefig.dpi": 150,
    "grid.alpha": 0.35,
})

# Fixed metric order + display info
METRICS = ["pm1_0", "temperature", "pressure"]
METRIC_INFO = {
    "pm1_0":       {"name": "PM1.0",       "unit": "µg/m³", "color": "#2ca02c"},  # green
    "temperature": {"name": "Temperature", "unit": "°C",    "color": "#ff7f0e"},  # orange
    "pressure":    {"name": "Pressure",    "unit": "hPa",   "color": "#1f77b4"},  # blue
}
LEVENE_COLOR = "#9467bd"   # purple
RANGE_FILL_COLOR = "#6b7280"
RANGE_EDGE_COLOR = "#4b5563"
ALPHA = 0.01
LOG_ALPHA = -np.log10(ALPHA)    # 2.0
P_FLOOR_LOG = -np.log10(1e-15)  # 15.0 ceiling for floored p-values


# --------------------------------------------------------------------------
# small helpers
# --------------------------------------------------------------------------
def _info(metric):
    return METRIC_INFO.get(metric, {"name": metric, "unit": "", "color": "gray"})


def _metric_rows(df, metric, sort_by_date=True):
    """Rows for one metric, chronologically sorted. Assumes df already carries a
    'parsed_date' column (added once in generate_category_plots)."""
    d = df[df["metric"] == metric]
    if d.empty or not sort_by_date:
        return d
    return d.sort_values("parsed_date")


def _neg_log10p(series):
    """Parse a p-value column (formatted strings are fine) into -log10(p),
    floored at 1e-15 so the log never blows up."""
    p = pd.to_numeric(series, errors="coerce").fillna(1.0).to_numpy()
    return -np.log10(np.clip(p, 1e-15, 1.0))


def _as_bool(series):
    """Coerce a flag column to real booleans. pandas infers bool dtype when a
    CSV column is all True/False, but a stray NaN forces object/string dtype —
    and "False".astype(bool) is truthy. Map the strings explicitly instead."""
    if series.dtype == bool:
        return series.to_numpy()
    mapped = series.map({"True": True, "False": False, True: True, False: False})
    return mapped.fillna(False).astype(bool).to_numpy()


def _real_data_blocks(d, max_gap_days=45, min_std=0.05):
    """Find date blocks where at least one side of the comparison is non-flat."""
    real = d[(d["old_std"].abs() > min_std) | (d["new_std"].abs() > min_std)].sort_values("parsed_date")
    if real.empty:
        return []

    blocks = []
    start = prev = real["parsed_date"].iloc[0]
    for current in real["parsed_date"].iloc[1:]:
        if (current - prev).days > max_gap_days:
            blocks.append((start, prev))
            start = current
        prev = current
    blocks.append((start, prev))
    return blocks


def _finish(fig, path, suptitle):
    try:
        fig.suptitle(suptitle, fontsize=13, fontweight="bold")
        fig.tight_layout(rect=[0, 0, 1, 0.97])
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        fig.savefig(path, bbox_inches="tight")
    finally:
        plt.close(fig)
    print(f"  -> {os.path.basename(path)}")


def _format_time_axis(ax):
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ax.xaxis.get_major_locator()))


def _pair_labels(d):
    return [f"{o}\nvs {n}" for o, n in zip(d["old_period"], d["new_period"])]


# --------------------------------------------------------------------------
# dense (time-series) panels
# --------------------------------------------------------------------------
def _dense_metric_panels(df, suptitle, out_path, kind):
    """kind in {'mean','std','zscore'} -> 3 stacked time-series panels (one per metric)."""
    fig, axes = plt.subplots(3, 1, figsize=(11, 9), sharex=True)

    for ax, metric in zip(axes, METRICS):
        d = _metric_rows(df, metric)
        info = _info(metric)
        if d.empty:
            ax.text(0.5, 0.5, f"No data for {info['name']}", ha="center", va="center", transform=ax.transAxes)
            ax.set_title(info["name"])
            continue

        dates = d["parsed_date"].to_numpy()
        use_markers = len(d) <= 40
        mk = dict(marker="o", markersize=4) if use_markers else {}

        if kind == "mean":
            ax.fill_between(dates, d["new_min"], d["new_max"], color=RANGE_FILL_COLOR, alpha=0.26,
                            linewidth=0.45, edgecolor=RANGE_EDGE_COLOR, label="min-max range")
            ax.plot(dates, d["new_mean"], color=info["color"], linewidth=1.8, label="period mean", **mk)
            ax.set_ylabel(f"{info['name']}\n({info['unit']})")

        elif kind == "std":
            ax.plot(dates, d["new_std"], color=info["color"], linewidth=1.8, label="period std dev", **mk)
            ax.set_ylabel(f"{info['name']} σ\n({info['unit']})")

        elif kind == "zscore":
            means = d["new_mean"].to_numpy(dtype=float)
            mu, sd = means.mean(), means.std()
            z = (means - mu) / sd if sd > 1e-9 else np.zeros_like(means)
            ax.axhspan(-2, 2, color="gray", alpha=0.08, linewidth=0)
            ax.axhline(0, color="black", linewidth=0.8, alpha=0.5)
            for lvl in (2, 3):
                ax.axhline(lvl, color="#d62728", linestyle="--", linewidth=0.8, alpha=0.5)
                ax.axhline(-lvl, color="#d62728", linestyle="--", linewidth=0.8, alpha=0.5)
            ax.plot(dates, z, color=info["color"], linewidth=1.6, label="z vs series mean", **mk)
            ax.set_ylabel(f"{info['name']}\nz-score")

        ax.set_title(info["name"], loc="left")
        ax.margins(x=0.01)

    axes[0].legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), borderaxespad=0.0)
    _format_time_axis(axes[-1])
    axes[-1].set_xlabel("Period (new period date)")
    _finish(fig, out_path, suptitle)


def _dense_significance(df, suptitle, out_path, p_alpha=ALPHA):
    """3 stacked panels of -log10(p) vs time for Welch (mean) & Levene (variance)."""
    log_alpha = -np.log10(p_alpha)
    fig, axes = plt.subplots(3, 1, figsize=(11, 9), sharex=True)

    for ax, metric in zip(axes, METRICS):
        d = _metric_rows(df, metric)
        info = _info(metric)

        if d.empty:
            ax.text(0.5, 0.5, f"No data for {info['name']}", ha="center", va="center", transform=ax.transAxes)
            ax.set_title(info["name"])
            continue

        dates = d["parsed_date"].to_numpy()
        lw = _neg_log10p(d["p_welch"])
        ll = _neg_log10p(d["p_levene"])

        mean_sig = _as_bool(d["mean_shift"])
        var_sig = _as_bool(d["variance_shift"])

        # Shade the "not significant" band (below the alpha threshold)
        ax.axhspan(0, log_alpha, color="gray", alpha=0.12, linewidth=0)
        ax.axhline(log_alpha, color="#d62728", linestyle="--", linewidth=1.0, alpha=0.7,
                   label=f"α = {p_alpha} threshold")

        ax.plot(dates, lw, color=info["color"], linewidth=1.3, alpha=0.9, label="Welch  (mean shift)")
        ax.plot(dates, ll, color=LEVENE_COLOR, linewidth=1.1, alpha=0.75, label="Levene (variance shift)")

        # Mark gated-significant periods (filled) vs the rest (open circles)
        if (~mean_sig).any():
            ax.scatter(dates[~mean_sig], lw[~mean_sig], facecolors="none", edgecolors=info["color"],
                       s=42, linewidths=1.3, zorder=5, label="mean: not significant")
        if (~var_sig).any():
            ax.scatter(dates[~var_sig], ll[~var_sig], facecolors="none", edgecolors=LEVENE_COLOR,
                       s=42, linewidths=1.3, zorder=5, label="variance: not significant")

        ax.set_ylim(-0.5, P_FLOOR_LOG + 1)
        ax.set_ylabel("-log10(p)\n(higher = stronger)")
        ax.set_title(info["name"], loc="left")
        ax.margins(x=0.01)

    axes[0].legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), borderaxespad=0.0)
    _format_time_axis(axes[-1])
    axes[-1].set_xlabel("Period (new period date)")
    _finish(fig, out_path, suptitle)


def _zoomed_metric_significance(df, metric, start, end, block_num, plots_dir, p_alpha=ALPHA):
    log_alpha = -np.log10(p_alpha)
    info = _info(metric)
    d = _metric_rows(df, metric)
    window = d[(d["parsed_date"] >= start) & (d["parsed_date"] <= end)].copy()
    if window.empty:
        return

    dates = window["parsed_date"].to_numpy()
    lw = _neg_log10p(window["p_welch"])
    ll = _neg_log10p(window["p_levene"])
    mean_sig = _as_bool(window["mean_shift"])
    var_sig = _as_bool(window["variance_shift"])

    fig, ax = plt.subplots(figsize=(10.5, 4.8))
    ax.axhspan(0, log_alpha, color="gray", alpha=0.12, linewidth=0)
    ax.axhline(log_alpha, color="#d62728", linestyle="--", linewidth=1.0, alpha=0.7,
               label=f"alpha = {p_alpha} threshold")

    use_markers = len(window) <= 45
    mk = dict(marker="o", markersize=4) if use_markers else {}
    ax.plot(dates, lw, color=info["color"], linewidth=1.5, alpha=0.95, label="Welch (mean shift)", **mk)
    ax.plot(dates, ll, color=LEVENE_COLOR, linewidth=1.35, alpha=0.85, label="Levene (variance shift)", **mk)

    if (~mean_sig).any():
        ax.scatter(dates[~mean_sig], lw[~mean_sig], facecolors="none", edgecolors=info["color"],
                   s=42, linewidths=1.3, zorder=5, label="mean: not significant")
    if (~var_sig).any():
        ax.scatter(dates[~var_sig], ll[~var_sig], facecolors="none", edgecolors=LEVENE_COLOR,
                   s=42, linewidths=1.3, zorder=5, label="variance: not significant")

    y_max = min(max(float(np.nanmax([lw.max(), ll.max(), log_alpha])) + 1.0, log_alpha + 1.0), P_FLOOR_LOG + 1)
    ax.set_ylim(-0.2, y_max)
    ax.set_xlim(start, end)
    ax.set_ylabel("-log10(p)\n(higher = stronger)")
    ax.set_xlabel("Day-to-day comparison date")
    ax.set_title(f"{info['name']} real-data block {block_num}: {start:%Y-%m-%d} to {end:%Y-%m-%d}", loc="left")
    ax.margins(x=0.02)
    _format_time_axis(ax)
    ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), borderaxespad=0.0)

    filename = f"day_to_day_{metric}_stats_tests_zoom_{block_num}.png"
    _finish(fig, os.path.join(plots_dir, filename), f"Day To Day: {info['name']} Test Significance Zoom")


def generate_day_to_day_zoomed_significance(csv_path, plots_dir, metrics=("temperature", "pressure"), p_alpha=ALPHA):
    df = pd.read_csv(csv_path)
    if df.empty:
        return

    df["parsed_date"] = pd.to_datetime(df["new_period"], errors="coerce")
    for metric in metrics:
        d = _metric_rows(df, metric)
        blocks = _real_data_blocks(d)
        for block_num, (start, end) in enumerate(blocks, 1):
            _zoomed_metric_significance(df, metric, start, end, block_num, plots_dir, p_alpha)


# --------------------------------------------------------------------------
# sparse (bar) panels  -- year-to-year, first-vs-last-month
# --------------------------------------------------------------------------
def _annotate_bars(ax, bars, fmt="{:.1f}"):
    for b in bars:
        h = b.get_height()
        ax.annotate(fmt.format(h), (b.get_x() + b.get_width() / 2, h),
                    ha="center", va="bottom", fontsize=8,
                    xytext=(0, 2), textcoords="offset points")


def _sparse_metric_panels(df, suptitle, out_path, kind):
    fig, axes = plt.subplots(3, 1, figsize=(9, 9))

    for ax, metric in zip(axes, METRICS):
        d = _metric_rows(df, metric, sort_by_date=False)
        info = _info(metric)

        if d.empty:
            ax.text(0.5, 0.5, f"No data for {info['name']}", ha="center", va="center", transform=ax.transAxes)
            ax.set_title(info["name"])
            continue

        labels = _pair_labels(d)
        x = np.arange(len(labels))

        if kind == "zscore":
            means = d["new_mean"].to_numpy(dtype=float)
            mu, sd = means.mean(), means.std()
            z = (means - mu) / sd if sd > 1e-9 else np.zeros_like(means)
            bars = ax.bar(x, z, width=0.5, color=info["color"], alpha=0.85, edgecolor="black")
            ax.axhline(0, color="black", linewidth=0.8, alpha=0.5)
            _annotate_bars(ax, bars, "{:.2f}")
            ax.set_ylabel(f"{info['name']} z-score")

        else:
            old_col, new_col = ("old_mean", "new_mean") if kind == "mean" else ("old_std", "new_std")
            w = 0.38
            b1 = ax.bar(x - w / 2, d[old_col], w, color="#b0b0b0", edgecolor="black", label="old period")
            b2 = ax.bar(x + w / 2, d[new_col], w, color=info["color"], alpha=0.9, edgecolor="black", label="new period")
            _annotate_bars(ax, b1)
            _annotate_bars(ax, b2)
            unit = info["unit"]
            ax.set_ylabel(f"{info['name']} ({unit})" if kind == "mean" else f"{info['name']} σ ({unit})")
            ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), borderaxespad=0.0)

        ax.set_title(info["name"], loc="left")
        ax.set_xticks(x)
        ax.set_xticklabels(labels, fontsize=8)
        ax.margins(y=0.18)

    _finish(fig, out_path, suptitle)


def _sparse_significance(df, suptitle, out_path, p_alpha=ALPHA):
    log_alpha = -np.log10(p_alpha)
    fig, axes = plt.subplots(3, 1, figsize=(9, 9))

    for ax, metric in zip(axes, METRICS):
        d = _metric_rows(df, metric, sort_by_date=False)
        info = _info(metric)

        if d.empty:
            ax.text(0.5, 0.5, f"No data for {info['name']}", ha="center", va="center", transform=ax.transAxes)
            ax.set_title(info["name"])
            continue

        labels = _pair_labels(d)
        x = np.arange(len(labels))
        w = 0.38
        ax.bar(x - w / 2, _neg_log10p(d["p_welch"]), w, color=info["color"], alpha=0.9, edgecolor="black", label="Welch (mean)")
        ax.bar(x + w / 2, _neg_log10p(d["p_levene"]), w, color=LEVENE_COLOR, alpha=0.8, edgecolor="black", label="Levene (variance)")
        ax.axhline(log_alpha, color="#d62728", linestyle="--", linewidth=1.0, alpha=0.7, label=f"α = {p_alpha}")

        ax.set_ylim(0, P_FLOOR_LOG + 1)
        ax.set_ylabel("-log10(p)")
        ax.set_title(info["name"], loc="left")
        ax.set_xticks(x)
        ax.set_xticklabels(labels, fontsize=8)
        ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), borderaxespad=0.0)

    _finish(fig, out_path, suptitle)


# --------------------------------------------------------------------------
# orchestration
# --------------------------------------------------------------------------
def generate_category_plots(csv_path, plots_dir, p_alpha=ALPHA):
    """Render each sensor independently; propagate failures to the caller."""
    validate_alpha(p_alpha)
    category = os.path.basename(csv_path).removeprefix("period_").removesuffix(".csv")
    pretty = category.replace("_", " ").title()
    df = pd.read_csv(csv_path)
    if df.empty:
        return
    df["parsed_date"] = pd.to_datetime(df["new_period"], errors="raise")
    groups = list(df.groupby("sensor", sort=True))
    for number, (sensor, frame) in enumerate(groups, 1):
        # Numeric directories cannot collide or escape via a sensor identifier.
        destination = plots_dir if len(groups) == 1 else os.path.join(plots_dir, f"sensor_{number}")
        title = f"{pretty}: {sensor}"
        is_dense = frame["new_period"].nunique() >= 3
        metric_panels = _dense_metric_panels if is_dense else _sparse_metric_panels
        significance = _dense_significance if is_dense else _sparse_significance
        for kind, suffix, label in (("mean", "means", "Metric Means"),
                                    ("std", "stds", "Standard Deviations"),
                                    ("zscore", "zscores", "Standardized Period Means")):
            metric_panels(frame, f"{title}: {label}", os.path.join(destination, f"{category}_{suffix}.png"), kind)
        significance(frame, f"{title}: Test Significance (alpha = {p_alpha})",
                     os.path.join(destination, f"{category}_stats_tests.png"), p_alpha)
        if category == "day_to_day":
            for metric in ("temperature", "pressure"):
                for block, (first, last) in enumerate(_real_data_blocks(_metric_rows(frame, metric)), 1):
                    _zoomed_metric_significance(frame, metric, first, last, block, destination, p_alpha)


def run_all_plotting(output_dir, plots_dir, p_alpha=ALPHA):
    """Scan output_dir for period_*.csv files and render plots into plots_dir."""
    os.makedirs(plots_dir, exist_ok=True)
    print(f"Output Directory: {output_dir}")
    print(f"Plots Directory : {plots_dir}\n")

    csv_files = sorted(glob.glob(os.path.join(output_dir, "period_*.csv")))
    if not csv_files:
        print("No period_*.csv files found. Run the period analysis first!")
        return

    print(f"Found {len(csv_files)} category files to plot.\n")
    for csv_file in csv_files:
        generate_category_plots(csv_file, plots_dir, p_alpha)

    print("\nAll plotting complete. Images saved in", plots_dir)
