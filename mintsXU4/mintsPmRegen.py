# ***************************************************************************
#  Regenerate all PM images from the 1-second data (all 7 IPS7100 size bins)
#  --------------------------------------------------------------------------
#  For every PM size bin this produces, under output/<field>/ :
#     plots/      - period "category" plots (day/week/month/year x
#                   means / stds / zscores / stats_tests), one figure per field
#     histograms/ - distribution-fit overlay + standard-normal-test histograms
#                   for adapted recent windows (last 2 days/weeks/months,
#                   first-vs-last 30-day)
#
#  Descriptive stats (means/stds/min/max) use the FULL 1 s data per bucket.
#  The Welch / Levene tests run on a bounded random subsample per bucket side
#  (N_TEST_MAX) — at 1 s a monthly bucket is ~2.5M points, where the tests both
#  saturate and are slow; a subsample keeps them meaningful and fast.
#
#  No CSV/stat files are written (images only).  Run:  python mintsPmRegen.py
# ***************************************************************************

import os
import sys
import shutil
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import scipy.stats as st

# standardNormalVisualizer.py lives at the repo root, one level up from mintsXU4/
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from mintsDriftAnalysis import sample_comparison
import mintsPeriodAnalysis as mpa
import mintsPeriodPlotter as mpp  # sets report style on import; we reuse its helpers
from standardNormalVisualizer import zscore, plot_standard_normal_comparison
import mints1sLoader as loader

logger = logging.getLogger(__name__)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(SCRIPT_DIR, "output")

PM_FIELDS = loader.PM_FIELDS
SENSOR = loader.SENSOR_NAME
UNIT = "µg/m³"

# Per-bin display info (cool->warm by particle size)
PM_INFO = {
    "pm0_1":  {"name": "PM0.1",  "color": "#1f77b4"},
    "pm0_3":  {"name": "PM0.3",  "color": "#17becf"},
    "pm0_5":  {"name": "PM0.5",  "color": "#2ca02c"},
    "pm1_0":  {"name": "PM1.0",  "color": "#bcbd22"},
    "pm2_5":  {"name": "PM2.5",  "color": "#ff7f0e"},
    "pm5_0":  {"name": "PM5.0",  "color": "#d62728"},
    "pm10_0": {"name": "PM10",   "color": "#9467bd"},
}

ALPHA = 0.01
LOG_ALPHA = -np.log10(ALPHA)
P_FLOOR_LOG = -np.log10(1e-15)
RANGE_FILL = "#6b7280"
RANGE_EDGE = "#4b5563"
LEVENE_COLOR = "#9467bd"

MIN_SAMPLES = 2
N_TEST_MAX = 50_000      # cap per bucket side for Welch/Levene
HIST_MAX = 80_000        # cap per window side for histogram plotting
FIT_MAX = 20_000         # cap for distribution MLE fitting (burr/gengamma are slow)
_RNG = np.random.default_rng(0)


def _subsample(a, cap):
    a = np.asarray(a, dtype=float)
    if a.size > cap:
        return a[_RNG.choice(a.size, cap, replace=False)]
    return a


# --------------------------------------------------------------------------
# Period "category" frames (reuses the audited drift math + bucketizer)
# --------------------------------------------------------------------------
def _period_row(field, stem, op, npd, ov, nv, fmt):
    """Descriptive stats from the full bucket; tests from a bounded subsample."""
    res = sample_comparison(_subsample(ov, N_TEST_MAX), _subsample(nv, N_TEST_MAX),
                            ALPHA, metric=field)
    return {
        "sensor": SENSOR, "metric": field, "granularity": stem,
        "old_period": op.strftime(fmt), "new_period": npd.strftime(fmt),
        "old_n": int(ov.size), "new_n": int(nv.size),
        "old_n_eff": res["old_n_eff"], "new_n_eff": res["new_n_eff"],
        "old_mean": round(float(ov.mean()), 4), "new_mean": round(float(nv.mean()), 4),
        "mean_delta": round(float(nv.mean() - ov.mean()), 4),
        "old_std": round(float(ov.std()), 4), "new_std": round(float(nv.std()), 4),
        "old_min": round(float(ov.min()), 4), "old_max": round(float(ov.max()), 4),
        "new_min": round(float(nv.min()), 4), "new_max": round(float(nv.max()), 4),
        "cohens_d": round(res["cohens_d"], 4), "std_ratio": round(res["std_ratio"], 4),
        "p_welch": format(res["p_welch"], ".2e"), "p_levene": format(res["p_levene"], ".2e"),
        "mean_shift": res["mean_shift"], "variance_shift": res["variance_shift"],
    }


def build_period_frames(series, field):
    """Return {category_stem: DataFrame} for one PM field's full-year series."""
    frames = {}
    for stem, freq, fmt in mpa.GRANULARITIES:
        buckets = mpa._bucketize(series, freq, MIN_SAMPLES)
        rows = [_period_row(field, stem, op, npd, ov, nv, fmt)
                for (op, ov), (npd, nv) in zip(buckets, buckets[1:])]
        frames[stem] = pd.DataFrame(rows, columns=mpa.CSV_COLUMNS)

    months = mpa._bucketize(series, "MS", MIN_SAMPLES)
    if len(months) >= 2:
        (op, ov), (npd, nv) = months[0], months[-1]
        frames["first_vs_last_month"] = pd.DataFrame(
            [_period_row(field, "first_vs_last_month", op, npd, ov, nv, "%Y-%m")],
            columns=mpa.CSV_COLUMNS)
    else:
        frames["first_vs_last_month"] = pd.DataFrame(columns=mpa.CSV_COLUMNS)
    return frames


# --------------------------------------------------------------------------
# Period plotting (single field per figure)
# --------------------------------------------------------------------------
def _fmt_time_axis(ax):
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ax.xaxis.get_major_locator()))


def _finish(fig, path, title):
    fig.suptitle(title, fontsize=13, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def _zscores(means):
    mu, sd = means.mean(), means.std()
    return (means - mu) / sd if sd > 1e-9 else np.zeros_like(means)


def _plot_metric(df, field, kind, title, out_path):
    info = PM_INFO[field]
    fig, ax = plt.subplots(figsize=(11, 4.6))
    if df.empty:
        ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes)
        _finish(fig, out_path, title)
        return

    d = df.sort_values("parsed_date")
    dense = d["new_period"].nunique() >= 3
    dates = d["parsed_date"].to_numpy()

    if dense:
        mk = dict(marker="o", markersize=4) if len(d) <= 40 else {}
        if kind == "mean":
            ax.fill_between(dates, d["new_min"], d["new_max"], color=RANGE_FILL, alpha=0.25,
                            linewidth=0.45, edgecolor=RANGE_EDGE, label="min–max range")
            ax.plot(dates, d["new_mean"], color=info["color"], linewidth=1.8, label="period mean", **mk)
            ax.set_ylabel(f"{info['name']} ({UNIT})")
        elif kind == "std":
            ax.plot(dates, d["new_std"], color=info["color"], linewidth=1.8, label="period σ", **mk)
            ax.set_ylabel(f"{info['name']} σ ({UNIT})")
        else:  # zscore
            z = _zscores(d["new_mean"].to_numpy(dtype=float))
            ax.axhspan(-2, 2, color="gray", alpha=0.08, linewidth=0)
            ax.axhline(0, color="black", linewidth=0.8, alpha=0.5)
            for lvl in (2, 3):
                ax.axhline(lvl, color="#d62728", ls="--", lw=0.8, alpha=0.5)
                ax.axhline(-lvl, color="#d62728", ls="--", lw=0.8, alpha=0.5)
            ax.plot(dates, z, color=info["color"], linewidth=1.6, label="z vs series mean", **mk)
            ax.set_ylabel(f"{info['name']} z-score")
        _fmt_time_axis(ax)
        ax.set_xlabel("Period (new period date)")
        ax.margins(x=0.01)
    else:
        labels = [f"{o}\nvs {n}" for o, n in zip(d["old_period"], d["new_period"])]
        x = np.arange(len(labels))
        if kind == "zscore":
            z = _zscores(d["new_mean"].to_numpy(dtype=float))
            ax.bar(x, z, width=0.5, color=info["color"], alpha=0.85, edgecolor="black")
            ax.axhline(0, color="black", lw=0.8, alpha=0.5)
            ax.set_ylabel(f"{info['name']} z-score")
        else:
            oc, nc = ("old_mean", "new_mean") if kind == "mean" else ("old_std", "new_std")
            w = 0.38
            ax.bar(x - w / 2, d[oc], w, color="#b0b0b0", edgecolor="black", label="old period")
            ax.bar(x + w / 2, d[nc], w, color=info["color"], alpha=0.9, edgecolor="black", label="new period")
            ax.set_ylabel(f"{info['name']} ({UNIT})" if kind == "mean" else f"{info['name']} σ ({UNIT})")
            ax.legend(loc="best")
        ax.set_xticks(x)
        ax.set_xticklabels(labels, fontsize=8)
        ax.margins(y=0.18)

    if dense:
        ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), borderaxespad=0.0)
    ax.set_title(info["name"], loc="left")
    _finish(fig, out_path, title)


def _plot_significance(df, field, title, out_path):
    info = PM_INFO[field]
    fig, ax = plt.subplots(figsize=(11, 4.6))
    if df.empty:
        ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes)
        _finish(fig, out_path, title)
        return

    d = df.sort_values("parsed_date")
    dense = d["new_period"].nunique() >= 3
    lw = mpp._neg_log10p(d["p_welch"])
    ll = mpp._neg_log10p(d["p_levene"])

    if dense:
        dates = d["parsed_date"].to_numpy()
        mean_sig = mpp._as_bool(d["mean_shift"])
        var_sig = mpp._as_bool(d["variance_shift"])
        ax.axhspan(0, LOG_ALPHA, color="gray", alpha=0.12, linewidth=0)
        ax.axhline(LOG_ALPHA, color="#d62728", ls="--", lw=1.0, alpha=0.7, label=f"α = {ALPHA}")
        ax.plot(dates, lw, color=info["color"], lw=1.3, alpha=0.9, label="Welch (mean)")
        ax.plot(dates, ll, color=LEVENE_COLOR, lw=1.1, alpha=0.75, label="Levene (variance)")
        if (~mean_sig).any():
            ax.scatter(dates[~mean_sig], lw[~mean_sig], facecolors="none", edgecolors=info["color"],
                       s=42, linewidths=1.3, zorder=5, label="mean: not gated-significant")
        if (~var_sig).any():
            ax.scatter(dates[~var_sig], ll[~var_sig], facecolors="none", edgecolors=LEVENE_COLOR,
                       s=42, linewidths=1.3, zorder=5, label="variance: not gated-significant")
        ax.set_ylim(-0.5, P_FLOOR_LOG + 1)
        _fmt_time_axis(ax)
        ax.set_xlabel("Period (new period date)")
        ax.margins(x=0.01)
        ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), borderaxespad=0.0)
    else:
        labels = [f"{o}\nvs {n}" for o, n in zip(d["old_period"], d["new_period"])]
        x = np.arange(len(labels))
        w = 0.38
        ax.bar(x - w / 2, lw, w, color=info["color"], alpha=0.9, edgecolor="black", label="Welch (mean)")
        ax.bar(x + w / 2, ll, w, color=LEVENE_COLOR, alpha=0.8, edgecolor="black", label="Levene (variance)")
        ax.axhline(LOG_ALPHA, color="#d62728", ls="--", lw=1.0, alpha=0.7, label=f"α = {ALPHA}")
        ax.set_ylim(0, P_FLOOR_LOG + 1)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, fontsize=8)
        ax.legend(loc="best")

    ax.set_ylabel("-log10(p)  (higher = stronger)")
    ax.set_title(info["name"], loc="left")
    _finish(fig, out_path, title)


def plot_category_set(field, frames, plots_dir):
    os.makedirs(plots_dir, exist_ok=True)
    name = PM_INFO[field]["name"]
    for stem, _, _ in mpa.GRANULARITIES:
        df = frames[stem].copy()
        if not df.empty:
            df["parsed_date"] = pd.to_datetime(df["new_period"], errors="coerce")
        pretty = stem.replace("_", " ").title()
        _plot_metric(df, field, "mean", f"{name} {pretty}: Mean Over Time",
                     os.path.join(plots_dir, f"{stem}_means.png"))
        _plot_metric(df, field, "std", f"{name} {pretty}: Volatility (σ) Over Time",
                     os.path.join(plots_dir, f"{stem}_stds.png"))
        _plot_metric(df, field, "zscore", f"{name} {pretty}: Standardized Mean (z-score)",
                     os.path.join(plots_dir, f"{stem}_zscores.png"))
        _plot_significance(df, field, f"{name} {pretty}: Test Significance (α = {ALPHA})",
                           os.path.join(plots_dir, f"{stem}_stats_tests.png"))


# --------------------------------------------------------------------------
# Histograms: distribution-fit overlay + standard-normal test
# --------------------------------------------------------------------------
CANDIDATES = [
    ("Normal", st.norm), ("Log-Normal", st.lognorm), ("Gamma", st.gamma),
    ("Weibull", st.weibull_min), ("Log-Gamma", st.loggamma), ("Burr", st.burr),
    ("Inv-Gamma", st.invgamma), ("Gen-Gamma", st.gengamma), ("Exp", st.expon),
]
_HCOLORS = ["steelblue", "darkorange"]


def _best_fit(data):
    out = []
    for nm, dist in CANDIDATES:
        try:
            params = dist.fit(data)
            ks, p = st.kstest(data, dist.cdf, args=params)
            out.append((ks, nm, dist, params))
        except Exception:
            pass
    return sorted(out, key=lambda r: r[0])


def _overlay_histogram(ax, s1, s2, l1, l2, title, xlabel):
    for i, (data, color, label) in enumerate(zip([s1, s2], _HCOLORS, [l1, l2])):
        _, bins, _ = ax.hist(data, bins="auto", color=color, edgecolor="white",
                             linewidth=0.4, alpha=0.4, density=True, label=label)
        fit = _best_fit(_subsample(data, FIT_MAX))
        if fit:
            ks, nm, dist, params = fit[0]
            x = np.linspace(bins[0], bins[-1], 600)
            ax.plot(x, dist.pdf(x, *params), color=color, linewidth=2,
                    label=f"Period {i+1} best fit: {nm} (KS={ks:.4f})")
    ax.set_xlabel(xlabel)
    ax.set_ylabel("Density")
    ax.set_title(title, fontsize=13, fontweight="bold")
    ax.legend(fontsize=8)


def _windows(wide):
    """Adapted comparison windows derived from the actual 1s data range."""
    start = wide.index.min().normalize()
    end = wide.index.max().normalize()
    last_full = end - pd.Timedelta(days=1)   # final file ends at next-day 00:00:00

    def ds(t):
        return t.strftime("%Y-%m-%d")

    return [
        {"name": "Last 2 Days", "slug": "last2days", "samples": [
            (f"Day 1 ({ds(last_full - pd.Timedelta(days=1))})", last_full - pd.Timedelta(days=1), last_full - pd.Timedelta(days=1)),
            (f"Day 2 ({ds(last_full)})", last_full, last_full)]},
        {"name": "Last 2 Weeks", "slug": "last2weeks", "samples": [
            (f"Week 1 ({ds(last_full - pd.Timedelta(days=13))} to {ds(last_full - pd.Timedelta(days=7))})",
             last_full - pd.Timedelta(days=13), last_full - pd.Timedelta(days=7)),
            (f"Week 2 ({ds(last_full - pd.Timedelta(days=6))} to {ds(last_full)})",
             last_full - pd.Timedelta(days=6), last_full)]},
        {"name": "Last 2 Months", "slug": "last2months", "samples": [
            (f"Month 1 ({ds(last_full - pd.Timedelta(days=59))} to {ds(last_full - pd.Timedelta(days=30))})",
             last_full - pd.Timedelta(days=59), last_full - pd.Timedelta(days=30)),
            (f"Month 2 ({ds(last_full - pd.Timedelta(days=29))} to {ds(last_full)})",
             last_full - pd.Timedelta(days=29), last_full)]},
        {"name": "First vs Last Month", "slug": "first_vs_last_month", "samples": [
            (f"First 30d ({ds(start)} to {ds(start + pd.Timedelta(days=29))})",
             start, start + pd.Timedelta(days=29)),
            (f"Last 30d ({ds(last_full - pd.Timedelta(days=29))} to {ds(last_full)})",
             last_full - pd.Timedelta(days=29), last_full)],
         "color_as": "year"},
    ]


def histograms_for_field(field, wide, hist_dir):
    os.makedirs(hist_dir, exist_ok=True)
    name = PM_INFO[field]["name"]
    series = wide[field].dropna()

    for comp in _windows(wide):
        samples = []
        for sample_name, a, b in comp["samples"]:
            vals = series.loc[str(a):str(b)].to_numpy(dtype=float)
            if vals.size == 0:
                logger.warning("  %s %s: no data in %s..%s", field, comp["slug"], a, b)
                continue
            samples.append((sample_name, vals))
        if len(samples) < 2:
            continue

        (l1, v1), (l2, v2) = samples[0], samples[1]
        v1s, v2s = _subsample(v1, HIST_MAX), _subsample(v2, HIST_MAX)

        # 1) distribution-fit overlay histogram
        fig, ax = plt.subplots(figsize=(11, 6))
        _overlay_histogram(ax, v1s, v2s, l1, l2,
                           f"{name} Distribution — {comp['name']} (vaLo Node 01)", f"{name} ({UNIT})")
        fig.tight_layout()
        fig.savefig(os.path.join(hist_dir, f"{field}_{comp['slug']}_histogram.png"), dpi=150)
        plt.close(fig)

        # 2) standard-normal test (reuse the shared visualizer; it expects a Path)
        period_samples = [(l1, v1s, _HCOLORS[0]), (l2, v2s, _HCOLORS[1])]
        plot_standard_normal_comparison(name, {"name": comp["name"], "slug": comp["slug"]},
                                        period_samples, Path(hist_dir), field)


# --------------------------------------------------------------------------
# Cleanup of old PM1.0-only outputs + period CSV snapshots
# --------------------------------------------------------------------------
def cleanup_old():
    removed = []
    old_pm = os.path.join(OUT_DIR, "pm1_0")
    if os.path.isdir(old_pm):
        shutil.rmtree(old_pm)
        removed.append(os.path.relpath(old_pm, SCRIPT_DIR))
    # Old period CSV snapshots (user does not want CSV deliverables)
    pa = os.path.join(OUT_DIR, "period_analysis")
    if os.path.isdir(pa):
        for fn in os.listdir(pa):
            if fn.startswith("period_") and fn.endswith(".csv"):
                os.remove(os.path.join(pa, fn))
                removed.append(os.path.relpath(os.path.join(pa, fn), SCRIPT_DIR))
    return removed


# --------------------------------------------------------------------------
def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    # Remove old PM1.0-only images + CSV snapshots first, so regenerating into
    # output/pm1_0/ does not get clobbered. (These are git-tracked, so a `git
    # checkout` restores them if needed.)
    for r in cleanup_old():
        logger.info("removed old output: %s", r)

    wide = loader.load_wide()
    logger.info("Loaded wide PM frame %s  range %s -> %s",
                wide.shape, wide.index.min(), wide.index.max())

    for field in PM_FIELDS:
        logger.info("=== %s ===", PM_INFO[field]["name"])
        field_dir = os.path.join(OUT_DIR, field)
        series = wide[field].dropna().sort_index()

        frames = build_period_frames(series, field)
        plot_category_set(field, frames, os.path.join(field_dir, "plots"))
        logger.info("  category plots done")

        histograms_for_field(field, wide, os.path.join(field_dir, "histograms"))
        logger.info("  histograms done")

    logger.info("PM image regeneration complete.")


if __name__ == "__main__":
    main()
