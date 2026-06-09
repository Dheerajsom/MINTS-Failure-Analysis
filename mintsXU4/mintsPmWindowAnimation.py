# ***************************************************************************
#  PM1.0 Moving Window PDF Animation
#  ---------------------------------
#  Builds a time-series animation for pm1_0 from the bundled vaLo Node CSV.
#  Each frame highlights a one-hour window and updates a PDF panel on the
#  left side of the y-axis for the values inside that current window.
# ***************************************************************************

import argparse
import os
import tempfile
from pathlib import Path

_CACHE_DIR = Path(tempfile.gettempdir()) / "mints_failure_analysis_matplotlib"
os.environ.setdefault("MPLCONFIGDIR", str(_CACHE_DIR / "matplotlib"))
os.environ.setdefault("XDG_CACHE_HOME", str(_CACHE_DIR / "xdg"))

import matplotlib

matplotlib.use("Agg")
import matplotlib.animation as animation
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy import stats

from mintsDriftAnalysis import HARD_BOUNDS, load_pivoted_dataframe


CSV_PATH = Path(__file__).resolve().parent / "data" / "valo_node_01_full_year.csv"
OUT_DIR = Path(__file__).resolve().parent / "output" / "animations"
DEFAULT_OUT = OUT_DIR / "pm1_0_1h_window_pdf_timeseries.gif"

FIELD = "pm1_0"
FIELD_LABEL = "PM1.0"
FIELD_UNIT = "ug/m^3"
WINDOW = pd.Timedelta(hours=1)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Animate pm1_0 time series with a moving one-hour window PDF."
    )
    parser.add_argument("--csv", type=Path, default=CSV_PATH, help="Input Influx-style CSV path.")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help="Output animation path (.gif or .html).")
    parser.add_argument(
        "--step-minutes",
        type=float,
        default=60.0,
        help="Minutes between animation frames. The analysis window remains one hour.",
    )
    parser.add_argument("--fps", type=int, default=12, help="Frames per second for GIF output.")
    parser.add_argument(
        "--max-frames",
        type=int,
        default=None,
        help="Optional cap for quick previews. Omit to animate from start to end.",
    )
    parser.add_argument(
        "--background-points",
        type=int,
        default=6000,
        help="Maximum points used for the faint full-series background line.",
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=110,
        help="Animation DPI. Lower values keep GIF output smaller.",
    )
    return parser.parse_args()


def load_pm_series(csv_path):
    df, metric_cols = load_pivoted_dataframe(str(csv_path))
    if df is None:
        raise FileNotFoundError(f"Could not load {csv_path}")
    if FIELD not in metric_cols:
        raise ValueError(f"Field '{FIELD}' was not found. Available metrics: {metric_cols}")

    series = pd.to_numeric(df[FIELD], errors="coerce").dropna().sort_index()
    low, high = HARD_BOUNDS[FIELD]
    series = series[(series >= low) & (series <= high)]
    if series.empty:
        raise ValueError(f"No valid {FIELD} values found after numeric coercion and hard-bound filtering.")
    return series


def frame_times(series, step_minutes, max_frames):
    start = series.index.min()
    end = series.index.max()
    step = pd.Timedelta(minutes=step_minutes)
    if step <= pd.Timedelta(0):
        raise ValueError("--step-minutes must be greater than 0")

    times = pd.date_range(start=start, end=end, freq=step)
    if times.empty or times[-1] != end:
        times = times.append(pd.DatetimeIndex([end]))

    if max_frames is not None and max_frames > 0 and len(times) > max_frames:
        idx = np.linspace(0, len(times) - 1, max_frames, dtype=int)
        times = times[idx]
    return times


def window_values(series, end_time):
    start_time = end_time - WINDOW
    return series.loc[(series.index > start_time) & (series.index <= end_time)]


def pdf_for_window(values, y_grid):
    data = values.to_numpy(dtype=float)
    data = data[np.isfinite(data)]
    if data.size < 2:
        return np.zeros_like(y_grid)

    if np.nanstd(data) < 1e-9:
        center = float(np.nanmean(data))
        spread = max(0.05, abs(center) * 0.01)
        return stats.norm.pdf(y_grid, loc=center, scale=spread)

    try:
        return stats.gaussian_kde(data)(y_grid)
    except Exception:
        return stats.norm.pdf(y_grid, loc=float(np.mean(data)), scale=float(np.std(data)))


def make_animation(series, times, out_path, fps, dpi, background_points):
    out_path.parent.mkdir(parents=True, exist_ok=True)

    ymin = max(0.0, float(series.quantile(0.001)) - 0.1 * float(series.std()))
    ymax = float(series.quantile(0.999)) + 0.1 * float(series.std())
    if not np.isfinite(ymin) or not np.isfinite(ymax) or ymin == ymax:
        ymin, ymax = float(series.min()), float(series.max() + 1.0)
    y_grid = np.linspace(ymin, ymax, 400)

    fig = plt.figure(figsize=(12, 6.4), dpi=dpi)
    gs = fig.add_gridspec(1, 2, width_ratios=[1.05, 5.0], wspace=0.04)
    pdf_ax = fig.add_subplot(gs[0, 0])
    ts_ax = fig.add_subplot(gs[0, 1], sharey=pdf_ax)

    dates = mdates.date2num(series.index.to_pydatetime())
    if background_points and len(series) > background_points:
        bg_idx = np.linspace(0, len(series) - 1, background_points, dtype=int)
        bg_dates = dates[bg_idx]
        bg_values = series.to_numpy()[bg_idx]
    else:
        bg_dates = dates
        bg_values = series.to_numpy()

    ts_ax.plot(bg_dates, bg_values, "-", color="#556b7f", linewidth=0.55, alpha=0.35)
    window_line, = ts_ax.plot([], [], "-", color="#d9480f", linewidth=1.8)
    window_points, = ts_ax.plot([], [], "o", color="#d9480f", markersize=3.2, alpha=0.9)
    current_line = ts_ax.axvline(dates[0], color="#1f2937", linewidth=1.0, alpha=0.8)
    window_band = ts_ax.axvspan(dates[0], dates[0], color="#f59f00", alpha=0.20)

    pdf_line, = pdf_ax.plot([], [], color="#0f766e", linewidth=2.1)
    pdf_fill = None
    pdf_mean = pdf_ax.axhline(ymin, color="#0f766e", linestyle="--", linewidth=1.0, alpha=0.75)

    ts_ax.set_ylim(ymin, ymax)
    ts_ax.set_xlim(dates[0], dates[-1])
    ts_ax.set_ylabel(f"{FIELD_LABEL} ({FIELD_UNIT})")
    ts_ax.yaxis.tick_right()
    ts_ax.yaxis.set_label_position("right")
    ts_ax.set_xlabel("Date/time")
    ts_ax.grid(True, alpha=0.3)
    ts_ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ts_ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ts_ax.xaxis.get_major_locator()))

    pdf_ax.set_ylim(ymin, ymax)
    pdf_ax.set_xlabel("PDF")
    pdf_ax.set_ylabel(f"{FIELD_LABEL} ({FIELD_UNIT})")
    pdf_ax.invert_xaxis()
    pdf_ax.grid(True, alpha=0.25)
    pdf_ax.spines["right"].set_linewidth(1.5)
    pdf_ax.spines["right"].set_color("#111827")
    pdf_ax.yaxis.tick_left()

    title = fig.suptitle("", fontsize=13, fontweight="bold")
    stats_text = ts_ax.text(
        0.012,
        0.98,
        "",
        transform=ts_ax.transAxes,
        ha="left",
        va="top",
        fontsize=9,
        bbox={"boxstyle": "round,pad=0.35", "facecolor": "white", "edgecolor": "#cbd5e1", "alpha": 0.9},
    )

    def update(frame_num):
        nonlocal window_band, pdf_fill

        end_time = times[frame_num]
        start_time = end_time - WINDOW
        values = window_values(series, end_time)
        win_dates = mdates.date2num(values.index.to_pydatetime())
        win_vals = values.to_numpy(dtype=float)
        pdf = pdf_for_window(values, y_grid)

        window_line.set_data(win_dates, win_vals)
        window_points.set_data(win_dates, win_vals)
        current_line.set_xdata([mdates.date2num(end_time), mdates.date2num(end_time)])

        window_band.remove()
        window_band = ts_ax.axvspan(
            mdates.date2num(start_time),
            mdates.date2num(end_time),
            color="#f59f00",
            alpha=0.20,
        )

        pdf_line.set_data(pdf, y_grid)
        pdf_ax.set_xlim(max(float(pdf.max()) * 1.12, 1.0), 0.0)
        if pdf_fill is not None:
            pdf_fill.remove()
        pdf_fill = pdf_ax.fill_betweenx(y_grid, 0, pdf, color="#0f766e", alpha=0.18)

        if len(values):
            mean_val = float(values.mean())
            pdf_mean.set_ydata([mean_val, mean_val])
            stats_text.set_text(
                f"Window: {start_time:%Y-%m-%d %H:%M} to {end_time:%Y-%m-%d %H:%M}\n"
                f"n={len(values)}   mean={mean_val:.2f}   std={values.std(ddof=0):.2f}"
            )
        else:
            pdf_mean.set_ydata([ymin, ymin])
            stats_text.set_text(f"Window: {start_time:%Y-%m-%d %H:%M} to {end_time:%Y-%m-%d %H:%M}\nn=0")

        title.set_text(f"{FIELD_LABEL} Time Series with Moving 1-Hour Window PDF ({frame_num + 1}/{len(times)})")
        return window_line, window_points, current_line, pdf_line, pdf_mean, stats_text, title, window_band

    ani = animation.FuncAnimation(fig, update, frames=len(times), interval=1000 / fps, blit=False)

    suffix = out_path.suffix.lower()
    if suffix == ".html":
        out_path.write_text(ani.to_jshtml(fps=fps), encoding="utf-8")
    else:
        if suffix != ".gif":
            out_path = out_path.with_suffix(".gif")
        writer = animation.PillowWriter(fps=fps)
        ani.save(out_path, writer=writer, dpi=dpi)

    plt.close(fig)
    return out_path


def main():
    args = parse_args()
    series = load_pm_series(args.csv)
    times = frame_times(series, args.step_minutes, args.max_frames)
    out_path = make_animation(series, times, args.out, args.fps, args.dpi, args.background_points)
    print(f"Saved {out_path}")
    print(f"Frames: {len(times)} | Window: 1 hour | Step: {args.step_minutes:g} minutes")
    print(f"Data range: {series.index.min()} to {series.index.max()} | Points: {len(series)}")


if __name__ == "__main__":
    main()
