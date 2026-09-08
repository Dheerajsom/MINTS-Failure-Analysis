# ***************************************************************************
#  PM/PC Moving Window PDF Animation
#  ---------------------------------
#  Builds a time-series animation for a chosen particulate channel from the vaLo Node
#  data. Each frame highlights a one-hour window and updates a PDF panel on the
#  left side of the y-axis for the values inside that current window.
#
#  Data sources (in precedence order):
#    --data-dir : directory of valo_node_01_*.csv.gz daily files (full 1s set)
#    --csv      : single Influx-style CSV (downsampled bundle)
# ***************************************************************************

import argparse
import glob
import os
import sys
import tempfile
from pathlib import Path

_REPO_ROOT = str(Path(__file__).resolve().parents[1])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

_CACHE_DIR = Path(tempfile.gettempdir()) / "safe_matplotlib"
os.environ.setdefault("MPLCONFIGDIR", str(_CACHE_DIR / "matplotlib"))
os.environ.setdefault("XDG_CACHE_HOME", str(_CACHE_DIR / "xdg"))

import matplotlib

matplotlib.use("Agg")
import matplotlib.animation as animation
import matplotlib.dates as mdates
import matplotlib.patches as patches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from safe.animation import (
    FieldMetadata,
    field_bounds,
    field_label,
    field_metadata,
    pdf_axis_upper_limit,
    pdf_for_window,
)
from safe.loader import load_pivoted_dataframe


CSV_PATH = Path(__file__).resolve().parent / "data" / "valo_node_01_full_year.csv"
OUT_DIR = Path(__file__).resolve().parent / "output" / "animations"

WINDOW = pd.Timedelta(hours=1)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Animate a PM or particle-count (PC) channel with a moving one-hour window PDF."
    )
    parser.add_argument(
        "--field",
        type=str,
        default="pm1_0",
        help="Available PM or PC channel to animate (e.g. pm1_0, pm2_5, pc0_1).",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Directory of valo_node_01_*.csv.gz daily files. Takes precedence over --csv.",
    )
    parser.add_argument("--csv", type=Path, default=CSV_PATH, help="Input Influx-style CSV path.")
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output animation path (.gif or .html). Defaults to output/animations/<field>_1h_window_pdf_timeseries.gif.",
    )
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


def load_field_series(csv_path, field):
    """Load one available PM/PC field from an Influx-style CSV."""
    csv_path = Path(csv_path)
    if not csv_path.is_file():
        raise FileNotFoundError(f"Input CSV not found: {csv_path}")
    df, metric_cols = load_pivoted_dataframe(str(csv_path))
    if df is None:
        raise ValueError(f"Could not load Influx-style data from {csv_path}")
    if field not in metric_cols:
        raise ValueError(f"Requested field '{field}' was not found in {csv_path}. Available fields: {metric_cols}")

    if df["_sensor_name"].nunique() != 1:
        raise ValueError("Animation requires a single sensor export")
    series = pd.to_numeric(df[field], errors="coerce").dropna().sort_index()
    low, high = field_bounds(field)
    series = series[(series >= low) & (series <= high)]
    if series.empty:
        raise ValueError(f"No valid {field} values found after numeric coercion and hard-bound filtering.")
    return series


def load_field_series_from_dir(data_dir, field):
    """Stream a single PM/PC field out of gzipped daily files in *data_dir*.

    Only rows for the requested field are kept from each file before
    concatenation, so peak memory stays well below loading all channels.
    """
    data_dir = Path(data_dir)
    files = sorted(glob.glob(str(data_dir / "valo_node_01_*.csv.gz")))
    if not files:
        raise FileNotFoundError(f"No valo_node_01_*.csv.gz files found in {data_dir}")

    frames = []
    available_fields = set()
    for fpath in files:
        try:
            chunk = pd.read_csv(
                fpath,
                compression="gzip",
                comment="#",
                usecols=["_time", "_value", "_field"],
            )
        except (ValueError, pd.errors.EmptyDataError):
            # Header-only / empty chunk files (sensor offline gaps) — skip.
            continue
        available_fields.update(chunk["_field"].dropna().astype(str).unique())
        chunk = chunk.loc[chunk["_field"] == field, ["_time", "_value"]]
        if not chunk.empty:
            frames.append(chunk)

    if not frames:
        raise ValueError(
            f"Requested field '{field}' was not found across {len(files)} daily files in {data_dir}. "
            f"Available fields: {sorted(available_fields)}"
        )

    raw = pd.concat(frames, ignore_index=True)
    idx = pd.to_datetime(raw["_time"], utc=True, errors="coerce")
    values = pd.to_numeric(raw["_value"], errors="coerce")
    series = pd.Series(values.to_numpy(), index=idx, name=field)
    series = series[series.index.notna()].dropna()
    series = series[~series.index.duplicated(keep="first")].sort_index()

    low, high = field_bounds(field)
    series = series[(series >= low) & (series <= high)]
    if series.empty:
        raise ValueError(f"No valid {field} values found after numeric coercion and hard-bound filtering.")
    return series


def frame_times(series, step_minutes, max_frames):
    if max_frames is not None and max_frames < 1:
        raise ValueError("max_frames must be positive")
    if not np.isfinite(step_minutes):
        raise ValueError("step_minutes must be finite")
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
    left = series.index.searchsorted(start_time, side="right")
    right = series.index.searchsorted(end_time, side="right")
    return series.iloc[left:right]


def make_animation(series, times, out_path, fps, dpi, background_points, metadata: FieldMetadata):
    out_path.parent.mkdir(parents=True, exist_ok=True)

    ymin = max(0.0, float(series.quantile(0.001)) - 0.1 * float(series.std()))
    ymax = float(series.quantile(0.999)) + 0.1 * float(series.std())
    if not np.isfinite(ymin) or not np.isfinite(ymax) or ymin == ymax:
        ymin, ymax = float(series.min()), float(series.max() + 1.0)
    y_grid = np.linspace(ymin, ymax, 400)

    # Compute every frame first. Each panel gets its own density scale so PC
    # KDEs cannot be flattened by an unrelated narrow, high-density hour.
    pdfs = np.stack([pdf_for_window(window_values(series, end_time), y_grid) for end_time in times])
    pdf_xmaxs = np.asarray([pdf_axis_upper_limit(pdf) for pdf in pdfs])

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

    pdf_line, = pdf_ax.plot([], [], color="#111827", linewidth=1.6, zorder=3)
    pdf_grad_im = None
    pdf_grad_clip = None
    # Smooth vertical jet ramp (one column): row 0 -> ymin (blue), last row -> ymax (red).
    jet_gradient = np.linspace(0.0, 1.0, 256).reshape(-1, 1)
    pdf_mean = pdf_ax.axhline(ymin, color="#111827", linestyle="--", linewidth=1.0, alpha=0.75, zorder=4)

    ts_ax.set_ylim(ymin, ymax)
    ts_ax.set_xlim(dates[0], dates[-1])
    ts_ax.set_ylabel(f"{metadata.label} ({metadata.unit})")
    ts_ax.yaxis.tick_right()
    ts_ax.yaxis.set_label_position("right")
    ts_ax.set_xlabel("Date/time")
    ts_ax.grid(True, alpha=0.3)
    ts_ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ts_ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ts_ax.xaxis.get_major_locator()))

    pdf_ax.set_ylim(ymin, ymax)
    pdf_ax.set_xlabel("PDF")
    pdf_ax.set_ylabel(f"{metadata.label} ({metadata.unit})")
    def set_pdf_xlim(xmax):
        pdf_ax.set_xlim(xmax, 0.0)

    set_pdf_xlim(float(pdf_xmaxs[0]))
    pdf_ax.ticklabel_format(axis="x", style="sci", scilimits=(-2, 3), useMathText=True)
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
        nonlocal window_band, pdf_grad_im, pdf_grad_clip

        end_time = times[frame_num]
        start_time = end_time - WINDOW
        values = window_values(series, end_time)
        win_dates = mdates.date2num(values.index.to_pydatetime())
        win_vals = values.to_numpy(dtype=float)
        pdf = pdfs[frame_num]
        pdf_xmax = float(pdf_xmaxs[frame_num])
        set_pdf_xlim(pdf_xmax)

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
        # Jet gradient fill under the PDF curve: low values -> blue, high values -> red.
        if pdf_grad_im is not None:
            pdf_grad_im.remove()
        if pdf_grad_clip is not None:
            pdf_grad_clip.remove()
        verts = np.column_stack(
            [
                np.concatenate([[0.0], pdf, [0.0]]),
                np.concatenate([[y_grid[0]], y_grid, [y_grid[-1]]]),
            ]
        )
        pdf_grad_clip = patches.Polygon(
            verts, closed=True, transform=pdf_ax.transData, facecolor="none", edgecolor="none"
        )
        pdf_ax.add_patch(pdf_grad_clip)
        pdf_grad_im = pdf_ax.imshow(
            jet_gradient,
            aspect="auto",
            cmap="jet",
            origin="lower",
            extent=[0.0, pdf_xmax, ymin, ymax],
            alpha=0.9,
            zorder=1,
        )
        pdf_grad_im.set_clip_path(pdf_grad_clip)

        if len(values):
            mean_val = float(values.mean())
            pdf_mean.set_ydata([mean_val, mean_val])
            stats_text.set_text(
                f"Window: {start_time:%Y-%m-%d %H:%M} to {end_time:%Y-%m-%d %H:%M}\n"
                f"n={len(values)}   mean={mean_val:.2f}   std={values.std(ddof=0):.2f} {metadata.unit}"
            )
        else:
            pdf_mean.set_ydata([ymin, ymin])
            stats_text.set_text(f"Window: {start_time:%Y-%m-%d %H:%M} to {end_time:%Y-%m-%d %H:%M}\nn=0")

        title.set_text(f"{metadata.label} Time Series with Moving 1-Hour Window PDF ({frame_num + 1}/{len(times)})")
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
    metadata = field_metadata(args.field)
    field = metadata.field

    out_path = args.out
    if out_path is None:
        out_path = OUT_DIR / f"{field}_1h_window_pdf_timeseries.gif"

    if args.data_dir is not None:
        print(f"Loading {field} from gzip directory {args.data_dir} ...")
        series = load_field_series_from_dir(args.data_dir, field)
    else:
        print(f"Loading {field} from CSV {args.csv} ...")
        series = load_field_series(args.csv, field)

    times = frame_times(series, args.step_minutes, args.max_frames)
    out_path = make_animation(
        series, times, out_path, args.fps, args.dpi, args.background_points, metadata
    )
    print(f"Saved {out_path}")
    print(
        f"Field: {field} ({metadata.label}, {metadata.unit}) | Frames: {len(times)} | "
        f"Window: 1 hour | Step: {args.step_minutes:g} minutes"
    )
    print(f"Data range: {series.index.min()} to {series.index.max()} | Points: {len(series)}")


if __name__ == "__main__":
    main()


# Compatibility aliases for callers that imported the old PM-specific names.
load_pm_series = load_field_series
load_pm_series_from_dir = load_field_series_from_dir
