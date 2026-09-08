#!/usr/bin/env python3
"""Moving-window PM animation for the Fort Worth BAM station export.

This is the right-hand panel of ``mintsPmWindCombined.py`` on its own: a narrow
PDF axis hugging a wider time series that share one y-axis, restyled onto the
same dark teal theme (with the same light-theme option) but with no wind rose.

The source file is an hourly BAM report, not 1-second node data, so the moving
window has to be much wider than the original one hour -- a one-hour window
would hold a single sample and the PDF panel would collapse to a spike. The
default is a 7-day window stepped forward one hour per frame, which keeps ~168
samples in view at all times.
"""

from __future__ import annotations

import argparse
import gc
import os
import subprocess
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
import imageio_ffmpeg
import matplotlib.animation as animation
import matplotlib.dates as mdates
import matplotlib.patches as patches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.ticker import MaxNLocator

matplotlib.rcParams["animation.ffmpeg_path"] = imageio_ffmpeg.get_ffmpeg_exe()

from safe.animation import pdf_axis_upper_limit, pdf_for_window

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CSV = SCRIPT_DIR / "data" / "FortWorthData.csv"
DEFAULT_OUT_DIR = SCRIPT_DIR / "output" / "FortWorth"

# The BAM report carries four preamble lines before the real column header.
HEADER_ROWS = 4
TIME_COLUMN = "Time"
DEFAULT_FIELD = "ConcHR(ug/m3)"
SENTINEL = 99999.0

STATION_TITLE = "Fort Worth BAM Station 310"
FIELD_LABELS = {
    "ConcRT(ug/m3)": ("PM real-time concentration", r"PM ($\mu$g/m$^3$)"),
    "ConcHR(ug/m3)": ("PM hourly average concentration", r"PM ($\mu$g/m$^3$)"),
    "ConcS(ug/m3)": ("PM standard concentration", r"PM ($\mu$g/m$^3$)"),
}

# Dark teal theme, matched to mintsWindRoseAnimation / mintsPmWindCombined.
PAGE = "#0e2e2c"
SURFACE = "#0e2e2c"
INK_PRIMARY = "#ffffff"
INK_SECONDARY = "#cfe8e3"
INK_MUTED = "#8fbdb6"
GRIDLINE = "#2f5450"
BASELINE = "#3f6b65"
ACCENT = "#7fe0c8"
PANEL = "#12403c"

LIGHT_THEME = {
    "PAGE": "#ffffff",
    "SURFACE": "#ffffff",
    "INK_PRIMARY": "#16333d",
    "INK_SECONDARY": "#3d5c66",
    "INK_MUTED": "#8aa0a7",
    "GRIDLINE": "#dde6e9",
    "BASELINE": "#b7c9ce",
    "ACCENT": "#0e7c6b",
    "PANEL": "#f4f8f7",
}

NVENC_EXTRA_ARGS = [
    "-preset", "p4",
    "-tune", "hq",
    "-rc", "vbr",
    "-cq", "23",
    "-b:v", "0",
    "-pix_fmt", "yuv420p",
]
LIBX264_EXTRA_ARGS = ["-pix_fmt", "yuv420p"]


def apply_theme(theme: str) -> None:
    """Swap the module palette for the requested theme."""
    if theme == "dark":
        return
    for name, value in LIGHT_THEME.items():
        globals()[name] = value


def available_ffmpeg_encoders(ffmpeg_path: str | None = None) -> set[str]:
    """Return video encoders advertised by the configured FFmpeg binary."""
    executable = ffmpeg_path or imageio_ffmpeg.get_ffmpeg_exe()
    result = subprocess.run(
        [executable, "-hide_banner", "-encoders"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Could not inspect FFmpeg encoders: {result.stderr.strip()}")
    return {
        line.split()[1]
        for line in result.stdout.splitlines()
        if len(line.split()) >= 2 and line.lstrip().startswith("V")
    }


def resolve_encoder(requested: str, encoders: set[str] | None = None) -> str:
    """Resolve ``auto`` and fail clearly for an unavailable explicit codec."""
    available = encoders if encoders is not None else available_ffmpeg_encoders()
    if requested == "auto":
        return "h264_nvenc" if "h264_nvenc" in available else "libx264"
    if requested not in available:
        raise RuntimeError(
            f"FFmpeg encoder '{requested}' is unavailable. "
            f"Available H.264 choices: {sorted(e for e in available if '264' in e)}"
        )
    return requested


def encoder_extra_args(encoder: str) -> list[str]:
    """Return stable MP4 arguments for the selected H.264 encoder."""
    return list(NVENC_EXTRA_ARGS if encoder == "h264_nvenc" else LIBX264_EXTRA_ARGS)


def load_station_series(csv_path: Path, field: str) -> pd.Series:
    """Load one BAM channel as a time-indexed series, sentinel-free."""
    frame = pd.read_csv(csv_path, skiprows=HEADER_ROWS)
    if TIME_COLUMN not in frame.columns:
        raise ValueError(f"'{TIME_COLUMN}' column not found in {csv_path}. Columns: {list(frame.columns)}")
    if field not in frame.columns:
        raise ValueError(f"Field '{field}' not found in {csv_path}. Columns: {list(frame.columns)}")

    index = pd.to_datetime(frame[TIME_COLUMN], errors="coerce")
    values = pd.to_numeric(frame[field], errors="coerce")
    series = pd.Series(values.to_numpy(), index=index, name=field)
    series = series[series.index.notna()].dropna().sort_index()

    # 99999 is the BAM's "no data" sentinel; negatives are baseline drift.
    series = series[(series != SENTINEL) & (series >= 0.0)]
    if series.empty:
        raise ValueError(f"No valid '{field}' samples remain in {csv_path} after filtering.")
    return series


def build_window_starts(
    series: pd.Series,
    window: pd.Timedelta,
    step: pd.Timedelta,
    max_frames: int | None,
) -> list[pd.Timestamp]:
    """Evenly spaced window start times covering the whole record."""
    first = series.index[0]
    last = series.index[-1]
    if last - first <= window:
        raise ValueError(
            f"The record spans {last - first}, which is not longer than the {window} window."
        )
    starts = list(pd.date_range(first, last - window, freq=step))
    if not starts:
        raise ValueError("No window start times were produced; try a smaller --step or --window.")
    if max_frames is not None and max_frames > 0:
        starts = starts[:max_frames]
    return starts


def precompute_window_frames(series: pd.Series, starts, window: pd.Timedelta, y_grid: np.ndarray):
    """Per-frame PDF plus (n, mean, std, start, end) stats for each window."""
    pdfs = np.zeros((len(starts), y_grid.size))
    stats_rows = []
    index = series.index
    for i, start_ts in enumerate(starts):
        end_ts = start_ts + window
        lo = index.searchsorted(start_ts, side="left")
        hi = index.searchsorted(end_ts, side="left")
        values = series.iloc[lo:hi]
        pdfs[i] = pdf_for_window(values, y_grid)
        if len(values):
            stats_rows.append((len(values), float(values.mean()), float(values.std(ddof=0)), start_ts, end_ts))
        else:
            stats_rows.append((0, np.nan, np.nan, start_ts, end_ts))
    return pdfs, stats_rows


def style_axis(ax) -> None:
    ax.set_facecolor(SURFACE)
    for spine in ax.spines.values():
        spine.set_color(BASELINE)
        spine.set_linewidth(0.9)
    ax.tick_params(colors=INK_SECONDARY, labelsize=9.5)
    ax.grid(True, color=GRIDLINE, alpha=0.55, linewidth=0.7)


def format_window(window: pd.Timedelta) -> str:
    """Human label for a window length, e.g. '7-day', '24-hour'."""
    total_hours = window.total_seconds() / 3600.0
    if total_hours >= 24 and abs(total_hours % 24) < 1e-9:
        days = int(total_hours // 24)
        return f"{days}-day"
    return f"{total_hours:g}-hour"


def make_animation(
    series: pd.Series,
    starts,
    window: pd.Timedelta,
    out_path: Path,
    fps: int,
    dpi: int,
    field: str,
    encoder: str = "libx264",
    clip_quantile: float = 0.995,
) -> Path:
    if fps <= 0:
        raise ValueError("--fps must be greater than zero")
    if dpi <= 0:
        raise ValueError("--dpi must be greater than zero")

    out_path = out_path.with_suffix(".mp4")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    channel_name, axis_label = FIELD_LABELS.get(field, (field, field))
    window_label = format_window(window)

    # Quantile-clipped y-range so a single spike cannot flatten the video. The
    # hourly BAM record is far more skewed than the 1-second node data (0.999
    # sits near 150 while 99% of hours are under 36), so the default clip is
    # tighter than the 0.999 the 1-second animations use.
    ymin = max(0.0, float(series.quantile(1.0 - clip_quantile)) - 0.1 * float(series.std()))
    ymax = float(series.quantile(clip_quantile)) + 0.1 * float(series.std())
    if not np.isfinite(ymin) or not np.isfinite(ymax) or ymin == ymax:
        ymin, ymax = float(series.min()), float(series.max() + 1.0)
    y_grid = np.linspace(ymin, ymax, 400)

    print("Precomputing per-frame PDFs ...")
    pdfs, stats_rows = precompute_window_frames(series, starts, window, y_grid)
    pdf_xmaxs = np.asarray([pdf_axis_upper_limit(pdf) for pdf in pdfs])

    fig = plt.figure(figsize=(19.2, 10.8), dpi=dpi, facecolor=PAGE)

    # ---- header ---------------------------------------------------------
    fig.text(0.045, 0.945, STATION_TITLE, fontsize=15, color=ACCENT, fontweight="bold")
    fig.text(0.045, 0.888, "PM Hourly", fontsize=30, color=INK_PRIMARY, fontweight="bold")
    fig.text(0.046, 0.848,
             f"{channel_name} · hourly samples · moving {window_label} window",
             fontsize=12, color=INK_MUTED)
    clock_text = fig.text(0.955, 0.910, "", fontsize=17, color=INK_PRIMARY,
                          fontweight="bold", ha="right")
    clock_sub = fig.text(0.955, 0.874, "", fontsize=10.5, color=INK_MUTED, ha="right")

    # ---- PDF panel + time series (shared y) ------------------------------
    pdf_ax = fig.add_axes((0.055, 0.095, 0.115, 0.71))
    ts_ax = fig.add_axes((0.175, 0.095, 0.775, 0.71), sharey=pdf_ax)

    dates = mdates.date2num(series.index.to_pydatetime())
    background_points = 6000
    if len(series) > background_points:
        bg_idx = np.linspace(0, len(series) - 1, background_points, dtype=int)
        bg_dates, bg_values = dates[bg_idx], series.to_numpy()[bg_idx]
    else:
        bg_dates, bg_values = dates, series.to_numpy()

    style_axis(ts_ax)
    ts_ax.plot(bg_dates, bg_values, "-", color=INK_MUTED, linewidth=0.55, alpha=0.5)
    window_line, = ts_ax.plot([], [], "-", color=ACCENT, linewidth=1.7, zorder=5)
    current_line = ts_ax.axvline(bg_dates[0], color=INK_PRIMARY, linewidth=1.0, alpha=0.7, zorder=4)
    window_band = ts_ax.axvspan(bg_dates[0], bg_dates[0], color=ACCENT, alpha=0.14, zorder=1)

    ts_ax.set_ylim(ymin, ymax)
    ts_ax.set_xlim(dates[0], dates[-1])
    ts_ax.yaxis.tick_right()
    ts_ax.yaxis.set_label_position("right")
    ts_ax.set_ylabel(axis_label, color=INK_SECONDARY, fontsize=12)
    locator = mdates.AutoDateLocator()
    ts_ax.xaxis.set_major_locator(locator)
    ts_ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
    for text in ts_ax.get_xticklabels() + ts_ax.get_yticklabels():
        text.set_color(INK_SECONDARY)

    style_axis(pdf_ax)
    pdf_line, = pdf_ax.plot([], [], color=INK_PRIMARY, linewidth=1.5, zorder=3)
    pdf_mean = pdf_ax.axhline(ymin, color=INK_PRIMARY, linestyle="--", linewidth=1.0, alpha=0.7, zorder=4)
    pdf_ax.set_ylim(ymin, ymax)

    def set_pdf_xlim(xmax: float) -> None:
        """Set the inverted density scale for the current PDF curve."""
        pdf_ax.set_xlim(xmax, 0.0)
        # Skip zero so it cannot collide with the adjoining date label.
        ticks = MaxNLocator(nbins=4, steps=[1, 2, 5, 10]).tick_values(0.0, xmax)
        pdf_ax.set_xticks([tick for tick in ticks if 0.0 < tick <= xmax])

    set_pdf_xlim(float(pdf_xmaxs[0]))
    pdf_ax.ticklabel_format(axis="x", style="sci", scilimits=(-2, 3), useMathText=True)
    pdf_ax.set_xlabel("PDF", color=INK_SECONDARY, fontsize=10.5)
    pdf_ax.yaxis.tick_left()
    pdf_ax.tick_params(axis="y", labelleft=False)
    pdf_ax.spines["right"].set_color(INK_SECONDARY)
    pdf_ax.spines["right"].set_linewidth(1.4)

    # Smooth vertical jet ramp clipped under the PDF curve (project convention:
    # low values = blue, high values = red).
    jet_gradient = np.linspace(0.0, 1.0, 256).reshape(-1, 1)
    grad_state = {"im": None, "clip": None}

    stats_text = ts_ax.text(
        0.012, 0.985, "", transform=ts_ax.transAxes, ha="left", va="top",
        fontsize=9.5, color=INK_SECONDARY, zorder=6,
        bbox={"boxstyle": "round,pad=0.4", "facecolor": PANEL, "edgecolor": BASELINE, "alpha": 0.92},
    )

    def update(index: int):
        nonlocal window_band
        n, mean_val, std_val, start_ts, end_ts = stats_rows[index]
        start_num = mdates.date2num(start_ts)
        end_num = mdates.date2num(end_ts)

        clock_text.set_text(f"{start_ts:%A, %B %d %Y — %H:00}")
        clock_sub.set_text(f"window {start_ts:%b %d %H:%M} → {end_ts:%b %d %H:%M}")

        lo = series.index.searchsorted(start_ts, side="left")
        hi = series.index.searchsorted(end_ts, side="left")
        window_line.set_data(dates[lo:hi], series.to_numpy()[lo:hi])
        current_line.set_xdata([start_num, start_num])
        window_band.remove()
        window_band = ts_ax.axvspan(start_num, end_num, color=ACCENT, alpha=0.14, zorder=1)

        pdf = pdfs[index]
        pdf_xmax = float(pdf_xmaxs[index])
        set_pdf_xlim(pdf_xmax)
        pdf_line.set_data(pdf, y_grid)

        if grad_state["im"] is not None:
            grad_state["im"].remove()
        if grad_state["clip"] is not None:
            grad_state["clip"].remove()
        verts = np.column_stack(
            [
                np.concatenate([[0.0], pdf, [0.0]]),
                np.concatenate([[y_grid[0]], y_grid, [y_grid[-1]]]),
            ]
        )
        grad_state["clip"] = patches.Polygon(
            verts, closed=True, transform=pdf_ax.transData, facecolor="none", edgecolor="none"
        )
        pdf_ax.add_patch(grad_state["clip"])
        grad_state["im"] = pdf_ax.imshow(
            jet_gradient, aspect="auto", cmap="jet", origin="lower",
            extent=[0.0, pdf_xmax, ymin, ymax], alpha=0.9, zorder=1,
        )
        grad_state["im"].set_clip_path(grad_state["clip"])

        if n:
            pdf_mean.set_ydata([mean_val, mean_val])
            stats_text.set_text(
                f"{window_label} window {start_ts:%b %d %Y %H:%M} – {end_ts:%b %d %Y %H:%M}\n"
                f"n={n:,}   mean={mean_val:.2f}   std={std_val:.2f} µg/m³"
            )
        else:
            pdf_mean.set_ydata([ymin, ymin])
            stats_text.set_text(
                f"{window_label} window {start_ts:%b %d %Y %H:%M} – {end_ts:%b %d %Y %H:%M}\nn=0 (no data)"
            )
        return ()

    video = animation.FuncAnimation(fig, update, frames=len(starts), interval=1000 / fps, blit=False)
    writer = animation.FFMpegWriter(fps=fps, codec=encoder, extra_args=encoder_extra_args(encoder))
    try:
        video.save(out_path, writer=writer, dpi=dpi, savefig_kwargs={"facecolor": PAGE})
    finally:
        plt.close(fig)
        plt.close("all")
        gc.collect()
    return out_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Moving-window PDF + time-series video for the Fort Worth BAM hourly export."
    )
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV, help="Fort Worth BAM report CSV.")
    parser.add_argument("--field", default=DEFAULT_FIELD, help="Channel to plot (default: hourly average).")
    parser.add_argument("--out", type=Path, default=None, help="MP4 output path.")
    parser.add_argument("--window", default="7D", help="Moving window length as a pandas offset (e.g. 24h, 7D).")
    parser.add_argument("--step", default="1h", help="How far the window advances per frame (e.g. 1h, 3h).")
    parser.add_argument("--fps", type=int, default=60, help="Frames per second.")
    parser.add_argument("--dpi", type=int, default=100, help="Render DPI (figure is 19.2 x 10.8 in, so 100 dpi = 1920x1080).")
    parser.add_argument("--max-frames", type=int, default=None, help="Optional frame cap for previews.")
    parser.add_argument(
        "--clip-quantile",
        type=float,
        default=0.995,
        help="Upper quantile used to clip the shared y-axis against rare spikes.",
    )
    parser.add_argument(
        "--encoder",
        choices=("auto", "h264_nvenc", "libx264"),
        default="auto",
        help="H.264 encoder. 'auto' prefers NVENC and falls back to libx264.",
    )
    parser.add_argument(
        "--theme",
        choices=("dark", "light"),
        default="dark",
        help="Color theme: the original dark teal, or a clean white background.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    apply_theme(args.theme)

    window = pd.Timedelta(args.window)
    step = pd.Timedelta(args.step)
    if window <= pd.Timedelta(0) or step <= pd.Timedelta(0):
        raise ValueError("--window and --step must both be positive durations.")

    suffix = "" if args.theme == "dark" else f"_{args.theme}"
    out_path = args.out or (
        DEFAULT_OUT_DIR / f"fortworth_pm_hourly_{format_window(window).replace('-', '')}_window{suffix}.mp4"
    )

    encoder = resolve_encoder(args.encoder)

    print(f"Loading {args.field} from {args.csv} ...")
    series = load_station_series(args.csv, args.field)
    print(f"Samples: {len(series):,} covering {series.index.min()} .. {series.index.max()}")

    starts = build_window_starts(series, window, step, args.max_frames)
    print(f"Frames: {len(starts):,} ({window} window advancing {step} per frame)")

    print(f"Encoding with {encoder} ...")
    try:
        out = make_animation(
            series, starts, window, out_path, args.fps, args.dpi, args.field,
            encoder=encoder, clip_quantile=args.clip_quantile,
        )
    finally:
        plt.close("all")
        gc.collect()
    print(f"Saved {out}")
    print(f"Rendered frames: {len(starts)} at {args.fps} fps ({len(starts) / args.fps:.1f} s of video)")


if __name__ == "__main__":
    main()
