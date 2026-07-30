#!/usr/bin/env python3
"""Side-by-side wind-rose / particulate-channel animation over one window.

Left: the hourly WIMDA wind rose (identical petals, colors, and smooth
hour-to-hour interpolation as ``mintsWindRoseAnimation.py``) with a condensed
one-row speed-class legend beneath it.

Right: the PM or PC time series for the same window with a moving one-hour PDF
panel hugging the shared y-axis, restyled onto the wind rose's dark teal
theme. The PDF axis rescales per frame, which keeps both low-density PC
windows and narrow, high-density windows legible.

Both panels are driven by one clock.  The wind rose shows the hour bucket
[t, t+1h); the particulate window is the same forward-looking hour, so at every
keyframe the two panels describe the exact same 60 minutes.
"""

from __future__ import annotations

import argparse
import gc
import glob
import os
import pickle
import subprocess
import sys
import tempfile
from pathlib import Path

_REPO_ROOT = str(Path(__file__).resolve().parents[1])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

_CACHE_DIR = Path(tempfile.gettempdir()) / "mints_failure_analysis_matplotlib"
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
from matplotlib.patches import FancyBboxPatch
from matplotlib.ticker import MaxNLocator

matplotlib.rcParams["animation.ffmpeg_path"] = imageio_ffmpeg.get_ffmpeg_exe()

from safe.animation import FieldMetadata, field_bounds, field_metadata, pdf_axis_upper_limit, pdf_for_window

try:
    from . import mintsWindRoseAnimation as windrose
    from .mintsWindRoseAnimation import (
        ACCENT,
        BASELINE,
        GRIDLINE,
        INK_MUTED,
        INK_PRIMARY,
        INK_SECONDARY,
        PAGE,
        SPEED_COLORS,
        SPEED_EDGES,
        SURFACE,
        AnimFrame,
        draw_rose,
        hourly_counts,
        interpolate_frames,
        limit_frames,
        load_wind_data,
    )
except ImportError:  # direct script execution from mintsXU4/
    import mintsWindRoseAnimation as windrose
    from mintsWindRoseAnimation import (
    ACCENT,
    BASELINE,
    GRIDLINE,
    INK_MUTED,
    INK_PRIMARY,
    INK_SECONDARY,
    PAGE,
    SPEED_COLORS,
    SPEED_EDGES,
    SURFACE,
    AnimFrame,
    draw_rose,
    hourly_counts,
    interpolate_frames,
    limit_frames,
    load_wind_data,
    )

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_WIND_CSV = SCRIPT_DIR / "data" / "valo_node_01_wimda_1s" / "valo_node_01_wimda_last_7d_1s.csv.gz"
DEFAULT_PM_DIR = SCRIPT_DIR / "data" / "valo_node_01_1s"
DEFAULT_OUT = SCRIPT_DIR / "output" / "windrose_videos" / "pm1_0_wind_rose_sidebyside_last_7d.mp4"

WINDOW = pd.Timedelta(hours=1)
PANEL = "#12403c"  # slightly raised surface for the stats card
NVENC_EXTRA_ARGS = [
    "-preset", "p4",
    "-tune", "hq",
    "-rc", "vbr",
    "-cq", "23",
    "-b:v", "0",
    "-pix_fmt", "yuv420p",
]
LIBX264_EXTRA_ARGS = ["-pix_fmt", "yuv420p"]

# Clean light theme: white page, near-black ink, deep-teal accent.  The jet
# speed/PM colors are shared with the dark theme and read fine on white.
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


def apply_theme(theme: str) -> None:
    """Swap the shared palette for the requested theme.

    The wind-rose module reads its colors at draw time from its own globals,
    so the light palette has to be written both here and there.
    """
    if theme == "dark":
        return
    for name, value in LIGHT_THEME.items():
        globals()[name] = value
        if hasattr(windrose, name):
            setattr(windrose, name, value)


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


def load_hourly_wind_cache(cache_path: Path) -> list[tuple[pd.Timestamp, np.ndarray, int]]:
    """Load and validate a trusted local cache of hourly wind matrices."""
    cache_path = Path(cache_path)
    if not cache_path.is_file():
        raise FileNotFoundError(f"Hourly wind cache was not found: {cache_path}")
    with cache_path.open("rb") as handle:
        hourly = pickle.load(handle)
    if not isinstance(hourly, list) or not hourly:
        raise ValueError(f"Hourly wind cache is empty or malformed: {cache_path}")
    for row in hourly:
        if not isinstance(row, tuple) or len(row) != 3:
            raise ValueError(f"Hourly wind cache contains a malformed row: {cache_path}")
    return hourly


def load_hourly_wind(
    wind_csv: Path,
    wind_hourly_cache: Path | None,
    max_frames: int | None,
) -> list[tuple[pd.Timestamp, np.ndarray, int]]:
    """Load hourly wind matrices, preferring an explicit cache when given."""
    if wind_hourly_cache is not None:
        hourly = load_hourly_wind_cache(wind_hourly_cache)
    else:
        hourly = hourly_counts(load_wind_data(wind_csv))
    return limit_frames(hourly, max_frames)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Side-by-side hourly wind rose and PM/PC moving 1-hour window PDF video."
    )
    parser.add_argument("--wind-csv", type=Path, default=DEFAULT_WIND_CSV, help="WIMDA long-format CSV/CSV.GZ export.")
    parser.add_argument(
        "--wind-hourly-cache",
        type=Path,
        default=None,
        help="Trusted pickle cache of hourly wind matrices. Takes precedence over --wind-csv.",
    )
    parser.add_argument(
        "--pm-dir", "--data-dir", dest="data_dir", type=Path, default=DEFAULT_PM_DIR,
        help="Directory of valo_node_01_*.csv.gz daily PM/PC files (--pm-dir is retained for compatibility).",
    )
    parser.add_argument("--field", default="pm1_0", help="Available PM or PC channel (e.g. pm1_0, pm2_5, pc0_1).")
    parser.add_argument("--out", type=Path, default=None, help="MP4 output path.")
    parser.add_argument("--fps", type=int, default=60, help="Frames per second.")
    parser.add_argument("--dpi", type=int, default=100, help="Render DPI (figure is 19.2 x 10.8 in, so 100 dpi = 1920x1080).")
    parser.add_argument("--max-frames", type=int, default=None, help="Optional real-hour cap for previews (applied before interpolation).")
    parser.add_argument(
        "--interp-steps",
        type=int,
        default=10,
        help="Sub-frames rendered per hour-to-hour transition (1 = no interpolation). "
             "The default pairs with 60 fps to keep the same 6-hours-per-second pace "
             "the 30 fps / 5-step wind-rose video uses, just twice as smooth.",
    )
    parser.add_argument("--speed-unit", default="m/s", help="Label for windSpeedMetersPerSecond.")
    parser.add_argument(
        "--encoder",
        choices=("auto", "h264_nvenc", "libx264"),
        default="auto",
        help="H.264 encoder. 'auto' prefers NVENC and falls back to libx264.",
    )
    parser.add_argument(
        "--history-label",
        default="7-day window",
        help="Subtitle prefix describing the rendered time span.",
    )
    parser.add_argument(
        "--theme",
        choices=("dark", "light"),
        default="dark",
        help="Color theme: the original dark teal, or a clean white background.",
    )
    return parser.parse_args()


def load_field_series(data_dir: Path, field: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.Series:
    """Stream one PM/PC field from daily gzip files overlapping [start, end]."""
    files = sorted(glob.glob(str(Path(data_dir) / "valo_node_01_*.csv.gz")))
    if not files:
        raise FileNotFoundError(f"No valo_node_01_*.csv.gz files found in {data_dir}")

    keep = []
    for fpath in files:
        stem = Path(fpath).name.replace(".csv.gz", "")
        try:
            day_start = pd.Timestamp(stem.split("_")[-2], tz="UTC")
            day_end = pd.Timestamp(stem.split("_")[-1], tz="UTC") + pd.Timedelta(days=1)
        except (ValueError, IndexError):
            keep.append(fpath)  # unparseable name: let the content filter decide
            continue
        if day_end >= start and day_start <= end:
            keep.append(fpath)
    if not keep:
        raise ValueError(f"No daily files in {data_dir} overlap {start} .. {end}")

    frames = []
    available_fields = set()
    for fpath in keep:
        try:
            chunk = pd.read_csv(fpath, compression="gzip", comment="#", usecols=["_time", "_value", "_field"])
        except (ValueError, pd.errors.EmptyDataError):
            continue  # header-only / empty chunk files (sensor offline gaps)
        available_fields.update(chunk["_field"].dropna().astype(str).unique())
        chunk = chunk.loc[chunk["_field"] == field, ["_time", "_value"]]
        if not chunk.empty:
            # Convert each day immediately. Retaining millions of timestamp
            # strings until the final concat used several times more memory
            # and made parallel field renders unnecessarily expensive.
            index = pd.to_datetime(chunk["_time"], utc=True, errors="coerce")
            values = pd.to_numeric(chunk["_value"], errors="coerce")
            block = pd.Series(values.to_numpy(), index=index, name=field)
            frames.append(block[block.index.notna()].dropna())
    if not frames:
        raise ValueError(
            f"Requested field '{field}' was not found in the {len(keep)} overlapping files. "
            f"Available fields: {sorted(available_fields)}"
        )

    series = pd.concat(frames).sort_index()
    series = series.loc[(series.index >= start) & (series.index <= end)]

    low, high = field_bounds(field)
    series = series[(series >= low) & (series <= high)]
    if series.empty:
        raise ValueError(f"No valid {field} values inside {start} .. {end} after hard-bound filtering.")
    return series


def condensed_speed_labels(speed_unit: str) -> list[str]:
    """Compact range labels for the one-row legend, e.g. '0–0.5', '≥4'."""
    labels = []
    for lower, upper in zip(SPEED_EDGES[:-1], SPEED_EDGES[1:]):
        if np.isinf(upper):
            labels.append(f"≥{lower:g}")
        else:
            labels.append(f"{lower:g}–{upper:g}")
    return labels


def draw_condensed_legend(fig, x0: float, y: float, width: float, speed_unit: str) -> None:
    """One horizontal row of speed-class pills with compact range labels."""
    legend_ax = fig.add_axes((0.0, 0.0, 1.0, 1.0))
    legend_ax.set_axis_off()
    legend_ax.patch.set_alpha(0)

    labels = condensed_speed_labels(speed_unit)
    fig.text(x0, y + 0.028, f"Wind speed ({speed_unit})", fontsize=11, fontweight="bold", color=INK_SECONDARY)

    slot = width / len(labels)
    pill_w, pill_h = 0.020, 0.014
    for index, (color, label) in enumerate(zip(SPEED_COLORS, labels)):
        x = x0 + index * slot
        pill = FancyBboxPatch(
            (x, y - pill_h / 2), pill_w, pill_h,
            boxstyle=f"round,pad=0,rounding_size={pill_h / 2}",
            linewidth=0, facecolor=color, zorder=2, transform=fig.transFigure,
        )
        legend_ax.add_patch(pill)
        legend_ax.text(x + pill_w + 0.005, y, label, ha="left", va="center",
                       fontsize=9.5, color=INK_SECONDARY, transform=fig.transFigure)


def precompute_field_frames(series: pd.Series, frames: list[AnimFrame], y_grid: np.ndarray):
    """For every animation frame, compute the forward 1-hour PDF and stats.

    Returns (pdfs, stats) where stats rows are (n, mean, std, start_ts, end_ts).
    """
    pdfs = np.zeros((len(frames), y_grid.size))
    stats_rows = []
    index = series.index
    for i, (_, _, _, scrub_ts) in enumerate(frames):
        start_ts = scrub_ts
        end_ts = scrub_ts + WINDOW
        lo = index.searchsorted(start_ts, side="left")
        hi = index.searchsorted(end_ts, side="left")
        values = series.iloc[lo:hi]
        pdfs[i] = pdf_for_window(values, y_grid)
        if len(values):
            stats_rows.append((len(values), float(values.mean()), float(values.std(ddof=0)), start_ts, end_ts))
        else:
            stats_rows.append((0, np.nan, np.nan, start_ts, end_ts))
    return pdfs, stats_rows


def style_dark_axis(ax) -> None:
    ax.set_facecolor(SURFACE)
    for spine in ax.spines.values():
        spine.set_color(BASELINE)
        spine.set_linewidth(0.9)
    ax.tick_params(colors=INK_SECONDARY, labelsize=9.5)
    ax.grid(True, color=GRIDLINE, alpha=0.55, linewidth=0.7)


def make_animation(
    frames: list[AnimFrame],
    series: pd.Series,
    out_path: Path,
    fps: int,
    dpi: int,
    speed_unit: str,
    metadata: FieldMetadata,
    encoder: str = "libx264",
    history_label: str = "7-day window",
) -> Path:
    if fps <= 0:
        raise ValueError("--fps must be greater than zero")
    if dpi <= 0:
        raise ValueError("--dpi must be greater than zero")

    out_path = out_path.with_suffix(".mp4")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    radial_max = max(float(pct.sum(axis=1).max()) for _, pct, _, _ in frames)
    radial_max = max(5.0, np.ceil(radial_max * 1.15 / 5.0) * 5.0)

    # Field y-range over the whole 7-day window (quantile-clipped like the
    # original PDF animation so a single spike cannot flatten the video).
    ymin = max(0.0, float(series.quantile(0.001)) - 0.1 * float(series.std()))
    ymax = float(series.quantile(0.999)) + 0.1 * float(series.std())
    if not np.isfinite(ymin) or not np.isfinite(ymax) or ymin == ymax:
        ymin, ymax = float(series.min()), float(series.max() + 1.0)
    y_grid = np.linspace(ymin, ymax, 400)

    print("Precomputing per-frame PDFs ...")
    pdfs, stats_rows = precompute_field_frames(series, frames, y_grid)
    # PC density peaks can vary by orders of magnitude between hours. A
    # whole-history maximum flattens ordinary windows into an invisible line,
    # so reserve a small per-frame headroom instead.
    pdf_xmaxs = np.asarray([pdf_axis_upper_limit(pdf) for pdf in pdfs])

    label_math = (
        rf"{metadata.label[:2]}$_{{{metadata.label[2:]}}}$"
        if metadata.label.startswith(("PM", "PC")) else metadata.label
    )

    fig = plt.figure(figsize=(19.2, 10.8), dpi=dpi, facecolor=PAGE)

    # ---- header ---------------------------------------------------------
    fig.text(0.035, 0.955, "vaLo Node 01", fontsize=15, color=ACCENT, fontweight="bold")
    fig.text(0.035, 0.905, f"Hourly Wind Rose × {label_math}", fontsize=26,
             color=INK_PRIMARY, fontweight="bold")
    fig.text(0.036, 0.868,
             f"{history_label} · 1-second samples · both panels describe the same hour",
             fontsize=11, color=INK_MUTED)
    clock_text = fig.text(0.965, 0.925, "", fontsize=17, color=INK_PRIMARY,
                          fontweight="bold", ha="right")
    clock_sub = fig.text(0.965, 0.893, "", fontsize=10.5, color=INK_MUTED, ha="right")

    # ---- left: wind rose + condensed legend -----------------------------
    rose_ax = fig.add_axes((0.015, 0.135, 0.42, 0.64), projection="polar")
    draw_condensed_legend(fig, 0.055, 0.055, 0.36, speed_unit)

    # ---- right: PDF panel + time series (shared y) -----------------------
    pdf_ax = fig.add_axes((0.505, 0.135, 0.115, 0.70))
    ts_ax = fig.add_axes((0.625, 0.135, 0.345, 0.70), sharey=pdf_ax)

    dates = mdates.date2num(series.index.to_pydatetime())
    background_points = 6000
    if len(series) > background_points:
        bg_idx = np.linspace(0, len(series) - 1, background_points, dtype=int)
        bg_dates, bg_values = dates[bg_idx], series.to_numpy()[bg_idx]
    else:
        bg_dates, bg_values = dates, series.to_numpy()

    style_dark_axis(ts_ax)
    ts_ax.plot(bg_dates, bg_values, "-", color=INK_MUTED, linewidth=0.55, alpha=0.5)
    window_line, = ts_ax.plot([], [], "-", color=ACCENT, linewidth=1.7, zorder=5)
    current_line = ts_ax.axvline(bg_dates[0], color=INK_PRIMARY, linewidth=1.0, alpha=0.7, zorder=4)
    window_band = ts_ax.axvspan(bg_dates[0], bg_dates[0], color=ACCENT, alpha=0.14, zorder=1)

    ts_ax.set_ylim(ymin, ymax)
    ts_ax.set_xlim(dates[0], dates[-1])
    ts_ax.yaxis.tick_right()
    ts_ax.yaxis.set_label_position("right")
    ts_ax.set_ylabel(f"{label_math} ({metadata.unit})", color=INK_SECONDARY, fontsize=12)
    locator = mdates.AutoDateLocator()
    ts_ax.xaxis.set_major_locator(locator)
    ts_ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
    for text in ts_ax.get_xticklabels() + ts_ax.get_yticklabels():
        text.set_color(INK_SECONDARY)

    style_dark_axis(pdf_ax)
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
    # low values = blue, high values = red, matching the rose color family).
    jet_gradient = np.linspace(0.0, 1.0, 256).reshape(-1, 1)
    grad_state = {"im": None, "clip": None}

    stats_text = ts_ax.text(
        0.012, 0.985, "", transform=ts_ax.transAxes, ha="left", va="top",
        fontsize=9.5, color=INK_SECONDARY, zorder=6,
        bbox={"boxstyle": "round,pad=0.4", "facecolor": PANEL, "edgecolor": BASELINE, "alpha": 0.92},
    )

    def update(index: int):
        nonlocal window_band
        label_ts, percentages, label_samples, scrub_ts = frames[index]

        draw_rose(rose_ax, label_ts, percentages, label_samples, radial_max)
        rose_ax.set_title("")  # the shared header clock replaces the per-rose title

        clock_text.set_text(f"{label_ts:%A, %B %d %Y — %H:00 UTC}")
        clock_sub.set_text(f"wind n = {label_samples:,} · one-second samples in this hour")

        n, mean_val, std_val, start_ts, end_ts = stats_rows[index]
        start_num = mdates.date2num(start_ts)
        end_num = mdates.date2num(end_ts)

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
                f"{label_math} window {start_ts:%b %d %H:%M}–{end_ts:%H:%M} UTC\n"
                f"n={n:,}   mean={mean_val:.2f}   std={std_val:.2f} {metadata.unit}"
            )
        else:
            pdf_mean.set_ydata([ymin, ymin])
            stats_text.set_text(
                f"{label_math} window {start_ts:%b %d %H:%M}–{end_ts:%H:%M} UTC\nn=0 (no {metadata.label} data)"
            )
        return rose_ax.patches

    video = animation.FuncAnimation(fig, update, frames=len(frames), interval=1000 / fps, blit=False)
    writer = animation.FFMpegWriter(
        fps=fps,
        codec=encoder,
        extra_args=encoder_extra_args(encoder),
    )
    try:
        video.save(out_path, writer=writer, dpi=dpi, savefig_kwargs={"facecolor": PAGE})
    finally:
        plt.close(fig)
        plt.close("all")
        gc.collect()
    return out_path


def main() -> None:
    args = parse_args()
    apply_theme(args.theme)
    metadata = field_metadata(args.field)
    field = metadata.field
    suffix = "" if args.theme == "dark" else f"_{args.theme}"
    out_path = args.out or (
        SCRIPT_DIR / "output" / "windrose_videos" / f"{field}_wind_rose_sidebyside_last_7d{suffix}.mp4"
    )

    encoder = resolve_encoder(args.encoder)
    wind_source = args.wind_hourly_cache or args.wind_csv
    print(f"Loading hourly wind data from {wind_source} ...")
    hourly = load_hourly_wind(args.wind_csv, args.wind_hourly_cache, args.max_frames)
    frames = interpolate_frames(hourly, args.interp_steps)

    window_start = frames[0][0]
    window_end = frames[-1][0] + WINDOW
    print(f"Wind hours: {len(hourly)} covering {window_start} .. {window_end}")

    print(f"Loading {field} ({metadata.label}, {metadata.unit}) from {args.data_dir} ...")
    series = load_field_series(args.data_dir, field, window_start, window_end)
    print(f"{metadata.label} points: {len(series):,} covering {series.index.min()} .. {series.index.max()}")

    print(f"Encoding with {encoder} ...")
    try:
        out = make_animation(
            frames,
            series,
            out_path,
            args.fps,
            args.dpi,
            args.speed_unit,
            metadata,
            encoder=encoder,
            history_label=args.history_label,
        )
    finally:
        plt.close("all")
        gc.collect()
    print(f"Saved {out}")
    print(f"Rendered frames: {len(frames)} at {args.fps} fps ({len(frames) / args.fps:.1f} s of video)")


if __name__ == "__main__":
    main()


# Compatibility aliases for callers of the historical PM-specific helpers.
load_pm_series = load_field_series
precompute_pm_frames = precompute_field_frames
