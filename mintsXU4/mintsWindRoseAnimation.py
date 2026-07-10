#!/usr/bin/env python3
"""Render an hourly WIMDA wind-rose animation from an InfluxDB CSV export.

Each keyframe aggregates all valid one-second observations in one UTC hour.
The 16-point compass rose is broken into 22.5-degree petals; their stacked
radial segments show the frequency of the configured wind-speed classes.
Consecutive hourly keyframes are linearly interpolated into extra in-between
frames (see ``--interp-steps``) so the petals morph smoothly at a normal
video frame rate instead of jump-cutting once every 1/6 second.  A scrubber
strip beneath the rose shows where the current moment sits within the full
multi-day window.

Uses WIMDA's ``windAngleTrue`` (already geographic-north referenced, unlike
WIMWV's sensor-relative angle) and ``windSpeedMetersPerSecond`` (unambiguous
units, unlike WIMWV's unlabeled ``windSpeed``).
"""

from __future__ import annotations

import argparse
import os
import tempfile
from pathlib import Path

_CACHE_DIR = Path(tempfile.gettempdir()) / "mints_failure_analysis_matplotlib"
os.environ.setdefault("MPLCONFIGDIR", str(_CACHE_DIR / "matplotlib"))
os.environ.setdefault("XDG_CACHE_HOME", str(_CACHE_DIR / "xdg"))

import matplotlib

matplotlib.use("Agg")
import imageio_ffmpeg
import matplotlib.animation as animation
from matplotlib.patches import FancyBboxPatch
from matplotlib.ticker import MaxNLocator
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

matplotlib.rcParams["animation.ffmpeg_path"] = imageio_ffmpeg.get_ffmpeg_exe()


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CSV = SCRIPT_DIR / "data" / "valo_node_01_wimda_1s" / "valo_node_01_wimda_last_7d_1s.csv.gz"
DEFAULT_OUT = SCRIPT_DIR / "output" / "wind" / "wimda_last_7d_hourly_wind_rose.mp4"

DIRECTION_WIDTH = 22.5  # 16-point compass rose
COMPASS_LABELS = [
    "N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
    "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW",
]

# Speed-class edges (m/s) tuned to the last-7d quantile spread (p10-p90 spans
# roughly 0.4-2.4, median ~1.1) so seven classes carry comparable petal
# weight instead of dumping most samples into one bucket, with a long tail
# class for the rare gusts up to ~11 m/s.
SPEED_EDGES = np.array([0.0, 0.5, 1.0, 1.5, 2.0, 2.8, 4.0, np.inf])

# Jet colormap sampled at 7 even stops: blue = calm through red = strongest,
# matching the jet convention used across the project's PM animations.
SPEED_COLORS = ["#0000b6", "#0050ff", "#05ecf1", "#83ff73", "#ffe900", "#ff5500", "#9f0000"]

# Dark teal theme (professional wind-rose look).
PAGE = "#0e2e2c"
SURFACE = "#0e2e2c"
INK_PRIMARY = "#ffffff"
INK_SECONDARY = "#cfe8e3"
INK_MUTED = "#8fbdb6"
GRIDLINE = "#2f5450"
BASELINE = "#3f6b65"
ACCENT = "#7fe0c8"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate one 22.5-degree wind rose per hour and save them as an MP4."
    )
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV, help="Long-format InfluxDB CSV or CSV.GZ export.")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help="MP4 output path.")
    parser.add_argument("--fps", type=int, default=30, help="Frames per second.")
    parser.add_argument("--dpi", type=int, default=100, help="Render DPI.")
    parser.add_argument("--max-frames", type=int, default=None, help="Optional real-hour cap for previews (applied before interpolation).")
    parser.add_argument(
        "--interp-steps",
        type=int,
        default=5,
        help="Sub-frames rendered per hour-to-hour transition (1 = no interpolation, "
             "matches the old jump-cut look).",
    )
    parser.add_argument(
        "--speed-unit",
        default="m/s",
        help="Label for windSpeedMetersPerSecond.",
    )
    return parser.parse_args()


def load_wind_data(path: Path) -> pd.DataFrame:
    """Load paired angle/speed readings from a long-format Influx CSV export."""
    if not path.is_file():
        raise FileNotFoundError(f"Wind CSV was not found: {path}")

    raw = pd.read_csv(path, compression="infer", comment="#", usecols=["_time", "_field", "_value"])
    raw = raw.loc[raw["_field"].isin(["windAngleTrue", "windSpeedMetersPerSecond"])].copy()
    raw["_time"] = pd.to_datetime(raw["_time"], utc=True, errors="coerce")
    raw["_value"] = pd.to_numeric(raw["_value"], errors="coerce")
    raw = raw.dropna(subset=["_time", "_value"])

    wind = raw.pivot_table(index="_time", columns="_field", values="_value", aggfunc="last")
    missing = {"windAngleTrue", "windSpeedMetersPerSecond"}.difference(wind.columns)
    if missing:
        raise ValueError(f"Wind CSV is missing required fields: {sorted(missing)}")

    wind = wind[["windAngleTrue", "windSpeedMetersPerSecond"]].rename(
        columns={"windAngleTrue": "windAngle", "windSpeedMetersPerSecond": "windSpeed"}
    ).dropna().sort_index()
    # A valid azimuth is [0, 360); negative speed readings are invalid.
    wind = wind.loc[
        wind["windAngle"].between(0.0, 360.0, inclusive="left") & (wind["windSpeed"] >= 0.0)
    ].copy()
    if wind.empty:
        raise ValueError("No valid paired windAngle/windSpeed samples were found.")
    return wind


def hourly_counts(wind: pd.DataFrame) -> list[tuple[pd.Timestamp, np.ndarray, int]]:
    """Return hourly 16-sector x speed-class percentage matrices."""
    sector_count = int(round(360.0 / DIRECTION_WIDTH))
    output: list[tuple[pd.Timestamp, np.ndarray, int]] = []
    hours = [(timestamp, hour) for timestamp, hour in wind.groupby(pd.Grouper(freq="1h")) if not hour.empty]
    typical_samples = float(np.median([len(hour) for _, hour in hours]))
    # A trailing partial hour can still be useful, but discard a leading/trailing
    # fragment that carries less than half an ordinary hour of observations.
    minimum_samples = max(1, int(np.ceil(typical_samples * 0.5)))
    for timestamp, hour in hours:
        if len(hour) < minimum_samples:
            continue
        direction = hour["windAngle"].to_numpy(dtype=float) % 360.0
        speed = hour["windSpeed"].to_numpy(dtype=float)
        sectors = ((direction + DIRECTION_WIDTH / 2) // DIRECTION_WIDTH).astype(int) % sector_count
        speed_classes = np.digitize(speed, SPEED_EDGES, right=False) - 1
        speed_classes = np.clip(speed_classes, 0, len(SPEED_EDGES) - 2)

        counts = np.zeros((sector_count, len(SPEED_EDGES) - 1), dtype=float)
        np.add.at(counts, (sectors, speed_classes), 1.0)
        output.append((timestamp, counts * 100.0 / len(hour), len(hour)))
    if not output:
        raise ValueError("No non-empty hourly windows were available.")
    return output


def speed_labels(speed_unit: str) -> list[str]:
    labels = []
    for lower, upper in zip(SPEED_EDGES[:-1], SPEED_EDGES[1:]):
        if np.isinf(upper):
            labels.append(f"≥ {lower:g} {speed_unit}")
        else:
            labels.append(f"{lower:g} – {upper:g} {speed_unit}")
    return labels


def draw_rose(ax, timestamp: pd.Timestamp, percentages: np.ndarray, samples: int, radial_max: float) -> None:
    """Draw one stacked 22.5-degree rose on an existing polar axis."""
    ax.clear()
    ax.set_facecolor(SURFACE)
    ax.set_theta_zero_location("N")
    ax.set_theta_direction(-1)
    sector_count = percentages.shape[0]
    theta = np.deg2rad(np.arange(sector_count) * DIRECTION_WIDTH)
    width = np.deg2rad(DIRECTION_WIDTH * 0.88)
    bottom = np.zeros(sector_count)

    for index, color in enumerate(SPEED_COLORS):
        heights = percentages[:, index]
        ax.bar(
            theta,
            heights,
            width=width,
            bottom=bottom,
            color=color,
            edgecolor=PAGE,
            linewidth=1.2,
            align="center",
            zorder=3,
        )
        bottom += heights

    ax.set_thetagrids(np.arange(0, 360, DIRECTION_WIDTH), labels=COMPASS_LABELS)
    ax.tick_params(axis="x", colors=INK_SECONDARY, labelsize=11)
    for label in ax.get_xticklabels():
        label.set_fontweight("bold")

    ticks = MaxNLocator(nbins=5, steps=[1, 2, 5, 10]).tick_values(0, radial_max)
    ticks = ticks[(ticks >= 0) & (ticks <= radial_max)]
    ax.set_ylim(0, radial_max)
    ax.set_yticks(ticks)
    ax.set_rlabel_position(22.5)
    ax.yaxis.set_major_formatter(lambda value, _pos: f"{value:g}%")
    ax.tick_params(axis="y", colors=ACCENT, labelsize=9.5)
    for label in ax.get_yticklabels():
        label.set_fontweight("bold")
    ax.grid(True, color=GRIDLINE, alpha=1.0, linestyle="-", linewidth=0.9, zorder=0)
    ax.spines["polar"].set_color(BASELINE)
    ax.spines["polar"].set_linewidth(0.9)

    ax.set_title(
        f"{timestamp:%A, %B %d %Y — %H:00 UTC}    ·    n = {samples:,} one-second samples",
        va="bottom",
        pad=34,
        fontsize=13,
        color=INK_PRIMARY,
        fontweight="bold",
    )


def draw_timeline(ax, timestamp: pd.Timestamp, start: pd.Timestamp, end: pd.Timestamp) -> None:
    """Draw a horizontal scrubber showing the current hour within the full window."""
    ax.clear()
    ax.set_facecolor(PAGE)
    span = (end - start).total_seconds()
    position = 0.0 if span <= 0 else (timestamp - start).total_seconds() / span
    position = min(max(position, 0.0), 1.0)

    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    ax.plot([0, 1], [0.5, 0.5], color=GRIDLINE, linewidth=3, solid_capstyle="round", zorder=1)
    ax.plot([0, position], [0.5, 0.5], color=ACCENT, linewidth=3, solid_capstyle="round", zorder=2)
    ax.plot(position, 0.5, marker="o", markersize=8, color=ACCENT, markeredgecolor=PAGE, markeredgewidth=1.6, zorder=3)

    day_starts = pd.date_range(start.floor("D"), end.ceil("D"), freq="1D")
    for day in day_starts:
        if day < start or day > end:
            continue
        day_pos = (day - start).total_seconds() / span if span > 0 else 0.0
        ax.plot([day_pos, day_pos], [0.36, 0.64], color=BASELINE, linewidth=1, zorder=1)
        ax.text(day_pos, 0.08, f"{day:%b %d}", ha="center", va="top", fontsize=8, color=INK_MUTED)


def limit_frames(frames: list[tuple[pd.Timestamp, np.ndarray, int]], max_frames: int | None) -> list[tuple[pd.Timestamp, np.ndarray, int]]:
    if max_frames is None or max_frames <= 0 or len(frames) <= max_frames:
        return frames
    indices = np.linspace(0, len(frames) - 1, max_frames, dtype=int)
    return [frames[index] for index in indices]


AnimFrame = tuple[pd.Timestamp, np.ndarray, int, pd.Timestamp]  # label_ts, percentages, label_samples, scrub_ts


def interpolate_frames(frames: list[tuple[pd.Timestamp, np.ndarray, int]], steps: int) -> list[AnimFrame]:
    """Linearly interpolate the petal heights between each pair of hourly
    frames so playback morphs smoothly instead of jump-cutting once an hour.

    The displayed hour label and sample count stay pinned to the segment's
    starting hour (so the title never shows a fabricated fractional hour);
    only the petal heights and the timeline scrubber move continuously.
    """
    if steps <= 1 or len(frames) < 2:
        return [(ts, pct, n, ts) for ts, pct, n in frames]

    output: list[AnimFrame] = []
    for (t0, p0, n0), (t1, p1, _) in zip(frames, frames[1:]):
        for step in range(steps):
            alpha = step / steps
            scrub_ts = t0 + (t1 - t0) * alpha
            percentages = p0 * (1 - alpha) + p1 * alpha
            output.append((t0, percentages, n0, scrub_ts))
    t_last, p_last, n_last = frames[-1]
    output.append((t_last, p_last, n_last, t_last))
    return output


def draw_legend(fig, speed_unit: str) -> None:
    """Draw a fixed left-column legend of rounded speed-class pills, like a
    printed wind-rose reference sheet (heading, pill + label per row, caption)."""
    fig.text(0.045, 0.775, "Wind speed", fontsize=15, fontweight="bold", color=INK_PRIMARY)

    labels = speed_labels(speed_unit)
    row_top, row_step, pill_w, pill_h = 0.735, 0.052, 0.075, 0.024
    legend_ax = fig.add_axes((0.0, 0.0, 1.0, 1.0))
    legend_ax.set_axis_off()
    legend_ax.patch.set_alpha(0)

    for index, (color, label) in enumerate(zip(SPEED_COLORS, labels)):
        y = row_top - index * row_step
        pill = FancyBboxPatch(
            (0.045, y - pill_h / 2), pill_w, pill_h,
            boxstyle=f"round,pad=0,rounding_size={pill_h / 2}",
            linewidth=0, facecolor=color, zorder=2, transform=fig.transFigure,
        )
        legend_ax.add_patch(pill)
        legend_ax.text(0.045 + pill_w + 0.018, y, label, ha="left", va="center",
                        fontsize=11, color=INK_SECONDARY, transform=fig.transFigure)

    caption_y = row_top - len(labels) * row_step - 0.03
    fig.text(
        0.045, caption_y,
        "Petal length is the share of one-second readings that hour blowing\n"
        "from that direction, as a percent of all samples.",
        ha="left", va="top", fontsize=10, color=INK_MUTED, linespacing=1.6,
    )
    fig.text(
        0.045, caption_y - 0.075,
        f"Color is wind speed in {speed_unit}, stacked from calm (blue) to\n"
        "strong (red) within each petal.",
        ha="left", va="top", fontsize=10, color=INK_MUTED, linespacing=1.6,
    )


def make_animation(frames: list[AnimFrame], out_path: Path, fps: int, dpi: int, speed_unit: str) -> Path:
    if fps <= 0:
        raise ValueError("--fps must be greater than zero")
    if dpi <= 0:
        raise ValueError("--dpi must be greater than zero")

    out_path = out_path.with_suffix(".mp4")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    radial_max = max(float(percentages.sum(axis=1).max()) for _, percentages, _, _ in frames)
    radial_max = max(5.0, np.ceil(radial_max * 1.15 / 5.0) * 5.0)

    window_start = frames[0][0]
    window_end = frames[-1][0] + pd.Timedelta(hours=1)

    fig = plt.figure(figsize=(14.0, 10.6), dpi=dpi, facecolor=PAGE)
    ax = fig.add_axes((0.42, 0.08, 0.54, 0.72), projection="polar")
    timeline_ax = fig.add_axes((0.44, 0.03, 0.50, 0.045))

    fig.text(0.045, 0.93, "vaLo Node 01", fontsize=17, color=ACCENT, fontweight="bold")
    fig.suptitle(
        "Hourly Wind Rose",
        x=0.045,
        y=0.885,
        ha="left",
        fontsize=30,
        fontweight="bold",
        color=INK_PRIMARY,
    )
    fig.text(
        0.048, 0.835,
        "7-day window · 1-second WIMDA samples aggregated by hour",
        ha="left",
        fontsize=11,
        color=INK_MUTED,
    )

    draw_legend(fig, speed_unit)

    def update(index: int):
        label_ts, percentages, label_samples, scrub_ts = frames[index]
        draw_rose(ax, label_ts, percentages, label_samples, radial_max)
        draw_timeline(timeline_ax, scrub_ts, window_start, window_end)
        return ax.patches

    video = animation.FuncAnimation(fig, update, frames=len(frames), interval=1000 / fps, blit=False)
    writer = animation.FFMpegWriter(fps=fps, codec="libx264", extra_args=["-pix_fmt", "yuv420p"])
    video.save(out_path, writer=writer, dpi=dpi, savefig_kwargs={"facecolor": PAGE})
    plt.close(fig)
    return out_path


def main() -> None:
    args = parse_args()
    print(f"Loading {args.csv} ...")
    wind = load_wind_data(args.csv)
    hourly = limit_frames(hourly_counts(wind), args.max_frames)
    frames = interpolate_frames(hourly, args.interp_steps)
    out_path = make_animation(frames, args.out, args.fps, args.dpi, args.speed_unit)
    print(f"Saved {out_path}")
    print(
        f"Hours: {len(hourly)} | Rendered frames: {len(frames)} | "
        f"Samples: {len(wind):,} | Range: {wind.index.min()} to {wind.index.max()}"
    )


if __name__ == "__main__":
    main()
