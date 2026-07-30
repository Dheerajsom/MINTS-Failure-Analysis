#!/usr/bin/env python3
"""Render all full-history PM/PC wind-rose videos in parallel."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import imageio_ffmpeg


REPO = Path(__file__).resolve().parents[1]
COMBINED_SCRIPT = REPO / "mintsXU4" / "mintsPmWindCombined.py"
DEFAULT_DATA_DIR = REPO / "mintsXU4" / "data" / "valo_node_01_1s"
DEFAULT_OUT_DIR = REPO / "mintsXU4" / "output" / "windrose_videos"
DEFAULT_WIND_CACHE = Path(tempfile.gettempdir()) / "valo_node_01_wimda_full_history_hourly.pkl"
DEFAULT_LOG_DIR = Path(tempfile.gettempdir()) / "full_history_particle_video_logs"
FIELDS = (
    "pm0_1", "pm0_3", "pm0_5", "pm1_0", "pm2_5", "pm5_0", "pm10_0",
    "pc0_1", "pc0_3", "pc0_5", "pc1_0", "pc2_5", "pc5_0", "pc10_0",
)
NVENC_ARGS = (
    "-preset", "p4",
    "-tune", "hq",
    "-rc", "vbr",
    "-cq", "23",
    "-b:v", "0",
    "-pix_fmt", "yuv420p",
)


@dataclass(frozen=True)
class RenderJob:
    field: str
    wind_cache: Path
    data_dir: Path
    out_dir: Path
    log_dir: Path
    encoder: str
    fps: int
    dpi: int
    max_frames: int | None
    overwrite: bool


@dataclass(frozen=True)
class RenderResult:
    field: str
    returncode: int
    output: Path
    log: Path
    skipped: bool = False


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def worker_count(requested: int, field_count: int, cpu_count: int | None = None) -> int:
    """Bound parallelism by fields, CPUs, and the explicit safety cap of eight."""
    available_cpus = cpu_count if cpu_count is not None else (os.cpu_count() or 1)
    return max(1, min(requested, 8, available_cpus, field_count))


def build_command(job: RenderJob) -> list[str]:
    command = [
        sys.executable,
        "-u",
        str(COMBINED_SCRIPT),
        "--wind-hourly-cache", str(job.wind_cache),
        "--data-dir", str(job.data_dir),
        "--field", job.field,
        "--out", str(job.out_dir / f"{job.field}_wind_rose_sidebyside_full_history_light.mp4"),
        "--fps", str(job.fps),
        "--dpi", str(job.dpi),
        "--interp-steps", "1",
        "--theme", "light",
        "--encoder", job.encoder,
        "--history-label", "Full history",
    ]
    if job.max_frames is not None:
        command.extend(["--max-frames", str(job.max_frames)])
    return command


def preflight_encoder(encoder: str, ffmpeg_path: str | None = None) -> None:
    """Initialize the requested encoder with a real one-frame encode."""
    ffmpeg = ffmpeg_path or imageio_ffmpeg.get_ffmpeg_exe()
    extra_args = list(NVENC_ARGS) if encoder == "h264_nvenc" else ["-pix_fmt", "yuv420p"]
    null_output = "NUL" if os.name == "nt" else "/dev/null"
    command = [
        ffmpeg,
        "-hide_banner",
        "-loglevel", "error",
        "-f", "lavfi",
        "-i", "color=c=black:s=256x256:r=1:d=1",
        "-frames:v", "1",
        "-c:v", encoder,
        *extra_args,
        "-f", "null",
        null_output,
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        details = (result.stderr or result.stdout).strip()
        raise RuntimeError(f"FFmpeg encoder preflight failed for {encoder}: {details}")


def render(job: RenderJob) -> RenderResult:
    output = job.out_dir / f"{job.field}_wind_rose_sidebyside_full_history_light.mp4"
    log_path = job.log_dir / f"{job.field}.log"
    if output.is_file() and output.stat().st_size > 0 and not job.overwrite:
        with log_path.open("a", encoding="utf-8") as log:
            log.write(f"[{stamp()}] SKIP existing {output}\n")
        return RenderResult(job.field, 0, output, log_path, skipped=True)

    if job.overwrite and output.exists():
        output.unlink()

    command = build_command(job)
    with log_path.open("w", encoding="utf-8") as log:
        log.write(f"[{stamp()}] START {' '.join(command)}\n")
        log.flush()
        completed = subprocess.run(
            command,
            cwd=REPO,
            stdout=log,
            stderr=subprocess.STDOUT,
            check=False,
        )
        log.write(f"[{stamp()}] EXIT {completed.returncode}\n")
    return RenderResult(job.field, completed.returncode, output, log_path)


def failed_fields(results: list[RenderResult]) -> list[str]:
    return [
        result.field
        for result in results
        if result.returncode != 0
        or not result.output.is_file()
        or result.output.stat().st_size == 0
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wind-cache", type=Path, default=DEFAULT_WIND_CACHE)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--log-dir", type=Path, default=DEFAULT_LOG_DIR)
    parser.add_argument("--fields", nargs="+", default=list(FIELDS))
    parser.add_argument("--workers", type=int, default=4, help="Parallel renders (1-8; additionally CPU/field bounded).")
    parser.add_argument("--encoder", choices=("h264_nvenc", "libx264"), default="h264_nvenc")
    parser.add_argument("--fps", type=int, default=60)
    parser.add_argument("--dpi", type=int, default=67, help="67 DPI at 19.2x10.8 inches is approximately 1286x724.")
    parser.add_argument("--max-frames", type=int, default=None, help="Optional real-hour cap for previews.")
    parser.add_argument("--overwrite", action="store_true", help="Delete and regenerate matching outputs.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not 1 <= args.workers <= 8:
        raise ValueError("--workers must be between 1 and 8")
    if not args.wind_cache.is_file():
        raise FileNotFoundError(f"Hourly wind cache was not found: {args.wind_cache}")
    if not args.data_dir.is_dir():
        raise FileNotFoundError(f"Particle data directory was not found: {args.data_dir}")

    args.out_dir.mkdir(parents=True, exist_ok=True)
    args.log_dir.mkdir(parents=True, exist_ok=True)
    preflight_encoder(args.encoder)

    workers = worker_count(args.workers, len(args.fields))
    jobs = [
        RenderJob(
            field=field,
            wind_cache=args.wind_cache,
            data_dir=args.data_dir,
            out_dir=args.out_dir,
            log_dir=args.log_dir,
            encoder=args.encoder,
            fps=args.fps,
            dpi=args.dpi,
            max_frames=args.max_frames,
            overwrite=args.overwrite,
        )
        for field in args.fields
    ]

    print(
        f"[{stamp()}] Starting {len(jobs)} renders with workers={workers}, "
        f"encoder={args.encoder}, dpi={args.dpi}",
        flush=True,
    )
    results: list[RenderResult] = []
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(render, job): job.field for job in jobs}
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            size = result.output.stat().st_size if result.output.exists() else 0
            state = "skipped" if result.skipped else f"exit={result.returncode}"
            print(f"[{stamp()}] {result.field}: {state}, bytes={size}", flush=True)

    failures = failed_fields(results)
    if failures:
        print(f"[{stamp()}] FAILED: {', '.join(failures)}", flush=True)
        raise SystemExit(1)
    print(f"[{stamp()}] All {len(results)} renders completed successfully", flush=True)


if __name__ == "__main__":
    main()
