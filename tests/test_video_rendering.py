import pickle
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from mintsXU4.mintsPmWindCombined import (
    encoder_extra_args,
    load_hourly_wind,
    resolve_encoder,
)
from scripts.render_all_full_history_particles import (
    RenderJob,
    RenderResult,
    build_command,
    failed_fields,
    worker_count,
)


def test_encoder_resolution_prefers_nvenc_and_preserves_fallback():
    assert resolve_encoder("auto", {"h264_nvenc", "libx264"}) == "h264_nvenc"
    assert resolve_encoder("auto", {"libx264"}) == "libx264"
    assert resolve_encoder("libx264", {"libx264"}) == "libx264"
    with pytest.raises(RuntimeError, match="h264_nvenc"):
        resolve_encoder("h264_nvenc", {"libx264"})


def test_nvenc_writer_arguments():
    args = encoder_extra_args("h264_nvenc")
    assert args == [
        "-preset", "p4",
        "-tune", "hq",
        "-rc", "vbr",
        "-cq", "23",
        "-b:v", "0",
        "-pix_fmt", "yuv420p",
    ]
    assert encoder_extra_args("libx264") == ["-pix_fmt", "yuv420p"]


@pytest.mark.parametrize(
    ("requested", "fields", "cpus", "expected"),
    [(4, 14, 24, 4), (8, 3, 24, 3), (8, 14, 2, 2), (24, 30, 32, 8)],
)
def test_worker_count_is_safely_bounded(requested, fields, cpus, expected):
    assert worker_count(requested, fields, cpus) == expected


def test_hourly_cache_takes_precedence_over_missing_csv(tmp_path):
    timestamp = pd.Timestamp("2026-01-01T00:00:00Z")
    expected = [(timestamp, np.zeros((16, 7)), 3600)]
    cache = tmp_path / "wind.pkl"
    with cache.open("wb") as handle:
        pickle.dump(expected, handle)

    loaded = load_hourly_wind(tmp_path / "missing.csv", cache, max_frames=None)

    assert len(loaded) == 1
    assert loaded[0][0] == timestamp
    np.testing.assert_array_equal(loaded[0][1], expected[0][1])


def test_batch_command_uses_cache_nvenc_and_full_history_label(tmp_path):
    job = RenderJob(
        field="pc0_1",
        wind_cache=tmp_path / "wind.pkl",
        data_dir=tmp_path / "data",
        out_dir=tmp_path / "out",
        log_dir=tmp_path / "logs",
        encoder="h264_nvenc",
        fps=60,
        dpi=67,
        max_frames=2,
        overwrite=True,
    )

    command = build_command(job)

    assert command[command.index("--encoder") + 1] == "h264_nvenc"
    assert command[command.index("--wind-hourly-cache") + 1] == str(job.wind_cache)
    assert command[command.index("--history-label") + 1] == "Full history"
    assert command[command.index("--max-frames") + 1] == "2"
    assert command[command.index("--dpi") + 1] == "67"


def test_failure_summary_catches_exit_and_empty_output(tmp_path):
    good = tmp_path / "good.mp4"
    good.write_bytes(b"video")
    empty = tmp_path / "empty.mp4"
    empty.touch()
    log = Path(tmp_path / "render.log")
    results = [
        RenderResult("pm1_0", 0, good, log),
        RenderResult("pc0_1", 1, good, log),
        RenderResult("pc0_3", 0, empty, log),
    ]

    assert failed_fields(results) == ["pc0_1", "pc0_3"]
