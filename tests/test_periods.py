import numpy as np
import pandas as pd
import pytest

from safe.periods import CSV_COLUMNS, apply_hard_bounds, bucketize, run_period_analysis
from tests.test_loader import write_influx_csv


class TestBucketize:
    def test_daily_buckets(self):
        idx = pd.date_range("2026-01-01", periods=48, freq="h")
        s = pd.Series(np.arange(48.0), index=idx)
        buckets = bucketize(s, "D", min_samples=2)
        assert len(buckets) == 2
        assert buckets[0][0] == pd.Timestamp("2026-01-01")
        assert len(buckets[0][1]) == 24

    def test_min_samples_filters_sparse_buckets(self):
        idx = pd.DatetimeIndex(["2026-01-01 00:00", "2026-01-02 00:00", "2026-01-02 01:00"])
        s = pd.Series([1.0, 2.0, 3.0], index=idx)
        buckets = bucketize(s, "D", min_samples=2)
        assert len(buckets) == 1  # Jan 1 has only one reading

    def test_nans_dropped(self):
        idx = pd.date_range("2026-01-01", periods=10, freq="h")
        s = pd.Series([np.nan] * 5 + [1.0] * 5, index=idx)
        buckets = bucketize(s, "D", min_samples=2)
        assert len(buckets[0][1]) == 5


class TestHardBounds:
    def test_out_of_bounds_become_nan(self):
        df = pd.DataFrame({"pm1_0": [5.0, -1.0, 20000.0], "unknown": [1.0, 2.0, 3.0]})
        out = apply_hard_bounds(df, ["pm1_0", "unknown"])
        assert out["pm1_0"].isna().sum() == 2
        assert out["unknown"].isna().sum() == 0  # no bounds defined


class TestRunPeriodAnalysis:
    def test_end_to_end_csv_outputs(self, tmp_path):
        rng = np.random.default_rng(1)
        rows = []
        # 3 days of hourly readings, level shifts on day 3
        for day in range(1, 4):
            level = 10.0 if day < 3 else 30.0
            for hour in range(24):
                t = f"2026-01-{day:02d}T{hour:02d}:00:00Z"
                rows.append((t, round(float(rng.normal(level, 1.0)), 3),
                             "pm1_0", "IPS7100MHC001", "dev1"))
        csv = write_influx_csv(tmp_path / "p.csv", rows)
        out_dir = tmp_path / "out"

        run_period_analysis(csv, str(out_dir), make_plots=False)

        day_csv = out_dir / "period_day_to_day.csv"
        assert day_csv.exists()
        df = pd.read_csv(day_csv)
        assert list(df.columns) == CSV_COLUMNS
        assert len(df) == 2  # day1->day2, day2->day3

        # The day2 -> day3 comparison must flag the 20-sigma level shift
        shift_row = df.iloc[-1]
        assert shift_row["new_period"] == "2026-01-03"
        assert bool(shift_row["mean_shift"]) is True
        # n_eff columns present and sane
        assert 0 < shift_row["new_n_eff"] <= shift_row["new_n"]

    def test_all_granularity_files_written(self, tmp_path):
        rows = [(f"2026-01-01T{h:02d}:00:00Z", 10.0 + h * 0.01, "pm1_0", "M", "d")
                for h in range(24)]
        csv = write_influx_csv(tmp_path / "g.csv", rows)
        out_dir = tmp_path / "out"
        run_period_analysis(csv, str(out_dir), make_plots=False)
        for stem in ["day_to_day", "week_to_week", "month_to_month",
                     "year_to_year", "first_vs_last_month"]:
            assert (out_dir / f"period_{stem}.csv").exists()
