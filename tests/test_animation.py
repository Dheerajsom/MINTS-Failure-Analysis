import pandas as pd
import pytest

from safe.animation import field_metadata, pdf_axis_upper_limit
from safe.config import HARD_BOUNDS, PC_BOUNDS
from mintsXU4.mintsWindowPdfAnimation import load_field_series, load_field_series_from_dir


def write_influx_csv(path, rows):
    lines = [
        "# Influx export fixture",
        ",result,table,_time,_value,_field,_measurement,device_id",
    ]
    for timestamp, value, field in rows:
        lines.append(f",_result,0,{timestamp},{value},{field},IPS7100MHC001,dev1")
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


@pytest.mark.parametrize(
    ("field", "label", "unit", "bounds"),
    [
        ("pm1_0", "PM1.0", "µg/m³", HARD_BOUNDS["pm1_0"]),
        ("pc0_1", "PC0.1", "particles/L", HARD_BOUNDS["pc0_1"]),
        ("pc42_0", "PC42.0", "particles/L", PC_BOUNDS),
    ],
)
def test_field_metadata(field, label, unit, bounds):
    metadata = field_metadata(field)
    assert metadata.label == label
    assert metadata.unit == unit
    assert metadata.bounds == bounds


@pytest.mark.parametrize("values", [[0.2, 0.9], [1.0, 2.0], [2.01, 2.2]])
def test_pdf_axis_upper_limit_preserves_pdf_magnitude(values):
    limit = pdf_axis_upper_limit(values)
    assert limit > max(values)
    assert limit == pytest.approx(max(values) * 1.1)


def test_pdf_axis_upper_limit_keeps_tiny_pc_density_visible():
    limit = pdf_axis_upper_limit([0.000002])

    assert limit == pytest.approx(0.0000022)
    assert limit < 0.00001


def test_pc_field_loads_from_influx_csv_without_pm_clipping(tmp_path):
    csv_path = write_influx_csv(
        tmp_path / "pc.csv",
        [
            ("2026-01-01T00:00:00Z", 250_000, "pc0_1"),
            ("2026-01-01T00:01:00Z", 260_000, "pc0_1"),
            ("2026-01-01T00:00:00Z", 12.5, "pm1_0"),
        ],
    )

    series = load_field_series(csv_path, "pc0_1")

    assert series.name == "pc0_1"
    assert series.tolist() == [250_000, 260_000]


def test_pc_field_loads_from_daily_gzip(tmp_path):
    data_dir = tmp_path / "daily"
    data_dir.mkdir()
    daily = pd.DataFrame(
        {
            "_time": ["2026-01-01T00:00:00Z", "2026-01-01T00:01:00Z"],
            "_value": [250_000, 260_000],
            "_field": ["pc0_1", "pc0_1"],
        }
    )
    daily.to_csv(data_dir / "valo_node_01_20260101_20260102.csv.gz", index=False, compression="gzip")

    series = load_field_series_from_dir(data_dir, "pc0_1")

    assert series.tolist() == [250_000, 260_000]
