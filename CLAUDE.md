# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SAFE (Sensor Analysis & Failure Evaluation) — real-time drift detection and failure analysis for low-cost air quality sensors in the [MINTS](http://utdmints.info/) (Multi-scale Integrated Sensing and Simulation) network at UT Dallas.

## Running the Code

Run real-time-style drift analysis on the bundled test data:
```
python mintsXU4/mintsDriftAnalysis.py
```

Run period-over-period drift analysis (day/week/month/year comparisons + plots) — writes CSVs and PNGs under `mintsXU4/output/`:
```
python mintsXU4/mintsPeriodAnalysis.py
```

Debug system configuration (port discovery, MAC address, MQTT settings):
```
python mintsXU4/mintsDefinitions.py
```

## Dependencies

Install required packages:
```
pip install -r requirements.txt
```
(`requirements.txt` lists numpy, scipy, pandas, matplotlib, openpyxl, pyserial, paho-mqtt, getmac, pynmea2, netifaces, pyyaml — names only, no pins.)

## Architecture

### Core module: `mintsXU4/mintsDriftAnalysis.py`

Contains the `SensorDrift` class and the `parse_and_process_valo_data()` ingestion pipeline. Also exposes shared building blocks reused by the period module:
- `load_pivoted_dataframe(file_path)` — reads the CSV export and returns a timestamp-indexed pivoted DataFrame plus the list of metric columns.
- `sample_comparison(old, new, p_alpha)` — the Welch/Levene comparison of two samples, returning descriptive stats + drift-test results.
- `HARD_BOUNDS` — the per-metric physical limits dictionary.

**Detection pipeline (per metric, per reading):**
1. **Hard bounds** — rejects physically impossible values (e.g. humidity > 100%). Value is not added to history.
2. **Z-score** (requires ≥30 buffer values) — flags values more than `z_threshold` (default 3.5) standard deviations from the buffer mean. Uses a noise floor of `1e-3` on std to handle flat-line baselines.
3. **Step-change detection** — if 10 consecutive Z-score outliers accumulate, the buffer is flushed and re-seeded with those outlier values so the new regime is tracked immediately.
4. **Drift evaluation** — runs every `window_size` (default 200) accepted samples. Splits the buffer in half and runs:
   - **Welch's T-test** — detects a shift in the mean
   - **Levene's F-test** — detects variance inflation
   - Special handling for flat-line halves (variance < `1e-12`) to avoid SciPy division errors.
5. **Alert cooldown** — 1800-second per-key `(sensor_name, metric, alert_type)` cooldown prevents alert spam.

Evaluation windows overlap by half (`window_size // 2`) to avoid blind spots between adjacent windows.

### `mintsXU4/mintsPeriodAnalysis.py`

Period-over-period drift analysis built on the core module. Resamples the loaded DataFrame into calendar buckets (day/week/month/year) and compares each non-empty bucket to the previous one (plus a first-month-vs-last-month comparison) using `sample_comparison()`. Writes one `period_*.csv` per granularity into `output/`, then auto-invokes the plotter. Imports the core module with a script/package-aware fallback (`from mintsDriftAnalysis ...` else `from mintsXU4.mintsDriftAnalysis ...`).

### `mintsXU4/mintsPeriodPlotter.py`

Reads the `period_*.csv` files and renders, per granularity, figures over time (means with min–max band, standard deviations, z-scores, and Welch/Levene significance). Uses the headless matplotlib `Agg` backend and writes PNGs under `output/plots/`. Dense categories are drawn as time series, sparse ones (year-to-year, first-vs-last-month) as bar charts.

### `mintsXU4/mintsDefinitions.py`

System configuration loaded at import time: serial port discovery (IPS7100, RG15, GPS), MAC address detection, data folder paths, and MQTT connection parameters. YAML configs are loaded via a `_loadYaml()` helper that warns and returns `None` if a file is missing, so importing the module does **not** crash when the untracked credentials are absent. Reads from:
- `mintsXU4/credentials/portIDs.yml`
- `mintsXU4/credentials/mintsDefinitions.yaml`
- `mintsXU4/credentials.yml` (MQTT username/password, loaded by `mintsLatest.py`)

### `mintsXU4/mintsLatest.py`

MQTT and JSON I/O utilities. Publishes sensor dictionaries to `mqtt.circ.utdallas.edu:8883` (TLS) under topic `{macAddress}/{sensorName}`, and writes/reads JSON "latest" files to local data folders. Write helpers create the parent directory and return a success/failure bool. Currently not wired into `mintsDriftAnalysis.py` (import is commented out).

### `mintsXU4/mintsSensorReader.py`

Serial data parser for the full MINTS sensor suite (BME280/680, IPS7100, OPCN2/3, GPS, INA219, etc.). Each `*Write()` function parses a raw delimited string into an `OrderedDict` and calls `sensorFinisher()` which writes to timestamped CSV files under `dataFolder/{macAddress}/YYYY/MM/DD/`.

## Test Data Format

`mintsXU4/data/valo_node_01_full_year.csv` is an InfluxDB export (moved from `.xlsm` to CSV to avoid spreadsheet row limits). `load_pivoted_dataframe()` reads it with `comment='#'` to skip InfluxDB annotation/metadata lines and requires the columns `_time`, `_value`, `_field`, `_measurement`, `device_id`. It coerces `_value` to numeric, drops duplicate `(_time, _measurement, device_id, _field)` rows, then pivots on `(_time, _measurement, device_id)` so each `_field` becomes a column. Timestamps are converted vectorized and normalized to tz-naive UTC; the result is a `DatetimeIndex`-indexed DataFrame so the period module can resample. `parse_and_process_valo_data()` then feeds the rows to `SensorDrift.data_processing()`.