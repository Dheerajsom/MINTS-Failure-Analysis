# AGENTS.md

Guidance for coding agents working in this repository.

## Project

SAFE, Sensor Analysis and Failure Evaluation, is a Python analysis project for
MINTS low-cost air-quality sensor data. It detects outliers, drift,
distribution changes, and period-over-period changes.

The core lives in the `safe` package at the repo root; `mintsXU4/` holds thin
compatibility shims plus older live-sensor utilities. Bundled source data
lives at `mintsXU4/data/valo_node_01_full_year.csv`. Generated outputs live
under `mintsXU4/output/`.

## Setup

```bash
pip install -e ".[dev]"
```

Core dependencies: `numpy`, `scipy`, `pandas`, `matplotlib`. The `sensor`
extra adds the legacy live-node packages (`pyserial`, `paho-mqtt`, `pyyaml`, ...).

## Common Commands

```bash
python -m pytest tests/                 # test suite — run this first
safe stream mintsXU4/data/valo_node_01_full_year.csv
safe periods mintsXU4/data/valo_node_01_full_year.csv -o mintsXU4/output
python mintsXU4/mintsDriftAnalysis.py   # legacy entry points still work
python mintsXU4/mintsPeriodAnalysis.py
python dataVisualizer.py
```

## Key Files

- `safe/stats.py`: Source of truth for `sample_comparison()` — Welch/Levene
  drift tests with effect-size gates and AR(1) effective-sample-size (n_eff)
  correction.
- `safe/engine.py`: `SensorDrift` streaming engine — hard bounds, robust
  (median/MAD+IQR) modified z-score, step-change detection, optional
  Page-Hinkley layer, windowed drift evaluation. `PageHinkley` is off by
  default because ambient diurnal cycles trigger it daily.
- `safe/config.py`: `HARD_BOUNDS`, effect-size gates, flat-step thresholds.
- `safe/loader.py`: InfluxDB-export CSV loading (`load_pivoted_dataframe`) and
  streaming replay (`replay_csv`).
- `safe/periods.py` + `safe/plotting.py`: period-over-period comparisons and
  their plots.
- `mintsXU4/mintsDriftAnalysis.py`, `mintsXU4/mintsPeriodAnalysis.py`,
  `mintsXU4/mintsPeriodPlotter.py`: compatibility shims re-exporting from
  `safe`; keep them in sync when renaming public symbols.
- `mintsXU4/mints1sLoader.py`, `mintsXU4/mintsPmRegen.py`: 1-second PM data
  pipeline (data is git-ignored, ~5 GB local).
- `mintsXU4/mintsDefinitions.py`, `mintsXU4/mintsLatest.py`,
  `mintsXU4/mintsSensorReader.py`: older/live sensor utilities. Avoid changing
  these unless the task is about live sensor behavior.

## Data Notes

The bundled CSV is an InfluxDB-style export. The loader expects `_time`,
`_value`, `_field`, `_measurement`, `device_id`. `load_pivoted_dataframe()`
handles numeric coercion, duplicate removal, pivoting fields into metric
columns, and timestamp normalization.

## Output Notes

Treat files under `mintsXU4/output/` as generated. Prefer regenerating outputs
from source scripts instead of manually editing generated CSV/PNG/Markdown
files. Important areas:

- Period CSVs/plots: `mintsXU4/output/period_*.csv`, `mintsXU4/output/plots/`
- Distribution visualizer outputs: `mintsXU4/output/<field>/`

## Coding Guidelines

- Put shared drift math in `safe/` — never duplicate it in scripts.
- Use pandas/numpy vectorized operations for CSV processing.
- Keep plotting headless with the existing matplotlib `Agg` pattern.
- Do not add live MQTT, serial, or credential side effects to offline analysis
  paths.
- Missing local credential YAML files should not crash imports.
- Do not commit `__pycache__`, `.DS_Store`, credentials, or local device data.

## Verification

Before handing off substantial changes:

1. Run `python -m pytest tests/`.
2. Run the relevant analysis or plotting script on the bundled CSV.
3. Confirm outputs land in the expected `mintsXU4/output/` location.
4. Check `git status --short` and call out generated files that changed.
