# AGENTS.md

Guidance for coding agents working in this repository.

## Project

SAFE, Sensor Analysis and Failure Evaluation, is a Python analysis project for MINTS low-cost air-quality sensor data. It detects outliers, drift, distribution changes, and period-over-period changes.

Bundled source data lives at `mintsXU4/data/valo_node_01_full_year.csv`. Generated outputs live under `mintsXU4/output/`.

## Setup

Install dependencies from the repository root:

```bash
pip install -r requirements.txt
```

Main dependencies include `numpy`, `scipy`, `pandas`, `matplotlib`, `openpyxl`, and older sensor/network packages such as `pyserial`, `paho-mqtt`, and `pyyaml`.

## Common Commands

```bash
python mintsXU4/mintsDriftAnalysis.py
python mintsXU4/mintsPeriodAnalysis.py
python mintsXU4/mintsPeriodPlotter.py
python dataVisualizer.py
```

Syntax check:

```bash
python -m py_compile dataVisualizer.py mintsXU4/mintsDriftAnalysis.py mintsXU4/mintsPeriodAnalysis.py mintsXU4/mintsPeriodPlotter.py mintsXU4/mintsDefinitions.py mintsXU4/mintsLatest.py mintsXU4/mintsSensorReader.py
```

There is no automated test suite. For behavior changes, run the most relevant script against the bundled CSV and inspect the generated CSV/PNG outputs.

## Key Files

- `mintsXU4/mintsDriftAnalysis.py`: Core SAFE drift engine. Source of truth for `HARD_BOUNDS`, `sample_comparison()`, z-score outliers, step-change detection, Welch mean-shift tests, and Levene variance-shift tests.
- `mintsXU4/mintsPeriodAnalysis.py`: Builds day/week/month/year and first-vs-last-month comparisons using the shared drift math.
- `mintsXU4/mintsPeriodPlotter.py`: Generates period plots from `period_*.csv` files using headless matplotlib.
- `dataVisualizer.py`: Compares `pm1_0` and `temperature` distributions, fits SciPy distributions, and writes histogram/stat summaries.
- `mintsXU4/mintsDefinitions.py`, `mintsXU4/mintsLatest.py`, `mintsXU4/mintsSensorReader.py`: Older/live sensor utilities. Avoid changing these unless the task is about live sensor behavior.

## Data Notes

The bundled CSV is an InfluxDB-style export. The main loader expects:

- `_time`
- `_value`
- `_field`
- `_measurement`
- `device_id`

`load_pivoted_dataframe()` handles numeric coercion, duplicate removal, pivoting fields into metric columns, and timestamp normalization.

## Output Notes

Treat files under `mintsXU4/output/` as generated unless the user specifically asks to edit outputs.

Important output areas:

- Period CSVs/plots: `mintsXU4/output/period_*.csv`, `mintsXU4/output/plots/`
- Older checked-in period snapshot: `mintsXU4/output/period_analysis/`
- Distribution visualizer outputs: `mintsXU4/output/pm1_0/` and `mintsXU4/output/temperature/`

Prefer regenerating outputs from source scripts instead of manually editing generated CSV/PNG/Markdown files.

## Coding Guidelines

- Keep changes small and compatible with direct script execution from the repo root.
- Reuse `mintsDriftAnalysis.py` for shared drift math instead of duplicating logic.
- Use pandas/numpy vectorized operations for CSV processing.
- Keep plotting headless with the existing matplotlib `Agg` pattern.
- Do not add live MQTT, serial, or credential side effects to offline analysis paths.
- Missing local credential YAML files should not crash imports.
- Do not commit `__pycache__`, `.DS_Store`, credentials, or local device data.

## Verification

Before handing off substantial changes:

1. Run `py_compile` on touched Python files.
2. Run the relevant analysis or plotting script on the bundled CSV.
3. Confirm outputs land in the expected `mintsXU4/output/` location.
4. Check `git status --short` and call out generated files that changed.
