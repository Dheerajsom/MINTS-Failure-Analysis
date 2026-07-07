# SAFE — Sensor Analysis & Failure Evaluation

Drift detection and failure analysis for the MINTS-AI lab's low-cost air-quality
sensor nodes deployed around the Dallas area.

The core engine lives in the `safe` package and layers five detectors, fastest
to slowest:

| Layer | Detector | Catches | Latency |
|---|---|---|---|
| 1 | Hard physical bounds | impossible values (e.g. negative PM) | instant |
| 2 | Robust modified z-score (median / MAD+IQR) | single-reading outliers | instant |
| 3 | Consecutive-outlier step-change | abrupt regime shifts | ~10 readings |
| 4 | Page-Hinkley sequential test (opt-in) | sustained small mean shifts | tens of readings |
| 5 | Welch + Levene windowed tests | slow mean / variance drift | one window |

Two safeguards keep the windowed tests honest on real sensor data:

- **Effect-size gates** — with large samples, p-values collapse toward zero for
  meaningless shifts, so a drift flag additionally requires Cohen's *d* ≥ 0.2
  (mean) or a ≥ 50% spread change (variance).
- **Autocorrelation correction** — sensor readings are serially correlated, so
  the tests use the AR(1) effective sample size
  `n_eff = n·(1−ρ)/(1+ρ)` instead of the nominal *n*. At 5-minute resolution a
  288-reading day typically carries only ~10–60 independent observations, and
  the reported p-values reflect that.

## Install

```bash
pip install -e .            # core package + `safe` CLI
pip install -e ".[dev]"     # + pytest
pip install -e ".[sensor]"  # + legacy live-node extras (serial/MQTT)
```

## Usage

```bash
# Replay an InfluxDB-export CSV through the streaming engine
safe stream mintsXU4/data/valo_node_01_full_year.csv

# Period-over-period analysis (day/week/month/year CSVs + plots)
safe periods mintsXU4/data/valo_node_01_full_year.csv -o mintsXU4/output
```

Or from Python:

```python
from safe import SensorDrift, replay_csv, sample_comparison

engine = SensorDrift(on_alert=my_mqtt_publisher)
replay_csv("data.csv", engine=engine)
print(engine.alerts)
```

## Tests

```bash
python -m pytest tests/
```

## Repository layout

- `safe/` — the SAFE package: `engine` (streaming detectors), `stats`
  (drift-test math), `loader` (InfluxDB CSV handling), `periods` +
  `plotting` (period-over-period analysis), `cli`.
- `tests/` — pytest suite for the engine, stats, loader, and period analysis.
- `mintsXU4/` — MINTS node scripts. `mintsDriftAnalysis.py`,
  `mintsPeriodAnalysis.py`, and `mintsPeriodPlotter.py` are thin
  compatibility shims over `safe`; the rest are live-sensor utilities and
  1-second-data visualization tools.
- `mintsXU4/data/valo_node_01_full_year.csv` — bundled two-year, 5-minute
  export from vaLo Node 01 used by the examples above.
- `mintsXU4/output/` — generated CSVs and plots.
