# SAFE — Sensor Analysis & Failure Evaluation

SAFE analyzes MINTS low-cost air-quality sensor data for impossible readings,
outliers, abrupt changes, and slower statistical drift. It is designed for two
complementary workflows:

- **Streaming replay:** process each reading as it arrives and emit alerts.
- **Period analysis:** compare calendar periods to produce CSV summaries and
  static plots for investigation and reporting.

The maintained implementation is the [`safe/`](safe/) package. The
[`mintsXU4/`](mintsXU4/) directory retains compatibility entry points and older
live-node utilities.

## Quick start

```bash
pip install -e ".[dev]"

# Replay the bundled InfluxDB export through the streaming engine
safe stream mintsXU4/data/valo_node_01_full_year.csv

# Compare adjacent days, weeks, months, and years; write CSVs and plots
safe periods mintsXU4/data/valo_node_01_full_year.csv -o mintsXU4/output

# Run the test suite
python -m pytest tests/
```

The bundled data is a two-year, five-minute export for Influx measurement
`IPS7100MHC001` (device ID `001e064a1520`). SAFE displays it as
`IPS7100_MHC_001` in output and alerts.

## How SAFE processes data

InfluxDB exports are stored in a *long* format: each row contains one field
and value. `safe.loader` validates the expected columns, coerces numeric
values, removes duplicate records, normalizes timestamps to UTC, and pivots
the rows into one timestamped record containing metric columns such as
`pm1_0`, `temperature`, and `pressure`.

```text
InfluxDB-style CSV (long rows)
        │
        ▼
safe.loader: validate, clean, pivot, normalize timestamps
        │
        ├──► safe.engine: per-reading alerts and streaming state
        │
        └──► safe.periods: calendar comparisons, CSVs, and plots
```

Each sensor and metric gets its own streaming state: a sliding history buffer,
a robust baseline, an outlier streak, an optional Page-Hinkley detector, and
per-alert cooldowns. A bad PM reading therefore cannot alter the baseline for
temperature, nor can an alert from one sensor suppress another sensor's alert.

## Streaming SAFE engine

`SensorDrift` applies the following layers from fastest to slowest. A reading
that fails a hard-bound or outlier check is kept out of the normal history, so
obvious failures do not contaminate future comparisons.

| Layer | Detector | What it catches | Typical latency |
|---:|---|---|---|
| 1 | Hard physical bounds | Impossible values, such as negative PM or humidity over 100% | One reading |
| 2 | Robust modified z-score | Isolated readings far from the median/MAD+IQR baseline | One reading |
| 3 | Consecutive-outlier rule | Abrupt level changes that persist for 10 readings | About 10 readings |
| 4 | Page-Hinkley (optional) | Sustained smaller mean shifts in a stable stream | Tens of readings |
| 5 | Windowed Welch + Levene tests | Practical changes in mean or variance across a full window | One evaluation window |

### What happens after a potential failure

- **Hard-bound violation:** alerts immediately and is never stored in the
  metric history.
- **Single robust outlier:** alerts (subject to cooldown) and is not stored.
- **Ten consecutive outliers:** alerts as a step change, then reseeds the
  history at the new level so monitoring can recover instead of rejecting all
  subsequent readings.
- **Windowed drift:** compares the older and newer halves of the sliding
  buffer. Evaluations overlap by half a window to avoid gaps between checks.
- **Cooldowns:** alerts of the same sensor, metric, and type are spaced by 30
  minutes by default; all alerts remain available in `engine.alerts`.

Hard bounds are defined for the common MINTS metrics in
[`safe/config.py`](safe/config.py). Unknown numeric metrics can still receive
outlier and drift analysis, but do not have a metric-specific physical bound
unless one is added there.

### Why a small p-value is not enough

The shared `sample_comparison()` function in [`safe/stats.py`](safe/stats.py)
is used by both the streaming and period-analysis paths. It prevents two common
false-alarm patterns in environmental sensor data:

- **Practical-effect gate:** a mean shift must be statistically significant
  *and* have `|Cohen's d| >= 0.2`. A variance shift must be significant and
  represent at least a 1.5x spread increase (or the corresponding decrease).
- **Autocorrelation correction:** observations seconds or minutes apart are
  related, so the engine estimates lag-1 AR(1) autocorrelation and uses an
  effective sample size rather than treating every reading as independent.
- **Flat-signal handling:** two constant windows are not passed through
  meaningless variance tests; SAFE checks whether their levels moved by a
  metric-specific amount instead.

The Page-Hinkley layer is off by default. Outdoor sensor streams often contain
real daily environmental cycles, and enabling it for ambient data can produce
alerts for weather rather than device faults. It is more useful for stable,
high-rate signals such as `shuntVoltage`.

## Command-line usage

```bash
# Process one or more CSV/CSV.GZ exports, or a directory of exports.
# One engine is kept alive across directory files, preserving history.
safe stream mintsXU4/data/valo_node_01_full_year.csv

# Restrict to one metric and choose a larger drift window.
safe stream mintsXU4/data/valo_node_01_full_year.csv --metric pm1_0 --window 7200

# Enable the sequential detector only for an appropriately stable signal.
safe stream data.csv --metric shuntVoltage --page-hinkley

# Create period_*.csv reports and PNG plots.
safe periods mintsXU4/data/valo_node_01_full_year.csv -o mintsXU4/output

# Generate reports without plots.
safe periods data.csv -o mintsXU4/output --no-plots
```

For one-second PM data, use a substantially larger `--window` (the project
examples use `7200`). At that sampling rate, a short 200-reading window often
has fewer than three effective independent observations after autocorrelation
correction, so the statistical window test intentionally remains conservative.

### Python API

```python
from safe import SensorDrift, replay_csv

def publish_alert(sensor_name, alert, timestamp):
    print(sensor_name, timestamp, alert)

engine = SensorDrift(on_alert=publish_alert)
replay_csv("data.csv", engine=engine)

for sensor_name, alert, timestamp in engine.alerts:
    print(sensor_name, timestamp, alert["alert"])
```

`on_alert` is a callback, so applications may replace the example function
with their own logger, database writer, or MQTT publisher without changing
SAFE's analysis code.

Inputs use ISO timestamps; naive timestamps are interpreted as UTC. Invalid
timestamps, missing identifiers and nonfinite readings are discarded with a
warning. Duplicate readings at the same UTC instant keep the first valid value.
Other devices sharing the bundled measurement receive a device-ID suffix so
their baselines remain separate. Replay files must be chronological and
non-overlapping; out-of-order readings fail the replay. `replay_csv` returns
`None` on load or processing failure, with partial state retained in a supplied
engine. The CLI returns a nonzero exit code for these failures.

`SensorDrift` requires an integer window of at least 30 readings, positive finite
z threshold, significance level strictly between 0 and 1, and nonnegative finite
cooldown. `sample_comparison` requires finite one-dimensional samples of at least
two readings each. AR(1) correction and thinning are approximations; they do not
remove seasonal confounding or establish that a detected change is a sensor fault.

## Period analysis and outputs

`safe periods` filters physically impossible values, then compares consecutive
non-empty day, week, month, and year buckets for every sensor and metric. It
also compares the first available month with the last available month. Output
CSVs include sample counts, effective sample counts, mean and spread changes,
Cohen's *d*, p-values, and boolean mean/variance drift flags.

Plots are written below `mintsXU4/output/plots/` by default. The standard
multi-panel report focuses on `pm1_0`, `temperature`, and `pressure`; all
available metrics remain present in the generated CSV reports.

Plots honor `--alpha`, isolate different sensors in numbered subdirectories when
necessary, and propagate rendering failures to the command's exit status.
The separate distribution visualizers are exploratory tools for a single sensor:
their nominal two-sample p-values do not apply SAFE's autocorrelation or effect-size
gates. The normality visualizer reports the KS distance without a p-value because
standardization estimates the mean and spread from the tested sample.

Treat `mintsXU4/output/` as generated output. Regenerate it from the source
data and scripts instead of editing its CSVs or images by hand.

### PM and particle-count animations

The moving-window animation accepts any `pm*` or `pc*` field that is actually
present in the selected Influx-style CSV or daily `.csv.gz` directory. PM axes
use `µg/m³`; IPS-7100 PC values use the sensor protocol's default
`particles/L` unit. For example:

```bash
# Historical entry point (kept for compatibility)
python mintsXU4/mintsPmWindowAnimation.py --field pc0_1

# Neutral entry point; explicit output paths still support GIF or HTML
python mintsXU4/mintsWindowPdfAnimation.py --field pm1_0
python mintsXU4/mintsWindowPdfAnimation.py --field pc0_1 --out pc0_1_preview.html --max-frames 2
```

Without `--out`, files retain the established naming scheme under
`mintsXU4/output/animations/`, such as
`pm1_0_1h_window_pdf_timeseries.gif` and
`pc0_1_1h_window_pdf_timeseries.gif`. Use `--data-dir` for the optional daily
archive; a missing source or absent requested field produces an error listing
the expected path or available fields.

## High-resolution and legacy utilities

Install `pip install -e ".[video]"` for FFmpeg-backed video scripts. The development
extra includes this dependency for video helper tests. PM regeneration preserves
existing outputs and overwrites only the files it generates. The wide PM pickle
cache is trusted local data; rebuild it when source files change.

- [`mintsInfluxDownloader.py`](mintsInfluxDownloader.py) downloads large PM
  histories from InfluxDB in resumable chunks. It reads credentials from
  environment variables rather than command-line arguments.
- [`mintsXU4/mints1sLoader.py`](mintsXU4/mints1sLoader.py) loads the optional
  gzipped one-second PM archive into a cached wide DataFrame.
- [`mintsXU4/mintsPmRegen.py`](mintsXU4/mintsPmRegen.py) regenerates
  PM-specific plots and distribution visualizations from that archive.
- [`mintsXU4/mintsWindowPdfAnimation.py`](mintsXU4/mintsWindowPdfAnimation.py)
  generates PM/PC moving-window PDF animations;
  [`mintsPmWindowAnimation.py`](mintsXU4/mintsPmWindowAnimation.py) is its
  backward-compatible historical entry point.
- [`mintsXU4/mintsDriftAnalysis.py`](mintsXU4/mintsDriftAnalysis.py),
  [`mintsPeriodAnalysis.py`](mintsXU4/mintsPeriodAnalysis.py), and
  [`mintsPeriodPlotter.py`](mintsXU4/mintsPeriodPlotter.py) are compatibility
  shims over the maintained `safe` package.
- The remaining live sensor, serial, and MQTT utilities require the optional
  sensor dependencies:

  ```bash
  pip install -e ".[sensor]"
  ```

See [`scripts.md`](scripts.md) for a fuller one-second-data workflow.

## Repository layout

- [`safe/`](safe/) — maintained package: engine, statistics, loader, period
  comparisons, plotting, and CLI.
- [`tests/`](tests/) — tests for the engine, statistics, loader, and reports.
- [`mintsXU4/`](mintsXU4/) — compatibility shims, legacy live-node tools, and
  high-resolution PM utilities.
- [`mintsXU4/data/`](mintsXU4/data/) — bundled five-minute example export;
  large one-second source files are intentionally ignored by Git.
- [`mintsXU4/output/`](mintsXU4/output/) — generated analysis artifacts.
