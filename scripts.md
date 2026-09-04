# SAFE / MINTS-Failure-Analysis — terminal cheat sheet

Reference for running this repo's **1-second PM/PC data** workflows on a fresh
machine. All commands assume you're in the repo root unless noted.
PowerShell is called out separately wherever its syntax differs from bash.

## 0. Setup

```bash
git clone <repo-url> MINTS-Failure-Analysis
cd MINTS-Failure-Analysis

python -m venv .venv
source .venv/bin/activate          # PowerShell: .venv\Scripts\Activate.ps1

pip install -e ".[dev]"            # safe package + CLI + pytest
pip install -e ".[sensor]"         # only if you need the live-node serial/MQTT scripts

python -m pytest tests/            # full regression suite
```

## 1. The data

`mintsXU4/data/valo_node_01_1s/` (git-ignored) can hold gzipped daily files
for the seven IPS7100 PM bins and seven corresponding PC bins (`pc0_1` through
`pc10_0`). Archives downloaded with the downloader's default fields remain
PM-only; include PC fields with `--fields` when downloading them. PC values
use the IPS protocol's default `particles/L` unit. No
temperature/pressure/humidity is included at this resolution.

- Filename pattern: `valo_node_01_YYYYMMDD_YYYYMMDD.csv.gz` (one calendar day each)
- Size: ~2.9 GB gzipped per year
- Range as of 2026-07: `2025-06-14` → `2026-06-14`

Neither this directory nor its files are tracked in git — download them
(section 2) or copy them from another machine.

## 2. Downloading data from InfluxDB

`mintsInfluxDownloader.py` needs credentials as environment variables —
never pass the token on the command line:

```bash
export INFLUX_HOST="http://mdash.circ.utdallas.edu:8086"
export INFLUX_TOKEN="<api token>"
# INFLUX_ORG defaults to "MINTS"
```

Pull the full 1-second PM history. It's resumable — safe to re-run, it only
fills in missing day-files. `--start auto` probes the first available point;
`--stop` defaults to now if omitted:

```bash
python mintsInfluxDownloader.py --window 1s --gzip --start auto
```

| Flag | Purpose |
|---|---|
| `--start YYYY-MM-DD` | explicit start date instead of `auto` |
| `--stop YYYY-MM-DD` | explicit end date instead of "now" |
| `--chunk-days N` | days per request (default 1 — keep small at 1s resolution) |
| `--fields ...` | override the 7 default PM fields |
| `--out-dir DIR` | default: `mintsXU4/data/valo_node_01_1s` |
| `--merge` | also stitch everything into one merged CSV |
| `--backend cli` | shell out to the `influx` CLI instead of raw HTTP |

## 3. Live terminal alerts — `safe stream`

Replay 1-second PM data through the streaming drift engine, one metric at a
time, across a directory of day-files, as a single continuous stream (state
carries across day boundaries — buffers and baselines aren't reset at each
file):

```bash
python -m safe.cli stream mintsXU4/data/valo_node_01_1s --metric pm1_0 --window 7200
```

> `--window 7200` (2 hours of 1s data) matters, not just a knob: `window=200`
> (the CLI default) is far too short at 1s resolution for the windowed
> Welch/Levene drift test to ever fire — lag-1 autocorrelation is ~0.98, so a
> 200-sample window has two 100-reading halves, each carrying only ~1 effective observation.

If `safe` isn't on `PATH` (e.g. Git Bash on some setups), use
`python -m safe.cli` as above instead of the bare `safe` command.

**One week only** — first 7 day-files, chronological by filename:

```powershell
# PowerShell
$files = (Get-ChildItem mintsXU4\data\valo_node_01_1s\*.csv.gz | Sort-Object Name | Select-Object -First 7).FullName
python -m safe.cli stream $files --metric pm1_0 --window 7200
```

```bash
# bash
python -m safe.cli stream $(ls mintsXU4/data/valo_node_01_1s/*.csv.gz | sort | head -7) --metric pm1_0 --window 7200
```

Swap `-First 7` / `head -7` for a later slice to shift the window, e.g.
`-Skip 30 -First 7` for the 5th week of data.

| Flag | Purpose |
|---|---|
| `--metric NAME` | repeatable; restrict to one or more metrics |
| `--z-threshold N` | modified z-score outlier cutoff (default 3.5) |
| `--alpha N` | significance level for Welch/Levene (default 0.01) |
| `--page-hinkley` | opt-in sequential mean-shift layer (off by default — alarms daily on genuine diurnal weather shifts on ambient outdoor data) |
| `--no-autocorr` | disable the n_eff autocorrelation correction |

## 4. Daily alert-count summary over the whole 1s dataset

Loops a **fresh** engine per day-file (no cross-day state) and tabulates
alert counts to a CSV — good for eyeballing trends across many days at once,
not for live terminal alerts (use section 3 for that):

```bash
python scripts/summarize_daily_drift.py
python scripts/summarize_daily_drift.py --variant default --variant no-autocorr --variant page-hinkley
python scripts/summarize_daily_drift.py --limit 10 -o /tmp/drift.csv
```

Default output: `mintsXU4/output/drift_day_summary.csv`

## 5. Regenerating PM plots/histograms from the 1s data

```bash
cd mintsXU4
python mintsPmRegen.py
```

Regenerates **all 7 PM bins'** plots/histograms into `output/<field>/plots/`
and `output/<field>/histograms/` (168 PNGs total; no CSVs written). Reads
and caches a wide float32 frame at
`data/valo_node_01_1s/_wide_pm_cache.pkl` (git-ignored) — pass
`rebuild=True` in `mints1sLoader.load_wide()` if the raw data changed.

## 6. Tests

```bash
python -m pytest tests/                      # full suite
python -m pytest tests/test_engine.py -v     # one module, verbose
```
