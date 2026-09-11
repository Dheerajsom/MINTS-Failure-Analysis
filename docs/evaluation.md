# Reproducible SAFE baseline evaluation

Run `.venv\Scripts\python.exe scripts/evaluate_engine.py --seed 1729`.
Outputs are `evaluation_output/evaluation.json` and `summary.md`; this directory
is ignored. Override it with `--output`. No production detector code or thresholds
are changed. Engine defaults, shared statistical gates, physical bounds, internal
constants, package versions, Git revision, tracked dirty flag, seeds, durations,
sampling intervals, restarts, and ingestion errors are recorded in JSON.

`safe.scenarios` contains the input model and deterministic generators.
`safe.evaluation.replay` collects unmodified emitted payloads through the public
callback. `score` assigns alerts to labels; `evaluate` aggregates; `write_reports`
serializes strict JSON (undefined quantities use null) and Markdown. To add real
labeled data, construct `Scenario`, `Reading`, and `Fault` instances and call
`evaluate`. Times are Unix seconds in UTC. Arrival order is preserved. Explicit
observation bounds and sensor/metric identities include periods without readings.
Each declared sensor is assumed observed for the entire scenario interval; split
datasets into scenarios when deployment/exposure bounds differ between sensors.

## Attribution policy (version 1)

An alert is evidence compatible with a label, not confirmation of a sensor
failure. Matching requires exact sensor and metric identity, a compatible category,
and timestamp in `[fault.start, fault.end + grace_seconds)`, clipped at observation
end. Default grace is zero, with no early tolerance. A single-reading label spans
one sampling interval. Overlapping candidates are resolved by earliest start then
lexicographic fault ID. One alert belongs to at most one fault. Later repeated
alerts stay attributed to that event and never add another true positive.

| Alert type | Eligible label categories |
|---|---|
| hard-bounds-violation | physical_bounds |
| z-score-outlier | spike, level_offset, calibration_drift, noise_increase |
| Step-Change Detected | level_offset, calibration_drift |
| Sensor Drift Detected Step-Change | level_offset, calibration_drift |
| Sustained Mean Shift (Page-Hinkley) | level_offset, calibration_drift |
| Variance Regime Change | freeze, noise_increase, sensitivity_loss |
| Sensor Drift Detected | level_offset/calibration_drift only when mean_shift; freeze/noise_increase/sensitivity_loss only when variance_shift |

Unknown types are unmatched. Missing data and timestamp order have no compatible
production alert. Expected out-of-order `ValueError`s are separate diagnostics;
unexpected errors fail the run. Restart discards all engine history and cooldowns
before the marked reading while retaining already collected alerts. No fabricated
gap readings, timer callbacks, or forced freeze alarms are introduced.

## Units and denominators

- TP and FN count labeled fault events, detected or missed. FP counts unmatched
  emitted alerts, including incompatible alerts inside a labeled fault interval.
- `precision = TP / (TP + FP)` is an event/alert hybrid useful for alarm burden,
  not conventional reading classification precision. `alert_precision` separately
  divides attributed alerts by all emitted alerts. Recall is TP / labeled events.
  No true-negative reading count, specificity, or accuracy is inferred.
- Reading-level alerts count emitted payloads after native cooldowns; multiple
  layers may emit at one reading. These are not unique anomalous readings.
- Delay is first qualifying alert time minus onset; per-event delays and counts
  are retained. Mean delay excludes misses (null), so inspect recall alongside it.
- Alerts per expected fault uses all emitted alerts / all labeled events;
  matched alerts per expected fault excludes FP. Both are null without labels.
- Sensor-days sum elapsed scenario duration times distinct sensor count, including
  gaps and restart downtime. Metrics on the same sensor do not multiply exposure.
- Metric breakdowns use exposure for sensors declaring that metric. Category/type
  breakdowns use total suite sensor-days. Category `unmatched` holds all FP;
  their causal category is unknown. Alert-type recall includes only labels eligible
  for that type, and recomputes counts/delays using that type's assigned alerts.
  Events can appear under multiple alert types; do not sum their TP/FN totals.

## Fixtures and limitations

Sixteen independent seeded fixtures each span seven days at five-minute sampling.
They include IID Gaussian noise (sigma 0.3), stationary AR(1) rho 0.95, an 8-degree
diurnal sinusoid, a 12-degree slow ramp, and a legitimate Gaussian-shaped PM episode.
Faults include a 150-degree reading, spaced 12-degree spikes, an 8-degree permanent
offset, an 8-degree calibration ramp, a constant freeze, a one-day gap, fivefold
noise, and sinusoidal sensitivity reduced to 15%. An out-of-order arrival tests
rejection; restart occurs one reading after offset onset. Startup freeze is an
additional unsupported-mode control. Generator code is the exact specification;
fixture child seed is suite seed plus fixed fixture index.

These are simplified signals, not validated sensor physics or field labels. A
seven-day ramp is only a seasonal-like segment. Abrupt-offset first detection may
be a generic outlier before the later step alert; attribution does not establish
fault diagnosis. Restart detection can precede restart and does not prove recovery.
Long label intervals may credit coincidental alerts. Results cover one fixed seed,
cadence, and configuration without confidence intervals or environmental diversity.
Healthy environmental alerts are scored as FP for *sensor failure*, even if useful
as environmental-change notifications. Do not infer field accuracy or readiness.

Baseline observed with seed 1729: IID 2 unmatched alerts (0.286/day), AR(1) 17
(2.429/day), diurnal 0, slow movement 20 (2.857/day), legitimate PM episode 16
(2.286/day). This particular smooth diurnal fixture does not reproduce prior
diurnal false alerts; the other healthy cases expose substantial alarm burden.
Hard bounds and abrupt offset each detect one event at zero delay. Missing data,
startup freeze, decreased sensitivity, and timestamp order each miss their label.
These observations are regression anchors, not targets for tuning this phase.

## Phase 1 verification record

Original base: `safe-v2.5`, commit `1f62592ed26941fb56d791973b7443ad0b22f7b2`.
The pre-change suite passed 111 tests; the completed suite passed 132. `pip check`
reported no broken requirements. The pre-existing pytest cache permission warning
does not affect test results. Two seed-1729 CLI runs produced byte-identical JSON.
Overall: 126 emitted alerts, 63 attributed alerts, 63 unmatched alerts, 15 detected
fault events, 4 missed events; recall 15/19 and event/alert precision 15/78.

Commands from the repository root (PowerShell):

```powershell
.\.venv\Scripts\python.exe -m pytest tests/
.\.venv\Scripts\python.exe -m pip check
.\.venv\Scripts\python.exe scripts/evaluate_engine.py --seed 1729 --output evaluation_output/run1
.\.venv\Scripts\python.exe scripts/evaluate_engine.py --seed 1729 --output evaluation_output/run2
Get-FileHash evaluation_output/run1/evaluation.json,evaluation_output/run2/evaluation.json
```

Deleted source CSV/generated outputs and all other pre-existing changes were
preserved. Offline validation extracted the committed source with `git archive`
into `C:\Users\dheer\Documents\SAFE\evaluation_output\offline_validation`, excluding
old generated outputs. The engine/loader/plotting sources were unchanged by Phase 1.
The following commands ran there with the repository's explicit `.venv` Python
(`python` below denotes that executable); `python -m safe.cli` invokes the same
entry point as the installed `safe` command:

```text
python -m safe.cli stream mintsXU4/data/valo_node_01_full_year.csv
python -m safe.cli periods mintsXU4/data/valo_node_01_full_year.csv -o mintsXU4/output
python mintsXU4/mintsDriftAnalysis.py
python mintsXU4/mintsPeriodAnalysis.py
python dataVisualizer.py
```

The visualizer initially exited 1 because its existing Unicode arrow could not
be printed to a cp1252 log. Retrying with `PYTHONIOENCODING=utf-8` and an isolated
`MPLCONFIGDIR` succeeded; no production source change was needed. All five workflows
then exited 0. Outputs remained under the isolated `mintsXU4/output/` layout.
Period CSV schemas, finite probabilities in [0,1], positive finite sample counts,
and decoding/dimensions of all 30 PNGs were checked; a period plot was visually
inspected. Missing CSV invocations of stream/periods and a negative evaluation
grace interval each exited 1. Generated reports, logs, validation copy and archive
remain ignored under `evaluation_output/` and are not committed.
