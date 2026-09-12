# SAFE 2 implementation and synthetic pilot — 2026-09-12

Starting branch: `astra/testing`, commit `c152efa`. Yesterday's work was the
Phase 1 evaluation harness (`f742739`), merged into this branch. Its 132 tests
passed before implementation. The original `safe.engine.SensorDrift` algorithm,
its thresholds, compatibility shims, and its baseline scoring were preserved.

This implementation adds the remaining software layers: incident correlation and
recovery; availability/freeze/startup/plausibility detectors; robust environmental
profiles and optional reference residuals; validated per-deployment configuration;
incident-unit evaluation and operator labels; state persistence, bounded retention,
JSON evidence output, and CI. The [health guide](health.md) provides usage and limits.

## Reproducible operating characteristics

Command: `python scripts/evaluate_health.py --require-targets`. Actual local runs
used `.venv/Scripts/python.exe`. Output: `evaluation_output/health-final/`.
The default configuration targets outdoor five-minute MINTS data. The split seeds
were declared before running them; no automated fitting or threshold search used
the evaluation labels. These are separate noise realizations of the same simplified
scenario families, not independent field deployments.

| Split / seed | Legacy raw alerts | Legacy unmatched alerts | Health incidents | False actionable incidents | Diagnostically detected faults | Operator notifications |
|---|---:|---:|---:|---:|---:|---:|
| Calibration / 1729 | 126 | 63 | 29 | 2 | 18/19 | 11 |
| Development / 2718 | 151 | 80 | 27 | 1 | 18/19 | 10 |
| Holdout / 31415 | 147 | 77 | 28 | 0 | 18/19 | 9 |

Each split represents 112 sensor-days. Raw alerts and incidents have different
units; fewer notifications alone do not establish better fault detection. The
health report separately exposes informational detections and actionable recall.
The nine injected isolated spikes are informational; they do not page an operator.

The holdout passed the initial targets: all hard-bound, missing-data, and freeze
faults detected; all persistent-offset faults became actionable; median persistent
offset delay was seven sampling intervals (the eighth reading); zero false
actionable incidents; no raw Page-Hinkley notifications on the diurnal fixture;
and at most one notification per correlated incident.

**Remaining miss:** the slow calibration ramp. An isolated sensor cannot distinguish
that trajectory from legitimate slow environmental movement. Reduced sensitivity
produces general change/noise evidence rather than a validated sensitivity diagnosis.
The calibration and development splits also contain false actionable incidents.
Do not interpret the holdout's zero false incidents as a universal guarantee.

## Verification record

- 177 tests pass, including the original 132, incident scoring, CLI failure paths,
  timestamp quarantine, warm-up contamination, peer freshness, recovery, state
  corruption, and capacity checks. `pip check` reports no broken requirements.
- A 40-day simulation with daily restarts matches uninterrupted engine state
  exactly. Histories and identity counts remain bounded, with checkpoint-size checks.
- A deterministic full-year temperature cycle and a separate daily pressure cycle
  produce no operator notifications with the specified test profiles.
- All five required offline workflows and the new health workflow succeed on the
  committed bundled CSV. Five period CSV schemas, finite probabilities/sample
  counts, all 30 PNG decodes/dimensions, and missing-input failure exits were checked.
  A period plot was visually inspected.
- The source CSV and generated outputs had pre-existing local deletions, so the
  final validation ran in `evaluation_output/health-offline-final-20260912/`, with
  the normal `mintsXU4/output/` layout. `verification.json` records current-source
  hashes, committed input hash, commands, exit codes, and output path.

The unlabeled archive run produced 2,067 incidents and 1,464 notification attempts
over the source history; two incidents remained active at its end. Those counts
are operational measurements, not confirmed failures or a field false-alarm rate.
This is still a substantial investigation burden. Independent labels and peer or
reference data are needed before calling the deployment calibrated or reliable.

Repeated incident updates in the CLI export are spaced by 30 minutes by default;
new evidence and lifecycle changes export immediately, while internal detection
counts remain complete. File archives still require rotation; memory retention and
disk retention are separate concerns.

## Field validation still required

The user selected synthetic validation because real labels/reference data were
not available. No physical inlet/fan experiment or actual deployment was performed.
The software now provides the annotation format, chronological partitioning,
reference input, evidence, and metrics needed for that pilot. Collect healthy
lead-in data, exact fault/recovery times, and independent holdout labels before
claiming calibrated accuracy or enabling stationary-residual Page-Hinkley.

Existing local deletions, `notes.txt`, `.gitattributes`, local device data, and other
unrelated untracked files were preserved. Generated evaluation/validation outputs
remain ignored and are not staged with the source changes.
