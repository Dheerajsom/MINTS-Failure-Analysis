# SAFE code quality audit — September 2026

## Scope and starting state

Reviewed the maintained `safe` package (statistics, streaming engine, loader,
periods, plotting, animation helpers, CLI and exports), compatibility shims,
high-resolution PM loaders/regeneration, both distribution visualizers,
PM/PC/wind/Fort Worth animation and batch-rendering utilities, the downloader,
daily summary script, tests, packaging, README, scripts guide and AGENTS.md.
Live definitions, MQTT and serial utilities received static review only; their
hardware behavior was not changed or exercised.

Started on `safe-v2` at `ff1d5b9`. Existing changes included deletion of the
bundled CSV and 26 generated outputs, a modified `notes.txt`, and untracked
documents, local data, `.gitattributes`, and agent artifacts. These were preserved
and excluded from the audit commit. Work was moved to the requested `safe-v2.5`.

## Fixed findings

- **Loading and identity:** mixed-offset timestamps could fail or replay out of
  chronological order, and equivalent instants were deduplicated as different
  strings. Normalize to UTC first, keep the first valid duplicate, sort stably,
  preserve fractional Unix seconds and leading-zero device IDs. Drop invalid
  timestamps, missing identifiers and nonfinite values with diagnostics. Empty,
  unreadable and malformed input returns the documented failure sentinel.
  Reserved metric names cannot overwrite replay metadata. The bundled sensor's
  historical label is retained; other devices get a stable device-ID suffix.
- **Replay:** replaced the full list of row dictionaries with tuple iteration
  and one transient reading dictionary. Partial processing errors return failure
  rather than a misleading success. Supplied engines retain partial state.
  Out-of-order readings are rejected before changing detection state. Equal
  timestamps remain allowed for separately delivered metrics; callers must avoid
  replaying overlapping files or duplicate readings across files.
- **Engine:** validate window size, thresholds, alpha, cooldown, callback and
  timestamps. Tuple cooldown keys prevent underscore collisions. Engine-local
  hard bounds no longer mutate global configuration. Alternating high/low spikes
  no longer trigger a false sustained step. Hard-bound violations break a pending
  outlier run. Page-Hinkley rejects nonfinite residuals and invalid parameters.
- **Statistics and periods:** enforce finite 1-D samples with at least two
  readings and valid alpha. Validate bucket minimum sizes and remove infinities
  even for unknown metrics. Avoid redundant sorts of already ordered series.
  Preserve the existing Welch formula, pooled effect-size calculation, flat
  thresholds and conservative AR(1) policy; qualify its statistical limitations.
- **Plots and CLI:** plotting failures now fail period analysis and CLI execution;
  legacy analysis entry points also return nonzero on load failure. Plot alpha
  follows the analysis parameter. Sensor series no longer get connected or
  standardized together. Figures close when saving fails. Reuse parsed frames
  for zoom plots rather than reading each CSV again. Shared shims retain their
  existing public re-exports and default call signatures.
- **Visualizers:** `dataVisualizer` is import-safe and headless, uses the shared
  loader/bounds, handles absent fits, and writes UTF-8 Markdown. Both visualizers
  resolve default paths from their source location and reject mixed-sensor input.
  Correct overlapping week/month ranges. Omit the ordinary KS p-value after
  fitting mean/std from the tested sample; retain the descriptive distance.
  Small/flat samples do not run the normality test. Explain that exploratory
  distribution tests do not apply SAFE's drift gates or correlation correction.
  See [SciPy's fitted goodness-of-fit guidance](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.goodness_of_fit.html).
- **High-resolution workflows:** convert each daily PM pivot to float32 before
  retaining it; accept CSV annotations, normalize timestamps, remove overlapping
  timestamps and apply shared PM bounds. Validate `max_files`. Remove automatic
  recursive deletion of old PM deliverables from regeneration. Correct date-only
  end slices that previously selected just midnight. Animation windows use
  binary-search slicing instead of scanning the full index on every frame.
- **Utilities:** daily summary reports fail when input processing fails, with no
  empty-frame groupby crash or import-time logger mutation. Downloader rejects
  zero/negative chunk sizes, retries and timeouts before networking; the old zero
  chunk size could loop forever. Auto-start considers all returned tables.
  Stream CLI stderr to a temporary file to avoid a full-stderr-pipe deadlock.
- **Development:** declare pandas >=2.0 for ISO8601 parsing and the missing
  `imageio-ffmpeg` video/dev dependency. Ignore local virtual environments and
  credentials. Update README, scripts.md and AGENTS.md with validation, failure
  semantics, data contracts, generated-output preservation and Git procedures.

## Verification

- Initial `python -m pytest tests/` could not run because Python was absent from
  PATH. The bundled runtime lacked pytest; after installing development
  dependencies, collection exposed undeclared `imageio_ffmpeg`. Once installed,
  the unchanged baseline passed **76 tests**.
- Final full suite: **111 passed**, including 35 new regression cases. Pytest's
  cache was redirected to temporary storage because the pre-existing repository
  cache was not writable. `pip check` passed.
- Both installed `safe` commands, both legacy analysis scripts and
  `dataVisualizer.py` passed against the committed bundled CSV. Also passed
  `standardNormalVisualizer.py` and a two-frame GIF animation preview.
- To preserve the user's deleted data/output files, validation ran in an isolated
  temporary source copy with the original CSV extracted read-only from Git.
  Output retained the required `mintsXU4/output/` layout. Local evidence lives in
  `C:/Users/dheer/AppData/Local/Temp/safe-audit-al9rlc6l/`, including workflow logs.
- The five period CSVs contained 1,980 daily, 285 weekly, 63 monthly, 6 yearly and
  3 first/last-month comparisons. Their column schemas, sample counts and
  probability ranges passed checks. All 30 PNGs from the five requested
  workflows decoded successfully; a significance plot was also visually checked.
- A row-preparation microbenchmark on the 187,638-row pivoted bundled frame used
  67.43 MiB peak traced allocation and 3.28 seconds for the old dictionary list,
  versus 0.01 MiB and 1.00 second for tuple iteration plus transient dictionaries.
  This excludes CSV loading, DataFrame storage, detection and alert history;
  it is not an end-to-end memory or runtime claim.

## Remaining limitations and follow-up decisions

- AR(1) effective size and Levene thinning are heuristics for regularly sampled
  correlated data. Higher-order dependence, missing intervals, seasonal cycles,
  multiple testing and overlapping evaluations still require empirical false
  alarm calibration. Flat/half-flat regime alerts are heuristic signals, not
  proof of sensor failure. No unvalidated statistical redesign was introduced.
- General replay still reads/pivots a complete CSV, and alert history grows
  without a retention cap. Truly bounded-memory input needs an explicit chunk
  boundary, duplicate and ordering contract. Period analysis still retains
  buckets and output rows; large archives need memory-budget testing.
- Specialized daily archive and wind loaders assume one device and retain some
  separate parsing/plotting code. Consolidating these with the general loader
  needs fixture coverage for archive-specific metadata and cache behavior.
- The PM pickle cache still requires explicit rebuild after source changes;
  trusted pickle inputs must remain local. Automatic provenance/invalidation and
  atomic cache/output publishing are follow-up work.
- Video scripts still duplicate encoder/palette helpers. An advertised NVENC
  encoder does not establish usable GPU hardware; batch rendering preflights it,
  but standalone automatic encoder selection needs a similar capability check.
  Full-year video, multi-GB local archives, real hardware and network downloads
  were not exercised. Existing helper tests and a short offline animation passed.
- Live modules still enumerate hardware/read configuration on import and use
  legacy MQTT callbacks. They remain outside offline imports; changing their
  supported behavior requires hardware and sensor-extra validation.
- Exploratory distributions retain nominal two-sample p-values and fixed
  historical date comparisons. Standard period plots intentionally cover the
  three documented metrics. Calibration, configurable date ranges and plots for
  every metric are separate product/statistical decisions.
- Historical generated files remain tracked. This audit commits source, tests
  and guidance, not regenerated output or pre-existing user modifications.
