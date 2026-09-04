# ***************************************************************************
#  SAFE — InfluxDB-export CSV loading and streaming replay
# ***************************************************************************

import logging
import os

import numpy as np
import pandas as pd

from safe.config import SENSOR_DISPLAY_DEVICES, SENSOR_DISPLAY_NAMES

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = ['_time', '_value', '_field', '_measurement', 'device_id']

# Metadata columns of the pivoted frame; everything else is a metric
_META_COLS = ['_time', '_measurement', 'device_id', '_unix_time', '_str_time', '_sensor_name']


def load_pivoted_dataframe(file_path):
    """Load an InfluxDB-style long CSV and pivot fields into metric columns.

    Returns (pivot_df, metric_cols) or (None, None) on failure. The frame is
    indexed by a tz-naive UTC DatetimeIndex named '_dt' so callers can resample.
    """
    if not os.path.exists(file_path):
        logger.error(f"Data file not found: {file_path}")
        return None, None

    logger.info(f"Reading data from {file_path}...")

    try:
        header = pd.read_csv(file_path, comment='#', nrows=0)
    except (OSError, ValueError, pd.errors.ParserError) as exc:
        logger.error("Cannot read %s: %s", file_path, exc)
        return None, None
    missing = [col for col in REQUIRED_COLUMNS if col not in header.columns]
    if missing:
        logger.error(f"Missing expected columns: {missing}. Detected: {header.columns.tolist()}")
        return None, None

    try:
        df = pd.read_csv(file_path, comment='#', usecols=REQUIRED_COLUMNS,
                         dtype={'device_id': 'string', '_measurement': 'string', '_field': 'string'})
    except (OSError, ValueError, pd.errors.ParserError) as exc:
        logger.error("Cannot read %s: %s", file_path, exc)
        return None, None

    # Ensure numeric values are properly typed, then drop NaN in critical columns
    df['_value'] = pd.to_numeric(df['_value'], errors='coerce')
    df['_time'] = pd.to_datetime(df['_time'], format='ISO8601', utc=True, errors='coerce')
    valid = df[REQUIRED_COLUMNS].notna().all(axis=1) & np.isfinite(df['_value'])
    if not valid.all():
        logger.warning("Discarding %d rows with invalid values, timestamps or identifiers", (~valid).sum())
    df = df.loc[valid]
    if df.empty:
        logger.error("No valid readings in %s", file_path)
        return None, None
    reserved = set(_META_COLS) | {'dateTime', 'unix_timestamp', 'str_timestamp'}
    if reserved.intersection(df['_field']):
        logger.error("Metric names collide with reserved metadata columns")
        return None, None

    # Warn on and drop duplicate index rows before pivoting (pivot_table would
    # silently average them)
    dup_keys = ['_time', '_measurement', 'device_id', '_field']
    duplicates = df.duplicated(subset=dup_keys).sum()
    if duplicates:
        logger.warning(f"{duplicates} duplicate {tuple(dup_keys)} rows found — keeping first occurrence.")
        df = df.drop_duplicates(subset=dup_keys)

    # Pivot so each _field becomes its own column
    pivot_df = df.pivot(index=['_time', '_measurement', 'device_id'],
                        columns='_field', values='_value').reset_index()

    logger.info("Pre-converting timestamps...")

    parsed_times = pivot_df['_time']

    # Timestamps were parsed as UTC before duplicate removal.
    naive_times = parsed_times.dt.tz_localize(None)

    pivot_df['_unix_time'] = naive_times.astype('datetime64[ns]').astype('int64') / 1e9
    pivot_df['_str_time'] = parsed_times.dt.strftime('%Y-%m-%d %H:%M:%S')

    # Friendly sensor names (fall back to "{measurement}_{device_id}")
    pivot_df['_sensor_name'] = pivot_df['_measurement'].map(SENSOR_DISPLAY_NAMES).fillna(
        pivot_df['_measurement'] + '_' + pivot_df['device_id'].astype(str)
    )
    # Preserve the historical display name for the bundled device. Other
    # devices must retain their identity, including across separate CSV files.
    known_other = pivot_df['_measurement'].isin(SENSOR_DISPLAY_NAMES) & (
        pivot_df['device_id'] != pivot_df['_measurement'].map(SENSOR_DISPLAY_DEVICES))
    pivot_df.loc[known_other, '_sensor_name'] += '_' + pivot_df.loc[known_other, 'device_id']

    metric_cols = [col for col in pivot_df.columns if col not in _META_COLS]

    # Index by timestamp (DatetimeIndex) so the period analysis can resample
    pivot_df.index = pd.DatetimeIndex(naive_times.values, name='_dt')
    pivot_df = pivot_df[['_sensor_name', '_unix_time', '_str_time'] + metric_cols].sort_index(kind='stable')

    return pivot_df, metric_cols


def replay_csv(file_path, engine=None, metrics=None):
    """Replay a CSV through a SensorDrift engine as if it were streaming.

    `metrics`, if given, restricts processing to that subset of metric columns
    (e.g. ['pm1_0']) — other fields present in the file are ignored entirely.

    Returns the engine (with its .alerts history) or None on load or row failure.
    A supplied engine retains any successfully processed rows on failure.
    """
    # Imported here so loading data never requires the engine's dependencies
    from safe.engine import SensorDrift

    if engine is None:
        engine = SensorDrift()

    pivot_df, metric_cols = load_pivoted_dataframe(file_path)
    if pivot_df is None:
        return None

    if metrics is not None:
        missing = [m for m in metrics if m not in metric_cols]
        if missing:
            logger.warning(f"Requested metric(s) not found in {file_path}: {missing}")
        metric_cols = [m for m in metric_cols if m in metrics]
        if not metric_cols:
            logger.error(f"None of the requested metrics {metrics} are present in {file_path}")
            return None

    records = pivot_df[['_sensor_name', '_unix_time', '_str_time'] + metric_cols].itertuples(index=False, name=None)
    logger.info("Processing %d data points...", len(pivot_df))

    # Limit per-row error reporting: show the first few in detail, then count
    MAX_ROW_ERROR_TRACES = 3
    error_count = 0

    for sensor_name, timestamp, label, *values in records:
        record = dict(zip(metric_cols, values))
        record['unix_timestamp'] = timestamp
        record['str_timestamp'] = label

        try:
            engine.data_processing(sensor_name, record)
        except Exception as row_err:
            error_count += 1
            if error_count <= MAX_ROW_ERROR_TRACES:
                logger.exception(f"Error processing row: {row_err}")
            elif error_count == MAX_ROW_ERROR_TRACES + 1:
                logger.warning("Further row errors will be counted silently and summarized at the end...")

    logger.info("Data processing complete.")
    if error_count:
        logger.warning(f"Skipped {error_count} row(s) due to processing errors.")
        return None

    return engine


def replay_csvs(file_paths, engine=None, metrics=None):
    """Replay several CSVs through ONE SensorDrift engine, in order.

    Files must be supplied in chronological, non-overlapping order.
    State (buffers, baselines, cooldowns) carries across files, so a run over
    consecutive day-files behaves like a single continuous stream instead of
    resetting at each file boundary. Returns the engine, or None if any file
    fails to load.
    """
    from safe.engine import SensorDrift

    if engine is None:
        engine = SensorDrift()

    for i, file_path in enumerate(file_paths, 1):
        logger.info(f"[{i}/{len(file_paths)}] {file_path}")
        result = replay_csv(file_path, engine=engine, metrics=metrics)
        if result is None:
            return None

    return engine


# Backwards-compatible alias (previous public name)
def parse_and_process_valo_data(file_path, engine=None):
    return replay_csv(file_path, engine=engine)
