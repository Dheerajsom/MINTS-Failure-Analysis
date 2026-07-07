# ***************************************************************************
#  SAFE — InfluxDB-export CSV loading and streaming replay
# ***************************************************************************

import logging
import os

import pandas as pd

from safe.config import SENSOR_DISPLAY_NAMES

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

    header = pd.read_csv(file_path, comment='#', nrows=0)
    missing = [col for col in REQUIRED_COLUMNS if col not in header.columns]
    if missing:
        logger.error(f"Missing expected columns: {missing}. Detected: {header.columns.tolist()}")
        return None, None

    df = pd.read_csv(file_path, comment='#', usecols=REQUIRED_COLUMNS)

    # Ensure numeric values are properly typed, then drop NaN in critical columns
    df['_value'] = pd.to_numeric(df['_value'], errors='coerce')
    df = df.dropna(subset=['_value', '_time'])

    # Warn on and drop duplicate index rows before pivoting (pivot_table would
    # silently average them)
    dup_keys = ['_time', '_measurement', 'device_id', '_field']
    duplicates = df.duplicated(subset=dup_keys).sum()
    if duplicates:
        logger.warning(f"{duplicates} duplicate {tuple(dup_keys)} rows found — keeping first occurrence.")
        df = df.drop_duplicates(subset=dup_keys)

    # Pivot so each _field becomes its own column
    pivot_df = df.pivot_table(index=['_time', '_measurement', 'device_id'],
                              columns='_field', values='_value', aggfunc='first').reset_index()

    logger.info("Pre-converting timestamps...")

    parsed_times = pd.to_datetime(pivot_df['_time'], format='ISO8601')

    # Normalize to tz-naive UTC. tz_convert raises on tz-naive input, so localize
    # those first; convert tz-aware (any offset) to UTC before dropping the tz.
    if parsed_times.dt.tz is None:
        naive_times = parsed_times
    else:
        naive_times = parsed_times.dt.tz_convert('UTC').dt.tz_localize(None)

    pivot_df['_unix_time'] = naive_times.astype('datetime64[s]').astype('int64')
    pivot_df['_str_time'] = parsed_times.dt.strftime('%Y-%m-%d %H:%M:%S')

    # Friendly sensor names (fall back to "{measurement}_{device_id}")
    pivot_df['_sensor_name'] = pivot_df['_measurement'].map(SENSOR_DISPLAY_NAMES).fillna(
        pivot_df['_measurement'] + '_' + pivot_df['device_id'].astype(str)
    )

    metric_cols = [col for col in pivot_df.columns if col not in _META_COLS]

    # Index by timestamp (DatetimeIndex) so the period analysis can resample
    pivot_df.index = pd.DatetimeIndex(naive_times.values, name='_dt')
    pivot_df = pivot_df[['_sensor_name', '_unix_time', '_str_time'] + metric_cols]

    return pivot_df, metric_cols


def replay_csv(file_path, engine=None):
    """Replay a CSV through a SensorDrift engine as if it were streaming.

    Returns the engine (with its .alerts history) or None on load failure.
    """
    # Imported here so loading data never requires the engine's dependencies
    from safe.engine import SensorDrift

    if engine is None:
        engine = SensorDrift()

    pivot_df, metric_cols = load_pivoted_dataframe(file_path)
    if pivot_df is None:
        return None

    # List of dicts is much faster to iterate than iterrows()
    records = pivot_df[['_sensor_name', '_unix_time', '_str_time'] + metric_cols].to_dict(orient='records')
    logger.info(f"Processing {len(records)} data points...")

    # Limit per-row error reporting: show the first few in detail, then count
    MAX_ROW_ERROR_TRACES = 3
    error_count = 0

    for record in records:
        sensor_name = record.pop('_sensor_name')
        record['unix_timestamp'] = record.pop('_unix_time')
        record['str_timestamp'] = record.pop('_str_time')

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

    return engine


# Backwards-compatible alias (previous public name)
def parse_and_process_valo_data(file_path, engine=None):
    return replay_csv(file_path, engine=engine)
