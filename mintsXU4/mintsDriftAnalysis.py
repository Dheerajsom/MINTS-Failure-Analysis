# ***************************************************************************

#  Real-Time Sensor Drift & Failure Detection for MINTS
#   ---------------------------------
#   Sensor drift & failure detection using:
#     - Welch's T-test   : mean shift detection
#     - Levene's F-test  : variance inflation detection
#     - Z-score          : per-reading outlier flagging
#     - Hard bounds      : physically impossible value detection
#   --------------------------------------------------------------------------
#   https://github.com/mi3nts/failureAnalysis

# ***************************************************************************

import numpy as np
from scipy import stats
from collections import deque
import warnings
import logging
import pandas as pd
import os

logger = logging.getLogger(__name__)

'''
Don't need this since we're doing local testing not MQTT
'''
# from mintsXU4 import mintsLatest as mL

warnings.filterwarnings("ignore", category=RuntimeWarning)

# Cleaner display names
SENSOR_DISPLAY_NAMES = {
    'IPS7100MHC001': 'IPS7100_MHC_001',
}

# Hard bounds per metric
# SensorDrift class and mintsPeriodAnalysis.py share this
HARD_BOUNDS = {

    'temperature': (-40.0, 100.0),    # Celsius
    'humidity':    (0.0, 100.0),
    'pressure':    (300.0, 1200.0),
    'pm1_0':       (0.0, 10000.0),    # µg/m³
    'shuntVoltage': (-0.320, 0.320)   # INA219 MAX shunt voltage range (V)

}

# --------------------------------------------------------------------------
# Practical-significance gates (shared by streaming + period analysis)
# --------------------------------------------------------------------------
# With large, autocorrelated samples a Welch/Levene p-value collapses toward 0 for
# practically meaningless shifts (e.g. PM1.0 moving 0.3 µg/m³ over ~8000 readings still
# yields p < 1e-15). Statistical significance is therefore necessary but NOT sufficient:
# we additionally require a minimum *effect size* before flagging drift. Effect sizes are
# scale-free and, unlike p-values, do not inflate with sample size or autocorrelation.
#   - MIN_COHENS_D : Cohen's "small" effect floor for a real mean shift (0.2 = small,
#                    0.5 = medium, 0.8 = large). Raise toward 0.5 to alert only on
#                    operationally meaningful PM drift.
#   - MIN_STD_RATIO: spread must change by >= +50% or <= -33% to count as a variance shift.
# Both are intentionally conservative defaults — tune against ground-truth events.
MIN_COHENS_D = 0.2
MIN_STD_RATIO = 1.5

# Smallest move of a *constant* level that counts as a step-change between two flat
# windows, per metric (units match HARD_BOUNDS). Replaces the old single 0.01 threshold,
# which was far too small for some metrics (0.01 µg/m³ PM is noise; 0.01 V shunt is not).
FLAT_MEAN_SHIFT_THRESHOLDS = {
    'temperature':  0.05,   # °C
    'humidity':     0.2,    # %RH
    'pressure':     0.05,   # hPa
    'pm1_0':        0.1,    # µg/m³
    'shuntVoltage': 0.001,  # V
}
DEFAULT_FLAT_MEAN_SHIFT = 0.01

# --------------------------------------------------------------------------
# Recommended deeper enhancements (not yet implemented — see audit notes):
#   1. Autocorrelation-aware effective sample size (n_eff) for the Welch/Levene tests so
#      the reported p-values are honest, e.g. n_eff = n*(1-rho)/(1+rho) for AR(1).
#      The effect-size gates above already neutralize the resulting false-alarm flood,
#      so this is a reporting-accuracy improvement rather than a correctness blocker.
#   2. MAD-based modified z-score (median/MAD) for the per-reading detector — robust to
#      the very outliers a mean/std z-score is non-robust against (masking/swamping).
#   3. A Page-Hinkley / CUSUM layer for faster abrupt-shift detection than the 200-sample
#      window lag. (The consecutive-outlier step-change logic is a lightweight stand-in.)
# --------------------------------------------------------------------------

# --------------------------------------------------
# MQTT Alert Publishing (we will utilize this later)
# --------------------------------------------------

def _publish_alert(sensor_name: str, alert_dict: dict, data_time: str = "N/A") -> None:

    try:
        lines = [f"\n[ALERT] Sensor: {sensor_name} | Data Time: {data_time}"]
        for key, value in alert_dict.items():
            lines.append(f"  - {key}: {value}")
        lines.append("-" * 30)
        logger.warning("\n".join(lines))

    except Exception:
        logger.exception(f"Alert logging failed for {sensor_name}")


# -----------------------------------
# Unpacking & Parsing valo node data
# -----------------------------------

def load_pivoted_dataframe(file_path):

    if not os.path.exists(file_path):
        logger.error(f"Data file not found: {file_path}")
        return None, None

    logger.info(f"Reading data from {file_path}...")

    required_cols = ['_time', '_value', '_field', '_measurement', 'device_id']

    header = pd.read_csv(file_path, comment='#', nrows=0)
    missing = [col for col in required_cols if col not in header.columns]

    # Checks if any headers are missing from the .csv
    if missing:

        logger.error(f"Missing expected columns: {missing}. Detected: {header.columns.tolist()}")
        return None, None

    df = pd.read_csv(file_path, comment='#', usecols=required_cols)

    # Ensure numeric values are properly typed, then drop NaN in critical columns
    df['_value'] = pd.to_numeric(df['_value'], errors='coerce')
    df = df.dropna(subset=['_value', '_time'])

    # Warn on and drop duplicate index rows before pivoting (pivot_table would silently average them)
    duplicates = df.duplicated(subset=['_time', '_measurement', 'device_id', '_field']).sum()

    if duplicates:
        logger.warning(f"{duplicates} duplicate (_time, _measurement, device_id, _field) rows found — keeping first occurrence.")
        df = df.drop_duplicates(subset=['_time', '_measurement', 'device_id', '_field'])

    # Pivot so each _field becomes its own column
    pivot_df = df.pivot_table(index=['_time', '_measurement', 'device_id'], columns='_field', values='_value', aggfunc='first').reset_index()

    logger.info("Pre-converting timestamps...")

    # Vectorized timestamp conversion (avoids slow per-row .apply)
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

    # Metric columns = everything that isn't metadata
    meta_cols = ['_time', '_measurement', 'device_id', '_unix_time', '_str_time', '_sensor_name']
    metric_cols = [col for col in pivot_df.columns if col not in meta_cols]

    # Index by timestamp (DatetimeIndex) so the period analysis can resample; keep needed cols
    pivot_df.index = pd.DatetimeIndex(naive_times.values, name='_dt')
    pivot_df = pivot_df[['_sensor_name', '_unix_time', '_str_time'] + metric_cols]

    return pivot_df, metric_cols

def parse_and_process_valo_data(file_path, engine=None):    

    if engine is None:
        engine = SensorDrift()

    try:
        pivot_df, metric_cols = load_pivoted_dataframe(file_path)

        if pivot_df is None:
            return

        # Convert to list of dicts --> wayy faster than iterrows
        records = pivot_df[['_sensor_name', '_unix_time', '_str_time'] + metric_cols].to_dict(orient='records')

        # Progress log that tells you how many rows the pipeline is about to feed into SAFE Engine
        logger.info(f"Processing {len(records)} data points...")

        # Limit per-row error reporting --> show the first few in detail, then increment counter
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

    except Exception as e:

        logger.exception(f"Error parsing data: {e}")

# -----------
# SAFE Logic
# -----------

# Compare two samples and return descriptive stats + drift-test results.
# `metric` (optional) selects the per-metric flat-step threshold.
def sample_comparison(old, new, p_alpha=0.01, metric=None):

    old = np.asarray(old, dtype=float)
    new = np.asarray(new, dtype=float)

    n_old = int(old.size)
    n_new = int(new.size)

    old_var = float(np.var(old))
    new_var = float(np.var(new))

    old_std = old_var ** 0.5
    new_std = new_var ** 0.5

    old_mean = float(np.mean(old))
    new_mean = float(np.mean(new))
    mean_delta = new_mean - old_mean

    # Variance below this is basically flat
    FLAT_VAR_THRESHOLD = 1e-12

    old_flat = old_var < FLAT_VAR_THRESHOLD
    new_flat = new_var < FLAT_VAR_THRESHOLD

    both_flat = old_flat and new_flat
    half_flat = old_flat != new_flat

    # Per-metric "did the constant level move" threshold (flat-vs-flat step change)
    flat_threshold = FLAT_MEAN_SHIFT_THRESHOLDS.get(metric, DEFAULT_FLAT_MEAN_SHIFT)
    mean_val_changed = abs(mean_delta) > flat_threshold

    # Cohen's d (pooled SD): scale-free practical magnitude of the mean shift. Unlike the
    # p-value it does not inflate with sample size or autocorrelation, so it is the right
    # second gate against large-n false positives.
    denom = n_old + n_new - 2
    pooled_std = ((n_old * old_var + n_new * new_var) / denom) ** 0.5 if denom > 0 else 0.0
    cohens_d = mean_delta / pooled_std if pooled_std > 1e-12 else 0.0

    # Directional fold-change in spread; inf when only one side is flat (regime change).
    if old_std > 1e-12:
        std_ratio = new_std / old_std
    elif new_std > 1e-12:
        std_ratio = float('inf')
    else:
        std_ratio = 1.0

    if both_flat:

        # Two constant arrays --> SciPy tests are meaningless; drift = did the level move
        p_welch = 1.0
        p_levene = 1.0
        mean_shift = mean_val_changed
        variance_shift = False

    else:

        _, p_welch = stats.ttest_ind(old, new, equal_var=False)
        # Brown-Forsythe (median-centered) Levene: robust for the skewed, heavy-tailed
        # distributions typical of PM / environmental data. (Was center="mean".)
        _, p_levene = stats.levene(old, new, center="median")

        p_welch = 1.0 if np.isnan(p_welch) else max(p_welch, 1e-15)
        p_levene = 1.0 if np.isnan(p_levene) else max(p_levene, 1e-15)

        # Two-gate rule: statistically significant AND practically meaningful.
        mean_shift = (p_welch < p_alpha) and (abs(cohens_d) >= MIN_COHENS_D)
        variance_shift = (p_levene < p_alpha) and (
            std_ratio >= MIN_STD_RATIO or std_ratio <= 1.0 / MIN_STD_RATIO
        )

    return{

        'old_n': n_old,
        'new_n': n_new,
        'old_mean': old_mean,
        'new_mean': new_mean,
        'mean_delta': mean_delta,
        'old_std': old_std,
        'new_std': new_std,
        'old_var': old_var,
        'new_var': new_var,
        'both_flat': both_flat,
        'half_flat': half_flat,
        'mean_val_changed': mean_val_changed,
        'cohens_d': float(cohens_d),
        'std_ratio': float(std_ratio),
        'p_welch': float(p_welch),
        'p_levene': float(p_levene),
        'mean_shift': bool(mean_shift),
        'variance_shift': bool(variance_shift),

    }


class SensorDrift:

    # NOTE on z_threshold: 3.5 is the Iglewicz-Hoaglin cutoff for the *modified* z-score
    # (median/MAD based). This detector uses a classic mean/std z-score, for which ~3.0 is
    # the conventional cutoff; 3.5 here is therefore slightly conservative. A mean/std
    # z-score is also non-robust to the outliers it hunts (a burst inflates mean/std and
    # masks later outliers) — a rolling median/MAD modified z-score would be more robust.
    def __init__(self, window_size=200, z_threshold=3.5, p_alpha=0.01):

        self.window_size = window_size
        self.z_threshold = z_threshold
        self.p_alpha = p_alpha

        # Dict. to store the last time an alert was sent
        self._last_alert_time = {}

        # Dict. of deques to store recent vals
        self.history = {}

        # Dict. to track samples since last evaluation for non-overlapping windows
        self.eval_counters = {}

        # Dict. to track consecutive outlier vals per metric (for step-change detection)
        # Stores the actual vals (not just a count) so we can seed the buffer on step-change
        self._consecutive_outliers = {}

        # Running sum and sum-of-squares per (sensor, metric), kept in sync with each buffer.
        # Lets the Z-score read mean/std in O(1) instead of rescanning the whole buffer every reading.
        self._run_sum = {}
        self._run_sumsq = {}

        # Hard limits per metric (module-level constant, shared with the period analysis)
        self.hard_bounds = HARD_BOUNDS

    # Helper function to prevent alert spam
    def _alert_cooldown(self, sensor_name: str, metric: str, alert_type: str, current_timestamp: float, cooldown_seconds=1800) -> bool:

        key = f"{sensor_name}_{metric}_{alert_type}"
        last_time = self._last_alert_time.get(key)

        # If we have alerted before, check the delta
        if last_time is not None:
            delta = current_timestamp - last_time

            if delta < cooldown_seconds:
                return False

        self._last_alert_time[key] = current_timestamp
        return True

    # Update history with new sensor data, ensuring we maintain a fixed window size
    def data_processing(self, sensor_name: str, sensor_dict: dict):

        # Extract pre-converted timestamps (use .get() to avoid mutating the caller's dict)
        current_timestamp = sensor_dict.get('unix_timestamp')
        data_time_str = sensor_dict.get('str_timestamp')

        # Fallback if not using the optimized parser
        if current_timestamp is None:
            dt = sensor_dict.get('dateTime')

            if dt is None:
                return

            current_timestamp = pd.to_datetime(dt).timestamp()
            data_time_str = str(dt)

        # Initialize history for this sensor if not present
        if sensor_name not in self.history:
            self.history[sensor_name] = {}
            self.eval_counters[sensor_name] = {}
            self._consecutive_outliers[sensor_name] = {}
            self._run_sum[sensor_name] = {}
            self._run_sumsq[sensor_name] = {}

        # Process key-value pairs in the sensor dict --> skip non-numeric values and "dateTime"
        for key, val in sensor_dict.items():

            if key in ("dateTime", "unix_timestamp", "str_timestamp"):
                continue

            try:
                value = float(val)

                if not np.isfinite(value):
                    continue

            except (ValueError, TypeError):
                continue # Skip non-numeric values

            # Checking if value violates hard bounds --> publish alert if TRUE
            hard_bounds = self.hard_bounds.get(key)

            if hard_bounds and (value < hard_bounds[0] or value > hard_bounds[1]):

                if self._alert_cooldown(sensor_name, key, "hard-bounds", current_timestamp):

                    _publish_alert(sensor_name, {
                        "alert": "hard-bounds-violation",
                        "metric": key,
                        "value": round(value, 3),
                        "bounds": hard_bounds
                    }, data_time_str)

                # Don't add this value to history
                continue

            # Check if we have a deque for this key to store recent vals
            if key not in self.history[sensor_name]:
                self.history[sensor_name][key] = deque(maxlen=self.window_size)
                self.eval_counters[sensor_name][key] = 0
                self._consecutive_outliers[sensor_name][key] = deque(maxlen=10)
                self._run_sum[sensor_name][key] = 0.0
                self._run_sumsq[sensor_name][key] = 0.0

            # Add new value to the history buffer
            buffer = self.history[sensor_name][key]

            # Z-score outlier detection, only run with 30+ values
            if len(buffer) >= 30:
                # Mean/std from the running accumulators (O(1)) instead of rescanning the buffer.
                # variance = E[x^2] - E[x]^2; clamp at 0 to absorb floating-point noise.
                n = len(buffer)
                mean = self._run_sum[sensor_name][key] / n
                variance = max(self._run_sumsq[sensor_name][key] / n - mean * mean, 0.0)
                std = variance ** 0.5

                # Clamp std to a noise floor so Z-score is always evaluated even on flat baselines when std ≈ 0
                effective_std = max(std, 1e-3)
                z_score = abs((value - mean) / effective_std)

                # If z-score > threshold --> flag as outlier
                if z_score > self.z_threshold:
                    outlier_deque = self._consecutive_outliers[sensor_name][key]
                    outlier_deque.append(value)

                    # If too many consecutive outliers, this is a step-change / regime shift,
                    # not random noise. Reset the buffer and seed it with the saved outlier values.
                    if len(outlier_deque) >= 10:
                        saved_values = list(outlier_deque)

                        if self._alert_cooldown(sensor_name, key, "step-change", current_timestamp):
                            _publish_alert(sensor_name, {
                                "alert": "Step-Change Detected",
                                "metric": key,
                                "value": round(value, 3),
                                "consecutive_outliers": len(saved_values),
                                "old_mean": round(mean, 3)
                            }, data_time_str)

                        # Flush buffer and seed it with the saved outlier values
                        # so the new baseline starts tracking immediately
                        buffer.clear()
                        buffer.extend(saved_values)
                        self.eval_counters[sensor_name][key] = len(saved_values)
                        self._consecutive_outliers[sensor_name][key].clear()

                        # Rebuild the accumulators to match the reseeded buffer
                        self._run_sum[sensor_name][key] = float(sum(saved_values))
                        self._run_sumsq[sensor_name][key] = float(sum(v * v for v in saved_values))
                    else:
                        # Regular single outlier alert (with cooldown)
                        if self._alert_cooldown(sensor_name, key, "z-score", current_timestamp):
                            _publish_alert(sensor_name, {
                                "alert": "z-score-outlier",
                                "metric": key,
                                "value": round(value, 3),
                                "z_score": round(z_score, 3)
                            }, data_time_str)

                    # Don't append outliers to the buffer and don't increment the eval counter
                    continue

            # Value passed all checks — reset consecutive outlier deque and append to buffer
            self._consecutive_outliers[sensor_name][key].clear()

            # Keep the running accumulators in sync with the buffer. A full deque evicts
            # buffer[0] on append, so remove its contribution before adding the new value.
            if len(buffer) == self.window_size:
                evicted = buffer[0]
                self._run_sum[sensor_name][key] -= evicted
                self._run_sumsq[sensor_name][key] -= evicted * evicted

            buffer.append(value)
            self._run_sum[sensor_name][key] += value
            self._run_sumsq[sensor_name][key] += value * value

            # Increment eval counter only for values that actually entered the buffer (Bug 1 fix)
            self.eval_counters[sensor_name][key] += 1

            # Run drift evaluation once we have processed window_size new samples
            if self.eval_counters[sensor_name][key] >= self.window_size:
                # Buffer should be full since counter is now synced with appends
                if len(buffer) == self.window_size:
                    self._evaluate_drift(sensor_name, key, list(buffer), current_timestamp, data_time_str)

                # Rebuild the accumulators from the buffer to clear any floating-point
                # drift accumulated by the incremental add/subtract above.
                self._run_sum[sensor_name][key] = float(sum(buffer))
                self._run_sumsq[sensor_name][key] = float(sum(v * v for v in buffer))

                # Reset counter to half the window for overlapping evaluation windows
                # This ensures we compare samples 101-200 against 201-300, eliminating blind spots
                self.eval_counters[sensor_name][key] = self.window_size // 2


    def _evaluate_drift(self, sensor_name: str, metric: str, data: list, current_timestamp: float, data_time_str: str):

        # Compare the older half of the window against the newer half
        mid = len(data) // 2
        
        result = sample_comparison(data[:mid], data[mid:], self.p_alpha, metric=metric)

        old_mean = result['old_mean']
        new_mean = result['new_mean']

        # Both halves flat: a step-change only matters if the mean actually moved.
        # Return early — SciPy's tests are meaningless on two constant arrays.
        if result['both_flat']:
            if result['mean_val_changed']:
                if self._alert_cooldown(sensor_name, metric, "drift", current_timestamp):

                    _publish_alert(sensor_name, {
                        "alert": "Sensor Drift Detected Step-Change",
                        "metric": metric,
                        "old_mean": round(old_mean, 3),
                        "new_mean": round(new_mean, 3)
                    }, data_time_str)

            return

        # One half flat, the other not: variance regime change. Publish, but do NOT return —
        # we still want the mean/variance tests below (e.g., flatline at 10 → noisy around 100).
        if result['half_flat']:
            if self._alert_cooldown(sensor_name, metric, "variance-regime", current_timestamp):
                _publish_alert(sensor_name, {
                    "alert": "Variance Regime Change",
                    "metric": metric,
                    "detail": "One half is flat while the other has variance",
                    "old_mean": round(old_mean, 3),
                    "new_mean": round(new_mean, 3),
                    "old_variance": round(float(result['old_var']), 6),
                    "new_variance": round(float(result['new_var']), 6)
                }, data_time_str)

        mean_shift_detected = result['mean_shift']
        variance_shift_detected = result['variance_shift']

        # Send alert if either test detected a shift
        if mean_shift_detected or variance_shift_detected:

            # Use the cooldown helper to avoid spamming requests
            if not self._alert_cooldown(sensor_name, metric, "drift", current_timestamp):
                return

            _publish_alert(sensor_name, {
                "alert": "Sensor Drift Detected",
                "metric": metric,
                "mean_shift": mean_shift_detected,
                "variance_shift": variance_shift_detected,
                "cohens_d": round(result['cohens_d'], 3),
                "std_ratio": round(result['std_ratio'], 3),
                "p_welch": format(result['p_welch'], ".2e"),
                "p_levene": format(result['p_levene'], ".2e")
            }, data_time_str)


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    # Get the directory where the script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Path to valo data relative to this script
    data_file = os.path.join(script_dir, 'data', 'valo_node_01_full_year.csv')

    logger.info(f"Current Working Directory: {os.getcwd()}")
    logger.info(f"Resolved Data File Path: {data_file}")

    # Run the parse function
    parse_and_process_valo_data(data_file)