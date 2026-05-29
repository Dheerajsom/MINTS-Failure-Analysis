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
import traceback
import pandas as pd
import os

'''
Don't need this since we're doing local testing not MQTT
'''
# from mintsXU4 import mintsLatest as mL

warnings.filterwarnings("ignore", category=RuntimeWarning)

# Shorter display names for IPS sensor
SENSOR_DISPLAY_NAMES = {
    'IPS7100MHC001': 'IPS7100_MHC_001',
}

# --------------------------------------------------
# MQTT Alert Publishing (we will utilize this later)
# --------------------------------------------------

def _publish_alert(sensor_name: str, alert_dict: dict, data_time: str = "N/A") -> None:

    try:
        print(f"\n[ALERT] Sensor: {sensor_name} | Data Time: {data_time}") # Local testing, we print the alerts directly to the console

        for key, value in alert_dict.items():

            print(f"  - {key}: {value}")

        print("-" * 30)

    except Exception:
        print(f"Alert logging failed for {sensor_name}")
        traceback.print_exc()


# -----------------------------------
# Unpacking & Parsing valo node data
# -----------------------------------

def parse_and_process_valo_data(file_path):
   
    if not os.path.exists(file_path):

        print(f"Data file not found: {file_path}")
        return

    print(f"Reading data from {file_path}...")
    
    try:
        required_cols = ['_time', '_value', '_field', '_measurement', 'device_id']

        # InfluxDB annotated-CSV header rows (#group/#datatype/#default); plain CSV works too.
        # usecols keeps memory bounded by dropping unused columns (result, table, _start, _stop).
        header = pd.read_csv(file_path, comment='#', nrows=0)
        missing = [col for col in required_cols if col not in header.columns]

        if missing:
            print(f"Error: Missing expected columns: {missing}")
            print(f"Detected columns were: {header.columns.tolist()}")
            return

        df = pd.read_csv(file_path, comment='#', usecols=required_cols)

        # Ensure numeric values are properly typed
        df['_value'] = pd.to_numeric(df['_value'], errors='coerce')
        
        # Drop rows with NaN values in critical columns
        df = df.dropna(subset=['_value', '_time'])
        
        # Pivot the data to get fields as columns if multiple fields exist for the same timestamp
        pivot_df = df.pivot_table(index=['_time', '_measurement', 'device_id'], columns='_field', values='_value').reset_index()

        print("Pre-converting timestamps...")

        # Pre-convert timestamps to avoid doing it per-row in the loop
        parsed_times = pd.to_datetime(pivot_df['_time'], format='ISO8601')

        # Vectorized Unix timestamp conversion (avoids slow per-row .apply(lambda))
        pivot_df['_unix_time'] = parsed_times.dt.tz_convert(None).astype('datetime64[s]').astype('int64')
        pivot_df['_str_time'] = parsed_times.dt.strftime('%Y-%m-%d %H:%M:%S')

        # Pre-calculate sensor names vectorially, using friendly display names where defined
        # (falls back to "{measurement}_{device_id}" for any measurement not in the map)
        pivot_df['_sensor_name'] = pivot_df['_measurement'].map(SENSOR_DISPLAY_NAMES).fillna(
            pivot_df['_measurement'] + '_' + pivot_df['device_id'].astype(str)
        )

        # Identify metric columns (everything that isn't metadata)
        meta_cols = ['_time', '_measurement', 'device_id', '_unix_time', '_str_time', '_sensor_name']
        metric_cols = [col for col in pivot_df.columns if col not in meta_cols]

        # Convert to list of dicts --> wayy faster than iterrows
        records = pivot_df[['_sensor_name', '_unix_time', '_str_time'] + metric_cols].to_dict(orient='records')

        print(f"Processing {len(records)} data points...")

        # Limit per-row error reporting --> show the first few in detail, then increment counter
        MAX_ROW_ERROR_TRACES = 3
        error_count = 0

        for record in records:

            sensor_name = record.pop('_sensor_name')
            record['unix_timestamp'] = record.pop('_unix_time')
            record['str_timestamp'] = record.pop('_str_time')

            try:

                drift_engine.data_processing(sensor_name, record)

            except Exception as row_err:

                error_count += 1

                if error_count <= MAX_ROW_ERROR_TRACES:
                    print(f"Error processing row: {row_err}")
                    traceback.print_exc()
                elif error_count == MAX_ROW_ERROR_TRACES + 1:
                    print("Further row errors will be counted silently and summarized at the end...")

            
        print("Data processing complete.")

        if error_count:
            print(f"WARNING: skipped {error_count} row(s) due to processing errors.")

    except Exception as e:

        print(f"Error parsing data: {e}")
        traceback.print_exc()

# -----------
# SAFE Logic
# -----------

class SensorDrift:

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
        self._consecutive_outliers = {}

        '''
        > Running sum and sum-of-squares
        > _run_sum = the sum of the everything in the buffer
        > _run_sumsq = the sum of the squares of everything in the buffer
        '''
        self._run_sum = {}
        self._run_sumsq = {}

        # Hard limits for each sensor
        self.hard_bounds = {
            'temperature': (-40.0, 100.0),    # Celsius
            'humidity':    (0.0, 100.0),
            'pressure':    (300.0, 1200.0),
            'pm1_0':       (0.0, 10000.0),    # µg/m³
            'shuntVoltage': (-0.320, 0.320)   # INA219 MAX shunt voltage range (V)
        }

    # Helper function to prevent alert spam
    def _alert_cooldown(self, sensor_name: str, metric: str, alert_type: str, current_timestamp: float, cooldown_seconds=1800) -> bool:

        key = f"{sensor_name}_{metric}_{alert_type}"
        last_time = self._last_alert_time.get(key)
        
        # If an alert  was sent --> check the delta
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
            
                bufferLen = len(buffer)

                mean = self._run_sum[sensor_name][key] / bufferLen
                variance = max(self._run_sumsq[sensor_name][key] / bufferLen - mean * mean, 0.0)
                std = variance ** 0.5

                # Clamp std to a noise floor so Z-score is always evaluated even on flat baselines when std ≈ 0
                clamped_std = max(std, 1e-3)
                z_score = abs((value - mean) / clamped_std)

                # z-score > threshold --> outlier
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

        # Split data in half and check for avg shift and variance inflation 
        mid = len(data) // 2
        old_half = data[:mid]
        new_half = data[mid:]

        old_variance = np.var(old_half)
        new_variance = np.var(new_half)

        old_mean = np.mean(old_half)
        new_mean = np.mean(new_half)

        # Variance below this value is considered effectively flat, accounting for floating-point noise
        FLAT_VAR_THRESHOLD = 1e-12

        # Mean difference must exceed this value to count as real shift, not numerical noise
        MEAN_SHIFT_THRESHOLD = 0.01 

        old_flat = old_variance < FLAT_VAR_THRESHOLD
        new_flat = new_variance < FLAT_VAR_THRESHOLD
        mean_val_changed = abs(old_mean - new_mean) > MEAN_SHIFT_THRESHOLD


        # Handle step-changes early to avoid division errors
        if old_flat and new_flat:

            if mean_val_changed:

                if self._alert_cooldown(sensor_name, metric, "drift", current_timestamp):

                    _publish_alert(sensor_name, {
                        "alert": "Sensor Drift Detected Step-Change",
                        "metric": metric,
                        "old_mean": round(old_mean, 3),
                        "new_mean": round(new_mean, 3)
                    }, data_time_str)

            return

        # Handle half-flat windows: one half is constant while the other has variance
        # Publish a variance regime change alert
        if old_flat != new_flat:

            if self._alert_cooldown(sensor_name, metric, "variance-regime", current_timestamp):

                _publish_alert(sensor_name, {
                    "alert": "Variance Regime Change",
                    "metric": metric,
                    "detail": "One half is flat while the other has variance",
                    "old_mean": round(old_mean, 3),
                    "new_mean": round(new_mean, 3),
                    "old_variance": round(float(old_variance), 6),
                    "new_variance": round(float(new_variance), 6)
                }, data_time_str)
        
        # Welch T-Test: Detects a shift in the mean (average value)
        _, p_welch = stats.ttest_ind(old_half, new_half, equal_var=False)

        # Levene F-Test: Detects a shift in variance (noise levels)
        _, p_levene = stats.levene(old_half, new_half, center="mean")

        # Handle NaNs and Floor small p-values for reporting
        p_welch  = 1.0 if np.isnan(p_welch)  else max(p_welch,  1e-15)
        p_levene = 1.0 if np.isnan(p_levene) else max(p_levene, 1e-15)


        ''' 
        > Use independent checks instead of combined 
        > If either test falls below alpha, there is most likely notable drift
        '''

        if p_welch < self.p_alpha:
            mean_shift_detected = True

        else:
            mean_shift_detected = False


        if p_levene < self.p_alpha:
            variance_shift_detected = True

        else:
            variance_shift_detected = False

        # Send alert if any shift was detected
        if mean_shift_detected or variance_shift_detected:
            
            # Use the cooldown helper to avoid spamming requests
            if not self._alert_cooldown(sensor_name, metric, "drift", current_timestamp):
                return
 
            _publish_alert(sensor_name, {
                "alert": "Sensor Drift Detected", 
                "metric": metric,
                "mean_shift": mean_shift_detected,
                "variance_shift": variance_shift_detected,
                "p_welch": format(p_welch, ".2e"), 
                "p_levene": format(p_levene, ".2e")
            }, data_time_str)


drift_engine = SensorDrift() 

if __name__ == "__main__":

    # Get the directory where the script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Path to valo data relative to this script
    data_file = os.path.join(script_dir, 'data', 'valo_node_01_full_year.csv')

    print(f"Current Working Directory: {os.getcwd()}")
    print(f"Resolved Data File Path: {data_file}")

    # Run the parse function
    parse_and_process_valo_data(data_file)
