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

import time
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

# Used AI for parse_and_process function

def parse_and_process_valo_data(file_path):
   
    if not os.path.exists(file_path):

        print(f"Data file not found: {file_path}")
        return

    print(f"Reading data from {file_path}...")
    
    try:
        # InfluxDB exports have 3 metadata lines: #group, #datatype, #default
        # Row 4 (index 3) contains the actual column names (_time, _value, etc.)
        df = pd.read_excel(file_path, skiprows=3)

        # In case the format varies, let's ensure we have the right columns
        required_cols = ['_time', '_value', '_field', '_measurement', 'device_id']
        missing = [col for col in required_cols if col not in df.columns]
        
        if missing:
            print(f"Error: Missing expected columns: {missing}")
            print(f"Detected columns were: {df.columns.tolist()}")
            return

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

        # Pre-calculate sensor names vectorially
        pivot_df['_sensor_name'] = pivot_df['_measurement'] + '_' + pivot_df['device_id'].astype(str)

        # Identify metric columns (everything that isn't metadata)
        meta_cols = ['_time', '_measurement', 'device_id', '_unix_time', '_str_time', '_sensor_name']
        metric_cols = [col for col in pivot_df.columns if col not in meta_cols]

        # Convert to list of dicts --> wayy faster than iterrows
        records = pivot_df[['_sensor_name', '_unix_time', '_str_time'] + metric_cols].to_dict(orient='records')

        print(f"Processing {len(records)} data points...")

        for record in records:

            sensor_name = record.pop('_sensor_name')
            record['unix_timestamp'] = record.pop('_unix_time')
            record['str_timestamp'] = record.pop('_str_time')

            try:

                drift_engine.data_processing(sensor_name, record)

            except Exception as row_err:

                print(f"Error processing row: {row_err}")
                traceback.print_exc()

            
        print("Data processing complete.")

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
        
        # Dictionary to store the last time an alert was sent
        self._last_alert_time = {}

        # Dictionary of deques to store recent values
        self.history = {}

        # Dictionary to track samples since last evaluation for non-overlapping windows
        self.eval_counters = {}

        # Dictionary to track consecutive outlier values per metric (for step-change detection)
        # Stores the actual values (not just a count) so we can seed the buffer on step-change
        self._consecutive_outliers = {}

        # Hard limits for each sensor 
        self.hard_bounds = {
            'temperature': (-40.0, 100.0),  # Celsius
            'humidity':    (0.0, 100.0),       
            'pressure':    (300.0, 1200.0),   
            'pm2_5':       (0.0, 10000.0),         
            'pm10':        (0.0, 10000.0),
            'shuntVoltage': (-0.320, 0.320) # INA219 MAX shunt voltage range (V)     
        }

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
                        "value": value, 
                        "bounds": hard_bounds
                    }, data_time_str)

                # Don't add this value to history
                continue

            # Check if we have a deque for this key to store recent vals
            if key not in self.history[sensor_name]:
                self.history[sensor_name][key] = deque(maxlen=self.window_size)
                self.eval_counters[sensor_name][key] = 0
                self._consecutive_outliers[sensor_name][key] = deque(maxlen=10)
        
            # Add new value to the history buffer
            buffer = self.history[sensor_name][key]
        
            # Z-score outlier detection, only run with 30+ values
            if len(buffer) >= 30:
                arr = np.array(buffer)

                mean = np.mean(arr)
                std = np.std(arr)

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
                                "value": value,
                                "consecutive_outliers": len(saved_values),
                                "old_mean": round(mean, 3)
                            }, data_time_str)

                        # Flush buffer and seed it with the saved outlier values
                        # so the new baseline starts tracking immediately
                        buffer.clear()
                        buffer.extend(saved_values)
                        self.eval_counters[sensor_name][key] = len(saved_values)
                        self._consecutive_outliers[sensor_name][key].clear()
                    else:
                        # Regular single outlier alert (with cooldown)
                        if self._alert_cooldown(sensor_name, key, "z-score", current_timestamp):
                            _publish_alert(sensor_name, {
                                "alert": "z-score-outlier",
                                "metric": key,
                                "value": value,
                                "z_score": round(z_score, 3)
                            }, data_time_str)

                    # Don't append outliers to the buffer and don't increment the eval counter
                    continue

            # Value passed all checks — reset consecutive outlier deque and append to buffer
            self._consecutive_outliers[sensor_name][key].clear()
            buffer.append(value)

            # Increment eval counter only for values that actually entered the buffer (Bug 1 fix)
            self.eval_counters[sensor_name][key] += 1

            # Run drift evaluation once we have processed window_size new samples
            if self.eval_counters[sensor_name][key] >= self.window_size:
                # Buffer should be full since counter is now synced with appends
                if len(buffer) == self.window_size:
                    self._evaluate_drift(sensor_name, key, list(buffer), current_timestamp, data_time_str)
                
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


        # Handle flat-line transitions (step-changes) early to avoid SciPy division errors
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

        # Handle half-flat windows: one half is constant while the other has variance.
        # Publish a variance regime change alert, but do NOT return early —
        # SciPy's ttest_ind and levene can handle one-side-zero-variance safely,
        # and we still need to check for mean shifts (e.g., flatline at 10 → noisy around 100).
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
    data_file = os.path.join(script_dir, 'data', 'valo_node_01_full_year.xlsm')

    print(f"Current Working Directory: {os.getcwd()}")
    print(f"Resolved Data File Path: {data_file}")

    # Run the parse function
    parse_and_process_valo_data(data_file)
