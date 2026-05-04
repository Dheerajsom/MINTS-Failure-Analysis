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


from mintsXU4 import mintsLatest as mL

warnings.filterwarnings("ignore", category=RuntimeWarning)

# --------------------------------------------------
# MQTT Alert Publishing (we will utilize this later)
# --------------------------------------------------

def _publish_alert(sensor_name: str, alert_dict: dict) -> None:

    try:
        mL.writeMQTTLatest(alert_dict, sensor_name)

    except Exception:
        print(f"[mintsFailureAnalysis] MQTT publish failed for {sensor_name}")
        traceback.print_exc()


# -----------------------------------
# Unpacking & Parsing valo node data
# -----------------------------------





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
    def _alert_cooldown(self, sensor_name: str, metric: str, alert_type: str, cooldown_seconds=600) -> bool:

        key = f"{sensor_name}_{metric}_{alert_type}"
        current_time = time.time()
        
        if current_time - self._last_alert_time.get(key, 0) < cooldown_seconds:
            return False
            
        self._last_alert_time[key] = current_time
        return True

    # Update history with new sensor data, ensuring we maintain a fixed window size
    def data_processing(self, sensor_name: str, sensor_dict: dict):

        # Initialize history for this sensor if not present
        if sensor_name not in self.history:     
            self.history[sensor_name] = {}

        # Process key-value pairs in the sensor dict --> skip non-numeric values and "dateTime"
        for key, val in sensor_dict.items():

            if key == "dateTime":
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
                if self._alert_cooldown(sensor_name, key, "hard-bounds"):

                    _publish_alert(sensor_name, {
                        "alert": "hard-bounds-violation",
                        "metric": key, 
                        "value": value, 
                        "bounds": hard_bounds
                    })

                # Don't add this value to history
                continue

            # Check if we have a deque for this key to store recent vals
            if key not in self.history[sensor_name]:
                self.history[sensor_name][key] = deque(maxlen=self.window_size)
        
            # Add new value to the history buffer
            buffer = self.history[sensor_name][key]
        
            # Z-score outlier detection, only run with 30+ values to have a stable mean/std
            if len(buffer) >= 30:
                arr = np.array(buffer)

                mean = np.mean(arr)
                std = np.std(arr)

                if std > 0:
                    z_score = abs((value - mean) / std)

                    # If z-score > threshold --> publish an alert with details
                    if z_score > self.z_threshold:
                        if self._alert_cooldown(sensor_name, key, "z-score"):
                            _publish_alert(sensor_name, {
                                "alert": "z-score-outlier",
                                "metric": key,
                                "value": value,
                                "z_score": round(z_score, 3)
                            })
                        
                        continue

            # Add current value to history buffer before running drift evaluation
            buffer.append(value)

            # Run drift evaluation once the buffer is full
            if len(buffer) >= self.window_size:
                self._evaluate_drift(sensor_name, key, list(buffer))
                
    
    def _evaluate_drift(self, sensor_name: str, metric: str, data: list):

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


        # SKIPPING all flat windows even when the flat value changed --> wrong logic below need to fix
        '''
        if np.var(old_half) == 0 and np.var(new_half) == 0:
            return 
        '''
        
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
            if not self._alert_cooldown(sensor_name, metric, "drift"):
                return
 
            _publish_alert(sensor_name, {
                "alert": "Sensor Drift Detected", 
                "metric": metric,
                "mean_shift": mean_shift_detected,
                "variance_shift": variance_shift_detected,
                "p_welch": format(p_welch, ".2e"), 
                "p_levene": format(p_levene, ".2e")
            })

drift_engine = SensorDrift() 