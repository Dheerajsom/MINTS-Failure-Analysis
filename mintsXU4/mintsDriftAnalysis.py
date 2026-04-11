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

import datetime
import numpy as np
from scipy import stats
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from typing import Optional
import warnings
import traceback
import json
import ssl
import time


from mintsXU4 import mintsLatest as mL
from mintsXU4.mintsSensorReader import sensorFinisher

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

        # Dictionary of deques to store recent values
        self.history = {}

        # Hard limits for each sensor 
        self.hard_bounds = {
            'temperature': (-40.0, 100.0),  # Celsius
            'humidity':    (0.0, 100.0),       
            'pressure':    (300.0, 1100.0),   
            'pm2_5':       (0.0, 1000.0),         
            'pm10':        (0.0, 1000.0),
            'shuntVoltage': (-320.0, 320.0) # mV     
        }

    # Update history with new sensor data, ensuring we maintain a fixed window size
    def data_reading(self, sensor_name: str, sensor_dict: dict):

        # Initialize history for this sensor if not present
        if sensor_name not in self.history:     
            self.history[sensor_name] = {}


'''

def sensor_drift_testing(baseline_data: list, recent_data:   list, p_threshold:   float = 0.05) -> Optional[dict]:
 
    if len(baseline_data) < 2 or len(recent_data) < 2:
        return None
 
    # T-test => Detects mean shifts
    t_stat, t_pvalue = stats.ttest_ind(baseline_data, recent_data, equal_var=False)
 
    # F-test => Detects variance inflation
    f_stat, f_pvalue = stats.levene(baseline_data, recent_data, center="mean")
 
    # In case of NaN p-values (like due to zero variance), set them to 1.0 (no drift)
    t_pvalue = 1.0 if np.isnan(t_pvalue) else t_pvalue
    f_pvalue = 1.0 if np.isnan(f_pvalue) else f_pvalue
    t_stat   = 0.0 if np.isnan(t_stat)   else t_stat
    f_stat   = 0.0 if np.isnan(f_stat)   else f_stat
 
    # Fisher method => Combine p-values from T-test and F-test for overall drift significance
    t_p_safe = max(t_pvalue, 1e-10)
    f_p_safe = max(f_pvalue, 1e-10)

    # Use -, for trashing & unpacking the combined p-value, since we only care about the value itself for drift decision
    _, combined_pvalue = stats.combine_pvalues([t_p_safe, f_p_safe], method="fisher")
 
    # Compile results into a dictionary, including drift decision and effect size metrics
    return {
        "t_stat":          round(float(t_stat), 4),
        "t_pvalue":        round(float(t_pvalue), 6),
        "f_stat":          round(float(f_stat), 4),
        "f_pvalue":        round(float(f_pvalue), 6),
        "combined_pvalue": round(float(combined_pvalue), 6),
        "is_drifting":     bool(combined_pvalue < p_threshold),
        "mean_shift":      round(float(np.mean(recent_data) - np.mean(baseline_data)), 5),
        "variance_ratio":  round(float(np.var(recent_data) / max(np.var(baseline_data), 1e-10)), 4),
    }
 

# Test and report drift for a given sensor, comparing recent data to baseline, and publish alert if drifting
def test_and_report_drift(sensor_name: str, baseline_data: list, recent_data: list, p_threshold: float = 0.05) -> Optional[dict]:

    drift_stats = sensor_drift_testing(baseline_data, recent_data, p_threshold)
 
    if drift_stats and drift_stats["is_drifting"]:
        _publish_alert(f"{sensor_name}_DriftAlert", drift_stats)
 
    return drift_stats

'''