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

from mintsXU4 import mintsDefinitions  as mD
from mintsXU4 import mintsSensorReader as mSR
from mintsXU4 import mintsLatest       as mL
from mintsXU4.mintsSensorReader import sensorFinisher

warnings.filterwarnings("ignore", category=RuntimeWarning)


def _publish_alert(sensor_name: str, alert_dict: dict) -> None:
    """
    Publish a drift/outlier alert via the existing MINTS MQTT pipeline.
    Topic: <macAddress>/<sensor_name>
    """
    if not mD.mqttOn:
        return
    try:
        # Handles the connection, TLS, credentials, and publishes to <macAddress> / <sensorName>
        mL.writeMQTTLatest(alert_dict, sensor_name)
    except Exception:
        print(f"[mintsFailureAnalysis] MQTT publish failed for {sensor_name}")
        traceback.print_exc()