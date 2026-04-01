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
 
import yaml
import paho.mqtt.client as mqttClient
 
from mintsXU4 import mintsDefinitions  as mD
from mintsXU4 import mintsSensorReader as mSR
 
# Re-use the existing writer infrastructure from mintsSensorReader
# sensorFinisher(dateTime, sensorName, sensorDictionary) writes CSV + MQTT + latest JSON
from mintsXU4.mintsSensorReader import sensorFinisher
 
warnings.filterwarnings("ignore", category=RuntimeWarning)
 
 
# ---------------------------------------------------------------------------
# MQTT alert publisher
# Mirrors the connect / publish pattern in mintsLatest.py but publishes to
# a dedicated drift-alert topic so the lab can subscribe separately:
#   <macAddress>/driftAlerts/<sensorName>
# ---------------------------------------------------------------------------
 
_mqtt_connected = False
_mqtt_client    = mqttClient.Client()
 
def _on_connect(client, userdata, flags, rc):
    global _mqtt_connected
    if rc == 0:
        print("[mintsDriftAnalysis] MQTT connected")
        _mqtt_connected = True
    else:
        print(f"[mintsDriftAnalysis] MQTT connection failed (rc={rc})")
 
def _on_publish(client, userdata, result):
    print("[mintsDriftAnalysis] MQTT alert published")
 
def _mqtt_connect() -> bool:
    """
    Lazy-connect using the same credentials / broker / TLS settings
    defined in mintsDefinitions (credentials.yml, mqtt.circ.utdallas.edu:8883).
    """
    global _mqtt_connected, _mqtt_client
    try:
        if _mqtt_client.is_connected():
            return True
 
        credentials = yaml.safe_load(open(mD.mqttCredentialsFile))
        mqttUN  = credentials['mqtt']['username']
        mqttPW  = credentials['mqtt']['password']
        tlsCert = "/etc/ssl/certs/ca-certificates.crt"
 
        _mqtt_client.username_pw_set(mqttUN, password=mqttPW)
        _mqtt_client.on_connect = _on_connect
        _mqtt_client.on_publish = _on_publish
        _mqtt_client.tls_set(
            ca_certs=tlsCert, certfile=None, keyfile=None,
            cert_reqs=ssl.CERT_REQUIRED,
            tls_version=ssl.PROTOCOL_TLSv1_2,
            ciphers=None,
        )
        _mqtt_client.tls_insecure_set(False)
        _mqtt_client.connect(mD.mqttBroker, port=mD.mqttPort)
        _mqtt_client.loop_start()
 
        attempts = 0
        while not _mqtt_connected and attempts < 5:
            print("[mintsDriftAnalysis] Waiting for MQTT connection...")
            time.sleep(1)
            attempts += 1
 
        if not _mqtt_connected:
            print("[mintsDriftAnalysis] ERROR: Could not connect to MQTT broker")
            return False
 
        return True
 
    except Exception:
        traceback.print_exc()
        return False
 
 
def _publish_alert(sensor_name: str, alert_dict: dict) -> None:
    """
    Publish a JSON alert to:
        <macAddress>/driftAlerts/<sensorName>
 
    This fires immediately (per-reading for outliers, per-window for drift)
    so lab dashboards / Node-RED / InfluxDB subscriptions get real-time alerts.
    """
    if not mD.mqttOn:
        return
    try:
        if _mqtt_connect():
            topic   = f"{mD.macAddress}/driftAlerts/{sensor_name}"
            payload = json.dumps(alert_dict)
            _mqtt_client.publish(topic, payload)
            print(f"[mintsDriftAnalysis] Alert → {topic}")
    except Exception:
        print("[mintsDriftAnalysis] MQTT publish failed")
        traceback.print_exc()