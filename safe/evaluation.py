"""Replay, explicit alert attribution, event scoring, and reproducible reports.

No detector settings or production alert payloads are modified by this module.
See docs/evaluation.md for scoring units and the limitations of attribution.
"""

from dataclasses import asdict
import json
from pathlib import Path
import subprocess
import sys

import numpy as np
import pandas as pd
import scipy

from safe import __version__, config
from safe import engine as engine_module
from safe.engine import SensorDrift
from safe.scenarios import Scenario


ALERT_CATEGORIES = {
    "hard-bounds-violation": ("physical_bounds",),
    "z-score-outlier": ("spike", "level_offset", "calibration_drift", "noise_increase"),
    "Step-Change Detected": ("level_offset", "calibration_drift"),
    "Sensor Drift Detected Step-Change": ("level_offset", "calibration_drift"),
    "Sustained Mean Shift (Page-Hinkley)": ("level_offset", "calibration_drift"),
    "Variance Regime Change": ("freeze", "noise_increase", "sensitivity_loss"),
    "Sensor Drift Detected": ("level_offset", "calibration_drift", "freeze",
                              "noise_increase", "sensitivity_loss"),
}


def qualifying_categories(payload):
    categories = set(ALERT_CATEGORIES.get(payload["alert"], ()))
    if payload["alert"] == "Sensor Drift Detected":
        categories = set()
        if payload.get("mean_shift"):
            categories.update(("level_offset", "calibration_drift"))
        if payload.get("variance_shift"):
            categories.update(("freeze", "noise_increase", "sensitivity_loss"))
    return categories


def validate_scenario(scenario):
    if not np.isfinite([scenario.start, scenario.end]).all() or scenario.end <= scenario.start:
        raise ValueError("scenario must have a finite positive observation interval")
    identities = set(scenario.identities)
    if not identities or len(identities) != len(scenario.identities):
        raise ValueError("identities must be nonempty and unique")
    if any(not sensor or not metric for sensor, metric in identities):
        raise ValueError("sensor and metric identities must be nonempty")
    if len({f.id for f in scenario.faults}) != len(scenario.faults):
        raise ValueError("fault IDs must be unique within a scenario")
    for fault in scenario.faults:
        if ((fault.sensor, fault.metric) not in identities
                or not scenario.start <= fault.start < fault.end <= scenario.end):
            raise ValueError("fault identity or interval is outside scenario")
    for reading in scenario.readings:
        if not scenario.start <= reading.timestamp < scenario.end:
            raise ValueError("reading timestamp outside scenario")
        if not reading.values or any((reading.sensor, m) not in identities for m in reading.values):
            raise ValueError("reading identity outside scenario")
        if not np.isfinite(list(reading.values.values())).all():
            raise ValueError("readings must be finite; represent gaps by absent readings")


def replay(scenario: Scenario, engine_config=None):
    """Preserve arrival order and gaps; retain alerts across explicit restarts.

    Only explicitly expected out-of-order rejections are continued; every other
    ingestion exception fails the evaluation instead of silently truncating it.
    """
    validate_scenario(scenario)
    options = dict(engine_config or {})
    if "on_alert" in options:
        raise ValueError("evaluation owns the alert collector")
    alerts, errors = [], []
    current = None

    def collect(sensor, payload, data_time):
        alerts.append({"sensor": sensor, "metric": payload["metric"],
                       "timestamp": current.timestamp, "alert_type": payload["alert"],
                       "payload": dict(payload)})

    engine = SensorDrift(on_alert=collect, **options)
    settings = {name: getattr(engine, name) for name in (
        "window_size", "z_threshold", "p_alpha", "cooldown_seconds",
        "enable_page_hinkley", "autocorr_correction")}
    for index, current in enumerate(scenario.readings):
        if current.restart:
            engine = SensorDrift(on_alert=collect, **options)
        try:
            engine.data_processing(current.sensor, {
                **current.values, "unix_timestamp": current.timestamp,
                "str_timestamp": pd.Timestamp(current.timestamp, unit="s", tz="UTC").isoformat(),
            })
        except ValueError as exc:
            if not current.expected_rejection or "out-of-order reading" not in str(exc):
                raise
            errors.append({"reading_index": index, "timestamp": current.timestamp,
                           "sensor": current.sensor, "error": str(exc)})
        else:
            if current.expected_rejection:
                raise AssertionError("expected timestamp rejection did not occur")
    return alerts, errors, settings


def score(scenario, alerts, grace_seconds=0.0):
    """One alert belongs to at most one event, earliest (start, id) wins ties.

    Match [fault.start, fault.end + grace), clipped to scenario.end, with exact
    sensor/metric identity and qualifying category. Repeats count as attributed
    alerts but never additional true-positive events. Unknown types stay FP.
    """
    validate_scenario(scenario)
    if not np.isfinite(grace_seconds) or grace_seconds < 0:
        raise ValueError("grace_seconds must be finite and nonnegative")
    events = [{**asdict(f), "alert_count": 0, "detection_delay_seconds": None}
              for f in sorted(scenario.faults, key=lambda f: (f.start, f.id))]
    scored = []
    for alert in alerts:
        if ((alert["sensor"], alert["metric"]) not in scenario.identities
                or not scenario.start <= alert["timestamp"] < scenario.end):
            raise ValueError("alert identity or timestamp outside scenario")
        match = next((e for e in events if
                      (e["sensor"], e["metric"]) == (alert["sensor"], alert["metric"])
                      and e["category"] in qualifying_categories(alert["payload"])
                      and e["start"] <= alert["timestamp"] < min(scenario.end, e["end"] + grace_seconds)), None)
        scored.append({**alert, "fault_id": match["id"] if match else None,
                       "fault_category": match["category"] if match else "unmatched"})
        if match is not None:
            match["alert_count"] += 1
            delay = alert["timestamp"] - match["start"]
            old = match["detection_delay_seconds"]
            match["detection_delay_seconds"] = delay if old is None else min(old, delay)
    return scored, events


def summarize(alerts, events, sensor_days):
    tp = sum(e["alert_count"] > 0 for e in events)
    fp = sum(a["fault_id"] is None for a in alerts)
    fn = len(events) - tp
    delays = [e["detection_delay_seconds"] for e in events if e["alert_count"]]
    return {
        "reading_level_alerts": len(alerts), "matched_alerts": len(alerts) - fp,
        "true_positive_events": tp, "false_positive_alerts": fp,
        "false_negative_events": fn, "expected_faults": len(events),
        "precision": tp / (tp + fp) if tp + fp else None,
        "recall": tp / len(events) if events else None,
        "alert_precision": (len(alerts) - fp) / len(alerts) if alerts else None,
        "mean_detection_delay_seconds": float(np.mean(delays)) if delays else None,
        "alerts_per_expected_fault": len(alerts) / len(events) if events else None,
        "matched_alerts_per_expected_fault": (len(alerts) - fp) / len(events) if events else None,
        "sensor_days": sensor_days, "alerts_per_sensor_day": len(alerts) / sensor_days,
        "false_alerts_per_sensor_day": fp / sensor_days,
    }


def evaluate(scenarios, engine_config=None, grace_seconds=0.0, seed=None):
    scenarios = list(scenarios)
    if not scenarios or len({s.name for s in scenarios}) != len(scenarios):
        raise ValueError("provide scenarios with unique names")
    results, all_alerts, all_events = [], [], []
    for scenario in scenarios:
        raw, errors, settings = replay(scenario, engine_config)
        alerts, events = score(scenario, raw, grace_seconds)
        days = (scenario.end - scenario.start) / 86400
        exposure = days * len({s for s, _ in scenario.identities})
        metrics = summarize(alerts, events, exposure)
        results.append({"name": scenario.name, "description": scenario.description,
                        "seed": scenario.seed, "start": scenario.start, "end": scenario.end,
                        "duration_seconds": scenario.end - scenario.start,
                        "sampling_interval_seconds": scenario.sampling_interval_seconds,
                        "parameters": scenario.parameters, "identities": scenario.identities,
                        "reading_count": len(scenario.readings), "ingestion_errors": errors,
                        "restart_count": sum(r.restart for r in scenario.readings),
                        "metrics": metrics, "alerts": alerts, "faults": events})
        all_alerts.extend({**a, "scenario": scenario.name} for a in alerts)
        all_events.extend({**e, "scenario": scenario.name} for e in events)
    grouped = {}
    for dimension, key in (("metric", "metric"), ("fault_category", "category"),
                           ("alert_type", "alert_type")):
        groups = {}
        if dimension == "metric":
            values = sorted({m for s in scenarios for _, m in s.identities})
        elif dimension == "fault_category":
            values = sorted({e["category"] for e in all_events} | {"unmatched"})
        else:
            values = sorted(set(ALERT_CATEGORIES) | {a[key] for a in all_alerts})
        for value in values:
            if dimension == "metric":
                aa = [a for a in all_alerts if a[key] == value]
                ee = [e for e in all_events if e[key] == value]
                exposure = sum((s.end - s.start) / 86400 * len({sensor for sensor, m in s.identities if m == value}) for s in scenarios)
            else:
                aa = [a for a in all_alerts if a["fault_category" if dimension == "fault_category" else key] == value]
                ee = [dict(e) for e in all_events if (e["category"] == value if dimension == "fault_category" else e["category"] in ALERT_CATEGORIES.get(value, ()))]
                exposure = sum(r["metrics"]["sensor_days"] for r in results)
                if dimension == "alert_type":
                    for e in ee:
                        hits = [a for a in aa if (a["scenario"], a["fault_id"]) == (e["scenario"], e["id"])]
                        e["alert_count"] = len(hits)
                        e["detection_delay_seconds"] = min((a["timestamp"] - e["start"] for a in hits), default=None)
            groups[value] = summarize(aa, ee, exposure)
        grouped[dimension] = groups
    root = Path(__file__).resolve().parents[1]
    try:
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True, stderr=subprocess.DEVNULL).strip()
        dirty = bool(subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=no"], cwd=root, text=True, stderr=subprocess.DEVNULL).strip())
    except (OSError, subprocess.CalledProcessError):
        commit, dirty = None, None
    constants = {name: getattr(config, name) for name in (
        "HARD_BOUNDS", "MIN_COHENS_D", "MIN_STD_RATIO", "FLAT_MEAN_SHIFT_THRESHOLDS",
        "DEFAULT_FLAT_MEAN_SHIFT", "FLAT_VAR_THRESHOLD")}
    constants.update({name: getattr(engine_module, name) for name in (
        "MIN_HISTORY", "STEP_CHANGE_RUN", "BASELINE_REFRESH_EVERY", "MAD_TO_SIGMA")})
    return {"schema_version": 1, "metadata": {
        "safe_version": __version__, "git_commit": commit, "tracked_worktree_dirty": dirty,
        "seed": seed, "engine_configuration": settings, "constants": constants,
        "page_hinkley_defaults": {"delta": engine_module.PageHinkley().delta, "lam": engine_module.PageHinkley().lam},
        "versions": {"python": sys.version.split()[0], "numpy": np.__version__, "pandas": pd.__version__, "scipy": scipy.__version__},
        "grace_seconds": grace_seconds, "alert_category_mapping": ALERT_CATEGORIES,
        "matching_policy": "exact identity; [start, end+grace), clipped to observation; earliest start/id; drift flags required",
    }, "overall": summarize(all_alerts, all_events, sum(r["metrics"]["sensor_days"] for r in results)),
        "by_scenario": {r["name"]: r["metrics"] for r in results},
        "by_metric": grouped["metric"], "by_fault_category": grouped["fault_category"],
        "by_alert_type": grouped["alert_type"], "scenarios": results}


def write_reports(report, output):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    (output / "evaluation.json").write_text(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    lines = ["# SAFE synthetic baseline", "",
             "Synthetic operating characteristics only; no field-accuracy or readiness claim.", "",
             f"SAFE {report['metadata']['safe_version']}; Git `{report['metadata']['git_commit']}`; seed {report['metadata']['seed']}.",
             "Engine configuration: `" + json.dumps(report['metadata']['engine_configuration'], sort_keys=True) + "`.", "",
             "TP/FN count fault events; FP counts unmatched emitted alerts. Repeats never increase TP.",
             "Precision = TP/(TP+FP) mixes event and alert units; alert_precision is separately in JSON.",
             "Undefined ratios are null. Delay is seconds from onset to first qualifying alert, for detected events only.",
             "Exposure uses elapsed observation time, including gaps. Each fixture below lasts 7 days at nominal 300-second sampling." if all(s["duration_seconds"] == 604800 and s["sampling_interval_seconds"] == 300 for s in report["scenarios"]) else "See JSON for per-scenario duration and sampling interval.",
             "", "| Scenario | Alerts | TP | FP | FN | FP/sensor-day | Mean delay (s) |", "|---|---:|---:|---:|---:|---:|---:|"]
    for row in report["scenarios"]:
        m = row["metrics"]
        delay = m["mean_detection_delay_seconds"]
        lines.append(f"| {row['name']} | {m['reading_level_alerts']} | {m['true_positive_events']} | {m['false_positive_alerts']} | {m['false_negative_events']} | {m['false_alerts_per_sensor_day']:.3f} | {delay if delay is not None else '—'} |")
    overall = report["overall"]
    lines += ["", "Overall: " + "; ".join(
        f"{key}={overall[key]}" for key in (
            "true_positive_events", "false_positive_alerts", "false_negative_events",
            "precision", "recall", "alerts_per_expected_fault", "alerts_per_sensor_day")) + "."]
    lines += ["", "Timestamp rejections are ingestion diagnostics, not sensor-fault alerts. Missing-data and timestamp-order labels have no qualifying alert type.",
              "Alert attribution is a scoring convention, not causal confirmation. See docs/evaluation.md; JSON contains raw payloads, per-fault delays/counts, mapping, constants, and all breakdowns."]
    (output / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
