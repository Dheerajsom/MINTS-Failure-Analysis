from dataclasses import replace
import json
from pathlib import Path
import subprocess
import sys

import pytest

from safe.evaluation import evaluate, replay, score, summarize, write_reports
from safe.scenarios import Fault, Reading, Scenario


def fixture():
    return Scenario("labeled", (), (Fault("f", "s", "temperature", "level_offset", 10, 20),),
                    0, 86400, (("s", "temperature"), ("other", "temperature")))


def alert(time=10, kind="z-score-outlier", sensor="s", metric="temperature", **flags):
    return {"timestamp": time, "sensor": sensor, "metric": metric, "alert_type": kind,
            "payload": {"alert": kind, "metric": metric, **flags}}


def test_repeats_are_not_extra_true_positives():
    aa, ee = score(fixture(), [alert(10), alert(12), alert(9), alert(20)])
    m = summarize(aa, ee, 2)
    assert (m["true_positive_events"], m["false_positive_alerts"], m["false_negative_events"]) == (1, 2, 0)
    assert m["precision"] == pytest.approx(1 / 3)
    assert m["recall"] == 1
    assert m["alert_precision"] == 0.5
    assert m["alerts_per_expected_fault"] == 4
    assert m["alerts_per_sensor_day"] == 2
    assert ee[0]["alert_count"] == 2
    assert ee[0]["detection_delay_seconds"] == 0


@pytest.mark.parametrize("candidate", [alert(sensor="other"), alert(kind="unknown"),
    alert(kind="hard-bounds-violation"), alert(kind="Sensor Drift Detected", variance_shift=True)])
def test_identity_and_category_must_qualify(candidate):
    aa, ee = score(fixture(), [candidate])
    assert aa[0]["fault_id"] is None
    assert ee[0]["alert_count"] == 0


def test_metric_identity_and_drift_flags():
    s = replace(fixture(), identities=fixture().identities + (("s", "humidity"),))
    aa, ee = score(s, [alert(metric="humidity"), alert(13, "Sensor Drift Detected", mean_shift=True)])
    assert aa[0]["fault_id"] is None
    assert aa[1]["fault_id"] == "f"
    assert ee[0]["detection_delay_seconds"] == 3


def test_grace_and_overlap_have_deterministic_single_attribution():
    s = fixture()
    s = replace(s, faults=s.faults + (replace(s.faults[0], id="g"),))
    aa, ee = score(s, [alert(20), alert(21)], grace_seconds=1)
    assert [a["fault_id"] for a in aa] == ["f", None]
    assert [e["alert_count"] for e in ee] == [1, 0]


def test_gap_exposure_includes_elapsed_time_and_multiple_metrics_once():
    s = Scenario("gap", (Reading(0, "s", {"temperature": 20, "humidity": 50}),), (),
                 0, 86400, (("s", "temperature"), ("s", "humidity"), ("other", "temperature")))
    report = evaluate([s])
    assert report["overall"]["sensor_days"] == 2
    assert report["by_metric"]["temperature"]["sensor_days"] == 2
    assert report["by_metric"]["humidity"]["sensor_days"] == 1
    assert report["overall"]["precision"] is None
    assert report["overall"]["recall"] is None
    assert report["scenarios"][0]["reading_count"] == 1


def test_restart_retains_raw_alerts_and_resets_cooldown():
    s = replace(fixture(), readings=(Reading(1, "s", {"temperature": 150}),
                                    Reading(2, "s", {"temperature": 150}, restart=True)))
    aa, errors, settings = replay(s)
    assert len(aa) == 2
    assert not errors
    assert settings["cooldown_seconds"] == 1800


def test_expected_rejection_is_diagnostic_and_unexpected_errors_propagate():
    readings = (Reading(2, "s", {"temperature": 20}),
                Reading(1, "s", {"temperature": 20}, expected_rejection=True),
                Reading(3, "s", {"temperature": 150}))
    s = replace(fixture(), readings=readings)
    aa, errors, _ = replay(s)
    assert len(errors) == 1 and len(aa) == 1
    with pytest.raises(ValueError, match="out-of-order"):
        replay(replace(s, readings=(readings[0], replace(readings[1], expected_rejection=False))))
    with pytest.raises(AssertionError, match="expected timestamp"):
        replay(replace(s, readings=(replace(readings[0], expected_rejection=True),)))


@pytest.mark.parametrize("change", [dict(end=0), dict(identities=()),
    dict(faults=(Fault("f", "s", "temperature", "freeze", 20, 10),)),
    dict(readings=(Reading(5, "s", {"temperature": float("nan")}),))])
def test_invalid_labels_or_inputs_fail(change):
    with pytest.raises(ValueError):
        replay(replace(fixture(), **change))


def test_negative_grace_fails():
    with pytest.raises(ValueError):
        score(fixture(), [], -1)


def test_type_breakdowns_recompute_first_detection(tmp_path):
    s = replace(fixture(), faults=(Fault("bound", "s", "temperature", "physical_bounds", 10, 20),),
                readings=(Reading(12, "s", {"temperature": 150}),))
    report = evaluate([s])
    assert report["by_alert_type"]["hard-bounds-violation"]["mean_detection_delay_seconds"] == 2
    assert report["by_fault_category"]["physical_bounds"]["true_positive_events"] == 1
    write_reports(report, tmp_path)
    assert json.loads((tmp_path / "evaluation.json").read_text())["overall"] == report["overall"]
    assert "Synthetic" in (tmp_path / "summary.md").read_text()


def test_cli_reproducibility_and_failure_exit(tmp_path):
    script = Path(__file__).resolve().parents[1] / "scripts/evaluate_engine.py"
    for name in ("a", "b"):
        subprocess.run([sys.executable, str(script), "--seed", "1729", "--output", str(tmp_path / name)], check=True)
    assert (tmp_path / "a/evaluation.json").read_bytes() == (tmp_path / "b/evaluation.json").read_bytes()
    proc = subprocess.run([sys.executable, str(script), "--grace-seconds", "-1", "--output", str(tmp_path / "bad")], capture_output=True)
    assert proc.returncode != 0
    assert not (tmp_path / "bad/evaluation.json").exists()
