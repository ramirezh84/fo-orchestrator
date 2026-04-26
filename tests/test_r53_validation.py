#!/usr/bin/env python3
"""Tests for the R53 ↔ CW alarm wiring validator (Vigil v1.6 PR9 / F2).

Pure-function tests against ``detect_wiring_inconsistency`` in
``tools/validate_r53_alarm_wiring.py``. No AWS calls — every test
constructs the inputs that the standalone tool's AWS plumbing would have
fetched, then asserts the heuristic verdict.

Run: python3 -m pytest tests/test_r53_validation.py -v
"""

import os
import sys
from datetime import datetime, timedelta, timezone

import pytest


# Make tools/ importable.
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_TOOLS = os.path.join(_ROOT, "tools")
if _TOOLS not in sys.path:
    sys.path.insert(0, _TOOLS)

from validate_r53_alarm_wiring import (  # noqa: E402
    detect_wiring_inconsistency,
    _value_breaches,
)


_NOW = datetime.now(timezone.utc)
_MINUTE_AGO = _NOW - timedelta(minutes=1)
_TEN_MIN_AGO = _NOW - timedelta(minutes=10)


def _datapoint(value: float, age_seconds: int):
    return {
        "Timestamp": _NOW - timedelta(seconds=age_seconds),
        "Maximum": value,
        "Average": value,
    }


# ---------------------------------------------------------------------------
# The v1.5 drill's exact failure mode: alarm=ALARM but R53=Success.
# ---------------------------------------------------------------------------

def test_alarm_in_alarm_but_r53_says_success_flagged():
    problem = detect_wiring_inconsistency(
        alarm_state="ALARM",
        alarm_state_updated=_MINUTE_AGO,
        r53_status_text=(
            "Success: 2 datapoints were not less than the threshold (1.0), "
            "CW datapoints - requested: 4, received: 4, used: 2, breached: 0"
        ),
        r53_checked_time=_NOW,
        metric_datapoints=[_datapoint(0.0, 30), _datapoint(0.0, 90)],
        threshold=1.0,
        comparison_operator="LessThanThreshold",
    )
    assert problem is not None
    assert "Recreate the R53 health check" in problem


def test_alarm_in_alarm_with_r53_breached_zero_phrase_flagged():
    """Even if 'Success' isn't in the text, 'breached: 0' is the smoking gun."""
    problem = detect_wiring_inconsistency(
        alarm_state="ALARM",
        alarm_state_updated=_MINUTE_AGO,
        r53_status_text="breached: 0 — all datapoints inside evaluation period",
        r53_checked_time=_NOW,
        metric_datapoints=[_datapoint(0.0, 30)],
        threshold=1.0,
        comparison_operator="LessThanThreshold",
    )
    assert problem is not None
    assert "stale" in problem.lower()


# ---------------------------------------------------------------------------
# Inverse: alarm=OK but R53 reports Failure.
# ---------------------------------------------------------------------------

def test_alarm_ok_but_r53_says_failure_flagged():
    problem = detect_wiring_inconsistency(
        alarm_state="OK",
        alarm_state_updated=_MINUTE_AGO,
        r53_status_text="Failure: insufficient data points",
        r53_checked_time=_NOW,
        metric_datapoints=[_datapoint(1.0, 30)],
        threshold=1.0,
        comparison_operator="LessThanThreshold",
    )
    assert problem is not None
    assert "stale" in problem.lower()


# ---------------------------------------------------------------------------
# Healthy case: alarm and R53 agree.
# ---------------------------------------------------------------------------

def test_alarm_ok_and_r53_success_no_problem():
    problem = detect_wiring_inconsistency(
        alarm_state="OK",
        alarm_state_updated=_TEN_MIN_AGO,
        r53_status_text="Success: 2 datapoints were not less than the threshold (1.0)",
        r53_checked_time=_NOW,
        metric_datapoints=[_datapoint(1.0, 30)],
        threshold=1.0,
        comparison_operator="LessThanThreshold",
    )
    assert problem is None


def test_alarm_in_alarm_and_r53_failure_no_problem():
    problem = detect_wiring_inconsistency(
        alarm_state="ALARM",
        alarm_state_updated=_MINUTE_AGO,
        r53_status_text="Failure: 2 datapoints below threshold",
        r53_checked_time=_NOW,
        metric_datapoints=[_datapoint(0.0, 30)],
        threshold=1.0,
        comparison_operator="LessThanThreshold",
    )
    assert problem is None


# ---------------------------------------------------------------------------
# R53 hasn't seen the new alarm state yet (timestamp lag).
# ---------------------------------------------------------------------------

def test_r53_observation_older_than_alarm_transition_flagged():
    """If the alarm transitioned 5 min ago but R53 last checked 10 min ago,
    R53 hasn't picked up the new state yet."""
    five_min_ago = _NOW - timedelta(minutes=5)
    fifteen_min_ago = _NOW - timedelta(minutes=15)
    problem = detect_wiring_inconsistency(
        alarm_state="ALARM",
        alarm_state_updated=five_min_ago,
        r53_status_text="Failure: 2 datapoints below threshold",  # consistent verdict-wise
        r53_checked_time=fifteen_min_ago,
        metric_datapoints=[_datapoint(0.0, 30)],
        threshold=1.0,
        comparison_operator="LessThanThreshold",
    )
    assert problem is not None
    assert "hasn't seen" in problem.lower()


# ---------------------------------------------------------------------------
# Alarm-side stuck state: ALARM but metric value clears the threshold.
# ---------------------------------------------------------------------------

def test_alarm_in_alarm_but_latest_metric_clears_threshold_flagged():
    problem = detect_wiring_inconsistency(
        alarm_state="ALARM",
        alarm_state_updated=_MINUTE_AGO,
        r53_status_text="Failure: below threshold",  # R53 agrees (so it's not the R53 bug)
        r53_checked_time=_NOW,
        metric_datapoints=[_datapoint(1.0, 30)],   # ← clears LessThan(1.0) threshold
        threshold=1.0,
        comparison_operator="LessThanThreshold",
    )
    assert problem is not None
    assert "stuck" in problem.lower()


# ---------------------------------------------------------------------------
# _value_breaches operator coverage
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value,threshold,op,expected", [
    (0.0, 1.0, "LessThanThreshold", True),
    (1.0, 1.0, "LessThanThreshold", False),
    (1.0, 1.0, "LessThanOrEqualToThreshold", True),
    (2.0, 1.0, "GreaterThanThreshold", True),
    (1.0, 1.0, "GreaterThanThreshold", False),
    (1.0, 1.0, "GreaterThanOrEqualToThreshold", True),
    (5.0, 1.0, "GreaterThanThreshold", True),
    (5.0, 1.0, "WeirdOp", False),  # unknown operator → defensive False
])
def test_value_breaches(value, threshold, op, expected):
    assert _value_breaches(value, threshold, op) is expected
