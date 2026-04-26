#!/usr/bin/env python3
"""Validate Route 53 health-check ↔ CloudWatch alarm wiring (Vigil v1.6, F2).

Background:
  The v1.5 drill found that after modifying a CloudWatch alarm's `Namespace`
  (or, by extension, dimensions / threshold), the existing Route 53
  CLOUDWATCH_METRIC health check kept reporting the OLD evaluation context
  for many minutes. Specifically: the alarm was correctly transitioning to
  ALARM state on metric=0.0, but the R53 health check observation kept
  saying "Success: 2 datapoints were not less than the threshold (1.0),
  breached: 0" — i.e. it appeared to still be looking at the previous
  namespace's data.

  This is operationally dangerous because Route 53 failover records react
  to the R53 health check, NOT directly to the underlying alarm. So an
  alarm that correctly fires can be invisible to traffic routing.

  Mitigation in the runbook (CLAUDE.md "Operational hazards"): always
  recreate the R53 health check after modifying its underlying alarm.

Purpose of this tool:
  Read-only audit. For each R53 CLOUDWATCH_METRIC health check matching
  the name prefix you pass, verifies that:

    * The alarm referenced by AlarmIdentifier exists and is reachable.
    * The alarm's actual state (from DescribeAlarms) matches what the R53
      health check is reporting (from GetHealthCheckStatus).
    * The metric stream the alarm is configured to watch has recent data.
    * If the alarm is in ALARM state, the most recent datapoints really
      did breach the threshold.

  Any mismatch is reported as a FAIL row in the output table. Exit code
  is 0 when everything is consistent, 1 otherwise — suitable for a
  pre-deploy gate or a CI step.

Usage:
    python3 tools/validate_r53_alarm_wiring.py
    python3 tools/validate_r53_alarm_wiring.py --prefix fo-v10x-s1
    python3 tools/validate_r53_alarm_wiring.py --health-check-id e807cc4a-...

Requires AWS creds with read-only IAM:
    route53:ListHealthChecks
    route53:GetHealthCheck
    route53:GetHealthCheckStatus
    cloudwatch:DescribeAlarms
    cloudwatch:GetMetricStatistics

NOTE: This is a read-only audit. It cannot recreate health checks; that
is intentional — recreation requires changing the Route 53 record set
that points at the health check, which is out of scope for a local
validation tool.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import boto3


# ---------------------------------------------------------------------------
# Pure staleness-check logic (testable without AWS calls)
# ---------------------------------------------------------------------------


def detect_wiring_inconsistency(
    *,
    alarm_state: str,
    alarm_state_updated: datetime,
    r53_status_text: str,
    r53_checked_time: datetime,
    metric_datapoints: list,
    threshold: float,
    comparison_operator: str,
) -> Optional[str]:
    """Return a one-line problem description if R53 ↔ alarm are inconsistent.

    Returns ``None`` if the wiring is consistent or the data is insufficient
    to make a determination (the caller should report INSUFFICIENT_DATA in
    that case).

    Heuristics (all of these MUST be consistent for a healthy stack):
      * If the alarm is ALARM, the R53 status text should NOT include
        "Success" or "breached: 0". This is the v1.5 drill's exact symptom.
      * If the alarm is OK, the R53 status SHOULD include "Success" — a
        contradiction the other way is just as bad.
      * If the alarm transitioned to its current state more than 60 seconds
        ago, the R53 last-checked time should be more recent than that
        transition. (Otherwise R53 is stuck on a stale observation.)
      * If the alarm is ALARM, the most recent metric datapoint should
        actually breach the threshold per the comparison operator. If the
        alarm is in ALARM but the metric is currently above-threshold, the
        alarm is itself stuck (alarm-side bug, not R53-side).
    """
    state = alarm_state.upper()
    status_lower = r53_status_text.lower()

    # Case 1: alarm fired but R53 still says Success.
    if state == "ALARM" and ("success" in status_lower or "breached: 0" in status_lower):
        return (
            "Alarm is ALARM but R53 still reports Success/breached:0. "
            "Recreate the R53 health check — its observation cache is stale."
        )

    # Case 2: alarm OK but R53 reports a failure.
    if state == "OK" and "fail" in status_lower:
        return (
            "Alarm is OK but R53 still reports Failure. "
            "Recreate the R53 health check — its observation cache is stale."
        )

    # Case 3: R53 last-checked timestamp is older than the alarm transition.
    transition_age = (datetime.now(timezone.utc) - alarm_state_updated).total_seconds()
    r53_age = (datetime.now(timezone.utc) - r53_checked_time).total_seconds()
    if transition_age > 60 and r53_age > transition_age:
        return (
            f"R53 last-checked is {r53_age:.0f}s ago but alarm transitioned "
            f"{transition_age:.0f}s ago — R53 hasn't seen the new state yet."
        )

    # Case 4: alarm is ALARM but the latest metric data actually clears the
    # threshold per the comparison operator. This is an alarm-side stuck-
    # state — orthogonal to R53 but worth flagging from this tool.
    if state == "ALARM" and metric_datapoints:
        latest = max(metric_datapoints, key=lambda d: d["Timestamp"])
        latest_value = latest.get("Maximum", latest.get("Average", 0.0))
        if not _value_breaches(latest_value, threshold, comparison_operator):
            return (
                f"Alarm is ALARM but latest metric value {latest_value} does "
                f"NOT breach threshold {threshold} ({comparison_operator}). "
                "The alarm is stuck — investigate CloudWatch directly."
            )

    return None


def _value_breaches(value: float, threshold: float, operator: str) -> bool:
    """Does ``value`` breach ``threshold`` per the CW comparison operator?"""
    op = operator.lower()
    if op in ("lessthanthreshold", "lt"):
        return value < threshold
    if op in ("lessthanorequaltothreshold", "lte"):
        return value <= threshold
    if op in ("greaterthanthreshold", "gt"):
        return value > threshold
    if op in ("greaterthanorequaltothreshold", "gte"):
        return value >= threshold
    return False  # unknown operator — treat as "not breaching" defensively


# ---------------------------------------------------------------------------
# AWS plumbing
# ---------------------------------------------------------------------------


def _list_relevant_health_checks(prefix: str, only_id: Optional[str]) -> list:
    """Return CLOUDWATCH_METRIC R53 health checks matching the filter."""
    r53 = boto3.client("route53")
    resp = r53.list_health_checks()
    out = []
    for hc in resp.get("HealthChecks", []):
        cfg = hc.get("HealthCheckConfig", {})
        if cfg.get("Type") != "CLOUDWATCH_METRIC":
            continue
        if only_id and hc["Id"] != only_id:
            continue
        if prefix:
            alarm_name = cfg.get("AlarmIdentifier", {}).get("Name", "")
            if not alarm_name.startswith(prefix):
                continue
        out.append(hc)
    return out


def _fetch_alarm(region: str, name: str) -> Optional[dict]:
    cw = boto3.client("cloudwatch", region_name=region)
    resp = cw.describe_alarms(AlarmNames=[name])
    alarms = resp.get("MetricAlarms", [])
    return alarms[0] if alarms else None


def _fetch_metric_datapoints(region: str, alarm: dict) -> list:
    """Pull last 10 min of metric data the alarm is configured to watch."""
    cw = boto3.client("cloudwatch", region_name=region)
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=10)
    resp = cw.get_metric_statistics(
        Namespace=alarm["Namespace"],
        MetricName=alarm["MetricName"],
        Dimensions=alarm.get("Dimensions", []),
        StartTime=start,
        EndTime=end,
        Period=60,
        Statistics=["Maximum", "Average"],
    )
    return resp.get("Datapoints", [])


def _r53_status(hc_id: str) -> tuple:
    r53 = boto3.client("route53")
    resp = r53.get_health_check_status(HealthCheckId=hc_id)
    obs = resp.get("HealthCheckObservations", [])
    if not obs:
        return ("(no observations)", datetime.fromtimestamp(0, tz=timezone.utc))
    # Use the most recent observation across all checker regions.
    latest = max(obs, key=lambda o: o["StatusReport"]["CheckedTime"])
    return (
        latest["StatusReport"]["Status"],
        latest["StatusReport"]["CheckedTime"].astimezone(timezone.utc),
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: Optional[list] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    p.add_argument(
        "--prefix",
        default="",
        help="Only audit R53 health checks whose AlarmIdentifier.Name starts with this prefix.",
    )
    p.add_argument(
        "--health-check-id",
        default=None,
        help="Restrict to a single health check by ID.",
    )
    args = p.parse_args(argv)

    health_checks = _list_relevant_health_checks(args.prefix, args.health_check_id)
    if not health_checks:
        print("No matching CLOUDWATCH_METRIC R53 health checks found.")
        return 0

    rows = []
    any_fail = False
    for hc in health_checks:
        hc_id = hc["Id"]
        cfg = hc["HealthCheckConfig"]
        alarm_name = cfg["AlarmIdentifier"]["Name"]
        alarm_region = cfg["AlarmIdentifier"]["Region"]

        alarm = _fetch_alarm(alarm_region, alarm_name)
        if alarm is None:
            rows.append((hc_id, alarm_name, "MISSING", "alarm not found"))
            any_fail = True
            continue

        datapoints = _fetch_metric_datapoints(alarm_region, alarm)
        r53_status_text, r53_checked = _r53_status(hc_id)

        problem = detect_wiring_inconsistency(
            alarm_state=alarm["StateValue"],
            alarm_state_updated=alarm["StateUpdatedTimestamp"].astimezone(timezone.utc),
            r53_status_text=r53_status_text,
            r53_checked_time=r53_checked,
            metric_datapoints=datapoints,
            threshold=alarm["Threshold"],
            comparison_operator=alarm["ComparisonOperator"],
        )
        if problem:
            rows.append((hc_id, alarm_name, "FAIL", problem))
            any_fail = True
        else:
            rows.append((hc_id, alarm_name, "OK", f"alarm={alarm['StateValue']}"))

    # Print table (no external deps — keep it simple).
    print(f"\n{'Health Check ID':<40} {'Alarm':<45} {'Status':<6} Detail")
    print("─" * 130)
    for hc_id, alarm_name, status, detail in rows:
        print(f"{hc_id:<40} {alarm_name:<45} {status:<6} {detail}")
    print()

    return 1 if any_fail else 0


if __name__ == "__main__":
    sys.exit(main())
