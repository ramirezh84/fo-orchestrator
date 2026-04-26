#!/usr/bin/env python3
"""End-to-end configuration matrix tests for v1.6 (PR7).

The orchestrator and failback Lambda must behave correctly across nine
baseline configurations C1–C9, defined as the cross-product of:

  Aurora  ∈ {absent, present + manual, present + auto}
  Redis   ∈ {absent, present + manual, present + auto}

This file pins the per-configuration contract that PR1's
``detect_data_tier_config()`` enables, PR5 enforces in the failback gates,
and PR6 enforces in the retry/escalation loop. Each test is parametrized
over the relevant config IDs and a small helper builds the matching env
patch dict.

Run: python3 -m pytest tests/test_config_matrix.py -v
"""

import os
from contextlib import ExitStack, contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest


# Minimum env required for both Lambda modules to import.
_MIN_ENV = {
    "SNS_TOPIC_ARN": "arn:aws:sns:us-east-1:123456789012:failover-alerts",
    "AWS_REGION": "us-east-1",
    "PRIMARY_REGION": "us-east-1",
    "SECONDARY_REGION": "us-east-2",
    "STATE_BACKEND": "dynamodb",
    "STATE_TABLE": "failover-state",
}
for k, v in _MIN_ENV.items():
    os.environ.setdefault(k, v)

# Mock boto3 + state backend before importing — same pattern as the other
# v1.6 test files (see tests/test_orchestrator.py for the rationale).
_mock_boto3_patcher = patch("boto3.client")
_mock_boto3_patcher.start().return_value = MagicMock()
_mock_create_backend_patcher = patch("state_backend.create_backend")
_mock_create_backend_patcher.start().return_value = MagicMock()

import failover_orchestrator_v3 as orch  # noqa: E402
import manual_failback_v2 as failback    # noqa: E402

_mock_boto3_patcher.stop()
_mock_create_backend_patcher.stop()


# ---------------------------------------------------------------------------
# Configuration matrix
# ---------------------------------------------------------------------------

# Each row: (id, aurora_id, aurora_auto, redis_id, redis_auto)
_CONFIGS = [
    ("C1", "",       False, "",          False),  # nothing
    ("C2", "ac-w1",  False, "",          False),  # Aurora manual only
    ("C3", "ac-w1",  True,  "",          False),  # Aurora auto only
    ("C4", "",       False, "rg-global", False),  # Redis manual only
    ("C5", "ac-w1",  False, "rg-global", False),  # both manual
    ("C6", "ac-w1",  True,  "rg-global", False),  # Aurora auto, Redis manual
    ("C7", "",       False, "rg-global", True),   # Redis auto only
    ("C8", "ac-w1",  False, "rg-global", True),   # Aurora manual, Redis auto
    ("C9", "ac-w1",  True,  "rg-global", True),   # both auto
]
_CONFIGS_BY_ID = {c[0]: c for c in _CONFIGS}


def _env(cid: str) -> dict:
    """Build the os.environ patch dict for a given configuration ID."""
    _, aurora_id, aurora_auto, redis_id, redis_auto = _CONFIGS_BY_ID[cid]
    return {
        "AURORA_CLUSTER_ID": aurora_id,
        "AURORA_AUTO_PROMOTE": "true" if aurora_auto else "false",
        "AURORA_GLOBAL_CLUSTER_ID": "ac-global" if aurora_id else "",
        "TARGET_AURORA_CLUSTER_ID": "ac-w2" if aurora_id else "",
        "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID": redis_id,
        "ELASTICACHE_REPLICATION_GROUP_ID": "rg-w1" if redis_id else "",
        "ELASTICACHE_AUTO_PROMOTE": "true" if redis_auto else "false",
    }


@contextmanager
def _config_active(cid: str, *, on_orchestrator: bool = False):
    """Patch BOTH os.environ and Lambda module-level constants for `cid`.

    detect_data_tier_config() reads os.environ at call time (PR3c) but the
    command-builder helpers (build_aurora_switchover_commands, etc.)
    reference module-level constants captured at import. Tests need both
    surfaces patched in lockstep so the helpers and the gate logic see the
    same configuration.

    By default we patch the failback Lambda's constants (most tests in this
    file exercise the failback handler). Pass on_orchestrator=True to also
    patch the orchestrator's module-level constants — needed when the test
    runs the orchestrator's reminder handlers, which call
    build_aurora_promotion_commands / build_elasticache_promotion_commands.
    """
    _, aurora_id, _, redis_id, _ = _CONFIGS_BY_ID[cid]
    aurora_global = "ac-global" if aurora_id else ""
    target_aurora = "ac-w2" if aurora_id else ""
    redis_local = "rg-w1" if redis_id else ""
    patches = [
        patch.dict(os.environ, _env(cid)),
        patch.object(failback, "AURORA_CLUSTER_ID", aurora_id),
        patch.object(failback, "AURORA_GLOBAL_CLUSTER_ID", aurora_global),
        patch.object(failback, "TARGET_AURORA_CLUSTER_ID", target_aurora),
        patch.object(failback, "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", redis_id),
        patch.object(failback, "ELASTICACHE_REPLICATION_GROUP_ID", redis_local),
        patch.object(failback, "_AWS_ACCOUNT_ID", "123456789012"),
    ]
    if on_orchestrator:
        patches.extend([
            patch.object(orch, "AURORA_CLUSTER_ID", aurora_id),
            patch.object(orch, "AURORA_GLOBAL_CLUSTER_ID", aurora_global),
            patch.object(orch, "TARGET_AURORA_CLUSTER_ID", target_aurora),
            patch.object(orch, "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", redis_id),
            patch.object(orch, "ELASTICACHE_REPLICATION_GROUP_ID", redis_local),
            patch.object(orch, "_AWS_ACCOUNT_ID", "123456789012"),
        ])
    with ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        yield


def _required_flags(cid: str) -> dict:
    """Return the minimum payload flags needed for failback to succeed in `cid`.

    Mirrors the gate logic in manual_failback_v2.py:
      - aurora_confirmed=True needed when Aurora present + manual
      - redis_confirmed=True needed when Redis present + manual
      - (auto-promote tiers don't need flags — Lambda handles them)
    """
    _, aurora_id, aurora_auto, redis_id, redis_auto = _CONFIGS_BY_ID[cid]
    flags: dict = {}
    if aurora_id and not aurora_auto:
        flags["aurora_confirmed"] = True
    if redis_id and not redis_auto:
        flags["redis_confirmed"] = True
    return flags


def _make_state(**overrides):
    base = {
        "active_region": "us-east-2",
        "state": "SECONDARY_ACTIVE",
        "latch_engaged": True,
        "consecutive_failures": 0,
        "last_failover_ts": (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
        "aurora_promotion_pending": False,
        "redis_promotion_pending": False,
        "last_warning_notification_ts": "1970-01-01T00:00:00Z",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# detect_data_tier_config: round-trip across 9 configs (orchestrator + failback)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("cid", [c[0] for c in _CONFIGS])
def test_orchestrator_detect_data_tier_matches_config(cid):
    """Orchestrator's helper sees the env values we set."""
    _, aurora_id, aurora_auto, redis_id, redis_auto = _CONFIGS_BY_ID[cid]
    with patch.dict(os.environ, _env(cid)):
        cfg = orch.detect_data_tier_config()
    assert cfg["aurora_present"] is bool(aurora_id)
    assert cfg["aurora_auto"] is (bool(aurora_id) and aurora_auto)
    assert cfg["redis_present"] is bool(redis_id)
    assert cfg["redis_auto"] is (bool(redis_id) and redis_auto)


@pytest.mark.parametrize("cid", [c[0] for c in _CONFIGS])
def test_failback_detect_data_tier_matches_orchestrator(cid):
    """Failback Lambda's helper must report the same flags as the orchestrator
    for the same env. This is what keeps the gate logic in lockstep."""
    with patch.dict(os.environ, _env(cid)):
        assert failback.detect_data_tier_config() == orch.detect_data_tier_config()


# ---------------------------------------------------------------------------
# Failback gate: SUCCESS path with the minimum required flags
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("cid", [c[0] for c in _CONFIGS])
def test_failback_succeeds_with_minimum_flags_per_config(cid):
    """For every configuration, failback must succeed when the minimum-required
    payload flags are passed. Auto-promote tiers should not need flags."""
    with patch.object(failback, "create_backend"), \
         patch.object(failback, "publish_region_health_metric"), \
         patch.object(failback, "update_failover_state"), \
         patch.object(failback, "get_failover_state", return_value=_make_state()), \
         patch.object(failback, "sns") as mock_sns, \
         patch.object(failback, "_auto_switchover_aurora",
                      return_value={"success": True, "error": ""}) as mock_aurora, \
         patch.object(failback, "_auto_failover_redis",
                      return_value={"success": True, "error": ""}) as mock_redis, \
         _config_active(cid):

        payload = {
            "target_region": "us-east-1",
            "operator": "matrix-test",
            "skip_health_check": True,
            "skip_readiness_check": True,
            **_required_flags(cid),
        }
        result = failback.handler(payload, None)

    assert result["statusCode"] == 200, (
        f"{cid} failback should succeed with payload {payload}, got {result}"
    )
    # Confirm that the auto-promote helpers were invoked exactly when expected.
    _, aurora_id, aurora_auto, redis_id, redis_auto = _CONFIGS_BY_ID[cid]
    if aurora_id and aurora_auto:
        mock_aurora.assert_called_once_with("us-east-1")
    else:
        mock_aurora.assert_not_called()
    if redis_id and redis_auto:
        mock_redis.assert_called_once_with("us-east-1")
    else:
        mock_redis.assert_not_called()


# ---------------------------------------------------------------------------
# Failback gate: REJECTION when a required flag is missing
# ---------------------------------------------------------------------------

# Cases where Aurora is present and manual → require aurora_confirmed
_AURORA_MANUAL_CIDS = [c[0] for c in _CONFIGS
                       if c[1] and not c[2]]  # aurora_id set, aurora_auto False
# Cases where Redis is present and manual → require redis_confirmed
_REDIS_MANUAL_CIDS = [c[0] for c in _CONFIGS
                      if c[3] and not c[4]]  # redis_id set, redis_auto False


@pytest.mark.parametrize("cid", _AURORA_MANUAL_CIDS)
def test_aurora_manual_configs_reject_without_aurora_confirmed(cid):
    """C2/C5/C8: failback must reject 400 when aurora_confirmed is missing,
    returning the Aurora switchover commands."""
    with patch.object(failback, "create_backend"), \
         patch.object(failback, "publish_region_health_metric"), \
         patch.object(failback, "update_failover_state"), \
         patch.object(failback, "get_failover_state", return_value=_make_state()), \
         patch.object(failback, "sns"), \
         _config_active(cid):

        # Pass redis flag if Redis is also configured manual, so we isolate the
        # Aurora-gate failure. Otherwise pass nothing.
        payload = {
            "target_region": "us-east-1",
            "operator": "matrix-test",
            "skip_health_check": True,
            "skip_readiness_check": True,
        }
        if "redis_confirmed" in _required_flags(cid):
            payload["redis_confirmed"] = True
        # aurora_confirmed deliberately missing
        result = failback.handler(payload, None)

    assert result["statusCode"] == 400
    body = result["body"]
    assert "Aurora switchover has NOT been confirmed" in body
    assert "switchover-global-cluster" in body


@pytest.mark.parametrize("cid", _REDIS_MANUAL_CIDS)
def test_redis_manual_configs_reject_without_redis_confirmed(cid):
    """C4/C5/C6: failback must reject 400 when redis_confirmed is missing,
    returning the Redis failover commands."""
    with patch.object(failback, "create_backend"), \
         patch.object(failback, "publish_region_health_metric"), \
         patch.object(failback, "update_failover_state"), \
         patch.object(failback, "get_failover_state", return_value=_make_state()), \
         patch.object(failback, "sns"), \
         patch.object(failback, "_auto_switchover_aurora",
                      return_value={"success": True, "error": ""}), \
         _config_active(cid):

        payload = {
            "target_region": "us-east-1",
            "operator": "matrix-test",
            "skip_health_check": True,
            "skip_readiness_check": True,
        }
        if "aurora_confirmed" in _required_flags(cid):
            payload["aurora_confirmed"] = True
        # redis_confirmed deliberately missing
        result = failback.handler(payload, None)

    assert result["statusCode"] == 400
    body = result["body"]
    assert "ElastiCache failover has NOT been confirmed" in body
    assert "failover-global-replication-group" in body


# ---------------------------------------------------------------------------
# Failback gate: skip_*_check break-glass overrides
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("cid", _AURORA_MANUAL_CIDS)
def test_skip_aurora_check_bypasses_aurora_gate(cid):
    """skip_aurora_check=true lets failback through without aurora_confirmed.

    Use case: operator has human-verified Aurora is already in target_region
    from a prior step and just wants the latch released."""
    with patch.object(failback, "create_backend"), \
         patch.object(failback, "publish_region_health_metric"), \
         patch.object(failback, "update_failover_state"), \
         patch.object(failback, "get_failover_state", return_value=_make_state()), \
         patch.object(failback, "sns"), \
         patch.object(failback, "_auto_switchover_aurora",
                      return_value={"success": True, "error": ""}), \
         patch.object(failback, "_auto_failover_redis",
                      return_value={"success": True, "error": ""}), \
         _config_active(cid):

        payload = {
            "target_region": "us-east-1",
            "operator": "matrix-test",
            "skip_health_check": True,
            "skip_readiness_check": True,
            "skip_aurora_check": True,
        }
        if "redis_confirmed" in _required_flags(cid):
            payload["redis_confirmed"] = True
        result = failback.handler(payload, None)

    assert result["statusCode"] == 200


@pytest.mark.parametrize("cid", _REDIS_MANUAL_CIDS)
def test_skip_redis_check_bypasses_redis_gate(cid):
    """skip_redis_check=true lets failback through without redis_confirmed."""
    with patch.object(failback, "create_backend"), \
         patch.object(failback, "publish_region_health_metric"), \
         patch.object(failback, "update_failover_state"), \
         patch.object(failback, "get_failover_state", return_value=_make_state()), \
         patch.object(failback, "sns"), \
         patch.object(failback, "_auto_switchover_aurora",
                      return_value={"success": True, "error": ""}), \
         _config_active(cid):

        payload = {
            "target_region": "us-east-1",
            "operator": "matrix-test",
            "skip_health_check": True,
            "skip_readiness_check": True,
            "skip_redis_check": True,
        }
        if "aurora_confirmed" in _required_flags(cid):
            payload["aurora_confirmed"] = True
        result = failback.handler(payload, None)

    assert result["statusCode"] == 200


# ---------------------------------------------------------------------------
# Auto-promote failure: Lambda must reject 500 (not silently complete)
# ---------------------------------------------------------------------------

# Configs where the failback Lambda calls _auto_switchover_aurora itself.
_AURORA_AUTO_CIDS = [c[0] for c in _CONFIGS if c[1] and c[2]]
# Configs where the failback Lambda calls _auto_failover_redis itself.
_REDIS_AUTO_CIDS = [c[0] for c in _CONFIGS if c[3] and c[4]]


@pytest.mark.parametrize("cid", _AURORA_AUTO_CIDS)
def test_aurora_auto_failure_rejects_500(cid):
    """When AURORA_AUTO_PROMOTE=true and the API call fails, the failback
    Lambda must reject with 500 — never silently complete."""
    with patch.object(failback, "create_backend"), \
         patch.object(failback, "publish_region_health_metric"), \
         patch.object(failback, "update_failover_state"), \
         patch.object(failback, "get_failover_state", return_value=_make_state()), \
         patch.object(failback, "sns"), \
         patch.object(failback, "_auto_switchover_aurora",
                      return_value={"success": False, "error": "API throttle"}), \
         patch.object(failback, "_auto_failover_redis",
                      return_value={"success": True, "error": ""}), \
         _config_active(cid):

        payload = {
            "target_region": "us-east-1",
            "operator": "matrix-test",
            "skip_health_check": True,
            "skip_readiness_check": True,
            **_required_flags(cid),  # only redis_confirmed if Redis manual
        }
        result = failback.handler(payload, None)

    assert result["statusCode"] == 500
    assert "Aurora auto-switchover FAILED" in result["body"]


@pytest.mark.parametrize("cid", _REDIS_AUTO_CIDS)
def test_redis_auto_failure_rejects_500(cid):
    """Symmetric: redis_auto path must reject 500 on API failure."""
    with patch.object(failback, "create_backend"), \
         patch.object(failback, "publish_region_health_metric"), \
         patch.object(failback, "update_failover_state"), \
         patch.object(failback, "get_failover_state", return_value=_make_state()), \
         patch.object(failback, "sns"), \
         patch.object(failback, "_auto_switchover_aurora",
                      return_value={"success": True, "error": ""}), \
         patch.object(failback, "_auto_failover_redis",
                      return_value={"success": False, "error": "API throttle"}), \
         _config_active(cid):

        payload = {
            "target_region": "us-east-1",
            "operator": "matrix-test",
            "skip_health_check": True,
            "skip_readiness_check": True,
            **_required_flags(cid),
        }
        result = failback.handler(payload, None)

    assert result["statusCode"] == 500
    assert "ElastiCache auto-failover FAILED" in result["body"]


# ---------------------------------------------------------------------------
# PR6 retry-cap independence: Aurora and Redis counters are independent
# ---------------------------------------------------------------------------

def test_aurora_escalation_does_not_silence_redis_reminders():
    """If Aurora has escalated (3+ retries done) but Redis is still in retry
    range, Redis reminders MUST keep firing. The retry counters are per-tier
    so a stuck Aurora doesn't mask a stuck Redis."""
    state = {
        "active_region": "us-east-2",
        "state": "WAITING_AURORA_PROMOTION",
        "latch_engaged": True,
        "consecutive_failures": 0,
        "last_failover_ts": (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat(),
        "aurora_promotion_pending": False,  # Aurora is done — only Redis pending
        "redis_promotion_pending": True,
        "redis_promotion_retry_count": 0,
        "redis_promotion_escalated": False,
        # Aurora previously escalated in some prior incident but its flag was
        # never cleared (or the operator chose to leave it). The Redis reminder
        # path must not look at Aurora's flag.
        "aurora_promotion_retry_count": 5,
        "aurora_promotion_escalated": True,
        "last_warning_notification_ts": "1970-01-01T00:00:00Z",
    }
    with patch.object(orch, "_check_if_elasticache_primary", return_value=False), \
         patch.object(orch, "update_failover_state"), \
         patch.object(orch, "sns") as mock_sns, \
         patch.object(orch, "CURRENT_REGION", "us-east-2"), \
         patch.dict(os.environ, _env("C5")):

        orch._handle_elasticache_promotion_reminder(state)

    # Redis reminder must still fire (its own retry_count is 0).
    assert mock_sns.publish.call_count == 1
    subject = mock_sns.publish.call_args.kwargs["Subject"]
    assert "ElastiCache promotion still pending" in subject
    assert "retry 1/3" in subject


def test_redis_escalation_does_not_silence_aurora_reminders():
    """Symmetric: stuck Redis must not silence Aurora reminders."""
    state = {
        "active_region": "us-east-2",
        "state": "WAITING_AURORA_PROMOTION",
        "latch_engaged": True,
        "consecutive_failures": 0,
        "last_failover_ts": (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat(),
        "aurora_promotion_pending": True,
        "aurora_promotion_retry_count": 0,
        "aurora_promotion_escalated": False,
        "redis_promotion_pending": False,
        "redis_promotion_retry_count": 5,
        "redis_promotion_escalated": True,
        "last_warning_notification_ts": "1970-01-01T00:00:00Z",
    }
    with patch.object(orch, "_check_if_aurora_writer", return_value=False), \
         patch.object(orch, "publish_region_health_metric"), \
         patch.object(orch, "update_failover_state"), \
         patch.object(orch, "sns") as mock_sns, \
         patch.object(orch, "CURRENT_REGION", "us-east-2"), \
         patch.dict(os.environ, _env("C5")), \
         patch.object(orch, "AURORA_GLOBAL_CLUSTER_ID", "ac-global"), \
         patch.object(orch, "AURORA_CLUSTER_ID", "ac-w1"), \
         patch.object(orch, "TARGET_AURORA_CLUSTER_ID", "ac-w2"):

        orch._handle_aurora_promotion_reminder(state)

    assert mock_sns.publish.call_count == 1
    subject = mock_sns.publish.call_args.kwargs["Subject"]
    assert "Aurora promotion still pending" in subject
    assert "retry 1/3" in subject


# ---------------------------------------------------------------------------
# PR6 success path: retry counters reset on detected promotion
# ---------------------------------------------------------------------------

def test_aurora_success_resets_retry_counters_and_escalation():
    """Detecting Aurora promoted (after retries had piled up) must reset every
    retry-tracking field so the next incident starts clean."""
    state = {
        "active_region": "us-east-2",
        "state": "WAITING_AURORA_PROMOTION",
        "latch_engaged": True,
        "consecutive_failures": 0,
        "last_failover_ts": (datetime.now(timezone.utc) - timedelta(minutes=20)).isoformat(),
        "aurora_promotion_pending": True,
        "aurora_promotion_retry_count": 2,
        "aurora_promotion_escalated": False,
        "aurora_promotion_last_retry_ts": datetime.now(timezone.utc).isoformat(),
        "redis_promotion_pending": False,
        "last_warning_notification_ts": "1970-01-01T00:00:00Z",
    }
    with patch.object(orch, "_check_if_aurora_writer", return_value=True), \
         patch.object(orch, "publish_region_health_metric"), \
         patch.object(orch, "update_failover_state") as mock_upd, \
         patch.object(orch, "sns"), \
         patch.object(orch, "CURRENT_REGION", "us-east-2"), \
         patch.dict(os.environ, _env("C9")):

        orch._handle_aurora_promotion_reminder(state)

    # Reset call must include all four fields zeroed/cleared.
    mock_upd.assert_called_with({
        "aurora_promotion_pending": False,
        "aurora_promotion_retry_count": 0,
        "aurora_promotion_last_retry_ts": "1970-01-01T00:00:00Z",
        "aurora_promotion_escalated": False,
    })


def test_redis_success_resets_retry_counters_and_escalation():
    """Symmetric reset for Redis."""
    state = {
        "active_region": "us-east-2",
        "state": "WAITING_AURORA_PROMOTION",
        "latch_engaged": True,
        "consecutive_failures": 0,
        "last_failover_ts": (datetime.now(timezone.utc) - timedelta(minutes=20)).isoformat(),
        "aurora_promotion_pending": False,
        "redis_promotion_pending": True,
        "redis_promotion_retry_count": 2,
        "redis_promotion_escalated": False,
        "redis_promotion_last_retry_ts": datetime.now(timezone.utc).isoformat(),
        "last_warning_notification_ts": "1970-01-01T00:00:00Z",
    }
    with patch.object(orch, "_check_if_elasticache_primary", return_value=True), \
         patch.object(orch, "update_failover_state") as mock_upd, \
         patch.object(orch, "sns"), \
         patch.object(orch, "CURRENT_REGION", "us-east-2"), \
         patch.dict(os.environ, _env("C9")):

        orch._handle_elasticache_promotion_reminder(state)

    mock_upd.assert_called_with({
        "redis_promotion_pending": False,
        "redis_promotion_retry_count": 0,
        "redis_promotion_last_retry_ts": "1970-01-01T00:00:00Z",
        "redis_promotion_escalated": False,
    })
