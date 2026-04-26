#!/usr/bin/env python3
"""
v1.7.2 tests for the three notification UX improvements:

1. First-failure notification re-framed as INFO 'AWARENESS' (was WARNING)
   so operators don't pager-respond on a transient blip.

2. NEW notification when state transitions WAITING_AURORA_PROMOTION → SECONDARY_ACTIVE
   ('all stable on secondary — failover complete'). Previously this was silent;
   the operator only knew failover was complete by the absence of further reminder
   emails.

3. Failback-complete notification re-framed as INFO 'all back to normal'
   (was CRITICAL). Failback completion is good news, not an incident.

Run: python3 -m pytest tests/test_v172_notifications.py -v
"""

import os
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest

_MIN_ENV = {
    "SNS_TOPIC_ARN": "arn:aws:sns:us-east-1:123456789012:failover-alerts",
    "AWS_REGION": "us-east-1",
    "PRIMARY_REGION": "us-east-1",
    "SECONDARY_REGION": "us-east-2",
    "STATE_BACKEND": "s3",
    "STATE_BUCKET": "test-bucket",
    "AURORA_GLOBAL_CLUSTER_ID": "test-aurora-global",
    "AURORA_CLUSTER_ID": "test-aurora-e1",
    "TARGET_AURORA_CLUSTER_ID": "test-aurora-e2",
    "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID": "test-redis-global",
    "ELASTICACHE_REPLICATION_GROUP_ID": "test-redis-e1",
    "APP_NAME": "Vigil",
    "ENVIRONMENT": "",
}
for k, v in _MIN_ENV.items():
    os.environ.setdefault(k, v)

_p1 = patch("boto3.client")
_p1.start()
_p2 = patch("state_backend.create_backend")
_p2.start()

import failover_orchestrator_v3 as orch  # noqa: E402

_p1.stop()
_p2.stop()


def _state(**overrides):
    """Build a baseline state dict, override fields as needed."""
    base = {
        "schema_version": 1,
        "active_region": "us-east-1",
        "state": "PRIMARY_ACTIVE",
        "last_failover_ts": "1970-01-01T00:00:00Z",
        "cooldown_minutes": 5,
        "initiated_by": "INIT",
        "reason": "Initial state",
        "latch_engaged": False,
        "consecutive_failures": 0,
        "last_active_metric_ts": datetime.now(timezone.utc).isoformat(),
        "aurora_promotion_pending": False,
        "redis_promotion_pending": False,
        "last_warning_notification_ts": "1970-01-01T00:00:00Z",
    }
    base.update(overrides)
    return base


def _unhealthy():
    return {
        "healthy": False,
        "decision_reason": "HTTP 503",
        "signals": [{"signal": "http_health", "healthy": False, "reason": "HTTP 503"}],
    }


def _get_subject(mock_sns):
    return mock_sns.publish.call_args.kwargs.get("Subject") or \
        mock_sns.publish.call_args[1].get("Subject")


def _get_body(mock_sns):
    return mock_sns.publish.call_args.kwargs.get("Message") or \
        mock_sns.publish.call_args[1].get("Message")


# ---------------------------------------------------------------------------
# 1. First-failure notification: INFO / AWARENESS
# ---------------------------------------------------------------------------

class TestFirstFailureAwareness:
    """v1.7.2: first failure is INFO/awareness, not WARNING."""

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "try_increment_failures", return_value=True)
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_first_failure_is_info_severity(
        self, mock_sns, mock_health, mock_inc, mock_pub, mock_upd
    ):
        """First failure (cf going 0→1) sends INFO with AWARENESS framing."""
        mock_health.return_value = _unhealthy()
        state = _state(consecutive_failures=0)

        orch._handle_active_region(state, "us-east-1", 0, "1970-01-01T00:00:00Z")

        subject = _get_subject(mock_sns)
        # v1.7.2 change: severity is INFO (was WARNING in v1.6/v1.7).
        assert "INFO" in subject
        assert "AWARENESS" in subject.upper()
        assert "no action needed" in subject.lower()
        assert "1 of 3" in subject

        body = _get_body(mock_sns)
        assert "FIRST failure" in body
        assert "awareness" in body.lower()
        # "no action needed yet" tells operator they don't need to pager-respond.
        assert "no action needed" in body.lower() or "no action required" in body.lower()

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "try_increment_failures", return_value=True)
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_second_failure_is_warning_severity(
        self, mock_sns, mock_health, mock_inc, mock_pub, mock_upd
    ):
        """Second failure (cf going 1→2) escalates to WARNING. The ladder is
        intentional: first = INFO/awareness, second = WARNING/sustained,
        third = CRITICAL/failover."""
        mock_health.return_value = _unhealthy()
        state = _state(consecutive_failures=1)

        orch._handle_active_region(state, "us-east-1", 1, "1970-01-01T00:00:00Z")

        subject = _get_subject(mock_sns)
        assert "WARNING" in subject
        assert "2 of 3" in subject
        # No AWARENESS framing on sustained failures.
        assert "AWARENESS" not in subject.upper()


# ---------------------------------------------------------------------------
# 2. "All stable on secondary" notification
# ---------------------------------------------------------------------------

class TestAllStableOnSecondary:
    """v1.7.2: when state transitions WAITING_AURORA_PROMOTION → SECONDARY_ACTIVE,
    fire a single INFO email summarising the new steady state."""

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "try_increment_failures", return_value=False)
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_transition_to_secondary_active_sends_info(
        self, mock_sns, mock_health, mock_inc, mock_pub, mock_upd
    ):
        """When _handle_active_region runs in WAITING_AURORA_PROMOTION with all
        flags cleared, it transitions to SECONDARY_ACTIVE and sends the new
        'all stable' notification."""
        # Make health evaluation return healthy so we don't fall into the
        # cf-increment branch — we want the WAITING→SECONDARY_ACTIVE branch.
        mock_health.return_value = {
            "healthy": True,
            "decision_reason": "all good",
            "signals": [],
        }
        state = _state(
            active_region="us-east-2",
            state="WAITING_AURORA_PROMOTION",
            aurora_promotion_pending=False,
            redis_promotion_pending=False,
            latch_engaged=True,
        )

        with patch.object(orch, "CURRENT_REGION", "us-east-2"):
            orch._handle_active_region(state, "us-east-2", 0, "1970-01-01T00:00:00Z")

        # SNS publish should fire at least once with the new "all stable" message.
        stable_calls = [
            c for c in mock_sns.publish.call_args_list
            if "all stable" in c[1].get("Subject", "").lower()
            or "failover complete" in c[1].get("Subject", "").lower()
        ]
        assert len(stable_calls) == 1, "Expected exactly one 'all stable on secondary' email"
        subject = stable_calls[0][1]["Subject"]
        assert "INFO" in subject
        assert "us-east-2" in subject
        assert "stable" in subject.lower() or "complete" in subject.lower()

        body = stable_calls[0][1]["Message"]
        # Body explains the system is in steady state, latch engaged, no action needed.
        assert "us-east-2" in body
        assert "us-east-1" in body  # the failed-over-from region
        assert "latch" in body.lower()
        assert "no action" in body.lower()

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "try_increment_failures", return_value=False)
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_transition_does_not_block_on_notification_failure(
        self, mock_sns, mock_health, mock_inc, mock_pub, mock_upd
    ):
        """If SNS publish fails for any reason, the state transition still
        happens. Notification is best-effort; the state machine keeps moving."""
        mock_health.return_value = {"healthy": True, "decision_reason": "ok", "signals": []}
        mock_sns.publish.side_effect = RuntimeError("SNS down")
        state = _state(
            active_region="us-east-2",
            state="WAITING_AURORA_PROMOTION",
            aurora_promotion_pending=False,
            redis_promotion_pending=False,
            latch_engaged=True,
        )

        with patch.object(orch, "CURRENT_REGION", "us-east-2"):
            # Must not raise — failure is logged but state transition proceeds.
            orch._handle_active_region(state, "us-east-2", 0, "1970-01-01T00:00:00Z")

        # The state-transition update_failover_state still fired.
        assert mock_upd.called
        # Look for the call that wrote state=SECONDARY_ACTIVE.
        state_writes = [
            c for c in mock_upd.call_args_list
            if c[0][0].get("state") == "SECONDARY_ACTIVE"
        ]
        assert len(state_writes) == 1, "State transition must still happen even if SNS fails"

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "try_increment_failures", return_value=False)
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_no_transition_when_promotion_still_pending(
        self, mock_sns, mock_health, mock_inc, mock_pub, mock_upd
    ):
        """If aurora_promotion_pending or redis_promotion_pending is still True,
        no transition happens and no 'all stable' email fires."""
        mock_health.return_value = {"healthy": True, "decision_reason": "ok", "signals": []}
        state = _state(
            active_region="us-east-2",
            state="WAITING_AURORA_PROMOTION",
            aurora_promotion_pending=True,  # ← still pending
            redis_promotion_pending=False,
            latch_engaged=True,
        )

        with patch.object(orch, "CURRENT_REGION", "us-east-2"):
            orch._handle_active_region(state, "us-east-2", 0, "1970-01-01T00:00:00Z")

        # No state=SECONDARY_ACTIVE write.
        state_writes = [
            c for c in mock_upd.call_args_list
            if c[0][0].get("state") == "SECONDARY_ACTIVE"
        ]
        assert len(state_writes) == 0
        # No "all stable" email.
        stable_calls = [
            c for c in mock_sns.publish.call_args_list
            if "stable" in c[1].get("Subject", "").lower()
        ]
        assert len(stable_calls) == 0
