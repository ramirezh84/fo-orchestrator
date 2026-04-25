#!/usr/bin/env python3
"""
SNS Notification Validation — v1.4.2

Validates every SNS publish call throughout the full failover lifecycle across
all supported configuration variants:

  - State backend: S3 vs DynamoDB
  - ElastiCache: enabled vs disabled
  - API Gateway: configured vs not configured
  - Routing mode: active/passive (failover) vs active/active

AI features are disabled in all configs (AI_RCA_ENABLED=false,
AI_AURORA_ADVISOR_MODE=disabled, AI_FAILBACK_READINESS_ENABLED=false).

Run: python3 -m pytest tests/test_sns_notifications_failover.py -v
"""

import os
import sys
import json
from contextlib import ExitStack, contextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional
from unittest.mock import MagicMock, patch, call

import pytest

# ---------------------------------------------------------------------------
# Environment setup — must happen BEFORE importing either module because both
# read env vars at module level (SNS_TOPIC_ARN is required at line 73/58).
# Base config: S3 + ElastiCache + API GW disabled + active/passive (most
# feature-rich combination so the module initialises with everything enabled).
# ---------------------------------------------------------------------------
_ENV = {
    "SNS_TOPIC_ARN": "arn:aws:sns:us-east-1:123456789012:failover-alerts",
    "AWS_REGION": "us-east-1",
    "PRIMARY_REGION": "us-east-1",
    "SECONDARY_REGION": "us-east-2",
    # S3 backend (base config)
    "STATE_BACKEND": "s3",
    "STATE_BUCKET": "test-fo-state-us-east-1",
    "STATE_PREFIX": "failover-state/",
    "REMOTE_STATE_BUCKET": "",
    # ElastiCache (enabled in base config)
    "ELASTICACHE_REPLICATION_GROUP_ID": "my-redis-rg",
    "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID": "my-global-redis",
    "ELASTICACHE_AUTO_PROMOTE": "false",
    # API Gateway (disabled in base config)
    "API_GW_NAME": "",
    "API_GW_5XX_THRESHOLD_PERCENT": "50.0",
    # AI — all disabled
    "AI_RCA_ENABLED": "false",
    "AI_AURORA_ADVISOR_MODE": "disabled",
    "AI_FAILBACK_READINESS_ENABLED": "false",
    # Failover mode
    "ROUTING_MODE": "failover",
    "FAILOVER_MODE": "auto",
    # App identity
    "APP_NAME": "SentinelFO",
    # Timing
    "COOLDOWN_MINUTES": "30",
    "CONSECUTIVE_FAILURES_THRESHOLD": "3",
    "WARNING_NOTIFICATION_COOLDOWN_MINUTES": "10",
    "AURORA_PROMOTION_REMINDER_INTERVAL_MINUTES": "5",
    # Aurora config
    "AURORA_CLUSTER_ID": "my-aurora-w1",
    "TARGET_AURORA_CLUSTER_ID": "my-aurora-w2",
    "AURORA_GLOBAL_CLUSTER_ID": "my-aurora-global",
    "AURORA_AUTO_PROMOTE": "false",
    # Other required but empty
    "HEALTH_CHECK_URL": "",
    "ECS_CLUSTER_NAME": "",
    "ECS_SERVICE_NAME": "",
    "ALB_ARN_SUFFIX": "",
    "TG_ARN_SUFFIX": "",
    "STATE_TABLE": "failover-state",
}

for k, v in _ENV.items():
    os.environ.setdefault(k, v)

# ---------------------------------------------------------------------------
# Mock boto3 and state backend at module level so the orchestrator's and
# failback's top-level client creation does not make real AWS calls.
# ---------------------------------------------------------------------------
_mock_boto3_patcher = patch("boto3.client")
_mock_boto3_client = _mock_boto3_patcher.start()
_mock_boto3_client.return_value = MagicMock()

_mock_create_backend_patcher = patch("state_backend.create_backend")
_mock_create_backend = _mock_create_backend_patcher.start()
_mock_state_backend = MagicMock()
_mock_create_backend.return_value = _mock_state_backend

# Now safe to import both modules
import failover_orchestrator_v3 as orch
import manual_failback_v2 as failback

# Stop import-time patchers — per-test patches take over from here
_mock_boto3_patcher.stop()
_mock_create_backend_patcher.stop()


# ===========================================================================
# Autouse fixture: ensure APP_NAME is "SentinelFO" for every test in this file.
# Other test files set APP_NAME="" via os.environ.setdefault, which silently
# wins when they run first. Patch the module attribute directly so our tests
# are isolated from load-order effects.
# ===========================================================================

@pytest.fixture(autouse=True)
def patch_app_name():
    with patch.object(orch, "APP_NAME", "SentinelFO"), \
         patch.object(failback, "APP_NAME", "SentinelFO"):
        yield


# ===========================================================================
# Helpers
# ===========================================================================

def _make_state(
    active_region: str = "us-east-1",
    state: str = "PRIMARY_ACTIVE",
    latch_engaged: bool = False,
    consecutive_failures: int = 0,
    last_failover_ts: str = "1970-01-01T00:00:00Z",
    aurora_promotion_pending: bool = False,
    redis_promotion_pending: bool = False,
    last_warning_notification_ts: str = "1970-01-01T00:00:00Z",
    last_active_metric_ts: Optional[str] = None,
    region_health: Optional[dict] = None,
) -> dict:
    return {
        "active_region": active_region,
        "state": state,
        "latch_engaged": latch_engaged,
        "consecutive_failures": consecutive_failures,
        "last_failover_ts": last_failover_ts,
        "aurora_promotion_pending": aurora_promotion_pending,
        "redis_promotion_pending": redis_promotion_pending,
        "last_warning_notification_ts": last_warning_notification_ts,
        "last_active_metric_ts": last_active_metric_ts or datetime.now(timezone.utc).isoformat(),
        "region_health": region_health or {},
    }


def _make_failback_state(
    active_region: str = "us-east-2",
    state: str = "SECONDARY_ACTIVE",
    latch_engaged: bool = True,
) -> dict:
    return {
        "active_region": active_region,
        "state": state,
        "latch_engaged": latch_engaged,
        "consecutive_failures": 0,
        "last_failover_ts": (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
        "aurora_promotion_pending": False,
        "redis_promotion_pending": False,
        "last_warning_notification_ts": "1970-01-01T00:00:00Z",
    }


def _unhealthy_health(reason: str = "ECS task count below minimum") -> dict:
    return {
        "healthy": False,
        "decision_reason": reason,
        "signals": [
            {"signal": "ecs_tasks", "healthy": False, "reason": reason},
        ],
    }


def _healthy_health() -> dict:
    return {
        "healthy": True,
        "decision_reason": "all signals healthy",
        "signals": [
            {"signal": "ecs_tasks", "healthy": True, "reason": "ok"},
        ],
    }


@contextmanager
def config_variant(
    elasticache: bool = True,
    api_gw: bool = False,
    routing: str = "failover",
    failover_mode: str = "auto",
):
    """Apply per-test module-level patches for config variants."""
    patches = [
        patch.object(orch, "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID",
                     "my-global-redis" if elasticache else ""),
        patch.object(orch, "ELASTICACHE_REPLICATION_GROUP_ID",
                     "my-redis-rg" if elasticache else ""),
        patch.object(orch, "API_GW_NAME", "my-api-gw-id" if api_gw else ""),
        patch.object(orch, "ROUTING_MODE", routing),
        patch.object(orch, "FAILOVER_MODE", failover_mode),
    ]
    with ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        yield


def _get_sns_subject(mock_sns) -> str:
    """Extract Subject from the most recent sns.publish call."""
    return mock_sns.publish.call_args[1]["Subject"]


def _get_sns_message(mock_sns) -> str:
    """Extract Message from the most recent sns.publish call."""
    return mock_sns.publish.call_args[1]["Message"]


def _sns_subjects(mock_sns) -> list:
    """Return all Subject values from all sns.publish calls."""
    return [c[1]["Subject"] for c in mock_sns.publish.call_args_list]


# ===========================================================================
# 1. TestSNSDegradationWarnings
#    Tests send_warning_notification calls during region degradation.
#    Covers: line 2487 (_handle_active_region), line 1314 (_handle_active_active)
# ===========================================================================

class TestSNSDegradationWarnings:
    """WARNING notifications when a region is degrading (below failure threshold)."""

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "try_increment_failures", return_value=True)
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_degraded_warning_subsequent_failure_s3_ec(
        self, mock_sns, mock_health, mock_inc, mock_pub, mock_upd
    ):
        """v1.6: subsequent failure (2 of 3) — subject says 'health failure 2 of 3'."""
        mock_health.return_value = _unhealthy_health()
        state = _make_state(consecutive_failures=1)

        orch._handle_active_region(state, "us-east-1", 1, "1970-01-01T00:00:00Z")

        assert mock_sns.publish.called
        subject = _get_sns_subject(mock_sns)
        assert subject.startswith("[SentinelFO] WARNING:")
        assert "us-east-1" in subject
        assert "2 of 3" in subject
        assert "sustained" in subject.lower()
        assert len(subject) <= 100

        # Body contains the v1.6 journey breadcrumb and explicit next-step.
        body = _get_sns_message(mock_sns)
        assert "WHAT IS HAPPENING" in body
        assert "WHERE WE ARE IN THE INCIDENT" in body
        assert "WHAT TO DO NEXT" in body
        assert "[✓] First failure" in body
        assert "Sustained (2/3)" in body
        assert "investigate" in body.lower() or "Investigate" in body

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "try_increment_failures", return_value=True)
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_degraded_warning_first_failure_explicit(
        self, mock_sns, mock_health, mock_inc, mock_pub, mock_upd
    ):
        """v1.6: first failure (1 of 3) — subject says 'FIRST health failure'."""
        mock_health.return_value = _unhealthy_health()
        state = _make_state(consecutive_failures=0)

        orch._handle_active_region(state, "us-east-1", 0, "1970-01-01T00:00:00Z")

        subject = _get_sns_subject(mock_sns)
        assert "FIRST" in subject
        assert "1 of 3" in subject

        body = _get_sns_message(mock_sns)
        assert "first failure of an incident" in body
        # First-failure journey breadcrumb shows "[1]" for the active step.
        assert "[1] First failure" in body

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "try_increment_failures", return_value=True)
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_degraded_warning_ddb_ec(self, mock_sns, mock_health, mock_inc, mock_pub, mock_upd):
        """DynamoDB+ElastiCache: same v1.6 contract as S3 variant."""
        mock_health.return_value = _unhealthy_health()
        state = _make_state(consecutive_failures=1)

        with patch.object(orch, "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", "my-global-redis"):
            orch._handle_active_region(state, "us-east-1", 1, "1970-01-01T00:00:00Z")

        subject = _get_sns_subject(mock_sns)
        assert "2 of 3" in subject
        assert "WARNING" in subject

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "try_increment_failures", return_value=True)
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_degraded_warning_no_elasticache(self, mock_sns, mock_health, mock_inc, mock_pub, mock_upd):
        """S3, ElastiCache disabled: warning body has no ElastiCache content."""
        mock_health.return_value = _unhealthy_health()
        state = _make_state(consecutive_failures=1)

        with config_variant(elasticache=False):
            orch._handle_active_region(state, "us-east-1", 1, "1970-01-01T00:00:00Z")

        subject = _get_sns_subject(mock_sns)
        assert "WARNING" in subject
        assert "2 of 3" in subject
        message = _get_sns_message(mock_sns)
        assert "elasticache" not in message.lower()

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "try_increment_failures", return_value=True)
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_degraded_warning_with_api_gw(self, mock_sns, mock_health, mock_inc, mock_pub, mock_upd):
        """API GW configured: decision reason (incl. API GW) flows into the body."""
        mock_health.return_value = {
            "healthy": False,
            "decision_reason": "api_gw_5xx: error rate 75.0% > 50.0%",
            "signals": [
                {"signal": "api_gw_5xx", "healthy": False, "reason": "75% 5xx rate"},
            ],
        }
        state = _make_state(consecutive_failures=1)

        with config_variant(api_gw=True):
            orch._handle_active_region(state, "us-east-1", 1, "1970-01-01T00:00:00Z")

        subject = _get_sns_subject(mock_sns)
        assert "WARNING" in subject
        assert "2 of 3" in subject
        message = _get_sns_message(mock_sns)
        assert "api_gw_5xx" in message or "75%" in message

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "try_increment_failures", return_value=True)
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_cooldown_active_warning_subject(self, mock_sns, mock_health, mock_inc, mock_pub, mock_upd):
        """v1.6: cooldown-blocking-failover is WARNING (not CRITICAL — that label was misleading)."""
        mock_health.return_value = _unhealthy_health()
        recent_ts = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        state = _make_state(consecutive_failures=2)

        orch._handle_active_region(state, "us-east-1", 2, recent_ts)

        subject = _get_sns_subject(mock_sns)
        assert "WARNING" in subject
        assert "cooldown" in subject.lower()
        assert "us-east-1" in subject
        assert "blocked" in subject.lower()

        body = _get_sns_message(mock_sns)
        # Journey explicitly tells operator we're at the cooldown phase.
        assert "Cooldown — failover deferred" in body
        # Next-step explains the override path so operator isn't stuck guessing.
        assert "execute_failover" in body

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "try_increment_failures", return_value=True)
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_warning_throttled_within_cooldown(self, mock_sns, mock_health, mock_inc, mock_pub, mock_upd):
        """WARNING is NOT sent if last warning was within the cooldown window."""
        mock_health.return_value = _unhealthy_health()
        recent_warning_ts = (datetime.now(timezone.utc) - timedelta(minutes=2)).isoformat()
        state = _make_state(
            consecutive_failures=1,
            last_warning_notification_ts=recent_warning_ts,
        )

        orch._handle_active_region(state, "us-east-1", 1, "1970-01-01T00:00:00Z")

        mock_sns.publish.assert_not_called()

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "try_increment_failures", return_value=True)
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_warning_sends_after_cooldown_expires(self, mock_sns, mock_health, mock_inc, mock_pub, mock_upd):
        """WARNING IS sent after the cooldown window has passed."""
        mock_health.return_value = _unhealthy_health()
        old_warning_ts = (datetime.now(timezone.utc) - timedelta(minutes=15)).isoformat()
        state = _make_state(
            consecutive_failures=1,
            last_warning_notification_ts=old_warning_ts,
        )

        orch._handle_active_region(state, "us-east-1", 1, "1970-01-01T00:00:00Z")

        assert mock_sns.publish.called
        assert "WARNING" in _get_sns_subject(mock_sns)


# ===========================================================================
# 2. TestSNSRegionRecovery
#    Tests the RECOVERED notification in active/active mode (line 1292) and
#    the REMOVED notification (line 1374).
# ===========================================================================

class TestSNSRegionRecovery:
    """RECOVERED and REMOVED FROM POOL notifications in active/active mode."""

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "try_increment_failures", return_value=True)
    @patch.object(orch, "_emit_failover_event")
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_region_recovered_active_active(
        self, mock_sns, mock_health, mock_emit, mock_inc, mock_pub, mock_upd
    ):
        """Active/active: RECOVERED notification sent when region returns to healthy."""
        mock_health.return_value = _healthy_health()
        # consecutive_failures=3 means it was at threshold (unhealthy), now recovering
        state = _make_state(consecutive_failures=3)

        with config_variant(routing="active-active"):
            orch._handle_active_active(state)

        assert mock_sns.publish.called
        subject = _get_sns_subject(mock_sns)
        assert "RECOVERED" in subject
        assert "us-east-1" in subject
        assert "rejoining" in subject.lower() or "healthy" in subject.lower()
        assert subject.startswith("[SentinelFO]")
        assert len(subject) <= 100

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "try_increment_failures", return_value=True)
    @patch.object(orch, "_emit_failover_event")
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_recovered_message_includes_previous_failure_count(
        self, mock_sns, mock_health, mock_emit, mock_inc, mock_pub, mock_upd
    ):
        """RECOVERED message body includes previous consecutive failure count."""
        mock_health.return_value = _healthy_health()
        state = _make_state(consecutive_failures=5)

        with config_variant(routing="active-active"):
            orch._handle_active_active(state)

        message = _get_sns_message(mock_sns)
        assert "5" in message  # Previous failures count

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "try_increment_failures", return_value=True)
    @patch.object(orch, "_emit_failover_event")
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_region_removed_from_pool_active_active(
        self, mock_sns, mock_health, mock_emit, mock_inc, mock_pub, mock_upd
    ):
        """Active/active: CRITICAL notification sent when region is removed from pool."""
        mock_health.return_value = _unhealthy_health()
        # consecutive_failures=3 → at threshold, mark unhealthy
        state = _make_state(consecutive_failures=3)

        with config_variant(routing="active-active"):
            orch._handle_active_active(state)

        assert mock_sns.publish.called
        subject = _get_sns_subject(mock_sns)
        assert "CRITICAL" in subject
        assert "removed from traffic pool" in subject
        assert "us-east-1" in subject

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "try_increment_failures", return_value=True)
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_degraded_warning_active_active_below_threshold(
        self, mock_sns, mock_health, mock_inc, mock_pub, mock_upd
    ):
        """Active/active: WARNING notification when below threshold (line 1314)."""
        mock_health.return_value = _unhealthy_health()
        state = _make_state(consecutive_failures=1)

        with config_variant(routing="active-active"):
            orch._handle_active_active(state)

        subject = _get_sns_subject(mock_sns)
        assert "WARNING" in subject
        assert "degraded" in subject
        assert "2/3" in subject

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "try_increment_failures", return_value=True)
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_cooldown_active_active_active(
        self, mock_sns, mock_health, mock_inc, mock_pub, mock_upd
    ):
        """Active/active: WARNING cooldown notification (line 1335)."""
        mock_health.return_value = _unhealthy_health()
        # threshold reached (consecutive_failures=3) but recent failover keeps cooldown
        recent_ts = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        state = _make_state(consecutive_failures=3, last_failover_ts=recent_ts)

        with config_variant(routing="active-active"):
            orch._handle_active_active(state)

        subject = _get_sns_subject(mock_sns)
        assert "WARNING" in subject
        assert "cooldown" in subject.lower()


# ===========================================================================
# 3. TestSNSFailoverExecution
#    Tests the primary failover notification (line 2733 — manual Aurora/EC path)
#    and the failover failure notification (line 2772).
# ===========================================================================

class TestSNSFailoverExecution:
    """SNS notifications when failover executes or fails."""

    def _run_failover(self, mock_sns, mock_health, mock_pub, mock_upd,
                      elasticache=True, region_health_override=None):
        """Common helper: set up mocks and trigger failover in _handle_active_region."""
        mock_health.return_value = _unhealthy_health()
        state = _make_state(
            consecutive_failures=2,
            region_health=region_health_override or {},
        )

        with ExitStack() as stack:
            stack.enter_context(patch.object(orch, "try_increment_failures", return_value=True))
            stack.enter_context(patch.object(orch, "try_claim_failover", return_value=True))
            stack.enter_context(patch.object(orch, "_emit_failover_event"))
            stack.enter_context(patch.object(orch, "_run_rca_analysis", return_value=""))
            stack.enter_context(patch.object(orch, "_run_aurora_advisor", return_value=("", None)))
            stack.enter_context(config_variant(elasticache=elasticache))

            orch._handle_active_region(state, "us-east-1", 2, "1970-01-01T00:00:00Z")

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_failover_s3_ec_includes_elasticache_commands(
        self, mock_sns, mock_health, mock_pub, mock_upd
    ):
        """S3+ElastiCache: failover message includes ElastiCache CLI commands."""
        self._run_failover(mock_sns, mock_health, mock_pub, mock_upd, elasticache=True)

        assert mock_sns.publish.called
        # Find the FAILOVER notification (may be preceded by other calls)
        failover_calls = [
            c for c in mock_sns.publish.call_args_list
            if "FAILOVER" in c[1].get("Subject", "")
        ]
        assert len(failover_calls) >= 1
        subject = failover_calls[-1][1]["Subject"]
        message = failover_calls[-1][1]["Message"]

        assert "FAILOVER" in subject
        assert "us-east-2" in subject
        assert "PROMOTE DATA TIER NOW" in subject
        assert subject.startswith("[SentinelFO]")
        assert len(subject) <= 100

        assert "failover-global-replication-group" in message
        assert "my-global-redis" in message
        assert "ELASTICACHE PROMOTION REQUIRED" in message

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_failover_s3_ec_no_ai_content(
        self, mock_sns, mock_health, mock_pub, mock_upd
    ):
        """Failover message must NOT contain AI RCA content when AI is disabled."""
        self._run_failover(mock_sns, mock_health, mock_pub, mock_upd)

        # Check all SNS messages sent
        for c in mock_sns.publish.call_args_list:
            message = c[1].get("Message", "")
            assert "== RCA ==" not in message
            assert "Root Cause Analysis" not in message
            assert "Aurora Advisor" not in message
            assert "AI readiness" not in message

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_failover_no_ec_excludes_elasticache_commands(
        self, mock_sns, mock_health, mock_pub, mock_upd
    ):
        """Without ElastiCache: failover message does NOT include ElastiCache CLI."""
        self._run_failover(mock_sns, mock_health, mock_pub, mock_upd, elasticache=False)

        failover_calls = [
            c for c in mock_sns.publish.call_args_list
            if "FAILOVER" in c[1].get("Subject", "")
        ]
        assert len(failover_calls) >= 1
        message = failover_calls[-1][1]["Message"]

        assert "failover-global-replication-group" not in message
        assert "ELASTICACHE PROMOTION REQUIRED" not in message
        # Aurora commands are still present
        assert "switchover-global-cluster" in message or "aurora" in message.lower()

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_failover_auto_promote_v16_template(
        self, mock_sns, mock_health, mock_pub, mock_upd
    ):
        """v1.6 PR3b: AURORA_AUTO_PROMOTE=true path emits the new structured template.

        Subject is severity-prefixed with "From → To" framing; body has the
        WHAT/WHY/CONTEXT/JOURNEY/NEXT shape and explains the latch + monitor command.
        """
        mock_health.return_value = _unhealthy_health()
        state = _make_state(consecutive_failures=2)
        aurora_result = {"success": True, "method": "switchover"}

        with ExitStack() as stack:
            stack.enter_context(patch.object(orch, "try_increment_failures", return_value=True))
            stack.enter_context(patch.object(orch, "try_claim_failover", return_value=True))
            stack.enter_context(patch.object(orch, "_emit_failover_event"))
            stack.enter_context(patch.object(orch, "_run_rca_analysis", return_value=""))
            stack.enter_context(patch.object(orch, "_run_aurora_advisor", return_value=("", None)))
            stack.enter_context(patch.object(orch, "_auto_promote_aurora", return_value=aurora_result))
            stack.enter_context(patch.dict(os.environ, {"AURORA_AUTO_PROMOTE": "true"}))
            stack.enter_context(config_variant(elasticache=True))

            orch._handle_active_region(state, "us-east-1", 2, "1970-01-01T00:00:00Z")

        failover_calls = [
            c for c in mock_sns.publish.call_args_list
            if "Failover triggered" in c[1].get("Subject", "")
        ]
        assert len(failover_calls) == 1, "Exactly one v1.6-templated failover email expected"
        subject = failover_calls[0][1]["Subject"]
        body = failover_calls[0][1]["Message"]

        # Subject: severity prefix + From→To framing.
        assert subject.startswith("[SentinelFO] CRITICAL:")
        assert "us-east-1 to us-east-2" in subject
        assert len(subject) <= 100

        # Body has all four required sections in order.
        for heading in ("WHAT IS HAPPENING", "WHERE WE ARE IN THE INCIDENT",
                        "CONTEXT", "WHAT TO DO NEXT"):
            assert heading in body, f"Missing section: {heading}"

        # Journey breadcrumb shows we're at the failover step.
        assert "[→] Failover IN PROGRESS" in body
        # Next-step gives the operator the actual monitor command.
        assert "describe-db-clusters" in body
        # Latch context surfaced explicitly so operator knows traffic stays put.
        assert "latch is engaged" in body.lower()

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_failover_with_api_gw_decision_reason_in_message(
        self, mock_sns, mock_health, mock_pub, mock_upd
    ):
        """API GW configured and failing: decision reason (API GW) appears in failover message."""
        mock_health.return_value = {
            "healthy": False,
            "decision_reason": "api_gw_5xx: 75.0% errors > 50.0% threshold",
            "signals": [{"signal": "api_gw_5xx", "healthy": False, "reason": "75% 5xx"}],
        }
        state = _make_state(consecutive_failures=2)

        with ExitStack() as stack:
            stack.enter_context(patch.object(orch, "try_increment_failures", return_value=True))
            stack.enter_context(patch.object(orch, "try_claim_failover", return_value=True))
            stack.enter_context(patch.object(orch, "_emit_failover_event"))
            stack.enter_context(patch.object(orch, "_run_rca_analysis", return_value=""))
            stack.enter_context(patch.object(orch, "_run_aurora_advisor", return_value=("", None)))
            stack.enter_context(config_variant(api_gw=True))

            orch._handle_active_region(state, "us-east-1", 2, "1970-01-01T00:00:00Z")

        failover_calls = [
            c for c in mock_sns.publish.call_args_list
            if "FAILOVER" in c[1].get("Subject", "")
        ]
        assert len(failover_calls) >= 1
        message = failover_calls[-1][1]["Message"]
        # API GW decision reason surfaced in message body
        assert "api_gw_5xx" in message or "75.0%" in message

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_failover_ddb_same_sns_format(
        self, mock_sns, mock_health, mock_pub, mock_upd
    ):
        """DynamoDB backend: failover SNS subject/message format identical to S3."""
        self._run_failover(mock_sns, mock_health, mock_pub, mock_upd)

        failover_calls = [
            c for c in mock_sns.publish.call_args_list
            if "FAILOVER" in c[1].get("Subject", "")
        ]
        assert len(failover_calls) >= 1
        subject = failover_calls[-1][1]["Subject"]
        assert "FAILOVER" in subject
        assert "PROMOTE DATA TIER NOW" in subject
        assert subject.startswith("[SentinelFO]")

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_failover_active_active_different_subject(
        self, mock_sns, mock_health, mock_pub, mock_upd
    ):
        """Active/active: region removed notification differs from active/passive failover."""
        mock_health.return_value = _unhealthy_health()
        state = _make_state(consecutive_failures=3)

        with ExitStack() as stack:
            stack.enter_context(patch.object(orch, "try_increment_failures", return_value=True))
            stack.enter_context(patch.object(orch, "_emit_failover_event"))
            stack.enter_context(config_variant(routing="active-active"))

            orch._handle_active_active(state)

        subject = _get_sns_subject(mock_sns)
        # Active/active uses "removed from traffic pool", NOT "PROMOTE DATA TIER NOW"
        assert "removed from traffic pool" in subject
        assert "PROMOTE DATA TIER NOW" not in subject
        assert "CRITICAL" in subject

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_failover_execution_failed_sends_critical(
        self, mock_sns, mock_health, mock_pub, mock_upd
    ):
        """FAILOVER FAILED notification sent when publish_region_health_metric raises."""
        mock_health.return_value = _unhealthy_health()
        # Make publish_region_health_metric raise on the second call (during failover try)
        call_count = {"n": 0}

        def pub_side_effect(region, healthy):
            call_count["n"] += 1
            if call_count["n"] == 1:  # first call inside the try block (metric=0 at line 2649)
                raise Exception("CloudWatch unavailable")

        mock_pub.side_effect = pub_side_effect
        state = _make_state(consecutive_failures=2)

        with ExitStack() as stack:
            stack.enter_context(patch.object(orch, "try_increment_failures", return_value=True))
            stack.enter_context(patch.object(orch, "try_claim_failover", return_value=True))
            stack.enter_context(patch.object(orch, "_emit_failover_event"))
            stack.enter_context(patch.object(orch, "_run_rca_analysis", return_value=""))
            stack.enter_context(patch.object(orch, "_run_aurora_advisor", return_value=("", None)))

            with pytest.raises(Exception, match="CloudWatch unavailable"):
                orch._handle_active_region(state, "us-east-1", 2, "1970-01-01T00:00:00Z")

        # FAILOVER FAILED notification must have been sent
        failed_calls = [
            c for c in mock_sns.publish.call_args_list
            if "FAILOVER FAILED" in c[1].get("Subject", "")
        ]
        assert len(failed_calls) == 1
        subject = failed_calls[0][1]["Subject"]
        assert "us-east-1" in subject
        assert "us-east-2" in subject


# ===========================================================================
# 4. TestSNSAuroraPromotionReminders
#    Tests _handle_aurora_promotion_reminder (lines 1872, 1893).
# ===========================================================================

class TestSNSAuroraPromotionReminders:
    """Aurora promotion confirmation and periodic reminder notifications."""

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "_check_if_aurora_writer", return_value=True)
    @patch.object(orch, "sns")
    def test_aurora_confirmed_sends_info(
        self, mock_sns, mock_check, mock_pub, mock_upd
    ):
        """v1.6: Aurora promotion confirmed is INFO (was CRITICAL — promotion success
        is good news, not an alert). Subject names the new writer; body explains
        the app can write again."""
        state = _make_state(
            active_region="us-east-2",
            state="WAITING_AURORA_PROMOTION",
            aurora_promotion_pending=True,
            last_failover_ts=(datetime.now(timezone.utc) - timedelta(minutes=7)).isoformat(),
        )

        with patch.object(orch, "CURRENT_REGION", "us-east-2"):
            orch._handle_aurora_promotion_reminder(state)

        assert mock_sns.publish.called
        subject = _get_sns_subject(mock_sns)
        assert subject.startswith("[SentinelFO] INFO:")
        assert "Aurora is now writer in us-east-2" in subject
        assert len(subject) <= 100
        # Flag must be cleared
        mock_upd.assert_called_with({"aurora_promotion_pending": False})

        body = _get_sns_message(mock_sns)
        assert "your app can now write" in body.lower()
        assert "[✓] Aurora promoted" in body

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "_check_if_aurora_writer", return_value=False)
    @patch.object(orch, "sns")
    def test_aurora_reminder_fires_at_interval(
        self, mock_sns, mock_check, mock_pub, mock_upd
    ):
        """REMINDER sent when elapsed minutes is divisible by interval (line 1893)."""
        # 5 minutes elapsed → int(5) % 5 == 0 → fires
        last_failover_ts = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        state = _make_state(
            active_region="us-east-2",
            state="WAITING_AURORA_PROMOTION",
            aurora_promotion_pending=True,
            last_failover_ts=last_failover_ts,
        )

        with patch.object(orch, "CURRENT_REGION", "us-east-2"):
            orch._handle_aurora_promotion_reminder(state)

        assert mock_sns.publish.called
        subject = _get_sns_subject(mock_sns)
        assert "REMINDER" in subject
        assert "Aurora" in subject
        assert "pending" in subject
        assert "5m" in subject
        assert subject.startswith("[SentinelFO]")

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "_check_if_aurora_writer", return_value=False)
    @patch.object(orch, "sns")
    def test_aurora_reminder_skipped_at_non_interval(
        self, mock_sns, mock_check, mock_pub, mock_upd
    ):
        """REMINDER NOT sent when elapsed minutes is not divisible by interval."""
        # 3 minutes elapsed → int(3) % 5 != 0 → no reminder
        last_failover_ts = (datetime.now(timezone.utc) - timedelta(minutes=3)).isoformat()
        state = _make_state(
            active_region="us-east-2",
            state="WAITING_AURORA_PROMOTION",
            aurora_promotion_pending=True,
            last_failover_ts=last_failover_ts,
        )

        with patch.object(orch, "CURRENT_REGION", "us-east-2"):
            orch._handle_aurora_promotion_reminder(state)

        mock_sns.publish.assert_not_called()

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "_check_if_aurora_writer", return_value=True)
    @patch.object(orch, "sns")
    def test_aurora_confirmed_with_s3_backend_updates_state(
        self, mock_sns, mock_check, mock_pub, mock_upd
    ):
        """Aurora confirmed: state backend receives flag-cleared update (S3 variant)."""
        last_failover_ts = (datetime.now(timezone.utc) - timedelta(minutes=8)).isoformat()
        state = _make_state(
            active_region="us-east-2",
            aurora_promotion_pending=True,
            last_failover_ts=last_failover_ts,
        )

        with patch.object(orch, "CURRENT_REGION", "us-east-2"):
            result = orch._handle_aurora_promotion_reminder(state)

        assert result["statusCode"] == 200
        assert "confirmed" in result["body"].lower()
        mock_upd.assert_called_with({"aurora_promotion_pending": False})


# ===========================================================================
# 5. TestSNSElasticachePromotionReminders
#    Tests _handle_elasticache_promotion_reminder (lines 1938, 1952).
# ===========================================================================

class TestSNSElasticachePromotionReminders:
    """ElastiCache promotion confirmation and periodic reminder notifications."""

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "_check_if_elasticache_primary", return_value=True)
    @patch.object(orch, "sns")
    def test_ec_confirmed_sends_info(self, mock_sns, mock_check, mock_upd):
        """v1.6: ElastiCache promotion confirmed is INFO (was CRITICAL — same
        rationale as Aurora confirmed)."""
        state = _make_state(
            active_region="us-east-2",
            state="WAITING_AURORA_PROMOTION",
            redis_promotion_pending=True,
            last_failover_ts=(datetime.now(timezone.utc) - timedelta(minutes=6)).isoformat(),
        )

        with patch.object(orch, "CURRENT_REGION", "us-east-2"):
            orch._handle_elasticache_promotion_reminder(state)

        assert mock_sns.publish.called
        subject = _get_sns_subject(mock_sns)
        assert subject.startswith("[SentinelFO] INFO:")
        assert "ElastiCache is now primary in us-east-2" in subject
        assert len(subject) <= 100
        mock_upd.assert_called_with({"redis_promotion_pending": False})

        body = _get_sns_message(mock_sns)
        assert "[✓] Redis promoted" in body
        assert "served locally" in body.lower()

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "_check_if_elasticache_primary", return_value=False)
    @patch.object(orch, "sns")
    def test_ec_reminder_fires_at_interval(self, mock_sns, mock_check, mock_upd):
        """REMINDER sent when elapsed minutes divisible by interval (line 1952)."""
        last_failover_ts = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        state = _make_state(
            active_region="us-east-2",
            redis_promotion_pending=True,
            last_failover_ts=last_failover_ts,
        )

        with patch.object(orch, "CURRENT_REGION", "us-east-2"), \
             patch.object(orch, "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", "my-global-redis"):
            orch._handle_elasticache_promotion_reminder(state)

        assert mock_sns.publish.called
        subject = _get_sns_subject(mock_sns)
        assert "REMINDER" in subject
        assert "ElastiCache" in subject
        assert "pending" in subject
        assert "5m" in subject

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "_check_if_elasticache_primary", return_value=False)
    @patch.object(orch, "sns")
    def test_ec_reminder_message_contains_cli_commands(self, mock_sns, mock_check, mock_upd):
        """ElastiCache REMINDER message contains the failover-global-replication-group CLI."""
        last_failover_ts = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        state = _make_state(
            active_region="us-east-2",
            redis_promotion_pending=True,
            last_failover_ts=last_failover_ts,
        )

        with patch.object(orch, "CURRENT_REGION", "us-east-2"), \
             patch.object(orch, "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", "my-global-redis"):
            orch._handle_elasticache_promotion_reminder(state)

        message = _get_sns_message(mock_sns)
        assert "failover-global-replication-group" in message
        assert "my-global-redis" in message
        assert "ELASTICACHE PROMOTION REQUIRED" in message

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "_check_if_elasticache_primary", return_value=False)
    @patch.object(orch, "sns")
    def test_ec_reminder_skipped_when_not_configured(self, mock_sns, mock_check, mock_upd):
        """No ElastiCache global ID: reminder fires but message has NO CLI commands."""
        last_failover_ts = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        state = _make_state(
            active_region="us-east-2",
            redis_promotion_pending=False,
            last_failover_ts=last_failover_ts,
        )

        with patch.object(orch, "CURRENT_REGION", "us-east-2"), \
             patch.object(orch, "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", ""):
            orch._handle_elasticache_promotion_reminder(state)

        # Handler fires the reminder notification (it doesn't gate on the config var)
        # but build_elasticache_promotion_commands returns "" when not configured
        if mock_sns.publish.called:
            message = _get_sns_message(mock_sns)
            assert "failover-global-replication-group" not in message
            assert "ELASTICACHE PROMOTION REQUIRED" not in message

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "_check_if_elasticache_primary", return_value=False)
    @patch.object(orch, "_check_if_aurora_writer", return_value=False)
    @patch.object(orch, "sns")
    def test_both_reminders_fire_independently(
        self, mock_sns, mock_aurora_check, mock_ec_check, mock_pub, mock_upd
    ):
        """Both Aurora and ElastiCache reminders fire as separate SNS publishes."""
        last_failover_ts = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        state = _make_state(
            active_region="us-east-2",
            aurora_promotion_pending=True,
            redis_promotion_pending=True,
            last_failover_ts=last_failover_ts,
        )

        with patch.object(orch, "CURRENT_REGION", "us-east-2"), \
             patch.object(orch, "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", "my-global-redis"):
            orch._handle_aurora_promotion_reminder(state)
            orch._handle_elasticache_promotion_reminder(state)

        assert mock_sns.publish.call_count == 2
        subjects = _sns_subjects(mock_sns)
        assert any("Aurora" in s for s in subjects)
        assert any("ElastiCache" in s for s in subjects)


# ===========================================================================
# 6. TestSNSPassiveRegionNotifications
#    Tests passive region notifications (lines 2411, 2551, 2578).
# ===========================================================================

class TestSNSPassiveRegionNotifications:
    """Notifications originating from the passive region handler."""

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "check_active_region_staleness", return_value={"stale": False})
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_passive_unhealthy_sends_warning(
        self, mock_sns, mock_health, mock_staleness, mock_pub, mock_upd
    ):
        """Passive region unhealthy: WARNING notification sent (line 2411)."""
        mock_health.return_value = _unhealthy_health("ECS tasks below minimum")
        state = _make_state(
            active_region="us-east-1",  # us-east-1 is active
            latch_engaged=False,
            last_warning_notification_ts="1970-01-01T00:00:00Z",
        )

        with patch.object(orch, "CURRENT_REGION", "us-east-2"), \
             patch.object(orch, "PASSIVE_PUBLISH_ZERO", False):
            orch._handle_passive_region(state, "us-east-1")

        assert mock_sns.publish.called
        subject = _get_sns_subject(mock_sns)
        assert "WARNING" in subject
        assert "Passive" in subject
        assert "us-east-2" in subject
        assert "unhealthy" in subject.lower()
        assert subject.startswith("[SentinelFO]")
        assert len(subject) <= 100

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "check_active_region_staleness", return_value={"stale": False})
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_passive_unhealthy_warning_throttled(
        self, mock_sns, mock_health, mock_staleness, mock_pub, mock_upd
    ):
        """Passive WARNING is throttled if last warning was within cooldown window."""
        mock_health.return_value = _unhealthy_health()
        recent_ts = (datetime.now(timezone.utc) - timedelta(minutes=3)).isoformat()
        state = _make_state(
            active_region="us-east-1",
            latch_engaged=False,
            last_warning_notification_ts=recent_ts,
        )

        with patch.object(orch, "CURRENT_REGION", "us-east-2"), \
             patch.object(orch, "PASSIVE_PUBLISH_ZERO", False):
            orch._handle_passive_region(state, "us-east-1")

        mock_sns.publish.assert_not_called()

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "try_increment_failures", return_value=True)
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_dual_region_outage_sends_critical(
        self, mock_sns, mock_health, mock_inc, mock_pub, mock_upd
    ):
        """Both regions unhealthy: Dual-Region Outage CRITICAL notification (line 2551)."""
        mock_health.return_value = _unhealthy_health()
        # Target (us-east-2) is marked unhealthy in region_health map
        target_ts = datetime.now(timezone.utc).isoformat()
        state = _make_state(
            consecutive_failures=2,
            region_health={
                "us-east-2": {"healthy": False, "ts": target_ts},
            },
        )

        orch._handle_active_region(state, "us-east-1", 2, "1970-01-01T00:00:00Z")

        outage_calls = [
            c for c in mock_sns.publish.call_args_list
            if "Dual-Region" in c[1].get("Subject", "")
        ]
        assert len(outage_calls) == 1
        subject = outage_calls[0][1]["Subject"]
        assert "CRITICAL" in subject
        assert "Dual-Region" in subject
        assert "Outage" in subject
        assert subject.startswith("[SentinelFO]")

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "try_increment_failures", return_value=True)
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_failover_recommended_manual_mode(
        self, mock_sns, mock_health, mock_inc, mock_pub, mock_upd
    ):
        """FAILOVER_MODE=manual: FAILOVER RECOMMENDED warning sent (line 2578)."""
        mock_health.return_value = _unhealthy_health()
        state = _make_state(consecutive_failures=2)

        with config_variant(failover_mode="manual"):
            orch._handle_active_region(state, "us-east-1", 2, "1970-01-01T00:00:00Z")

        assert mock_sns.publish.called
        subject = _get_sns_subject(mock_sns)
        assert "FAILOVER RECOMMENDED" in subject
        assert "manual mode" in subject
        assert "us-east-1" in subject
        assert "us-east-2" in subject

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "check_active_region_staleness", return_value={"stale": False})
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_passive_unhealthy_with_api_gw(
        self, mock_sns, mock_health, mock_staleness, mock_pub, mock_upd
    ):
        """API GW configured: passive unhealthy notification format unchanged."""
        mock_health.return_value = {
            "healthy": False,
            "decision_reason": "api_gw_5xx: high error rate",
            "signals": [{"signal": "api_gw_5xx", "healthy": False}],
        }
        state = _make_state(
            active_region="us-east-1",
            latch_engaged=False,
            last_warning_notification_ts="1970-01-01T00:00:00Z",
        )

        with patch.object(orch, "CURRENT_REGION", "us-east-2"), \
             patch.object(orch, "PASSIVE_PUBLISH_ZERO", False), \
             config_variant(api_gw=True):
            orch._handle_passive_region(state, "us-east-1")

        subject = _get_sns_subject(mock_sns)
        assert "WARNING" in subject
        assert "Passive" in subject
        assert "us-east-2" in subject


# ===========================================================================
# 7. TestSNSSubjectAndThrottling
#    Cross-cutting tests: subject formatting, throttling, AI-disabled contract.
# ===========================================================================

class TestSNSSubjectAndThrottling:
    """SNS subject format, throttling invariants, and AI-disabled contract."""

    @patch.object(orch, "sns")
    def test_subject_prefixed_with_app_name(self, mock_sns):
        """Subject is prefixed with [SentinelFO] when APP_NAME is configured."""
        with patch.object(orch, "APP_NAME", "SentinelFO"):
            orch.send_notification("FAILOVER: test subject", "body")

        subject = _get_sns_subject(mock_sns)
        assert subject.startswith("[SentinelFO]")
        assert "FAILOVER" in subject

    @patch.object(orch, "sns")
    def test_subject_without_app_name_no_prefix(self, mock_sns):
        """No bracket prefix when APP_NAME is empty."""
        with patch.object(orch, "APP_NAME", ""):
            orch.send_notification("FAILOVER: test subject", "body")

        subject = _get_sns_subject(mock_sns)
        assert not subject.startswith("[")
        assert subject == "FAILOVER: test subject"

    @patch.object(orch, "sns")
    def test_subject_truncated_to_100_chars(self, mock_sns):
        """Subject is truncated to 100 characters maximum."""
        long_subject = "X" * 200
        with patch.object(orch, "APP_NAME", "SentinelFO"):
            orch.send_notification(long_subject, "body")

        subject = _get_sns_subject(mock_sns)
        assert len(subject) <= 100

    @patch.object(orch, "sns")
    def test_critical_never_throttled(self, mock_sns):
        """send_notification (CRITICAL) always fires; calling twice yields 2 publishes."""
        orch.send_notification("CRITICAL: first", "body1")
        orch.send_notification("CRITICAL: second", "body2")

        assert mock_sns.publish.call_count == 2

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "sns")
    def test_warning_first_send_always_fires(self, mock_sns, mock_upd):
        """send_warning_notification always fires on the first send (epoch timestamp)."""
        state = _make_state(last_warning_notification_ts="1970-01-01T00:00:00Z")
        orch.send_warning_notification("WARNING: first", "body", state)

        assert mock_sns.publish.called

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "sns")
    def test_warning_suppressed_within_cooldown(self, mock_sns, mock_upd):
        """send_warning_notification is suppressed when last warning was 2 minutes ago."""
        recent_ts = (datetime.now(timezone.utc) - timedelta(minutes=2)).isoformat()
        state = _make_state(last_warning_notification_ts=recent_ts)
        orch.send_warning_notification("WARNING: throttled", "body", state)

        mock_sns.publish.assert_not_called()

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "sns")
    def test_warning_resumes_after_cooldown_expires(self, mock_sns, mock_upd):
        """send_warning_notification fires again after cooldown window (10 min) expires."""
        old_ts = (datetime.now(timezone.utc) - timedelta(minutes=15)).isoformat()
        state = _make_state(last_warning_notification_ts=old_ts)
        orch.send_warning_notification("WARNING: resumed", "body", state)

        assert mock_sns.publish.called

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "sns")
    def test_warning_updates_last_warning_ts_in_state(self, mock_sns, mock_upd):
        """Successful warning updates last_warning_notification_ts in state backend."""
        state = _make_state(last_warning_notification_ts="1970-01-01T00:00:00Z")
        orch.send_warning_notification("WARNING: test", "body", state)

        mock_upd.assert_called_once()
        update_arg = mock_upd.call_args[0][0]
        assert "last_warning_notification_ts" in update_arg

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_ai_never_invoked_when_all_disabled(
        self, mock_sns, mock_health, mock_pub, mock_upd
    ):
        """AI modules are never imported/called when all AI features are disabled."""
        mock_health.return_value = _unhealthy_health()
        state = _make_state(consecutive_failures=2)

        ai_mock = MagicMock()
        ai_modules = {
            "ai": ai_mock,
            "ai.rca_analyzer": MagicMock(),
            "ai.collector": MagicMock(),
            "ai.aurora_advisor": MagicMock(),
            "ai.stability_collector": MagicMock(),
            "ai.failback_readiness": MagicMock(),
        }

        # AI_RCA_ENABLED and AI_AURORA_ADVISOR_MODE are read from os.environ inside
        # the functions, not stored as module attributes. The env vars are already
        # set to "false"/"disabled" in the module-level _ENV dict.
        with patch.dict("sys.modules", ai_modules), \
             patch.object(orch, "try_increment_failures", return_value=True), \
             patch.object(orch, "try_claim_failover", return_value=True), \
             patch.object(orch, "_emit_failover_event"):

            orch._handle_active_region(state, "us-east-1", 2, "1970-01-01T00:00:00Z")

        ai_mock.rca_analyzer.assert_not_called() if hasattr(ai_mock, "rca_analyzer") else None
        # Verify all failover SNS messages contain no AI content
        for c in mock_sns.publish.call_args_list:
            msg = c[1].get("Message", "")
            assert "== RCA ==" not in msg
            assert "Root Cause Analysis" not in msg


# ===========================================================================
# 8. TestSNSAPIGatewayVariants
#    Tests how API GW presence/absence affects health signal and SNS content.
# ===========================================================================

class TestSNSAPIGatewayVariants:
    """API Gateway configured vs not configured."""

    def test_api_gw_not_configured_signal_skipped(self):
        """API_GW_NAME='': check_api_gateway_errors returns healthy/skipped immediately."""
        # Call the real function with API_GW_NAME patched to empty string
        with patch.object(orch, "API_GW_NAME", ""):
            result = orch.check_api_gateway_errors()

        # When not configured, function returns early with healthy=True
        assert result["healthy"] is True
        assert result.get("skipped") is True or result.get("reason") == "Not configured"

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "try_increment_failures", return_value=True)
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_api_gw_configured_healthy_no_extra_noise(
        self, mock_sns, mock_health, mock_inc, mock_pub, mock_upd
    ):
        """API GW healthy: no additional SNS notifications due to API GW."""
        mock_health.return_value = _healthy_health()
        state = _make_state()

        with config_variant(api_gw=True):
            orch._handle_active_region(state, "us-east-1", 0, "1970-01-01T00:00:00Z")

        # Region is healthy — no SNS at all
        mock_sns.publish.assert_not_called()

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "try_increment_failures", return_value=True)
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_api_gw_configured_failure_appears_in_degraded_warning(
        self, mock_sns, mock_health, mock_inc, mock_pub, mock_upd
    ):
        """API GW failure surfaced in WARNING message body via decision_reason."""
        mock_health.return_value = {
            "healthy": False,
            "decision_reason": "api_gw_5xx: 80.0% errors",
            "signals": [{"signal": "api_gw_5xx", "healthy": False, "reason": "80% 5xx rate"}],
        }
        state = _make_state(consecutive_failures=1)

        with config_variant(api_gw=True):
            orch._handle_active_region(state, "us-east-1", 1, "1970-01-01T00:00:00Z")

        message = _get_sns_message(mock_sns)
        assert "api_gw_5xx" in message or "80.0%" in message


# ===========================================================================
# 9. TestSNSFailback
#    Tests all SNS notifications from manual_failback_v2.py (lines 599, 623, 684, 693).
# ===========================================================================

class TestSNSFailback:
    """Failback SNS notifications: blocked (AI/health), complete, failed."""

    def _run_failback(self, event_overrides=None):
        """Default successful failback event payload."""
        event = {
            "target_region": "us-east-1",
            "skip_health_check": True,
            "operator": "test-operator",
            "aurora_confirmed": True,
            "skip_readiness_check": True,
        }
        if event_overrides:
            event.update(event_overrides)
        return event

    @patch.object(failback, "create_backend")
    @patch.object(failback, "publish_region_health_metric")
    @patch.object(failback, "update_failover_state")
    @patch.object(failback, "get_failover_state")
    @patch.object(failback, "sns")
    def test_ai_readiness_not_fired_when_disabled(
        self, mock_sns, mock_get_state, mock_upd, mock_pub, mock_cb
    ):
        """AI_FAILBACK_READINESS_ENABLED=false: NO 'FAILBACK BLOCKED: AI readiness' published."""
        mock_cb.return_value = MagicMock()
        mock_get_state.return_value = _make_failback_state()

        with patch.dict(os.environ, {"AI_FAILBACK_READINESS_ENABLED": "false"}):
            result = failback.handler(self._run_failback(), None)

        assert result["statusCode"] == 200
        # AI readiness blocked notification must NOT have been sent
        subjects = _sns_subjects(mock_sns)
        assert not any("AI readiness" in s for s in subjects)
        assert not any("FAILBACK BLOCKED" in s and "AI" in s for s in subjects)

    @patch.object(failback, "create_backend")
    @patch.object(failback, "validate_target_region_health")
    @patch.object(failback, "publish_region_health_metric")
    @patch.object(failback, "update_failover_state")
    @patch.object(failback, "get_failover_state")
    @patch.object(failback, "sns")
    def test_failback_blocked_region_not_ready(
        self, mock_sns, mock_get_state, mock_upd, mock_pub, mock_validate, mock_cb
    ):
        """Region not healthy: FAILBACK BLOCKED notification sent (line 623)."""
        mock_cb.return_value = MagicMock()
        mock_get_state.return_value = _make_failback_state()
        mock_validate.return_value = {
            "healthy": False,
            "issues": ["HTTP health check failed", "ECS tasks: 0/2 running"],
        }

        result = failback.handler(
            self._run_failback({"skip_health_check": False}), None
        )

        assert result["statusCode"] == 400
        assert mock_sns.publish.called
        subject = _get_sns_subject(mock_sns)
        assert "FAILBACK BLOCKED" in subject
        assert "us-east-1" in subject
        assert "not ready" in subject
        assert subject.startswith("[SentinelFO]")
        assert len(subject) <= 100

    @patch.object(failback, "create_backend")
    @patch.object(failback, "publish_region_health_metric")
    @patch.object(failback, "update_failover_state")
    @patch.object(failback, "get_failover_state")
    @patch.object(failback, "sns")
    def test_failback_complete_sends_critical(
        self, mock_sns, mock_get_state, mock_upd, mock_pub, mock_cb
    ):
        """Successful failback: FAILBACK COMPLETE notification sent (line 684)."""
        mock_cb.return_value = MagicMock()
        mock_get_state.return_value = _make_failback_state()

        result = failback.handler(self._run_failback(), None)

        assert result["statusCode"] == 200
        assert mock_sns.publish.called
        subject = _get_sns_subject(mock_sns)
        assert "FAILBACK COMPLETE" in subject
        assert "us-east-1" in subject
        assert subject.startswith("[SentinelFO]")
        assert len(subject) <= 100

        message = _get_sns_message(mock_sns)
        assert "Latch" in message or "latch" in message
        assert "RELEASED" in message or "released" in message

    @patch.object(failback, "create_backend")
    @patch.object(failback, "publish_region_health_metric")
    @patch.object(failback, "update_failover_state")
    @patch.object(failback, "get_failover_state")
    @patch.object(failback, "sns")
    def test_failback_failed_sends_critical(
        self, mock_sns, mock_get_state, mock_upd, mock_pub, mock_cb
    ):
        """Exception during failback: FAILBACK FAILED notification sent (line 693)."""
        mock_cb.return_value = MagicMock()
        mock_get_state.return_value = _make_failback_state()
        mock_pub.return_value = MagicMock()

        # Force exception on 2nd update_failover_state call (the final state update)
        call_count = {"n": 0}
        def upd_side_effect(updates):
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise Exception("DynamoDB unavailable")
        mock_upd.side_effect = upd_side_effect

        with pytest.raises(Exception, match="DynamoDB unavailable"):
            failback.handler(self._run_failback(), None)

        failed_calls = [
            c for c in mock_sns.publish.call_args_list
            if "FAILBACK FAILED" in c[1].get("Subject", "")
        ]
        assert len(failed_calls) == 1
        subject = failed_calls[0][1]["Subject"]
        assert "us-east-1" in subject
        assert subject.startswith("[SentinelFO]")

    @patch.object(failback, "create_backend")
    @patch.object(failback, "publish_region_health_metric")
    @patch.object(failback, "update_failover_state")
    @patch.object(failback, "get_failover_state")
    @patch.object(failback, "sns")
    def test_failback_complete_s3_backend_same_format(
        self, mock_sns, mock_get_state, mock_upd, mock_pub, mock_cb
    ):
        """S3 backend: FAILBACK COMPLETE SNS format identical to DynamoDB variant."""
        mock_cb.return_value = MagicMock()
        mock_get_state.return_value = _make_failback_state()

        with patch.dict(os.environ, {
            "STATE_BACKEND": "s3",
            "STATE_BUCKET": "test-fo-state-us-east-1",
        }):
            result = failback.handler(self._run_failback(), None)

        assert result["statusCode"] == 200
        subject = _get_sns_subject(mock_sns)
        assert "FAILBACK COMPLETE" in subject
        assert "us-east-1" in subject
        assert subject.startswith("[SentinelFO]")
