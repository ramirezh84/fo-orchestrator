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
    "APP_NAME": "Vigil",
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
# Autouse fixture: ensure APP_NAME is "Vigil" for every test in this file.
# Other test files set APP_NAME="" via os.environ.setdefault, which silently
# wins when they run first. Patch the module attribute directly so our tests
# are isolated from load-order effects.
# ===========================================================================

@pytest.fixture(autouse=True)
def patch_app_name():
    with patch.object(orch, "APP_NAME", "Vigil"), \
         patch.object(failback, "APP_NAME", "Vigil"):
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
    """Apply per-test module-level patches for config variants.

    PR3c: also patches os.environ since detect_data_tier_config() now reads
    env directly. Both module-attr and env patches are kept in lockstep so
    the orchestrator's lazy-read paths (config_aware notifications) and
    eager-read paths (existing imports) see consistent values.
    """
    redis_global = "my-global-redis" if elasticache else ""
    redis_local = "my-redis-rg" if elasticache else ""
    apigw = "my-api-gw-id" if api_gw else ""
    env_patch = {
        "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID": redis_global,
        "ELASTICACHE_REPLICATION_GROUP_ID": redis_local,
        "API_GW_NAME": apigw,
        "ROUTING_MODE": routing,
        "FAILOVER_MODE": failover_mode,
    }
    patches = [
        patch.object(orch, "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", redis_global),
        patch.object(orch, "ELASTICACHE_REPLICATION_GROUP_ID", redis_local),
        patch.object(orch, "API_GW_NAME", apigw),
        patch.object(orch, "ROUTING_MODE", routing),
        patch.object(orch, "FAILOVER_MODE", failover_mode),
        patch.dict(os.environ, env_patch),
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
        assert subject.startswith("[Vigil] WARNING:")
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
        """v1.6: Active/active recovery → INFO with rejoin journey."""
        mock_health.return_value = _healthy_health()
        state = _make_state(consecutive_failures=3)

        with config_variant(routing="active-active"):
            orch._handle_active_active(state)

        assert mock_sns.publish.called
        subject = _get_sns_subject(mock_sns)
        assert subject.startswith("[Vigil] INFO:")
        assert "us-east-1 recovered" in subject
        assert "rejoining traffic pool" in subject
        assert len(subject) <= 100

        body = _get_sns_message(mock_sns)
        assert "[✓] Rejoined Route 53 traffic pool" in body

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
        """v1.6: Active/active degradation → WARNING with active-active framing."""
        mock_health.return_value = _unhealthy_health()
        state = _make_state(consecutive_failures=1)

        with config_variant(routing="active-active"):
            orch._handle_active_active(state)

        subject = _get_sns_subject(mock_sns)
        assert subject.startswith("[Vigil] WARNING:")
        assert "(active-active)" in subject
        assert "2 of 3" in subject

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
        """Common helper: set up mocks and trigger failover in _handle_active_region.

        PR3c: pins AURORA_CLUSTER_ID in os.environ so detect_data_tier_config()
        sees Aurora as present. Other test files may have setdefault-ed
        AURORA_CLUSTER_ID="" before this file's _ENV ran, so we have to patch
        explicitly here rather than relying on module-load env state.
        """
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
            # Pin Aurora env explicitly — _ENV setdefault doesn't override
            # values another test file already set during pytest's collection
            # phase, so we have to be explicit here.
            stack.enter_context(patch.dict(os.environ, {
                "AURORA_CLUSTER_ID": "my-aurora-w1",
                "AURORA_AUTO_PROMOTE": "false",
            }))
            stack.enter_context(config_variant(elasticache=elasticache))

            orch._handle_active_region(state, "us-east-1", 2, "1970-01-01T00:00:00Z")

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_failover_s3_ec_includes_elasticache_commands(
        self, mock_sns, mock_health, mock_pub, mock_upd
    ):
        """S3+ElastiCache, AURORA_AUTO_PROMOTE=false: v1.6 manual data-tier email
        embeds Redis failover-global CLI command in the WHAT TO DO NEXT section."""
        self._run_failover(mock_sns, mock_health, mock_pub, mock_upd, elasticache=True)

        assert mock_sns.publish.called
        # The v1.6 manual path uses "operator action required to promote data tier".
        failover_calls = [
            c for c in mock_sns.publish.call_args_list
            if "promote data tier" in c[1].get("Subject", "")
        ]
        assert len(failover_calls) == 1
        subject = failover_calls[-1][1]["Subject"]
        message = failover_calls[-1][1]["Message"]

        assert subject.startswith("[Vigil] CRITICAL:")
        assert "us-east-2" in subject
        assert "data tier" in subject.lower()
        assert len(subject) <= 100

        # Body has the structured shape; CLI commands embedded in next-step.
        assert "WHAT TO DO NEXT" in message
        assert "failover-global-replication-group" in message
        assert "my-global-redis" in message
        # Journey distinguishes Aurora vs Redis manual steps.
        assert "Aurora — operator action required" in message
        assert "Redis — operator action required" in message

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
        """Without ElastiCache (C2 config): v1.6 message has no Redis content."""
        self._run_failover(mock_sns, mock_health, mock_pub, mock_upd, elasticache=False)

        failover_calls = [
            c for c in mock_sns.publish.call_args_list
            if "promote data tier" in c[1].get("Subject", "")
        ]
        assert len(failover_calls) == 1
        message = failover_calls[-1][1]["Message"]

        # No ElastiCache in body; no Redis row in journey.
        assert "failover-global-replication-group" not in message
        assert "Redis" not in message
        # Aurora commands and journey row still present.
        assert "switchover-global-cluster" in message or "aurora" in message.lower()
        assert "Aurora — operator action required" in message

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
        assert subject.startswith("[Vigil] CRITICAL:")
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
            if "promote data tier" in c[1].get("Subject", "")
        ]
        assert len(failover_calls) == 1
        message = failover_calls[-1][1]["Message"]
        # API GW decision reason surfaced in message body (Decision context line).
        assert "api_gw_5xx" in message or "75.0%" in message

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_failover_ddb_same_sns_format(
        self, mock_sns, mock_health, mock_pub, mock_upd
    ):
        """DynamoDB backend: v1.6 manual data-tier subject identical to S3 (state
        backend doesn't affect notification template)."""
        self._run_failover(mock_sns, mock_health, mock_pub, mock_upd)

        failover_calls = [
            c for c in mock_sns.publish.call_args_list
            if "promote data tier" in c[1].get("Subject", "")
        ]
        assert len(failover_calls) == 1
        subject = failover_calls[-1][1]["Subject"]
        assert subject.startswith("[Vigil] CRITICAL:")
        assert "data tier" in subject.lower()

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

        # v1.6: subject is "Auto-failover FAILED — X → Y did not complete"
        failed_calls = [
            c for c in mock_sns.publish.call_args_list
            if "Auto-failover FAILED" in c[1].get("Subject", "")
        ]
        assert len(failed_calls) == 1
        subject = failed_calls[0][1]["Subject"]
        assert "us-east-1" in subject
        assert "us-east-2" in subject

        # Body explains the state was reverted and what to investigate.
        body = failed_calls[0][1]["Message"]
        assert "[✗] Failover handler FAILED" in body
        assert "state reverted" in body.lower()


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
        assert subject.startswith("[Vigil] INFO:")
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
        """v1.6: Aurora promotion reminder fires every N min while pending."""
        # 5 minutes elapsed → int(5) % 5 == 0 → fires
        last_failover_ts = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        state = _make_state(
            active_region="us-east-2",
            state="WAITING_AURORA_PROMOTION",
            aurora_promotion_pending=True,
            last_failover_ts=last_failover_ts,
        )

        # Pin Aurora env so build_aurora_promotion_commands renders an actual
        # CLI rather than "no commands available". _ENV's setdefault may have
        # been beaten by another file's import-time env priming.
        env_overrides = {
            "AURORA_GLOBAL_CLUSTER_ID": "my-aurora-global",
            "AURORA_CLUSTER_ID": "my-aurora-w1",
            "TARGET_AURORA_CLUSTER_ID": "my-aurora-w2",
        }
        with patch.object(orch, "CURRENT_REGION", "us-east-2"), \
             patch.dict(os.environ, env_overrides), \
             patch.object(orch, "AURORA_GLOBAL_CLUSTER_ID", "my-aurora-global"), \
             patch.object(orch, "AURORA_CLUSTER_ID", "my-aurora-w1"), \
             patch.object(orch, "TARGET_AURORA_CLUSTER_ID", "my-aurora-w2"):
            orch._handle_aurora_promotion_reminder(state)

        assert mock_sns.publish.called
        subject = _get_sns_subject(mock_sns)
        assert subject.startswith("[Vigil] CRITICAL:")
        assert "Aurora promotion still pending" in subject
        assert "5 min" in subject
        assert "operator action required" in subject

        body = _get_sns_message(mock_sns)
        # Body explains DB writes are blocked + embeds the actual CLI command.
        assert "CANNOT WRITE" in body
        assert "switchover-global-cluster" in body
        # Journey shows we're stuck waiting on operator.
        assert "[→] Aurora — operator action required" in body

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
        assert subject.startswith("[Vigil] INFO:")
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
        assert subject.startswith("[Vigil] CRITICAL:")
        assert "ElastiCache promotion still pending" in subject
        assert "5 min" in subject
        assert "operator action required" in subject

        body = _get_sns_message(mock_sns)
        # Body explains writes go cross-region + embeds CLI.
        assert "CANNOT WRITE" in body
        assert "failover-global-replication-group" in body
        assert "[→] Redis — operator action required" in body

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
        """v1.6: Passive region unhealthy → WARNING explaining the failover safety
        net is gone. Subject says 'Standby region X is unhealthy — failover would
        be unsafe'."""
        mock_health.return_value = _unhealthy_health("ECS tasks below minimum")
        state = _make_state(
            active_region="us-east-1",
            latch_engaged=False,
            last_warning_notification_ts="1970-01-01T00:00:00Z",
        )

        with patch.object(orch, "CURRENT_REGION", "us-east-2"), \
             patch.object(orch, "PASSIVE_PUBLISH_ZERO", False):
            orch._handle_passive_region(state, "us-east-1")

        assert mock_sns.publish.called
        subject = _get_sns_subject(mock_sns)
        assert subject.startswith("[Vigil] WARNING:")
        assert "Standby region us-east-2 is unhealthy" in subject
        assert "failover would be unsafe" in subject.lower()
        assert len(subject) <= 100

        body = _get_sns_message(mock_sns)
        assert "[⚠] Standby region us-east-2: UNHEALTHY" in body
        assert "Failover safety net DOWN" in body

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
        """v1.6: Both regions unhealthy → CRITICAL with 'app is DOWN' framing,
        explicit 'NOT a failover candidate' next-step, and the on-call escalation."""
        mock_health.return_value = _unhealthy_health()
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
            if "Dual-region" in c[1].get("Subject", "")
        ]
        assert len(outage_calls) == 1
        subject = outage_calls[0][1]["Subject"]
        body = outage_calls[0][1]["Message"]
        assert subject.startswith("[Vigil] CRITICAL:")
        assert "Dual-region outage" in subject
        assert "DOWN" in subject

        # Body explicitly tells the operator NOT to failover.
        assert "NOT a failover candidate" in body
        assert "Page the on-call DBA and SRE" in body
        # Journey makes the halt visible.
        assert "[⏸] Failover HALTED" in body

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "try_increment_failures", return_value=True)
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "sns")
    def test_failover_recommended_manual_mode(
        self, mock_sns, mock_health, mock_inc, mock_pub, mock_upd
    ):
        """v1.6: FAILOVER_MODE=manual sends WARNING explaining the threshold was
        reached but DNS was NOT moved; body embeds the exact override Lambda invoke."""
        mock_health.return_value = _unhealthy_health()
        state = _make_state(consecutive_failures=2)

        with config_variant(failover_mode="manual"):
            orch._handle_active_region(state, "us-east-1", 2, "1970-01-01T00:00:00Z")

        assert mock_sns.publish.called
        subject = _get_sns_subject(mock_sns)
        assert subject.startswith("[Vigil] WARNING:")
        assert "RECOMMENDED" in subject
        assert "FAILOVER_MODE is manual" in subject

        body = _get_sns_message(mock_sns)
        # Override command embedded in next-step.
        assert "execute_failover" in body
        assert "lambda invoke" in body
        # Journey shows we're awaiting operator decision.
        assert "[⏸] Awaiting operator (manual mode)" in body

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
        assert subject.startswith("[Vigil] WARNING:")
        assert "Standby region us-east-2 is unhealthy" in subject


# ===========================================================================
# 7. TestSNSSubjectAndThrottling
#    Cross-cutting tests: subject formatting, throttling, AI-disabled contract.
# ===========================================================================

class TestSNSSubjectAndThrottling:
    """SNS subject format, throttling invariants, and AI-disabled contract."""

    @patch.object(orch, "sns")
    def test_subject_prefixed_with_app_name(self, mock_sns):
        """Subject is prefixed with [Vigil] when APP_NAME is configured."""
        with patch.object(orch, "APP_NAME", "Vigil"):
            orch.send_notification("FAILOVER: test subject", "body")

        subject = _get_sns_subject(mock_sns)
        assert subject.startswith("[Vigil]")
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
        with patch.object(orch, "APP_NAME", "Vigil"):
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
        """v1.6: Failback blocked when target region health gate fails. Subject says
        'BLOCKED — target region X is not ready'; body lists every issue and tells
        the operator the override flag."""
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
        assert subject.startswith("[Vigil] CRITICAL:")
        assert "Failback BLOCKED" in subject
        assert "us-east-1 is not ready" in subject
        assert len(subject) <= 100

        body = _get_sns_message(mock_sns)
        # Both issues are listed in the CONTEXT block.
        assert "HTTP health check failed" in body
        assert "ECS tasks: 0/2 running" in body
        # Operator gets the explicit override path in WHAT TO DO NEXT.
        assert "skip_health_check=true" in body
        # Journey shows we failed at the health gate, before Aurora switchover.
        assert "[✗] Target region health gate" in body

    @patch.object(failback, "create_backend")
    @patch.object(failback, "publish_region_health_metric")
    @patch.object(failback, "update_failover_state")
    @patch.object(failback, "get_failover_state")
    @patch.object(failback, "sns")
    def test_failback_complete_sends_critical(
        self, mock_sns, mock_get_state, mock_upd, mock_pub, mock_cb
    ):
        """v1.6: Successful failback emits CRITICAL completion email with full
        journey breadcrumb showing every recovery step is done."""
        mock_cb.return_value = MagicMock()
        mock_get_state.return_value = _make_failback_state()

        result = failback.handler(self._run_failback(), None)

        assert result["statusCode"] == 200
        assert mock_sns.publish.called
        subject = _get_sns_subject(mock_sns)
        assert subject.startswith("[Vigil] CRITICAL:")
        assert "Failback COMPLETE" in subject
        assert "us-east-1" in subject
        assert len(subject) <= 100

        body = _get_sns_message(mock_sns)
        # Latch released and full journey arc shown.
        assert "Latch" in body and "RELEASED" in body
        assert "[✓] Latch released" in body
        # Next-step tells operator what to watch for after recovery.
        assert "Confirm Route 53" in body or "confirm route 53" in body.lower()

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
            if "Failback FAILED" in c[1].get("Subject", "")
        ]
        assert len(failed_calls) == 1
        subject = failed_calls[0][1]["Subject"]
        assert "us-east-1" in subject
        assert subject.startswith("[Vigil] CRITICAL:")
        body = failed_calls[0][1]["Message"]
        # Body explains the failure happened AFTER the health gate passed,
        # so the operator knows where to focus debugging.
        assert "[✓] Health gate passed" in body
        assert "[✗] DNS/state update — FAILED" in body

    @patch.object(failback, "create_backend")
    @patch.object(failback, "publish_region_health_metric")
    @patch.object(failback, "update_failover_state")
    @patch.object(failback, "get_failover_state")
    @patch.object(failback, "sns")
    def test_failback_complete_s3_backend_same_format(
        self, mock_sns, mock_get_state, mock_upd, mock_pub, mock_cb
    ):
        """S3 backend: failback complete subject identical to DynamoDB variant
        (state backend is invisible to the notification template)."""
        mock_cb.return_value = MagicMock()
        mock_get_state.return_value = _make_failback_state()

        with patch.dict(os.environ, {
            "STATE_BACKEND": "s3",
            "STATE_BUCKET": "test-fo-state-us-east-1",
        }):
            result = failback.handler(self._run_failback(), None)

        assert result["statusCode"] == 200
        subject = _get_sns_subject(mock_sns)
        assert subject.startswith("[Vigil] CRITICAL:")
        assert "Failback COMPLETE" in subject
        assert "us-east-1" in subject


# ===========================================================================
# 9. TestSNSFailbackRedisGate (PR5 / F5)
#    Tests the new Redis-confirmed / auto-failover gates added in v1.6.
# ===========================================================================

class TestSNSFailbackRedisGate:
    """v1.6 PR5: failback Lambda's config-aware Redis gate.

    Mirror of the Aurora gate. Required when ElastiCache is configured;
    skipped entirely when it isn't (C1/C2/C3 stacks).
    """

    # C5 (Aurora manual + Redis manual): require BOTH _confirmed flags
    _C5_ENV = {
        "AURORA_CLUSTER_ID": "my-aurora-w1",
        "AURORA_AUTO_PROMOTE": "false",
        "AURORA_GLOBAL_CLUSTER_ID": "my-aurora-global",
        "TARGET_AURORA_CLUSTER_ID": "my-aurora-w2",
        "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID": "my-global-redis",
        "ELASTICACHE_REPLICATION_GROUP_ID": "my-redis-rg",
        "ELASTICACHE_AUTO_PROMOTE": "false",
    }

    # C9 (Aurora auto + Redis auto): no _confirmed flags needed
    _C9_ENV = {
        **_C5_ENV,
        "AURORA_AUTO_PROMOTE": "true",
        "ELASTICACHE_AUTO_PROMOTE": "true",
    }

    @patch.object(failback, "create_backend")
    @patch.object(failback, "publish_region_health_metric")
    @patch.object(failback, "update_failover_state")
    @patch.object(failback, "get_failover_state")
    @patch.object(failback, "sns")
    def test_redis_gate_rejects_when_redis_not_confirmed(
        self, mock_sns, mock_get_state, mock_upd, mock_pub, mock_cb
    ):
        """C5 (both manual): aurora_confirmed=true alone is NOT enough — failback
        must reject with the Redis failover commands until redis_confirmed=true."""
        mock_cb.return_value = MagicMock()
        mock_get_state.return_value = _make_failback_state()

        with patch.dict(os.environ, self._C5_ENV):
            result = failback.handler({
                "target_region": "us-east-1",
                "operator": "test",
                "aurora_confirmed": True,
                "skip_health_check": True,
                "skip_readiness_check": True,
                # redis_confirmed missing → should reject
            }, None)

        assert result["statusCode"] == 400
        body = result["body"]
        assert "ElastiCache failover has NOT been confirmed" in body
        assert "failover-global-replication-group" in body
        assert "redis_confirmed=true" in body or "skip_redis_check=true" in body

    @patch.object(failback, "create_backend")
    @patch.object(failback, "publish_region_health_metric")
    @patch.object(failback, "update_failover_state")
    @patch.object(failback, "get_failover_state")
    @patch.object(failback, "sns")
    def test_redis_gate_passes_when_redis_confirmed(
        self, mock_sns, mock_get_state, mock_upd, mock_pub, mock_cb
    ):
        """C5: aurora_confirmed=true + redis_confirmed=true → failback proceeds."""
        mock_cb.return_value = MagicMock()
        mock_get_state.return_value = _make_failback_state()

        with patch.dict(os.environ, self._C5_ENV):
            result = failback.handler({
                "target_region": "us-east-1",
                "operator": "test",
                "aurora_confirmed": True,
                "redis_confirmed": True,
                "skip_health_check": True,
                "skip_readiness_check": True,
            }, None)

        assert result["statusCode"] == 200
        subject = _get_sns_subject(mock_sns)
        assert "Failback COMPLETE" in subject

    @patch.object(failback, "create_backend")
    @patch.object(failback, "publish_region_health_metric")
    @patch.object(failback, "update_failover_state")
    @patch.object(failback, "get_failover_state")
    @patch.object(failback, "sns")
    def test_skip_redis_check_break_glass(
        self, mock_sns, mock_get_state, mock_upd, mock_pub, mock_cb
    ):
        """skip_redis_check=true bypasses the Redis gate even without redis_confirmed.

        Use case: operator has manually verified Redis is in target_region but
        doesn't want to bother with the explicit confirmation flag (e.g., Redis
        was already in target_region from the start of the incident)."""
        mock_cb.return_value = MagicMock()
        mock_get_state.return_value = _make_failback_state()

        with patch.dict(os.environ, self._C5_ENV):
            result = failback.handler({
                "target_region": "us-east-1",
                "operator": "test",
                "aurora_confirmed": True,
                "skip_redis_check": True,  # break-glass
                "skip_health_check": True,
                "skip_readiness_check": True,
            }, None)

        assert result["statusCode"] == 200

    @patch.object(failback, "_auto_failover_redis")
    @patch.object(failback, "_auto_switchover_aurora")
    @patch.object(failback, "create_backend")
    @patch.object(failback, "publish_region_health_metric")
    @patch.object(failback, "update_failover_state")
    @patch.object(failback, "get_failover_state")
    @patch.object(failback, "sns")
    def test_c9_lambda_does_both_data_tier_actions_itself(
        self, mock_sns, mock_get_state, mock_upd, mock_pub, mock_cb,
        mock_aurora, mock_redis,
    ):
        """C9 (both auto): no _confirmed flags needed. Lambda invokes the
        switchover-global-cluster + failover-global-replication-group APIs
        itself."""
        mock_cb.return_value = MagicMock()
        mock_get_state.return_value = _make_failback_state()
        mock_aurora.return_value = {"success": True, "error": ""}
        mock_redis.return_value = {"success": True, "error": ""}

        with patch.dict(os.environ, self._C9_ENV):
            result = failback.handler({
                "target_region": "us-east-1",
                "operator": "test",
                "skip_health_check": True,
                "skip_readiness_check": True,
                # No aurora_confirmed or redis_confirmed — Lambda handles both.
            }, None)

        assert result["statusCode"] == 200
        # Lambda made both API calls itself.
        mock_aurora.assert_called_once_with("us-east-1")
        mock_redis.assert_called_once_with("us-east-1")

    @patch.object(failback, "_auto_failover_redis")
    @patch.object(failback, "_auto_switchover_aurora")
    @patch.object(failback, "create_backend")
    @patch.object(failback, "publish_region_health_metric")
    @patch.object(failback, "update_failover_state")
    @patch.object(failback, "get_failover_state")
    @patch.object(failback, "sns")
    def test_c9_redis_auto_failure_rejects_with_500(
        self, mock_sns, mock_get_state, mock_upd, mock_pub, mock_cb,
        mock_aurora, mock_redis,
    ):
        """C9: when the Redis auto-failover API call fails, Lambda must reject
        with a 500 status code rather than silently complete the failback —
        otherwise the F5 problem (Redis split-brain after failback) returns."""
        mock_cb.return_value = MagicMock()
        mock_get_state.return_value = _make_failback_state()
        mock_aurora.return_value = {"success": True, "error": ""}
        mock_redis.return_value = {"success": False, "error": "API timeout"}

        with patch.dict(os.environ, self._C9_ENV):
            result = failback.handler({
                "target_region": "us-east-1",
                "operator": "test",
                "skip_health_check": True,
                "skip_readiness_check": True,
            }, None)

        assert result["statusCode"] == 500
        assert "ElastiCache auto-failover FAILED" in result["body"]
        # Operator gets the manual override path in the rejection body.
        assert "redis_confirmed=true" in result["body"] or "skip_redis_check=true" in result["body"]

    @patch.object(failback, "create_backend")
    @patch.object(failback, "publish_region_health_metric")
    @patch.object(failback, "update_failover_state")
    @patch.object(failback, "get_failover_state")
    @patch.object(failback, "sns")
    def test_c1_no_data_tier_no_gates(
        self, mock_sns, mock_get_state, mock_upd, mock_pub, mock_cb
    ):
        """C1 (no Aurora, no Redis): app-only stack. Failback should succeed
        without ANY confirmation flags — there's nothing to confirm."""
        mock_cb.return_value = MagicMock()
        mock_get_state.return_value = _make_failback_state()

        with patch.dict(os.environ, {
            "AURORA_CLUSTER_ID": "",
            "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID": "",
        }):
            result = failback.handler({
                "target_region": "us-east-1",
                "operator": "test",
                "skip_health_check": True,
                "skip_readiness_check": True,
            }, None)

        assert result["statusCode"] == 200
