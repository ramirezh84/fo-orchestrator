#!/usr/bin/env python3
"""
Regression tests for failover_orchestrator_v3.py.

Covers: health evaluation, individual health checks, consecutive failure counting,
cooldown window, latch mechanism, failover trigger (auto/manual), notification
throttling, RCA integration, and Lambda handler routing.

Run: python3 -m pytest tests/test_orchestrator.py -v
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional
from unittest.mock import MagicMock, patch, ANY, call

import pytest

# ---------------------------------------------------------------------------
# Environment setup — must happen BEFORE importing the orchestrator module
# because it reads env vars at module level (SNS_TOPIC_ARN is required).
# ---------------------------------------------------------------------------
_ENV = {
    "SNS_TOPIC_ARN": "arn:aws:sns:us-east-1:123456789012:failover-alerts",
    "AWS_REGION": "us-east-1",
    "PRIMARY_REGION": "us-east-1",
    "SECONDARY_REGION": "us-east-2",
    "STATE_BACKEND": "dynamodb",
    "STATE_TABLE": "failover-state",
    "HEALTH_CHECK_URL": "",
    "ECS_CLUSTER_NAME": "",
    "ECS_SERVICE_NAME": "",
    "ALB_ARN_SUFFIX": "",
    "TG_ARN_SUFFIX": "",
    "API_GW_NAME": "",
    "AURORA_CLUSTER_ID": "",
    "AURORA_GLOBAL_CLUSTER_ID": "",
    "FAILOVER_MODE": "auto",
    "ROUTING_MODE": "failover",
    "COOLDOWN_MINUTES": "30",
    "CONSECUTIVE_FAILURES_THRESHOLD": "3",
    "AI_RCA_ENABLED": "false",
    "PASSIVE_PUBLISH_ZERO": "false",
    "WARNING_NOTIFICATION_COOLDOWN_MINUTES": "10",
    "APP_NAME": "",
}

for k, v in _ENV.items():
    os.environ.setdefault(k, v)


# ---------------------------------------------------------------------------
# Mock boto3 and the state backend at module level so the orchestrator's
# top-level client creation does not make real AWS calls.
# ---------------------------------------------------------------------------
_mock_boto3_patcher = patch("boto3.client")
_mock_boto3_client = _mock_boto3_patcher.start()
_mock_boto3_client.return_value = MagicMock()

_mock_create_backend_patcher = patch("state_backend.create_backend")
_mock_create_backend = _mock_create_backend_patcher.start()
_mock_state_backend = MagicMock()
_mock_create_backend.return_value = _mock_state_backend

# Now safe to import
import failover_orchestrator_v3 as orch

# Stop the import-time patchers — per-test patches take over from here
_mock_boto3_patcher.stop()
_mock_create_backend_patcher.stop()


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
) -> dict:
    """Build a state dict with sensible defaults."""
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
    }


def _healthy_signal(signal_name: str) -> dict:
    return {"signal": signal_name, "healthy": True, "reason": "ok"}


def _unhealthy_signal(signal_name: str) -> dict:
    return {"signal": signal_name, "healthy": False, "reason": "failed"}


def _skipped_signal(signal_name: str) -> dict:
    return {"signal": signal_name, "healthy": True, "reason": "Not configured", "skipped": True}


# ===========================================================================
# 1. Health Evaluation — evaluate_region_health() quorum logic, HTTP bypass
# ===========================================================================

class TestEvaluateRegionHealth:
    """Tests for the aggregate health evaluation quorum logic."""

    @patch.object(orch, "check_aurora_cluster_status", return_value=_healthy_signal("aurora_status"))
    @patch.object(orch, "check_api_gateway_errors", return_value=_healthy_signal("api_gw_5xx"))
    @patch.object(orch, "check_ecs_running_tasks", return_value=_healthy_signal("ecs_running_tasks"))
    @patch.object(orch, "check_alb_healthy_hosts", return_value=_healthy_signal("alb_healthy_hosts"))
    @patch.object(orch, "check_http_health", return_value=_healthy_signal("http_health"))
    def test_all_healthy(self, mock_http, mock_alb, mock_ecs, mock_apigw, mock_aurora):
        result = orch.evaluate_region_health()
        assert result["healthy"] is True

    @patch.object(orch, "check_aurora_cluster_status", return_value=_healthy_signal("aurora_status"))
    @patch.object(orch, "check_api_gateway_errors", return_value=_healthy_signal("api_gw_5xx"))
    @patch.object(orch, "check_ecs_running_tasks", return_value=_healthy_signal("ecs_running_tasks"))
    @patch.object(orch, "check_alb_healthy_hosts", return_value=_healthy_signal("alb_healthy_hosts"))
    @patch.object(orch, "check_http_health", return_value=_unhealthy_signal("http_health"))
    def test_http_failure_bypasses_quorum(self, mock_http, mock_alb, mock_ecs, mock_apigw, mock_aurora):
        """HTTP failure makes the region unhealthy regardless of infra signals."""
        result = orch.evaluate_region_health()
        assert result["healthy"] is False
        assert "HTTP health check FAILED" in result["decision_reason"]

    @patch.object(orch, "check_aurora_cluster_status", return_value=_healthy_signal("aurora_status"))
    @patch.object(orch, "check_api_gateway_errors", return_value=_healthy_signal("api_gw_5xx"))
    @patch.object(orch, "check_ecs_running_tasks", return_value=_healthy_signal("ecs_running_tasks"))
    @patch.object(orch, "check_alb_healthy_hosts", return_value=_unhealthy_signal("alb_healthy_hosts"))
    @patch.object(orch, "check_http_health", return_value=_skipped_signal("http_health"))
    def test_one_infra_failure_below_quorum(self, mock_http, mock_alb, mock_ecs, mock_apigw, mock_aurora):
        """1 out of 4 infra signals failing is below quorum (threshold=2), region stays healthy."""
        result = orch.evaluate_region_health()
        assert result["healthy"] is True

    @patch.object(orch, "check_aurora_cluster_status", return_value=_unhealthy_signal("aurora_status"))
    @patch.object(orch, "check_api_gateway_errors", return_value=_unhealthy_signal("api_gw_5xx"))
    @patch.object(orch, "check_ecs_running_tasks", return_value=_healthy_signal("ecs_running_tasks"))
    @patch.object(orch, "check_alb_healthy_hosts", return_value=_healthy_signal("alb_healthy_hosts"))
    @patch.object(orch, "check_http_health", return_value=_skipped_signal("http_health"))
    def test_quorum_reached_marks_unhealthy(self, mock_http, mock_alb, mock_ecs, mock_apigw, mock_aurora):
        """2 out of 4 infra signals failing hits quorum (threshold=2), region is unhealthy."""
        result = orch.evaluate_region_health()
        assert result["healthy"] is False

    @patch.object(orch, "check_aurora_cluster_status", return_value=_skipped_signal("aurora_status"))
    @patch.object(orch, "check_api_gateway_errors", return_value=_skipped_signal("api_gw_5xx"))
    @patch.object(orch, "check_ecs_running_tasks", return_value=_skipped_signal("ecs_running_tasks"))
    @patch.object(orch, "check_alb_healthy_hosts", return_value=_skipped_signal("alb_healthy_hosts"))
    @patch.object(orch, "check_http_health", return_value=_skipped_signal("http_health"))
    def test_no_signals_configured_assumes_healthy(self, mock_http, mock_alb, mock_ecs, mock_apigw, mock_aurora):
        result = orch.evaluate_region_health()
        assert result["healthy"] is True
        assert "No signals configured" in result["decision_reason"]

    @patch.object(orch, "check_aurora_cluster_status", return_value=_unhealthy_signal("aurora_status"))
    @patch.object(orch, "check_api_gateway_errors", return_value=_skipped_signal("api_gw_5xx"))
    @patch.object(orch, "check_ecs_running_tasks", return_value=_skipped_signal("ecs_running_tasks"))
    @patch.object(orch, "check_alb_healthy_hosts", return_value=_skipped_signal("alb_healthy_hosts"))
    @patch.object(orch, "check_http_health", return_value=_healthy_signal("http_health"))
    def test_single_infra_signal_configured_and_failing(self, mock_http, mock_alb, mock_ecs, mock_apigw, mock_aurora):
        """When only 1 infra signal is configured, threshold=1, so 1 failure = unhealthy."""
        result = orch.evaluate_region_health()
        assert result["healthy"] is False


# ===========================================================================
# 2. Individual Health Checks — mocked boto3/urllib
# ===========================================================================

class TestCheckHttpHealth:
    """Tests for check_http_health()."""

    def test_skipped_when_url_not_configured(self):
        with patch.object(orch, "HEALTH_CHECK_URL", ""):
            result = orch.check_http_health()
            assert result["healthy"] is True
            assert result.get("skipped") is True

    @patch("failover_orchestrator_v3.urlopen")
    def test_healthy_on_200_with_up_status(self, mock_urlopen):
        mock_response = MagicMock()
        mock_response.getcode.return_value = 200
        mock_response.read.return_value = json.dumps({"status": "UP"}).encode()
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        with patch.object(orch, "HEALTH_CHECK_URL", "http://internal-alb:8080"):
            result = orch.check_http_health()
            assert result["healthy"] is True
            assert result["status_code"] == 200

    @patch("failover_orchestrator_v3.urlopen")
    def test_unhealthy_on_200_with_down_status(self, mock_urlopen):
        mock_response = MagicMock()
        mock_response.getcode.return_value = 200
        mock_response.read.return_value = json.dumps({"status": "DOWN"}).encode()
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        with patch.object(orch, "HEALTH_CHECK_URL", "http://internal-alb:8080"):
            result = orch.check_http_health()
            assert result["healthy"] is False

    @patch("failover_orchestrator_v3.urlopen")
    def test_unhealthy_on_connection_error(self, mock_urlopen):
        from urllib.error import URLError
        mock_urlopen.side_effect = URLError("Connection refused")

        with patch.object(orch, "HEALTH_CHECK_URL", "http://internal-alb:8080"):
            result = orch.check_http_health()
            assert result["healthy"] is False
            assert "Connection failed" in result["reason"]

    @patch("failover_orchestrator_v3.urlopen")
    def test_unhealthy_on_http_503(self, mock_urlopen):
        from urllib.error import HTTPError
        mock_urlopen.side_effect = HTTPError(
            url="http://internal-alb:8080/actuator/health",
            code=503, msg="Service Unavailable", hdrs=None, fp=None
        )

        with patch.object(orch, "HEALTH_CHECK_URL", "http://internal-alb:8080"):
            result = orch.check_http_health()
            assert result["healthy"] is False
            assert result["status_code"] == 503


class TestCheckEcsRunningTasks:
    """Tests for check_ecs_running_tasks()."""

    def test_skipped_when_not_configured(self):
        with patch.object(orch, "ECS_CLUSTER_NAME", ""), \
             patch.object(orch, "ECS_SERVICE_NAME", ""):
            result = orch.check_ecs_running_tasks()
            assert result["healthy"] is True
            assert result.get("skipped") is True

    def test_healthy_when_running_equals_desired(self):
        mock_ecs = MagicMock()
        mock_ecs.describe_services.return_value = {
            "services": [{"runningCount": 4, "desiredCount": 4}]
        }
        with patch.object(orch, "ECS_CLUSTER_NAME", "my-cluster"), \
             patch.object(orch, "ECS_SERVICE_NAME", "my-service"), \
             patch.object(orch, "ecs", mock_ecs):
            result = orch.check_ecs_running_tasks()
            assert result["healthy"] is True

    def test_unhealthy_when_running_below_half_desired(self):
        mock_ecs = MagicMock()
        mock_ecs.describe_services.return_value = {
            "services": [{"runningCount": 1, "desiredCount": 4}]
        }
        with patch.object(orch, "ECS_CLUSTER_NAME", "my-cluster"), \
             patch.object(orch, "ECS_SERVICE_NAME", "my-service"), \
             patch.object(orch, "ecs", mock_ecs):
            result = orch.check_ecs_running_tasks()
            assert result["healthy"] is False

    def test_unhealthy_when_service_not_found(self):
        mock_ecs = MagicMock()
        mock_ecs.describe_services.return_value = {"services": []}
        with patch.object(orch, "ECS_CLUSTER_NAME", "my-cluster"), \
             patch.object(orch, "ECS_SERVICE_NAME", "my-service"), \
             patch.object(orch, "ecs", mock_ecs):
            result = orch.check_ecs_running_tasks()
            assert result["healthy"] is False


class TestCheckAuroraClusterStatus:
    """Tests for check_aurora_cluster_status()."""

    def test_skipped_when_not_configured(self):
        with patch.object(orch, "AURORA_CLUSTER_ID", ""):
            result = orch.check_aurora_cluster_status()
            assert result["healthy"] is True
            assert result.get("skipped") is True

    def test_healthy_when_available(self):
        mock_rds = MagicMock()
        mock_rds.describe_db_clusters.return_value = {
            "DBClusters": [{"Status": "available"}]
        }
        with patch.object(orch, "AURORA_CLUSTER_ID", "my-cluster"), \
             patch.object(orch, "rds", mock_rds):
            result = orch.check_aurora_cluster_status()
            assert result["healthy"] is True

    def test_healthy_when_backing_up(self):
        mock_rds = MagicMock()
        mock_rds.describe_db_clusters.return_value = {
            "DBClusters": [{"Status": "backing-up"}]
        }
        with patch.object(orch, "AURORA_CLUSTER_ID", "my-cluster"), \
             patch.object(orch, "rds", mock_rds):
            result = orch.check_aurora_cluster_status()
            assert result["healthy"] is True

    def test_healthy_when_modifying(self):
        mock_rds = MagicMock()
        mock_rds.describe_db_clusters.return_value = {
            "DBClusters": [{"Status": "modifying"}]
        }
        with patch.object(orch, "AURORA_CLUSTER_ID", "my-cluster"), \
             patch.object(orch, "rds", mock_rds):
            result = orch.check_aurora_cluster_status()
            assert result["healthy"] is True

    def test_healthy_when_resetting_master_credentials(self):
        mock_rds = MagicMock()
        mock_rds.describe_db_clusters.return_value = {
            "DBClusters": [{"Status": "resetting-master-credentials"}]
        }
        with patch.object(orch, "AURORA_CLUSTER_ID", "my-cluster"), \
             patch.object(orch, "rds", mock_rds):
            result = orch.check_aurora_cluster_status()
            assert result["healthy"] is True

    def test_healthy_when_upgrading(self):
        mock_rds = MagicMock()
        mock_rds.describe_db_clusters.return_value = {
            "DBClusters": [{"Status": "upgrading"}]
        }
        with patch.object(orch, "AURORA_CLUSTER_ID", "my-cluster"), \
             patch.object(orch, "rds", mock_rds):
            result = orch.check_aurora_cluster_status()
            assert result["healthy"] is True

    def test_healthy_when_maintenance(self):
        mock_rds = MagicMock()
        mock_rds.describe_db_clusters.return_value = {
            "DBClusters": [{"Status": "maintenance"}]
        }
        with patch.object(orch, "AURORA_CLUSTER_ID", "my-cluster"), \
             patch.object(orch, "rds", mock_rds):
            result = orch.check_aurora_cluster_status()
            assert result["healthy"] is True

    def test_healthy_when_renaming(self):
        mock_rds = MagicMock()
        mock_rds.describe_db_clusters.return_value = {
            "DBClusters": [{"Status": "renaming"}]
        }
        with patch.object(orch, "AURORA_CLUSTER_ID", "my-cluster"), \
             patch.object(orch, "rds", mock_rds):
            result = orch.check_aurora_cluster_status()
            assert result["healthy"] is True

    def test_unhealthy_when_failing_over(self):
        mock_rds = MagicMock()
        mock_rds.describe_db_clusters.return_value = {
            "DBClusters": [{"Status": "failing-over"}]
        }
        with patch.object(orch, "AURORA_CLUSTER_ID", "my-cluster"), \
             patch.object(orch, "rds", mock_rds):
            result = orch.check_aurora_cluster_status()
            assert result["healthy"] is False


# ===========================================================================
# 3. Consecutive Failure Counting
# ===========================================================================

class TestConsecutiveFailureCounting:
    """Tests for try_increment_failures and threshold logic."""

    def test_increment_succeeds(self):
        mock_backend = MagicMock()
        mock_backend.conditional_update.return_value = True
        with patch.object(orch, "_state_backend", mock_backend):
            assert orch.try_increment_failures(0, 1) is True
            mock_backend.conditional_update.assert_called_once_with(
                condition_field="consecutive_failures",
                expected_value=0,
                updates={"consecutive_failures": 1},
            )

    def test_increment_fails_on_race(self):
        mock_backend = MagicMock()
        mock_backend.conditional_update.return_value = False
        with patch.object(orch, "_state_backend", mock_backend):
            assert orch.try_increment_failures(1, 2) is False

    @patch.object(orch, "_run_rca_analysis", return_value="")
    @patch.object(orch, "send_warning_notification")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "try_increment_failures", return_value=True)
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "update_failover_state")
    def test_below_threshold_stays_healthy(self, mock_update, mock_health,
                                           mock_try_inc, mock_publish,
                                           mock_warn, mock_rca):
        """When failures < threshold, metric stays 1 and warning is sent."""
        mock_health.return_value = {"healthy": False, "signals": [], "decision_reason": "test"}

        with patch.object(orch, "CURRENT_REGION", "us-east-1"), \
             patch.object(orch, "CONSECUTIVE_FAILURES_THRESHOLD", 3):
            state = _make_state(consecutive_failures=1)
            result = orch._handle_active_region(state, "us-east-1", 1, "1970-01-01T00:00:00Z")

        assert result["body"] == "Below threshold, monitoring"
        mock_publish.assert_called_with("us-east-1", True)
        mock_warn.assert_called_once()


# ===========================================================================
# 4. Cooldown Window
# ===========================================================================

class TestCooldownWindow:
    """Tests for cooldown preventing repeated failovers."""

    @patch.object(orch, "_run_rca_analysis", return_value="")
    @patch.object(orch, "send_warning_notification")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "try_increment_failures", return_value=True)
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "update_failover_state")
    def test_cooldown_blocks_failover(self, mock_update, mock_health,
                                      mock_try_inc, mock_publish,
                                      mock_warn, mock_rca):
        """When cooldown is active, failover is blocked even at threshold."""
        mock_health.return_value = {"healthy": False, "signals": [], "decision_reason": "test"}
        recent_ts = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()

        with patch.object(orch, "CURRENT_REGION", "us-east-1"), \
             patch.object(orch, "COOLDOWN_MINUTES", 30), \
             patch.object(orch, "CONSECUTIVE_FAILURES_THRESHOLD", 3):
            state = _make_state(consecutive_failures=2)
            result = orch._handle_active_region(state, "us-east-1", 2, recent_ts)

        assert result["body"] == "Cooldown active"
        # Should publish healthy (not trigger failover)
        mock_publish.assert_called_with("us-east-1", True)

    @patch.object(orch, "_run_rca_analysis", return_value="")
    @patch.object(orch, "_emit_failover_event")
    @patch.object(orch, "send_notification")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "try_claim_failover", return_value=True)
    @patch.object(orch, "try_increment_failures", return_value=True)
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "update_failover_state")
    def test_expired_cooldown_allows_failover(self, mock_update, mock_health,
                                               mock_try_inc, mock_claim,
                                               mock_publish, mock_notify,
                                               mock_emit, mock_rca):
        """When cooldown has expired, failover proceeds."""
        mock_health.return_value = {"healthy": False, "signals": [], "decision_reason": "test"}
        old_ts = (datetime.now(timezone.utc) - timedelta(minutes=60)).isoformat()

        with patch.object(orch, "CURRENT_REGION", "us-east-1"), \
             patch.object(orch, "COOLDOWN_MINUTES", 30), \
             patch.object(orch, "CONSECUTIVE_FAILURES_THRESHOLD", 3), \
             patch.object(orch, "FAILOVER_MODE", "auto"), \
             patch.object(orch, "AURORA_AUTO_PROMOTE", False):
            state = _make_state(consecutive_failures=2)
            result = orch._handle_active_region(state, "us-east-1", 2, old_ts)

        assert "failover" in result["body"].lower()
        mock_claim.assert_called_once()
        mock_publish.assert_any_call("us-east-1", False)


# ===========================================================================
# 5. Latch Mechanism
# ===========================================================================

class TestLatchMechanism:
    """Tests for latch preventing flip-flop."""

    @patch.object(orch, "publish_region_health_metric")
    def test_latched_passive_publishes_zero(self, mock_publish):
        """When latch is engaged, passive region publishes metric=0."""
        state = _make_state(
            active_region="us-east-2",
            state="SECONDARY_ACTIVE",
            latch_engaged=True,
        )
        with patch.object(orch, "CURRENT_REGION", "us-east-1"):
            result = orch._handle_passive_region(state, "us-east-2")

        assert result["body"] == "Latched region, staying marked unhealthy"
        mock_publish.assert_called_once_with("us-east-1", False)

    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "check_active_region_staleness")
    def test_unlatched_passive_publishes_real_health(self, mock_stale,
                                                      mock_publish, mock_health):
        """When latch is NOT engaged, passive region publishes its real health."""
        mock_stale.return_value = {"stale": False, "reason": "fresh"}
        mock_health.return_value = {"healthy": True, "signals": [], "decision_reason": "ok"}

        state = _make_state(
            active_region="us-east-2",
            state="SECONDARY_ACTIVE",
            latch_engaged=False,
        )
        with patch.object(orch, "CURRENT_REGION", "us-east-1"), \
             patch.object(orch, "PASSIVE_PUBLISH_ZERO", False):
            result = orch._handle_passive_region(state, "us-east-2")

        assert result["body"] == "Passive region check complete"
        mock_publish.assert_called_once_with("us-east-1", True)


# ===========================================================================
# 6. Failover Trigger — auto vs manual mode
# ===========================================================================

class TestFailoverTrigger:
    """Tests for failover trigger in auto and manual modes."""

    @patch.object(orch, "_run_rca_analysis", return_value="")
    @patch.object(orch, "_emit_failover_event")
    @patch.object(orch, "send_notification")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "try_claim_failover", return_value=True)
    @patch.object(orch, "try_increment_failures", return_value=True)
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "update_failover_state")
    def test_auto_mode_fires_at_threshold(self, mock_update, mock_health,
                                           mock_try_inc, mock_claim,
                                           mock_publish, mock_notify,
                                           mock_emit, mock_rca):
        """In auto mode, reaching threshold triggers DNS failover."""
        mock_health.return_value = {"healthy": False, "signals": [], "decision_reason": "test"}
        old_ts = "1970-01-01T00:00:00Z"

        with patch.object(orch, "CURRENT_REGION", "us-east-1"), \
             patch.object(orch, "FAILOVER_MODE", "auto"), \
             patch.object(orch, "CONSECUTIVE_FAILURES_THRESHOLD", 3), \
             patch.object(orch, "AURORA_AUTO_PROMOTE", False):
            state = _make_state(consecutive_failures=2)
            result = orch._handle_active_region(state, "us-east-1", 2, old_ts)

        # Should have claimed failover
        mock_claim.assert_called_once()
        claim_updates = mock_claim.call_args[0][1]
        assert claim_updates["state"] == "WAITING_AURORA_PROMOTION"
        assert claim_updates["active_region"] == "us-east-2"
        assert claim_updates["latch_engaged"] is True
        # DNS metric set to 0
        mock_publish.assert_any_call("us-east-1", False)
        # Notification sent
        mock_notify.assert_called_once()
        # v1.6 compose_message-based path uses positional args (subject, body).
        subject = mock_notify.call_args.args[0] if mock_notify.call_args.args else \
                  mock_notify.call_args.kwargs.get("subject", "")
        # v1.6 wording is config-aware; the manual data-tier path uses
        # "operator action required to promote data tier".
        assert "promote" in subject.lower() and "data tier" in subject.lower()

    @patch.object(orch, "_run_rca_analysis", return_value="")
    @patch.object(orch, "send_warning_notification")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "try_increment_failures", return_value=True)
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "update_failover_state")
    def test_manual_mode_notifies_only(self, mock_update, mock_health,
                                        mock_try_inc, mock_publish,
                                        mock_warn, mock_rca):
        """In manual mode, reaching threshold sends notification but no DNS change."""
        mock_health.return_value = {"healthy": False, "signals": [], "decision_reason": "test"}
        old_ts = "1970-01-01T00:00:00Z"

        with patch.object(orch, "CURRENT_REGION", "us-east-1"), \
             patch.object(orch, "FAILOVER_MODE", "manual"), \
             patch.object(orch, "CONSECUTIVE_FAILURES_THRESHOLD", 3):
            state = _make_state(consecutive_failures=2)
            result = orch._handle_active_region(state, "us-east-1", 2, old_ts)

        assert "manual mode" in result["body"].lower()
        # Metric stays healthy (no DNS change)
        mock_publish.assert_called_with("us-east-1", True)
        # Warning notification with recommendation
        mock_warn.assert_called_once()
        subject_arg = mock_warn.call_args[0][0] if mock_warn.call_args[0] else mock_warn.call_args[1].get("subject", "")
        # v1.6 wording: "Failover RECOMMENDED but blocked — FAILOVER_MODE is manual"
        assert "RECOMMENDED" in subject_arg and "manual" in subject_arg.lower()

    @patch.object(orch, "_run_rca_analysis", return_value="")
    @patch.object(orch, "_emit_failover_event")
    @patch.object(orch, "send_notification")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "try_claim_failover", return_value=False)
    @patch.object(orch, "try_increment_failures", return_value=True)
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "update_failover_state")
    def test_auto_mode_yields_if_claim_fails(self, mock_update, mock_health,
                                              mock_try_inc, mock_claim,
                                              mock_publish, mock_notify,
                                              mock_emit, mock_rca):
        """If another invocation already claimed failover, this one yields."""
        mock_health.return_value = {"healthy": False, "signals": [], "decision_reason": "test"}
        old_ts = "1970-01-01T00:00:00Z"

        with patch.object(orch, "CURRENT_REGION", "us-east-1"), \
             patch.object(orch, "FAILOVER_MODE", "auto"), \
             patch.object(orch, "CONSECUTIVE_FAILURES_THRESHOLD", 3):
            state = _make_state(consecutive_failures=2)
            result = orch._handle_active_region(state, "us-east-1", 2, old_ts)

        assert "already claimed" in result["body"].lower()
        mock_notify.assert_not_called()


# ===========================================================================
# 7. Notification Throttling
# ===========================================================================

class TestNotificationThrottling:
    """Tests for send_warning_notification cooldown."""

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "sns")
    def test_first_warning_always_sends(self, mock_sns, mock_update):
        """First warning notification sends immediately."""
        state = _make_state(last_warning_notification_ts="1970-01-01T00:00:00Z")

        with patch.object(orch, "WARNING_NOTIFICATION_COOLDOWN_MINUTES", 10):
            orch.send_warning_notification("test subject", "test body", state)

        mock_sns.publish.assert_called_once()
        mock_update.assert_called_once()

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "sns")
    def test_warning_throttled_within_cooldown(self, mock_sns, mock_update):
        """Warning notification suppressed if within cooldown window."""
        recent_ts = (datetime.now(timezone.utc) - timedelta(minutes=2)).isoformat()
        state = _make_state(last_warning_notification_ts=recent_ts)

        with patch.object(orch, "WARNING_NOTIFICATION_COOLDOWN_MINUTES", 10):
            orch.send_warning_notification("test subject", "test body", state)

        mock_sns.publish.assert_not_called()
        mock_update.assert_not_called()

    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "sns")
    def test_warning_sends_after_cooldown_expires(self, mock_sns, mock_update):
        """Warning notification sends once cooldown has expired."""
        old_ts = (datetime.now(timezone.utc) - timedelta(minutes=15)).isoformat()
        state = _make_state(last_warning_notification_ts=old_ts)

        with patch.object(orch, "WARNING_NOTIFICATION_COOLDOWN_MINUTES", 10):
            orch.send_warning_notification("test subject", "test body", state)

        mock_sns.publish.assert_called_once()

    @patch.object(orch, "sns")
    def test_critical_notification_never_throttled(self, mock_sns):
        """send_notification (CRITICAL) always sends, no throttle."""
        orch.send_notification("CRITICAL subject", "body")
        mock_sns.publish.assert_called_once()

        # Call again immediately
        mock_sns.reset_mock()
        orch.send_notification("Another CRITICAL", "body")
        mock_sns.publish.assert_called_once()


# ===========================================================================
# 8. RCA Integration
# ===========================================================================

class TestRCAIntegration:
    """Tests for _run_rca_analysis non-blocking behavior."""

    def test_returns_empty_when_disabled(self):
        with patch.dict(os.environ, {"AI_RCA_ENABLED": "false"}):
            result = orch._run_rca_analysis({"http": {"healthy": False}})
            assert result == ""

    @patch("ai.rca_analyzer.format_rca_for_sns", return_value="== RCA ==\nSummary")
    @patch("ai.rca_analyzer.analyze_incident", return_value="RCA text")
    @patch("ai.collector.collect_incident_context", return_value={"signals": {}})
    def test_returns_formatted_rca_when_enabled(self, mock_collect, mock_analyze, mock_format):
        with patch.dict(os.environ, {"AI_RCA_ENABLED": "true"}):
            result = orch._run_rca_analysis({"http": {"healthy": False}})
            assert "== RCA ==" in result
            mock_collect.assert_called_once()
            mock_analyze.assert_called_once()

    @patch("ai.collector.collect_incident_context", side_effect=Exception("API timeout"))
    def test_returns_empty_on_failure(self, mock_collect):
        """RCA failure is non-blocking — returns empty string, does not raise."""
        with patch.dict(os.environ, {"AI_RCA_ENABLED": "true"}):
            result = orch._run_rca_analysis({"http": {"healthy": False}})
            assert result == ""


# ===========================================================================
# 9. Lambda Handler Routing
# ===========================================================================

class TestHandlerRouting:
    """Tests for the handler function dispatching to correct code paths."""

    @patch.object(orch, "_reset_state", return_value={"statusCode": 200, "body": "reset"})
    def test_reset_state_event(self, mock_reset):
        result = orch.handler({"reset_state": True}, None)
        mock_reset.assert_called_once()
        assert result["statusCode"] == 200

    @patch.object(orch, "_handle_active_active", return_value={"statusCode": 200, "body": "aa"})
    @patch.object(orch, "get_failover_state", return_value=_make_state())
    def test_active_active_mode_routes_correctly(self, mock_state, mock_aa):
        with patch.dict(os.environ, {"ROUTING_MODE": "active-active"}):
            result = orch.handler({}, None)
        mock_aa.assert_called_once()

    @patch.object(orch, "_execute_manual_failover", return_value={"statusCode": 200, "body": "ok"})
    @patch.object(orch, "get_failover_state", return_value=_make_state())
    def test_execute_failover_event(self, mock_state, mock_exec):
        with patch.dict(os.environ, {"ROUTING_MODE": "failover"}):
            result = orch.handler({"execute_failover": True}, None)
        mock_exec.assert_called_once()

    @patch.object(orch, "_handle_active_region", return_value={"statusCode": 200, "body": "active"})
    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "get_failover_state")
    def test_routes_to_active_handler(self, mock_get_state, mock_update, mock_active):
        """When current region IS the active region, route to _handle_active_region."""
        mock_get_state.return_value = _make_state(active_region="us-east-1")
        with patch.object(orch, "CURRENT_REGION", "us-east-1"), \
             patch.object(orch, "ROUTING_MODE", "failover"):
            result = orch.handler({}, None)
        mock_active.assert_called_once()

    @patch.object(orch, "_handle_passive_region", return_value={"statusCode": 200, "body": "passive"})
    @patch.object(orch, "get_failover_state")
    def test_routes_to_passive_handler(self, mock_get_state, mock_passive):
        """When current region is NOT the active region, route to _handle_passive_region."""
        mock_get_state.return_value = _make_state(active_region="us-east-2")
        with patch.object(orch, "CURRENT_REGION", "us-east-1"), \
             patch.object(orch, "ROUTING_MODE", "failover"):
            result = orch.handler({}, None)
        mock_passive.assert_called_once()

    @patch.object(orch, "get_failover_state")
    def test_skips_when_failover_in_progress(self, mock_get_state):
        mock_get_state.return_value = _make_state(state="FAILOVER_IN_PROGRESS")
        with patch.object(orch, "CURRENT_REGION", "us-east-1"), \
             patch.object(orch, "ROUTING_MODE", "failover"):
            result = orch.handler({}, None)
        assert "Skipping" in result["body"]

    @patch.object(orch, "get_failover_state")
    def test_skips_when_failback_in_progress(self, mock_get_state):
        mock_get_state.return_value = _make_state(state="FAILBACK_IN_PROGRESS")
        with patch.object(orch, "CURRENT_REGION", "us-east-1"), \
             patch.object(orch, "ROUTING_MODE", "failover"):
            result = orch.handler({}, None)
        assert "Skipping" in result["body"]

    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "_handle_aurora_promotion_reminder",
                  return_value={"statusCode": 200, "body": "reminder"})
    @patch.object(orch, "get_failover_state")
    def test_aurora_promotion_pending_routes_to_reminder(self, mock_get_state, mock_reminder, mock_publish):
        """Active region in WAITING_AURORA_PROMOTION goes to reminder handler."""
        mock_get_state.return_value = _make_state(
            active_region="us-east-2",
            state="WAITING_AURORA_PROMOTION",
            aurora_promotion_pending=True,
        )
        with patch.object(orch, "CURRENT_REGION", "us-east-2"), \
             patch.object(orch, "ROUTING_MODE", "failover"):
            result = orch.handler({}, None)
        mock_reminder.assert_called_once()
        assert "Waiting" in result["body"]


# ===========================================================================
# 10. PASSIVE_PUBLISH_ZERO mode
# ===========================================================================

class TestPassivePublishZero:
    """Tests for PASSIVE_PUBLISH_ZERO passive region behavior."""

    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "check_active_region_staleness")
    def test_passive_publish_zero_always_publishes_zero(self, mock_stale, mock_publish):
        """With PASSIVE_PUBLISH_ZERO=true, passive always publishes 0."""
        mock_stale.return_value = {"stale": False, "reason": "fresh"}
        state = _make_state(active_region="us-east-1", latch_engaged=False)

        with patch.object(orch, "CURRENT_REGION", "us-east-2"), \
             patch.object(orch, "PASSIVE_PUBLISH_ZERO", True):
            result = orch._handle_passive_region(state, "us-east-1")

        assert "PASSIVE_PUBLISH_ZERO" in result["body"]
        mock_publish.assert_called_once_with("us-east-2", False)


# ===========================================================================
# 11. State management — get_failover_state default creation
# ===========================================================================

class TestGetFailoverState:
    """Tests for get_failover_state auto-initialization."""

    def test_creates_default_state_when_empty(self):
        mock_backend = MagicMock()
        mock_backend.get_state.return_value = {}
        with patch.object(orch, "_state_backend", mock_backend):
            state = orch.get_failover_state()
        assert state["active_region"] == "us-east-1"
        assert state["state"] == "PRIMARY_ACTIVE"
        assert state["latch_engaged"] is False
        assert state["consecutive_failures"] == 0
        mock_backend.put_state.assert_called_once()

    def test_returns_existing_state(self):
        existing = _make_state(active_region="us-east-2", state="SECONDARY_ACTIVE")
        mock_backend = MagicMock()
        mock_backend.get_state.return_value = existing
        with patch.object(orch, "_state_backend", mock_backend):
            state = orch.get_failover_state()
        assert state["active_region"] == "us-east-2"
        mock_backend.put_state.assert_not_called()


# ===========================================================================
# 12. Active region recovery resets failure counter
# ===========================================================================

class TestActiveRegionRecovery:

    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "update_failover_state")
    def test_recovery_resets_consecutive_failures(self, mock_update, mock_health, mock_publish):
        """When active region becomes healthy after failures, counter resets to 0."""
        mock_health.return_value = {"healthy": True, "signals": [], "decision_reason": "ok"}

        with patch.object(orch, "CURRENT_REGION", "us-east-1"):
            state = _make_state(consecutive_failures=2)
            result = orch._handle_active_region(state, "us-east-1", 2, "1970-01-01T00:00:00Z")

        assert result["body"] == "Region healthy"
        # Should have reset failures and updated heartbeat
        update_calls = [c[0][0] for c in mock_update.call_args_list]
        assert any("consecutive_failures" in u and u["consecutive_failures"] == 0 for u in update_calls)


# ===========================================================================
# 13. Passive region staleness detection triggers failover
# ===========================================================================

class TestPassiveStalenessFailover:

    @patch.object(orch, "_emit_failover_event")
    @patch.object(orch, "send_notification")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "try_claim_failover", return_value=True)
    @patch.object(orch, "check_active_region_staleness")
    def test_stale_active_triggers_failover_from_passive(self, mock_stale, mock_claim,
                                                          mock_publish, mock_notify,
                                                          mock_emit):
        """Passive region detects stale active and claims failover."""
        mock_stale.return_value = {
            "stale": True,
            "heartbeat_stale": True,
            "cw_stale": True,
            "reason": "both stale",
        }
        state = _make_state(active_region="us-east-1", latch_engaged=False)

        with patch.object(orch, "CURRENT_REGION", "us-east-2"), \
             patch.object(orch, "AURORA_AUTO_PROMOTE", False):
            result = orch._handle_passive_region(state, "us-east-1")

        assert "failover claimed" in result["body"].lower()
        mock_claim.assert_called_once()
        claim_updates = mock_claim.call_args[0][1]
        assert claim_updates["active_region"] == "us-east-2"
        assert claim_updates["latch_engaged"] is True


# ===========================================================================
# 13b. Staleness check three-state logic (v1.5)
#
# Directly exercises check_active_region_staleness() rather than mocking it
# wholesale. Verifies that:
#   - A CW network timeout is INCONCLUSIVE (cw_stale=None), not confirming.
#   - A borderline-stale heartbeat + CW timeout does NOT trigger failover
#     (the 2026-04-24 prod incident regression).
#   - A deeply-stale heartbeat (>2× threshold) fires failover even when CW is
#     unreachable (preserves failover capability under simultaneous outages).
#   - ClientError and empty-datapoints paths still CONFIRM staleness (unchanged).
# ===========================================================================

class TestStalenessCheckLogic:
    """Direct tests of check_active_region_staleness() tri-state cw_stale logic."""

    THRESHOLD_MIN = 3  # matches default ACTIVE_REGION_STALE_THRESHOLD_MINUTES

    def _state_with_heartbeat_age(self, age_seconds: float) -> dict:
        """Build a state dict whose last_active_metric_ts is `age_seconds` in the past."""
        ts = datetime.now(timezone.utc) - timedelta(seconds=age_seconds)
        return _make_state(last_active_metric_ts=ts.isoformat())

    def _cw_client_with_fresh_datapoints(self):
        mock_cw = MagicMock()
        mock_cw.get_metric_statistics.return_value = {
            "Datapoints": [{"Timestamp": datetime.now(timezone.utc), "Minimum": 1.0}]
        }
        return mock_cw

    def _cw_client_raising(self, exc):
        mock_cw = MagicMock()
        mock_cw.get_metric_statistics.side_effect = exc
        return mock_cw

    def _cw_client_with_empty_datapoints(self):
        mock_cw = MagicMock()
        mock_cw.get_metric_statistics.return_value = {"Datapoints": []}
        return mock_cw

    # ---- Case 1: fresh heartbeat + CW fresh → not stale (baseline) -------

    def test_fresh_heartbeat_and_cw_fresh_is_not_stale(self):
        state = self._state_with_heartbeat_age(30)  # well under 180s threshold
        with patch.object(orch, "ACTIVE_REGION_STALE_THRESHOLD_MINUTES", self.THRESHOLD_MIN), \
             patch.object(orch.boto3, "client", return_value=self._cw_client_with_fresh_datapoints()):
            result = orch.check_active_region_staleness("us-east-1", state)

        assert result["stale"] is False
        assert result["heartbeat_stale"] is False
        assert result["cw_stale"] is False
        assert result["cw_inconclusive"] is False

    # ---- Case 2: fresh heartbeat + CW timeout → not stale, inconclusive --

    def test_fresh_heartbeat_and_cw_timeout_is_not_stale(self):
        """VPC-blocked CW + healthy heartbeat: inconclusive, but not stale."""
        from botocore.exceptions import EndpointConnectionError as EndpointErr
        state = self._state_with_heartbeat_age(30)
        with patch.object(orch, "ACTIVE_REGION_STALE_THRESHOLD_MINUTES", self.THRESHOLD_MIN), \
             patch.object(
                orch.boto3, "client",
                return_value=self._cw_client_raising(
                    EndpointErr(endpoint_url="https://monitoring-us-east-1.amazonaws.com/")
                ),
             ):
            result = orch.check_active_region_staleness("us-east-1", state)

        assert result["stale"] is False
        assert result["heartbeat_stale"] is False
        assert result["cw_stale"] is None
        assert result["cw_inconclusive"] is True

    # ---- Case 3: borderline heartbeat + CW timeout → not stale -----------
    # REGRESSION TEST for the 2026-04-24 prod incident.

    def test_borderline_heartbeat_and_cw_timeout_is_not_stale(self):
        """
        Heartbeat just barely over threshold (205s old, 180s threshold) +
        simultaneous CW network timeout. Under pre-v1.5 code this collapsed
        into a false failover. Under v1.5 this must NOT fire.
        """
        from botocore.exceptions import HTTPClientError
        state = self._state_with_heartbeat_age(205)  # 25s over 180s threshold
        with patch.object(orch, "ACTIVE_REGION_STALE_THRESHOLD_MINUTES", self.THRESHOLD_MIN), \
             patch.object(
                orch.boto3, "client",
                return_value=self._cw_client_raising(
                    HTTPClientError(error="Connect timeout on endpoint URL")
                ),
             ):
            result = orch.check_active_region_staleness("us-east-1", state)

        assert result["heartbeat_stale"] is True, "heartbeat should be flagged stale"
        assert result["cw_stale"] is None, "CW timeout must be inconclusive, not confirming"
        assert result["cw_inconclusive"] is True
        assert result["stale"] is False, (
            "borderline heartbeat + CW timeout must NOT trigger failover "
            "(regression for 2026-04-24 incident)"
        )

    # ---- Case 4: deeply stale heartbeat + CW timeout → stale -------------

    def test_deeply_stale_heartbeat_and_cw_timeout_is_stale(self):
        """
        Heartbeat >2× threshold (e.g. 400s > 360s = 2×180s) + CW timeout.
        Preserves failover capability when CW is unreachable but the
        heartbeat is emphatically stale.
        """
        from botocore.exceptions import HTTPClientError
        state = self._state_with_heartbeat_age(400)  # 2× threshold is 360s
        with patch.object(orch, "ACTIVE_REGION_STALE_THRESHOLD_MINUTES", self.THRESHOLD_MIN), \
             patch.object(
                orch.boto3, "client",
                return_value=self._cw_client_raising(
                    HTTPClientError(error="Connect timeout on endpoint URL")
                ),
             ):
            result = orch.check_active_region_staleness("us-east-1", state)

        assert result["heartbeat_stale"] is True
        assert result["cw_stale"] is None
        assert result["cw_inconclusive"] is True
        assert result["stale"] is True, (
            "deep heartbeat staleness should fire failover even when CW is inconclusive"
        )

    # ---- Case 5: stale heartbeat + CW ClientError → stale (unchanged) ----

    def test_stale_heartbeat_and_cw_client_error_is_stale(self):
        """ClientError (CW API responded with error) is CONFIRMING, unchanged from v1.0."""
        from botocore.exceptions import ClientError
        err = ClientError({"Error": {"Code": "Throttling", "Message": "Rate exceeded"}}, "GetMetricStatistics")
        state = self._state_with_heartbeat_age(300)
        with patch.object(orch, "ACTIVE_REGION_STALE_THRESHOLD_MINUTES", self.THRESHOLD_MIN), \
             patch.object(orch.boto3, "client", return_value=self._cw_client_raising(err)):
            result = orch.check_active_region_staleness("us-east-1", state)

        assert result["heartbeat_stale"] is True
        assert result["cw_stale"] is True
        assert result["cw_inconclusive"] is False
        assert result["stale"] is True

    # ---- Case 6: stale heartbeat + CW empty datapoints → stale (unchanged)

    def test_stale_heartbeat_and_cw_empty_datapoints_is_stale(self):
        state = self._state_with_heartbeat_age(300)
        with patch.object(orch, "ACTIVE_REGION_STALE_THRESHOLD_MINUTES", self.THRESHOLD_MIN), \
             patch.object(orch.boto3, "client", return_value=self._cw_client_with_empty_datapoints()):
            result = orch.check_active_region_staleness("us-east-1", state)

        assert result["heartbeat_stale"] is True
        assert result["cw_stale"] is True
        assert result["cw_inconclusive"] is False
        assert result["stale"] is True

    # ---- Case 7: fresh heartbeat + CW ClientError → not stale (AND holds)

    def test_fresh_heartbeat_and_cw_client_error_is_not_stale(self):
        from botocore.exceptions import ClientError
        err = ClientError({"Error": {"Code": "AccessDenied", "Message": "nope"}}, "GetMetricStatistics")
        state = self._state_with_heartbeat_age(30)
        with patch.object(orch, "ACTIVE_REGION_STALE_THRESHOLD_MINUTES", self.THRESHOLD_MIN), \
             patch.object(orch.boto3, "client", return_value=self._cw_client_raising(err)):
            result = orch.check_active_region_staleness("us-east-1", state)

        assert result["heartbeat_stale"] is False
        assert result["cw_stale"] is True
        assert result["stale"] is False


# ===========================================================================
# 14. APP_NAME in notification subjects
# ===========================================================================

class TestFormatSubject:

    def test_subject_with_app_name(self):
        with patch.object(orch, "APP_NAME", "deposits-api"):
            result = orch._format_subject("FAILOVER: us-east-1 -> us-east-2")
            assert result.startswith("[deposits-api]")

    def test_subject_without_app_name(self):
        with patch.object(orch, "APP_NAME", ""):
            result = orch._format_subject("FAILOVER: us-east-1 -> us-east-2")
            assert result == "FAILOVER: us-east-1 -> us-east-2"

    def test_subject_truncated_to_100_chars(self):
        with patch.object(orch, "APP_NAME", "my-app"):
            long_subject = "X" * 200
            result = orch._format_subject(long_subject)
            assert len(result) <= 100


# ===========================================================================
# 15. Parked mode — staged deployment gate
# ===========================================================================

class TestParkedMode:
    """Tests for FAILOVER_MODE=parked early-exit behavior."""

    def test_parked_mode_returns_immediately(self):
        """Handler returns 200 with 'Parked' body, no further processing."""
        with patch.dict(os.environ, {"FAILOVER_MODE": "parked"}):
            result = orch.handler({}, None)
        assert result["statusCode"] == 200
        assert "Parked" in result["body"]

    @patch.object(orch, "get_failover_state")
    def test_parked_mode_no_state_access(self, mock_get_state):
        """State backend is never accessed in parked mode."""
        with patch.dict(os.environ, {"FAILOVER_MODE": "parked"}):
            orch.handler({}, None)
        mock_get_state.assert_not_called()

    @patch.object(orch, "publish_region_health_metric")
    def test_parked_mode_no_metric_published(self, mock_publish):
        """No CloudWatch metrics are published in parked mode."""
        with patch.dict(os.environ, {"FAILOVER_MODE": "parked"}):
            orch.handler({}, None)
        mock_publish.assert_not_called()

    @patch.object(orch, "_reload_dynamic_config")
    def test_parked_mode_no_reload_dynamic_config(self, mock_reload):
        """_reload_dynamic_config() is not called — avoids state backend init."""
        with patch.dict(os.environ, {"FAILOVER_MODE": "parked"}):
            orch.handler({}, None)
        mock_reload.assert_not_called()

    def test_parked_mode_works_without_state_bucket(self):
        """Parked mode succeeds even when STATE_BUCKET is missing (S3 mode).

        This is the key scenario: Lambda deployed but S3 bucket not created yet.
        Without parked mode, _reload_dynamic_config() → create_backend() would
        raise ValueError for missing STATE_BUCKET.
        """
        env = {"FAILOVER_MODE": "parked", "STATE_BACKEND": "s3"}
        with patch.dict(os.environ, env):
            # Remove STATE_BUCKET if present
            os.environ.pop("STATE_BUCKET", None)
            result = orch.handler({}, None)
        assert result["statusCode"] == 200
        assert "Parked" in result["body"]


# ===========================================================================
# 16. Pre-flight checks — resource validation before activation
# ===========================================================================

class TestPreflightChecks:
    """Tests for the {"preflight": true} resource validation handler."""

    @patch.object(orch, "sns")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "_state_backend")
    def test_preflight_all_pass(self, mock_backend, mock_publish, mock_sns):
        """All required resources accessible → ready=True."""
        mock_backend.get_state.return_value = _make_state()
        mock_sns.get_topic_attributes.return_value = {}
        with patch.dict(os.environ, {"FAILOVER_MODE": "auto"}):
            result = orch.handler({"preflight": True}, None)
        assert result["ready"] is True
        assert result["statusCode"] == 200
        assert result["checks"]["state_backend"]["status"] == "PASS"
        assert result["checks"]["cloudwatch_metric"]["status"] == "PASS"
        assert result["checks"]["sns_topic"]["status"] == "PASS"

    @patch.object(orch, "sns")
    @patch.object(orch, "publish_region_health_metric")
    def test_preflight_state_backend_fail(self, mock_publish, mock_sns):
        """State backend unreachable → ready=False with error detail."""
        mock_sns.get_topic_attributes.return_value = {}
        with patch.object(orch, "_reload_dynamic_config",
                          side_effect=Exception("table not found")), \
             patch.dict(os.environ, {"FAILOVER_MODE": "auto"}):
            result = orch.handler({"preflight": True}, None)
        assert result["ready"] is False
        assert result["checks"]["state_backend"]["status"] == "FAIL"
        assert "table not found" in result["checks"]["state_backend"]["error"]

    @patch.object(orch, "sns")
    @patch.object(orch, "publish_region_health_metric",
                  side_effect=Exception("AccessDenied"))
    @patch.object(orch, "_state_backend")
    def test_preflight_cloudwatch_fail(self, mock_backend, mock_publish, mock_sns):
        """CloudWatch permission denied → ready=False."""
        mock_backend.get_state.return_value = _make_state()
        mock_sns.get_topic_attributes.return_value = {}
        with patch.dict(os.environ, {"FAILOVER_MODE": "auto"}):
            result = orch.handler({"preflight": True}, None)
        assert result["ready"] is False
        assert result["checks"]["cloudwatch_metric"]["status"] == "FAIL"

    @patch.object(orch, "sns")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "_state_backend")
    def test_preflight_optional_signals_reported(self, mock_backend, mock_publish, mock_sns):
        """Non-configured optional signals show SKIPPED, don't block readiness."""
        mock_backend.get_state.return_value = _make_state()
        mock_sns.get_topic_attributes.return_value = {}
        with patch.dict(os.environ, {"FAILOVER_MODE": "auto",
                                     "AURORA_CLUSTER_ID": "",
                                     "ECS_CLUSTER_NAME": "",
                                     "HEALTH_CHECK_URL": ""}):
            result = orch.handler({"preflight": True}, None)
        assert result["ready"] is True
        assert result["checks"]["aurora"]["status"] == "SKIPPED"
        assert result["checks"]["ecs"]["status"] == "SKIPPED"
        assert result["checks"]["health_endpoint"]["status"] == "SKIPPED"
