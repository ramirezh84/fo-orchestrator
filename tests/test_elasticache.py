"""
Tests for ElastiCache Global Datastore failover support (v1.3).

Covers:
- check_elasticache_status() health signal
- _check_if_elasticache_primary() primary detection
- _auto_promote_elasticache() auto-promotion
- Combined gate logic (Aurora + Redis promotion pending)
- redis_promotion_pending state field behavior
"""

import os
import pytest
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Environment setup — must happen BEFORE importing the orchestrator module
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
    "ELASTICACHE_REPLICATION_GROUP_ID": "",
    "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID": "",
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

# Mock boto3 and state backend before import
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
    active_region="us-east-2",
    state="WAITING_AURORA_PROMOTION",
    aurora_promotion_pending=True,
    redis_promotion_pending=True,
    latch_engaged=True,
    consecutive_failures=0,
    last_failover_ts=None,
):
    """Build a state dict for ElastiCache tests."""
    return {
        "active_region": active_region,
        "state": state,
        "latch_engaged": latch_engaged,
        "consecutive_failures": consecutive_failures,
        "last_failover_ts": last_failover_ts or datetime.now(timezone.utc).isoformat(),
        "aurora_promotion_pending": aurora_promotion_pending,
        "redis_promotion_pending": redis_promotion_pending,
        "last_active_metric_ts": datetime.now(timezone.utc).isoformat(),
        "last_warning_notification_ts": "1970-01-01T00:00:00Z",
    }


def _client_error(code="ReplicationGroupNotFoundFault"):
    return ClientError(
        {"Error": {"Code": code, "Message": "test error"}},
        "DescribeReplicationGroups",
    )


# ===========================================================================
# 1. check_elasticache_status()
# ===========================================================================

class TestCheckElasticacheStatus:
    """Tests for the ElastiCache health signal."""

    def test_skipped_when_not_configured(self):
        with patch.object(orch, "ELASTICACHE_REPLICATION_GROUP_ID", ""):
            result = orch.check_elasticache_status()
            assert result["healthy"] is True
            assert result.get("skipped") is True
            assert result["signal"] == "elasticache_status"

    def test_healthy_when_available(self):
        mock_ec = MagicMock()
        mock_ec.describe_replication_groups.return_value = {
            "ReplicationGroups": [{"Status": "available"}]
        }
        with patch.object(orch, "ELASTICACHE_REPLICATION_GROUP_ID", "my-rg"), \
             patch.object(orch, "elasticache_client", mock_ec):
            result = orch.check_elasticache_status()
            assert result["healthy"] is True
            assert result["value"] == "available"

    def test_unhealthy_when_creating(self):
        mock_ec = MagicMock()
        mock_ec.describe_replication_groups.return_value = {
            "ReplicationGroups": [{"Status": "creating"}]
        }
        with patch.object(orch, "ELASTICACHE_REPLICATION_GROUP_ID", "my-rg"), \
             patch.object(orch, "elasticache_client", mock_ec):
            result = orch.check_elasticache_status()
            assert result["healthy"] is False

    def test_unhealthy_when_not_found(self):
        mock_ec = MagicMock()
        mock_ec.describe_replication_groups.return_value = {
            "ReplicationGroups": []
        }
        with patch.object(orch, "ELASTICACHE_REPLICATION_GROUP_ID", "my-rg"), \
             patch.object(orch, "elasticache_client", mock_ec):
            result = orch.check_elasticache_status()
            assert result["healthy"] is False
            assert "not found" in result["reason"]

    def test_unhealthy_on_api_error(self):
        mock_ec = MagicMock()
        mock_ec.describe_replication_groups.side_effect = _client_error()
        with patch.object(orch, "ELASTICACHE_REPLICATION_GROUP_ID", "my-rg"), \
             patch.object(orch, "elasticache_client", mock_ec):
            result = orch.check_elasticache_status()
            assert result["healthy"] is False


# ===========================================================================
# 2. _check_if_elasticache_primary()
# ===========================================================================

class TestCheckIfElasticachePrimary:
    """Tests for ElastiCache primary detection."""

    def test_returns_false_when_not_configured(self):
        with patch.object(orch, "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", ""):
            assert orch._check_if_elasticache_primary("us-east-2") is False

    @patch("failover_orchestrator_v3.boto3.client")
    def test_returns_true_when_primary(self, mock_boto_client):
        mock_ec = MagicMock()
        mock_ec.describe_global_replication_groups.return_value = {
            "GlobalReplicationGroups": [{
                "Members": [
                    {"ReplicationGroupRegion": "us-east-1", "Role": "SECONDARY"},
                    {"ReplicationGroupRegion": "us-east-2", "Role": "PRIMARY"},
                ]
            }]
        }
        mock_boto_client.return_value = mock_ec
        with patch.object(orch, "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", "my-global"):
            assert orch._check_if_elasticache_primary("us-east-2") is True

    @patch("failover_orchestrator_v3.boto3.client")
    def test_returns_false_when_secondary(self, mock_boto_client):
        mock_ec = MagicMock()
        mock_ec.describe_global_replication_groups.return_value = {
            "GlobalReplicationGroups": [{
                "Members": [
                    {"ReplicationGroupRegion": "us-east-1", "Role": "PRIMARY"},
                    {"ReplicationGroupRegion": "us-east-2", "Role": "SECONDARY"},
                ]
            }]
        }
        mock_boto_client.return_value = mock_ec
        with patch.object(orch, "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", "my-global"):
            assert orch._check_if_elasticache_primary("us-east-2") is False

    @patch("failover_orchestrator_v3.boto3.client")
    def test_returns_false_on_api_error(self, mock_boto_client):
        mock_ec = MagicMock()
        mock_ec.describe_global_replication_groups.side_effect = _client_error()
        mock_boto_client.return_value = mock_ec
        with patch.object(orch, "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", "my-global"):
            assert orch._check_if_elasticache_primary("us-east-2") is False

    @patch("failover_orchestrator_v3.boto3.client")
    def test_returns_false_when_region_not_found(self, mock_boto_client):
        mock_ec = MagicMock()
        mock_ec.describe_global_replication_groups.return_value = {
            "GlobalReplicationGroups": [{
                "Members": [
                    {"ReplicationGroupRegion": "us-east-1", "Role": "PRIMARY"},
                ]
            }]
        }
        mock_boto_client.return_value = mock_ec
        with patch.object(orch, "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", "my-global"):
            assert orch._check_if_elasticache_primary("us-east-2") is False


# ===========================================================================
# 3. _auto_promote_elasticache()
# ===========================================================================

class TestAutoPromoteElasticache:
    """Tests for ElastiCache auto-promotion."""

    def test_returns_error_when_not_configured(self):
        with patch.object(orch, "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", ""):
            result = orch._auto_promote_elasticache("us-east-2", "app_failure")
            assert result["success"] is False
            assert "not configured" in result["error"]

    @patch.object(orch, "_get_elasticache_rg_id_in_region", return_value="my-rg-east-2")
    def test_success(self, mock_get_rg):
        mock_ec = MagicMock()
        with patch.object(orch, "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", "my-global"), \
             patch.object(orch, "elasticache_client", mock_ec):
            result = orch._auto_promote_elasticache("us-east-2", "region_failure")
            assert result["success"] is True
            assert result["method"] == "failover"
            mock_ec.failover_global_replication_group.assert_called_once_with(
                GlobalReplicationGroupId="my-global",
                PrimaryRegion="us-east-2",
                PrimaryReplicationGroupId="my-rg-east-2",
            )

    @patch.object(orch, "_get_elasticache_rg_id_in_region", return_value="my-rg-east-2")
    def test_failure_on_api_error(self, mock_get_rg):
        mock_ec = MagicMock()
        mock_ec.failover_global_replication_group.side_effect = _client_error("InvalidParameterValue")
        with patch.object(orch, "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", "my-global"), \
             patch.object(orch, "elasticache_client", mock_ec):
            result = orch._auto_promote_elasticache("us-east-2", "app_failure")
            assert result["success"] is False
            assert "failed" in result["error"]

    @patch.object(orch, "_get_elasticache_rg_id_in_region", return_value="")
    def test_failure_when_target_rg_not_found(self, mock_get_rg):
        with patch.object(orch, "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", "my-global"):
            result = orch._auto_promote_elasticache("us-east-2", "app_failure")
            assert result["success"] is False
            assert "Cannot determine" in result["error"]


# ===========================================================================
# 4. _get_elasticache_rg_id_in_region()
# ===========================================================================

class TestGetElasticacheRgIdInRegion:
    """Tests for the replication group ID lookup helper."""

    def test_returns_empty_when_not_configured(self):
        with patch.object(orch, "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", ""):
            assert orch._get_elasticache_rg_id_in_region("us-east-2") == ""

    def test_returns_rg_id_for_target_region(self):
        mock_ec = MagicMock()
        mock_ec.describe_global_replication_groups.return_value = {
            "GlobalReplicationGroups": [{
                "Members": [
                    {"ReplicationGroupRegion": "us-east-1", "ReplicationGroupId": "rg-w1"},
                    {"ReplicationGroupRegion": "us-east-2", "ReplicationGroupId": "rg-w2"},
                ]
            }]
        }
        with patch.object(orch, "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", "my-global"), \
             patch.object(orch, "elasticache_client", mock_ec):
            assert orch._get_elasticache_rg_id_in_region("us-east-2") == "rg-w2"

    def test_returns_empty_on_api_error(self):
        mock_ec = MagicMock()
        mock_ec.describe_global_replication_groups.side_effect = _client_error()
        with patch.object(orch, "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", "my-global"), \
             patch.object(orch, "elasticache_client", mock_ec):
            assert orch._get_elasticache_rg_id_in_region("us-east-2") == ""


# ===========================================================================
# 5. Combined gate logic (Aurora + Redis promotion)
# ===========================================================================

class TestCombinedPromotionGate:
    """Tests for the handler gate logic with both Aurora and Redis pending."""

    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "_handle_elasticache_promotion_reminder",
                  return_value={"statusCode": 200, "body": "waiting"})
    @patch.object(orch, "_handle_aurora_promotion_reminder",
                  return_value={"statusCode": 200, "body": "waiting"})
    @patch.object(orch, "get_failover_state")
    def test_both_pending_waits(self, mock_get_state, mock_aurora_rem, mock_redis_rem, mock_publish):
        """When both are pending and not yet cleared, returns waiting."""
        mock_get_state.return_value = _make_state(
            aurora_promotion_pending=True,
            redis_promotion_pending=True,
        )
        with patch.object(orch, "CURRENT_REGION", "us-east-2"), \
             patch.object(orch, "ROUTING_MODE", "failover"):
            result = orch.handler({}, None)
        mock_aurora_rem.assert_called_once()
        mock_redis_rem.assert_called_once()
        assert "Waiting" in result["body"]

    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "_handle_elasticache_promotion_reminder",
                  return_value={"statusCode": 200, "body": "waiting"})
    @patch.object(orch, "_handle_aurora_promotion_reminder",
                  return_value={"statusCode": 200, "body": "confirmed"})
    @patch.object(orch, "get_failover_state")
    def test_aurora_done_redis_pending_still_waits(self, mock_get_state, mock_aurora_rem, mock_redis_rem, mock_publish):
        """When Aurora is cleared but Redis still pending, returns waiting."""
        mock_get_state.return_value = _make_state(
            aurora_promotion_pending=False,
            redis_promotion_pending=True,
        )
        with patch.object(orch, "CURRENT_REGION", "us-east-2"), \
             patch.object(orch, "ROUTING_MODE", "failover"):
            result = orch.handler({}, None)
        assert "Waiting" in result["body"]

    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "get_failover_state")
    def test_both_cleared_transitions_to_active(self, mock_get_state, mock_health, mock_update, mock_publish):
        """When both flags are False, falls through to _handle_active_region which transitions."""
        mock_get_state.return_value = _make_state(
            aurora_promotion_pending=False,
            redis_promotion_pending=False,
        )
        mock_health.return_value = {"healthy": True, "signals": [], "decision_reason": "OK"}
        with patch.object(orch, "CURRENT_REGION", "us-east-2"), \
             patch.object(orch, "ROUTING_MODE", "failover"):
            result = orch.handler({}, None)
        # Should have called update_failover_state to transition state
        calls = [str(c) for c in mock_update.call_args_list]
        assert any("SECONDARY_ACTIVE" in c for c in calls)

    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "_handle_aurora_promotion_reminder",
                  return_value={"statusCode": 200, "body": "waiting"})
    @patch.object(orch, "get_failover_state")
    def test_redis_not_configured_only_checks_aurora(self, mock_get_state, mock_aurora_rem, mock_publish):
        """When redis_promotion_pending=False (not configured), only Aurora matters."""
        mock_get_state.return_value = _make_state(
            aurora_promotion_pending=True,
            redis_promotion_pending=False,
        )
        with patch.object(orch, "CURRENT_REGION", "us-east-2"), \
             patch.object(orch, "ROUTING_MODE", "failover"):
            result = orch.handler({}, None)
        mock_aurora_rem.assert_called_once()
        assert "Waiting" in result["body"]


# ===========================================================================
# 6. redis_promotion_pending field behavior
# ===========================================================================

class TestRedisPromotionPendingField:
    """Tests that redis_promotion_pending is set correctly based on env var."""

    @patch.object(orch, "send_notification")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "_emit_failover_event")
    @patch.object(orch, "try_claim_failover", return_value=True)
    @patch.object(orch, "try_increment_failures", return_value=True)
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "_run_rca_analysis", return_value="")
    def test_failover_sets_redis_pending_when_configured(
        self, mock_rca, mock_update, mock_health, mock_try_inc,
        mock_claim, mock_emit, mock_publish, mock_notify
    ):
        """When ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID is set, redis_promotion_pending=True."""
        mock_health.return_value = {"healthy": False, "signals": [], "decision_reason": "test"}
        old_ts = "1970-01-01T00:00:00Z"

        with patch.object(orch, "CURRENT_REGION", "us-east-1"), \
             patch.object(orch, "FAILOVER_MODE", "auto"), \
             patch.object(orch, "CONSECUTIVE_FAILURES_THRESHOLD", 3), \
             patch.object(orch, "AURORA_AUTO_PROMOTE", False), \
             patch.object(orch, "ELASTICACHE_AUTO_PROMOTE", False), \
             patch.object(orch, "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", "my-global"):
            state = _make_state(
                active_region="us-east-1",
                state="PRIMARY_ACTIVE",
                consecutive_failures=2,
                aurora_promotion_pending=False,
                redis_promotion_pending=False,
            )
            orch._handle_active_region(state, "us-east-1", 2, old_ts)

        claim_updates = mock_claim.call_args[0][1]
        assert claim_updates["redis_promotion_pending"] is True

    @patch.object(orch, "send_notification")
    @patch.object(orch, "publish_region_health_metric")
    @patch.object(orch, "_emit_failover_event")
    @patch.object(orch, "try_claim_failover", return_value=True)
    @patch.object(orch, "try_increment_failures", return_value=True)
    @patch.object(orch, "evaluate_region_health")
    @patch.object(orch, "update_failover_state")
    @patch.object(orch, "_run_rca_analysis", return_value="")
    def test_failover_sets_redis_pending_false_when_not_configured(
        self, mock_rca, mock_update, mock_health, mock_try_inc,
        mock_claim, mock_emit, mock_publish, mock_notify
    ):
        """When ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID is empty, redis_promotion_pending=False."""
        mock_health.return_value = {"healthy": False, "signals": [], "decision_reason": "test"}
        old_ts = "1970-01-01T00:00:00Z"

        with patch.object(orch, "CURRENT_REGION", "us-east-1"), \
             patch.object(orch, "FAILOVER_MODE", "auto"), \
             patch.object(orch, "CONSECUTIVE_FAILURES_THRESHOLD", 3), \
             patch.object(orch, "AURORA_AUTO_PROMOTE", False), \
             patch.object(orch, "ELASTICACHE_AUTO_PROMOTE", False), \
             patch.object(orch, "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", ""):
            state = _make_state(
                active_region="us-east-1",
                state="PRIMARY_ACTIVE",
                consecutive_failures=2,
                aurora_promotion_pending=False,
                redis_promotion_pending=False,
            )
            orch._handle_active_region(state, "us-east-1", 2, old_ts)

        claim_updates = mock_claim.call_args[0][1]
        assert claim_updates["redis_promotion_pending"] is False


# ===========================================================================
# 7. build_elasticache_promotion_commands()
# ===========================================================================

class TestBuildElasticachePromotionCommands:
    """Tests for ElastiCache CLI command builder."""

    def test_returns_empty_when_not_configured(self):
        with patch.object(orch, "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", ""):
            assert orch.build_elasticache_promotion_commands("us-east-2") == ""

    def test_returns_commands_when_configured(self):
        with patch.object(orch, "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", "my-global"):
            result = orch.build_elasticache_promotion_commands("us-east-2")
            assert "failover-global-replication-group" in result
            assert "my-global" in result
            assert "us-east-2" in result
