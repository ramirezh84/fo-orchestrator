#!/usr/bin/env python3
"""
v1.7.1 (F10) tests for failback Lambda wait-for-promotion-completion logic.

The v1.7 drill surfaced F10: SwitchoverGlobalCluster and FailoverGlobalReplicationGroup
return immediately, but the actual writer/primary flip takes 1-3 minutes. The previous
failback flow ran validate_target_region_health right after the API call, saw the data
tier still in the wrong region, and refused with a 400 — even though the auto-promote
was correctly in flight.

The fix: _auto_switchover_aurora and _auto_failover_redis now block on the writer/primary
flip via wait_for_aurora_writer / wait_for_redis_primary before returning. Configurable
timeout via FAILBACK_PROMOTION_WAIT_TIMEOUT_SECONDS (default 480s).

Run: python3 -m pytest tests/test_v171_failback_wait.py -v
"""

import os
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
}
for k, v in _MIN_ENV.items():
    os.environ.setdefault(k, v)

_mock_boto3 = patch("boto3.client")
_mock_boto3.start()
_mock_create_backend = patch("state_backend.create_backend")
_mock_create_backend.start()

import manual_failback_v2 as failback  # noqa: E402

_mock_boto3.stop()
_mock_create_backend.stop()


# ---------------------------------------------------------------------------
# wait_for_aurora_writer
# ---------------------------------------------------------------------------

class TestWaitForAuroraWriter:
    def test_returns_success_immediately_when_writer_already_correct(self):
        """The very first poll sees the writer in target_region — return fast."""
        rds = MagicMock()
        rds.describe_global_clusters.return_value = {
            "GlobalClusters": [{
                "GlobalClusterMembers": [
                    {"DBClusterArn": "arn:aws:rds:us-east-2:account:cluster:c-e2", "IsWriter": False},
                    {"DBClusterArn": "arn:aws:rds:us-east-1:account:cluster:c-e1", "IsWriter": True},
                ]
            }]
        }
        with patch("boto3.client", return_value=rds), \
             patch.object(failback, "AURORA_GLOBAL_CLUSTER_ID", "test-aurora-global"), \
             patch.object(failback, "CURRENT_REGION", "us-east-1"):
            result = failback.wait_for_aurora_writer("us-east-1", timeout_seconds=10)
        assert result["success"] is True
        assert result["elapsed_seconds"] >= 0
        rds.describe_global_clusters.assert_called()

    def test_returns_failure_on_timeout(self):
        """Writer never flips → returns success=False with elapsed and error."""
        rds = MagicMock()
        # Writer stays in us-east-2 forever
        rds.describe_global_clusters.return_value = {
            "GlobalClusters": [{
                "GlobalClusterMembers": [
                    {"DBClusterArn": "arn:aws:rds:us-east-1:account:cluster:c-e1", "IsWriter": False},
                    {"DBClusterArn": "arn:aws:rds:us-east-2:account:cluster:c-e2", "IsWriter": True},
                ]
            }]
        }
        with patch("boto3.client", return_value=rds), \
             patch.object(failback, "AURORA_GLOBAL_CLUSTER_ID", "test-aurora-global"), \
             patch.object(failback, "CURRENT_REGION", "us-east-1"), \
             patch.object(failback.time, "sleep"):  # don't actually sleep
            result = failback.wait_for_aurora_writer("us-east-1", timeout_seconds=1)
        assert result["success"] is False
        assert "did not complete" in result["error"]

    def test_returns_failure_when_global_cluster_id_unset(self):
        with patch.object(failback, "AURORA_GLOBAL_CLUSTER_ID", ""):
            result = failback.wait_for_aurora_writer("us-east-1", timeout_seconds=1)
        assert result["success"] is False
        assert "AURORA_GLOBAL_CLUSTER_ID" in result["error"]

    def test_swallows_clienterror_and_keeps_polling(self):
        """A transient describe_global_clusters error shouldn't kill the wait."""
        from botocore.exceptions import ClientError
        rds = MagicMock()
        first = ClientError(
            {"Error": {"Code": "Throttling", "Message": "rate"}}, "DescribeGlobalClusters"
        )
        success_resp = {
            "GlobalClusters": [{
                "GlobalClusterMembers": [
                    {"DBClusterArn": "arn:aws:rds:us-east-1:account:cluster:c-e1", "IsWriter": True},
                ]
            }]
        }
        rds.describe_global_clusters.side_effect = [first, success_resp]
        with patch("boto3.client", return_value=rds), \
             patch.object(failback, "AURORA_GLOBAL_CLUSTER_ID", "test-aurora-global"), \
             patch.object(failback, "CURRENT_REGION", "us-east-1"), \
             patch.object(failback.time, "sleep"):
            result = failback.wait_for_aurora_writer("us-east-1", timeout_seconds=30)
        assert result["success"] is True
        assert rds.describe_global_clusters.call_count == 2


# ---------------------------------------------------------------------------
# wait_for_redis_primary
# ---------------------------------------------------------------------------

class TestWaitForRedisPrimary:
    def test_returns_success_when_target_is_primary(self):
        ec = MagicMock()
        ec.describe_global_replication_groups.return_value = {
            "GlobalReplicationGroups": [{
                "Members": [
                    {"ReplicationGroupRegion": "us-east-2", "Role": "SECONDARY"},
                    {"ReplicationGroupRegion": "us-east-1", "Role": "PRIMARY"},
                ]
            }]
        }
        with patch("boto3.client", return_value=ec), \
             patch.object(failback, "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", "test-rg"), \
             patch.object(failback, "CURRENT_REGION", "us-east-1"):
            result = failback.wait_for_redis_primary("us-east-1", timeout_seconds=10)
        assert result["success"] is True

    def test_returns_failure_on_timeout(self):
        ec = MagicMock()
        ec.describe_global_replication_groups.return_value = {
            "GlobalReplicationGroups": [{
                "Members": [
                    {"ReplicationGroupRegion": "us-east-1", "Role": "SECONDARY"},
                    {"ReplicationGroupRegion": "us-east-2", "Role": "PRIMARY"},
                ]
            }]
        }
        with patch("boto3.client", return_value=ec), \
             patch.object(failback, "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", "test-rg"), \
             patch.object(failback, "CURRENT_REGION", "us-east-1"), \
             patch.object(failback.time, "sleep"):
            result = failback.wait_for_redis_primary("us-east-1", timeout_seconds=1)
        assert result["success"] is False
        assert "did not complete" in result["error"]

    def test_returns_failure_when_global_rg_id_unset(self):
        with patch.object(failback, "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", ""):
            result = failback.wait_for_redis_primary("us-east-1", timeout_seconds=1)
        assert result["success"] is False


# ---------------------------------------------------------------------------
# _auto_switchover_aurora composes initiate + wait
# ---------------------------------------------------------------------------

class TestAutoSwitchoverAuroraComposesWait:
    def test_initiate_then_wait_success(self):
        """End-to-end: API call succeeds, wait sees writer flip, return success."""
        rds = MagicMock()
        # First switchover_global_cluster succeeds
        rds.switchover_global_cluster.return_value = {}
        # Then describe_global_clusters in wait_for_aurora_writer sees writer in target
        rds.describe_global_clusters.return_value = {
            "GlobalClusters": [{
                "GlobalClusterMembers": [
                    {"DBClusterArn": "arn:aws:rds:us-east-1:account:cluster:c-e1", "IsWriter": True},
                ]
            }]
        }
        with patch("boto3.client", return_value=rds), \
             patch.object(failback, "AURORA_GLOBAL_CLUSTER_ID", "test-aurora-global"), \
             patch.object(failback, "AURORA_CLUSTER_ID", "test-aurora-e1"), \
             patch.object(failback, "_AWS_ACCOUNT_ID", "123456789012"), \
             patch.object(failback, "CURRENT_REGION", "us-east-1"):
            result = failback._auto_switchover_aurora("us-east-1")
        assert result["success"] is True
        assert "elapsed_seconds" in result
        rds.switchover_global_cluster.assert_called_once()

    def test_initiate_failure_returns_immediately_no_wait(self):
        from botocore.exceptions import ClientError
        rds = MagicMock()
        rds.switchover_global_cluster.side_effect = ClientError(
            {"Error": {"Code": "InvalidParameterValue", "Message": "bad request"}},
            "SwitchoverGlobalCluster",
        )
        with patch("boto3.client", return_value=rds), \
             patch.object(failback, "AURORA_GLOBAL_CLUSTER_ID", "test-aurora-global"), \
             patch.object(failback, "AURORA_CLUSTER_ID", "test-aurora-e1"), \
             patch.object(failback, "_AWS_ACCOUNT_ID", "123456789012"), \
             patch.object(failback, "CURRENT_REGION", "us-east-1"):
            result = failback._auto_switchover_aurora("us-east-1")
        assert result["success"] is False
        assert "InvalidParameterValue" in result["error"] or "bad request" in result["error"]
        # No wait happened. _resolve_target_aurora_arn does call describe_global_clusters
        # once to find the ARN, but the wait loop never runs — total calls == 1.
        assert rds.describe_global_clusters.call_count == 1

    def test_idempotent_already_switching_over_falls_through_to_wait(self):
        """If switchover is already running, treat as success and let wait converge."""
        from botocore.exceptions import ClientError
        rds = MagicMock()
        rds.switchover_global_cluster.side_effect = ClientError(
            {"Error": {"Code": "InvalidParameterCombination",
                       "Message": "The global cluster is already switching over"}},
            "SwitchoverGlobalCluster",
        )
        rds.describe_global_clusters.return_value = {
            "GlobalClusters": [{
                "GlobalClusterMembers": [
                    {"DBClusterArn": "arn:aws:rds:us-east-1:account:cluster:c-e1", "IsWriter": True},
                ]
            }]
        }
        with patch("boto3.client", return_value=rds), \
             patch.object(failback, "AURORA_GLOBAL_CLUSTER_ID", "test-aurora-global"), \
             patch.object(failback, "AURORA_CLUSTER_ID", "test-aurora-e1"), \
             patch.object(failback, "_AWS_ACCOUNT_ID", "123456789012"), \
             patch.object(failback, "CURRENT_REGION", "us-east-1"):
            result = failback._auto_switchover_aurora("us-east-1")
        assert result["success"] is True


# ---------------------------------------------------------------------------
# _auto_failover_redis composes initiate + wait
# ---------------------------------------------------------------------------

class TestAutoFailoverRedisComposesWait:
    def test_initiate_then_wait_success(self):
        ec = MagicMock()
        ec.failover_global_replication_group.return_value = {}
        ec.describe_global_replication_groups.return_value = {
            "GlobalReplicationGroups": [{
                "Members": [
                    {"ReplicationGroupRegion": "us-east-1", "Role": "PRIMARY"},
                ]
            }]
        }
        with patch("boto3.client", return_value=ec), \
             patch.object(failback, "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", "test-rg"), \
             patch.object(failback, "ELASTICACHE_REPLICATION_GROUP_ID", "test-redis-e1"), \
             patch.object(failback, "CURRENT_REGION", "us-east-1"):
            result = failback._auto_failover_redis("us-east-1")
        assert result["success"] is True

    def test_idempotent_already_primary_falls_through_to_wait(self):
        from botocore.exceptions import ClientError
        ec = MagicMock()
        ec.failover_global_replication_group.side_effect = ClientError(
            {"Error": {"Code": "InvalidParameterValue",
                       "Message": "Region us-east-1 is already primary"}},
            "FailoverGlobalReplicationGroup",
        )
        ec.describe_global_replication_groups.return_value = {
            "GlobalReplicationGroups": [{
                "Members": [
                    {"ReplicationGroupRegion": "us-east-1", "Role": "PRIMARY"},
                ]
            }]
        }
        with patch("boto3.client", return_value=ec), \
             patch.object(failback, "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", "test-rg"), \
             patch.object(failback, "ELASTICACHE_REPLICATION_GROUP_ID", "test-redis-e1"), \
             patch.object(failback, "CURRENT_REGION", "us-east-1"):
            result = failback._auto_failover_redis("us-east-1")
        assert result["success"] is True


# ---------------------------------------------------------------------------
# Configurable timeout via env
# ---------------------------------------------------------------------------

def test_promotion_wait_timeout_respects_env_var():
    """FAILBACK_PROMOTION_WAIT_TIMEOUT_SECONDS controls the wait timeout."""
    rds = MagicMock()
    rds.switchover_global_cluster.return_value = {}
    # Writer never flips
    rds.describe_global_clusters.return_value = {
        "GlobalClusters": [{
            "GlobalClusterMembers": [
                {"DBClusterArn": "arn:aws:rds:us-east-2:account:cluster:c-e2", "IsWriter": True},
            ]
        }]
    }
    with patch("boto3.client", return_value=rds), \
         patch.dict(os.environ, {"FAILBACK_PROMOTION_WAIT_TIMEOUT_SECONDS": "1"}), \
         patch.object(failback, "AURORA_GLOBAL_CLUSTER_ID", "test-aurora-global"), \
         patch.object(failback, "AURORA_CLUSTER_ID", "test-aurora-e1"), \
         patch.object(failback, "_AWS_ACCOUNT_ID", "123456789012"), \
         patch.object(failback, "CURRENT_REGION", "us-east-1"), \
         patch.object(failback.time, "sleep"):
        result = failback._auto_switchover_aurora("us-east-1")
    assert result["success"] is False
    # Timeout error message
    assert "did not complete" in result["error"]
