#!/usr/bin/env python3
"""
Tests for the stability collector module.

Run: python3 -m pytest tests/test_stability_collector.py -v
"""

import os
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

os.environ.setdefault("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:123456789:test")
os.environ.setdefault("AI_RCA_ENABLED", "false")


class TestCloudWatchMetricSeries:
    """Tests for the generic CloudWatch metric helper."""

    @patch("ai.stability_collector.boto3")
    def test_returns_sorted_datapoints(self, mock_boto3):
        from ai.stability_collector import _collect_cloudwatch_metric_series

        mock_cw = MagicMock()
        mock_boto3.client.return_value = mock_cw
        mock_cw.get_metric_data.return_value = {
            "MetricDataResults": [{
                "Values": [5.0, 3.0, 8.0],
                "Timestamps": [
                    datetime(2026, 4, 3, 10, 2, tzinfo=timezone.utc),
                    datetime(2026, 4, 3, 10, 0, tzinfo=timezone.utc),
                    datetime(2026, 4, 3, 10, 4, tzinfo=timezone.utc),
                ],
            }]
        }

        result = _collect_cloudwatch_metric_series(
            region="us-east-1", namespace="AWS/RDS",
            metric_name="AuroraReplicaLag",
            dimensions=[{"Name": "DBInstanceIdentifier", "Value": "inst-1"}],
            stat="Average", period=60, window_minutes=10,
        )

        assert len(result["datapoints"]) == 3
        # Should be sorted chronologically
        assert result["datapoints"][0]["value"] == 3.0
        assert result["datapoints"][1]["value"] == 5.0
        assert result["datapoints"][2]["value"] == 8.0

    @patch("ai.stability_collector.boto3")
    def test_computes_summary_statistics(self, mock_boto3):
        from ai.stability_collector import _collect_cloudwatch_metric_series

        mock_cw = MagicMock()
        mock_boto3.client.return_value = mock_cw
        mock_cw.get_metric_data.return_value = {
            "MetricDataResults": [{
                "Values": [10.0, 20.0, 30.0],
                "Timestamps": [
                    datetime(2026, 4, 3, 10, 0, tzinfo=timezone.utc),
                    datetime(2026, 4, 3, 10, 1, tzinfo=timezone.utc),
                    datetime(2026, 4, 3, 10, 2, tzinfo=timezone.utc),
                ],
            }]
        }

        result = _collect_cloudwatch_metric_series(
            region="us-east-1", namespace="AWS/RDS",
            metric_name="AuroraReplicaLag",
            dimensions=[], stat="Average", period=60, window_minutes=10,
        )

        assert result["summary"]["min"] == 10.0
        assert result["summary"]["max"] == 30.0
        assert result["summary"]["avg"] == 20.0
        assert result["summary"]["latest"] == 30.0
        assert result["summary"]["datapoint_count"] == 3

    @patch("ai.stability_collector.boto3")
    def test_handles_no_data(self, mock_boto3):
        from ai.stability_collector import _collect_cloudwatch_metric_series

        mock_cw = MagicMock()
        mock_boto3.client.return_value = mock_cw
        mock_cw.get_metric_data.return_value = {
            "MetricDataResults": [{"Values": [], "Timestamps": []}]
        }

        result = _collect_cloudwatch_metric_series(
            region="us-east-1", namespace="AWS/RDS",
            metric_name="AuroraReplicaLag",
            dimensions=[], stat="Average", period=60, window_minutes=10,
        )

        assert result["datapoints"] == []
        assert result["summary"] is None
        assert "No data" in result["note"]

    @patch("ai.stability_collector.boto3")
    def test_handles_client_error(self, mock_boto3):
        from ai.stability_collector import _collect_cloudwatch_metric_series

        mock_cw = MagicMock()
        mock_boto3.client.return_value = mock_cw
        mock_cw.get_metric_data.side_effect = ClientError(
            {"Error": {"Code": "InternalError", "Message": "fail"}},
            "GetMetricData",
        )

        result = _collect_cloudwatch_metric_series(
            region="us-east-1", namespace="AWS/RDS",
            metric_name="AuroraReplicaLag",
            dimensions=[], stat="Average", period=60, window_minutes=10,
        )

        assert "error" in result


class TestAuroraReplicationLag:
    """Tests for Aurora replication lag collection."""

    @patch("ai.stability_collector._collect_cloudwatch_metric_series")
    @patch("ai.stability_collector.boto3")
    def test_collects_lag_for_replicas(self, mock_boto3, mock_cw_series):
        from ai.stability_collector import _collect_aurora_replication_lag

        mock_rds = MagicMock()
        mock_boto3.client.return_value = mock_rds
        mock_rds.describe_db_clusters.return_value = {
            "DBClusters": [{
                "DBClusterMembers": [
                    {"DBInstanceIdentifier": "writer-1", "IsClusterWriter": True},
                    {"DBInstanceIdentifier": "reader-1", "IsClusterWriter": False},
                ]
            }]
        }
        mock_cw_series.return_value = {
            "datapoints": [{"timestamp": "t1", "value": 5.0}],
            "summary": {"min": 5.0, "max": 5.0, "avg": 5.0, "latest": 5.0, "datapoint_count": 1},
        }

        result = _collect_aurora_replication_lag("us-east-1", "my-cluster", 10)

        assert result["replica_count"] == 1
        assert "reader-1" in result["replicas"]
        mock_cw_series.assert_called_once()

    @patch("ai.stability_collector.boto3")
    def test_no_replicas(self, mock_boto3):
        from ai.stability_collector import _collect_aurora_replication_lag

        mock_rds = MagicMock()
        mock_boto3.client.return_value = mock_rds
        mock_rds.describe_db_clusters.return_value = {
            "DBClusters": [{
                "DBClusterMembers": [
                    {"DBInstanceIdentifier": "writer-1", "IsClusterWriter": True},
                ]
            }]
        }

        result = _collect_aurora_replication_lag("us-east-1", "my-cluster", 10)

        assert result["replica_count"] == 0

    @patch("ai.stability_collector.boto3")
    def test_cluster_not_found(self, mock_boto3):
        from ai.stability_collector import _collect_aurora_replication_lag

        mock_rds = MagicMock()
        mock_boto3.client.return_value = mock_rds
        mock_rds.describe_db_clusters.return_value = {"DBClusters": []}

        result = _collect_aurora_replication_lag("us-east-1", "missing", 10)

        assert "error" in result


class TestAuroraClusterDetail:
    """Tests for Aurora cluster detail collection."""

    @patch("ai.stability_collector.boto3")
    def test_collects_detail(self, mock_boto3):
        from ai.stability_collector import _collect_aurora_cluster_detail

        mock_rds = MagicMock()
        mock_boto3.client.return_value = mock_rds
        mock_rds.describe_db_clusters.return_value = {
            "DBClusters": [{
                "Status": "available",
                "Engine": "aurora-postgresql",
                "EngineVersion": "15.4",
                "MultiAZ": True,
                "ReplicationSourceIdentifier": "",
                "LatestRestorableTime": datetime(2026, 4, 3, 10, 0, tzinfo=timezone.utc),
                "PendingModifiedValues": {},
            }]
        }

        result = _collect_aurora_cluster_detail("us-east-1", "my-cluster")

        assert result["status"] == "available"
        assert result["is_writer"] is True
        assert result["latest_restorable_time"] is not None
        assert "restorable_gap_seconds" in result

    @patch("ai.stability_collector.boto3")
    def test_replica_cluster(self, mock_boto3):
        from ai.stability_collector import _collect_aurora_cluster_detail

        mock_rds = MagicMock()
        mock_boto3.client.return_value = mock_rds
        mock_rds.describe_db_clusters.return_value = {
            "DBClusters": [{
                "Status": "available",
                "Engine": "aurora-postgresql",
                "EngineVersion": "15.4",
                "MultiAZ": False,
                "ReplicationSourceIdentifier": "arn:aws:rds:us-west-1:123:cluster:primary",
                "LatestRestorableTime": None,
                "PendingModifiedValues": {},
            }]
        }

        result = _collect_aurora_cluster_detail("us-west-2", "my-replica")

        assert result["is_writer"] is False
        assert result["replication_source"] != ""


class TestAuroraGlobalTopology:
    """Tests for Aurora global cluster topology collection."""

    @patch("ai.stability_collector.boto3")
    def test_collects_topology(self, mock_boto3):
        from ai.stability_collector import _collect_aurora_global_topology

        mock_rds = MagicMock()
        mock_boto3.client.return_value = mock_rds
        mock_rds.describe_global_clusters.return_value = {
            "GlobalClusters": [{
                "GlobalClusterIdentifier": "my-global",
                "Status": "available",
                "Engine": "aurora-postgresql",
                "GlobalClusterMembers": [
                    {
                        "DBClusterArn": "arn:aws:rds:us-west-1:123:cluster:primary",
                        "IsWriter": True,
                        "GlobalWriteForwardingStatus": "disabled",
                        "SynchronizationStatus": "connected",
                    },
                    {
                        "DBClusterArn": "arn:aws:rds:us-west-2:123:cluster:secondary",
                        "IsWriter": False,
                        "GlobalWriteForwardingStatus": "disabled",
                        "SynchronizationStatus": "connected",
                    },
                ],
            }]
        }

        result = _collect_aurora_global_topology("my-global", "us-west-1")

        assert result["status"] == "available"
        assert len(result["members"]) == 2
        assert result["members"][0]["is_writer"] is True
        assert result["members"][1]["synchronization_status"] == "connected"

    @patch("ai.stability_collector.boto3")
    def test_global_not_found(self, mock_boto3):
        from ai.stability_collector import _collect_aurora_global_topology

        mock_rds = MagicMock()
        mock_boto3.client.return_value = mock_rds
        mock_rds.describe_global_clusters.return_value = {"GlobalClusters": []}

        result = _collect_aurora_global_topology("missing", "us-west-1")

        assert "error" in result

    @patch("ai.stability_collector.boto3")
    def test_handles_client_error(self, mock_boto3):
        from ai.stability_collector import _collect_aurora_global_topology

        mock_rds = MagicMock()
        mock_boto3.client.return_value = mock_rds
        mock_rds.describe_global_clusters.side_effect = ClientError(
            {"Error": {"Code": "GlobalClusterNotFoundFault", "Message": "nope"}},
            "DescribeGlobalClusters",
        )

        result = _collect_aurora_global_topology("missing", "us-west-1")

        assert "error" in result


class TestAuroraInstanceStatus:
    """Tests for Aurora instance status collection."""

    @patch("ai.stability_collector.boto3")
    def test_collects_all_instances(self, mock_boto3):
        from ai.stability_collector import _collect_aurora_instance_status

        mock_rds = MagicMock()
        mock_boto3.client.return_value = mock_rds
        mock_rds.describe_db_clusters.return_value = {
            "DBClusters": [{
                "DBClusterMembers": [
                    {"DBInstanceIdentifier": "writer-1", "IsClusterWriter": True},
                    {"DBInstanceIdentifier": "reader-1", "IsClusterWriter": False},
                ]
            }]
        }
        mock_rds.describe_db_instances.side_effect = [
            {"DBInstances": [{"DBInstanceStatus": "available", "DBInstanceClass": "db.r6g.large", "PendingModifiedValues": {}}]},
            {"DBInstances": [{"DBInstanceStatus": "available", "DBInstanceClass": "db.r6g.large", "PendingModifiedValues": {}}]},
        ]

        result = _collect_aurora_instance_status("us-east-1", "my-cluster")

        assert len(result["instances"]) == 2
        assert result["instances"][0]["is_writer"] is True
        assert result["instances"][0]["status"] == "available"

    @patch("ai.stability_collector.boto3")
    def test_handles_instance_describe_failure(self, mock_boto3):
        from ai.stability_collector import _collect_aurora_instance_status

        mock_rds = MagicMock()
        mock_boto3.client.return_value = mock_rds
        mock_rds.describe_db_clusters.return_value = {
            "DBClusters": [{
                "DBClusterMembers": [
                    {"DBInstanceIdentifier": "writer-1", "IsClusterWriter": True},
                ]
            }]
        }
        mock_rds.describe_db_instances.side_effect = ClientError(
            {"Error": {"Code": "DBInstanceNotFound", "Message": "nope"}},
            "DescribeDBInstances",
        )

        result = _collect_aurora_instance_status("us-east-1", "my-cluster")

        assert len(result["instances"]) == 1
        assert "error" in result["instances"][0]


class TestECSTaskStability:
    """Tests for ECS task stability collection."""

    @patch("ai.stability_collector._collect_cloudwatch_metric_series")
    @patch("ai.stability_collector.boto3")
    def test_collects_stability(self, mock_boto3, mock_cw_series):
        from ai.stability_collector import _collect_ecs_task_stability

        mock_ecs = MagicMock()
        mock_boto3.client.return_value = mock_ecs
        mock_ecs.list_tasks.return_value = {"taskArns": ["arn:task1", "arn:task2"]}
        mock_cw_series.return_value = {
            "datapoints": [{"timestamp": "t1", "value": 4.0}],
            "summary": {"min": 4.0, "max": 4.0, "avg": 4.0, "latest": 4.0, "datapoint_count": 1},
        }

        result = _collect_ecs_task_stability("us-east-1", "cluster", "service", 10)

        assert "running_count_trend" in result
        assert result["recently_stopped_tasks"] == 2

    @patch("ai.stability_collector._collect_cloudwatch_metric_series")
    @patch("ai.stability_collector.boto3")
    def test_handles_stopped_tasks_error(self, mock_boto3, mock_cw_series):
        from ai.stability_collector import _collect_ecs_task_stability

        mock_ecs = MagicMock()
        mock_boto3.client.return_value = mock_ecs
        mock_ecs.list_tasks.side_effect = ClientError(
            {"Error": {"Code": "ClusterNotFoundException", "Message": "nope"}},
            "ListTasks",
        )
        mock_cw_series.return_value = {"datapoints": [], "summary": None, "note": "No data"}

        result = _collect_ecs_task_stability("us-east-1", "bad", "service", 10)

        assert result["recently_stopped_tasks"] is None


class TestCollectStabilityContext:
    """Tests for the top-level assembler."""

    @patch("ai.stability_collector._collect_alb_error_trend")
    @patch("ai.stability_collector._collect_ecs_task_stability")
    @patch("ai.stability_collector._collect_aurora_global_topology")
    @patch("ai.stability_collector._collect_aurora_events")
    @patch("ai.stability_collector._collect_aurora_instance_status")
    @patch("ai.stability_collector._collect_aurora_cluster_detail")
    @patch("ai.stability_collector._collect_aurora_replication_lag")
    def test_assembles_all_sources(
        self, mock_lag, mock_detail, mock_inst, mock_events,
        mock_global, mock_ecs, mock_alb
    ):
        from ai.stability_collector import collect_stability_context

        mock_lag.return_value = {"replica_count": 1}
        mock_detail.return_value = {"status": "available"}
        mock_inst.return_value = {"instances": []}
        mock_events.return_value = {"events": []}
        mock_global.return_value = {"status": "available"}
        mock_ecs.return_value = {"running_count_trend": {}}
        mock_alb.return_value = {"5xx_count": {}}

        ctx = collect_stability_context(
            region="us-east-1",
            aurora_cluster_id="cluster-1",
            aurora_global_cluster_id="global-1",
            ecs_cluster="ecs-cluster",
            ecs_service="ecs-service",
            alb_arn_suffix="app/my-alb/abc",
            window_minutes=10,
        )

        assert ctx["region"] == "us-east-1"
        assert ctx["window_minutes"] == 10
        assert "aurora_replication_lag" in ctx
        assert "aurora_cluster_detail" in ctx
        assert "aurora_instance_status" in ctx
        assert "aurora_events" in ctx
        assert "aurora_global_topology" in ctx
        assert "ecs_task_stability" in ctx
        assert "alb_error_trend" in ctx

    def test_skips_unconfigured_sources(self):
        from ai.stability_collector import collect_stability_context

        ctx = collect_stability_context(
            region="us-east-1",
            window_minutes=5,
        )

        assert ctx["region"] == "us-east-1"
        assert "aurora_replication_lag" not in ctx
        assert "ecs_task_stability" not in ctx
        assert "alb_error_trend" not in ctx

    @patch("ai.stability_collector._collect_aurora_events")
    @patch("ai.stability_collector._collect_aurora_instance_status")
    @patch("ai.stability_collector._collect_aurora_cluster_detail")
    @patch("ai.stability_collector._collect_aurora_replication_lag")
    def test_aurora_only(self, mock_lag, mock_detail, mock_inst, mock_events):
        from ai.stability_collector import collect_stability_context

        mock_lag.return_value = {"replica_count": 0}
        mock_detail.return_value = {"status": "available"}
        mock_inst.return_value = {"instances": []}
        mock_events.return_value = {"events": []}

        ctx = collect_stability_context(
            region="us-east-1",
            aurora_cluster_id="cluster-1",
        )

        assert "aurora_replication_lag" in ctx
        assert "aurora_global_topology" not in ctx
        assert "ecs_task_stability" not in ctx
