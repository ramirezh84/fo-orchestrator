"""
Collects time-series stability data from AWS services.

Unlike collector.py (point-in-time snapshots), this module gathers trend data
over a configurable window for stability analysis. Used by both failback
readiness assessment and aurora promotion advisor.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from ai.config import (
    AI_AURORA_STABILITY_WINDOW_MINUTES,
    AI_FAILBACK_STABILITY_WINDOW_MINUTES,
)

logger = logging.getLogger(__name__)


def collect_stability_context(
    region: str,
    aurora_cluster_id: str = "",
    aurora_global_cluster_id: str = "",
    ecs_cluster: str = "",
    ecs_service: str = "",
    alb_arn_suffix: str = "",
    window_minutes: int = None,
) -> dict:
    """
    Collect time-series stability data from multiple AWS sources.

    Returns a structured dict with trend data over the specified window.
    Partial failures are captured — missing data does not block collection.
    """
    if window_minutes is None:
        window_minutes = AI_FAILBACK_STABILITY_WINDOW_MINUTES

    now = datetime.now(timezone.utc)
    context = {
        "timestamp": now.isoformat(),
        "region": region,
        "window_minutes": window_minutes,
    }

    if aurora_cluster_id:
        context["aurora_replication_lag"] = _collect_aurora_replication_lag(
            region, aurora_cluster_id, window_minutes
        )
        context["aurora_cluster_detail"] = _collect_aurora_cluster_detail(
            region, aurora_cluster_id
        )
        context["aurora_instance_status"] = _collect_aurora_instance_status(
            region, aurora_cluster_id
        )
        context["aurora_events"] = _collect_aurora_events(
            region, aurora_cluster_id, window_minutes
        )

    if aurora_global_cluster_id:
        context["aurora_global_topology"] = _collect_aurora_global_topology(
            aurora_global_cluster_id, region
        )

    if ecs_cluster and ecs_service:
        context["ecs_task_stability"] = _collect_ecs_task_stability(
            region, ecs_cluster, ecs_service, window_minutes
        )

    if alb_arn_suffix:
        context["alb_error_trend"] = _collect_alb_error_trend(
            region, alb_arn_suffix, window_minutes
        )

    return context


# ── Aurora Replication Lag ─────────────────────────────────────────────────────


def _collect_aurora_replication_lag(
    region: str, cluster_id: str, window_minutes: int
) -> dict:
    """
    Collect AuroraReplicaLag metric trend from CloudWatch.

    This is the most critical metric for Aurora promotion decisions.
    Returns min/max/avg/latest values and raw datapoints.
    """
    try:
        rds = boto3.client("rds", region_name=region)
        resp = rds.describe_db_clusters(DBClusterIdentifier=cluster_id)
        clusters = resp.get("DBClusters", [])
        if not clusters:
            return {"error": f"Cluster {cluster_id} not found"}

        # Find replica instances (non-writer members)
        members = clusters[0].get("DBClusterMembers", [])
        replica_ids = [
            m["DBInstanceIdentifier"] for m in members
            if not m.get("IsClusterWriter", True)
        ]

        if not replica_ids:
            return {"replica_count": 0, "note": "No replicas found (single-writer cluster)"}

        # Query CloudWatch for AuroraReplicaLag on each replica
        lag_data = {}
        for instance_id in replica_ids:
            series = _collect_cloudwatch_metric_series(
                region=region,
                namespace="AWS/RDS",
                metric_name="AuroraReplicaLag",
                dimensions=[{"Name": "DBInstanceIdentifier", "Value": instance_id}],
                stat="Average",
                period=60,
                window_minutes=window_minutes,
            )
            lag_data[instance_id] = series

        return {
            "replica_count": len(replica_ids),
            "replicas": lag_data,
        }
    except (ClientError, Exception) as e:
        logger.warning(f"Failed to collect Aurora replication lag: {e}")
        return {"error": str(e)}


# ── Aurora Cluster Detail ──────────────────────────────────────────────────────


def _collect_aurora_cluster_detail(region: str, cluster_id: str) -> dict:
    """
    Collect extended Aurora cluster details for promotion analysis.

    Key field: LatestRestorableTime — gap between this and now indicates
    the unrecoverable transaction window (potential data loss on failover).
    """
    try:
        rds = boto3.client("rds", region_name=region)
        resp = rds.describe_db_clusters(DBClusterIdentifier=cluster_id)
        clusters = resp.get("DBClusters", [])
        if not clusters:
            return {"error": f"Cluster {cluster_id} not found"}

        c = clusters[0]
        latest_restorable = c.get("LatestRestorableTime")
        now = datetime.now(timezone.utc)

        result = {
            "status": c.get("Status"),
            "engine": c.get("Engine"),
            "engine_version": c.get("EngineVersion"),
            "multi_az": c.get("MultiAZ"),
            "replication_source": c.get("ReplicationSourceIdentifier", ""),
            "is_writer": not bool(c.get("ReplicationSourceIdentifier")),
            "latest_restorable_time": latest_restorable.isoformat() if latest_restorable else None,
            "pending_modified_values": c.get("PendingModifiedValues", {}),
        }

        if latest_restorable:
            gap_seconds = (now - latest_restorable).total_seconds()
            result["restorable_gap_seconds"] = round(gap_seconds, 1)

        return result
    except (ClientError, Exception) as e:
        logger.warning(f"Failed to collect Aurora cluster detail: {e}")
        return {"error": str(e)}


# ── Aurora Instance Status ─────────────────────────────────────────────────────


def _collect_aurora_instance_status(region: str, cluster_id: str) -> dict:
    """Collect status of each instance in the Aurora cluster."""
    try:
        rds = boto3.client("rds", region_name=region)
        cluster_resp = rds.describe_db_clusters(DBClusterIdentifier=cluster_id)
        clusters = cluster_resp.get("DBClusters", [])
        if not clusters:
            return {"error": f"Cluster {cluster_id} not found"}

        members = clusters[0].get("DBClusterMembers", [])
        instances = []
        for m in members:
            iid = m["DBInstanceIdentifier"]
            try:
                inst_resp = rds.describe_db_instances(DBInstanceIdentifier=iid)
                inst = inst_resp["DBInstances"][0] if inst_resp.get("DBInstances") else {}
                instances.append({
                    "instance_id": iid,
                    "is_writer": m.get("IsClusterWriter", False),
                    "status": inst.get("DBInstanceStatus", "unknown"),
                    "instance_class": inst.get("DBInstanceClass", "unknown"),
                    "pending_modified_values": inst.get("PendingModifiedValues", {}),
                })
            except (ClientError, Exception) as e:
                instances.append({
                    "instance_id": iid,
                    "is_writer": m.get("IsClusterWriter", False),
                    "error": str(e),
                })

        return {"instances": instances}
    except (ClientError, Exception) as e:
        logger.warning(f"Failed to collect Aurora instance status: {e}")
        return {"error": str(e)}


# ── Aurora Global Cluster Topology ─────────────────────────────────────────────


def _collect_aurora_global_topology(global_cluster_id: str, region: str) -> dict:
    """
    Collect global cluster topology and synchronization status.

    Key field: SynchronizationStatus — must be "connected" for safe promotion.
    """
    try:
        rds = boto3.client("rds", region_name=region)
        resp = rds.describe_global_clusters(
            GlobalClusterIdentifier=global_cluster_id
        )
        globals_list = resp.get("GlobalClusters", [])
        if not globals_list:
            return {"error": f"Global cluster {global_cluster_id} not found"}

        gc = globals_list[0]
        return {
            "global_cluster_id": gc.get("GlobalClusterIdentifier"),
            "status": gc.get("Status"),
            "engine": gc.get("Engine"),
            "members": [
                {
                    "cluster_arn": m.get("DBClusterArn", ""),
                    "is_writer": m.get("IsWriter", False),
                    "global_write_forwarding": m.get("GlobalWriteForwardingStatus", "disabled"),
                    "synchronization_status": m.get("SynchronizationStatus", "unknown"),
                }
                for m in gc.get("GlobalClusterMembers", [])
            ],
        }
    except (ClientError, Exception) as e:
        logger.warning(f"Failed to collect Aurora global topology: {e}")
        return {"error": str(e)}


# ── Aurora Events ──────────────────────────────────────────────────────────────


def _collect_aurora_events(region: str, cluster_id: str, window_minutes: int) -> dict:
    """Collect recent RDS events for the cluster."""
    try:
        rds = boto3.client("rds", region_name=region)
        resp = rds.describe_events(
            SourceIdentifier=cluster_id,
            SourceType="db-cluster",
            Duration=window_minutes,
        )
        return {
            "events": [
                {"timestamp": e["Date"].isoformat(), "message": e["Message"]}
                for e in resp.get("Events", [])
            ]
        }
    except (ClientError, Exception) as e:
        logger.warning(f"Failed to collect Aurora events: {e}")
        return {"error": str(e)}


# ── ECS Task Stability ─────────────────────────────────────────────────────────


def _collect_ecs_task_stability(
    region: str, cluster: str, service: str, window_minutes: int
) -> dict:
    """
    Collect ECS task stability data: running count trend and recent stopped tasks.
    """
    try:
        result = {}

        # Running task count trend from CloudWatch
        result["running_count_trend"] = _collect_cloudwatch_metric_series(
            region=region,
            namespace="AWS/ECS",
            metric_name="RunningTaskCount",
            dimensions=[
                {"Name": "ClusterName", "Value": cluster},
                {"Name": "ServiceName", "Value": service},
            ],
            stat="Average",
            period=60,
            window_minutes=window_minutes,
        )

        # Recent stopped tasks (indicates restarts/crashes)
        ecs = boto3.client("ecs", region_name=region)
        try:
            stopped_resp = ecs.list_tasks(
                cluster=cluster,
                serviceName=service,
                desiredStatus="STOPPED",
                maxResults=20,
            )
            stopped_count = len(stopped_resp.get("taskArns", []))
            result["recently_stopped_tasks"] = stopped_count
        except (ClientError, Exception):
            result["recently_stopped_tasks"] = None

        return result
    except (ClientError, Exception) as e:
        logger.warning(f"Failed to collect ECS task stability: {e}")
        return {"error": str(e)}


# ── ALB Error Trend ────────────────────────────────────────────────────────────


def _collect_alb_error_trend(
    region: str, alb_arn_suffix: str, window_minutes: int
) -> dict:
    """Collect ALB 5xx error rate trend from CloudWatch."""
    try:
        errors_5xx = _collect_cloudwatch_metric_series(
            region=region,
            namespace="AWS/ApplicationELB",
            metric_name="HTTPCode_Target_5XX_Count",
            dimensions=[{"Name": "LoadBalancer", "Value": alb_arn_suffix}],
            stat="Sum",
            period=60,
            window_minutes=window_minutes,
        )

        request_count = _collect_cloudwatch_metric_series(
            region=region,
            namespace="AWS/ApplicationELB",
            metric_name="RequestCount",
            dimensions=[{"Name": "LoadBalancer", "Value": alb_arn_suffix}],
            stat="Sum",
            period=60,
            window_minutes=window_minutes,
        )

        return {
            "5xx_count": errors_5xx,
            "request_count": request_count,
        }
    except (ClientError, Exception) as e:
        logger.warning(f"Failed to collect ALB error trend: {e}")
        return {"error": str(e)}


# ── CloudWatch Metric Series (shared helper) ──────────────────────────────────


def _collect_cloudwatch_metric_series(
    region: str,
    namespace: str,
    metric_name: str,
    dimensions: list,
    stat: str,
    period: int,
    window_minutes: int,
) -> dict:
    """
    Generic CloudWatch GetMetricData helper.

    Returns summary statistics (min/max/avg/latest) and raw datapoints.
    """
    try:
        cw = boto3.client("cloudwatch", region_name=region)
        now = datetime.now(timezone.utc)
        start = now - timedelta(minutes=window_minutes)

        resp = cw.get_metric_data(
            MetricDataQueries=[{
                "Id": "m1",
                "MetricStat": {
                    "Metric": {
                        "Namespace": namespace,
                        "MetricName": metric_name,
                        "Dimensions": dimensions,
                    },
                    "Period": period,
                    "Stat": stat,
                },
                "ReturnData": True,
            }],
            StartTime=start,
            EndTime=now,
        )

        results = resp.get("MetricDataResults", [])
        if not results or not results[0].get("Values"):
            return {
                "datapoints": [],
                "summary": None,
                "note": f"No data for {namespace}/{metric_name}",
            }

        values = results[0]["Values"]
        timestamps = results[0].get("Timestamps", [])

        # CloudWatch returns newest first; reverse for chronological order
        paired = list(zip(timestamps, values))
        paired.sort(key=lambda x: x[0])

        datapoints = [
            {"timestamp": ts.isoformat(), "value": round(val, 3)}
            for ts, val in paired
        ]

        summary = {
            "min": round(min(values), 3),
            "max": round(max(values), 3),
            "avg": round(sum(values) / len(values), 3),
            "latest": round(paired[-1][1], 3) if paired else None,
            "datapoint_count": len(values),
        }

        return {"datapoints": datapoints, "summary": summary}

    except (ClientError, Exception) as e:
        logger.warning(f"Failed to collect {namespace}/{metric_name}: {e}")
        return {"error": str(e)}
