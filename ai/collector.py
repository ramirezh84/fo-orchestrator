"""
Collects incident context from AWS services at failover time.

Gathers CloudWatch Logs, ECS events, Aurora status, ALB health,
and API Gateway metrics into a structured dict for LLM analysis.
"""

import logging
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

from ai.config import AI_RCA_LOG_WINDOW_MINUTES, AI_RCA_MAX_LOG_LINES

logger = logging.getLogger(__name__)


def collect_incident_context(
    region: str,
    health_signals: dict,
    ecs_cluster: str,
    ecs_service: str,
    aurora_cluster_id: str,
    alb_arn: str | None = None,
    api_gw_id: str | None = None,
    log_group: str | None = None,
) -> dict:
    """
    Collect incident context from multiple AWS sources.

    Returns a structured dict with all available context.
    Partial failures are captured — missing data does not block collection.
    """
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(minutes=AI_RCA_LOG_WINDOW_MINUTES)

    context = {
        "timestamp": now.isoformat(),
        "region": region,
        "health_signals": health_signals,
        "window_minutes": AI_RCA_LOG_WINDOW_MINUTES,
        "ecs_events": _collect_ecs_events(region, ecs_cluster, ecs_service),
        "aurora_status": _collect_aurora_status(region, aurora_cluster_id),
    }

    if log_group:
        context["application_logs"] = _collect_cloudwatch_logs(
            region, log_group, window_start, now
        )

    if alb_arn:
        context["alb_health"] = _collect_alb_health(region, alb_arn)

    return context


def _collect_ecs_events(region: str, cluster: str, service: str) -> dict:
    """Collect recent ECS service events (deployments, task failures)."""
    try:
        ecs = boto3.client("ecs", region_name=region)
        resp = ecs.describe_services(cluster=cluster, services=[service])
        if not resp["services"]:
            return {"error": f"Service {service} not found in cluster {cluster}"}

        svc = resp["services"][0]
        return {
            "status": svc.get("status"),
            "running_count": svc.get("runningCount"),
            "desired_count": svc.get("desiredCount"),
            "pending_count": svc.get("pendingCount"),
            "events": [
                {"timestamp": e["createdAt"].isoformat(), "message": e["message"]}
                for e in svc.get("events", [])[:10]
            ],
            "deployments": [
                {
                    "status": d.get("status"),
                    "running_count": d.get("runningCount"),
                    "desired_count": d.get("desiredCount"),
                    "rollout_state": d.get("rolloutState"),
                }
                for d in svc.get("deployments", [])
            ],
        }
    except (ClientError, Exception) as e:
        logger.warning(f"Failed to collect ECS events: {e}")
        return {"error": str(e)}


def _collect_aurora_status(region: str, cluster_id: str) -> dict:
    """Collect Aurora cluster status and recent events."""
    try:
        rds = boto3.client("rds", region_name=region)
        resp = rds.describe_db_clusters(DBClusterIdentifier=cluster_id)
        if not resp["DBClusters"]:
            return {"error": f"Cluster {cluster_id} not found"}

        cluster = resp["DBClusters"][0]
        result = {
            "status": cluster.get("Status"),
            "engine": cluster.get("Engine"),
            "multi_az": cluster.get("MultiAZ"),
            "replication_source": cluster.get("ReplicationSourceIdentifier", "none"),
            "members": [
                {
                    "instance_id": m.get("DBInstanceIdentifier"),
                    "is_writer": m.get("IsClusterWriter"),
                }
                for m in cluster.get("DBClusterMembers", [])
            ],
        }

        # Recent RDS events for this cluster
        events_resp = rds.describe_events(
            SourceIdentifier=cluster_id,
            SourceType="db-cluster",
            Duration=AI_RCA_LOG_WINDOW_MINUTES,
        )
        result["recent_events"] = [
            {"timestamp": e["Date"].isoformat(), "message": e["Message"]}
            for e in events_resp.get("Events", [])
        ]
        return result
    except (ClientError, Exception) as e:
        logger.warning(f"Failed to collect Aurora status: {e}")
        return {"error": str(e)}


def _collect_cloudwatch_logs(
    region: str, log_group: str, start: datetime, end: datetime
) -> dict:
    """Collect recent application logs from CloudWatch Logs."""
    try:
        logs = boto3.client("logs", region_name=region)
        resp = logs.filter_log_events(
            logGroupName=log_group,
            startTime=int(start.timestamp() * 1000),
            endTime=int(end.timestamp() * 1000),
            limit=AI_RCA_MAX_LOG_LINES,
            filterPattern="?ERROR ?WARN ?Exception ?FATAL ?CRITICAL",
        )
        events = resp.get("events", [])
        return {
            "count": len(events),
            "lines": [
                {"timestamp": e.get("timestamp"), "message": e.get("message", "").strip()}
                for e in events
            ],
        }
    except (ClientError, Exception) as e:
        logger.warning(f"Failed to collect CloudWatch logs: {e}")
        return {"error": str(e)}


def _collect_alb_health(region: str, alb_arn: str) -> dict:
    """Collect ALB target health status."""
    try:
        elbv2 = boto3.client("elbv2", region_name=region)

        # Get target groups for this ALB
        tg_resp = elbv2.describe_target_groups(LoadBalancerArn=alb_arn)
        results = []
        for tg in tg_resp.get("TargetGroups", []):
            health_resp = elbv2.describe_target_health(
                TargetGroupArn=tg["TargetGroupArn"]
            )
            results.append({
                "target_group": tg.get("TargetGroupName"),
                "targets": [
                    {
                        "id": t["Target"]["Id"],
                        "port": t["Target"].get("Port"),
                        "state": t["TargetHealth"]["State"],
                        "reason": t["TargetHealth"].get("Reason", ""),
                    }
                    for t in health_resp.get("TargetHealthDescriptions", [])
                ],
            })
        return {"target_groups": results}
    except (ClientError, Exception) as e:
        logger.warning(f"Failed to collect ALB health: {e}")
        return {"error": str(e)}
