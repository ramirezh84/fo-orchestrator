"""
Demo Lambda: Simulates a failover event and sends an SNS notification
with AI-powered Root Cause Analysis.
"""

import json
import os
import logging

import boto3

from ai.config import AI_RCA_MODEL
from ai.rca_analyzer import analyze_incident, format_rca_for_sns

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]


def lambda_handler(event, context):
    """Simulate a failover and send RCA notification."""

    # Simulated incident context — as if ECS tasks all died
    incident_context = {
        "region": "us-east-1",
        "timestamp": "2026-04-03T01:50:00+00:00",
        "window_minutes": 10,
        "health_signals": {
            "http": {"healthy": False, "status_code": 503, "detail": "Service Unavailable"},
            "alb": {"healthy": False, "healthy_hosts": 0, "min_required": 1},
            "ecs": {"healthy": False, "running": 0, "desired": 4},
            "api_gw": {"healthy": True, "error_rate": 2.1},
            "aurora": {"healthy": True, "status": "available"},
        },
        "ecs_events": {
            "status": "ACTIVE",
            "running_count": 0,
            "desired_count": 4,
            "events": [
                {
                    "timestamp": "2026-04-03T01:48:00Z",
                    "message": "service deposits-api has stopped 4 tasks: task abc123, task def456, task ghi789, task jkl012",
                },
                {
                    "timestamp": "2026-04-03T01:47:00Z",
                    "message": "service deposits-api has begun draining connections on 4 tasks",
                },
                {
                    "timestamp": "2026-04-03T01:46:30Z",
                    "message": "service deposits-api has started 1 tasks: task mno345 (deployment ecs-svc/123 attempt 1)",
                },
                {
                    "timestamp": "2026-04-03T01:46:00Z",
                    "message": "service deposits-api registered 1 targets in target-group deposits-tg",
                },
            ],
            "deployments": [
                {
                    "status": "PRIMARY",
                    "running_count": 0,
                    "desired_count": 4,
                    "rollout_state": "IN_PROGRESS",
                },
                {
                    "status": "ACTIVE",
                    "running_count": 0,
                    "desired_count": 4,
                    "rollout_state": "COMPLETED",
                },
            ],
        },
        "aurora_status": {
            "status": "available",
            "engine": "aurora-postgresql",
            "multi_az": True,
            "members": [
                {"instance_id": "deposits-writer-1", "is_writer": True},
                {"instance_id": "deposits-reader-1", "is_writer": False},
            ],
            "recent_events": [],
        },
        "alb_health": {
            "target_groups": [
                {
                    "target_group": "deposits-tg",
                    "targets": [
                        {
                            "id": "10.0.1.5",
                            "port": 8080,
                            "state": "draining",
                            "reason": "Target.DeregistrationInProgress",
                        },
                        {
                            "id": "10.0.1.6",
                            "port": 8080,
                            "state": "draining",
                            "reason": "Target.DeregistrationInProgress",
                        },
                    ],
                }
            ]
        },
    }

    # Run AI RCA
    logger.info("Calling Claude API for RCA analysis...")
    rca_text = analyze_incident(incident_context, region="us-east-1")
    rca_formatted = format_rca_for_sns(rca_text, incident_context)

    # Build the full failover notification (as the real orchestrator would)
    active_region = "us-east-1"
    target_region = "us-east-2"

    message = (
        f"Automated DNS failover triggered.\n\n"
        f"From: {active_region}\n"
        f"To: {target_region}\n"
        f"Time: 2026-04-03T01:50:00+00:00\n"
        f"Decision: 3 consecutive health check failures — HTTP 503, "
        f"ALB 0 healthy hosts, ECS 0/4 tasks running\n\n"
        f"DNS has been moved. Route 53 is now routing traffic to {target_region}.\n\n"
        f"ACTION REQUIRED: Aurora must be promoted MANUALLY.\n"
        f"Your app in {target_region} CANNOT WRITE until Aurora is promoted.\n\n"
        f"  aws rds switchover-global-cluster \\\n"
        f"    --global-cluster-identifier deposits-global \\\n"
        f"    --target-db-cluster-identifier arn:aws:rds:us-east-2:597088043823:cluster:deposits-secondary\n\n"
        f"Latch is ENGAGED. {active_region} will remain marked unhealthy.\n\n"
        f"Health Signals:\n{json.dumps(incident_context['health_signals'], indent=2)}"
        f"\n\n{rca_formatted}"
    )

    # Send SNS notification
    sns = boto3.client("sns", region_name="us-east-1")
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"FAILOVER: DNS moved to {target_region} - PROMOTE AURORA NOW",
        Message=message,
    )

    logger.info("SNS notification sent with RCA!")
    return {"statusCode": 200, "body": "Demo failover notification sent with AI RCA"}
