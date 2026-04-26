"""
Multi-Region Failover Orchestrator
==========================================
Deploy this Lambda in BOTH regions. Each runs on a 1-minute EventBridge schedule.

Supports three routing modes (set via ROUTING_MODE env var):

  ROUTING_MODE=failover (default):
    Active/passive. One region serves traffic. Automated failover with latch
    to prevent flip-flop. Manual failback required. See: CLAUDE.md

  ROUTING_MODE=failover + PASSIVE_PUBLISH_ZERO=true:
    Same as above, but secondary region starts with 0 containers. Application
    Auto Scaling brings containers up on failover, down on failback.
    See: zero_container_secondary_guide.md

  ROUTING_MODE=active-active:
    Both regions serve traffic via Route 53 latency-based routing. Each region
    independently evaluates its own health. No latch, no failback Lambda.
    Auto-recovery when health returns. See: active_active_guide.md

HEALTH EVALUATION (all modes):
  Five signals with quorum logic (>=50% must fail to declare unhealthy):
    1. HTTP /actuator/health on private ALB (any failure = immediately unhealthy)
    2. ALB HealthyHostCount >= MIN_HEALTHY_HOST_COUNT
    3. ECS RunningTasks >= 50% of desired
    4. API Gateway 5xx error rate < threshold
    5. Aurora cluster status = "available"

STATE BACKEND (set via STATE_BACKEND env var):
  "dynamodb" (default) — DynamoDB Global Table, sub-second cross-region replication
  "s3"                 — S3 Cross-Region Replication, ~23s replication lag

  State fields: active_region, state, latch_engaged, consecutive_failures,
  last_failover_ts, last_active_metric_ts, aurora_promotion_pending

ANTI-FLIP-FLOP (failover mode only):
  1. Consecutive failure threshold (default 3 min sustained)
  2. Cooldown window (default 30 min between failovers)
  3. Latch: old region publishes metric=0 even after recovery, until manual failback
"""

import os
import json
import logging
import ssl
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import (
    ClientError,
    EndpointConnectionError,
    HTTPClientError,
)

from state_backend import create_backend, ConditionalCheckFailedError
from notifications import (
    compose_message,
    SEVERITY_INFO,
    SEVERITY_WARNING,
    SEVERITY_CRITICAL,
)

# AI modules are imported lazily inside functions to support v1.0 mode
# (where AI_RCA_ENABLED=false and AI modules may not be needed)

# ---------------------------------------------------------------------------
# Configuration - set as Lambda environment variables
# ---------------------------------------------------------------------------
PRIMARY_REGION = os.environ.get("PRIMARY_REGION", "us-east-1")
SECONDARY_REGION = os.environ.get("SECONDARY_REGION", "us-east-2")
CURRENT_REGION = os.environ.get("AWS_REGION", "us-east-1")

# Application name - included in all SNS notifications to identify which app
# is alerting when this solution is deployed across multiple applications.
APP_NAME = os.environ.get("APP_NAME", "")

# Deployment environment (e.g., "prod", "staging", "demo"). When set together
# with APP_NAME, notification subjects are prefixed [ENVIRONMENT-APP_NAME] so
# operators can immediately tell which environment is alerting. Empty falls
# back to [APP_NAME] only (backwards-compat with v1.0–v1.5 deployments).
ENVIRONMENT = os.environ.get("ENVIRONMENT", "")

STATE_TABLE = os.environ.get("STATE_TABLE", "failover-state")
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
CW_NAMESPACE = os.environ.get("CW_NAMESPACE", "Custom/RegionFailover")
CW_METRIC_NAME = os.environ.get("CW_METRIC_NAME", "RegionActiveStatus")
FAILBACK_FUNCTION_NAME = os.environ.get("FAILBACK_FUNCTION_NAME", "failover-manual-failback")

# ---------------------------------------------------------------------------
# HTTP Health Check Configuration
# ---------------------------------------------------------------------------
HEALTH_CHECK_URL = os.environ.get("HEALTH_CHECK_URL", "")
HEALTH_ENDPOINT = os.environ.get("HEALTH_ENDPOINT", "/actuator/health")
HEALTH_CHECK_TIMEOUT_SECONDS = int(os.environ.get("HEALTH_CHECK_TIMEOUT_SECONDS", "5"))
HEALTHY_STATUS_CODES = {200}

# Set to "true" to skip SSL certificate verification for HTTPS health checks.
# Required when the ALB uses a self-signed or internal CA certificate.
HEALTH_CHECK_DISABLE_SSL_VERIFY = os.environ.get(
    "HEALTH_CHECK_DISABLE_SSL_VERIFY", "false"
).lower() == "true"

# Build SSL context once at module level
_ssl_context = None
if HEALTH_CHECK_DISABLE_SSL_VERIFY:
    _ssl_context = ssl.create_default_context()
    _ssl_context.check_hostname = False
    _ssl_context.verify_mode = ssl.CERT_NONE

# ---------------------------------------------------------------------------
# CloudWatch Metric Resource Identifiers (local region)
# ---------------------------------------------------------------------------
ALB_ARN_SUFFIX = os.environ.get("ALB_ARN_SUFFIX", "")
ALB_FULL_ARN = os.environ.get("ALB_FULL_ARN", "")
TG_ARN_SUFFIX = os.environ.get("TG_ARN_SUFFIX", "")
ECS_CLUSTER_NAME = os.environ.get("ECS_CLUSTER_NAME", "")
ECS_SERVICE_NAME = os.environ.get("ECS_SERVICE_NAME", "")
API_GW_NAME = os.environ.get("API_GW_NAME", "")

# ---------------------------------------------------------------------------
# AI RCA - Application log group for incident context collection
# ---------------------------------------------------------------------------
APP_LOG_GROUP = os.environ.get("APP_LOG_GROUP", "")
AURORA_CLUSTER_ID = os.environ.get("AURORA_CLUSTER_ID", "")
TARGET_AURORA_CLUSTER_ID = os.environ.get("TARGET_AURORA_CLUSTER_ID", "")
AURORA_GLOBAL_CLUSTER_ID = os.environ.get("AURORA_GLOBAL_CLUSTER_ID", "")

# ---------------------------------------------------------------------------
# Automated Aurora Promotion
# When enabled, the orchestrator automatically calls SwitchoverGlobalCluster
# (for app failures) or FailoverGlobalCluster (for region failures) as part
# of the failover process. When disabled (default), the operator receives
# SNS notifications with manual CLI commands.
#
# Set AURORA_AUTO_PROMOTE = "true" to enable. Requires IAM permissions:
#   rds:SwitchoverGlobalCluster, rds:FailoverGlobalCluster
# ---------------------------------------------------------------------------
AURORA_AUTO_PROMOTE = os.environ.get("AURORA_AUTO_PROMOTE", "false").lower() == "true"

# ---------------------------------------------------------------------------
# ElastiCache Global Datastore
# When configured, the orchestrator tracks Redis promotion alongside Aurora.
# The state field `redis_promotion_pending` gates SECONDARY_ACTIVE transition.
# When ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID is empty (default), Redis
# tracking is disabled and existing behavior is unchanged.
# ---------------------------------------------------------------------------
ELASTICACHE_REPLICATION_GROUP_ID = os.environ.get("ELASTICACHE_REPLICATION_GROUP_ID", "")
ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID = os.environ.get("ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", "")
ELASTICACHE_AUTO_PROMOTE = os.environ.get("ELASTICACHE_AUTO_PROMOTE", "false").lower() == "true"

# Derive AWS account ID from the SNS topic ARN (arn:aws:sns:region:account:name)
_AWS_ACCOUNT_ID = SNS_TOPIC_ARN.split(":")[4] if ":" in SNS_TOPIC_ARN else ""

# ---------------------------------------------------------------------------
# Failover Thresholds
# ---------------------------------------------------------------------------
COOLDOWN_MINUTES = int(os.environ.get("COOLDOWN_MINUTES", "30"))
CONSECUTIVE_FAILURES_THRESHOLD = int(os.environ.get("CONSECUTIVE_FAILURES_THRESHOLD", "3"))
HEALTH_EVALUATION_WINDOW_MINUTES = int(os.environ.get("HEALTH_EVALUATION_WINDOW_MINUTES", "5"))
MIN_HEALTHY_HOST_COUNT = int(os.environ.get("MIN_HEALTHY_HOST_COUNT", "1"))
API_GW_5XX_THRESHOLD_PERCENT = float(os.environ.get("API_GW_5XX_THRESHOLD_PERCENT", "50.0"))
ACTIVE_REGION_STALE_THRESHOLD_MINUTES = int(
    os.environ.get("ACTIVE_REGION_STALE_THRESHOLD_MINUTES", "3")
)

# ---------------------------------------------------------------------------
# Failover Mode
# "auto"   = full automated failover (default for production steady state)
# "manual" = detect and notify only, wait for operator to trigger failover
# "parked" = system inactive during staged deployment — handler exits
#            immediately without accessing state backend, evaluating health,
#            or publishing metrics. Activate by changing env var to "auto".
#
# In manual mode, the Lambda does everything the same (health evaluation,
# consecutive failure counting, cooldown checking) but when it reaches the
# point of triggering failover, it sends a CRITICAL notification instead
# and includes the exact CLI command to execute the failover.
#
# Start with "parked" during initial deployment, switch to "manual" for
# validation, then "auto" once the team has confidence in the system.
# ---------------------------------------------------------------------------
FAILOVER_MODE = os.environ.get("FAILOVER_MODE", "auto").lower()

# ---------------------------------------------------------------------------
# Routing Mode
# "failover"      = active/passive with latch, manual failback (default)
# "active-active" = both regions serve traffic, auto-recovery when healthy
#
# In active-active mode:
#   - Each region independently evaluates its own health
#   - No active/passive role determination, no latch
#   - Publishes metric=1 when healthy, metric=0 when consecutive failures ≥ threshold
#   - Auto-recovers: metric goes back to 1 as soon as health returns
#   - Route 53 latency-based records use the health check to add/remove regions
# ---------------------------------------------------------------------------
ROUTING_MODE = os.environ.get("ROUTING_MODE", "failover").lower()

# ---------------------------------------------------------------------------
# Passive Region Metric Behavior
# When True, the passive region always publishes RegionActiveStatus=0 for
# itself (Job 2) instead of its real health. This is required for the
# zero-container secondary use case where Application Auto Scaling is wired
# to the CloudWatch alarm:
#   - ALARM (metric=0) → scale-down to 0 containers
#   - OK (metric=1) → scale-up to N containers
# Without this, after failback the passive region publishes 1 (containers
# still running), the alarm stays OK, and scale-down never fires.
# ---------------------------------------------------------------------------
PASSIVE_PUBLISH_ZERO = os.environ.get("PASSIVE_PUBLISH_ZERO", "false").lower() == "true"

# ---------------------------------------------------------------------------
# Aurora Promotion Command Templates
# Used in SNS notifications so the operator has copy-paste-ready commands.
# ---------------------------------------------------------------------------
AURORA_PROMOTION_REMINDER_INTERVAL_MINUTES = int(
    os.environ.get("AURORA_PROMOTION_REMINDER_INTERVAL_MINUTES", "5")
)

# ---------------------------------------------------------------------------
# Notification Throttling
# WARNING-level notifications (degraded, cooldown active, passive unhealthy)
# can fire every minute and flood the inbox. This cooldown ensures the team
# gets the first alert immediately, then a repeat only every N minutes.
# CRITICAL one-time events (failover executed, region failure, failover failed)
# are NEVER throttled.
# ---------------------------------------------------------------------------
WARNING_NOTIFICATION_COOLDOWN_MINUTES = int(
    os.environ.get("WARNING_NOTIFICATION_COOLDOWN_MINUTES", "5")
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# AWS Clients - all use short timeouts to prevent silent hangs
# ---------------------------------------------------------------------------
_client_config = BotoConfig(
    connect_timeout=10, read_timeout=30, retries={"max_attempts": 2}
)

cloudwatch = boto3.client("cloudwatch", region_name=CURRENT_REGION, config=_client_config)
sns = boto3.client("sns", region_name=CURRENT_REGION, config=_client_config)
rds = boto3.client("rds", region_name=CURRENT_REGION, config=_client_config)
ecs = boto3.client("ecs", region_name=CURRENT_REGION, config=_client_config)
elasticache_client = boto3.client("elasticache", region_name=CURRENT_REGION, config=_client_config)

# State backend — DynamoDB (default) or S3 (via STATE_BACKEND env var)
_state_backend = create_backend(region=CURRENT_REGION, client_config=_client_config)

# For S3 backend: remote backend for cross-region writes during failover.
# Ensures the other region sees state changes immediately without waiting for CRR.
_REMOTE_STATE_BUCKET = os.environ.get("REMOTE_STATE_BUCKET", "")
_remote_state_backend = None
if _REMOTE_STATE_BUCKET and os.environ.get("STATE_BACKEND", "dynamodb").lower() == "s3":
    from state_backend import S3StateBackend
    _remote_region = SECONDARY_REGION if CURRENT_REGION == PRIMARY_REGION else PRIMARY_REGION
    _remote_state_backend = S3StateBackend(
        bucket=_REMOTE_STATE_BUCKET, region=_remote_region,
        prefix=os.environ.get("STATE_PREFIX", "failover-state/"),
        client_config=_client_config,
    )

logger.info(
    f"Module initialized: region={CURRENT_REGION}, failover_mode={FAILOVER_MODE}, "
    f"routing_mode={ROUTING_MODE}, app={APP_NAME or '(not set)'}, namespace={CW_NAMESPACE}"
)


# ===========================================================================
# Aurora Promotion Command Builder
# ===========================================================================

def build_aurora_promotion_commands(target_region: str, scenario: str) -> str:
    """
    Build the exact CLI commands the operator needs to run to promote Aurora.

    Returns a formatted string with copy-paste-ready commands and explanation.
    """
    if not AURORA_GLOBAL_CLUSTER_ID:
        return (
            "AURORA_GLOBAL_CLUSTER_ID is not configured.\n"
            "No Aurora promotion commands available."
        )

    # Try to look up the target cluster ARN for the operator
    target_cluster_arn = _get_aurora_cluster_arn_in_region(target_region)
    target_arn_display = target_cluster_arn if target_cluster_arn else "<TARGET_CLUSTER_ARN>"

    if scenario == "app_failure":
        # App-level failure: primary region still reachable, try planned switchover first
        return f"""
========================================================================
AURORA PROMOTION REQUIRED - APP-LEVEL FAILURE
========================================================================

DNS has been moved to {target_region}. Your app in {target_region} CANNOT
WRITE to the database until you promote Aurora.

STEP 1: Try planned switchover first (minimal downtime, no data loss):

  aws rds switchover-global-cluster \\
    --global-cluster-identifier {AURORA_GLOBAL_CLUSTER_ID} \\
    --target-db-cluster-identifier {target_arn_display} \\
    --region {PRIMARY_REGION}

STEP 2: If switchover fails (primary unreachable), use unplanned failover:

  aws rds failover-global-cluster \\
    --global-cluster-identifier {AURORA_GLOBAL_CLUSTER_ID} \\
    --target-db-cluster-identifier {target_arn_display} \\
    --allow-data-loss \\
    --region {target_region}

STEP 3: Monitor progress:

  aws rds describe-db-clusters \\
    --db-cluster-identifier {AURORA_CLUSTER_ID} \\
    --query 'DBClusters[0].{{Status:Status,ReplicationSource:ReplicationSourceIdentifier}}' \\
    --region {target_region}

When ReplicationSourceIdentifier is empty, {target_region} is the writer.
The orchestrator will automatically detect the promotion within 60 seconds
and clear the aurora_promotion_pending flag. No manual state update needed.

========================================================================
"""

    elif scenario == "region_failure":
        # Region-level failure: primary is gone, must use unplanned failover
        return f"""
========================================================================
AURORA PROMOTION REQUIRED - REGION-LEVEL FAILURE
========================================================================

Region {PRIMARY_REGION if target_region == SECONDARY_REGION else SECONDARY_REGION} appears
to be DOWN. DNS has been moved to {target_region}. Your app in {target_region}
CANNOT WRITE to the database until you promote Aurora.

Because the primary region is unreachable, you MUST use unplanned failover.

STEP 1: Promote Aurora in {target_region} (unplanned failover):

  aws rds failover-global-cluster \\
    --global-cluster-identifier {AURORA_GLOBAL_CLUSTER_ID} \\
    --target-db-cluster-identifier {target_arn_display} \\
    --allow-data-loss \\
    --region {target_region}

  NOTE: --allow-data-loss is required when the primary is unreachable.
  Typical data loss is under 1 second of transactions depending on
  replication lag at the time of failure.

STEP 2: Monitor progress:

  aws rds describe-db-clusters \\
    --db-cluster-identifier {AURORA_CLUSTER_ID} \\
    --query 'DBClusters[0].{{Status:Status,ReplicationSource:ReplicationSourceIdentifier}}' \\
    --region {target_region}

When ReplicationSourceIdentifier is empty, {target_region} is the writer.
The orchestrator will automatically detect the promotion within 60 seconds
and clear the aurora_promotion_pending flag. No manual state update needed.

========================================================================
"""

    elif scenario == "failback":
        # Failback: controlled return to primary
        return f"""
========================================================================
AURORA SWITCHOVER REQUIRED - FAILBACK TO {target_region}
========================================================================

You are failing back to {target_region}. Before DNS is moved, Aurora must
be switched over so {target_region} is the writer.

STEP 1: Switchover Aurora to {target_region}:

  aws rds switchover-global-cluster \\
    --global-cluster-identifier {AURORA_GLOBAL_CLUSTER_ID} \\
    --target-db-cluster-identifier {target_arn_display} \\
    --region {CURRENT_REGION}

STEP 2: Monitor progress (wait until {target_region} is the writer):

  aws rds describe-db-clusters \\
    --db-cluster-identifier {AURORA_CLUSTER_ID} \\
    --query 'DBClusters[0].{{Status:Status,ReplicationSource:ReplicationSourceIdentifier}}' \\
    --region {target_region}

  When ReplicationSourceIdentifier is empty, {target_region} is the writer.

STEP 3: Once Aurora switchover is complete, run the failback Lambda
  IN THE TARGET REGION:

  aws lambda invoke \\
    --function-name {FAILBACK_FUNCTION_NAME} \\
    --payload '{{"target_region": "{target_region}", "skip_health_check": false, "operator": "YOUR_NAME", "aurora_confirmed": true}}' \\
    --region {target_region} \\
    response.json

  NOTE: --region is {target_region} (the target), NOT {CURRENT_REGION}.
  The Lambda must run in the target region to reach the private ALB
  for HTTP health validation.

  The aurora_confirmed=true flag tells the failback Lambda that you have
  already promoted Aurora and it should proceed with DNS and state changes.

========================================================================
"""

    return "Unknown scenario"


def _get_aurora_cluster_arn_in_region(target_region: str) -> Optional[str]:
    """
    Return the Aurora cluster ARN for the target region.

    Priority:
      1. Query describe_global_clusters (most robust if allowed)
      2. Use explicit TARGET_AURORA_CLUSTER_ID if provided
      3. Fallback to suffix-swapping logic
    """
    if not AURORA_GLOBAL_CLUSTER_ID:
        return _construct_fallback_arn(target_region)

    try:
        resp = rds.describe_global_clusters(GlobalClusterIdentifier=AURORA_GLOBAL_CLUSTER_ID)
        for gc in resp.get("GlobalClusters", []):
            for member in gc.get("GlobalClusterMembers", []):
                arn = member.get("DBClusterArn", "")
                if f":{target_region}:" in arn:
                    return arn
    except Exception as e:
        logger.warning(f"Failed to look up Aurora cluster via Global Cluster API: {e}")

    # Fallback 1: Explicit target ID from environment
    if TARGET_AURORA_CLUSTER_ID and _AWS_ACCOUNT_ID:
        return f"arn:aws:rds:{target_region}:{_AWS_ACCOUNT_ID}:cluster:{TARGET_AURORA_CLUSTER_ID}"

    # Fallback 2: Suffix-swapping logic (last resort)
    return _construct_fallback_arn(target_region)


def _construct_fallback_arn(region: str) -> Optional[str]:
    """Last resort logic to guess the ARN based on local cluster ID."""
    if not AURORA_CLUSTER_ID or not _AWS_ACCOUNT_ID:
        return None
    target_cluster_id = AURORA_CLUSTER_ID
    if region == "us-west-2" and target_cluster_id.endswith("-w1"):
        target_cluster_id = target_cluster_id[:-3] + "-w2"
    elif region == "us-west-1" and target_cluster_id.endswith("-w2"):
        target_cluster_id = target_cluster_id[:-3] + "-w1"
    return f"arn:aws:rds:{region}:{_AWS_ACCOUNT_ID}:cluster:{target_cluster_id}"


# ===========================================================================
# HTTP Health Check (Application-Level)
# ===========================================================================

def check_http_health() -> dict:
    """
    Call the application health endpoint directly over the private network.

    Currently checks: /actuator/health (Spring Boot default)
    Future:           /actuator/deep-health (app + DB + dependencies)
    """
    if not HEALTH_CHECK_URL:
        return {
            "signal": "http_health",
            "healthy": True,
            "reason": "HEALTH_CHECK_URL not configured, skipping",
            "skipped": True,
        }

    url = f"{HEALTH_CHECK_URL.rstrip('/')}{HEALTH_ENDPOINT}"
    logger.info(f"Checking HTTP health: {url}")

    try:
        req = Request(url, method="GET")
        req.add_header("User-Agent", "FailoverOrchestrator/3.0")
        req.add_header("Accept", "application/json")

        with urlopen(req, timeout=HEALTH_CHECK_TIMEOUT_SECONDS,
                     context=_ssl_context) as response:
            status_code = response.getcode()
            body = response.read().decode("utf-8", errors="replace")

            health_status = "UNKNOWN"
            try:
                parsed = json.loads(body)
                health_status = parsed.get("status", "UNKNOWN")
            except (json.JSONDecodeError, ValueError):
                pass

            is_healthy = status_code in HEALTHY_STATUS_CODES and health_status != "DOWN"

            return {
                "signal": "http_health",
                "healthy": is_healthy,
                "status_code": status_code,
                "actuator_status": health_status,
                "endpoint": HEALTH_ENDPOINT,
                "reason": f"HTTP {status_code}, actuator status={health_status}",
            }

    except HTTPError as e:
        return {
            "signal": "http_health",
            "healthy": False,
            "status_code": e.code,
            "endpoint": HEALTH_ENDPOINT,
            "reason": f"HTTP {e.code}: {e.reason}",
        }
    except URLError as e:
        return {
            "signal": "http_health",
            "healthy": False,
            "status_code": None,
            "endpoint": HEALTH_ENDPOINT,
            "reason": f"Connection failed: {str(e.reason)}",
        }
    except Exception as e:
        return {
            "signal": "http_health",
            "healthy": False,
            "status_code": None,
            "endpoint": HEALTH_ENDPOINT,
            "reason": f"Unexpected error: {str(e)}",
        }


# ===========================================================================
# CloudWatch Metric Health Checks (Infrastructure-Level)
# ===========================================================================

def get_metric_average(namespace: str, metric_name: str, dimensions: list,
                       period_seconds: int = 60, window_minutes: int = 5) -> Optional[float]:
    """Retrieve the average of a CloudWatch metric over a time window."""
    now = datetime.now(timezone.utc)
    try:
        response = cloudwatch.get_metric_statistics(
            Namespace=namespace,
            MetricName=metric_name,
            Dimensions=dimensions,
            StartTime=now - timedelta(minutes=window_minutes),
            EndTime=now,
            Period=period_seconds,
            Statistics=["Average"],
        )
        datapoints = response.get("Datapoints", [])
        if not datapoints:
            return None
        latest = sorted(datapoints, key=lambda d: d["Timestamp"], reverse=True)[0]
        return latest["Average"]
    except ClientError as e:
        logger.error(f"Error getting metric {namespace}/{metric_name}: {e}")
        return None


def check_alb_healthy_hosts() -> dict:
    """Check ALB target group has minimum healthy hosts."""
    if not ALB_ARN_SUFFIX or not TG_ARN_SUFFIX:
        return {"signal": "alb_healthy_hosts", "healthy": True,
                "reason": "Not configured, skipping", "skipped": True}

    count = get_metric_average(
        namespace="AWS/ApplicationELB",
        metric_name="HealthyHostCount",
        dimensions=[
            {"Name": "LoadBalancer", "Value": ALB_ARN_SUFFIX},
            {"Name": "TargetGroup", "Value": TG_ARN_SUFFIX},
        ],
        window_minutes=HEALTH_EVALUATION_WINDOW_MINUTES,
    )

    if count is None:
        return {"signal": "alb_healthy_hosts", "healthy": False,
                "reason": "No data for ALB HealthyHostCount"}

    healthy = count >= MIN_HEALTHY_HOST_COUNT
    return {
        "signal": "alb_healthy_hosts",
        "healthy": healthy,
        "value": count,
        "threshold": MIN_HEALTHY_HOST_COUNT,
        "reason": f"HealthyHostCount={count}, threshold={MIN_HEALTHY_HOST_COUNT}",
    }


def check_ecs_running_tasks() -> dict:
    """Check ECS service has running tasks."""
    if not ECS_CLUSTER_NAME or not ECS_SERVICE_NAME:
        return {"signal": "ecs_running_tasks", "healthy": True,
                "reason": "Not configured, skipping", "skipped": True}

    try:
        response = ecs.describe_services(
            cluster=ECS_CLUSTER_NAME, services=[ECS_SERVICE_NAME],
        )
        services = response.get("services", [])
        if not services:
            return {"signal": "ecs_running_tasks", "healthy": False,
                    "reason": "Service not found"}

        running = services[0].get("runningCount", 0)
        desired = services[0].get("desiredCount", 0)
        healthy = running >= max(1, desired // 2)
        return {
            "signal": "ecs_running_tasks",
            "healthy": healthy,
            "value": running,
            "desired": desired,
            "reason": f"Running={running}, Desired={desired}",
        }
    except ClientError as e:
        logger.error(f"Error checking ECS: {e}")
        return {"signal": "ecs_running_tasks", "healthy": False, "reason": str(e)}


def check_api_gateway_errors() -> dict:
    """Check API Gateway 5xx error rate is below threshold."""
    if not API_GW_NAME:
        return {"signal": "api_gw_5xx", "healthy": True,
                "reason": "Not configured, skipping", "skipped": True}

    count_5xx = get_metric_average(
        namespace="AWS/ApiGateway",
        metric_name="5XXError",
        dimensions=[{"Name": "ApiId", "Value": API_GW_NAME}],
        window_minutes=HEALTH_EVALUATION_WINDOW_MINUTES,
    )
    count_total = get_metric_average(
        namespace="AWS/ApiGateway",
        metric_name="Count",
        dimensions=[{"Name": "ApiId", "Value": API_GW_NAME}],
        window_minutes=HEALTH_EVALUATION_WINDOW_MINUTES,
    )

    if count_total is None or count_total == 0:
        return {"signal": "api_gw_5xx", "healthy": True,
                "reason": "No API traffic in window, assuming healthy"}

    if count_5xx is None:
        count_5xx = 0

    error_rate = (count_5xx / count_total) * 100
    healthy = error_rate < API_GW_5XX_THRESHOLD_PERCENT
    return {
        "signal": "api_gw_5xx",
        "healthy": healthy,
        "value": error_rate,
        "threshold": API_GW_5XX_THRESHOLD_PERCENT,
        "reason": f"5xx rate={error_rate:.1f}%, threshold={API_GW_5XX_THRESHOLD_PERCENT}%",
    }


def check_aurora_cluster_status() -> dict:
    """Check Aurora cluster is available."""
    if not AURORA_CLUSTER_ID:
        return {"signal": "aurora_status", "healthy": True,
                "reason": "Not configured, skipping", "skipped": True}

    try:
        response = rds.describe_db_clusters(DBClusterIdentifier=AURORA_CLUSTER_ID)
        clusters = response.get("DBClusters", [])
        if not clusters:
            return {"signal": "aurora_status", "healthy": False,
                    "reason": "Cluster not found"}

        status = clusters[0].get("Status", "unknown")
        # Include transient maintenance statuses where the cluster is still
        # serving reads/writes (e.g., password reset, parameter changes).
        healthy = status in {
            "available", "backing-up", "modifying",
            "resetting-master-credentials", "upgrading",
            "maintenance", "renaming",
        }
        return {
            "signal": "aurora_status",
            "healthy": healthy,
            "value": status,
            "reason": f"Cluster status={status}",
        }
    except ClientError as e:
        logger.error(f"Error checking Aurora: {e}")
        return {"signal": "aurora_status", "healthy": False, "reason": str(e)}


def check_elasticache_status() -> dict:
    """Check ElastiCache replication group is available."""
    if not ELASTICACHE_REPLICATION_GROUP_ID:
        return {"signal": "elasticache_status", "healthy": True,
                "reason": "Not configured, skipping", "skipped": True}
    try:
        response = elasticache_client.describe_replication_groups(
            ReplicationGroupId=ELASTICACHE_REPLICATION_GROUP_ID
        )
        rgs = response.get("ReplicationGroups", [])
        if not rgs:
            return {"signal": "elasticache_status", "healthy": False,
                    "reason": "Replication group not found"}
        status = rgs[0].get("Status", "unknown")
        healthy = status == "available"
        return {
            "signal": "elasticache_status",
            "healthy": healthy,
            "value": status,
            "reason": f"Replication group status={status}",
        }
    except ClientError as e:
        logger.error(f"Error checking ElastiCache: {e}")
        return {"signal": "elasticache_status", "healthy": False, "reason": str(e)}


# ===========================================================================
# Aggregate Health Evaluation
# ===========================================================================

def evaluate_region_health() -> dict:
    """
    Evaluate ALL health signals and return an aggregate result.

    Decision logic:
      - If http_health fails -> region is unhealthy (app is down, period)
      - If http_health passes but infrastructure signals degrade -> use quorum
      - If http_health is not configured -> fall back to infrastructure quorum only
    """
    http_result = check_http_health()

    infra_signals = [
        check_alb_healthy_hosts(),
        check_ecs_running_tasks(),
        check_api_gateway_errors(),
        check_aurora_cluster_status(),
        check_elasticache_status(),
    ]

    all_signals = [http_result] + infra_signals

    configured = [s for s in all_signals if not s.get("skipped", False)]

    if not configured:
        logger.warning("No health signals configured - assuming healthy")
        return {"healthy": True, "signals": all_signals, "decision_reason": "No signals configured"}

    http_configured = not http_result.get("skipped", False)
    http_healthy = http_result.get("healthy", True)

    if http_configured and not http_healthy:
        return {
            "healthy": False,
            "signals": all_signals,
            "unhealthy_count": len([s for s in configured if not s["healthy"]]),
            "total_configured": len(configured),
            "decision_reason": (
                f"HTTP health check FAILED: {http_result['reason']}. "
                f"App is not responding on {HEALTH_ENDPOINT}."
            ),
        }

    infra_configured = [s for s in infra_signals if not s.get("skipped", False)]
    infra_unhealthy = [s for s in infra_configured if not s["healthy"]]

    if not infra_configured:
        return {
            "healthy": True,
            "signals": all_signals,
            "decision_reason": "HTTP health passed, no infrastructure signals configured",
        }

    threshold_count = max(1, len(infra_configured) // 2)
    region_healthy = len(infra_unhealthy) < threshold_count

    return {
        "healthy": region_healthy,
        "signals": all_signals,
        "unhealthy_count": len(infra_unhealthy),
        "total_configured": len(infra_configured),
        "threshold_count": threshold_count,
        "decision_reason": (
            f"HTTP={('PASS' if http_healthy else 'FAIL') if http_configured else 'SKIP'}, "
            f"Infra unhealthy={len(infra_unhealthy)}/{len(infra_configured)}, "
            f"threshold={threshold_count}"
        ),
    }


# ===========================================================================
# State Management (via pluggable backend — DynamoDB or S3)
# ===========================================================================

def get_failover_state() -> dict:
    """Read current failover state from the configured backend."""
    try:
        item = _state_backend.get_state()
        if not item:
            logger.info("No state found, writing default state")
            default_state = {
                "active_region": PRIMARY_REGION,
                "state": "PRIMARY_ACTIVE",
                "last_failover_ts": "1970-01-01T00:00:00Z",
                "cooldown_minutes": COOLDOWN_MINUTES,
                "initiated_by": "INIT",
                "reason": "Initial state",
                "latch_engaged": False,
                "consecutive_failures": 0,
                "last_active_metric_ts": datetime.now(timezone.utc).isoformat(),
                "aurora_promotion_pending": False,
                "redis_promotion_pending": False,
                "last_warning_notification_ts": "1970-01-01T00:00:00Z",
            }
            _state_backend.put_state(default_state)
            return default_state
        return item
    except Exception as e:
        logger.error(f"Failed to read state: {type(e).__name__}: {e}")
        raise


def update_failover_state(updates: dict) -> None:
    """Update failover state in the configured backend."""
    logger.info(f"Updating state: {json.dumps(updates, default=str)}")
    try:
        _state_backend.update_state(updates)
        if _remote_state_backend:
            try:
                _remote_state_backend.update_state(updates)
            except Exception as e:
                logger.warning(f"Remote state update failed (non-fatal): {type(e).__name__}: {e}")
    except Exception as e:
        logger.error(f"Error updating state: {e}")
        raise


def try_increment_failures(expected_current: int, new_count: int) -> bool:
    """
    Atomically increment consecutive_failures with a conditional check.

    Uses the backend's conditional_update to verify the current value matches
    what we read. If another Lambda instance already incremented it, the
    condition fails and we return False - the other instance wins.

    Returns True if the write succeeded, False if lost the race.
    """
    try:
        result = _state_backend.conditional_update(
            condition_field="consecutive_failures",
            expected_value=expected_current,
            updates={"consecutive_failures": new_count},
        )
        if not result:
            logger.warning(
                f"Concurrent write detected on consecutive_failures. "
                f"Expected {expected_current}, another invocation already updated it. "
                f"Yielding to the other invocation."
            )
        return result
    except Exception as e:
        logger.error(f"Error in try_increment_failures: {e}")
        raise


def try_claim_failover(expected_state: str, updates: dict) -> bool:
    """
    Atomically transition to failover state with a conditional check.

    Uses the backend's conditional_update to verify the state hasn't already
    been claimed by another Lambda invocation. Only the first invocation to
    reach this point will succeed - all others yield.

    Returns True if we claimed the failover, False if another instance got there first.
    """
    try:
        result = _state_backend.conditional_update(
            condition_field="state",
            expected_value=expected_state,
            updates=updates,
        )
        if not result:
            logger.warning(
                f"Concurrent failover detected. Expected state={expected_state}, "
                f"but another invocation already changed it. Yielding."
            )
        return result
    except Exception as e:
        logger.error(f"Error in try_claim_failover: {e}")
        raise


# ===========================================================================
# Route 53 Synthetic Health Metric
# ===========================================================================

def publish_region_health_metric(region: str, is_healthy: bool) -> None:
    """Publish the synthetic CloudWatch metric that Route 53 health checks monitor."""
    value = 1.0 if is_healthy else 0.0
    cw_client = boto3.client("cloudwatch", region_name=region, config=_client_config)
    try:
        cw_client.put_metric_data(
            Namespace=CW_NAMESPACE,
            MetricData=[{
                "MetricName": CW_METRIC_NAME,
                "Dimensions": [{"Name": "Region", "Value": region}],
                "Value": value,
                "Unit": "None",
                "Timestamp": datetime.now(timezone.utc),
            }],
        )
        logger.info(f"Published {CW_METRIC_NAME}={value} for {region}")
    except Exception as e:
        logger.error(f"Failed to publish metric for {region}: {type(e).__name__}: {e}")
        raise


# ===========================================================================
# Failover Event Log (structured JSON for Splunk / mission control)
# ===========================================================================

def _emit_failover_event(
    event_type: str,
    source_region: str,
    target_region: str,
    trigger: str,
    reason: str,
    severity: str = "CRITICAL",
    additional: dict = None,
) -> None:
    """
    Emit a structured JSON log line for mission control / Splunk alerting.

    This is the authoritative signal that a failover has been initiated.
    Parse on: event_source=failover-orchestrator AND event_type=FAILOVER_INITIATED
    """
    event = {
        "event_source": "failover-orchestrator",
        "event_type": event_type,
        "severity": severity,
        "app_name": APP_NAME or "(not set)",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source_region": source_region,
        "target_region": target_region,
        "trigger": trigger,
        "reason": reason,
        "failover_mode": FAILOVER_MODE,
        "aurora_auto_promote": AURORA_AUTO_PROMOTE,
        "aurora_global_cluster": AURORA_GLOBAL_CLUSTER_ID or "(not set)",
        "cooldown_minutes": COOLDOWN_MINUTES,
    }
    if additional:
        event.update(additional)
    logger.critical(f"FAILOVER_EVENT {json.dumps(event, default=str)}")


# ===========================================================================
# Active Region Staleness Detection (for passive region use)
# ===========================================================================

def check_active_region_staleness(active_region: str, state: dict) -> dict:
    """
    Called by the PASSIVE region Lambda to determine if the active region is down.

    Uses TWO independent detection methods:

    Method 1 - State heartbeat timestamp (PRIMARY, most resilient):
      The active region Lambda writes last_active_metric_ts to the state backend
      on every invocation. The passive region reads its LOCAL copy to check
      freshness. This works even if the entire active region is gone, because
      the local replica (DynamoDB Global Table or S3 CRR) is independent.
      No cross-region API call needed.

    Method 2 - Cross-region CloudWatch API (SECONDARY, corroborating):
      Query the active region's CloudWatch for its RegionActiveStatus metric.
      This requires a cross-region API call, which may itself fail. Failures
      are classified into two categories:
        - Confirming failure (ClientError, empty datapoints): CW responded
          but said the region is not publishing. Counts as "stale".
        - Inconclusive failure (network timeout, endpoint unreachable): we
          could not reach CW at all, so we have no information about the
          active region. Counts as "inconclusive" (cw_stale=None), NOT as
          confirming staleness.

    Decision (three-state cw_stale):
      - cw_stale=True  → both methods agree region is stale → failover
      - cw_stale=False → CW says region is fresh → not stale (AND blocks)
      - cw_stale=None  → CW inconclusive → fall back to heartbeat alone, but
        require DEEP staleness (2× threshold) before firing without CW
        corroboration. Prevents a transient network blip from upgrading a
        borderline heartbeat into a false failover.
    """
    now = datetime.now(timezone.utc)

    # -----------------------------------------------------------------------
    # Method 1: State heartbeat timestamp (local read, no cross-region call)
    # -----------------------------------------------------------------------
    heartbeat_stale = False
    heartbeat_reason = ""
    last_active_ts_str = state.get("last_active_metric_ts", "1970-01-01T00:00:00Z")
    stale_threshold_seconds = ACTIVE_REGION_STALE_THRESHOLD_MINUTES * 60
    age_seconds = 0.0

    try:
        last_active_ts = datetime.fromisoformat(last_active_ts_str.replace("Z", "+00:00"))
        age_seconds = (now - last_active_ts).total_seconds()

        if age_seconds > stale_threshold_seconds:
            heartbeat_stale = True
            heartbeat_reason = (
                f"Heartbeat last_active_metric_ts is {age_seconds:.0f}s old "
                f"(threshold: {stale_threshold_seconds}s)"
            )
            logger.warning(f"Heartbeat staleness detected: {heartbeat_reason}")
        else:
            heartbeat_reason = (
                f"Heartbeat last_active_metric_ts is fresh ({age_seconds:.0f}s old)"
            )
    except (ValueError, TypeError) as e:
        # Unparseable timestamp is treated as maximally stale.
        heartbeat_stale = True
        age_seconds = float("inf")
        heartbeat_reason = f"Cannot parse last_active_metric_ts: {last_active_ts_str} ({e})"
        logger.error(heartbeat_reason)

    # -----------------------------------------------------------------------
    # Method 2: Cross-region CloudWatch API call.
    # cw_stale tri-state: True = stale confirmed, False = fresh, None = inconclusive.
    # Uses short timeouts (5s connect, 10s read) so the Lambda doesn't hang.
    # -----------------------------------------------------------------------
    cw_stale = False  # optimistic default; set to True on confirm, None on inconclusive
    cw_reason = ""

    try:
        logger.info(f"Staleness check Method 2: cross-region CW call to {active_region}")
        cw_cross_region_config = BotoConfig(
            connect_timeout=5, read_timeout=10, retries={"max_attempts": 1}
        )
        cw_active = boto3.client(
            "cloudwatch", region_name=active_region, config=cw_cross_region_config
        )
        response = cw_active.get_metric_statistics(
            Namespace=CW_NAMESPACE,
            MetricName=CW_METRIC_NAME,
            Dimensions=[{"Name": "Region", "Value": active_region}],
            StartTime=now - timedelta(minutes=ACTIVE_REGION_STALE_THRESHOLD_MINUTES),
            EndTime=now,
            Period=60,
            Statistics=["Minimum"],
        )
        datapoints = response.get("Datapoints", [])

        if datapoints:
            latest = sorted(datapoints, key=lambda d: d["Timestamp"], reverse=True)[0]
            cw_age = (now - latest["Timestamp"]).total_seconds()
            cw_reason = f"CloudWatch metric is fresh ({cw_age:.0f}s old)"
        else:
            # CW responded but has no recent datapoints — confirming staleness.
            cw_stale = True
            cw_reason = (
                f"No CloudWatch metric data from {active_region} in the last "
                f"{ACTIVE_REGION_STALE_THRESHOLD_MINUTES} minutes"
            )

    except ClientError as e:
        # CW API responded but rejected us (throttled, auth, 4xx/5xx). The API
        # is reachable — this is a CONFIRMING signal that something is wrong.
        cw_stale = True
        cw_reason = f"Cannot reach CloudWatch in {active_region}: {str(e)}"
        if heartbeat_stale:
            logger.error(cw_reason)
        else:
            logger.debug(cw_reason)
    except (EndpointConnectionError, HTTPClientError) as e:
        # Network-layer failure (connect timeout, read timeout, DNS, socket).
        # We could not reach CW at all — we have NO information about the active
        # region. INCONCLUSIVE, not confirming. Prevents a transient network
        # blip from collapsing into a false failover (see 2026-04-24 incident).
        cw_stale = None
        cw_reason = f"Cross-region CW call inconclusive ({type(e).__name__}): {str(e)}"
        if heartbeat_stale:
            logger.warning(cw_reason)
        else:
            logger.debug(cw_reason)
    except Exception as e:
        # Unknown failure class. Err on the side of confirming staleness
        # (preserves pre-v1.5 behavior for any exception we didn't anticipate).
        cw_stale = True
        cw_reason = f"Cross-region CW call failed ({type(e).__name__}): {str(e)}"
        if heartbeat_stale:
            logger.error(cw_reason)
        else:
            logger.debug(cw_reason)

    # -----------------------------------------------------------------------
    # Decision (three-state):
    #
    #   cw_stale=True  → confirming signal. AND with heartbeat_stale.
    #   cw_stale=False → CW says region is fresh. AND with heartbeat_stale
    #                    (i.e. heartbeat alone cannot fire failover).
    #   cw_stale=None  → inconclusive. Fall back to heartbeat alone, but
    #                    require DEEP staleness (2× threshold) before firing
    #                    without CW corroboration. This protects against a
    #                    borderline heartbeat + simultaneous network blip.
    #
    # Rationale: the original AND logic assumed Method 2 always produced a
    # yes/no answer, so any exception was treated as "yes, stale". That
    # collapsed network-unreachability into confirmation, which is wrong —
    # if we can't reach CW, we don't know anything. The inconclusive path
    # preserves failover capability (via deep-staleness fallback) while
    # removing the false-positive mode.
    # -----------------------------------------------------------------------
    cw_inconclusive = cw_stale is None
    if cw_inconclusive:
        deep_stale_seconds = stale_threshold_seconds * 2
        is_stale = heartbeat_stale and age_seconds > deep_stale_seconds
    else:
        is_stale = heartbeat_stale and cw_stale

    combined_reason = f"Heartbeat: {heartbeat_reason} | CW: {cw_reason}"
    logger.info(
        f"Staleness result: stale={is_stale} (heartbeat={heartbeat_stale}, "
        f"cw={cw_stale}, inconclusive={cw_inconclusive})"
    )

    if is_stale:
        logger.warning(f"Active region staleness detected: {combined_reason}")
    else:
        logger.info(f"Active region is alive: {combined_reason}")

    return {
        "stale": is_stale,
        "heartbeat_stale": heartbeat_stale,
        "cw_stale": cw_stale,
        "cw_inconclusive": cw_inconclusive,
        "reason": combined_reason,
        "heartbeat_reason": heartbeat_reason,
        "cw_reason": cw_reason,
    }


# ===========================================================================
# Notification
# ===========================================================================

def _format_subject(subject: str) -> str:
    """Prepend [ENVIRONMENT-APP_NAME] to notification subject. SNS limit is 100 chars.

    Composition rules (so operators can immediately tell which environment is alerting):
      ENVIRONMENT="prod" + APP_NAME="critical-app"  -> [prod-critical-app] {subject}
      ENVIRONMENT=""     + APP_NAME="critical-app"  -> [critical-app] {subject}
      ENVIRONMENT="prod" + APP_NAME=""              -> [prod] {subject}
      both empty                                    -> {subject} (unchanged)
    """
    parts = [p for p in (ENVIRONMENT, APP_NAME) if p]
    if parts:
        return f"[{'-'.join(parts)}] {subject}"[:100]
    return subject[:100]


def send_notification(subject: str, message: str) -> None:
    """Send SNS notification about failover events. Used for CRITICAL one-time events."""
    full_subject = _format_subject(subject)
    logger.info(f"Sending notification: {full_subject[:80]}")
    try:
        sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=full_subject, Message=message)
    except Exception as e:
        logger.error(f"Notification failed: {type(e).__name__}: {e}")


def send_warning_notification(
    subject: str,
    message: str,
    state: dict,
    bypass_throttle: bool = False,
) -> None:
    """
    Send a WARNING-level SNS notification with throttling.

    WARNING notifications (degraded, cooldown active, passive unhealthy) can fire
    every minute and flood the inbox. This function ensures the first alert goes
    out immediately, then subsequent alerts are suppressed for N minutes.

    The first alert always sends (so the team knows something is wrong).
    Subsequent alerts send only every WARNING_NOTIFICATION_COOLDOWN_MINUTES
    (default 5min in v1.6, was 10min in v1.5).

    Set ``bypass_throttle=True`` for notifications that must always reach the
    operator regardless of recent traffic — e.g., the very first failure of an
    incident, retry-attempt-N alerts during stuck data-tier promotion, or the
    final escalation when retries exhaust. These signal-class events lose
    information if collapsed under a cooldown window.

    CRITICAL one-time events (failover, region failure, failover failed) should
    use send_notification() directly - they are NEVER throttled.
    """
    now = datetime.now(timezone.utc)
    last_warning_ts_str = state.get("last_warning_notification_ts", "1970-01-01T00:00:00Z")

    if not bypass_throttle:
        try:
            last_warning_ts = datetime.fromisoformat(last_warning_ts_str.replace("Z", "+00:00"))
            seconds_since_last = (now - last_warning_ts).total_seconds()
            cooldown_seconds = WARNING_NOTIFICATION_COOLDOWN_MINUTES * 60

            if seconds_since_last < cooldown_seconds:
                remaining = (cooldown_seconds - seconds_since_last) / 60
                logger.info(
                    f"Warning notification throttled. Last sent {seconds_since_last:.0f}s ago, "
                    f"cooldown {cooldown_seconds}s. Next in ~{remaining:.0f}m. "
                    f"Subject: {subject}"
                )
                return
        except (ValueError, TypeError):
            # Can't parse timestamp - send the notification to be safe
            pass

    # Send the notification and update the timestamp
    try:
        full_subject = _format_subject(subject)
        sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=full_subject, Message=message)
        update_failover_state({"last_warning_notification_ts": now.isoformat()})
        logger.info(f"Warning notification sent: {full_subject}")
    except ClientError as e:
        logger.error(f"Error sending warning notification: {e}")


# ---------------------------------------------------------------------------
# AI Root Cause Analysis
# ---------------------------------------------------------------------------
def _run_rca_analysis(health_signals: dict) -> str:
    """
    Run AI-powered root cause analysis if enabled.

    Returns formatted RCA text to append to SNS notifications,
    or empty string if disabled/failed. Never raises.
    """
    # Read at invocation time so Lambda env var updates take effect
    if os.environ.get("AI_RCA_ENABLED", "false").lower() != "true":
        return ""

    try:
        from ai.collector import collect_incident_context
        from ai.rca_analyzer import analyze_incident, format_rca_for_sns

        logger.info("AI RCA enabled — collecting incident context")
        context = collect_incident_context(
            region=CURRENT_REGION,
            health_signals=health_signals,
            ecs_cluster=ECS_CLUSTER_NAME,
            ecs_service=ECS_SERVICE_NAME,
            aurora_cluster_id=AURORA_CLUSTER_ID,
            alb_arn=ALB_FULL_ARN or None,
            log_group=APP_LOG_GROUP or None,
        )

        logger.info("Calling LLM API for RCA analysis")
        rca_text = analyze_incident(context, region=CURRENT_REGION)
        formatted = format_rca_for_sns(rca_text, context)
        logger.info("AI RCA analysis complete")
        return f"\n\n{formatted}"
    except Exception as e:
        logger.error(f"AI RCA failed (non-blocking): {type(e).__name__}: {e}")
        return ""


def _run_aurora_advisor(scenario: str) -> tuple:
    """
    Run AI-powered Aurora promotion advisor if enabled.

    Returns (appendix_str, recommendation_dict).
    appendix_str: formatted text for SNS, or empty string.
    recommendation_dict: full advisor output, or None.
    Never raises.
    """
    # Read at invocation time so Lambda env var updates take effect
    advisor_mode = os.environ.get("AI_AURORA_ADVISOR_MODE", "disabled").lower()
    if advisor_mode == "disabled":
        return "", None

    try:
        from ai.stability_collector import collect_stability_context
        from ai.aurora_advisor import advise_aurora_promotion, format_advisor_for_sns

        logger.info(f"Aurora advisor enabled (mode={advisor_mode}) — collecting stability data")
        stability = collect_stability_context(
            region=CURRENT_REGION,
            aurora_cluster_id=AURORA_CLUSTER_ID,
            aurora_global_cluster_id=AURORA_GLOBAL_CLUSTER_ID,
            ecs_cluster=ECS_CLUSTER_NAME,
            ecs_service=ECS_SERVICE_NAME,
            alb_arn_suffix=ALB_FULL_ARN.split("loadbalancer/")[-1] if ALB_FULL_ARN else "",
        )

        logger.info("Calling LLM API for Aurora advisor")
        recommendation = advise_aurora_promotion(
            stability, scenario, region=CURRENT_REGION
        )
        formatted = format_advisor_for_sns(recommendation, stability)
        logger.info(
            f"Aurora advisor complete: method={recommendation.get('recommended_method')}, "
            f"confidence={recommendation.get('confidence')}, "
            f"auto_execute={recommendation.get('should_auto_execute')}"
        )
        return f"\n\n{formatted}", recommendation
    except Exception as e:
        logger.error(f"Aurora advisor failed (non-blocking): {type(e).__name__}: {e}")
        return "", None


# ===========================================================================
# Active-Active Handler
# ===========================================================================

def _handle_active_active(state: dict) -> dict:
    """
    Handler for ROUTING_MODE=active-active.

    Each region independently evaluates its own health and publishes a metric.
    No active/passive roles, no latch, no failover state machine.
    Recovery is automatic when health returns.
    """
    consecutive_failures = int(state.get("consecutive_failures", 0))
    last_unhealthy_ts = state.get("last_failover_ts", "1970-01-01T00:00:00Z")
    now = datetime.now(timezone.utc)

    # Update heartbeat
    update_failover_state({"last_active_metric_ts": now.isoformat()})

    # Evaluate health
    health = evaluate_region_health()
    logger.info(
        f"Active-active health: healthy={health['healthy']}, "
        f"failures={consecutive_failures}, reason={health.get('decision_reason', 'N/A')}"
    )

    if health["healthy"]:
        # Region is healthy
        if consecutive_failures > 0:
            # Recovery — was failing, now healthy again
            logger.info(f"Region {CURRENT_REGION} recovered (was at {consecutive_failures} failures)")
            update_failover_state({"consecutive_failures": 0})

            if consecutive_failures >= CONSECUTIVE_FAILURES_THRESHOLD:
                # Was marked unhealthy, now recovering — emit event and notify
                _emit_failover_event(
                    event_type="REGION_RECOVERED",
                    source_region=CURRENT_REGION,
                    target_region=CURRENT_REGION,
                    trigger="AUTO_RECOVERY",
                    reason="Health restored, region rejoining traffic pool",
                    severity="INFO",
                )
                send_notification(
                    subject=f"RECOVERED: {CURRENT_REGION} healthy, rejoining traffic pool",
                    message=(
                        f"Region {CURRENT_REGION} has recovered and is publishing healthy.\n"
                        f"Route 53 will resume routing traffic here based on latency.\n\n"
                        f"Time: {now.isoformat()}\n"
                        f"Previous failures: {consecutive_failures}\n"
                    ),
                )

        publish_region_health_metric(CURRENT_REGION, True)
        return {"statusCode": 200, "body": "Region healthy"}

    # Region is unhealthy — increment failures
    new_count = consecutive_failures + 1
    won_race = try_increment_failures(consecutive_failures, new_count)
    if not won_race:
        return {"statusCode": 200, "body": "Concurrent invocation handled this cycle"}

    if new_count < CONSECUTIVE_FAILURES_THRESHOLD:
        # Below threshold — still publishing healthy, send warning
        publish_region_health_metric(CURRENT_REGION, True)
        send_warning_notification(
            subject=f"WARNING: {CURRENT_REGION} degraded ({new_count}/{CONSECUTIVE_FAILURES_THRESHOLD})",
            message=(
                f"Region {CURRENT_REGION} health check failing.\n"
                f"Consecutive failures: {new_count}/{CONSECUTIVE_FAILURES_THRESHOLD}\n"
                f"Reason: {health.get('decision_reason', 'N/A')}\n\n"
                f"If failures reach threshold, this region will be removed from the "
                f"Route 53 traffic pool.\n"
            ),
            state=state,
        )
        return {"statusCode": 200, "body": "Below threshold, monitoring"}

    # Threshold reached — check cooldown
    try:
        last_ts = datetime.fromisoformat(last_unhealthy_ts.replace("Z", "+00:00"))
        cooldown_window = timedelta(minutes=COOLDOWN_MINUTES)
        if now < last_ts + cooldown_window:
            remaining = (last_ts + cooldown_window - now).total_seconds()
            logger.info(f"Cooldown active: {remaining:.0f}s remaining")
            publish_region_health_metric(CURRENT_REGION, True)
            send_warning_notification(
                subject=f"WARNING: {CURRENT_REGION} unhealthy but cooldown active",
                message=(
                    f"Health threshold reached but cooldown prevents marking unhealthy.\n"
                    f"Cooldown remaining: {remaining:.0f}s\n"
                    f"Reason: {health.get('decision_reason', 'N/A')}\n"
                ),
                state=state,
            )
            return {"statusCode": 200, "body": "Cooldown active"}
    except (ValueError, TypeError):
        pass  # No valid last timestamp, proceed

    # Mark region unhealthy — publish metric=0 to remove from Route 53 pool
    logger.critical(
        f"ACTIVE-ACTIVE: Marking {CURRENT_REGION} unhealthy. "
        f"Route 53 will remove this region from the traffic pool."
    )

    update_failover_state({
        "last_failover_ts": now.isoformat(),
        "initiated_by": "AUTO_ACTIVE_ACTIVE",
        "reason": f"Region marked unhealthy: {health.get('decision_reason', 'N/A')}",
    })

    publish_region_health_metric(CURRENT_REGION, False)

    _emit_failover_event(
        event_type="REGION_REMOVED",
        source_region=CURRENT_REGION,
        target_region=CURRENT_REGION,
        trigger="AUTO_ACTIVE_ACTIVE",
        reason=health.get("decision_reason", "N/A"),
        additional={
            "consecutive_failures": new_count,
            "health_signals": health.get("signals", {}),
        },
    )

    send_notification(
        subject=f"CRITICAL: {CURRENT_REGION} removed from traffic pool",
        message=(
            f"Region {CURRENT_REGION} has been marked unhealthy.\n"
            f"Route 53 will stop routing traffic to this region.\n\n"
            f"Reason: {health.get('decision_reason', 'N/A')}\n"
            f"Consecutive failures: {new_count}\n"
            f"Time: {now.isoformat()}\n\n"
            f"The region will automatically rejoin the traffic pool when health is restored.\n"
            f"No manual intervention required.\n"
        ),
    )

    return {"statusCode": 200, "body": "Region marked unhealthy, removed from pool"}


# ===========================================================================
# Main Handler
# ===========================================================================

def _reload_dynamic_config():
    """Re-read config values that the portal may change between invocations.

    Called at the start of every handler() invocation. This allows the portal
    to change env vars (STATE_BACKEND, ROUTING_MODE, etc.) and have them
    take effect on the next 1-minute EventBridge cycle without requiring
    a Lambda cold start.

    Static values (regions, SNS topic, cluster names, health check URLs)
    are read once at module import time and don't change.

    Note: FAILOVER_MODE "parked" is handled BEFORE this function runs
    (in handler()) to avoid state backend init errors during staged deployment.
    Valid FAILOVER_MODE values: "auto", "manual", "parked".
    """
    global ROUTING_MODE, PASSIVE_PUBLISH_ZERO, FAILOVER_MODE
    global _state_backend, _remote_state_backend, _REMOTE_STATE_BUCKET
    global AURORA_AUTO_PROMOTE, ELASTICACHE_AUTO_PROMOTE
    global ELASTICACHE_REPLICATION_GROUP_ID, ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID
    global HEALTH_ENDPOINT, HEALTH_CHECK_URL, HEALTH_CHECK_TIMEOUT_SECONDS
    global COOLDOWN_MINUTES, CONSECUTIVE_FAILURES_THRESHOLD
    global MIN_HEALTHY_HOST_COUNT, API_GW_5XX_THRESHOLD_PERCENT
    global ACTIVE_REGION_STALE_THRESHOLD_MINUTES
    global CW_NAMESPACE
    global ENVIRONMENT

    # All dynamic config — re-read from os.environ every invocation
    ROUTING_MODE = os.environ.get("ROUTING_MODE", "failover").lower()
    PASSIVE_PUBLISH_ZERO = os.environ.get("PASSIVE_PUBLISH_ZERO", "false").lower() == "true"
    FAILOVER_MODE = os.environ.get("FAILOVER_MODE", "auto").lower()
    AURORA_AUTO_PROMOTE = os.environ.get("AURORA_AUTO_PROMOTE", "false").lower() == "true"
    ELASTICACHE_REPLICATION_GROUP_ID = os.environ.get("ELASTICACHE_REPLICATION_GROUP_ID", "")
    ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID = os.environ.get("ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", "")
    ELASTICACHE_AUTO_PROMOTE = os.environ.get("ELASTICACHE_AUTO_PROMOTE", "false").lower() == "true"
    HEALTH_ENDPOINT = os.environ.get("HEALTH_ENDPOINT", "/actuator/health")
    HEALTH_CHECK_URL = os.environ.get("HEALTH_CHECK_URL", "")
    HEALTH_CHECK_TIMEOUT_SECONDS = int(os.environ.get("HEALTH_CHECK_TIMEOUT_SECONDS", "5"))
    COOLDOWN_MINUTES = int(os.environ.get("COOLDOWN_MINUTES", "30"))
    CONSECUTIVE_FAILURES_THRESHOLD = int(os.environ.get("CONSECUTIVE_FAILURES_THRESHOLD", "3"))
    MIN_HEALTHY_HOST_COUNT = int(os.environ.get("MIN_HEALTHY_HOST_COUNT", "1"))
    API_GW_5XX_THRESHOLD_PERCENT = float(os.environ.get("API_GW_5XX_THRESHOLD_PERCENT", "50.0"))
    ACTIVE_REGION_STALE_THRESHOLD_MINUTES = int(os.environ.get("ACTIVE_REGION_STALE_THRESHOLD_MINUTES", "3"))
    CW_NAMESPACE = os.environ.get("CW_NAMESPACE", "Custom/RegionFailover")
    ENVIRONMENT = os.environ.get("ENVIRONMENT", "")

    # Reinitialize state backend (DynamoDB or S3)
    _state_backend = create_backend(region=CURRENT_REGION, client_config=_client_config)
    logger.info(f"Config reloaded: STATE_BACKEND={os.environ.get('STATE_BACKEND','?')}, backend={type(_state_backend).__name__}")

    # Reinitialize remote state backend for S3 CRR
    _REMOTE_STATE_BUCKET = os.environ.get("REMOTE_STATE_BUCKET", "")
    _remote_state_backend = None
    if _REMOTE_STATE_BUCKET and os.environ.get("STATE_BACKEND", "dynamodb").lower() == "s3":
        from state_backend import S3StateBackend
        _remote_region = SECONDARY_REGION if CURRENT_REGION == PRIMARY_REGION else PRIMARY_REGION
        _remote_state_backend = S3StateBackend(
            bucket=_REMOTE_STATE_BUCKET, region=_remote_region,
            prefix=os.environ.get("STATE_PREFIX", "failover-state/"),
            client_config=_client_config,
        )


def detect_data_tier_config() -> dict:
    """Inspect env to determine which data tiers are configured and how.

    Returns a dict with four boolean flags driving configuration-aware
    behavior (notification suppression, failback gates, retry policies):

      aurora_present : Aurora cluster ID is set (tier exists)
      aurora_auto    : Aurora present AND AURORA_AUTO_PROMOTE=true
      redis_present  : ElastiCache global RG ID is set (tier exists)
      redis_auto     : Redis present AND ELASTICACHE_AUTO_PROMOTE=true

    The nine combinations of (aurora ∈ {absent, manual, auto}) ×
    (redis ∈ {absent, manual, auto}) cover all supported deployments
    from app-only stacks (C1) to fully automated dual-tier (C9).

    Reads os.environ directly (not module globals) so portal env-var
    flips and per-test patch.dict overrides take effect immediately
    without depending on _reload_dynamic_config() ordering.
    """
    aurora_id = os.environ.get("AURORA_CLUSTER_ID", "")
    redis_id = os.environ.get("ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", "")
    aurora_auto = os.environ.get("AURORA_AUTO_PROMOTE", "false").lower() == "true"
    redis_auto = os.environ.get("ELASTICACHE_AUTO_PROMOTE", "false").lower() == "true"
    return {
        "aurora_present": bool(aurora_id),
        "aurora_auto":    bool(aurora_id) and aurora_auto,
        "redis_present":  bool(redis_id),
        "redis_auto":     bool(redis_id) and redis_auto,
    }


def _run_preflight_checks() -> dict:
    """Validate required resources are accessible before activation.

    Invoke from the AWS console Test tab with: {"preflight": true}
    Returns a diagnostic report showing what's ready and what's missing.
    No activation side effects — purely diagnostic (publishes one test metric).
    """
    checks = {}

    # 1. State backend reachable
    try:
        _reload_dynamic_config()
        _state_backend.get_state()
        checks["state_backend"] = {"status": "PASS"}
    except Exception as e:
        checks["state_backend"] = {"status": "FAIL", "error": str(e)}

    # 2. CloudWatch metric writable
    try:
        publish_region_health_metric(CURRENT_REGION, True)
        checks["cloudwatch_metric"] = {"status": "PASS"}
    except Exception as e:
        checks["cloudwatch_metric"] = {"status": "FAIL", "error": str(e)}

    # 3. SNS topic exists
    try:
        sns.get_topic_attributes(TopicArn=SNS_TOPIC_ARN)
        checks["sns_topic"] = {"status": "PASS"}
    except Exception as e:
        checks["sns_topic"] = {"status": "FAIL", "error": str(e)}

    # 4. Optional health signals — report configured vs skipped
    for name, var, val in [
        ("aurora", "AURORA_CLUSTER_ID", AURORA_CLUSTER_ID),
        ("ecs", "ECS_CLUSTER_NAME", ECS_CLUSTER_NAME),
        ("health_endpoint", "HEALTH_CHECK_URL", HEALTH_CHECK_URL),
        ("elasticache", "ELASTICACHE_REPLICATION_GROUP_ID",
         os.environ.get("ELASTICACHE_REPLICATION_GROUP_ID", "")),
    ]:
        checks[name] = (
            {"status": "CONFIGURED"} if val
            else {"status": "SKIPPED", "note": f"{var} not set"}
        )

    required = ["state_backend", "cloudwatch_metric", "sns_topic"]
    all_pass = all(checks[k]["status"] == "PASS" for k in required)

    return {
        "statusCode": 200 if all_pass else 400,
        "ready": all_pass,
        "checks": checks,
        "region": CURRENT_REGION,
    }


def handler(event, context):
    """
    Main Lambda handler — runs every 1 minute via EventBridge in BOTH regions.

    Behavior depends on FAILOVER_MODE:
      "parked"  → system inactive, exits immediately (staged deployment)
      "auto"    → full automated failover
      "manual"  → detect and notify only

    Behavior depends on ROUTING_MODE:
      "failover"      → active/passive roles, latch, manual failback
      "active-active"  → each region evaluates own health, auto-recovery

    Manual invocations:
      {"preflight": true}         — validate resources before activation
      {"execute_failover": true}  — trigger failover when FAILOVER_MODE=manual
      {"reset_state": true}       — reset state to PRIMARY_ACTIVE
    """
    # ── Parked mode gate ──────────────────────────────────────────────
    # FAILOVER_MODE=parked: system inactive during staged deployment.
    # Checked BEFORE _reload_dynamic_config() to avoid state backend
    # init errors when infrastructure doesn't exist yet.
    if os.environ.get("FAILOVER_MODE", "auto").lower() == "parked":
        logger.info("PARKED mode - system inactive, skipping all evaluation")
        return {"statusCode": 200, "body": "Parked - activation pending"}

    # ── Pre-flight check ──────────────────────────────────────────────
    # Invoke with {"preflight": true} from the AWS console to validate
    # resources before changing FAILOVER_MODE to "auto".
    if event.get("preflight", False):
        return _run_preflight_checks()

    # Re-read dynamic config (portal may have changed env vars)
    _reload_dynamic_config()

    logger.info(
        f"Failover Orchestrator running in {CURRENT_REGION}, "
        f"mode={FAILOVER_MODE}, routing={ROUTING_MODE}"
    )

    # -----------------------------------------------------------------
    # Reset state: operator invokes with reset_state=true
    # -----------------------------------------------------------------
    if event.get("reset_state", False):
        logger.info("State reset requested via event payload")
        return _reset_state()

    state = get_failover_state()

    # -----------------------------------------------------------------
    # Active-active mode: skip failover logic, just evaluate own health
    # -----------------------------------------------------------------
    if ROUTING_MODE == "active-active":
        return _handle_active_active(state)

    # -----------------------------------------------------------------
    # Manual failover trigger: operator invokes with execute_failover=true
    # (failover mode only — not applicable to active-active)
    # -----------------------------------------------------------------
    if event.get("execute_failover", False):
        logger.info("Manual failover execution requested via event payload")
        return _execute_manual_failover()
    active_region = state.get("active_region", PRIMARY_REGION)
    current_state = state.get("state", "PRIMARY_ACTIVE")
    latch_engaged = state.get("latch_engaged", False)
    consecutive_failures = int(state.get("consecutive_failures", 0))
    last_failover_ts = state.get("last_failover_ts", "1970-01-01T00:00:00Z")
    aurora_promotion_pending = state.get("aurora_promotion_pending", False)
    redis_promotion_pending = state.get("redis_promotion_pending", False)

    logger.info(
        f"State: active={active_region}, state={current_state}, "
        f"latch={latch_engaged}, failures={consecutive_failures}, "
        f"aurora_pending={aurora_promotion_pending}, "
        f"redis_pending={redis_promotion_pending}, current_region={CURRENT_REGION}"
    )

    # Skip if a failover or failback is already in progress
    if current_state in ("FAILOVER_IN_PROGRESS", "FAILBACK_IN_PROGRESS"):
        logger.info(f"State is {current_state}, skipping evaluation")
        return {"statusCode": 200, "body": f"Skipping - {current_state}"}

    # If data tier promotion is pending (Aurora and/or ElastiCache), handle
    # reminders. Only the Lambda in the NEW active region handles this.
    # The Lambda in the failed region falls through to passive handler.
    any_promotion_pending = aurora_promotion_pending or redis_promotion_pending
    if any_promotion_pending and current_state == "WAITING_AURORA_PROMOTION":
        if CURRENT_REGION == active_region:
            if aurora_promotion_pending:
                _handle_aurora_promotion_reminder(state)
            if redis_promotion_pending:
                _handle_elasticache_promotion_reminder(state)

            # Re-read state to check if both flags are now cleared
            state = get_failover_state()
            aurora_still = state.get("aurora_promotion_pending", False)
            redis_still = state.get("redis_promotion_pending", False)

            if aurora_still or redis_still:
                publish_region_health_metric(CURRENT_REGION, True)
                return {"statusCode": 200, "body": "Waiting for data tier promotion"}
            # Both cleared — fall through to _handle_active_region which
            # transitions from WAITING_AURORA_PROMOTION to steady state
        # If we're not the active region, fall through to passive handler
        # which will publish 0 for us (latch is engaged)

    if CURRENT_REGION != active_region:
        return _handle_passive_region(state, active_region)

    return _handle_active_region(state, active_region,
                                consecutive_failures, last_failover_ts)


def _execute_manual_failover() -> dict:
    """
    Execute failover when manually triggered by operator via:
      aws lambda invoke --function-name <orchestrator-lambda> \
        --payload '{"execute_failover": true}' --region <active-region>

    This is the same failover logic as the auto path but skips health
    evaluation, consecutive failure counting, and mode checks. The operator
    has already reviewed the notification and decided to proceed.
    """
    now = datetime.now(timezone.utc)
    state = get_failover_state()
    active_region = state.get("active_region", PRIMARY_REGION)
    current_state = state.get("state", "PRIMARY_ACTIVE")

    # Guard: don't execute if already in a failover/failback state
    if current_state in ("WAITING_AURORA_PROMOTION", "FAILOVER_IN_PROGRESS",
                         "FAILBACK_IN_PROGRESS"):
        msg = (
            f"Cannot execute failover - state is already {current_state}. "
            f"Active region is {active_region}."
        )
        logger.warning(msg)
        return {"statusCode": 409, "body": msg}

    # Guard: must be invoked in the active region
    if CURRENT_REGION != active_region:
        msg = (
            f"This Lambda is in {CURRENT_REGION} but the active region is "
            f"{active_region}. Invoke this command in {active_region}."
        )
        logger.error(msg)
        return {"statusCode": 400, "body": msg}

    target_region = SECONDARY_REGION if active_region == PRIMARY_REGION else PRIMARY_REGION
    logger.critical(
        f"EXECUTING MANUAL FAILOVER: {active_region} -> {target_region}"
    )

    expected_state = "PRIMARY_ACTIVE" if active_region == PRIMARY_REGION else "SECONDARY_ACTIVE"
    claimed = try_claim_failover(expected_state, {
        "state": "WAITING_AURORA_PROMOTION",
        "active_region": target_region,
        "last_failover_ts": now.isoformat(),
        "latch_engaged": True,
        "consecutive_failures": 0,
        "initiated_by": "MANUAL_EXECUTE",
        "reason": "Manual failover executed by operator",
        "aurora_promotion_pending": True,
        "redis_promotion_pending": True if ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID else False,
    })

    if not claimed:
        msg = "Another invocation already claimed the failover."
        logger.info(msg)
        return {"statusCode": 200, "body": msg}

    _emit_failover_event(
        event_type="FAILOVER_INITIATED",
        source_region=active_region,
        target_region=target_region,
        trigger="MANUAL_EXECUTE",
        reason="Manual failover executed by operator",
    )

    try:
        publish_region_health_metric(CURRENT_REGION, False)

        # Attempt automated Aurora promotion if enabled
        aurora_auto_done = False
        if os.environ.get("AURORA_AUTO_PROMOTE", "false").lower() == "true":
            aurora_result = _auto_promote_aurora(target_region, "app_failure")
            if aurora_result["success"]:
                aurora_auto_done = True
            else:
                logger.warning(
                    f"Auto Aurora promotion failed: {aurora_result['error']}. "
                    f"Falling back to manual notification."
                )

        # Attempt automated ElastiCache promotion if enabled
        elasticache_auto_done = False
        if ELASTICACHE_AUTO_PROMOTE and ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID:
            ec_result = _auto_promote_elasticache(target_region, "app_failure")
            if ec_result["success"]:
                elasticache_auto_done = True
            else:
                logger.warning(
                    f"Auto ElastiCache promotion failed: {ec_result['error']}. "
                    f"Falling back to manual notification."
                )

        # Build notification message based on what was auto-promoted
        auto_parts = []
        manual_parts = []
        if aurora_auto_done:
            auto_parts.append(f"Aurora {aurora_result['method']} initiated AUTOMATICALLY.")
        else:
            manual_parts.append(build_aurora_promotion_commands(target_region, "app_failure"))

        if ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID:
            if elasticache_auto_done:
                auto_parts.append("ElastiCache failover initiated AUTOMATICALLY.")
            else:
                manual_parts.append(build_elasticache_promotion_commands(target_region))

        if auto_parts and not manual_parts:
            # Everything auto-promoted
            send_notification(
                subject=f"FAILOVER EXECUTED: DNS moved to {target_region} - data tier promotion initiated",
                message=(
                    f"Manual failover executed by operator.\n\n"
                    f"From: {active_region}\n"
                    f"To: {target_region}\n"
                    f"Time: {now.isoformat()}\n\n"
                    f"DNS has been moved. Route 53 is now routing traffic to {target_region}.\n\n"
                    + "\n".join(auto_parts) + "\n\n"
                    f"Latch is ENGAGED. {active_region} will remain marked unhealthy."
                ),
            )
        else:
            # Some or all require manual action
            subject_action = "PROMOTE DATA TIER NOW" if manual_parts else "promotion initiated"
            send_notification(
                subject=f"FAILOVER EXECUTED: DNS moved to {target_region} - {subject_action}",
                message=(
                    f"Manual failover executed by operator.\n\n"
                    f"From: {active_region}\n"
                    f"To: {target_region}\n"
                    f"Time: {now.isoformat()}\n\n"
                    f"DNS has been moved. Route 53 is now routing traffic to {target_region}.\n\n"
                    + (("\n".join(auto_parts) + "\n\n") if auto_parts else "")
                    + "ACTION REQUIRED: Manual promotion needed.\n"
                    f"Your app in {target_region} CANNOT WRITE until promotion completes.\n\n"
                    + "\n".join(manual_parts) + "\n\n"
                    f"Latch is ENGAGED. {active_region} will remain marked unhealthy.\n"
                    f"You will receive reminders every "
                    f"{AURORA_PROMOTION_REMINDER_INTERVAL_MINUTES} minutes until "
                    f"promotion is detected automatically."
                ),
            )

        return {
            "statusCode": 200,
            "body": f"Manual failover executed: {active_region} -> {target_region}. Data tier promotion pending.",
        }

    except Exception as e:
        logger.error(f"MANUAL FAILOVER FAILED: {e}")
        update_failover_state({
            "state": expected_state,
            "consecutive_failures": 0,
            "aurora_promotion_pending": False,
            "redis_promotion_pending": False,
        })
        send_notification(
            subject=f"MANUAL FAILOVER FAILED: {active_region} -> {target_region}",
            message=(
                f"Manual failover FAILED.\n"
                f"Error: {str(e)}\n"
                f"Manual intervention required."
            ),
        )
        raise


def _reset_state() -> dict:
    """
    Reset state back to PRIMARY_ACTIVE with all counters cleared.

    Invoke with:
      aws lambda invoke \
        --function-name <orchestrator-lambda> \
        --payload '{"reset_state": true}' \
        --region us-east-1 \
        response.json
    """
    now = datetime.now(timezone.utc)
    try:
        reset_state = {
            "active_region": PRIMARY_REGION,
            "state": "PRIMARY_ACTIVE",
            "last_failover_ts": "1970-01-01T00:00:00Z",
            "cooldown_minutes": COOLDOWN_MINUTES,
            "initiated_by": "MANUAL_RESET",
            "reason": f"State reset at {now.isoformat()}",
            "latch_engaged": False,
            "consecutive_failures": 0,
            "last_active_metric_ts": now.isoformat(),
            "aurora_promotion_pending": False,
            "redis_promotion_pending": False,
            "last_warning_notification_ts": "1970-01-01T00:00:00Z",
        }
        _state_backend.put_state(reset_state)
        if _remote_state_backend:
            try:
                _remote_state_backend.put_state(reset_state)
            except Exception as e:
                logger.warning(f"Remote state reset failed (non-fatal): {type(e).__name__}: {e}")

        # Publish healthy metric for the primary region
        publish_region_health_metric(PRIMARY_REGION, True)

        msg = (
            f"State has been reset to PRIMARY_ACTIVE.\n"
            f"active_region={PRIMARY_REGION}, latch=false, failures=0\n"
            f"Time: {now.isoformat()}"
        )
        logger.info(msg)
        return {"statusCode": 200, "body": msg}

    except Exception as e:
        logger.error(f"State reset failed: {e}")
        return {"statusCode": 500, "body": f"Reset failed: {str(e)}"}


def _handle_aurora_promotion_reminder(state: dict) -> dict:
    """
    Runs while Aurora promotion is pending. Checks every minute whether
    the operator has promoted Aurora by querying DescribeDBClusters.

    If the local region is now the writer:
      - Automatically clears aurora_promotion_pending
      - Sends a confirmation notification
      - Next invocation enters the active handler which transitions
        from WAITING_AURORA_PROMOTION to SECONDARY_ACTIVE

    If not yet promoted:
      - Publishes RegionActiveStatus=1.0 (keep Route 53 routing here)
      - Sends periodic reminders every N minutes
    """
    active_region = state.get("active_region", SECONDARY_REGION)
    last_failover_ts = state.get("last_failover_ts", "1970-01-01T00:00:00Z")
    last_ts = datetime.fromisoformat(last_failover_ts.replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    minutes_since_failover = (now - last_ts).total_seconds() / 60

    # -----------------------------------------------------------------
    # Check if Aurora has been promoted by the operator.
    # Query DescribeDBClusters to see if our region is now the writer.
    # -----------------------------------------------------------------
    aurora_promoted = _check_if_aurora_writer(active_region)

    if aurora_promoted:
        logger.info(
            f"Aurora promotion detected! {active_region} is now the writer. "
            f"Clearing aurora_promotion_pending."
        )
        update_failover_state({"aurora_promotion_pending": False})

        cfg = detect_data_tier_config()
        redis_pending = bool(state.get("redis_promotion_pending"))
        # Journey: Aurora is done; Redis step shown only when Redis is configured.
        if cfg["redis_present"]:
            journey = [
                "[✓] Aurora promoted",
                f"[{'→' if redis_pending else '✓'}] Redis promoting",
                "[ ] Latch released",
            ]
            next_step = (
                "No action — Aurora is ready. The orchestrator is now waiting for "
                "ElastiCache promotion. You'll receive another email when Redis "
                "completes (and a CRITICAL alert if it gets stuck)."
                if redis_pending else
                "No action — both data tiers are promoted. The orchestrator will "
                "transition to SECONDARY_ACTIVE on the next cycle."
            )
        else:
            journey = ["[✓] Aurora promoted", "[ ] Latch released"]
            next_step = (
                "No action — Aurora is ready. The orchestrator will transition to "
                "SECONDARY_ACTIVE on the next cycle and the failover will be complete."
            )

        subject, body = compose_message(
            severity=SEVERITY_INFO,
            what=f"Aurora is now writer in {active_region}",
            why=(
                f"The Aurora Global Database promotion to {active_region} completed "
                f"after {int(minutes_since_failover)} minute(s). Your app can now "
                f"write to the database from this region."
            ),
            next_step=next_step,
            context={
                "Active region": active_region,
                "Aurora cluster": AURORA_CLUSTER_ID or "(not configured)",
                "Promotion took": f"~{int(minutes_since_failover)} min from failover",
            },
            journey=journey,
            source="failover-orchestrator",
            region=CURRENT_REGION,
        )
        send_notification(subject, body)

        publish_region_health_metric(CURRENT_REGION, True)
        return {"statusCode": 200, "body": "Aurora promotion detected and confirmed"}

    # -----------------------------------------------------------------
    # Aurora not yet promoted - send periodic reminders
    # -----------------------------------------------------------------
    if int(minutes_since_failover) % AURORA_PROMOTION_REMINDER_INTERVAL_MINUTES == 0:
        failed_region = PRIMARY_REGION if active_region == SECONDARY_REGION else SECONDARY_REGION

        send_notification(
            subject=f"REMINDER: Aurora promotion still pending ({int(minutes_since_failover)}m)",
            message=(
                f"DNS failover to {active_region} occurred {int(minutes_since_failover)} "
                f"minutes ago but Aurora has NOT been promoted yet.\n\n"
                f"Your app in {active_region} CANNOT WRITE to the database.\n\n"
                f"{build_aurora_promotion_commands(active_region, 'app_failure')}"
            ),
        )

    # Keep publishing ourselves as healthy for Route 53.
    # The failed region's metric is handled by the CW alarm's TreatMissingData=breaching
    # if the region is down, or by the latch in the passive handler if it's alive.
    publish_region_health_metric(CURRENT_REGION, True)

    return {"statusCode": 200, "body": "Waiting for Aurora promotion"}


def _handle_elasticache_promotion_reminder(state: dict) -> dict:
    """
    Runs while ElastiCache promotion is pending. Checks every minute whether
    the ElastiCache Global Datastore has been promoted to this region.

    If the local region is now the primary:
      - Clears redis_promotion_pending
      - Sends a confirmation notification

    If not yet promoted:
      - Sends periodic reminders with manual CLI commands
    """
    active_region = state.get("active_region", SECONDARY_REGION)
    last_failover_ts = state.get("last_failover_ts", "1970-01-01T00:00:00Z")
    last_ts = datetime.fromisoformat(last_failover_ts.replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    minutes_since_failover = (now - last_ts).total_seconds() / 60

    redis_promoted = _check_if_elasticache_primary(active_region)

    if redis_promoted:
        logger.info(
            f"ElastiCache promotion detected! {active_region} is now the primary. "
            f"Clearing redis_promotion_pending."
        )
        update_failover_state({"redis_promotion_pending": False})

        cfg = detect_data_tier_config()
        aurora_pending = bool(state.get("aurora_promotion_pending"))
        # Journey: Redis is done; show Aurora step only if Aurora is configured.
        if cfg["aurora_present"]:
            journey = [
                f"[{'→' if aurora_pending else '✓'}] Aurora promoting",
                "[✓] Redis promoted",
                "[ ] Latch released",
            ]
            next_step = (
                "No action — Redis is ready. The orchestrator is now waiting for "
                "Aurora promotion. You'll receive another email when Aurora "
                "completes (and a CRITICAL alert if it gets stuck)."
                if aurora_pending else
                "No action — both data tiers are promoted. The orchestrator will "
                "transition to SECONDARY_ACTIVE on the next cycle."
            )
        else:
            journey = ["[✓] Redis promoted", "[ ] Latch released"]
            next_step = (
                "No action — Redis is ready. The orchestrator will transition to "
                "SECONDARY_ACTIVE on the next cycle and the failover will be complete."
            )

        subject, body = compose_message(
            severity=SEVERITY_INFO,
            what=f"ElastiCache is now primary in {active_region}",
            why=(
                f"The ElastiCache Global Datastore promotion to {active_region} "
                f"completed after {int(minutes_since_failover)} minute(s). Cache "
                f"writes from this region are now served locally."
            ),
            next_step=next_step,
            context={
                "Active region": active_region,
                "ElastiCache RG": ELASTICACHE_REPLICATION_GROUP_ID or "(not configured)",
                "Promotion took": f"~{int(minutes_since_failover)} min from failover",
            },
            journey=journey,
            source="failover-orchestrator",
            region=CURRENT_REGION,
        )
        send_notification(subject, body)
        return {"statusCode": 200, "body": "ElastiCache promotion detected and confirmed"}

    # ElastiCache not yet promoted - send periodic reminders
    if int(minutes_since_failover) % AURORA_PROMOTION_REMINDER_INTERVAL_MINUTES == 0:
        send_notification(
            subject=f"REMINDER: ElastiCache promotion still pending ({int(minutes_since_failover)}m)",
            message=(
                f"DNS failover to {active_region} occurred {int(minutes_since_failover)} "
                f"minutes ago but ElastiCache has NOT been promoted yet.\n\n"
                f"Your app in {active_region} CANNOT WRITE to Redis.\n\n"
                f"{build_elasticache_promotion_commands(active_region)}"
            ),
        )

    return {"statusCode": 200, "body": "Waiting for ElastiCache promotion"}


def _check_if_aurora_writer(region: str) -> bool:
    """
    Check if the Aurora cluster in the specified region is the writer
    in the Global Database using DescribeDBClusters.

    A cluster is the writer (primary) if ReplicationSourceIdentifier is empty.
    A cluster is a reader (secondary) if ReplicationSourceIdentifier is set.

    Returns True if writer, False otherwise.
    Returns False on any error (safe default - keeps waiting).
    """
    if not AURORA_CLUSTER_ID:
        return False

    try:
        rds_client = boto3.client("rds", region_name=region, config=_client_config)
        response = rds_client.describe_db_clusters(
            DBClusterIdentifier=AURORA_CLUSTER_ID
        )
        clusters = response.get("DBClusters", [])
        if not clusters:
            logger.warning(f"Aurora cluster {AURORA_CLUSTER_ID} not found in {region}")
            return False

        replication_source = clusters[0].get("ReplicationSourceIdentifier", "")
        is_writer = not replication_source  # Empty = primary/writer
        logger.info(
            f"Aurora {AURORA_CLUSTER_ID} in {region}: "
            f"ReplicationSourceIdentifier={'(empty - WRITER)' if is_writer else replication_source}"
        )
        return is_writer
    except ClientError as e:
        logger.error(f"Error checking Aurora writer status: {e}")
        return False


def _auto_promote_aurora(target_region: str, scenario: str) -> dict:
    """
    Automatically promote Aurora in the target region.

    For app_failure (region is still reachable): tries SwitchoverGlobalCluster
    first (planned, no data loss). If switchover fails, falls back to
    FailoverGlobalCluster with --allow-data-loss.

    For region_failure (region is unreachable): goes directly to
    FailoverGlobalCluster with --allow-data-loss.

    Returns {"success": bool, "method": str, "error": str}
    """
    if not AURORA_GLOBAL_CLUSTER_ID:
        return {"success": False, "method": "none", "error": "AURORA_GLOBAL_CLUSTER_ID not configured"}

    target_arn = _get_aurora_cluster_arn_in_region(target_region)
    if not target_arn:
        return {"success": False, "method": "none", "error": "Cannot construct target cluster ARN"}

    # For app failure, try planned switchover first
    if scenario == "app_failure":
        logger.info(f"Attempting Aurora planned switchover to {target_region}")
        try:
            rds.switchover_global_cluster(
                GlobalClusterIdentifier=AURORA_GLOBAL_CLUSTER_ID,
                TargetDbClusterIdentifier=target_arn,
            )
            logger.info(f"Aurora switchover initiated to {target_region}")
            return {"success": True, "method": "switchover", "error": ""}
        except ClientError as e:
            logger.warning(
                f"Aurora switchover failed ({e}), falling back to unplanned failover"
            )
            # Fall through to unplanned failover

    # Unplanned failover (region failure, or switchover failed)
    logger.info(f"Attempting Aurora unplanned failover to {target_region}")
    try:
        rds.failover_global_cluster(
            GlobalClusterIdentifier=AURORA_GLOBAL_CLUSTER_ID,
            TargetDbClusterIdentifier=target_arn,
            AllowDataLoss=True,
        )
        logger.info(f"Aurora failover initiated to {target_region}")
        return {"success": True, "method": "failover", "error": ""}
    except ClientError as e:
        error_msg = f"Aurora failover failed: {e}"
        logger.error(error_msg)
        return {"success": False, "method": "failover", "error": error_msg}


def _check_if_elasticache_primary(region: str) -> bool:
    """
    Check if the ElastiCache replication group in the specified region
    is the PRIMARY in the Global Datastore.

    Uses describe_global_replication_groups to find the member whose
    ReplicationGroupRegion matches `region` and has Role=PRIMARY.

    Returns True if primary, False otherwise.
    Returns False on any error (safe default - keeps waiting).
    """
    if not ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID:
        return False
    try:
        ec_client = boto3.client("elasticache", region_name=region, config=_client_config)
        response = ec_client.describe_global_replication_groups(
            GlobalReplicationGroupId=ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID,
            ShowMemberInfo=True,
        )
        for grg in response.get("GlobalReplicationGroups", []):
            for member in grg.get("Members", []):
                if member.get("ReplicationGroupRegion") == region:
                    is_primary = member.get("Role", "").upper() == "PRIMARY"
                    logger.info(
                        f"ElastiCache {ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID} "
                        f"in {region}: Role={member.get('Role')}"
                    )
                    return is_primary
        logger.warning(f"No ElastiCache member found for region {region} in global datastore")
        return False
    except ClientError as e:
        logger.error(f"Error checking ElastiCache primary status: {e}")
        return False


def _get_elasticache_rg_id_in_region(target_region: str) -> str:
    """
    Find the ElastiCache replication group ID in the target region by
    querying the Global Datastore members.

    Returns the ReplicationGroupId string, or empty string if not found.
    """
    if not ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID:
        return ""
    try:
        response = elasticache_client.describe_global_replication_groups(
            GlobalReplicationGroupId=ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID,
            ShowMemberInfo=True,
        )
        for grg in response.get("GlobalReplicationGroups", []):
            for member in grg.get("Members", []):
                if member.get("ReplicationGroupRegion") == target_region:
                    return member.get("ReplicationGroupId", "")
        logger.warning(f"No ElastiCache member found for region {target_region}")
        return ""
    except ClientError as e:
        logger.error(f"Error looking up ElastiCache RG in {target_region}: {e}")
        return ""


def _auto_promote_elasticache(target_region: str, scenario: str) -> dict:
    """
    Automatically failover the ElastiCache Global Datastore to the target region.

    ElastiCache Global Datastore only supports failover (no switchover concept).
    The API handles async replication gracefully.

    Returns {"success": bool, "method": str, "error": str}
    """
    if not ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID:
        return {"success": False, "method": "none",
                "error": "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID not configured"}

    target_rg_id = _get_elasticache_rg_id_in_region(target_region)
    if not target_rg_id:
        return {"success": False, "method": "none",
                "error": f"Cannot determine ElastiCache replication group ID in {target_region}"}

    logger.info(f"Attempting ElastiCache global failover to {target_region} (RG: {target_rg_id})")
    try:
        elasticache_client.failover_global_replication_group(
            GlobalReplicationGroupId=ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID,
            PrimaryRegion=target_region,
            PrimaryReplicationGroupId=target_rg_id,
        )
        logger.info(f"ElastiCache failover initiated to {target_region}")
        return {"success": True, "method": "failover", "error": ""}
    except ClientError as e:
        error_msg = f"ElastiCache failover failed: {e}"
        logger.error(error_msg)
        return {"success": False, "method": "failover", "error": error_msg}


def build_elasticache_promotion_commands(target_region: str) -> str:
    """Build manual CLI commands for ElastiCache Global Datastore failover."""
    if not ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID:
        return ""
    return (
        "\n"
        "========================================================================\n"
        "ELASTICACHE PROMOTION REQUIRED\n"
        "========================================================================\n"
        "\n"
        f"  aws elasticache failover-global-replication-group \\\n"
        f"    --global-replication-group-id {ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID} \\\n"
        f"    --primary-region {target_region} \\\n"
        f"    --primary-replication-group-id <TARGET_RG_ID>\n"
        "\n"
        "Monitor progress:\n"
        f"  aws elasticache describe-global-replication-groups \\\n"
        f"    --global-replication-group-id {ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID} \\\n"
        f"    --show-member-info\n"
        "========================================================================\n"
    )


def _handle_passive_region(state: dict, active_region: str) -> dict:
    """
    Logic for the Lambda running in the PASSIVE region.

    CRITICAL LATCH BEHAVIOR:
      If the latch is engaged and we're NOT the active region, it means we are
      the region that was previously failed away from. We MUST publish
      RegionActiveStatus=0 for ourselves to prevent Route 53 from routing
      traffic back here.

      Without this, when both regions are healthy, Route 53 failover records
      would route to the PRIMARY record (us-east-1), causing an immediate
      flip-flop back to the region that just failed.

    Two jobs:
      Job 1: Detect if the active region has gone completely down (stale checks)
      Job 2: Publish our own metric (0 if latched, health-based if not latched)
    """
    logger.info(f"Running as PASSIVE region ({CURRENT_REGION})")
    latch_engaged = state.get("latch_engaged", False)

    # -----------------------------------------------------------------
    # LATCH CHECK: Am I the region that was failed away from?
    # -----------------------------------------------------------------
    our_health = evaluate_region_health()
    current_health_map = state.get("region_health", {})
    current_health_map[CURRENT_REGION] = {
        "healthy": our_health["healthy"],
        "ts": datetime.now(timezone.utc).isoformat()
    }
    update_failover_state({"region_health": current_health_map})

    if latch_engaged:
        logger.info(
            f"Latch is engaged and I am the PASSIVE region ({CURRENT_REGION}). "
            f"Publishing RegionActiveStatus=0 to prevent flip-flop. "
            f"Traffic must stay on {active_region} until manual failback."
        )
        publish_region_health_metric(CURRENT_REGION, False)
        return {"statusCode": 200, "body": "Latched region, staying marked unhealthy"}

    # -----------------------------------------------------------------
    # Job 1: Check if active region is still alive (staleness detection)
    # Uses both state heartbeat timestamp AND cross-region CloudWatch call
    # -----------------------------------------------------------------
    staleness = check_active_region_staleness(active_region, state)
    logger.info(f"Active region staleness check: {json.dumps(staleness, default=str)}")

    if staleness["stale"]:
        logger.critical(
            f"Active region {active_region} is STALE - possible region-level failure"
        )

        logger.critical(
            f"Region-level failure detected. Moving DNS to {CURRENT_REGION}. "
            f"Aurora promotion must be done MANUALLY."
        )
        now = datetime.now(timezone.utc)
        target_region = CURRENT_REGION

        # Claim the failover with a conditional write.
        # If another invocation (or the active region's Lambda) already handled
        # this, the condition fails and we yield.
        expected_state = (
            "PRIMARY_ACTIVE" if active_region == PRIMARY_REGION else "SECONDARY_ACTIVE"
        )
        claimed = try_claim_failover(expected_state, {
            "state": "WAITING_AURORA_PROMOTION",
            "active_region": target_region,
            "last_failover_ts": now.isoformat(),
            "latch_engaged": True,
            "consecutive_failures": 0,
            "initiated_by": "AUTO_PASSIVE",
            "reason": f"Region-level failure: {staleness['reason']}",
            "aurora_promotion_pending": True,
        "redis_promotion_pending": True if ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID else False,
        })

        if not claimed:
            logger.info(
                "Another invocation already claimed the region failure failover, yielding"
            )
            return {"statusCode": 200, "body": "Region failure already handled"}

        _emit_failover_event(
            event_type="FAILOVER_INITIATED",
            source_region=active_region,
            target_region=target_region,
            trigger="AUTO_PASSIVE",
            reason=f"Region-level failure: {staleness['reason']}",
            additional={
                "heartbeat_stale": staleness.get("heartbeat_stale"),
                "cw_stale": staleness.get("cw_stale"),
                "detection_method": "passive_staleness",
            },
        )

        # Run AI Aurora advisor (non-blocking)
        advisor_appendix, advisor_rec = _run_aurora_advisor("region_failure")

        try:

            # Publish our region (the new active) as healthy for Route 53.
            # We do NOT publish to the dead region - its CW alarm already
            # fired on missing data (TreatMissingData=breaching), so Route 53
            # already considers it unhealthy. A cross-region PutMetricData
            # call to a dead region would fail and crash the Lambda.
            publish_region_health_metric(target_region, True)

            # Attempt automated Aurora promotion
            aurora_handled = False

            if advisor_rec and advisor_rec.get("should_auto_execute"):
                aurora_result = _auto_promote_aurora(target_region, "region_failure")
                if aurora_result["success"]:
                    aurora_handled = True
                    send_notification(
                        subject=f"REGION FAILURE: DNS moved to {target_region} - Aurora {aurora_result['method']} initiated (AI-advised)",
                        message=(
                            f"REGION-LEVEL FAILURE DETECTED.\n\n"
                            f"The active region {active_region} has stopped responding.\n"
                            f"DNS has been moved to {target_region}.\n\n"
                            f"Aurora {aurora_result['method']} has been initiated AUTOMATICALLY "
                            f"(AI advisor confidence: {advisor_rec.get('confidence')}%).\n"
                            f"Monitor progress with:\n\n"
                            f"  aws rds describe-db-clusters \\\n"
                            f"    --db-cluster-identifier {AURORA_CLUSTER_ID} \\\n"
                            f"    --query 'DBClusters[0].{{Status:Status,ReplicationSource:ReplicationSourceIdentifier}}' \\\n"
                            f"    --region {target_region}\n\n"
                            f"Time: {now.isoformat()}\n"
                            f"Detection: {staleness['reason']}"
                            f"{advisor_appendix}"
                        ),
                    )
                else:
                    logger.warning(
                        f"AI-advised Aurora promotion failed: {aurora_result['error']}. "
                        f"Falling back to manual notification."
                    )
            elif os.environ.get("AURORA_AUTO_PROMOTE", "false").lower() == "true":
                aurora_result = _auto_promote_aurora(target_region, "region_failure")
                if aurora_result["success"]:
                    aurora_handled = True
                    send_notification(
                        subject=f"REGION FAILURE: DNS moved to {target_region} - Aurora {aurora_result['method']} initiated",
                        message=(
                            f"REGION-LEVEL FAILURE DETECTED.\n\n"
                            f"The active region {active_region} has stopped responding.\n"
                            f"DNS has been moved to {target_region}.\n\n"
                            f"Aurora {aurora_result['method']} has been initiated AUTOMATICALLY.\n"
                            f"Monitor progress with:\n\n"
                            f"  aws rds describe-db-clusters \\\n"
                            f"    --db-cluster-identifier {AURORA_CLUSTER_ID} \\\n"
                            f"    --query 'DBClusters[0].{{Status:Status,ReplicationSource:ReplicationSourceIdentifier}}' \\\n"
                            f"    --region {target_region}\n\n"
                            f"Time: {now.isoformat()}\n"
                            f"Detection: {staleness['reason']}"
                            f"{advisor_appendix}"
                        ),
                    )
                else:
                    logger.warning(
                        f"Auto Aurora promotion failed: {aurora_result['error']}. "
                        f"Falling back to manual notification."
                    )

            # Attempt automated ElastiCache promotion
            if ELASTICACHE_AUTO_PROMOTE and ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID:
                ec_result = _auto_promote_elasticache(target_region, "region_failure")
                if not ec_result["success"]:
                    logger.warning(f"Auto ElastiCache promotion failed: {ec_result['error']}")

            if not aurora_handled:
                # Manual Aurora promotion (default, or auto-promote failed)
                aurora_commands = build_aurora_promotion_commands(
                    target_region, "region_failure"
                )
                elasticache_commands = build_elasticache_promotion_commands(target_region)

                send_notification(
                    subject=f"REGION FAILURE: DNS moved to {target_region} - PROMOTE DATA TIER NOW",
                    message=(
                        f"REGION-LEVEL FAILURE DETECTED.\n\n"
                        f"The active region {active_region} has stopped responding.\n"
                        f"DNS has been moved to {target_region}.\n\n"
                        f"ACTION REQUIRED: Data tier must be promoted MANUALLY.\n"
                        f"Your app in {target_region} CANNOT WRITE until promotion completes.\n\n"
                        f"Time: {now.isoformat()}\n"
                        f"Detection: {staleness['reason']}\n\n"
                        f"{aurora_commands}\n"
                        f"{elasticache_commands}\n\n"
                        f"You will receive reminders every "
                        f"{AURORA_PROMOTION_REMINDER_INTERVAL_MINUTES} minutes until "
                        f"promotion is detected automatically."
                        f"{advisor_appendix}"
                    ),
                )

        except Exception as e:
            logger.error(f"Passive region failover handling FAILED: {e}")
            send_notification(
                subject=f"FAILOVER HANDLING FAILED in passive region",
                message=(
                    f"Region {active_region} appears down but failover handling "
                    f"in {CURRENT_REGION} FAILED.\n\n"
                    f"Error: {str(e)}\n\n"
                    f"MANUAL INTERVENTION REQUIRED."
                ),
            )
            raise

        # Failover claimed and handled successfully. Return immediately.
        # Do NOT fall through to Job 2 (health evaluation + metric publish)
        # because the health check might report unhealthy (e.g., Aurora not
        # promoted yet) and overwrite the RegionActiveStatus=1.0 we just
        # published, causing both regions to show unhealthy.
        return {"statusCode": 200, "body": "Region failover claimed, DNS moved"}

    # -----------------------------------------------------------------
    # Job 2: Evaluate our own health and publish metric
    # We are NOT latched, so we publish our real health status.
    # This tells Route 53 whether we're ready to receive traffic.
    #
    # Exception: PASSIVE_PUBLISH_ZERO mode (zero-container secondary).
    # Always publish 0 so the CloudWatch alarm stays in ALARM state
    # and Application Auto Scaling keeps desired=0. The metric only
    # goes to 1 when this region claims failover (Job 1 above), which
    # triggers scale-up.
    # -----------------------------------------------------------------
    if PASSIVE_PUBLISH_ZERO:
        logger.info(
            f"PASSIVE_PUBLISH_ZERO is enabled — publishing 0 for {CURRENT_REGION} "
            f"(auto-scaling will manage container count via alarm)"
        )
        publish_region_health_metric(CURRENT_REGION, False)
        return {"statusCode": 200, "body": "Passive region, PASSIVE_PUBLISH_ZERO active"}

    our_health = evaluate_region_health()
    publish_region_health_metric(CURRENT_REGION, our_health["healthy"])

    if not our_health["healthy"]:
        logger.warning(f"PASSIVE region {CURRENT_REGION} is NOT healthy!")
        send_warning_notification(
            subject=f"WARNING: Passive region {CURRENT_REGION} unhealthy",
            message=(
                f"The passive/standby region {CURRENT_REGION} is reporting unhealthy.\n"
                f"If the active region ({active_region}) fails, failover will route traffic "
                f"to an unhealthy region.\n\n"
                f"Decision: {our_health.get('decision_reason', 'N/A')}\n"
                f"Signals:\n{json.dumps(our_health['signals'], indent=2, default=str)}"
            ),
            state=state,
        )

    return {"statusCode": 200, "body": "Passive region check complete"}


def _handle_active_region(state: dict, active_region: str,
                          consecutive_failures: int, last_failover_ts: str) -> dict:
    """
    Logic for the Lambda running in the ACTIVE region.

    NOTE: The latch is NOT enforced here. The latch keeps the OLD (passive)
    region publishing 0 - that's handled in _handle_passive_region. This
    handler runs when CURRENT_REGION == active_region, meaning we ARE the
    region that should be serving traffic. Publishing 0 here would be wrong.
    """
    now = datetime.now(timezone.utc)
    current_state = state.get("state", "PRIMARY_ACTIVE")

    # If we're in WAITING_AURORA_PROMOTION but all promotion flags are cleared,
    # the operator has completed all data tier promotions. Transition to
    # the appropriate steady state so the system is fully normalized.
    if current_state == "WAITING_AURORA_PROMOTION":
        aurora_still = state.get("aurora_promotion_pending", False)
        redis_still = state.get("redis_promotion_pending", False)
        if aurora_still or redis_still:
            logger.info(
                f"Still waiting for data tier promotion: "
                f"aurora_pending={aurora_still}, redis_pending={redis_still}"
            )
            publish_region_health_metric(CURRENT_REGION, True)
            return {"statusCode": 200, "body": "Data tier promotion still pending"}
        new_state = "SECONDARY_ACTIVE" if active_region == SECONDARY_REGION else "PRIMARY_ACTIVE"
        logger.info(
            f"Data tier promotion complete, transitioning from "
            f"WAITING_AURORA_PROMOTION to {new_state}"
        )
        update_failover_state({"state": new_state})

    update_failover_state({
        "last_active_metric_ts": now.isoformat(),
    })

    health = evaluate_region_health()
    logger.info(f"Health evaluation: {json.dumps(health, default=str)}")

    if health["healthy"]:
        if consecutive_failures > 0:
            update_failover_state({"consecutive_failures": 0})
            logger.info("Region recovered, reset consecutive failures to 0")
        publish_region_health_metric(CURRENT_REGION, True)
        return {"statusCode": 200, "body": "Region healthy"}

    new_failure_count = consecutive_failures + 1
    if not try_increment_failures(consecutive_failures, new_failure_count):
        # Another invocation already incremented - let it handle the decision
        logger.info("Lost race on consecutive_failures increment, yielding")
        publish_region_health_metric(CURRENT_REGION, True)
        return {"statusCode": 200, "body": "Concurrent invocation handled this cycle"}

    logger.warning(
        f"Region unhealthy! Consecutive: {new_failure_count}/{CONSECUTIVE_FAILURES_THRESHOLD}. "
        f"Decision: {health.get('decision_reason', 'N/A')}"
    )

    if new_failure_count < CONSECUTIVE_FAILURES_THRESHOLD:
        publish_region_health_metric(CURRENT_REGION, True)
        target_region = SECONDARY_REGION if active_region == PRIMARY_REGION else PRIMARY_REGION
        is_first = new_failure_count == 1
        if is_first:
            what = (
                f"Region {CURRENT_REGION} reported its FIRST health failure "
                f"(1 of {CONSECUTIVE_FAILURES_THRESHOLD})"
            )
            why = (
                f"{health.get('decision_reason', 'health check failed')}. "
                f"This is the first failure of an incident — traffic is still flowing "
                f"normally to {CURRENT_REGION}."
            )
            next_step = (
                f"No action required. The orchestrator will keep evaluating health "
                f"every 60s. You will receive a follow-up email if the failure "
                f"persists. If {CONSECUTIVE_FAILURES_THRESHOLD} consecutive failures occur "
                f"and the cooldown window has expired, traffic will move to "
                f"{target_region} automatically."
            )
        else:
            what = (
                f"Region {CURRENT_REGION} health failure "
                f"{new_failure_count} of {CONSECUTIVE_FAILURES_THRESHOLD} — "
                f"sustained but below threshold"
            )
            why = (
                f"{health.get('decision_reason', 'health check failed')}. "
                f"This is failure {new_failure_count} in a row — one more and the "
                f"orchestrator will move traffic to {target_region}."
            )
            next_step = (
                f"Investigate the root cause in {CURRENT_REGION} now. If the next "
                f"cycle (in ~60s) also fails, automatic failover will fire."
            )
        ctx = {
            "Active region": CURRENT_REGION,
            "Standby region": target_region,
            "Consecutive failures": f"{new_failure_count} of {CONSECUTIVE_FAILURES_THRESHOLD}",
            "Decision": health.get("decision_reason", "N/A"),
        }
        signals_brief = ", ".join(
            f"{s['signal']}={'OK' if s.get('healthy') else 'FAIL'}"
            for s in health.get("signals", [])
            if not s.get("skipped")
        )
        if signals_brief:
            ctx["Health signals"] = signals_brief
        journey = [
            f"[{'1' if is_first else '✓'}] First failure",
            f"[{'→' if not is_first else ' '}] Sustained ({new_failure_count}/{CONSECUTIVE_FAILURES_THRESHOLD})",
            "[ ] Failover",
        ]
        subject, body = compose_message(
            severity=SEVERITY_WARNING,
            what=what,
            why=why,
            next_step=next_step,
            context=ctx,
            journey=journey,
            source="failover-orchestrator",
            region=CURRENT_REGION,
        )
        send_warning_notification(subject, body, state, bypass_throttle=is_first)
        return {"statusCode": 200, "body": "Below threshold, monitoring"}

    last_ts = datetime.fromisoformat(last_failover_ts.replace("Z", "+00:00"))
    cooldown_expiry = last_ts + timedelta(minutes=COOLDOWN_MINUTES)

    if now < cooldown_expiry:
        remaining = (cooldown_expiry - now).total_seconds() / 60
        logger.warning(f"Cooldown active, {remaining:.1f} min remaining. NOT failing over.")
        publish_region_health_metric(CURRENT_REGION, True)
        target_region = SECONDARY_REGION if active_region == PRIMARY_REGION else PRIMARY_REGION
        what = (
            f"Region {CURRENT_REGION} unhealthy but failover blocked by cooldown "
            f"({remaining:.0f} min remaining)"
        )
        why = (
            f"{health.get('decision_reason', 'health check failed')}. The "
            f"{CONSECUTIVE_FAILURES_THRESHOLD}-failure threshold has been reached, "
            f"but a previous failover happened within the {COOLDOWN_MINUTES}-minute "
            f"cooldown window so the orchestrator will NOT fail over again right now."
        )
        next_step = (
            f"Investigate {CURRENT_REGION} immediately — the app is degraded but "
            f"traffic is still routed here. If the region cannot be restored before "
            f"the cooldown expires (in {remaining:.0f} min), failover will fire on "
            f"the next cycle. To override the cooldown, an operator can manually "
            f"invoke the failover Lambda with {{\"execute_failover\": true}}."
        )
        ctx = {
            "Active region": CURRENT_REGION,
            "Standby region": target_region,
            "Cooldown remaining": f"{remaining:.0f} min",
            "Decision": health.get("decision_reason", "N/A"),
        }
        signals_brief = ", ".join(
            f"{s['signal']}={'OK' if s.get('healthy') else 'FAIL'}"
            for s in health.get("signals", [])
            if not s.get("skipped")
        )
        if signals_brief:
            ctx["Health signals"] = signals_brief
        journey = [
            "[✓] First failure",
            f"[✓] Sustained ({CONSECUTIVE_FAILURES_THRESHOLD}/{CONSECUTIVE_FAILURES_THRESHOLD})",
            "[⏸] Cooldown — failover deferred",
        ]
        subject, body = compose_message(
            severity=SEVERITY_WARNING,
            what=what,
            why=why,
            next_step=next_step,
            context=ctx,
            journey=journey,
            source="failover-orchestrator",
            region=CURRENT_REGION,
        )
        send_warning_notification(subject, body, state)
        return {"statusCode": 200, "body": "Cooldown active"}

    # =====================================================================
    # FAILOVER THRESHOLD REACHED
    # =====================================================================
    target_region = SECONDARY_REGION if active_region == PRIMARY_REGION else PRIMARY_REGION

    # ---------------------------------------------------------------------
    # DUAL-REGION CIRCUIT BREAKER
    # Before failing over, check if the target region is actually healthy.
    # If the target is ALSO unhealthy, we stay in the current region
    # and alert operators of a global outage to prevent flip-flopping.
    # ---------------------------------------------------------------------
    peer_health_info = state.get("region_health", {}).get(target_region, {})
    peer_healthy = peer_health_info.get("healthy", True)  # Assume healthy if unknown
    peer_ts_str = peer_health_info.get("ts")
    
    is_peer_stale = False
    if peer_ts_str:
        peer_ts = datetime.fromisoformat(peer_ts_str.replace("Z", "+00:00"))
        if now - peer_ts > timedelta(minutes=5):
            is_peer_stale = True

    if not peer_healthy or is_peer_stale:
        logger.critical(
            f"DUAL-REGION OUTAGE DETECTED! Target region {target_region} is "
            f"{'UNHEALTHY' if not peer_healthy else 'STALE'}. Halting failover."
        )
        publish_region_health_metric(CURRENT_REGION, True)  # Desperate attempt to keep DNS somewhere
        send_notification(
            subject=f"CRITICAL: Dual-Region Outage Detected ({APP_NAME})",
            message=(
                f"Region {CURRENT_REGION} has hit the failover threshold, BUT "
                f"the target region {target_region} is ALSO unhealthy.\n\n"
                f"Failover has been HALTED to prevent an infinite loop.\n"
                f"Manual intervention is REQUIRED immediately.\n\n"
                f"Current region health: UNHEALTHY\n"
                f"Target region ({target_region}) health: "
                f"{'UNHEALTHY' if not peer_healthy else 'STALE (Last heartbeat: ' + peer_ts_str + ')'}\n\n"
                f"Decision: {health.get('decision_reason', 'N/A')}"
            ),
        )
        return {"statusCode": 200, "body": "Dual-region outage, failover halted"}

    # In manual mode, notify but don't execute. The operator reviews
    # the notification and runs a single command to trigger failover.
    if FAILOVER_MODE == "manual":
        logger.warning(
            f"FAILOVER THRESHOLD REACHED but mode is MANUAL. "
            f"Notifying operator. Target would be {target_region}."
        )
        publish_region_health_metric(CURRENT_REGION, True)

        # Use throttled notification - in manual mode, the threshold stays
        # reached and this code runs every minute. The operator got the first
        # alert immediately, subsequent ones are throttled.
        send_warning_notification(
            subject=f"FAILOVER RECOMMENDED: {CURRENT_REGION} -> {target_region} (manual mode)",
            message=(
                f"The failover threshold has been reached but FAILOVER_MODE is set to 'manual'.\n"
                f"DNS has NOT been moved. Traffic is still going to {CURRENT_REGION}.\n\n"
                f"From: {active_region}\n"
                f"To: {target_region}\n"
                f"Decision: {health.get('decision_reason', 'N/A')}\n\n"
                f"ACTION REQUIRED - Execute failover:\n\n"
                f"  aws lambda invoke \\\n"
                f"    --function-name {os.environ.get('AWS_LAMBDA_FUNCTION_NAME', 'failover-orchestrator')} \\\n"
                f"    --payload '{{\"execute_failover\": true}}' \\\n"
                f"    --region {CURRENT_REGION} \\\n"
                f"    response.json\n\n"
                f"This will move DNS to {target_region} and send Aurora promotion commands.\n\n"
                f"To switch to automatic failover, change FAILOVER_MODE from 'manual' to 'auto'.\n\n"
                f"Health Signals:\n{json.dumps(health['signals'], indent=2, default=str)}"
            ),
            state=state,
        )

        return {
            "statusCode": 200,
            "body": f"Failover threshold reached, manual mode - operator notified",
        }

    # =====================================================================
    # TRIGGER FAILOVER - DNS ONLY, AURORA IS MANUAL
    # =====================================================================
    logger.critical(f"TRIGGERING DNS FAILOVER: {active_region} -> {target_region}")

    # Claim the failover with a conditional write on state.
    # If another invocation already claimed it, we yield.
    expected_state = "PRIMARY_ACTIVE" if active_region == PRIMARY_REGION else "SECONDARY_ACTIVE"
    claimed = try_claim_failover(expected_state, {
        "state": "WAITING_AURORA_PROMOTION",
        "active_region": target_region,
        "last_failover_ts": now.isoformat(),
        "latch_engaged": True,
        "consecutive_failures": 0,
        "initiated_by": "AUTO_ACTIVE",
        "reason": f"Auto failover: {health.get('decision_reason', 'N/A')}",
        "aurora_promotion_pending": True,
        "redis_promotion_pending": True if ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID else False,
    })

    if not claimed:
        logger.info("Another invocation already claimed the failover, yielding")
        return {"statusCode": 200, "body": "Failover already claimed by another invocation"}

    _emit_failover_event(
        event_type="FAILOVER_INITIATED",
        source_region=active_region,
        target_region=target_region,
        trigger="AUTO_ACTIVE",
        reason=health.get("decision_reason", "N/A"),
        additional={
            "consecutive_failures": consecutive_failures + 1,
            "health_signals": health.get("signals", {}),
            "detection_method": "active_health_evaluation",
        },
    )

    # Run AI analyses (non-blocking — return "" on failure or if disabled)
    rca_appendix = _run_rca_analysis(health.get("signals", {}))
    advisor_appendix, advisor_rec = _run_aurora_advisor("app_failure")
    ai_appendix = rca_appendix + advisor_appendix

    try:

        # Move DNS by publishing unhealthy for this region
        publish_region_health_metric(CURRENT_REGION, False)

        # Attempt automated Aurora promotion
        # Priority: advisor recommendation > AURORA_AUTO_PROMOTE toggle
        aurora_handled = False

        if advisor_rec and advisor_rec.get("should_auto_execute"):
            # Aurora advisor (guided/autonomous) decided to auto-execute
            method = advisor_rec.get("recommended_method", "switchover")
            logger.info(
                f"Aurora advisor recommends auto-execute: method={method}, "
                f"confidence={advisor_rec.get('confidence')}"
            )
            aurora_result = _auto_promote_aurora(target_region, "app_failure")
            if aurora_result["success"]:
                aurora_handled = True
                send_notification(
                    subject=f"FAILOVER: DNS moved to {target_region} - Aurora {aurora_result['method']} initiated (AI-advised)",
                    message=(
                        f"Automated DNS failover triggered.\n\n"
                        f"From: {active_region}\n"
                        f"To: {target_region}\n"
                        f"Time: {now.isoformat()}\n"
                        f"Decision: {health.get('decision_reason', 'N/A')}\n\n"
                        f"DNS has been moved. Route 53 is now routing traffic to {target_region}.\n\n"
                        f"Aurora {aurora_result['method']} has been initiated AUTOMATICALLY "
                        f"(AI advisor confidence: {advisor_rec.get('confidence')}%).\n"
                        f"Monitor progress with:\n\n"
                        f"  aws rds describe-db-clusters \\\n"
                        f"    --db-cluster-identifier {AURORA_CLUSTER_ID} \\\n"
                        f"    --query 'DBClusters[0].{{Status:Status,ReplicationSource:ReplicationSourceIdentifier}}' \\\n"
                        f"    --region {target_region}\n\n"
                        f"Latch is ENGAGED. {active_region} will remain marked unhealthy.\n\n"
                        f"Signals:\n{json.dumps(health['signals'], indent=2, default=str)}"
                        f"{ai_appendix}"
                    ),
                )
            else:
                logger.warning(
                    f"AI-advised Aurora promotion failed: {aurora_result['error']}. "
                    f"Falling back to manual notification."
                )
        elif os.environ.get("AURORA_AUTO_PROMOTE", "false").lower() == "true":
            # Legacy toggle — blind auto-promote without advisor
            aurora_result = _auto_promote_aurora(target_region, "app_failure")
            if aurora_result["success"]:
                aurora_handled = True
                cfg = detect_data_tier_config()
                # Journey: failover step is happening NOW; data-tier steps follow.
                journey_lines = ["[✓] Threshold reached", "[→] Failover IN PROGRESS"]
                if cfg["aurora_present"]:
                    journey_lines.append("[→] Aurora promoting")
                if cfg["redis_present"]:
                    journey_lines.append("[ ] Redis promoting" if cfg["redis_auto"]
                                          else "[ ] Redis (manual — operator)")
                ctx = {
                    "From region": active_region,
                    "To region": target_region,
                    "Decision": health.get("decision_reason", "N/A"),
                    "Aurora cluster": AURORA_CLUSTER_ID or "(not configured)",
                    "Aurora action": f"{aurora_result['method']} (auto, in progress)",
                }
                if cfg["redis_present"]:
                    ctx["ElastiCache action"] = (
                        "failover (auto, in progress)" if cfg["redis_auto"]
                        else "MANUAL — see follow-up email"
                    )
                signals_brief = ", ".join(
                    f"{s['signal']}={'OK' if s.get('healthy') else 'FAIL'}"
                    for s in health.get("signals", [])
                    if not s.get("skipped")
                )
                if signals_brief:
                    ctx["Health signals"] = signals_brief
                next_step = (
                    f"No immediate action. Aurora {aurora_result['method']} is in "
                    f"flight; you'll receive a confirmation email when it completes "
                    f"(typically 1–5 min). The latch is ENGAGED — once {target_region} "
                    f"is fully active, traffic will stay there until you run failback "
                    f"manually. Monitor Aurora progress with:\n"
                    f"  aws rds describe-db-clusters --db-cluster-identifier "
                    f"{AURORA_CLUSTER_ID or '<cluster-id>'} "
                    f"--query 'DBClusters[0].{{Status:Status,ReplicationSource:ReplicationSourceIdentifier}}' "
                    f"--region {target_region}"
                )
                subject, body = compose_message(
                    severity=SEVERITY_CRITICAL,
                    what=f"Failover triggered — traffic moving from {active_region} to {target_region}",
                    why=(
                        f"{health.get('decision_reason', 'health check failed')}. "
                        f"This was the third consecutive failure and the cooldown "
                        f"window had expired, so the orchestrator triggered an "
                        f"automatic DNS failover."
                    ),
                    next_step=next_step + (ai_appendix or ""),
                    context=ctx,
                    journey=journey_lines,
                    source="failover-orchestrator",
                    region=CURRENT_REGION,
                )
                send_notification(subject, body)
            else:
                logger.warning(
                    f"Auto Aurora promotion failed: {aurora_result['error']}. "
                    f"Falling back to manual notification."
                )

        # Attempt automated ElastiCache promotion
        if ELASTICACHE_AUTO_PROMOTE and ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID:
            ec_result = _auto_promote_elasticache(target_region, "app_failure")
            if not ec_result["success"]:
                logger.warning(f"Auto ElastiCache promotion failed: {ec_result['error']}")

        if not aurora_handled:
            # Manual Aurora promotion (default, or auto-promote failed)
            cfg = detect_data_tier_config()
            aurora_commands = (
                build_aurora_promotion_commands(target_region, "app_failure")
                if cfg["aurora_present"] else ""
            )
            elasticache_commands = (
                build_elasticache_promotion_commands(target_region)
                if cfg["redis_present"] else ""
            )

            # Journey reflects which manual steps the operator must do.
            journey_lines = ["[✓] Threshold reached", "[✓] Failover (DNS moved)"]
            if cfg["aurora_present"] and not cfg["aurora_auto"]:
                journey_lines.append("[→] Aurora — operator action required")
            elif cfg["aurora_present"]:
                journey_lines.append("[→] Aurora — auto-promote attempted but failed")
            if cfg["redis_present"] and not cfg["redis_auto"]:
                journey_lines.append("[→] Redis — operator action required")
            elif cfg["redis_present"]:
                journey_lines.append("[→] Redis — auto-promote attempted but failed")
            journey_lines.append("[ ] Latch released (after failback)")

            ctx = {
                "From region": active_region,
                "To region": target_region,
                "Decision": health.get("decision_reason", "N/A"),
            }
            if cfg["aurora_present"]:
                ctx["Aurora cluster"] = AURORA_CLUSTER_ID or "(local cluster)"
                ctx["Aurora action"] = (
                    "MANUAL — promotion required (see commands below)"
                    if not cfg["aurora_auto"]
                    else "auto-promote FAILED — manual recovery required"
                )
            if cfg["redis_present"]:
                ctx["ElastiCache RG"] = ELASTICACHE_REPLICATION_GROUP_ID or "(local RG)"
                ctx["ElastiCache action"] = (
                    "MANUAL — failover required (see commands below)"
                    if not cfg["redis_auto"]
                    else "auto-failover FAILED — manual recovery required"
                )
            signals_brief = ", ".join(
                f"{s['signal']}={'OK' if s.get('healthy') else 'FAIL'}"
                for s in health.get("signals", [])
                if not s.get("skipped")
            )
            if signals_brief:
                ctx["Health signals"] = signals_brief

            # next_step embeds the actual CLI commands so the operator can
            # copy/paste from the email without leaving the inbox.
            next_step_parts = [
                f"DNS has moved to {target_region} but the data tier is NOT ready. "
                f"Your app in {target_region} CANNOT WRITE until you complete the steps below."
            ]
            if aurora_commands:
                next_step_parts.append("\n--- Step 1: Promote Aurora ---\n" + aurora_commands)
            if elasticache_commands:
                step_n = 2 if aurora_commands else 1
                next_step_parts.append(f"\n--- Step {step_n}: Promote ElastiCache ---\n" + elasticache_commands)
            next_step_parts.append(
                f"\nThe orchestrator polls every minute and will detect promotion automatically. "
                f"You will receive a confirmation email per tier when each completes. The latch "
                f"is ENGAGED — {active_region} stays marked unhealthy until you run failback."
            )
            next_step = "\n".join(next_step_parts) + (ai_appendix or "")

            subject, body = compose_message(
                severity=SEVERITY_CRITICAL,
                what=f"Failover triggered — promote data tier in {target_region} now",
                why=(
                    f"{health.get('decision_reason', 'health check failed')}. "
                    f"DNS was moved automatically but data-tier auto-promote is "
                    f"disabled (or failed), so the operator must promote Aurora "
                    f"and/or ElastiCache manually before the new active region "
                    f"can serve writes."
                ),
                next_step=next_step,
                context=ctx,
                journey=journey_lines,
                source="failover-orchestrator",
                region=CURRENT_REGION,
            )
            send_notification(subject, body)

        return {
            "statusCode": 200,
            "body": f"DNS failover executed: {active_region} -> {target_region}. Data tier {'auto-promoted' if aurora_handled else 'promotion pending'}.",
        }

    except Exception as e:
        logger.error(f"FAILOVER FAILED: {e}")
        update_failover_state({
            "state": (
                "PRIMARY_ACTIVE"
                if active_region == PRIMARY_REGION
                else "SECONDARY_ACTIVE"
            ),
            "consecutive_failures": 0,
            "aurora_promotion_pending": False,
            "redis_promotion_pending": False,
        })
        send_notification(
            subject=f"FAILOVER FAILED: {active_region} -> {target_region}",
            message=(
                f"DNS failover FAILED.\n"
                f"Error: {str(e)}\n"
                f"Manual intervention required.\n\n"
                f"State has been reset. Orchestrator will re-evaluate next cycle."
            ),
        )
        raise
