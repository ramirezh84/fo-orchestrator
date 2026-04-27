"""
Deposits 2.0 Failback Lambda
============================
This Lambda is invoked MANUALLY by an operator to fail back to the primary region.

AURORA SWITCHOVER IS MANUAL:
  The operator must switchover Aurora BEFORE running this Lambda.
  This Lambda handles DNS (via CloudWatch metric) and state management only.

IMPORTANT - INVOKE IN THE TARGET REGION:
  This Lambda must be invoked in the REGION YOU ARE FAILING BACK TO.
  For example, to fail back to us-east-1, invoke it in us-east-1.
  This is because the Lambda is VPC-attached and needs to reach the private
  ALB in the target region to validate the app is responding on /actuator/health.

Workflow:
  1. Operator runs Aurora switchover manually (see SNS notification for commands)
  2. Operator verifies Aurora promotion is complete
  3. Operator invokes this Lambda IN THE TARGET REGION with aurora_confirmed=true
  4. Lambda validates target region health (HTTP, ECS, Aurora writer)
  5. Lambda updates Route 53 metrics and failover state
  6. Lambda releases the latch

Invocation (failing back to us-east-1 - note --region us-east-1):
  aws lambda invoke \
    --function-name <your-failback-lambda-name> \
    --payload '{"target_region": "us-east-1", "skip_health_check": false, "operator": "enrique", "aurora_confirmed": true}' \
    --region us-east-1 \
    response.json

If you need the Aurora switchover commands, invoke with aurora_confirmed=false
(or omit it) and the Lambda will return the commands without doing anything.
"""

import os
import json
import logging
import ssl
import time
from datetime import datetime, timezone
from typing import Optional
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from state_backend import create_backend, S3StateBackend
from notifications import (
    compose_message,
    SEVERITY_INFO,
    SEVERITY_WARNING,
    SEVERITY_CRITICAL,
)
from observability import (
    publish_state_metrics,
    publish_signal_metrics,
    increment_counter,
    record_duration_seconds,
)

# AI modules imported lazily inside functions to support v1.0 mode

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PRIMARY_REGION = os.environ.get("PRIMARY_REGION", "us-east-1")
SECONDARY_REGION = os.environ.get("SECONDARY_REGION", "us-east-2")
CURRENT_REGION = os.environ.get("AWS_REGION", "us-east-1")
STATE_TABLE = os.environ.get("STATE_TABLE", "failover-state")
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
CW_NAMESPACE = os.environ.get("CW_NAMESPACE", "Custom/RegionFailover")
CW_METRIC_NAME = os.environ.get("CW_METRIC_NAME", "RegionActiveStatus")
AURORA_GLOBAL_CLUSTER_ID = os.environ.get("AURORA_GLOBAL_CLUSTER_ID", "")
AURORA_CLUSTER_ID = os.environ.get("AURORA_CLUSTER_ID", "")
TARGET_AURORA_CLUSTER_ID = os.environ.get("TARGET_AURORA_CLUSTER_ID", "")
AURORA_AUTO_PROMOTE = os.environ.get("AURORA_AUTO_PROMOTE", "false").lower() == "true"
ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID = os.environ.get("ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", "")
ELASTICACHE_REPLICATION_GROUP_ID = os.environ.get("ELASTICACHE_REPLICATION_GROUP_ID", "")
ELASTICACHE_AUTO_PROMOTE = os.environ.get("ELASTICACHE_AUTO_PROMOTE", "false").lower() == "true"

# Derive AWS account ID from the SNS topic ARN
_AWS_ACCOUNT_ID = SNS_TOPIC_ARN.split(":")[4] if ":" in SNS_TOPIC_ARN else ""

# Application name - included in all SNS notifications
APP_NAME = os.environ.get("APP_NAME", "")

# Deployment environment (e.g., "prod", "staging", "demo"). Composed with
# APP_NAME into the [ENVIRONMENT-APP_NAME] subject prefix when both are set.
ENVIRONMENT = os.environ.get("ENVIRONMENT", "")

HEALTH_CHECK_URL = os.environ.get("HEALTH_CHECK_URL", "")
# Default to /healthcheck (shallow, app-only) per CLAUDE.md recommendation.
# /actuator/health and /deep-healthcheck typically include DB connectivity
# checks, which can return 503 from DB-config issues unrelated to the actual
# app health — those false positives blocked the v1.5 drill's failback step.
# Operators who genuinely want deep validation can override via env var.
HEALTH_ENDPOINT = os.environ.get("HEALTH_ENDPOINT", "/healthcheck")
HEALTH_CHECK_TIMEOUT_SECONDS = int(os.environ.get("HEALTH_CHECK_TIMEOUT_SECONDS", "5"))
ECS_CLUSTER_NAME = os.environ.get("ECS_CLUSTER_NAME", "")
ECS_SERVICE_NAME = os.environ.get("ECS_SERVICE_NAME", "")

HEALTH_CHECK_DISABLE_SSL_VERIFY = os.environ.get(
    "HEALTH_CHECK_DISABLE_SSL_VERIFY", "false"
).lower() == "true"

_ssl_context = None
if HEALTH_CHECK_DISABLE_SSL_VERIFY:
    _ssl_context = ssl.create_default_context()
    _ssl_context.check_hostname = False
    _ssl_context.verify_mode = ssl.CERT_NONE

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Log configuration at module load for debugging
# ---------------------------------------------------------------------------
logger.info(
    f"Module initialized: region={CURRENT_REGION}, app={APP_NAME or '(not set)'}, "
    f"table={STATE_TABLE}, namespace={CW_NAMESPACE}, "
    f"health_url={HEALTH_CHECK_URL or '(not set)'}, "
    f"ssl_verify_disabled={HEALTH_CHECK_DISABLE_SSL_VERIFY}"
)

# ---------------------------------------------------------------------------
# AWS Clients (with short timeouts to prevent hangs)
# ---------------------------------------------------------------------------
_client_config = BotoConfig(
    connect_timeout=10, read_timeout=30, retries={"max_attempts": 2}
)

sns = boto3.client("sns", region_name=CURRENT_REGION, config=_client_config)

# State backend — DynamoDB (default) or S3 (via STATE_BACKEND env var)
_state_backend = create_backend(region=CURRENT_REGION, client_config=_client_config)

# For S3 backend: also create a remote backend to write state cross-region.
# With S3 CRR, each region writes to its own bucket. After failback, the remote
# region's orchestrator may still be writing stale state to its bucket. Writing
# directly to the remote bucket ensures immediate consistency without waiting for CRR.
_REMOTE_STATE_BUCKET = os.environ.get("REMOTE_STATE_BUCKET", "")
_remote_state_backend = None
if _REMOTE_STATE_BUCKET and os.environ.get("STATE_BACKEND", "dynamodb").lower() == "s3":
    _remote_region = SECONDARY_REGION if CURRENT_REGION == PRIMARY_REGION else PRIMARY_REGION
    _remote_state_backend = S3StateBackend(
        bucket=_REMOTE_STATE_BUCKET,
        region=_remote_region,
        prefix=os.environ.get("STATE_PREFIX", "failover-state/"),
        client_config=_client_config,
    )
    logger.info(f"Remote state backend initialized: bucket={_REMOTE_STATE_BUCKET}, region={_remote_region}")

logger.info("AWS clients initialized")


def get_failover_state() -> dict:
    logger.info("Reading failover state...")
    try:
        item = _state_backend.get_state()
        logger.info(
            f"State: active_region={item.get('active_region')}, "
            f"state={item.get('state')}, latch={item.get('latch_engaged')}"
        )
        return item
    except Exception as e:
        logger.error(f"Failed to read state: {type(e).__name__}: {e}")
        raise


def update_failover_state(updates: dict) -> None:
    logger.info(f"Updating state: {json.dumps(updates, default=str)}")
    try:
        _state_backend.update_state(updates)
        logger.info("State update succeeded (local)")
        if _remote_state_backend:
            try:
                _remote_state_backend.update_state(updates)
                logger.info("State update succeeded (remote)")
            except Exception as e:
                logger.warning(f"Remote state update failed (non-fatal): {type(e).__name__}: {e}")
    except Exception as e:
        logger.error(f"State update FAILED: {type(e).__name__}: {e}")
        raise


def publish_region_health_metric(region: str, is_healthy: bool) -> None:
    value = 1.0 if is_healthy else 0.0
    logger.info(f"Publishing {CW_METRIC_NAME}={value} for {region}")
    try:
        cw = boto3.client("cloudwatch", region_name=region, config=_client_config)
        cw.put_metric_data(
            Namespace=CW_NAMESPACE,
            MetricData=[{
                "MetricName": CW_METRIC_NAME,
                "Dimensions": [{"Name": "Region", "Value": region}],
                "Value": value,
                "Unit": "None",
                "Timestamp": datetime.now(timezone.utc),
            }],
        )
        logger.info(f"Metric published successfully for {region}")
    except Exception as e:
        logger.error(f"Failed to publish metric for {region}: {type(e).__name__}: {e}")
        raise


def _format_subject(subject: str) -> str:
    """Prepend [ENVIRONMENT-APP_NAME] to notification subject. SNS limit is 100 chars.

    Composition rules (matches orchestrator's _format_subject):
      ENVIRONMENT="prod" + APP_NAME="critical-app"  -> [prod-critical-app] {subject}
      ENVIRONMENT=""     + APP_NAME="critical-app"  -> [critical-app] {subject}
      ENVIRONMENT="prod" + APP_NAME=""              -> [prod] {subject}
      both empty                                    -> {subject}
    """
    parts = [p for p in (ENVIRONMENT, APP_NAME) if p]
    if parts:
        return f"[{'-'.join(parts)}] {subject}"[:100]
    return subject[:100]


def detect_data_tier_config() -> dict:
    """Inspect env to determine which data tiers are present and how they promote.

    Returns the same shape as the orchestrator's helper. The failback Lambda
    uses this to decide which gates to enforce on the operator's payload
    (aurora_confirmed, redis_confirmed, etc.) and to suppress notifications
    for absent tiers.

    Reads os.environ directly so per-invocation env changes (and per-test
    patch.dict overrides) take effect without re-importing the module.

    See failover_orchestrator_v3.py:detect_data_tier_config for full docs.
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


def send_notification(subject: str, message: str) -> None:
    full_subject = _format_subject(subject)
    logger.info(f"Sending notification: {full_subject}")
    try:
        sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=full_subject, Message=message)
        logger.info("Notification sent successfully")
    except Exception as e:
        logger.error(f"Notification FAILED: {type(e).__name__}: {e}")



def validate_target_region_health(target_region: str) -> dict:
    """
    Validate the failback target region is healthy before moving traffic.

    Checks (in order):
      1. HTTP /health - is the app actually responding?
      2. ECS running tasks - are containers running at desired count?
      3. Aurora writer status - is this region the Aurora writer?
    """
    issues = []
    logger.info(f"=== Starting health validation for {target_region} ===")

    # ---------------------------------------------------------------
    # Check 1: HTTP health endpoint
    # ---------------------------------------------------------------
    if HEALTH_CHECK_URL:
        url = f"{HEALTH_CHECK_URL.rstrip('/')}{HEALTH_ENDPOINT}"
        logger.info(f"[Check 1/3] HTTP health: {url}")
        logger.info(
            f"  Timeout: {HEALTH_CHECK_TIMEOUT_SECONDS}s, "
            f"SSL verify disabled: {HEALTH_CHECK_DISABLE_SSL_VERIFY}"
        )

        try:
            req = Request(url, method="GET")
            req.add_header("User-Agent", "FailoverFailback/2.0")
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

                if status_code != 200 or health_status == "DOWN":
                    issue = (
                        f"HTTP health: {url} returned HTTP {status_code}, "
                        f"actuator status={health_status}. App is not healthy."
                    )
                    logger.error(f"  FAILED: {issue}")
                    issues.append(issue)
                else:
                    logger.info(
                        f"  PASSED: HTTP {status_code}, status={health_status}"
                    )

        except HTTPError as e:
            issue = f"HTTP health: {url} returned HTTP {e.code}: {e.reason}"
            logger.error(f"  FAILED: {issue}")
            issues.append(issue)
        except URLError as e:
            issue = (
                f"HTTP health: Cannot reach {url} - {str(e.reason)}. "
                f"Verify this Lambda is running in {target_region} so it can "
                f"reach the private ALB."
            )
            logger.error(f"  FAILED: {issue}")
            issues.append(issue)
        except Exception as e:
            issue = (
                f"HTTP health: Unexpected error calling {url} - "
                f"{type(e).__name__}: {str(e)}"
            )
            logger.error(f"  FAILED: {issue}")
            issues.append(issue)
    else:
        logger.warning(
            "[Check 1/3] HTTP health: SKIPPED (HEALTH_CHECK_URL not configured)"
        )

    # ---------------------------------------------------------------
    # Check 2: ECS running tasks
    # ---------------------------------------------------------------
    if ECS_CLUSTER_NAME and ECS_SERVICE_NAME:
        logger.info(
            f"[Check 2/3] ECS: cluster={ECS_CLUSTER_NAME}, "
            f"service={ECS_SERVICE_NAME}"
        )
        try:
            ecs = boto3.client(
                "ecs", region_name=target_region, config=_client_config
            )
            resp = ecs.describe_services(
                cluster=ECS_CLUSTER_NAME, services=[ECS_SERVICE_NAME]
            )
            services = resp.get("services", [])
            if services:
                running = services[0].get("runningCount", 0)
                desired = services[0].get("desiredCount", 0)
                if running < desired:
                    issue = f"ECS: {running}/{desired} tasks running"
                    logger.error(f"  FAILED: {issue}")
                    issues.append(issue)
                else:
                    logger.info(f"  PASSED: {running}/{desired} tasks running")
            else:
                issue = "ECS: Service not found in target region"
                logger.error(f"  FAILED: {issue}")
                issues.append(issue)
        except Exception as e:
            issue = f"ECS check failed: {type(e).__name__}: {e}"
            logger.error(f"  FAILED: {issue}")
            issues.append(issue)
    else:
        logger.warning("[Check 2/3] ECS: SKIPPED (not configured)")

    # ---------------------------------------------------------------
    # Check 3: Aurora writer status
    # Uses DescribeDBClusters to check ReplicationSourceIdentifier.
    # Empty = primary/writer. Set = secondary/reader.
    # ---------------------------------------------------------------
    if AURORA_CLUSTER_ID:
        logger.info(
            f"[Check 3/3] Aurora: cluster={AURORA_CLUSTER_ID}"
        )
        try:
            rds_client = boto3.client(
                "rds", region_name=target_region, config=_client_config
            )
            resp = rds_client.describe_db_clusters(
                DBClusterIdentifier=AURORA_CLUSTER_ID
            )
            clusters = resp.get("DBClusters", [])
            if clusters:
                replication_source = clusters[0].get(
                    "ReplicationSourceIdentifier", ""
                )
                target_is_writer = not replication_source
                logger.info(
                    f"  ReplicationSourceIdentifier: "
                    f"{'(empty - WRITER)' if target_is_writer else replication_source}"
                )

                if not target_is_writer:
                    issue = (
                        f"Aurora: The cluster in {target_region} is NOT the "
                        f"writer (ReplicationSourceIdentifier is set). "
                        f"You must switchover Aurora BEFORE running failback."
                    )
                    logger.error(f"  FAILED: {issue}")
                    issues.append(issue)
                else:
                    logger.info(
                        f"  PASSED: {target_region} is the Aurora writer"
                    )
            else:
                issue = f"Aurora: Cluster {AURORA_CLUSTER_ID} not found in {target_region}"
                logger.error(f"  FAILED: {issue}")
                issues.append(issue)
        except Exception as e:
            issue = f"Aurora check failed: {type(e).__name__}: {e}"
            logger.error(f"  FAILED: {issue}")
            issues.append(issue)
    else:
        logger.warning(
            "[Check 3/3] Aurora: SKIPPED (AURORA_CLUSTER_ID not configured)"
        )

    # ---------------------------------------------------------------
    # Check 4: ElastiCache Global Datastore primary status (optional)
    # ---------------------------------------------------------------
    if ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID:
        logger.info(
            f"[Check 4] ElastiCache: global_rg={ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID}"
        )
        try:
            ec_client = boto3.client(
                "elasticache", region_name=target_region, config=_client_config
            )
            resp = ec_client.describe_global_replication_groups(
                GlobalReplicationGroupId=ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID,
                ShowMemberInfo=True,
            )
            target_is_primary = False
            for grg in resp.get("GlobalReplicationGroups", []):
                for member in grg.get("Members", []):
                    if member.get("ReplicationGroupRegion") == target_region:
                        target_is_primary = member.get("Role", "").upper() == "PRIMARY"
                        logger.info(f"  Role: {member.get('Role')}")

            if not target_is_primary:
                issue = (
                    f"ElastiCache: The replication group in {target_region} is NOT "
                    f"the primary. You must failover ElastiCache BEFORE running failback."
                )
                logger.error(f"  FAILED: {issue}")
                issues.append(issue)
            else:
                logger.info(f"  PASSED: {target_region} is the ElastiCache primary")
        except Exception as e:
            issue = f"ElastiCache check failed: {type(e).__name__}: {e}"
            logger.error(f"  FAILED: {issue}")
            issues.append(issue)
    else:
        logger.info(
            "[Check 4] ElastiCache: SKIPPED (not configured)"
        )

    passed = len(issues) == 0
    logger.info(
        f"=== Health validation complete: "
        f"{'PASSED' if passed else f'FAILED ({len(issues)} issues)'} ==="
    )

    return {
        "healthy": passed,
        "issues": issues,
    }


def build_aurora_switchover_commands(target_region: str) -> str:
    """Build the Aurora switchover commands for the operator."""
    if not AURORA_GLOBAL_CLUSTER_ID:
        return "AURORA_GLOBAL_CLUSTER_ID not configured."

    # Construct the target cluster ARN
    target_cluster_id = TARGET_AURORA_CLUSTER_ID or AURORA_CLUSTER_ID
    
    # Fallback to suffix swapping if TARGET_AURORA_CLUSTER_ID is missing
    if not TARGET_AURORA_CLUSTER_ID and target_cluster_id:
        if target_region == "us-west-2" and target_cluster_id.endswith("-w1"):
            target_cluster_id = target_cluster_id[:-3] + "-w2"
        elif target_region == "us-west-1" and target_cluster_id.endswith("-w2"):
            target_cluster_id = target_cluster_id[:-3] + "-w1"

    if target_cluster_id and _AWS_ACCOUNT_ID:
        target_arn = f"arn:aws:rds:{target_region}:{_AWS_ACCOUNT_ID}:cluster:{target_cluster_id}"
    else:
        target_arn = "<TARGET_CLUSTER_ARN>"

    return f"""
========================================================================
AURORA SWITCHOVER COMMANDS - RUN THESE BEFORE FAILBACK
========================================================================

STEP 1: Switchover Aurora to {target_region}:

  aws rds switchover-global-cluster \\
    --global-cluster-identifier {AURORA_GLOBAL_CLUSTER_ID} \\
    --target-db-cluster-identifier {target_arn} \\
    --region {CURRENT_REGION}

STEP 2: Wait for switchover to complete. Monitor with:

  aws rds describe-db-clusters \\
    --db-cluster-identifier {AURORA_CLUSTER_ID} \\
    --query 'DBClusters[0].{{Status:Status,ReplicationSource:ReplicationSourceIdentifier}}' \\
    --region {target_region}

  When ReplicationSourceIdentifier is empty, {target_region} is the writer.

STEP 3: Then run this Lambda IN THE TARGET REGION with aurora_confirmed=true:

  aws lambda invoke \\
    --function-name {os.environ.get('AWS_LAMBDA_FUNCTION_NAME', 'failover-manual-failback')} \\
    --payload '{{"target_region": "{target_region}", "skip_health_check": false, "operator": "YOUR_NAME", "aurora_confirmed": true}}' \\
    --region {target_region} \\
    response.json

  NOTE: --region is {target_region} (the target), NOT {CURRENT_REGION}.
  The Lambda must run in the target region to reach the private ALB
  for HTTP health validation.

========================================================================
"""


def build_redis_failover_commands(target_region: str) -> str:
    """Build the ElastiCache Global Datastore failover commands for the operator.

    Mirror of build_aurora_switchover_commands. Used by the Redis manual gate
    in C4/C5/C8 configs (Redis present, ELASTICACHE_AUTO_PROMOTE=false).
    """
    if not ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID:
        return "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID not configured."

    target_rg_id = ELASTICACHE_REPLICATION_GROUP_ID or "<TARGET_RG_ID>"
    # Suffix swap when target_rg appears to be the local-region RG and we're
    # invoking from the other region's perspective (mirror of the Aurora trick).
    if target_rg_id != "<TARGET_RG_ID>":
        if target_region == "us-west-2" and target_rg_id.endswith("-w1"):
            target_rg_id = target_rg_id[:-3] + "-w2"
        elif target_region == "us-west-1" and target_rg_id.endswith("-w2"):
            target_rg_id = target_rg_id[:-3] + "-w1"

    return f"""
========================================================================
ELASTICACHE FAILOVER COMMANDS - RUN THESE BEFORE FAILBACK
========================================================================

STEP 1: Failover ElastiCache Global Datastore to {target_region}:

  aws elasticache failover-global-replication-group \\
    --global-replication-group-id {ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID} \\
    --primary-region {target_region} \\
    --primary-replication-group-id {target_rg_id}

STEP 2: Wait for failover to complete. Monitor with:

  aws elasticache describe-global-replication-groups \\
    --global-replication-group-id {ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID} \\
    --show-member-info

  When the member in {target_region} shows Role=PRIMARY, failover is complete.

STEP 3: Then re-invoke this Lambda with redis_confirmed=true (alongside
        aurora_confirmed=true if Aurora is also configured):

  aws lambda invoke \\
    --function-name {os.environ.get('AWS_LAMBDA_FUNCTION_NAME', 'failover-manual-failback')} \\
    --payload '{{"target_region": "{target_region}", "operator": "YOUR_NAME", "aurora_confirmed": true, "redis_confirmed": true}}' \\
    --region {target_region} \\
    response.json

========================================================================
"""


def _resolve_target_aurora_arn(target_region: str) -> Optional[str]:
    """Resolve the Aurora cluster ARN for the cluster in *target_region*.

    Priority (mirrors the orchestrator's helper):
      1. describe_global_clusters → find member whose ARN contains :target_region:
      2. AURORA_CLUSTER_ID (LOCAL cluster, since failback Lambda runs in the
         target region by invariant — the local cluster IS the failback target)
      3. Hard-coded -w1/-w2 suffix swap as last resort

    v1.7 fix (F9): the previous logic used TARGET_AURORA_CLUSTER_ID first,
    which is the PEER cluster (the one currently writing in the OTHER region)
    — exactly the wrong target. Switchover wants to PROMOTE a cluster, so the
    target is the LOCAL one in the failback region, not the peer.
    """
    if not AURORA_GLOBAL_CLUSTER_ID:
        return None

    # Best path: ask AWS which cluster lives in target_region.
    try:
        rds = boto3.client("rds", region_name=CURRENT_REGION, config=_client_config)
        resp = rds.describe_global_clusters(GlobalClusterIdentifier=AURORA_GLOBAL_CLUSTER_ID)
        for gc in resp.get("GlobalClusters", []):
            for member in gc.get("GlobalClusterMembers", []):
                arn = member.get("DBClusterArn", "")
                if f":{target_region}:" in arn:
                    return arn
    except ClientError as e:
        logger.warning(f"describe_global_clusters failed during ARN lookup: {e}")

    if not _AWS_ACCOUNT_ID:
        return None

    # The failback Lambda runs IN target_region (by invariant), so AURORA_CLUSTER_ID
    # in this Lambda's env IS the cluster we want to promote.
    if AURORA_CLUSTER_ID and CURRENT_REGION == target_region:
        return f"arn:aws:rds:{target_region}:{_AWS_ACCOUNT_ID}:cluster:{AURORA_CLUSTER_ID}"

    # Last-resort suffix swap (kept for backwards compat with -w1/-w2 stacks).
    cid = AURORA_CLUSTER_ID
    if cid:
        if target_region == "us-west-2" and cid.endswith("-w1"):
            cid = cid[:-3] + "-w2"
        elif target_region == "us-west-1" and cid.endswith("-w2"):
            cid = cid[:-3] + "-w1"
        return f"arn:aws:rds:{target_region}:{_AWS_ACCOUNT_ID}:cluster:{cid}"
    return None


def _auto_switchover_aurora(target_region: str) -> dict:
    """Trigger Aurora switchover-global-cluster AND wait for completion.

    Used in the C3/C6/C9 paths where AURORA_AUTO_PROMOTE=true. Mirror of the
    orchestrator's _auto_promote_aurora but tailored to failback (planned
    switchover, never failover-with-data-loss).

    v1.7.1 (F10): now blocks on writer-flip completion before returning. The
    SwitchoverGlobalCluster API returns immediately, but the actual writer
    role flip takes 1–3 minutes — without this, validate_target_region_health
    runs too early and refuses the failback. Configurable timeout via
    FAILBACK_PROMOTION_WAIT_TIMEOUT_SECONDS (default 480s).

    Returns: ``{"success": bool, "error": str, "elapsed_seconds": int}``.
    """
    target_arn = _resolve_target_aurora_arn(target_region)
    if not target_arn:
        return {"success": False, "error": "Cannot determine target cluster ARN — set AURORA_GLOBAL_CLUSTER_ID + AURORA_CLUSTER_ID", "elapsed_seconds": 0}

    try:
        rds = boto3.client("rds", region_name=CURRENT_REGION, config=_client_config)
        rds.switchover_global_cluster(
            GlobalClusterIdentifier=AURORA_GLOBAL_CLUSTER_ID,
            TargetDbClusterIdentifier=target_arn,
        )
        logger.info(
            f"Aurora switchover initiated: {AURORA_GLOBAL_CLUSTER_ID} → {target_arn}"
        )
    except ClientError as e:
        # Idempotency: if a switchover to this same target is already running
        # (e.g. operator re-invoked the Lambda), treat it as success and fall
        # through to the wait loop — it'll converge naturally.
        msg = str(e)
        if "already switching over" in msg.lower():
            logger.info(
                f"Aurora switchover to {target_arn} is already in progress; "
                f"falling through to wait-for-completion."
            )
        else:
            full_msg = f"Aurora switchover-global-cluster API failed: {e}"
            logger.error(full_msg)
            return {"success": False, "error": full_msg, "elapsed_seconds": 0}

    # Block until target_region is the writer (or until timeout).
    timeout = int(os.environ.get("FAILBACK_PROMOTION_WAIT_TIMEOUT_SECONDS", "480"))
    return wait_for_aurora_writer(target_region, timeout)


def wait_for_aurora_writer(target_region: str, timeout_seconds: int) -> dict:
    """Poll the global cluster until target_region's member is writer.

    F10 (v1.7.1): SwitchoverGlobalCluster returns immediately but the actual
    writer flip takes 1-3 minutes. Without this wait,
    validate_target_region_health runs too early and refuses the failback.

    Returns: ``{"success": bool, "elapsed_seconds": int, "error": str}``.
    """
    if not AURORA_GLOBAL_CLUSTER_ID:
        return {"success": False, "elapsed_seconds": 0,
                "error": "AURORA_GLOBAL_CLUSTER_ID not configured"}

    deadline = time.monotonic() + timeout_seconds
    start = time.monotonic()
    poll_interval = 10  # seconds
    rds = boto3.client("rds", region_name=CURRENT_REGION, config=_client_config)

    while True:
        try:
            resp = rds.describe_global_clusters(GlobalClusterIdentifier=AURORA_GLOBAL_CLUSTER_ID)
            for gc in resp.get("GlobalClusters", []):
                for member in gc.get("GlobalClusterMembers", []):
                    arn = member.get("DBClusterArn", "")
                    if f":{target_region}:" in arn and member.get("IsWriter"):
                        elapsed = int(time.monotonic() - start)
                        logger.info(
                            f"Aurora writer is now in {target_region} after {elapsed}s"
                        )
                        record_duration_seconds(
                            "AuroraPromotionDurationSeconds", elapsed, CURRENT_REGION,
                            dimensions={"Tier": "Aurora", "Source": "failback"},
                        )
                        return {"success": True, "elapsed_seconds": elapsed, "error": ""}
        except ClientError as e:
            logger.warning(f"describe_global_clusters during wait: {e}")

        if time.monotonic() >= deadline:
            elapsed = int(time.monotonic() - start)
            return {
                "success": False, "elapsed_seconds": elapsed,
                "error": f"Aurora switchover did not complete in {elapsed}s",
            }
        time.sleep(poll_interval)


def _auto_failover_redis(target_region: str) -> dict:
    """Trigger ElastiCache Global Datastore failover from the failback Lambda.

    Used in the C7/C8/C9 paths where ELASTICACHE_AUTO_PROMOTE=true. Mirror of
    the orchestrator's _auto_promote_elasticache.

    Returns: ``{"success": bool, "error": str}``.
    """
    if not ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID:
        return {"success": False, "error": "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID not configured"}

    target_rg_id = ELASTICACHE_REPLICATION_GROUP_ID
    # Suffix swap when ELASTICACHE_REPLICATION_GROUP_ID is the local-region RG.
    if target_rg_id:
        if target_region == "us-west-2" and target_rg_id.endswith("-w1"):
            target_rg_id = target_rg_id[:-3] + "-w2"
        elif target_region == "us-west-1" and target_rg_id.endswith("-w2"):
            target_rg_id = target_rg_id[:-3] + "-w1"

    if not target_rg_id:
        return {"success": False, "error": "Cannot determine target RG ID"}

    # F11 (v1.7.2): Global Datastore can be in a transient "modifying" state for
    # several minutes after a recent failover/disassociation. The API surfaces
    # this as InvalidGlobalReplicationGroupState. Retry the API call a few times
    # with backoff before giving up. The wait loop after the API call still
    # protects against the role-flip latency once the API accepts the call.
    max_initiate_attempts = 5
    initiate_backoff = 30  # seconds between retries on InvalidState

    initiated = False
    last_invalid_state_error = None
    ec = boto3.client("elasticache", region_name=CURRENT_REGION, config=_client_config)
    for attempt in range(1, max_initiate_attempts + 1):
        try:
            ec.failover_global_replication_group(
                GlobalReplicationGroupId=ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID,
                PrimaryRegion=target_region,
                PrimaryReplicationGroupId=target_rg_id,
            )
            logger.info(
                f"ElastiCache failover initiated: "
                f"{ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID} → {target_region}/{target_rg_id} "
                f"(attempt {attempt}/{max_initiate_attempts})"
            )
            initiated = True
            break
        except ClientError as e:
            msg = str(e)
            # Idempotency: if target region is already primary, treat as success
            # and let wait_for_redis_primary confirm.
            if "already primary" in msg.lower() or "in-eligible" in msg.lower():
                logger.info(
                    f"ElastiCache target region {target_region} is already primary; "
                    f"falling through to wait-for-completion."
                )
                initiated = True
                break
            # F11: InvalidGlobalReplicationGroupState — transient, the Global
            # Datastore is still settling from a recent operation. Wait + retry.
            if "InvalidGlobalReplicationGroupState" in msg or "not in a valid state" in msg.lower():
                last_invalid_state_error = msg
                logger.info(
                    f"ElastiCache Global Datastore in transient state on attempt "
                    f"{attempt}/{max_initiate_attempts} (will retry in {initiate_backoff}s): {msg}"
                )
                if attempt < max_initiate_attempts:
                    time.sleep(initiate_backoff)
                    continue
                # Exhausted retries on InvalidState — give up with a clear error
                full_msg = (
                    f"ElastiCache Global Datastore stayed in transient state for "
                    f"~{(max_initiate_attempts - 1) * initiate_backoff}s "
                    f"({max_initiate_attempts} retries): {last_invalid_state_error}"
                )
                logger.error(full_msg)
                return {"success": False, "error": full_msg, "elapsed_seconds": 0}
            # Any other error — fail immediately
            full_msg = f"ElastiCache failover-global-replication-group API failed: {e}"
            logger.error(full_msg)
            return {"success": False, "error": full_msg, "elapsed_seconds": 0}

    if not initiated:
        # Defensive — should be unreachable
        return {"success": False, "error": "ElastiCache failover not initiated", "elapsed_seconds": 0}

    timeout = int(os.environ.get("FAILBACK_PROMOTION_WAIT_TIMEOUT_SECONDS", "480"))
    return wait_for_redis_primary(target_region, timeout)


def wait_for_redis_primary(target_region: str, timeout_seconds: int) -> dict:
    """Poll the Global Datastore until target_region's RG has Role=PRIMARY.

    F10 (v1.7.1): same shape as wait_for_aurora_writer — failover_global_replication_group
    returns immediately but the role flip takes 30s-3min.

    Returns: ``{"success": bool, "elapsed_seconds": int, "error": str}``.
    """
    if not ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID:
        return {"success": False, "elapsed_seconds": 0,
                "error": "ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID not configured"}

    deadline = time.monotonic() + timeout_seconds
    start = time.monotonic()
    poll_interval = 10
    ec = boto3.client("elasticache", region_name=CURRENT_REGION, config=_client_config)

    while True:
        try:
            resp = ec.describe_global_replication_groups(
                GlobalReplicationGroupId=ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID,
                ShowMemberInfo=True,
            )
            for grg in resp.get("GlobalReplicationGroups", []):
                for member in grg.get("Members", []):
                    if (member.get("ReplicationGroupRegion") == target_region
                            and member.get("Role") == "PRIMARY"):
                        elapsed = int(time.monotonic() - start)
                        logger.info(
                            f"ElastiCache primary is now in {target_region} after {elapsed}s"
                        )
                        record_duration_seconds(
                            "RedisPromotionDurationSeconds", elapsed, CURRENT_REGION,
                            dimensions={"Tier": "Redis", "Source": "failback"},
                        )
                        return {"success": True, "elapsed_seconds": elapsed, "error": ""}
        except ClientError as e:
            logger.warning(f"describe_global_replication_groups during wait: {e}")

        if time.monotonic() >= deadline:
            elapsed = int(time.monotonic() - start)
            return {
                "success": False, "elapsed_seconds": elapsed,
                "error": f"ElastiCache failover did not complete in {elapsed}s",
            }
        time.sleep(poll_interval)


def _reload_dynamic_config():
    """Re-read config that the portal may change between invocations."""
    global _state_backend, _remote_state_backend, _REMOTE_STATE_BUCKET
    global ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID
    ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID = os.environ.get("ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", "")
    _state_backend = create_backend(region=CURRENT_REGION, client_config=_client_config)
    _REMOTE_STATE_BUCKET = os.environ.get("REMOTE_STATE_BUCKET", "")
    _remote_state_backend = None
    if _REMOTE_STATE_BUCKET and os.environ.get("STATE_BACKEND", "dynamodb").lower() == "s3":
        _remote_region = SECONDARY_REGION if CURRENT_REGION == PRIMARY_REGION else PRIMARY_REGION
        _remote_state_backend = S3StateBackend(
            bucket=_REMOTE_STATE_BUCKET, region=_remote_region,
            prefix=os.environ.get("STATE_PREFIX", "failover-state/"),
            client_config=_client_config,
        )


def handler(event, context):
    """
    Manual failback handler.

    Expected event payload:
    {
        "target_region": "us-east-1",
        "skip_health_check": false,
        "operator": "enrique",

        # Aurora gates (only checked when Aurora is configured per
        # detect_data_tier_config). If AURORA_AUTO_PROMOTE=true the Lambda
        # performs the switchover itself and aurora_confirmed is not needed;
        # if AURORA_AUTO_PROMOTE=false the operator must run the switchover
        # manually first and pass aurora_confirmed=true.
        "aurora_confirmed": true,         # required when Aurora present + manual
        "skip_aurora_check": false,       # break-glass for the Aurora gate

        # Redis gates (only checked when ElastiCache is configured). Same
        # auto-vs-manual contract as Aurora. New in v1.6 (PR5/F5).
        "redis_confirmed": true,          # required when Redis present + manual
        "skip_redis_check": false,        # break-glass for the Redis gate
    }
    """
    _reload_dynamic_config()

    target_region = event.get("target_region", PRIMARY_REGION)
    skip_health_check = event.get("skip_health_check", False)
    operator = event.get("operator", "unknown")
    aurora_confirmed = event.get("aurora_confirmed", False)
    skip_aurora_check = event.get("skip_aurora_check", False)
    redis_confirmed = event.get("redis_confirmed", False)
    skip_redis_check = event.get("skip_redis_check", False)
    cfg = detect_data_tier_config()
    now = datetime.now(timezone.utc)

    logger.info(
        f"Manual failback initiated by {operator} to {target_region} | "
        f"skip_health_check={skip_health_check}, "
        f"aurora_confirmed={aurora_confirmed}, skip_aurora_check={skip_aurora_check}, "
        f"redis_confirmed={redis_confirmed}, skip_redis_check={skip_redis_check}, "
        f"current_region={CURRENT_REGION}, "
        f"data_tier_config={cfg}"
    )

    # Step 1: Validate current state
    logger.info("Step 1: Reading current failover state...")
    state = get_failover_state()
    active_region = state.get("active_region", PRIMARY_REGION)
    current_state = state.get("state", "PRIMARY_ACTIVE")

    logger.info(
        f"Current state: active_region={active_region}, state={current_state}, "
        f"target_region={target_region}"
    )

    if active_region == target_region and current_state not in (
        "WAITING_AURORA_PROMOTION", "FAILOVER_IN_PROGRESS"
    ):
        msg = (
            f"Target region {target_region} is already the active region. "
            f"No failback needed."
        )
        logger.info(msg)
        return {"statusCode": 200, "body": msg}

    if current_state == "FAILBACK_IN_PROGRESS":
        msg = (
            f"Cannot failback while state is {current_state}. "
            f"Wait for current operation."
        )
        logger.error(msg)
        return {"statusCode": 409, "body": msg}

    # Step 2: Config-aware data-tier gates (PR5 / F5)
    #
    # The failback Lambda has to behave differently for each of the 9 baseline
    # configurations (C1–C9). detect_data_tier_config() returns the four flags
    # that drive the per-tier branching below:
    #
    #   aurora_present + aurora_auto      → Lambda runs switchover itself
    #   aurora_present + not aurora_auto  → require aurora_confirmed=true
    #   not aurora_present                → no Aurora gate at all
    #   (same shape for Redis)
    #
    # Either tier can be overridden with skip_*_check=true as a break-glass
    # when the operator has human-verified the data tier is ready and just
    # wants the latch released.

    # Aurora gate
    if cfg["aurora_present"]:
        if cfg["aurora_auto"] and not aurora_confirmed and not skip_aurora_check:
            logger.info("Step 2a: Aurora auto-switchover from failback Lambda...")
            # _auto_switchover_aurora now blocks until the switchover completes
            # (F10 / v1.7.1). It returns success only after target_region is the
            # writer per describe_global_clusters, or fails with a 504-style
            # timeout error if the flip didn't happen in
            # FAILBACK_PROMOTION_WAIT_TIMEOUT_SECONDS (default 480s).
            result = _auto_switchover_aurora(target_region)
            if not result["success"]:
                # Distinguish initiate-failure vs wait-timeout for status code.
                is_timeout = "did not complete" in result.get("error", "")
                status_code = 504 if is_timeout else 500
                msg = (
                    f"Aurora auto-switchover FAILED.\n"
                    f"Error: {result['error']}\n\n"
                    f"Either restore Aurora's switchover capability and re-invoke "
                    f"this Lambda, or run the switchover manually and re-invoke "
                    f"with aurora_confirmed=true (or skip_aurora_check=true to "
                    f"bypass the gate entirely).\n\n"
                    f"{build_aurora_switchover_commands(target_region)}"
                )
                logger.error(msg)
                return {"statusCode": status_code, "body": msg}
            logger.info(
                f"Aurora switchover confirmed in "
                f"{result.get('elapsed_seconds', 0)}s"
            )
            aurora_confirmed = True  # we just did it AND verified completion
        elif not cfg["aurora_auto"] and not aurora_confirmed and not skip_aurora_check:
            commands = build_aurora_switchover_commands(target_region)
            msg = (
                f"Aurora switchover has NOT been confirmed and "
                f"AURORA_AUTO_PROMOTE is not enabled.\n\n"
                f"You must switchover Aurora manually BEFORE running failback, "
                f"OR pass skip_aurora_check=true if you have human-verified "
                f"that Aurora is already in {target_region}.\n\n"
                f"{commands}"
            )
            logger.info("Aurora not confirmed (manual mode), returning switchover commands")
            return {"statusCode": 400, "body": msg}
        else:
            logger.info(
                f"Aurora gate cleared "
                f"(aurora_confirmed={aurora_confirmed}, skip_aurora_check={skip_aurora_check})"
            )
    else:
        logger.info("Aurora gate skipped — Aurora not configured")

    # Redis gate (mirror of Aurora). Only fires when ElastiCache is configured.
    if cfg["redis_present"]:
        if cfg["redis_auto"] and not redis_confirmed and not skip_redis_check:
            logger.info("Step 2b: Redis auto-failover from failback Lambda...")
            result = _auto_failover_redis(target_region)
            if not result["success"]:
                msg = (
                    f"ElastiCache auto-failover FAILED.\n"
                    f"Error: {result['error']}\n\n"
                    f"Either restore ElastiCache's failover capability and "
                    f"re-invoke this Lambda, or run the failover manually and "
                    f"re-invoke with redis_confirmed=true (or skip_redis_check="
                    f"true to bypass the gate entirely).\n\n"
                    f"{build_redis_failover_commands(target_region)}"
                )
                logger.error(msg)
                return {"statusCode": 500, "body": msg}
            logger.info(
                f"ElastiCache failover confirmed in "
                f"{result.get('elapsed_seconds', 0)}s"
            )
            redis_confirmed = True  # we just did it AND verified completion
        elif not cfg["redis_auto"] and not redis_confirmed and not skip_redis_check:
            commands = build_redis_failover_commands(target_region)
            msg = (
                f"ElastiCache failover has NOT been confirmed and "
                f"ELASTICACHE_AUTO_PROMOTE is not enabled.\n\n"
                f"You must failover ElastiCache manually BEFORE running "
                f"failback, OR pass skip_redis_check=true if you have human-"
                f"verified that ElastiCache is already primary in {target_region}.\n\n"
                f"{commands}"
            )
            logger.info("Redis not confirmed (manual mode), returning failover commands")
            return {"statusCode": 400, "body": msg}
        else:
            logger.info(
                f"Redis gate cleared "
                f"(redis_confirmed={redis_confirmed}, skip_redis_check={skip_redis_check})"
            )
    else:
        logger.info("Redis gate skipped — ElastiCache not configured")

    # Step 2.5: AI Failback Readiness Assessment (non-blocking)
    readiness_appendix = ""
    skip_readiness_check = event.get("skip_readiness_check", False)
    readiness_enabled = os.environ.get("AI_FAILBACK_READINESS_ENABLED", "false").lower() == "true"
    if readiness_enabled and not skip_readiness_check:
        logger.info("Step 2.5: Running AI failback readiness assessment...")
        try:
            from ai.stability_collector import collect_stability_context
            from ai.failback_readiness import assess_failback_readiness, format_readiness_for_sns

            window_minutes = int(os.environ.get("AI_FAILBACK_STABILITY_WINDOW_MINUTES", "15"))
            stability = collect_stability_context(
                region=target_region,
                aurora_cluster_id=AURORA_CLUSTER_ID,
                aurora_global_cluster_id=AURORA_GLOBAL_CLUSTER_ID,
                ecs_cluster=ECS_CLUSTER_NAME,
                ecs_service=ECS_SERVICE_NAME,
                window_minutes=window_minutes,
            )
            assessment = assess_failback_readiness(stability, region=target_region)
            readiness_appendix = "\n\n" + format_readiness_for_sns(assessment, stability)

            verdict = assessment.get("verdict", "CAUTION")
            logger.info(
                f"Readiness verdict: {verdict}, "
                f"confidence: {assessment.get('confidence')}"
            )

            if verdict == "NO_GO":
                risks_lines = "\n".join(f"  • {r}" for r in assessment.get("risks", []))
                next_step = (
                    "Address the risks above, OR re-invoke the failback Lambda with "
                    "`skip_readiness_check=true` in the payload to override the AI "
                    "veto if you have human-verified that failback is safe.\n"
                    + (readiness_appendix or "")
                )
                logger.warning(f"Failback blocked by AI readiness: {verdict}")
                subject, body = compose_message(
                    severity=SEVERITY_CRITICAL,
                    what="Failback BLOCKED — AI readiness assessment says NO GO",
                    why=(
                        f"The AI failback-readiness check returned NO GO with "
                        f"{assessment.get('confidence')}% confidence. "
                        f"Reasoning: {assessment.get('reasoning', 'N/A')}"
                    ),
                    next_step=next_step,
                    context={
                        "Operator": operator,
                        "Target region": target_region,
                        "AI verdict": verdict,
                        "AI confidence": f"{assessment.get('confidence')}%",
                        "Risks":
                            f"\n{risks_lines}" if risks_lines else "(none reported)",
                    },
                    journey=[
                        "[✓] Failback requested",
                        "[✗] AI readiness gate — NO GO",
                        "[ ] Aurora switchover",
                        "[ ] Latch released",
                    ],
                    source="failover-failback",
                    region=CURRENT_REGION,
                )
                send_notification(subject, body)
                return {"statusCode": 400, "body": body}
        except Exception as e:
            logger.error(f"AI readiness assessment failed (non-blocking): {e}")
            # Continue with failback — AI failure should not block
    elif skip_readiness_check:
        logger.warning("Step 2.5: AI readiness check SKIPPED (skip_readiness_check=true)")

    # Step 3: Validate target region health (unless skipped)
    if not skip_health_check:
        logger.info("Step 3: Validating target region health...")
        health = validate_target_region_health(target_region)
        if not health["healthy"]:
            issue_lines = "\n".join(f"  • {i}" for i in health["issues"])
            msg_for_log = (
                f"Target region {target_region} is NOT healthy. Issues:\n{issue_lines}"
            )
            logger.error(msg_for_log)
            next_step = (
                f"Resolve each issue above in {target_region} before re-invoking "
                f"failback. Common causes: ECS tasks not yet healthy, Aurora not "
                f"yet the writer in {target_region} (run `switchover-global-cluster` "
                f"first), or app /healthcheck endpoint returning non-200. "
                f"To override the gate (only after you've human-verified target "
                f"readiness), re-invoke with `skip_health_check=true` in the payload."
            )
            subject, body = compose_message(
                severity=SEVERITY_CRITICAL,
                what=f"Failback BLOCKED — target region {target_region} is not ready",
                why=(
                    f"The pre-flight health check for {target_region} found "
                    f"{len(health['issues'])} issue(s). Failback would route traffic "
                    f"to a region that cannot serve it, so the orchestrator refused "
                    f"to release the latch."
                ),
                next_step=next_step,
                context={
                    "Operator": operator,
                    "Target region": target_region,
                    "Issues found": f"\n{issue_lines}",
                },
                journey=[
                    "[✓] Failback requested",
                    "[✗] Target region health gate — FAILED",
                    "[ ] Aurora switchover",
                    "[ ] Latch released",
                ],
                source="failover-failback",
                region=CURRENT_REGION,
            )
            send_notification(subject, body)
            return {"statusCode": 400, "body": body}
        logger.info(f"All health checks passed for {target_region}")
    else:
        logger.warning(
            "Step 3: Health checks SKIPPED (skip_health_check=true)"
        )

    # Step 4: Execute failback (DNS and state only)
    logger.info("Step 4: Executing failback...")
    try:
        logger.info("  4a: Setting state to FAILBACK_IN_PROGRESS...")
        update_failover_state({
            "state": "FAILBACK_IN_PROGRESS",
            "initiated_by": "MANUAL",
            "reason": f"Manual failback by {operator}",
        })

        logger.info(
            f"  4b: Publishing metric 1.0 for target region {target_region}..."
        )
        publish_region_health_metric(target_region, True)

        # NOTE: We do NOT publish for the old active region (active_region).
        # That would require a cross-region PutMetricData call to
        # monitoring.<other-region>.amazonaws.com, which times out in VPCs
        # with interface endpoints that only resolve local region endpoints.
        # The other region's orchestrator Lambda will publish its own metric
        # on its next 1-minute cycle, see the latch is released, evaluate
        # its health, and publish accordingly.

        logger.info("  4c: Releasing latch and setting final state...")
        new_state = (
            "PRIMARY_ACTIVE"
            if target_region == PRIMARY_REGION
            else "SECONDARY_ACTIVE"
        )
        update_failover_state({
            "active_region": target_region,
            "state": new_state,
            "last_failover_ts": now.isoformat(),
            "latch_engaged": False,
            "consecutive_failures": 0,
            "initiated_by": "MANUAL",
            "reason": f"Manual failback by {operator}",
            "aurora_promotion_pending": False,
            "redis_promotion_pending": False,
        })

        logger.info("  4d: Sending confirmation notification...")
        cfg = detect_data_tier_config()
        # Journey: failback is the inverse of failover. All earlier journey
        # steps map to "✓" and the lifecycle is back at PRIMARY_ACTIVE.
        journey = ["[✓] Failback requested", "[✓] Aurora switchover (operator)"]
        if cfg["redis_present"]:
            journey.append("[✓] Redis switchover (operator)")
        journey.append("[✓] Latch released — system back to PRIMARY_ACTIVE")
        next_step = (
            f"No action required — the system is fully back to normal. The "
            f"orchestrator's automated health monitoring is active again in both "
            f"regions. The latch has been released, so if {target_region} fails "
            f"again, automatic failover to {active_region} will fire normally. "
            f"Confirm Route 53 traffic is flowing to {target_region} (DNS TTL "
            f"~60s) and watch for any sustained-failure WARNINGs in the next "
            f"5–10 minutes to confirm the underlying issue is fully resolved."
            + (readiness_appendix or "")
        )
        subject, body = compose_message(
            severity=SEVERITY_INFO,
            what=(
                f"All back to normal — traffic returned to {target_region}, "
                f"failback complete"
            ),
            why=(
                f"Operator {operator} initiated and completed manual failback "
                f"from {active_region} back to {target_region}. The orchestrator "
                f"validated target health (HTTP 200, ECS healthy, Aurora writer in "
                f"{target_region}, Redis primary in {target_region} where applicable), "
                f"released the latch, and updated state to PRIMARY_ACTIVE. The "
                f"system is in steady state with {target_region} fully serving "
                f"traffic and the failover safety net armed."
            ),
            next_step=next_step,
            context={
                "Operator": operator,
                "From region": active_region,
                "To region": target_region,
                "New state": new_state,
                "Latch": "RELEASED — failover safety net armed",
            },
            journey=journey,
            source="failover-failback",
            region=CURRENT_REGION,
        )
        send_notification(subject, body)
        msg_for_return = body  # full body for any caller that logs it

        # Issue #98: emit lifecycle metrics so the dashboard sees the latch
        # release and the cumulative failback count immediately, without
        # waiting for the orchestrator's next 60s cycle.
        increment_counter("FailbacksCompleted", CURRENT_REGION,
                          dimensions={"Operator": operator})
        try:
            publish_state_metrics(get_failover_state(), CURRENT_REGION)
        except Exception as e:
            logger.warning(
                f"Failed to publish state metrics after failback (non-fatal): "
                f"{type(e).__name__}: {e}"
            )

        logger.info(f"FAILBACK COMPLETE: {active_region} -> {target_region}")
        return {"statusCode": 200, "body": msg_for_return}

    except Exception as e:
        logger.error(
            f"FAILBACK FAILED at step 4: {type(e).__name__}: {e}"
        )
        next_step = (
            "Manual intervention required. Inspect the Lambda's CloudWatch logs "
            "for the full traceback. Common causes: state backend write failure "
            "(check IAM + DynamoDB/S3 health), CloudWatch PutMetricData failure "
            "(check IAM), or Aurora describe-db-clusters returning unexpected "
            "shape. Once the root cause is fixed, re-invoke the failback Lambda "
            "with the same payload."
        )
        subject, body = compose_message(
            severity=SEVERITY_CRITICAL,
            what=f"Failback FAILED — {target_region} did not regain control",
            why=(
                f"The failback Lambda threw {type(e).__name__} during the "
                f"DNS/state update step (after health checks passed). Error: {e}"
            ),
            next_step=next_step,
            context={
                "Operator": operator,
                "Target region": target_region,
                "Error type": type(e).__name__,
                "Error detail": str(e)[:200],
            },
            journey=[
                "[✓] Failback requested",
                "[✓] Health gate passed",
                "[✗] DNS/state update — FAILED",
                "[ ] Latch released",
            ],
            source="failover-failback",
            region=CURRENT_REGION,
        )
        send_notification(subject, body)
        raise
