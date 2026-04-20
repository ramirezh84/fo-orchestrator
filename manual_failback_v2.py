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
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from state_backend import create_backend, S3StateBackend

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
ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID = os.environ.get("ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID", "")

# Derive AWS account ID from the SNS topic ARN
_AWS_ACCOUNT_ID = SNS_TOPIC_ARN.split(":")[4] if ":" in SNS_TOPIC_ARN else ""

# Application name - included in all SNS notifications
APP_NAME = os.environ.get("APP_NAME", "")

HEALTH_CHECK_URL = os.environ.get("HEALTH_CHECK_URL", "")
HEALTH_ENDPOINT = os.environ.get("HEALTH_ENDPOINT", "/actuator/health")
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
    """Prepend APP_NAME to notification subject if configured."""
    if APP_NAME:
        return f"[{APP_NAME}] {subject}"[:100]
    return subject[:100]


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
        "aurora_confirmed": true    <-- REQUIRED: operator confirms Aurora is switched
    }
    """
    _reload_dynamic_config()

    target_region = event.get("target_region", PRIMARY_REGION)
    skip_health_check = event.get("skip_health_check", False)
    operator = event.get("operator", "unknown")
    aurora_confirmed = event.get("aurora_confirmed", False)
    now = datetime.now(timezone.utc)

    logger.info(
        f"Manual failback initiated by {operator} to {target_region} | "
        f"skip_health_check={skip_health_check}, "
        f"aurora_confirmed={aurora_confirmed}, "
        f"current_region={CURRENT_REGION}"
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

    # Step 2: Check if Aurora has been confirmed by the operator
    logger.info("Step 2: Checking Aurora confirmation...")
    if not aurora_confirmed:
        commands = build_aurora_switchover_commands(target_region)
        msg = (
            f"Aurora switchover has NOT been confirmed.\n\n"
            f"You must switchover Aurora BEFORE running failback.\n\n"
            f"{commands}\n\n"
            f"Once Aurora switchover is complete, run this Lambda again "
            f"with aurora_confirmed=true."
        )
        logger.info("Aurora not confirmed, returning switchover commands")
        return {"statusCode": 400, "body": msg}
    logger.info("Aurora confirmed by operator")

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
                msg = (
                    f"AI readiness assessment: NO GO (confidence: {assessment.get('confidence')}%)\n\n"
                    f"Reasoning: {assessment.get('reasoning', 'N/A')}\n\n"
                    f"Risks:\n"
                    + "\n".join(f"  - {r}" for r in assessment.get("risks", []))
                    + "\n\nTo override, set skip_readiness_check=true in the payload."
                )
                logger.warning(f"Failback blocked by AI readiness: {verdict}")
                send_notification(
                    f"FAILBACK BLOCKED: AI readiness assessment says NO GO",
                    msg + readiness_appendix,
                )
                return {"statusCode": 400, "body": msg}
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
            msg = (
                f"Target region {target_region} is NOT healthy. Issues:\n"
                + "\n".join(f"  - {i}" for i in health["issues"])
                + "\n\nTo override, set skip_health_check=true in the payload."
                + "\nIf Aurora is not the writer, you must switchover Aurora "
                + "first."
            )
            logger.error(msg)
            send_notification(
                f"FAILBACK BLOCKED: {target_region} not ready", msg
            )
            return {"statusCode": 400, "body": msg}
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
        msg = (
            f"Manual failback completed successfully.\n\n"
            f"Operator: {operator}\n"
            f"From: {active_region}\n"
            f"To: {target_region}\n"
            f"Time: {now.isoformat()}\n\n"
            f"Latch has been RELEASED. Automated health monitoring is active.\n"
            f"The orchestrator Lambda will resume normal health evaluation."
        )
        send_notification(f"FAILBACK COMPLETE: -> {target_region}", msg + readiness_appendix)

        logger.info(f"FAILBACK COMPLETE: {active_region} -> {target_region}")
        return {"statusCode": 200, "body": msg}

    except Exception as e:
        logger.error(
            f"FAILBACK FAILED at step 4: {type(e).__name__}: {e}"
        )
        send_notification(
            f"FAILBACK FAILED: -> {target_region}",
            f"Manual failback failed.\nError: {str(e)}\n\n"
            f"Manual intervention required.",
        )
        raise
