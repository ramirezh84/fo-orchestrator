"""AWS operations for the SentinelFO portal."""

import json
import logging
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

from portal.config import (
    PRIMARY_REGION, SECONDARY_REGION, BOTH_REGIONS, ACCOUNT_ID,
    ORCHESTRATOR_LAMBDA, FAILBACK_LAMBDA,
    ECS_CLUSTER, ECS_SERVICE,
    STATE_TABLE,
    AURORA_GLOBAL_CLUSTER, AURORA_CLUSTER_W1, AURORA_CLUSTER_W2,
    AURORA_INSTANCE_W1, AURORA_INSTANCE_W2,
    EVENTBRIDGE_RULE,
    S3_STATE_BUCKET_W1, S3_STATE_BUCKET_W2,
    VERSIONS, ARCHITECTURES, BACKENDS, PROVIDERS,
)

logger = logging.getLogger(__name__)

_clients = {}


def _client(service, region=PRIMARY_REGION):
    key = (service, region)
    if key not in _clients:
        _clients[key] = boto3.client(service, region_name=region)
    return _clients[key]


# ── Lambda Operations ──────────────────────────────────────────────────────────


def get_lambda_aliases(region=PRIMARY_REGION):
    """Get all aliases for the orchestrator Lambda."""
    try:
        resp = _client("lambda", region).list_aliases(FunctionName=ORCHESTRATOR_LAMBDA)
        return {
            a["Name"]: {"version": a["FunctionVersion"], "description": a.get("Description", "")}
            for a in resp.get("Aliases", [])
        }
    except ClientError:
        return {}


def get_active_alias_version(region=PRIMARY_REGION):
    """Get the version number that the 'active' alias points to."""
    try:
        resp = _client("lambda", region).get_alias(
            FunctionName=ORCHESTRATOR_LAMBDA, Name="active"
        )
        return resp["FunctionVersion"]
    except ClientError:
        return None


def switch_active_alias(version_alias, region):
    """Point 'active' alias to the same version as the given alias."""
    lam = _client("lambda", region)
    # Get version from the source alias
    resp = lam.get_alias(FunctionName=ORCHESTRATOR_LAMBDA, Name=version_alias)
    version = resp["FunctionVersion"]

    # Update 'active' on both Lambdas
    for func in [ORCHESTRATOR_LAMBDA, FAILBACK_LAMBDA]:
        try:
            lam.update_alias(FunctionName=func, Name="active", FunctionVersion=version)
        except ClientError as e:
            if "ResourceNotFoundException" in str(e):
                lam.create_alias(FunctionName=func, Name="active", FunctionVersion=version)
            else:
                raise


def update_lambda_env(env_vars, region):
    """Update environment variables on both Lambdas."""
    lam = _client("lambda", region)
    for func in [ORCHESTRATOR_LAMBDA, FAILBACK_LAMBDA]:
        try:
            resp = lam.get_function_configuration(FunctionName=func)
            existing = resp.get("Environment", {}).get("Variables", {})
            existing.update(env_vars)

            # Wait for any pending update
            waiter = lam.get_waiter("function_updated_v2")
            waiter.wait(FunctionName=func, WaiterConfig={"Delay": 2, "MaxAttempts": 30})

            lam.update_function_configuration(
                FunctionName=func, Environment={"Variables": existing}
            )
        except ClientError as e:
            logger.warning(f"Failed to update env on {func} in {region}: {e}")


# ── ECS Operations ─────────────────────────────────────────────────────────────


def get_ecs_status(region):
    """Get ECS service running/desired counts."""
    try:
        resp = _client("ecs", region).describe_services(
            cluster=ECS_CLUSTER, services=[ECS_SERVICE]
        )
        if not resp["services"]:
            return {"running": None, "desired": None}
        svc = resp["services"][0]
        return {"running": svc.get("runningCount"), "desired": svc.get("desiredCount")}
    except ClientError:
        return {"running": None, "desired": None}


def scale_ecs(desired, region):
    """Scale ECS service to desired count."""
    _client("ecs", region).update_service(
        cluster=ECS_CLUSTER, service=ECS_SERVICE, desiredCount=desired
    )


# ── Aurora Operations ──────────────────────────────────────────────────────────


def get_aurora_status():
    """Get Aurora instance status in both regions."""
    result = {}
    for region, inst_id in [(PRIMARY_REGION, AURORA_INSTANCE_W1), (SECONDARY_REGION, AURORA_INSTANCE_W2)]:
        try:
            resp = _client("rds", region).describe_db_instances(DBInstanceIdentifier=inst_id)
            if resp.get("DBInstances"):
                result[region] = resp["DBInstances"][0].get("DBInstanceStatus", "unknown")
            else:
                result[region] = "not-found"
        except ClientError:
            result[region] = "not-found"
    return result


def create_aurora_instances():
    """Create Aurora instances in both regions (~5 min each)."""
    errors = []
    for region, cluster_id, inst_id in [
        (PRIMARY_REGION, AURORA_CLUSTER_W1, AURORA_INSTANCE_W1),
        (SECONDARY_REGION, AURORA_CLUSTER_W2, AURORA_INSTANCE_W2),
    ]:
        try:
            _client("rds", region).describe_db_instances(DBInstanceIdentifier=inst_id)
            continue  # Already exists
        except ClientError:
            pass

        try:
            _client("rds", region).create_db_instance(
                DBInstanceIdentifier=inst_id,
                DBClusterIdentifier=cluster_id,
                DBInstanceClass="db.r6g.large",
                Engine="aurora-postgresql",
            )
        except ClientError as e:
            errors.append(f"{region}: {e}")

    return errors


def delete_aurora_instances():
    """Delete Aurora instances in both regions."""
    errors = []
    for region, inst_id in [
        (SECONDARY_REGION, AURORA_INSTANCE_W2),
        (PRIMARY_REGION, AURORA_INSTANCE_W1),
    ]:
        try:
            _client("rds", region).delete_db_instance(
                DBInstanceIdentifier=inst_id, SkipFinalSnapshot=True
            )
        except ClientError as e:
            if "DBInstanceNotFound" not in str(e) and "is already being deleted" not in str(e):
                errors.append(f"{region}: {e}")
    return errors


# ── EventBridge Operations ────────────────────────────────────────────────────


def get_eventbridge_state(region):
    """Get EventBridge rule state."""
    try:
        resp = _client("events", region).describe_rule(Name=EVENTBRIDGE_RULE)
        return resp.get("State", "UNKNOWN")
    except ClientError:
        return "NOT_FOUND"


def enable_eventbridge(region):
    """Enable the EventBridge rule."""
    _client("events", region).enable_rule(Name=EVENTBRIDGE_RULE)


def disable_eventbridge(region):
    """Disable the EventBridge rule."""
    try:
        _client("events", region).disable_rule(Name=EVENTBRIDGE_RULE)
    except ClientError:
        pass


# ── State Operations ──────────────────────────────────────────────────────────


def get_failover_state():
    """Read the current failover state from DynamoDB."""
    try:
        resp = _client("dynamodb", PRIMARY_REGION).get_item(
            TableName=STATE_TABLE,
            Key={"pk": {"S": "REGION_STATE"}},
            ConsistentRead=True,
        )
        item = resp.get("Item")
        if not item:
            return {}
        result = {}
        for k, v in item.items():
            if "S" in v:
                result[k] = v["S"]
            elif "N" in v:
                result[k] = int(v["N"])
            elif "BOOL" in v:
                result[k] = v["BOOL"]
        return result
    except ClientError:
        return {}


def reset_state(backend="dynamodb"):
    """Reset failover state to PRIMARY_ACTIVE."""
    now = datetime.now(timezone.utc).isoformat()

    if backend == "s3":
        data = {
            "active_region": PRIMARY_REGION,
            "state": "PRIMARY_ACTIVE",
            "latch_engaged": False,
            "consecutive_failures": 0,
            "last_failover_ts": "1970-01-01T00:00:00Z",
            "last_updated": now,
        }
        _client("s3", PRIMARY_REGION).put_object(
            Bucket=S3_STATE_BUCKET_W1,
            Key="failover-state/REGION_STATE.json",
            Body=json.dumps(data).encode("utf-8"),
            ContentType="application/json",
        )
    else:
        _client("dynamodb", PRIMARY_REGION).put_item(
            TableName=STATE_TABLE,
            Item={
                "pk": {"S": "REGION_STATE"},
                "active_region": {"S": PRIMARY_REGION},
                "state": {"S": "PRIMARY_ACTIVE"},
                "latch_engaged": {"BOOL": False},
                "consecutive_failures": {"N": "0"},
                "last_failover_ts": {"S": "1970-01-01T00:00:00Z"},
                "last_updated": {"S": now},
                "cooldown_reset": {"BOOL": False},
                "last_warning_notification_ts": {"S": "1970-01-01T00:00:00Z"},
            },
        )


# ── Composite Operations ─────────────────────────────────────────────────────


def configure_test(version, architecture, backend, provider):
    """Set Lambda env vars + switch alias for the given test configuration."""
    env_vars = {}

    # Version-specific overrides
    ver_config = VERSIONS.get(version, {})
    env_vars.update(ver_config.get("env_overrides", {}))

    # Architecture overrides
    arch_config = ARCHITECTURES.get(architecture, {})
    env_vars.update(arch_config.get("env_overrides", {}))

    # Backend overrides
    backend_config = BACKENDS.get(backend, {})
    env_vars.update(backend_config.get("env_overrides", {}))

    # Provider
    if ver_config.get("supports_provider") and provider:
        prov_config = PROVIDERS.get(provider, {})
        env_vars[prov_config.get("env_key", "AI_RCA_PROVIDER")] = prov_config.get("env_value", "claude")

    # S3 backend needs per-region bucket names
    if backend == "s3":
        # Set in the per-region loop below
        pass

    for region in BOTH_REGIONS:
        region_vars = dict(env_vars)
        if backend == "s3":
            if region == PRIMARY_REGION:
                region_vars["STATE_BUCKET"] = S3_STATE_BUCKET_W1
                region_vars["REMOTE_STATE_BUCKET"] = S3_STATE_BUCKET_W2
            else:
                region_vars["STATE_BUCKET"] = S3_STATE_BUCKET_W2
                region_vars["REMOTE_STATE_BUCKET"] = S3_STATE_BUCKET_W1

        update_lambda_env(region_vars, region)
        switch_active_alias(version, region)


def start_test(version, architecture, backend, provider):
    """Full test activation: configure, scale ECS, enable EventBridge, reset state."""
    configure_test(version, architecture, backend, provider)

    for region in BOTH_REGIONS:
        scale_ecs(2, region)
        enable_eventbridge(region)

    reset_state(backend)


def stop_test():
    """Full test deactivation: disable EventBridge, scale ECS to 0."""
    for region in BOTH_REGIONS:
        disable_eventbridge(region)
        scale_ecs(0, region)


def trigger_failover():
    """Inject failure by scaling ECS to 0 in primary and resetting cooldown."""
    # Reset cooldown
    try:
        _client("dynamodb", PRIMARY_REGION).update_item(
            TableName=STATE_TABLE,
            Key={"pk": {"S": "REGION_STATE"}},
            UpdateExpression="SET consecutive_failures = :zero, last_failover_ts = :epoch, cooldown_reset = :t",
            ExpressionAttributeValues={
                ":zero": {"N": "0"},
                ":epoch": {"S": "1970-01-01T00:00:00Z"},
                ":t": {"BOOL": True},
            },
        )
    except ClientError:
        pass

    scale_ecs(0, PRIMARY_REGION)


def get_full_status():
    """Aggregate status from all AWS services."""
    aliases = get_lambda_aliases()
    active_version = get_active_alias_version()

    # Find which named alias matches the active version
    active_alias_name = None
    for name, info in aliases.items():
        if name != "active" and info["version"] == active_version:
            active_alias_name = name
            break

    return {
        "lambda_aliases": aliases,
        "active_version": active_version,
        "active_alias_name": active_alias_name,
        "ecs": {
            PRIMARY_REGION: get_ecs_status(PRIMARY_REGION),
            SECONDARY_REGION: get_ecs_status(SECONDARY_REGION),
        },
        "aurora": get_aurora_status(),
        "eventbridge": {
            PRIMARY_REGION: get_eventbridge_state(PRIMARY_REGION),
            SECONDARY_REGION: get_eventbridge_state(SECONDARY_REGION),
        },
        "state": get_failover_state(),
    }
