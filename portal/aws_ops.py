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
    """Create Aurora instances in both regions (~5 min each).

    If the secondary cluster was detached from the global (by delete_aurora_instances),
    rejoins it first. Creates primary instance first (required before secondary can join).
    """
    errors = []

    # Ensure secondary cluster exists and is part of the global cluster
    _ensure_secondary_cluster_in_global()

    # Create primary instance first (must exist before secondary instance)
    for region, cluster_id, inst_id in [
        (PRIMARY_REGION, AURORA_CLUSTER_W1, AURORA_INSTANCE_W1),
        (SECONDARY_REGION, AURORA_CLUSTER_W2, AURORA_INSTANCE_W2),
    ]:
        try:
            resp = _client("rds", region).describe_db_instances(DBInstanceIdentifier=inst_id)
            status = resp["DBInstances"][0].get("DBInstanceStatus", "") if resp.get("DBInstances") else ""
            if status not in ("", "deleting"):
                continue  # Already exists and not being deleted
        except ClientError:
            pass

        try:
            _client("rds", region).create_db_instance(
                DBInstanceIdentifier=inst_id,
                DBClusterIdentifier=cluster_id,
                DBInstanceClass="db.r6g.large",
                Engine="aurora-postgresql",
            )
            logger.info(f"Creating Aurora instance {inst_id} in {region}")

            # For primary: wait until available before creating secondary
            if region == PRIMARY_REGION:
                try:
                    waiter = _client("rds", region).get_waiter("db_instance_available")
                    waiter.wait(DBInstanceIdentifier=inst_id,
                                WaiterConfig={"Delay": 15, "MaxAttempts": 40})
                except Exception as e:
                    logger.warning(f"Waiter timeout for {inst_id}: {e}")

        except ClientError as e:
            if "DBInstanceAlreadyExists" not in str(e):
                errors.append(f"{region}: {e}")

    return errors


def delete_aurora_instances():
    """Delete Aurora instances only. Keep clusters in the global cluster.

    Deletes secondary instance first, waits, then deletes primary.
    Does NOT remove clusters from the global — that way create_aurora_instances
    only needs to create instances, not rebuild clusters.
    """
    import time
    errors = []

    # Step 1: Delete secondary instance
    try:
        _client("rds", SECONDARY_REGION).delete_db_instance(
            DBInstanceIdentifier=AURORA_INSTANCE_W2, SkipFinalSnapshot=True
        )
        logger.info("Deleting secondary Aurora instance...")
    except ClientError as e:
        if "DBInstanceNotFound" not in str(e) and "is already being deleted" not in str(e):
            errors.append(f"secondary instance: {e}")

    # Wait for secondary instance deletion
    for _ in range(60):
        try:
            resp = _client("rds", SECONDARY_REGION).describe_db_instances(
                DBInstanceIdentifier=AURORA_INSTANCE_W2
            )
            status = resp["DBInstances"][0].get("DBInstanceStatus", "") if resp.get("DBInstances") else ""
            if not status:
                break
            time.sleep(10)
        except ClientError:
            break  # Gone

    # Step 2: Delete primary instance
    # With secondary instance gone but cluster still in global, we need to
    # temporarily remove secondary cluster from global to delete primary instance
    try:
        _client("rds", PRIMARY_REGION).delete_db_instance(
            DBInstanceIdentifier=AURORA_INSTANCE_W1, SkipFinalSnapshot=True
        )
        logger.info("Deleting primary Aurora instance...")
    except ClientError as e:
        if "Cannot delete the last instance of the master cluster" in str(e):
            # Need to detach secondary cluster first
            logger.info("Detaching secondary cluster from global to allow primary deletion...")
            try:
                cluster_arn = f"arn:aws:rds:{SECONDARY_REGION}:{ACCOUNT_ID}:cluster:{AURORA_CLUSTER_W2}"
                _client("rds", SECONDARY_REGION).remove_from_global_cluster(
                    GlobalClusterIdentifier=AURORA_GLOBAL_CLUSTER,
                    DbClusterIdentifier=cluster_arn,
                )
                # Wait for detach
                for _ in range(30):
                    try:
                        g_resp = _client("rds", PRIMARY_REGION).describe_global_clusters(
                            GlobalClusterIdentifier=AURORA_GLOBAL_CLUSTER
                        )
                        members = [m["DBClusterArn"] for m in g_resp["GlobalClusters"][0].get("GlobalClusterMembers", [])]
                        if cluster_arn not in members:
                            break
                    except ClientError:
                        break
                    time.sleep(5)
                # Retry primary deletion
                _client("rds", PRIMARY_REGION).delete_db_instance(
                    DBInstanceIdentifier=AURORA_INSTANCE_W1, SkipFinalSnapshot=True
                )
                logger.info("Primary instance deleting after detach...")
            except ClientError as e2:
                errors.append(f"primary instance (after detach): {e2}")
        elif "DBInstanceNotFound" not in str(e) and "is already being deleted" not in str(e):
            errors.append(f"primary instance: {e}")

    return errors


def _ensure_secondary_cluster_in_global():
    """Ensure the secondary cluster exists and is part of the global cluster.

    Handles three states:
    1. Cluster exists and is in global → nothing to do
    2. Cluster exists but standalone (detached) → delete it, recreate in global
    3. Cluster doesn't exist → create it in global
    """
    import time
    cluster_arn = f"arn:aws:rds:{SECONDARY_REGION}:{ACCOUNT_ID}:cluster:{AURORA_CLUSTER_W2}"

    # Check if already in global
    try:
        g_resp = _client("rds", PRIMARY_REGION).describe_global_clusters(
            GlobalClusterIdentifier=AURORA_GLOBAL_CLUSTER
        )
        members = [m["DBClusterArn"] for m in g_resp["GlobalClusters"][0].get("GlobalClusterMembers", [])]
        if cluster_arn in members:
            logger.info("Secondary cluster already in global cluster")
            return
    except ClientError as e:
        logger.error(f"Cannot check global cluster: {e}")
        return

    # Check if cluster exists as standalone (needs to be deleted first)
    try:
        _client("rds", SECONDARY_REGION).describe_db_clusters(
            DBClusterIdentifier=AURORA_CLUSTER_W2
        )
        # Exists but not in global — delete it
        logger.info("Deleting standalone secondary cluster...")
        try:
            _client("rds", SECONDARY_REGION).delete_db_cluster(
                DBClusterIdentifier=AURORA_CLUSTER_W2, SkipFinalSnapshot=True
            )
        except ClientError:
            pass
        for _ in range(40):
            try:
                _client("rds", SECONDARY_REGION).describe_db_clusters(
                    DBClusterIdentifier=AURORA_CLUSTER_W2
                )
                time.sleep(5)
            except ClientError:
                break
    except ClientError:
        pass  # Doesn't exist — good, we'll create it

    # Create secondary cluster in the global
    logger.info("Creating secondary cluster in global...")
    kms_key = _client("kms", SECONDARY_REGION).describe_key(
        KeyId="alias/aws/rds"
    )["KeyMetadata"]["Arn"]
    sg_resp = _client("ec2", SECONDARY_REGION).describe_security_groups(
        Filters=[{"Name": "group-name", "Values": ["*AuroraSG*"]}]
    )
    sg_id = sg_resp["SecurityGroups"][0]["GroupId"] if sg_resp["SecurityGroups"] else None

    # Get engine version from the global cluster
    g_resp = _client("rds", PRIMARY_REGION).describe_global_clusters(
        GlobalClusterIdentifier=AURORA_GLOBAL_CLUSTER
    )
    engine_version = g_resp["GlobalClusters"][0].get("EngineVersion", "17.4")

    kwargs = {
        "DBClusterIdentifier": AURORA_CLUSTER_W2,
        "GlobalClusterIdentifier": AURORA_GLOBAL_CLUSTER,
        "Engine": "aurora-postgresql",
        "EngineVersion": engine_version,
        "StorageEncrypted": True,
        "KmsKeyId": kms_key,
        "DBSubnetGroupName": "fo-demo-aurora-subnet-group",
    }
    if sg_id:
        kwargs["VpcSecurityGroupIds"] = [sg_id]
    _client("rds", SECONDARY_REGION).create_db_cluster(**kwargs)

    # Wait for it to be available
    for _ in range(30):
        try:
            resp = _client("rds", SECONDARY_REGION).describe_db_clusters(
                DBClusterIdentifier=AURORA_CLUSTER_W2
            )
            if resp["DBClusters"][0].get("Status") == "available":
                logger.info("Secondary cluster created and available")
                return
        except ClientError:
            pass
        time.sleep(10)
    logger.warning("Timeout waiting for secondary cluster")


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


def get_failover_state(backend=None):
    """Read the current failover state from the active backend."""
    if backend is None:
        backend = _get_active_backend()

    if backend == "s3":
        return _get_state_from_s3()
    return _get_state_from_dynamodb()


def _get_active_backend():
    """Determine the active backend from the lock's test config."""
    try:
        from portal.lock import get_lock_status
        lock_info = get_lock_status()
        if lock_info.get("locked") and lock_info.get("test_config"):
            import json
            cfg = json.loads(lock_info["test_config"])
            return cfg.get("backend", "dynamodb")
    except Exception:
        pass
    return "dynamodb"


def _get_state_from_dynamodb():
    """Read state from DynamoDB."""
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


def _get_state_from_s3():
    """Read state from S3."""
    try:
        resp = _client("s3", PRIMARY_REGION).get_object(
            Bucket=S3_STATE_BUCKET_W1,
            Key="failover-state/REGION_STATE.json",
        )
        import json
        return json.loads(resp["Body"].read().decode("utf-8"))
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

    # Resolve alias name (v1.0 -> v1-0)
    ver_alias = ver_config.get("alias", version.replace(".", "-"))

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
        switch_active_alias(ver_alias, region)


def start_test(version, architecture, backend, provider):
    """Full test activation: configure, scale ECS, enable EventBridge, reset state."""
    configure_test(version, architecture, backend, provider)

    for region in BOTH_REGIONS:
        scale_ecs(2, region)
        enable_eventbridge(region)

    reset_state(backend)


def stop_test():
    """Full test deactivation: disable EventBridge, scale ECS to 0, reset state."""
    backend = _get_active_backend()
    for region in BOTH_REGIONS:
        disable_eventbridge(region)
        scale_ecs(0, region)
    reset_state(backend)


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

    active_backend = _get_active_backend()

    return {
        "lambda_aliases": aliases,
        "active_version": active_version,
        "active_alias_name": active_alias_name,
        "active_backend": active_backend,
        "ecs": {
            PRIMARY_REGION: get_ecs_status(PRIMARY_REGION),
            SECONDARY_REGION: get_ecs_status(SECONDARY_REGION),
        },
        "aurora": get_aurora_status(),
        "eventbridge": {
            PRIMARY_REGION: get_eventbridge_state(PRIMARY_REGION),
            SECONDARY_REGION: get_eventbridge_state(SECONDARY_REGION),
        },
        "state": get_failover_state(active_backend),
    }
