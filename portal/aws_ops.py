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
    """Get a cached boto3 client. Clears cache on credential errors."""
    key = (service, region)
    if key not in _clients:
        _clients[key] = boto3.client(service, region_name=region)
    return _clients[key]


def _clear_client_cache():
    """Clear cached clients (e.g., after credential refresh)."""
    _clients.clear()


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

    # Update 'active' on both Lambdas — resolve version per-function
    for func in [ORCHESTRATOR_LAMBDA, FAILBACK_LAMBDA]:
        try:
            resp = lam.get_alias(FunctionName=func, Name=version_alias)
            version = resp["FunctionVersion"]
        except ClientError:
            # Alias doesn't exist on this function — use $LATEST
            version = "$LATEST"

        try:
            lam.update_alias(FunctionName=func, Name="active", FunctionVersion=version)
        except ClientError as e:
            if "ResourceNotFoundException" in str(e):
                lam.create_alias(FunctionName=func, Name="active", FunctionVersion=version)
            else:
                raise


def update_lambda_env(env_vars, region):
    """Update environment variables on both Lambdas and force cold start."""
    lam = _client("lambda", region)
    for func in [ORCHESTRATOR_LAMBDA, FAILBACK_LAMBDA]:
        try:
            resp = lam.get_function_configuration(FunctionName=func)
            existing = resp.get("Environment", {}).get("Variables", {})
            existing.update(env_vars)

            # Wait for any pending update
            waiter = lam.get_waiter("function_updated_v2")
            waiter.wait(FunctionName=func, WaiterConfig={"Delay": 2, "MaxAttempts": 30})

            # Update env vars — this forces a new Lambda execution environment
            # (cold start), ensuring config changes take effect immediately
            lam.update_function_configuration(
                FunctionName=func, Environment={"Variables": existing}
            )

            waiter.wait(FunctionName=func, WaiterConfig={"Delay": 2, "MaxAttempts": 30})
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
    """Get Aurora instance status and writer/reader role in both regions."""
    result = {}
    for region, inst_id, cluster_id in [
        (PRIMARY_REGION, AURORA_INSTANCE_W1, AURORA_CLUSTER_W1),
        (SECONDARY_REGION, AURORA_INSTANCE_W2, AURORA_CLUSTER_W2),
    ]:
        try:
            resp = _client("rds", region).describe_db_instances(DBInstanceIdentifier=inst_id)
            if resp.get("DBInstances"):
                status = resp["DBInstances"][0].get("DBInstanceStatus", "unknown")
            else:
                status = "not-found"
        except ClientError:
            status = "not-found"

        # Determine writer/reader role from cluster's ReplicationSourceIdentifier
        role = "unknown"
        if status not in ("not-found",):
            try:
                c_resp = _client("rds", region).describe_db_clusters(DBClusterIdentifier=cluster_id)
                if c_resp.get("DBClusters"):
                    repl_src = c_resp["DBClusters"][0].get("ReplicationSourceIdentifier", "")
                    role = "reader" if repl_src else "writer"
            except ClientError:
                pass

        result[region] = {"status": status, "role": role}
    return result


def create_aurora_instances():
    """Create Aurora instances in both regions (~10 min total).

    Order: create primary instance → wait → ensure secondary cluster in global → create secondary instance.
    Primary must have a running instance before secondary cluster can join the global.
    """
    errors = []

    # Step 1: Create primary instance
    try:
        resp = _client("rds", PRIMARY_REGION).describe_db_instances(DBInstanceIdentifier=AURORA_INSTANCE_W1)
        status = resp["DBInstances"][0].get("DBInstanceStatus", "") if resp.get("DBInstances") else ""
        if status in ("", "deleting"):
            raise ClientError({"Error": {"Code": "DBInstanceNotFound"}}, "")
        logger.info(f"Primary instance already exists: {status}")
    except ClientError:
        try:
            _client("rds", PRIMARY_REGION).create_db_instance(
                DBInstanceIdentifier=AURORA_INSTANCE_W1,
                DBClusterIdentifier=AURORA_CLUSTER_W1,
                DBInstanceClass="db.r6g.large",
                Engine="aurora-postgresql",
            )
            logger.info("Creating primary Aurora instance...")
        except ClientError as e:
            if "DBInstanceAlreadyExists" not in str(e):
                errors.append(f"primary: {e}")
                return errors

    # Step 2: Wait for primary to be available
    logger.info("Waiting for primary instance...")
    try:
        waiter = _client("rds", PRIMARY_REGION).get_waiter("db_instance_available")
        waiter.wait(DBInstanceIdentifier=AURORA_INSTANCE_W1,
                    WaiterConfig={"Delay": 15, "MaxAttempts": 40})
    except Exception as e:
        logger.warning(f"Primary waiter: {e}")

    # Step 3: Ensure secondary cluster exists in global (needs primary instance running)
    _ensure_secondary_cluster_in_global()

    # Step 4: Create secondary instance
    try:
        resp = _client("rds", SECONDARY_REGION).describe_db_instances(DBInstanceIdentifier=AURORA_INSTANCE_W2)
        status = resp["DBInstances"][0].get("DBInstanceStatus", "") if resp.get("DBInstances") else ""
        if status in ("", "deleting"):
            raise ClientError({"Error": {"Code": "DBInstanceNotFound"}}, "")
        logger.info(f"Secondary instance already exists: {status}")
    except ClientError:
        try:
            _client("rds", SECONDARY_REGION).create_db_instance(
                DBInstanceIdentifier=AURORA_INSTANCE_W2,
                DBClusterIdentifier=AURORA_CLUSTER_W2,
                DBInstanceClass="db.r6g.large",
                Engine="aurora-postgresql",
            )
            logger.info("Creating secondary Aurora instance...")
        except ClientError as e:
            if "DBInstanceAlreadyExists" not in str(e):
                errors.append(f"secondary: {e}")

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
            # Need to detach AND delete secondary cluster first
            logger.info("Detaching secondary cluster from global...")
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

                # Delete the now-standalone secondary cluster
                logger.info("Deleting standalone secondary cluster...")
                try:
                    _client("rds", SECONDARY_REGION).delete_db_cluster(
                        DBClusterIdentifier=AURORA_CLUSTER_W2, SkipFinalSnapshot=True
                    )
                    for _ in range(40):
                        try:
                            _client("rds", SECONDARY_REGION).describe_db_clusters(
                                DBClusterIdentifier=AURORA_CLUSTER_W2
                            )
                            time.sleep(5)
                        except ClientError:
                            break
                except ClientError:
                    pass

                # Now retry primary deletion
                _client("rds", PRIMARY_REGION).delete_db_instance(
                    DBInstanceIdentifier=AURORA_INSTANCE_W1, SkipFinalSnapshot=True
                )
                logger.info("Primary instance deleting after detach+delete...")
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

    # Wait for it to be available (can take a few minutes)
    for _ in range(60):
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
    logger.warning("Timeout waiting for secondary cluster — it may still be creating")


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


def promote_aurora():
    """Promote Aurora to the non-writer region (switchover to wherever the reader is)."""
    # Determine current writer
    aurora = get_aurora_status()
    w1_role = aurora.get(PRIMARY_REGION, {}).get("role", "unknown") if isinstance(aurora.get(PRIMARY_REGION), dict) else "unknown"
    w2_role = aurora.get(SECONDARY_REGION, {}).get("role", "unknown") if isinstance(aurora.get(SECONDARY_REGION), dict) else "unknown"

    # Target is the reader (the one that needs to become writer)
    if w2_role == "reader":
        target_region = SECONDARY_REGION
        target_cluster = AURORA_CLUSTER_W2
        target_name = "us-west-2"
    elif w1_role == "reader":
        target_region = PRIMARY_REGION
        target_cluster = AURORA_CLUSTER_W1
        target_name = "us-west-1"
    else:
        return {"ok": False, "error": f"Cannot determine reader. w1={w1_role}, w2={w2_role}"}

    target_arn = f"arn:aws:rds:{target_region}:{ACCOUNT_ID}:cluster:{target_cluster}"

    try:
        _client("rds", PRIMARY_REGION).switchover_global_cluster(
            GlobalClusterIdentifier=AURORA_GLOBAL_CLUSTER,
            TargetDbClusterIdentifier=target_arn,
        )
        return {"ok": True, "message": f"Aurora switchover initiated to {target_name}"}
    except ClientError as e:
        if "InvalidGlobalClusterStateFault" in str(e):
            try:
                _client("rds", target_region).failover_global_cluster(
                    GlobalClusterIdentifier=AURORA_GLOBAL_CLUSTER,
                    TargetDbClusterIdentifier=target_arn,
                    AllowDataLoss=True,
                )
                return {"ok": True, "message": f"Aurora failover (unplanned) initiated to {target_name}"}
            except ClientError as e2:
                return {"ok": False, "error": str(e2)}
        return {"ok": False, "error": str(e)}


def invoke_failback(operator):
    """Invoke the failback Lambda to return traffic to primary."""
    payload = json.dumps({
        "target_region": PRIMARY_REGION,
        "skip_health_check": False,
        "operator": operator,
        "aurora_confirmed": True,
        "skip_readiness_check": True,
    })
    try:
        resp = _client("lambda", PRIMARY_REGION).invoke(
            FunctionName=f"{FAILBACK_LAMBDA}:active",
            InvocationType="RequestResponse",
            Payload=payload.encode("utf-8"),
        )
        result = json.loads(resp["Payload"].read().decode("utf-8"))
        return {"ok": True, "message": "Failback completed", "result": result}
    except ClientError as e:
        return {"ok": False, "error": str(e)}


def full_reset():
    """Full reset: switchover Aurora to primary if needed, stop test, reset state."""
    import time
    messages = []

    # Stop test first
    stop_test()
    messages.append("Test stopped")

    # Check if Aurora writer needs to switch back to primary
    aurora = get_aurora_status()
    w1_role = aurora.get(PRIMARY_REGION, {}).get("role", "unknown") if isinstance(aurora.get(PRIMARY_REGION), dict) else "unknown"

    if w1_role == "reader":
        # Need to switchover Aurora back to primary
        try:
            target_arn = f"arn:aws:rds:{PRIMARY_REGION}:{ACCOUNT_ID}:cluster:{AURORA_CLUSTER_W1}"
            _client("rds", PRIMARY_REGION).switchover_global_cluster(
                GlobalClusterIdentifier=AURORA_GLOBAL_CLUSTER,
                TargetDbClusterIdentifier=target_arn,
            )
            messages.append("Aurora switchover to primary initiated")

            # Wait for switchover
            for _ in range(30):
                try:
                    g_resp = _client("rds", PRIMARY_REGION).describe_global_clusters(
                        GlobalClusterIdentifier=AURORA_GLOBAL_CLUSTER
                    )
                    w1_is_writer = any(
                        m["IsWriter"] and AURORA_CLUSTER_W1 in m["DBClusterArn"]
                        for m in g_resp["GlobalClusters"][0].get("GlobalClusterMembers", [])
                    )
                    if w1_is_writer:
                        messages.append("Aurora writer restored to primary")
                        break
                except ClientError:
                    pass
                time.sleep(10)
        except ClientError as e:
            messages.append(f"Aurora switchover error: {e}")
    else:
        messages.append("Aurora writer already in primary")

    # Reset state in both backends
    reset_state("dynamodb")
    reset_state("s3")
    messages.append("State reset to PRIMARY_ACTIVE")

    return {"ok": True, "message": "; ".join(messages)}


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
