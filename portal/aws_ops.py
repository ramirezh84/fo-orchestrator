"""AWS operations for the SentinelFO portal.

Simplified: Aurora and ECS run permanently. The portal only manages
Lambda configuration, EventBridge rules, and state resets.
"""

import json
import logging
import time
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


def _client(service, region=PRIMARY_REGION):
    return boto3.client(service, region_name=region)


# ── Lambda ──────────────────────────────────────────────────────────────────


def get_lambda_aliases(region=PRIMARY_REGION):
    try:
        resp = _client("lambda", region).list_aliases(FunctionName=ORCHESTRATOR_LAMBDA)
        return {a["Name"]: a["FunctionVersion"] for a in resp.get("Aliases", [])}
    except ClientError:
        return {}


def get_active_version(region=PRIMARY_REGION):
    try:
        resp = _client("lambda", region).get_alias(FunctionName=ORCHESTRATOR_LAMBDA, Name="active")
        return resp["FunctionVersion"]
    except ClientError:
        return None


def switch_alias(version_alias, region):
    """Point 'active' alias to the same version as version_alias, per function."""
    lam = _client("lambda", region)
    for func in [ORCHESTRATOR_LAMBDA, FAILBACK_LAMBDA]:
        try:
            resp = lam.get_alias(FunctionName=func, Name=version_alias)
            ver = resp["FunctionVersion"]
        except ClientError:
            ver = "$LATEST"
        try:
            lam.update_alias(FunctionName=func, Name="active", FunctionVersion=ver)
        except ClientError:
            try:
                lam.create_alias(FunctionName=func, Name="active", FunctionVersion=ver)
            except ClientError:
                pass


def set_lambda_env(env_vars, region):
    """Update env vars on both Lambdas. Simple retry on conflict."""
    lam = _client("lambda", region)
    for func in [ORCHESTRATOR_LAMBDA, FAILBACK_LAMBDA]:
        for attempt in range(5):
            try:
                resp = lam.get_function_configuration(FunctionName=func)
                if resp.get("LastUpdateStatus") == "InProgress":
                    time.sleep(2)
                    continue
                existing = resp.get("Environment", {}).get("Variables", {})
                existing.update(env_vars)
                lam.update_function_configuration(FunctionName=func, Environment={"Variables": existing})
                break
            except ClientError as e:
                if "ResourceConflictException" in str(e):
                    time.sleep(2)
                else:
                    logger.warning(f"Failed to update {func} in {region}: {e}")
                    break


# ── ECS ─────────────────────────────────────────────────────────────────────


def get_ecs_status(region):
    try:
        resp = _client("ecs", region).describe_services(cluster=ECS_CLUSTER, services=[ECS_SERVICE])
        if not resp["services"]:
            return {"running": 0, "desired": 0}
        svc = resp["services"][0]
        return {"running": svc.get("runningCount", 0), "desired": svc.get("desiredCount", 0)}
    except ClientError:
        return {"running": 0, "desired": 0}


def scale_ecs(desired, region):
    _client("ecs", region).update_service(cluster=ECS_CLUSTER, service=ECS_SERVICE, desiredCount=desired)


# ── Aurora ──────────────────────────────────────────────────────────────────


def get_aurora_status():
    result = {}
    for region, inst_id, cluster_id in [
        (PRIMARY_REGION, AURORA_INSTANCE_W1, AURORA_CLUSTER_W1),
        (SECONDARY_REGION, AURORA_INSTANCE_W2, AURORA_CLUSTER_W2),
    ]:
        status = "not-found"
        role = "unknown"
        try:
            resp = _client("rds", region).describe_db_instances(DBInstanceIdentifier=inst_id)
            if resp.get("DBInstances"):
                status = resp["DBInstances"][0].get("DBInstanceStatus", "unknown")
        except ClientError:
            pass
        try:
            c_resp = _client("rds", region).describe_db_clusters(DBClusterIdentifier=cluster_id)
            if c_resp.get("DBClusters"):
                repl_src = c_resp["DBClusters"][0].get("ReplicationSourceIdentifier", "")
                role = "reader" if repl_src else "writer"
        except ClientError:
            pass
        result[region] = {"status": status, "role": role}
    return result


def promote_aurora():
    """Switchover Aurora to whichever region is currently the reader."""
    aurora = get_aurora_status()
    w1_role = aurora.get(PRIMARY_REGION, {}).get("role", "unknown")
    w2_role = aurora.get(SECONDARY_REGION, {}).get("role", "unknown")

    if w2_role == "reader":
        target_arn = f"arn:aws:rds:{SECONDARY_REGION}:{ACCOUNT_ID}:cluster:{AURORA_CLUSTER_W2}"
        target_name = "us-west-2"
    elif w1_role == "reader":
        target_arn = f"arn:aws:rds:{PRIMARY_REGION}:{ACCOUNT_ID}:cluster:{AURORA_CLUSTER_W1}"
        target_name = "us-west-1"
    else:
        return {"ok": False, "error": f"Cannot determine reader (w1={w1_role}, w2={w2_role})"}

    try:
        _client("rds", PRIMARY_REGION).switchover_global_cluster(
            GlobalClusterIdentifier=AURORA_GLOBAL_CLUSTER,
            TargetDbClusterIdentifier=target_arn,
        )
        return {"ok": True, "message": f"Aurora switchover initiated to {target_name}"}
    except ClientError as e:
        return {"ok": False, "error": str(e)}


# ── EventBridge ─────────────────────────────────────────────────────────────


def get_eventbridge_state(region):
    try:
        resp = _client("events", region).describe_rule(Name=EVENTBRIDGE_RULE)
        return resp.get("State", "UNKNOWN")
    except ClientError:
        return "NOT_FOUND"


def enable_eventbridge(region):
    _client("events", region).enable_rule(Name=EVENTBRIDGE_RULE)


def disable_eventbridge(region):
    try:
        _client("events", region).disable_rule(Name=EVENTBRIDGE_RULE)
    except ClientError:
        pass


# ── State ───────────────────────────────────────────────────────────────────


def get_failover_state(backend=None):
    if backend is None:
        backend = _get_active_backend()
    if backend == "s3":
        return _get_state_s3()
    return _get_state_ddb()


def _get_active_backend():
    try:
        from portal.lock import get_lock_status
        info = get_lock_status()
        if info.get("locked") and info.get("test_config"):
            cfg = json.loads(info["test_config"])
            return cfg.get("backend", "dynamodb")
    except Exception:
        pass
    return "dynamodb"


def _get_state_ddb():
    try:
        resp = _client("dynamodb", PRIMARY_REGION).get_item(
            TableName=STATE_TABLE, Key={"pk": {"S": "REGION_STATE"}}, ConsistentRead=True
        )
        item = resp.get("Item")
        if not item:
            return {}
        result = {}
        for k, v in item.items():
            if "S" in v: result[k] = v["S"]
            elif "N" in v: result[k] = int(v["N"])
            elif "BOOL" in v: result[k] = v["BOOL"]
        return result
    except ClientError:
        return {}


def _get_state_s3():
    try:
        resp = _client("s3", PRIMARY_REGION).get_object(
            Bucket=S3_STATE_BUCKET_W1, Key="failover-state/REGION_STATE.json"
        )
        return json.loads(resp["Body"].read().decode("utf-8"))
    except ClientError:
        return {}


def reset_state(backend="dynamodb"):
    now = datetime.now(timezone.utc).isoformat()
    if backend == "s3":
        data = {"active_region": PRIMARY_REGION, "state": "PRIMARY_ACTIVE",
                "latch_engaged": False, "consecutive_failures": 0,
                "last_failover_ts": "1970-01-01T00:00:00Z", "last_updated": now}
        try:
            _client("s3", PRIMARY_REGION).put_object(
                Bucket=S3_STATE_BUCKET_W1, Key="failover-state/REGION_STATE.json",
                Body=json.dumps(data).encode("utf-8"), ContentType="application/json"
            )
        except ClientError:
            pass
    else:
        try:
            _client("dynamodb", PRIMARY_REGION).put_item(
                TableName=STATE_TABLE,
                Item={
                    "pk": {"S": "REGION_STATE"}, "active_region": {"S": PRIMARY_REGION},
                    "state": {"S": "PRIMARY_ACTIVE"}, "latch_engaged": {"BOOL": False},
                    "consecutive_failures": {"N": "0"},
                    "last_failover_ts": {"S": "1970-01-01T00:00:00Z"},
                    "last_updated": {"S": now}, "cooldown_reset": {"BOOL": False},
                    "last_warning_notification_ts": {"S": "1970-01-01T00:00:00Z"},
                }
            )
        except ClientError:
            pass


# ── Composite Operations ────────────────────────────────────────────────────


def start_test(version, architecture, backend, provider):
    """Configure Lambda env vars, switch alias, enable EventBridge, reset state."""
    env_vars = {}
    ver_config = VERSIONS.get(version, {})
    env_vars.update(ver_config.get("env_overrides", {}))
    env_vars.update(ARCHITECTURES.get(architecture, {}).get("env_overrides", {}))
    env_vars.update(BACKENDS.get(backend, {}).get("env_overrides", {}))
    if ver_config.get("supports_provider") and provider:
        prov = PROVIDERS.get(provider, {})
        env_vars[prov.get("env_key", "AI_RCA_PROVIDER")] = prov.get("env_value", "claude")

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
        set_lambda_env(region_vars, region)
        switch_alias(ver_alias, region)
        enable_eventbridge(region)

    reset_state(backend)


def stop_test():
    """Disable EventBridge, reset state."""
    backend = _get_active_backend()
    for region in BOTH_REGIONS:
        disable_eventbridge(region)
    reset_state(backend)


def trigger_failover():
    """Inject failure: scale ECS to 0 in primary, reset cooldown."""
    try:
        _client("dynamodb", PRIMARY_REGION).update_item(
            TableName=STATE_TABLE, Key={"pk": {"S": "REGION_STATE"}},
            UpdateExpression="SET consecutive_failures = :zero, last_failover_ts = :epoch, cooldown_reset = :t",
            ExpressionAttributeValues={
                ":zero": {"N": "0"}, ":epoch": {"S": "1970-01-01T00:00:00Z"}, ":t": {"BOOL": True}
            }
        )
    except ClientError:
        pass
    scale_ecs(0, PRIMARY_REGION)


def full_reset():
    """Stop test, switchover Aurora to primary if needed, reset state, restore ECS."""
    messages = []

    # Stop test
    for region in BOTH_REGIONS:
        disable_eventbridge(region)
    messages.append("EventBridge disabled")

    # Check if Aurora needs switchover
    aurora = get_aurora_status()
    w1_role = aurora.get(PRIMARY_REGION, {}).get("role", "unknown")
    if w1_role == "reader":
        try:
            target_arn = f"arn:aws:rds:{PRIMARY_REGION}:{ACCOUNT_ID}:cluster:{AURORA_CLUSTER_W1}"
            _client("rds", PRIMARY_REGION).switchover_global_cluster(
                GlobalClusterIdentifier=AURORA_GLOBAL_CLUSTER,
                TargetDbClusterIdentifier=target_arn,
            )
            messages.append("Aurora switchover to primary initiated (~60s)")
            for _ in range(30):
                try:
                    g = _client("rds", PRIMARY_REGION).describe_global_clusters(
                        GlobalClusterIdentifier=AURORA_GLOBAL_CLUSTER)
                    if any(m["IsWriter"] and AURORA_CLUSTER_W1 in m["DBClusterArn"]
                           for m in g["GlobalClusters"][0].get("GlobalClusterMembers", [])):
                        messages.append("Aurora writer restored to primary")
                        break
                except ClientError:
                    pass
                time.sleep(5)
        except ClientError as e:
            messages.append(f"Aurora switchover: {e}")
    else:
        messages.append("Aurora already in primary")

    # Reset state
    reset_state("dynamodb")
    reset_state("s3")
    messages.append("State reset")

    # Restore ECS in both regions
    for region in BOTH_REGIONS:
        scale_ecs(2, region)
    messages.append("ECS restored to 2 in both regions")

    return {"ok": True, "message": "; ".join(messages)}


def invoke_failback(operator):
    """Invoke the failback Lambda."""
    payload = json.dumps({
        "target_region": PRIMARY_REGION, "skip_health_check": False,
        "operator": operator, "aurora_confirmed": True, "skip_readiness_check": True,
    })
    try:
        resp = _client("lambda", PRIMARY_REGION).invoke(
            FunctionName=f"{FAILBACK_LAMBDA}:active",
            InvocationType="RequestResponse", Payload=payload.encode("utf-8"),
        )
        result = json.loads(resp["Payload"].read().decode("utf-8"))
        return {"ok": True, "message": "Failback completed", "result": result}
    except ClientError as e:
        return {"ok": False, "error": str(e)}


def get_full_status():
    aliases = get_lambda_aliases()
    active_version = get_active_version()
    active_alias_name = None
    for name, ver in aliases.items():
        if name != "active" and ver == active_version:
            active_alias_name = name
            break

    backend = _get_active_backend()
    return {
        "lambda_aliases": aliases,
        "active_version": active_version,
        "active_alias_name": active_alias_name,
        "active_backend": backend,
        "ecs": {
            PRIMARY_REGION: get_ecs_status(PRIMARY_REGION),
            SECONDARY_REGION: get_ecs_status(SECONDARY_REGION),
        },
        "aurora": get_aurora_status(),
        "eventbridge": {
            PRIMARY_REGION: get_eventbridge_state(PRIMARY_REGION),
            SECONDARY_REGION: get_eventbridge_state(SECONDARY_REGION),
        },
        "state": get_failover_state(backend),
    }
