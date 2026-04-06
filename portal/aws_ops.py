"""AWS operations for the SentinelFO portal — stack-aware.

Every function takes a stack_id ('ddb' or 's3') to select which
set of AWS resources to operate on. No runtime env var switching.
"""

import json
import logging
import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

from portal.config import (
    PRIMARY_REGION, SECONDARY_REGION, BOTH_REGIONS, ACCOUNT_ID,
    ECS_CLUSTER, SNS_TOPIC_ARN, STACKS,
    VERSIONS, ARCHITECTURES, BACKENDS, PROVIDERS,
)

logger = logging.getLogger(__name__)


def _client(service, region=PRIMARY_REGION):
    return boto3.client(service, region_name=region)


def _stk(stack_id):
    """Get stack config dict."""
    return STACKS[stack_id]


# ── Lambda ──────────────────────────────────────────────────────────────────


def get_lambda_aliases(stack_id, region=PRIMARY_REGION):
    try:
        resp = _client("lambda", region).list_aliases(FunctionName=_stk(stack_id)["orchestrator_lambda"])
        return {a["Name"]: a["FunctionVersion"] for a in resp.get("Aliases", [])}
    except ClientError:
        return {}


def get_active_version(stack_id, region=PRIMARY_REGION):
    try:
        resp = _client("lambda", region).get_alias(
            FunctionName=_stk(stack_id)["orchestrator_lambda"], Name="active")
        return resp["FunctionVersion"]
    except ClientError:
        return None


def switch_alias(stack_id, version_alias, region):
    lam = _client("lambda", region)
    s = _stk(stack_id)
    for func in [s["orchestrator_lambda"], s["failback_lambda"]]:
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


def set_lambda_env(stack_id, env_vars, region):
    lam = _client("lambda", region)
    s = _stk(stack_id)
    for func in [s["orchestrator_lambda"], s["failback_lambda"]]:
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


def get_ecs_status(stack_id, region):
    try:
        resp = _client("ecs", region).describe_services(
            cluster=ECS_CLUSTER, services=[_stk(stack_id)["ecs_service"]])
        if not resp["services"]:
            return {"running": 0, "desired": 0}
        svc = resp["services"][0]
        return {"running": svc.get("runningCount", 0), "desired": svc.get("desiredCount", 0)}
    except ClientError:
        return {"running": 0, "desired": 0}


def scale_ecs(stack_id, desired, region):
    _client("ecs", region).update_service(
        cluster=ECS_CLUSTER, service=_stk(stack_id)["ecs_service"], desiredCount=desired)


# ── Aurora ──────────────────────────────────────────────────────────────────


def get_aurora_status(stack_id):
    s = _stk(stack_id)
    result = {}
    for region, inst_id, cluster_id in [
        (PRIMARY_REGION, s["aurora_instance_w1"], s["aurora_cluster_w1"]),
        (SECONDARY_REGION, s["aurora_instance_w2"], s["aurora_cluster_w2"]),
    ]:
        status, role = "not-found", "unknown"
        try:
            resp = _client("rds", region).describe_db_instances(DBInstanceIdentifier=inst_id)
            if resp.get("DBInstances"):
                status = resp["DBInstances"][0].get("DBInstanceStatus", "unknown")
        except ClientError:
            pass
        try:
            c = _client("rds", region).describe_db_clusters(DBClusterIdentifier=cluster_id)
            if c.get("DBClusters"):
                role = "reader" if c["DBClusters"][0].get("ReplicationSourceIdentifier") else "writer"
        except ClientError:
            pass
        result[region] = {"status": status, "role": role}
    return result


def promote_aurora(stack_id):
    s = _stk(stack_id)
    aurora = get_aurora_status(stack_id)
    w1_role = aurora.get(PRIMARY_REGION, {}).get("role", "unknown")
    w2_role = aurora.get(SECONDARY_REGION, {}).get("role", "unknown")
    if w2_role == "reader":
        target_arn = f"arn:aws:rds:{SECONDARY_REGION}:{ACCOUNT_ID}:cluster:{s['aurora_cluster_w2']}"
        target_name = "us-west-2"
    elif w1_role == "reader":
        target_arn = f"arn:aws:rds:{PRIMARY_REGION}:{ACCOUNT_ID}:cluster:{s['aurora_cluster_w1']}"
        target_name = "us-west-1"
    else:
        return {"ok": False, "error": f"Cannot determine reader (w1={w1_role}, w2={w2_role})"}
    try:
        _client("rds", PRIMARY_REGION).switchover_global_cluster(
            GlobalClusterIdentifier=s["aurora_global"], TargetDbClusterIdentifier=target_arn)
        return {"ok": True, "message": f"Aurora switchover initiated to {target_name}"}
    except ClientError as e:
        return {"ok": False, "error": str(e)}


# ── EventBridge ─────────────────────────────────────────────────────────────


def get_eventbridge_state(stack_id, region):
    try:
        resp = _client("events", region).describe_rule(Name=_stk(stack_id)["eventbridge_rule"])
        return resp.get("State", "UNKNOWN")
    except ClientError:
        return "NOT_FOUND"


def enable_eventbridge(stack_id, region):
    _client("events", region).enable_rule(Name=_stk(stack_id)["eventbridge_rule"])


def disable_eventbridge(stack_id, region):
    try:
        _client("events", region).disable_rule(Name=_stk(stack_id)["eventbridge_rule"])
    except ClientError:
        pass


# ── State ───────────────────────────────────────────────────────────────────


def get_failover_state(stack_id):
    s = _stk(stack_id)
    if s["state_backend"] == "s3":
        return _get_state_s3(s["s3_bucket_w1"])
    return _get_state_ddb(s["state_table"])


def _get_state_ddb(table):
    try:
        resp = _client("dynamodb", PRIMARY_REGION).get_item(
            TableName=table, Key={"pk": {"S": "REGION_STATE"}}, ConsistentRead=True)
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


def _get_state_s3(bucket):
    try:
        resp = _client("s3", PRIMARY_REGION).get_object(
            Bucket=bucket, Key="failover-state/REGION_STATE.json")
        return json.loads(resp["Body"].read().decode("utf-8"))
    except ClientError:
        return {}


def reset_state(stack_id):
    s = _stk(stack_id)
    now = datetime.now(timezone.utc).isoformat()
    if s["state_backend"] == "s3":
        data = {"active_region": PRIMARY_REGION, "state": "PRIMARY_ACTIVE",
                "latch_engaged": False, "consecutive_failures": 0,
                "last_failover_ts": "1970-01-01T00:00:00Z", "last_updated": now}
        for bucket in [s["s3_bucket_w1"], s["s3_bucket_w2"]]:
            try:
                _client("s3", PRIMARY_REGION).put_object(
                    Bucket=bucket, Key="failover-state/REGION_STATE.json",
                    Body=json.dumps(data).encode("utf-8"), ContentType="application/json")
            except ClientError:
                pass
    else:
        try:
            _client("dynamodb", PRIMARY_REGION).put_item(
                TableName=s["state_table"],
                Item={
                    "pk": {"S": "REGION_STATE"}, "active_region": {"S": PRIMARY_REGION},
                    "state": {"S": "PRIMARY_ACTIVE"}, "latch_engaged": {"BOOL": False},
                    "consecutive_failures": {"N": "0"},
                    "last_failover_ts": {"S": "1970-01-01T00:00:00Z"},
                    "last_updated": {"S": now}, "cooldown_reset": {"BOOL": False},
                    "last_warning_notification_ts": {"S": "1970-01-01T00:00:00Z"},
                })
        except ClientError:
            pass


# ── Composite Operations ────────────────────────────────────────────────────


def start_test(stack_id, version, architecture, backend_unused, provider):
    """Configure Lambda, reset state, then enable EventBridge.

    backend_unused: ignored — backend is baked into the stack at deploy time.
    """
    s = _stk(stack_id)

    # Step 1: Disable EventBridge
    for region in BOTH_REGIONS:
        disable_eventbridge(stack_id, region)

    # Step 2: Verify Aurora writer is in primary
    aurora = get_aurora_status(stack_id)
    if aurora.get(PRIMARY_REGION, {}).get("role") != "writer":
        raise RuntimeError("Aurora writer is not in primary. Click Reset Everything first.")

    # Step 3: ECS — primary always 2, secondary depends on architecture
    is_zero_container = architecture == "zero-container"
    scale_ecs(stack_id, 2, PRIMARY_REGION)
    if is_zero_container:
        scale_ecs(stack_id, 0, SECONDARY_REGION)
    else:
        ecs = get_ecs_status(stack_id, SECONDARY_REGION)
        if ecs["desired"] == 0:
            scale_ecs(stack_id, 2, SECONDARY_REGION)

    # Step 4: Reset state
    reset_state(stack_id)

    # Step 5: Configure Lambda env vars + alias
    env_vars = {}
    ver_config = VERSIONS.get(version, {})
    env_vars.update(ver_config.get("env_overrides", {}))
    env_vars["ROUTING_MODE"] = ARCHITECTURES.get(architecture, {}).get("env_overrides", {}).get("ROUTING_MODE", "failover")
    if ver_config.get("supports_provider") and provider:
        prov = PROVIDERS.get(provider, {})
        env_vars[prov.get("env_key", "AI_RCA_PROVIDER")] = prov.get("env_value", "claude")

    ver_alias = ver_config.get("alias", version.replace(".", "-"))

    for region in BOTH_REGIONS:
        region_vars = dict(env_vars)
        if is_zero_container and region == SECONDARY_REGION:
            region_vars["PASSIVE_PUBLISH_ZERO"] = "true"
        else:
            region_vars["PASSIVE_PUBLISH_ZERO"] = "false"
        set_lambda_env(stack_id, region_vars, region)
        switch_alias(stack_id, ver_alias, region)

    # Step 6: Enable EventBridge LAST
    for region in BOTH_REGIONS:
        enable_eventbridge(stack_id, region)


def stop_test(stack_id):
    for region in BOTH_REGIONS:
        disable_eventbridge(stack_id, region)
        scale_ecs(stack_id, 2, region)
    reset_state(stack_id)


def trigger_failover(stack_id):
    s = _stk(stack_id)
    # Reset cooldown in DynamoDB (works for DDB stack; S3 stack reads from S3 but cooldown reset helps)
    if s["state_backend"] == "dynamodb":
        try:
            _client("dynamodb", PRIMARY_REGION).update_item(
                TableName=s["state_table"], Key={"pk": {"S": "REGION_STATE"}},
                UpdateExpression="SET consecutive_failures = :zero, last_failover_ts = :epoch, cooldown_reset = :t",
                ExpressionAttributeValues={
                    ":zero": {"N": "0"}, ":epoch": {"S": "1970-01-01T00:00:00Z"}, ":t": {"BOOL": True}})
        except ClientError:
            pass
    else:
        # S3: overwrite state with reset cooldown
        state = get_failover_state(stack_id)
        if state:
            state["consecutive_failures"] = 0
            state["last_failover_ts"] = "1970-01-01T00:00:00Z"
            state["cooldown_reset"] = True
            try:
                _client("s3", PRIMARY_REGION).put_object(
                    Bucket=s["s3_bucket_w1"], Key="failover-state/REGION_STATE.json",
                    Body=json.dumps(state).encode("utf-8"), ContentType="application/json")
            except ClientError:
                pass
    scale_ecs(stack_id, 0, PRIMARY_REGION)


def full_reset(stack_id):
    messages = []
    for region in BOTH_REGIONS:
        disable_eventbridge(stack_id, region)
    messages.append("EventBridge disabled")

    # Aurora switchover back to primary if needed
    s = _stk(stack_id)
    aurora = get_aurora_status(stack_id)
    if aurora.get(PRIMARY_REGION, {}).get("role") == "reader":
        try:
            target_arn = f"arn:aws:rds:{PRIMARY_REGION}:{ACCOUNT_ID}:cluster:{s['aurora_cluster_w1']}"
            _client("rds", PRIMARY_REGION).switchover_global_cluster(
                GlobalClusterIdentifier=s["aurora_global"], TargetDbClusterIdentifier=target_arn)
            messages.append("Aurora switchover initiated (~60s)")
            for _ in range(30):
                try:
                    g = _client("rds", PRIMARY_REGION).describe_global_clusters(
                        GlobalClusterIdentifier=s["aurora_global"])
                    if any(m["IsWriter"] and s["aurora_cluster_w1"] in m["DBClusterArn"]
                           for m in g["GlobalClusters"][0].get("GlobalClusterMembers", [])):
                        messages.append("Aurora writer restored")
                        break
                except ClientError:
                    pass
                time.sleep(5)
        except ClientError as e:
            messages.append(f"Aurora: {e}")
    else:
        messages.append("Aurora already in primary")

    reset_state(stack_id)
    messages.append("State reset")
    for region in BOTH_REGIONS:
        scale_ecs(stack_id, 2, region)
    messages.append("ECS restored")
    return {"ok": True, "message": "; ".join(messages)}


def invoke_failback(stack_id, operator):
    s = _stk(stack_id)
    payload = json.dumps({
        "target_region": PRIMARY_REGION, "skip_health_check": False,
        "operator": operator, "aurora_confirmed": True, "skip_readiness_check": True})
    try:
        resp = _client("lambda", PRIMARY_REGION).invoke(
            FunctionName=f"{s['failback_lambda']}:active",
            InvocationType="RequestResponse", Payload=payload.encode("utf-8"))
        result = json.loads(resp["Payload"].read().decode("utf-8"))
        return {"ok": True, "message": "Failback completed", "result": result}
    except ClientError as e:
        return {"ok": False, "error": str(e)}


def get_full_status(stack_id):
    s = _stk(stack_id)
    aliases = get_lambda_aliases(stack_id)
    active_version = get_active_version(stack_id)
    active_alias_name = None
    for name, ver in aliases.items():
        if name != "active" and ver == active_version:
            active_alias_name = name
            break
    return {
        "stack": stack_id,
        "stack_name": s["name"],
        "lambda_aliases": aliases,
        "active_version": active_version,
        "active_alias_name": active_alias_name,
        "ecs": {
            PRIMARY_REGION: get_ecs_status(stack_id, PRIMARY_REGION),
            SECONDARY_REGION: get_ecs_status(stack_id, SECONDARY_REGION),
        },
        "aurora": get_aurora_status(stack_id),
        "eventbridge": {
            PRIMARY_REGION: get_eventbridge_state(stack_id, PRIMARY_REGION),
            SECONDARY_REGION: get_eventbridge_state(stack_id, SECONDARY_REGION),
        },
        "state": get_failover_state(stack_id),
    }
