#!/usr/bin/env python3
"""
Demo Manager CLI — manages 9 failover orchestrator demo environments.

Each environment is a combination of version (v1.0, v1.1-claude, v1.1-gemini)
and use case (active/passive, zero-container, active/active).

Usage:
    python3 tools/demo_manager.py status
    python3 tools/demo_manager.py activate fo-v10-s1
    python3 tools/demo_manager.py deactivate fo-v10-s1
    python3 tools/demo_manager.py trigger fo-v10-s1 [--watch]
    python3 tools/demo_manager.py reset fo-v10-s1
    python3 tools/demo_manager.py activate-all
    python3 tools/demo_manager.py deactivate-all
"""

import argparse
import sys
import time
import json
import os
import traceback
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    print("ERROR: boto3 is required. Install with: pip install boto3")
    sys.exit(1)

# ── Constants ───────────────────────────────────────────────────────────────────

PRIMARY_REGION = "us-west-1"
SECONDARY_REGION = "us-west-2"
ACCOUNT_ID = "597088043823"
ECS_CLUSTER = "fo-demo-cluster"
AURORA_ENGINE = "aurora-postgresql"
AURORA_ENGINE_VERSION = "16.4"
AURORA_INSTANCE_CLASS = "db.r5.large"  # Minimum for Aurora Global Database
AURORA_MASTER_USER = "appuser"
AURORA_MASTER_PASSWORD = "changeme"  # Demo only
AURORA_DB_NAME = "appdb"
ECS_DESIRED_COUNT = 2

REGION_SUFFIX = {
    "us-west-1": "w1",
    "us-west-2": "w2",
}

# ── ANSI Color Helpers ──────────────────────────────────────────────────────────

_COLOR_ENABLED = hasattr(sys.stdout, "isatty") and sys.stdout.isatty()


def _c(code, text):
    if _COLOR_ENABLED:
        return "\033[{}m{}\033[0m".format(code, text)
    return str(text)


def green(t):
    return _c("32", t)


def red(t):
    return _c("31", t)


def yellow(t):
    return _c("33", t)


def cyan(t):
    return _c("36", t)


def bold(t):
    return _c("1", t)


def dim(t):
    return _c("2", t)


# ── Scenario Definitions ───────────────────────────────────────────────────────

SCENARIOS = {
    "fo-v10-s1": {
        "description": "v1.0 - Active/Passive",
        "version": "v1.0",
        "routing_mode": "failover",
        "passive_publish_zero": False,
        "ai_rca_enabled": False,
        "ai_rca_provider": None,
    },
    "fo-v10-s2": {
        "description": "v1.0 - Active/Passive Zero-Container",
        "version": "v1.0",
        "routing_mode": "failover",
        "passive_publish_zero": True,
        "ai_rca_enabled": False,
        "ai_rca_provider": None,
    },
    "fo-v10-s3": {
        "description": "v1.0 - Active/Active",
        "version": "v1.0",
        "routing_mode": "active-active",
        "passive_publish_zero": False,
        "ai_rca_enabled": False,
        "ai_rca_provider": None,
    },
    "fo-v11c-s1": {
        "description": "v1.1 Claude - Active/Passive",
        "version": "v1.1",
        "routing_mode": "failover",
        "passive_publish_zero": False,
        "ai_rca_enabled": True,
        "ai_rca_provider": "claude",
    },
    "fo-v11c-s2": {
        "description": "v1.1 Claude - Active/Passive Zero-Container",
        "version": "v1.1",
        "routing_mode": "failover",
        "passive_publish_zero": True,
        "ai_rca_enabled": True,
        "ai_rca_provider": "claude",
    },
    "fo-v11c-s3": {
        "description": "v1.1 Claude - Active/Active",
        "version": "v1.1",
        "routing_mode": "active-active",
        "passive_publish_zero": False,
        "ai_rca_enabled": True,
        "ai_rca_provider": "claude",
    },
    "fo-v11g-s1": {
        "description": "v1.1 Gemini - Active/Passive",
        "version": "v1.1",
        "routing_mode": "failover",
        "passive_publish_zero": False,
        "ai_rca_enabled": True,
        "ai_rca_provider": "gemini",
    },
    "fo-v11g-s2": {
        "description": "v1.1 Gemini - Active/Passive Zero-Container",
        "version": "v1.1",
        "routing_mode": "failover",
        "passive_publish_zero": True,
        "ai_rca_enabled": True,
        "ai_rca_provider": "gemini",
    },
    "fo-v11g-s3": {
        "description": "v1.1 Gemini - Active/Active",
        "version": "v1.1",
        "routing_mode": "active-active",
        "passive_publish_zero": False,
        "ai_rca_enabled": True,
        "ai_rca_provider": "gemini",
    },
}

# ── Resource Naming ─────────────────────────────────────────────────────────────


def ecs_service_name(env):
    return "{}-app-svc".format(env)


def orchestrator_lambda_name(env):
    return "{}-orchestrator".format(env)


def failback_lambda_name(env):
    return "{}-failback".format(env)


def sns_topic_name(env):
    return "{}-alerts".format(env)


def state_table_name(env):
    return "{}-state".format(env)


def aurora_global_cluster_id(env):
    return "{}-aurora-global".format(env)


def aurora_cluster_id(env, region):
    return "{}-aurora-{}".format(env, REGION_SUFFIX[region])


def aurora_instance_id(env, region):
    return "{}-aurora-{}-inst".format(env, REGION_SUFFIX[region])


def cw_namespace(env):
    return "Custom/{}".format(env)


def cw_alarm_name(env, region):
    return "{}-region-inactive-{}".format(env, region)


def eventbridge_rule_name(env):
    # From CFN: fo-${Env}-orchestrator-schedule but env already has fo- prefix
    # The user spec says {env}-schedule-{region} but CFN says {env}-orchestrator-schedule
    # CFN is deployed, so use the CFN naming pattern
    return "{}-orchestrator-schedule".format(env)


# ── AWS Client Cache ───────────────────────────────────────────────────────────

_clients = {}


def client(service, region=PRIMARY_REGION):
    key = (service, region)
    if key not in _clients:
        _clients[key] = boto3.client(service, region_name=region)
    return _clients[key]


# ── Helper: Get DynamoDB State ──────────────────────────────────────────────────


def get_dynamo_state(env):
    """Read the failover state from DynamoDB. Returns dict or None."""
    table = state_table_name(env)
    try:
        resp = client("dynamodb", PRIMARY_REGION).get_item(
            TableName=table,
            Key={"pk": {"S": "REGION_STATE"}},
            ConsistentRead=True,
        )
        item = resp.get("Item")
        if not item:
            return None
        result = {}
        for k, v in item.items():
            if "S" in v:
                result[k] = v["S"]
            elif "N" in v:
                result[k] = v["N"]
            elif "BOOL" in v:
                result[k] = v["BOOL"]
            else:
                result[k] = str(v)
        return result
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "ResourceNotFoundException":
            return None
        raise


def dynamo_table_exists(env):
    """Check if the DynamoDB table exists."""
    try:
        client("dynamodb", PRIMARY_REGION).describe_table(
            TableName=state_table_name(env)
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            return False
        raise


# ── Helper: ECS Status ──────────────────────────────────────────────────────────


def get_ecs_status(env, region):
    """Return (running, desired) task counts, or (None, None) if service missing."""
    svc = ecs_service_name(env)
    try:
        resp = client("ecs", region).describe_services(
            cluster=ECS_CLUSTER, services=[svc]
        )
        services = resp.get("services", [])
        if not services or services[0].get("status") == "INACTIVE":
            return (None, None)
        s = services[0]
        return (s.get("runningCount", 0), s.get("desiredCount", 0))
    except ClientError:
        return (None, None)


def ecs_service_exists(env, region):
    """Check if the ECS service exists and is ACTIVE."""
    running, desired = get_ecs_status(env, region)
    return running is not None


def scale_ecs(env, region, desired):
    """Update ECS service desired count."""
    svc = ecs_service_name(env)
    print("  Scaling ECS {}/{} to {} in {}...".format(ECS_CLUSTER, svc, desired, region))
    try:
        client("ecs", region).update_service(
            cluster=ECS_CLUSTER,
            service=svc,
            desiredCount=desired,
        )
    except ClientError as e:
        print(red("  ERROR scaling ECS: {}".format(e)))
        raise


# ── Helper: EventBridge ─────────────────────────────────────────────────────────


def get_eventbridge_state(env, region):
    """Return rule state string or None if missing."""
    rule = eventbridge_rule_name(env)
    try:
        resp = client("events", region).describe_rule(Name=rule)
        return resp.get("State", "UNKNOWN")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            return None
        raise


def enable_eventbridge(env, region):
    rule = eventbridge_rule_name(env)
    print("  Enabling EventBridge rule {} in {}...".format(rule, region))
    try:
        client("events", region).enable_rule(Name=rule)
    except ClientError as e:
        print(red("  ERROR enabling rule: {}".format(e)))
        raise


def disable_eventbridge(env, region):
    rule = eventbridge_rule_name(env)
    print("  Disabling EventBridge rule {} in {}...".format(rule, region))
    try:
        client("events", region).disable_rule(Name=rule)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            print(dim("  Rule {} not found in {}, skipping.".format(rule, region)))
        else:
            raise


# ── Helper: Aurora ──────────────────────────────────────────────────────────────


def aurora_global_exists(env):
    """Check if the Aurora global cluster exists."""
    gid = aurora_global_cluster_id(env)
    try:
        resp = client("rds", PRIMARY_REGION).describe_global_clusters(
            GlobalClusterIdentifier=gid
        )
        clusters = resp.get("GlobalClusters", [])
        return len(clusters) > 0
    except ClientError as e:
        if "GlobalClusterNotFoundFault" in str(e):
            return False
        raise


def aurora_cluster_exists(env, region):
    """Check if the Aurora regional cluster exists."""
    cid = aurora_cluster_id(env, region)
    try:
        resp = client("rds", region).describe_db_clusters(DBClusterIdentifier=cid)
        clusters = resp.get("DBClusters", [])
        return len(clusters) > 0
    except ClientError as e:
        if "DBClusterNotFoundFault" in str(e):
            return False
        raise


def aurora_instance_exists(env, region):
    """Check if the Aurora instance exists."""
    iid = aurora_instance_id(env, region)
    try:
        resp = client("rds", region).describe_db_instances(DBInstanceIdentifier=iid)
        instances = resp.get("DBInstances", [])
        return len(instances) > 0
    except ClientError as e:
        if "DBInstanceNotFoundFault" in str(e) or "DBInstanceNotFound" in str(e):
            return False
        raise


def aurora_instance_status(env, region):
    """Return the instance status string or None."""
    iid = aurora_instance_id(env, region)
    try:
        resp = client("rds", region).describe_db_instances(DBInstanceIdentifier=iid)
        instances = resp.get("DBInstances", [])
        if instances:
            return instances[0].get("DBInstanceStatus", "unknown")
        return None
    except ClientError:
        return None


def get_aurora_subnet_group(env, region):
    """Find or create the Aurora DB subnet group for this scenario."""
    rds = client("rds", region)
    name = "{}-aurora-subnet".format(env)

    # Check if it already exists
    try:
        resp = rds.describe_db_subnet_groups(DBSubnetGroupName=name)
        if resp.get("DBSubnetGroups"):
            return name
    except ClientError:
        pass

    # Create it using subnets from the scenario app stack
    try:
        cfn = client("cloudformation", region)
        stack_name = "{}-app".format(env)
        resp = cfn.describe_stacks(StackName=stack_name)
        outputs = {o["OutputKey"]: o["OutputValue"] for o in resp["Stacks"][0].get("Outputs", [])}
        subnet1 = outputs.get("PrivateSubnet1Id")
        subnet2 = outputs.get("PrivateSubnet2Id")
        if subnet1 and subnet2:
            rds.create_db_subnet_group(
                DBSubnetGroupName=name,
                DBSubnetGroupDescription="Aurora subnet group for {}".format(env),
                SubnetIds=[subnet1, subnet2],
                Tags=[{"Key": "Name", "Value": name}, {"Key": "env", "Value": env}],
            )
            print("  Created DB subnet group {}.".format(name))
            return name
    except ClientError as e:
        print("  Warning: failed to create subnet group: {}".format(e))

    return None


def get_aurora_security_group(env, region):
    """Find the Aurora security group from the scenario app stack outputs."""
    try:
        cfn = client("cloudformation", region)
        stack_name = "{}-app".format(env)
        resp = cfn.describe_stacks(StackName=stack_name)
        outputs = {o["OutputKey"]: o["OutputValue"] for o in resp["Stacks"][0].get("Outputs", [])}
        sg = outputs.get("AuroraSGId")
        if sg:
            return sg
    except ClientError:
        pass
    return None


def create_aurora_global(env):
    """Create the Aurora global cluster."""
    gid = aurora_global_cluster_id(env)
    print("  Creating Aurora global cluster {}...".format(gid))
    client("rds", PRIMARY_REGION).create_global_cluster(
        GlobalClusterIdentifier=gid,
        Engine=AURORA_ENGINE,
        EngineVersion=AURORA_ENGINE_VERSION,
        DatabaseName=AURORA_DB_NAME,
        StorageEncrypted=True,
    )


def create_aurora_primary(env):
    """Create Aurora cluster + instance in primary region (writer)."""
    cid = aurora_cluster_id(env, PRIMARY_REGION)
    iid = aurora_instance_id(env, PRIMARY_REGION)
    gid = aurora_global_cluster_id(env)
    region = PRIMARY_REGION
    rds = client("rds", region)

    subnet_group = get_aurora_subnet_group(env, region)
    sg_id = get_aurora_security_group(env, region)

    if not aurora_cluster_exists(env, region):
        print("  Creating Aurora cluster {} in {}...".format(cid, region))
        kwargs = dict(
            DBClusterIdentifier=cid,
            GlobalClusterIdentifier=gid,
            Engine=AURORA_ENGINE,
            EngineVersion=AURORA_ENGINE_VERSION,
            MasterUsername=AURORA_MASTER_USER,
            MasterUserPassword=AURORA_MASTER_PASSWORD,
            DatabaseName=AURORA_DB_NAME,
            StorageEncrypted=True,
            Tags=[{"Key": "Name", "Value": cid}, {"Key": "env", "Value": env}],
        )
        if subnet_group:
            kwargs["DBSubnetGroupName"] = subnet_group
        if sg_id:
            kwargs["VpcSecurityGroupIds"] = [sg_id]
        rds.create_db_cluster(**kwargs)
    else:
        print(dim("  Aurora cluster {} already exists.".format(cid)))

    if not aurora_instance_exists(env, region):
        print("  Creating Aurora instance {} in {}...".format(iid, region))
        rds.create_db_instance(
            DBInstanceIdentifier=iid,
            DBClusterIdentifier=cid,
            DBInstanceClass=AURORA_INSTANCE_CLASS,
            Engine=AURORA_ENGINE,
            Tags=[{"Key": "Name", "Value": iid}, {"Key": "env", "Value": env}],
        )
    else:
        print(dim("  Aurora instance {} already exists.".format(iid)))


def create_aurora_secondary(env):
    """Create Aurora cluster + instance in secondary region (reader, joins global)."""
    cid = aurora_cluster_id(env, SECONDARY_REGION)
    iid = aurora_instance_id(env, SECONDARY_REGION)
    gid = aurora_global_cluster_id(env)
    region = SECONDARY_REGION
    rds = client("rds", region)

    subnet_group = get_aurora_subnet_group(env, region)
    sg_id = get_aurora_security_group(env, region)

    if not aurora_cluster_exists(env, region):
        print("  Creating Aurora cluster {} in {} (joining global)...".format(cid, region))
        # Get the default AWS managed RDS KMS key in the secondary region
        kms = client("kms", region)
        kms_resp = kms.describe_key(KeyId="alias/aws/rds")
        kms_key_id = kms_resp["KeyMetadata"]["Arn"]

        kwargs = dict(
            DBClusterIdentifier=cid,
            GlobalClusterIdentifier=gid,
            Engine=AURORA_ENGINE,
            EngineVersion=AURORA_ENGINE_VERSION,
            StorageEncrypted=True,
            KmsKeyId=kms_key_id,
            Tags=[{"Key": "Name", "Value": cid}, {"Key": "env", "Value": env}],
        )
        if subnet_group:
            kwargs["DBSubnetGroupName"] = subnet_group
        if sg_id:
            kwargs["VpcSecurityGroupIds"] = [sg_id]
        rds.create_db_cluster(**kwargs)
    else:
        print(dim("  Aurora cluster {} already exists.".format(cid)))

    if not aurora_instance_exists(env, region):
        print("  Creating Aurora instance {} in {}...".format(iid, region))
        rds.create_db_instance(
            DBInstanceIdentifier=iid,
            DBClusterIdentifier=cid,
            DBInstanceClass=AURORA_INSTANCE_CLASS,
            Engine=AURORA_ENGINE,
            Tags=[{"Key": "Name", "Value": iid}, {"Key": "env", "Value": env}],
        )
    else:
        print(dim("  Aurora instance {} already exists.".format(iid)))


def wait_aurora_instances(env, timeout_minutes=15):
    """Wait for Aurora instances in both regions to become available."""
    instances = [
        (env, PRIMARY_REGION),
        (env, SECONDARY_REGION),
    ]
    start = time.time()
    deadline = start + timeout_minutes * 60
    spinner = ["|", "/", "-", "\\"]
    tick = 0

    while time.time() < deadline:
        all_ready = True
        statuses = []
        for e, region in instances:
            status = aurora_instance_status(e, region)
            statuses.append((aurora_instance_id(e, region), region, status))
            if status != "available":
                all_ready = False

        elapsed = int(time.time() - start)
        status_str = "  {} Waiting for Aurora ({:d}s) — ".format(
            spinner[tick % len(spinner)], elapsed
        )
        parts = []
        for iid, region, st in statuses:
            color_fn = green if st == "available" else yellow
            parts.append("{}: {}".format(iid, color_fn(st or "pending")))
        sys.stdout.write("\r" + status_str + ", ".join(parts) + "   ")
        sys.stdout.flush()

        if all_ready:
            sys.stdout.write("\n")
            print(green("  All Aurora instances are available."))
            return True

        time.sleep(15)
        tick += 1

    sys.stdout.write("\n")
    print(red("  TIMEOUT waiting for Aurora after {} minutes.".format(timeout_minutes)))
    return False


def delete_aurora_instance(env, region):
    """Delete Aurora instance (skip final snapshot)."""
    iid = aurora_instance_id(env, region)
    if not aurora_instance_exists(env, region):
        print(dim("  Aurora instance {} not found, skipping.".format(iid)))
        return
    print("  Deleting Aurora instance {} in {}...".format(iid, region))
    try:
        client("rds", region).delete_db_instance(
            DBInstanceIdentifier=iid,
            SkipFinalSnapshot=True,
        )
    except ClientError as e:
        if "is already being deleted" in str(e):
            print(dim("  Instance already being deleted, will wait for completion."))
        else:
            raise


def wait_aurora_instance_deleted(env, region, timeout_minutes=15):
    """Wait for Aurora instance to be fully deleted."""
    iid = aurora_instance_id(env, region)
    start = time.time()
    deadline = start + timeout_minutes * 60
    spinner = ["|", "/", "-", "\\"]
    tick = 0

    while time.time() < deadline:
        status = aurora_instance_status(env, region)
        if status is None:
            sys.stdout.write("\n")
            print(green("  Aurora instance {} deleted.".format(iid)))
            return True
        elapsed = int(time.time() - start)
        sys.stdout.write(
            "\r  {} Waiting for {} deletion ({:d}s) — status: {}   ".format(
                spinner[tick % len(spinner)], iid, elapsed, yellow(status)
            )
        )
        sys.stdout.flush()
        time.sleep(15)
        tick += 1

    sys.stdout.write("\n")
    print(red("  TIMEOUT waiting for instance deletion."))
    return False


def delete_aurora_cluster(env, region):
    """Delete Aurora regional cluster (no final snapshot)."""
    cid = aurora_cluster_id(env, region)
    if not aurora_cluster_exists(env, region):
        print(dim("  Aurora cluster {} not found, skipping.".format(cid)))
        return
    print("  Deleting Aurora cluster {} in {}...".format(cid, region))
    client("rds", region).delete_db_cluster(
        DBClusterIdentifier=cid,
        SkipFinalSnapshot=True,
    )


def wait_aurora_cluster_deleted(env, region, timeout_minutes=10):
    """Wait for Aurora cluster to be fully deleted."""
    cid = aurora_cluster_id(env, region)
    start = time.time()
    deadline = start + timeout_minutes * 60
    spinner = ["|", "/", "-", "\\"]
    tick = 0

    while time.time() < deadline:
        if not aurora_cluster_exists(env, region):
            sys.stdout.write("\n")
            print(green("  Aurora cluster {} deleted.".format(cid)))
            return True
        elapsed = int(time.time() - start)
        sys.stdout.write(
            "\r  {} Waiting for {} deletion ({:d}s)   ".format(
                spinner[tick % len(spinner)], cid, elapsed
            )
        )
        sys.stdout.flush()
        time.sleep(15)
        tick += 1

    sys.stdout.write("\n")
    print(red("  TIMEOUT waiting for cluster deletion."))
    return False


def remove_from_global_cluster(env, region):
    """Remove a regional cluster from the global cluster and wait for detach."""
    cid = aurora_cluster_id(env, region)
    cluster_arn = "arn:aws:rds:{}:{}:cluster:{}".format(region, ACCOUNT_ID, cid)
    gid = aurora_global_cluster_id(env)
    print("  Removing {} from global cluster {}...".format(cid, gid))
    try:
        client("rds", region).remove_from_global_cluster(
            GlobalClusterIdentifier=gid,
            DbClusterIdentifier=cluster_arn,
        )
    except ClientError as e:
        if "is not a member" in str(e) or "is not found" in str(e):
            print(dim("  Cluster not a member of global, skipping."))
            return
        else:
            raise
    # Wait for the cluster to fully detach (status transitions from
    # "removing-from-global-cluster" back to "available" as standalone)
    wait_cluster_detached_from_global(env, region)


def wait_cluster_detached_from_global(env, region, timeout_minutes=10):
    """Wait for a cluster to finish detaching from its global cluster."""
    cid = aurora_cluster_id(env, region)
    start = time.time()
    deadline = start + timeout_minutes * 60
    spinner = ["|", "/", "-", "\\"]
    tick = 0

    while time.time() < deadline:
        try:
            resp = client("rds", region).describe_db_clusters(DBClusterIdentifier=cid)
            clusters = resp.get("DBClusters", [])
            if not clusters:
                print(green("  Cluster {} gone.".format(cid)))
                return True
            status = clusters[0].get("Status", "unknown")
            # Once status is "available" (not "removing-from-global-cluster"), it's detached
            if status == "available":
                elapsed = int(time.time() - start)
                print(green("  Cluster {} detached from global ({:d}s).".format(cid, elapsed)))
                return True
            elapsed = int(time.time() - start)
            sys.stdout.write(
                "\r  {} Waiting for {} detach ({:d}s) — status: {}   ".format(
                    spinner[tick % len(spinner)], cid, elapsed, yellow(status)
                )
            )
            sys.stdout.flush()
        except ClientError:
            print(green("  Cluster {} gone.".format(cid)))
            return True
        time.sleep(10)
        tick += 1

    sys.stdout.write("\n")
    print(red("  TIMEOUT waiting for cluster detach."))
    return False


def delete_aurora_global(env):
    """Delete the global cluster (must be empty first)."""
    gid = aurora_global_cluster_id(env)
    if not aurora_global_exists(env):
        print(dim("  Global cluster {} not found, skipping.".format(gid)))
        return
    print("  Deleting Aurora global cluster {}...".format(gid))
    try:
        client("rds", PRIMARY_REGION).delete_global_cluster(
            GlobalClusterIdentifier=gid
        )
    except ClientError as e:
        print(red("  ERROR deleting global cluster: {}".format(e)))
        raise


# ── Helper: DynamoDB State Management ──────────────────────────────────────────


def reset_dynamo_state(env, state="PRIMARY_ACTIVE", active_region=None):
    """Reset DynamoDB state to the given state."""
    if active_region is None:
        active_region = PRIMARY_REGION
    table = state_table_name(env)
    now = datetime.now(timezone.utc).isoformat()
    print("  Resetting DynamoDB state to {}...".format(state))
    try:
        client("dynamodb", PRIMARY_REGION).put_item(
            TableName=table,
            Item={
                "pk": {"S": "REGION_STATE"},
                "active_region": {"S": active_region},
                "state": {"S": state},
                "latch_engaged": {"BOOL": False},
                "consecutive_failures": {"N": "0"},
                "last_failover_ts": {"S": "1970-01-01T00:00:00Z"},
                "last_updated": {"S": now},
                "cooldown_reset": {"BOOL": False},
                "last_warning_notification_ts": {"S": "1970-01-01T00:00:00Z"},
            },
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            print(yellow("  DynamoDB table {} does not exist, skipping state reset.".format(table)))
        else:
            raise


def reset_cooldown_in_dynamo(env):
    """Reset cooldown and failure counters to allow immediate failover."""
    table = state_table_name(env)
    print("  Resetting cooldown and failure counters...")
    try:
        client("dynamodb", PRIMARY_REGION).update_item(
            TableName=table,
            Key={"pk": {"S": "REGION_STATE"}},
            UpdateExpression=(
                "SET consecutive_failures = :zero, "
                "last_failover_ts = :epoch, "
                "cooldown_reset = :t, "
                "last_warning_notification_ts = :epoch"
            ),
            ExpressionAttributeValues={
                ":zero": {"N": "0"},
                ":epoch": {"S": "1970-01-01T00:00:00Z"},
                ":t": {"BOOL": True},
            },
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            print(yellow("  DynamoDB table not found, skipping."))
        else:
            raise


# ── Command: status ─────────────────────────────────────────────────────────────


def cmd_status(args):
    """Show status table for all 9 scenarios."""
    print()
    print(bold("Failover Orchestrator Demo Environments"))
    print(bold("=" * 95))
    print()

    # Header
    header = "{:<14s} {:<42s} {:<6s} {:<10s} {:<14s} {:<12s}".format(
        "ENV", "DESCRIPTION", "VER", "STATE", "ACTIVE REGION", "ECS (pri)"
    )
    print(bold(header))
    print("-" * 95)

    for env, scen in SCENARIOS.items():
        desc = scen["description"]
        ver = scen["version"]

        # Determine state
        ecs_running, ecs_desired = get_ecs_status(env, PRIMARY_REGION)
        eb_state = get_eventbridge_state(env, PRIMARY_REGION)

        if ecs_running is None and eb_state is None:
            # Check if any resources exist at all
            if not ecs_service_exists(env, PRIMARY_REGION):
                state_str = dim("UNDEPLOYED")
                active_region = dim("N/A")
                ecs_str = dim("N/A")
            else:
                state_str = dim("UNKNOWN")
                active_region = dim("N/A")
                ecs_str = dim("N/A")
        else:
            # Read DynamoDB state
            ddb = get_dynamo_state(env)
            if ddb:
                active_region = ddb.get("active_region", "N/A")
                ddb_state = ddb.get("state", "UNKNOWN")
            else:
                active_region = dim("N/A")
                ddb_state = None

            if ecs_running is not None and ecs_desired is not None:
                ecs_str = "{}/{}".format(ecs_running, ecs_desired)
            else:
                ecs_str = dim("N/A")

            # Determine high-level state
            if ecs_desired == 0 and (eb_state == "DISABLED" or eb_state is None):
                state_str = yellow("PARKED")
                active_region = dim("N/A")
            elif ddb_state in ("WAITING_AURORA_PROMOTION", "FAILBACK_IN_PROGRESS"):
                state_str = red("TRIGGERED")
            elif ddb_state == "SECONDARY_ACTIVE":
                state_str = red("TRIGGERED")
            elif ecs_running is not None and ecs_running > 0:
                state_str = green("ACTIVE")
            elif ecs_desired is not None and ecs_desired > 0 and (ecs_running == 0):
                state_str = yellow("STARTING")
            else:
                state_str = yellow("PARKED")
                active_region = dim("N/A")

        row = "{:<14s} {:<42s} {:<6s} {:<10s} {:<14s} {:<12s}".format(
            cyan(env), desc, ver, state_str, str(active_region), ecs_str
        )
        print(row)

    print()


# ── Command: activate ──────────────────────────────────────────────────────────


def cmd_activate(args):
    """Activate a demo scenario."""
    env = args.env
    if env not in SCENARIOS:
        print(red("Unknown scenario: {}".format(env)))
        sys.exit(1)

    scen = SCENARIOS[env]
    print()
    print(bold("Activating: {} ({})".format(env, scen["description"])))
    print("=" * 60)

    is_active_active = scen["routing_mode"] == "active-active"
    is_zero_container = scen["passive_publish_zero"]

    # Step 1: Aurora global cluster
    print()
    print(bold("[1/7] Aurora Global Cluster"))
    if not aurora_global_exists(env):
        create_aurora_global(env)
    else:
        print(dim("  Global cluster already exists."))

    # Step 2: Aurora primary cluster + instance
    print()
    print(bold("[2/7] Aurora Primary Cluster ({})" .format(PRIMARY_REGION)))
    create_aurora_primary(env)

    # Step 3: Wait for primary to be available before creating secondary
    print()
    print(bold("[3/7] Waiting for Primary Aurora to be Available"))
    primary_status = aurora_instance_status(env, PRIMARY_REGION)
    if primary_status != "available":
        start = time.time()
        deadline = start + 900  # 15 min
        spinner = ["|", "/", "-", "\\"]
        tick = 0
        while time.time() < deadline:
            status = aurora_instance_status(env, PRIMARY_REGION)
            if status == "available":
                sys.stdout.write("\n")
                print(green("  Primary Aurora instance available."))
                break
            elapsed = int(time.time() - start)
            sys.stdout.write(
                "\r  {} Waiting for primary ({:d}s) — {}   ".format(
                    spinner[tick % len(spinner)], elapsed, yellow(status or "creating")
                )
            )
            sys.stdout.flush()
            time.sleep(15)
            tick += 1
        else:
            sys.stdout.write("\n")
            print(red("  TIMEOUT waiting for primary Aurora."))
            print(red("  Manual cleanup may be needed for: {}".format(env)))
            sys.exit(1)
    else:
        print(green("  Primary Aurora already available."))

    # Step 4: Aurora secondary cluster + instance
    print()
    print(bold("[4/7] Aurora Secondary Cluster ({})".format(SECONDARY_REGION)))
    create_aurora_secondary(env)

    # Step 5: Wait for all Aurora instances
    print()
    print(bold("[5/7] Waiting for All Aurora Instances"))
    if not wait_aurora_instances(env):
        print(red("  Aurora setup incomplete. Manual cleanup may be needed."))
        print(red("  Resources to check:"))
        print(red("    - Global: {}".format(aurora_global_cluster_id(env))))
        print(red("    - Primary: {}".format(aurora_cluster_id(env, PRIMARY_REGION))))
        print(red("    - Secondary: {}".format(aurora_cluster_id(env, SECONDARY_REGION))))
        sys.exit(1)

    # Step 6: Scale ECS
    print()
    print(bold("[6/7] Scaling ECS Services"))
    if ecs_service_exists(env, PRIMARY_REGION):
        scale_ecs(env, PRIMARY_REGION, ECS_DESIRED_COUNT)
    else:
        print(yellow("  ECS service not found in {}. Deploy CFN stack first.".format(PRIMARY_REGION)))

    if is_active_active or not is_zero_container:
        if ecs_service_exists(env, SECONDARY_REGION):
            scale_ecs(env, SECONDARY_REGION, ECS_DESIRED_COUNT)
        else:
            print(yellow("  ECS service not found in {}. Deploy CFN stack first.".format(SECONDARY_REGION)))
    else:
        # Zero-container: keep secondary at 0
        if ecs_service_exists(env, SECONDARY_REGION):
            scale_ecs(env, SECONDARY_REGION, 0)
            print(dim("  (Zero-container mode: secondary stays at 0)"))

    # Step 7: EventBridge + state
    print()
    print(bold("[7/7] EventBridge Rules & State"))
    for region in [PRIMARY_REGION, SECONDARY_REGION]:
        eb = get_eventbridge_state(env, region)
        if eb is not None:
            enable_eventbridge(env, region)
        else:
            print(yellow("  EventBridge rule not found in {}. Deploy CFN stack first.".format(region)))

    reset_dynamo_state(env)

    print()
    print(green("Scenario {} activated successfully.".format(bold(env))))
    print("  Aurora provisioning took ~10 min. ECS tasks should be running in ~2 min.")
    print()


# ── Command: deactivate ────────────────────────────────────────────────────────


def cmd_deactivate(args):
    """Deactivate (park) a demo scenario."""
    env = args.env
    if env not in SCENARIOS:
        print(red("Unknown scenario: {}".format(env)))
        sys.exit(1)

    scen = SCENARIOS[env]
    print()
    print(bold("Deactivating: {} ({})".format(env, scen["description"])))
    print("=" * 60)

    # Step 1: Disable EventBridge
    print()
    print(bold("[1/5] Disabling EventBridge Rules"))
    for region in [PRIMARY_REGION, SECONDARY_REGION]:
        disable_eventbridge(env, region)

    # Step 2: Scale ECS to 0
    print()
    print(bold("[2/5] Scaling ECS to 0"))
    for region in [PRIMARY_REGION, SECONDARY_REGION]:
        if ecs_service_exists(env, region):
            scale_ecs(env, region, 0)
        else:
            print(dim("  ECS service not found in {}, skipping.".format(region)))

    # Step 3: Remove secondary from global cluster and tear down secondary Aurora
    # AWS requires the replica cluster to be fully removed before the master's
    # last instance can be deleted, so we must handle secondary first end-to-end.
    print()
    print(bold("[3/5] Removing Secondary Aurora (replica must go before master)"))
    if aurora_instance_exists(env, SECONDARY_REGION):
        delete_aurora_instance(env, SECONDARY_REGION)
        wait_aurora_instance_deleted(env, SECONDARY_REGION)
    if aurora_cluster_exists(env, SECONDARY_REGION):
        remove_from_global_cluster(env, SECONDARY_REGION)
        delete_aurora_cluster(env, SECONDARY_REGION)
        wait_aurora_cluster_deleted(env, SECONDARY_REGION)

    # Step 4: Tear down primary Aurora
    print()
    print(bold("[4/5] Removing Primary Aurora"))
    if aurora_instance_exists(env, PRIMARY_REGION):
        delete_aurora_instance(env, PRIMARY_REGION)
        wait_aurora_instance_deleted(env, PRIMARY_REGION)
    if aurora_cluster_exists(env, PRIMARY_REGION):
        remove_from_global_cluster(env, PRIMARY_REGION)
        delete_aurora_cluster(env, PRIMARY_REGION)
        wait_aurora_cluster_deleted(env, PRIMARY_REGION)

    # Step 5: Delete global cluster
    print()
    print(bold("[5/5] Deleting Aurora Global Cluster"))
    delete_aurora_global(env)

    print()
    print(green("Scenario {} deactivated (parked).".format(bold(env))))
    print()


# ── Command: trigger ────────────────────────────────────────────────────────────


def cmd_trigger(args):
    """Inject a failure to trigger failover."""
    env = args.env
    if env not in SCENARIOS:
        print(red("Unknown scenario: {}".format(env)))
        sys.exit(1)

    scen = SCENARIOS[env]
    print()
    print(bold("Triggering failover for: {} ({})".format(env, scen["description"])))
    print("=" * 60)

    # Verify scenario is active
    running, desired = get_ecs_status(env, PRIMARY_REGION)
    if running is None or running == 0:
        print(red("  Scenario is not active (ECS running={}).".format(running)))
        print(red("  Activate it first: python3 tools/demo_manager.py activate {}".format(env)))
        sys.exit(1)

    # Reset cooldown so failover fires immediately
    print()
    reset_cooldown_in_dynamo(env)

    # Scale ECS to 0 in primary
    print()
    print(bold("Injecting failure: scaling ECS to 0 in {}".format(PRIMARY_REGION)))
    scale_ecs(env, PRIMARY_REGION, 0)

    print()
    print(green("Failure injected for {}.".format(bold(env))))
    print("  Failover will trigger in ~3 minutes (consecutive failure threshold).")
    print()

    if args.watch:
        print(bold("Tailing orchestrator logs (Ctrl+C to stop)..."))
        print()
        _tail_lambda_logs(env)


def _tail_lambda_logs(env):
    """Tail CloudWatch logs for the orchestrator Lambda."""
    log_group = "/aws/lambda/{}".format(orchestrator_lambda_name(env))
    logs = client("logs", PRIMARY_REGION)

    # Start from now
    start_time = int(time.time() * 1000) - 60000  # 1 min ago
    seen_events = set()

    try:
        while True:
            try:
                resp = logs.filter_log_events(
                    logGroupName=log_group,
                    startTime=start_time,
                    interleaved=True,
                    limit=50,
                )
                for event in resp.get("events", []):
                    eid = event["eventId"]
                    if eid not in seen_events:
                        seen_events.add(eid)
                        ts = datetime.fromtimestamp(
                            event["timestamp"] / 1000, tz=timezone.utc
                        ).strftime("%H:%M:%S")
                        msg = event["message"].rstrip()
                        print("{} {}".format(dim(ts), msg))
                        start_time = event["timestamp"] + 1
            except ClientError as e:
                if "ResourceNotFoundException" in str(e):
                    print(dim("  Log group not found yet, waiting..."))
                else:
                    print(yellow("  Log error: {}".format(e)))
            time.sleep(5)
    except KeyboardInterrupt:
        print()
        print(dim("Stopped tailing logs."))


# ── Command: reset ──────────────────────────────────────────────────────────────


def cmd_reset(args):
    """Reset a scenario back to PRIMARY_ACTIVE after a triggered failover."""
    env = args.env
    if env not in SCENARIOS:
        print(red("Unknown scenario: {}".format(env)))
        sys.exit(1)

    scen = SCENARIOS[env]
    print()
    print(bold("Resetting: {} ({})".format(env, scen["description"])))
    print("=" * 60)

    # Step 1: Scale ECS back up in primary
    print()
    print(bold("[1/4] Scaling ECS in primary region"))
    if ecs_service_exists(env, PRIMARY_REGION):
        scale_ecs(env, PRIMARY_REGION, ECS_DESIRED_COUNT)
    else:
        print(red("  ECS service not found in primary. Cannot reset."))
        sys.exit(1)

    # Step 2: Wait for ECS tasks to be running
    print()
    print(bold("[2/4] Waiting for ECS tasks"))
    start = time.time()
    deadline = start + 300  # 5 min
    spinner = ["|", "/", "-", "\\"]
    tick = 0
    while time.time() < deadline:
        running, desired = get_ecs_status(env, PRIMARY_REGION)
        if running is not None and running >= ECS_DESIRED_COUNT:
            sys.stdout.write("\n")
            print(green("  ECS has {}/{} tasks running.".format(running, desired)))
            break
        elapsed = int(time.time() - start)
        sys.stdout.write(
            "\r  {} Waiting for ECS ({:d}s) — running: {}   ".format(
                spinner[tick % len(spinner)], elapsed, running
            )
        )
        sys.stdout.flush()
        time.sleep(10)
        tick += 1
    else:
        sys.stdout.write("\n")
        print(yellow("  TIMEOUT waiting for ECS. Proceeding with reset anyway."))

    # Step 3: Invoke failback Lambda
    print()
    print(bold("[3/4] Invoking failback Lambda"))
    fb_name = failback_lambda_name(env)
    try:
        resp = client("lambda", PRIMARY_REGION).invoke(
            FunctionName=fb_name,
            InvocationType="RequestResponse",
            Payload=json.dumps({
                "aurora_confirmed": True,
                "skip_health_check": True,
            }).encode(),
        )
        status_code = resp.get("StatusCode", 0)
        payload = resp.get("Payload")
        if payload:
            body = json.loads(payload.read().decode())
            print("  Failback response (HTTP {}): {}".format(status_code, json.dumps(body, indent=2)))
        else:
            print("  Failback invoked (HTTP {})".format(status_code))
    except ClientError as e:
        print(yellow("  Could not invoke failback Lambda: {}".format(e)))
        print(yellow("  Proceeding with DynamoDB reset."))

    # Step 4: Reset DynamoDB state
    print()
    print(bold("[4/4] Resetting DynamoDB state"))
    reset_dynamo_state(env)

    print()
    print(green("Scenario {} reset to PRIMARY_ACTIVE.".format(bold(env))))
    print()


# ── Command: activate-all / deactivate-all ──────────────────────────────────────


def cmd_activate_all(args):
    """Activate all 9 scenarios."""
    print()
    print(bold("Activating ALL 9 demo scenarios"))
    print(bold("=" * 60))
    print()
    print(yellow("This will create 9 Aurora global clusters and scale up 9 ECS services."))
    print(yellow("Estimated time: ~15 minutes (Aurora creation is the bottleneck)."))
    print()

    confirm = input("Continue? [y/N] ").strip().lower()
    if confirm != "y":
        print("Aborted.")
        return

    errors = {}
    for env in SCENARIOS:
        try:
            # Create a namespace for the args
            ns = argparse.Namespace(env=env)
            cmd_activate(ns)
        except Exception as e:
            print(red("ERROR activating {}: {}".format(env, e)))
            errors[env] = str(e)

    print()
    if errors:
        print(red("The following scenarios had errors:"))
        for env, err in errors.items():
            print(red("  {}: {}".format(env, err)))
    else:
        print(green("All 9 scenarios activated successfully."))
    print()


def cmd_deactivate_all(args):
    """Deactivate all 9 scenarios."""
    print()
    print(bold("Deactivating ALL 9 demo scenarios"))
    print(bold("=" * 60))
    print()
    print(yellow("This will delete all Aurora clusters and scale ECS to 0."))
    print()

    confirm = input("Continue? [y/N] ").strip().lower()
    if confirm != "y":
        print("Aborted.")
        return

    errors = {}
    for env in SCENARIOS:
        try:
            ns = argparse.Namespace(env=env)
            cmd_deactivate(ns)
        except Exception as e:
            print(red("ERROR deactivating {}: {}".format(env, e)))
            errors[env] = str(e)

    print()
    if errors:
        print(red("The following scenarios had errors:"))
        for env, err in errors.items():
            print(red("  {}: {}".format(env, err)))
    else:
        print(green("All 9 scenarios deactivated (parked)."))
    print()


# ── CLI Entry Point ─────────────────────────────────────────────────────────────


def build_parser():
    parser = argparse.ArgumentParser(
        description="Failover Orchestrator Demo Manager — manage 9 demo environments",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Scenarios:
  fo-v10-s1    v1.0 - Active/Passive
  fo-v10-s2    v1.0 - Active/Passive Zero-Container
  fo-v10-s3    v1.0 - Active/Active
  fo-v11c-s1   v1.1 Claude - Active/Passive
  fo-v11c-s2   v1.1 Claude - Active/Passive Zero-Container
  fo-v11c-s3   v1.1 Claude - Active/Active
  fo-v11g-s1   v1.1 Gemini - Active/Passive
  fo-v11g-s2   v1.1 Gemini - Active/Passive Zero-Container
  fo-v11g-s3   v1.1 Gemini - Active/Active

Examples:
  %(prog)s status
  %(prog)s activate fo-v10-s1
  %(prog)s trigger fo-v10-s1 --watch
  %(prog)s reset fo-v10-s1
  %(prog)s deactivate fo-v10-s1
  %(prog)s activate-all
  %(prog)s deactivate-all
""",
    )
    sub = parser.add_subparsers(dest="command", help="Command to run")
    sub.required = True

    # status
    sub.add_parser("status", help="Show status of all 9 demo environments")

    # activate
    p_act = sub.add_parser("activate", help="Activate a demo scenario")
    p_act.add_argument("env", choices=list(SCENARIOS.keys()), help="Scenario env name")

    # deactivate
    p_deact = sub.add_parser("deactivate", help="Deactivate (park) a demo scenario")
    p_deact.add_argument("env", choices=list(SCENARIOS.keys()), help="Scenario env name")

    # trigger
    p_trig = sub.add_parser("trigger", help="Inject failure to trigger failover")
    p_trig.add_argument("env", choices=list(SCENARIOS.keys()), help="Scenario env name")
    p_trig.add_argument("--watch", action="store_true", help="Tail orchestrator logs after triggering")

    # reset
    p_reset = sub.add_parser("reset", help="Reset scenario to PRIMARY_ACTIVE")
    p_reset.add_argument("env", choices=list(SCENARIOS.keys()), help="Scenario env name")

    # activate-all
    sub.add_parser("activate-all", help="Activate all 9 scenarios")

    # deactivate-all
    sub.add_parser("deactivate-all", help="Deactivate all 9 scenarios")

    return parser


COMMAND_MAP = {
    "status": cmd_status,
    "activate": cmd_activate,
    "deactivate": cmd_deactivate,
    "trigger": cmd_trigger,
    "reset": cmd_reset,
    "activate-all": cmd_activate_all,
    "deactivate-all": cmd_deactivate_all,
}


def main():
    parser = build_parser()
    args = parser.parse_args()

    handler = COMMAND_MAP.get(args.command)
    if handler is None:
        parser.print_help()
        sys.exit(1)

    try:
        handler(args)
    except KeyboardInterrupt:
        print()
        print(dim("Interrupted."))
        sys.exit(130)
    except ClientError as e:
        print()
        print(red("AWS Error: {}".format(e)))
        sys.exit(1)
    except Exception as e:
        print()
        print(red("Error: {}".format(e)))
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
