#!/usr/bin/env python3
"""
Deploy Scenarios — deploys CloudFormation stacks for all 9 failover demo scenarios.

Each scenario gets its own app stack (ALB, ECS, SGs) and failover stack (Lambda,
EventBridge, SNS, CloudWatch alarms, Route 53 health checks) in both regions.

The network stack (fo-demo-network) must already be deployed in both regions.

Usage:
    python3 tools/deploy_scenarios.py deploy fo-v10-s1
    python3 tools/deploy_scenarios.py deploy fo-v10-s1 --region us-west-1
    python3 tools/deploy_scenarios.py deploy-all
    python3 tools/deploy_scenarios.py teardown fo-v10-s1
    python3 tools/deploy_scenarios.py teardown-all
    python3 tools/deploy_scenarios.py list
    python3 tools/deploy_scenarios.py status fo-v10-s1
"""

import argparse
import json
import os
import sys
import time
import tempfile
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

try:
    import boto3
    from botocore.exceptions import ClientError, WaiterError
except ImportError:
    print("ERROR: boto3 is required. Install with: pip install boto3")
    sys.exit(1)

# ── Constants ────────────────────────────────────────────────────────────────────

PRIMARY_REGION = "us-west-1"
SECONDARY_REGION = "us-west-2"
BOTH_REGIONS = [PRIMARY_REGION, SECONDARY_REGION]
NETWORK_STACK = "fo-demo-network"
SHARED_APP_STACK = "fo-demo-app"
NOTIFICATION_EMAIL = "ranohep@gmail.com"

# Paths relative to this script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
CFN_DIR = os.path.join(PROJECT_ROOT, "cfn")
APP_TEMPLATE = os.path.join(CFN_DIR, "scenario-app.yaml")
FAILOVER_TEMPLATE = os.path.join(CFN_DIR, "failover.yaml")

# Lambda source files
ORCHESTRATOR_SRC = os.path.join(PROJECT_ROOT, "failover_orchestrator_v3.py")
FAILBACK_SRC = os.path.join(PROJECT_ROOT, "manual_failback_v2.py")
STATE_BACKEND_SRC = os.path.join(PROJECT_ROOT, "state_backend.py")
AI_DIR = os.path.join(PROJECT_ROOT, "ai")

REGION_SUFFIX = {
    "us-west-1": "w1",
    "us-west-2": "w2",
}

# ── Scenario Definitions (mirrored from demo_manager.py) ─────────────────────

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

# ── ANSI Color Helpers ────────────────────────────────────────────────────────

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


# ── AWS Client Cache ─────────────────────────────────────────────────────────

_clients = {}


def client(service, region=PRIMARY_REGION):
    key = (service, region)
    if key not in _clients:
        _clients[key] = boto3.client(service, region_name=region)
    return _clients[key]


# ── Resource Naming (consistent with demo_manager.py) ─────────────────────────


def app_stack_name(env):
    return "{}-app".format(env)


def failover_stack_name(env):
    return "{}-failover".format(env)


def state_table_name(env):
    return "{}-state".format(env)


def aurora_global_cluster_id(env):
    return "{}-aurora-global".format(env)


def aurora_cluster_id(env, region):
    return "{}-aurora-{}".format(env, REGION_SUFFIX[region])


def orchestrator_lambda_name(env):
    return "fo-{}-orchestrator".format(env)


def failback_lambda_name(env):
    return "fo-{}-failback".format(env)


def eventbridge_rule_name(env):
    return "fo-{}-orchestrator-schedule".format(env)


# ── Template Reading ──────────────────────────────────────────────────────────


def read_template(path):
    """Read a CloudFormation template file and return its contents."""
    with open(path, "r") as f:
        return f.read()


# ── Lambda Zip Building ──────────────────────────────────────────────────────


def build_orchestrator_zip(version):
    """Build the orchestrator Lambda deployment zip.

    For v1.0, includes only the orchestrator and state backend.
    For v1.1, also includes the ai/ module.
    """
    tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
    tmp.close()

    with zipfile.ZipFile(tmp.name, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(ORCHESTRATOR_SRC, "failover_orchestrator_v3.py")
        zf.write(STATE_BACKEND_SRC, "state_backend.py")

        if version == "v1.1" and os.path.isdir(AI_DIR):
            for root, _dirs, files in os.walk(AI_DIR):
                for fname in files:
                    if fname.endswith(".py"):
                        full_path = os.path.join(root, fname)
                        arc_name = os.path.relpath(full_path, PROJECT_ROOT)
                        zf.write(full_path, arc_name)

    return tmp.name


def build_failback_zip():
    """Build the failback Lambda deployment zip."""
    tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
    tmp.close()

    with zipfile.ZipFile(tmp.name, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(FAILBACK_SRC, "manual_failback_v2.py")
        zf.write(STATE_BACKEND_SRC, "state_backend.py")

    return tmp.name


# ── CloudFormation Helpers ────────────────────────────────────────────────────


def stack_exists(stack_name, region):
    """Check if a CFN stack exists and is not in a deleted state."""
    cfn = client("cloudformation", region)
    try:
        resp = cfn.describe_stacks(StackName=stack_name)
        stacks = resp.get("Stacks", [])
        if not stacks:
            return False
        status = stacks[0].get("StackStatus", "")
        # Consider DELETE_COMPLETE as non-existent
        if status == "DELETE_COMPLETE":
            return False
        return True
    except ClientError as e:
        msg = str(e)
        if "does not exist" in msg:
            return False
        raise


def get_stack_status(stack_name, region):
    """Get the current status of a CFN stack."""
    cfn = client("cloudformation", region)
    try:
        resp = cfn.describe_stacks(StackName=stack_name)
        stacks = resp.get("Stacks", [])
        if not stacks:
            return None
        return stacks[0].get("StackStatus")
    except ClientError as e:
        if "does not exist" in str(e):
            return None
        raise


def get_stack_outputs(stack_name, region):
    """Get the outputs of a CFN stack as a dict."""
    cfn = client("cloudformation", region)
    try:
        resp = cfn.describe_stacks(StackName=stack_name)
        stacks = resp.get("Stacks", [])
        if not stacks:
            return {}
        outputs = stacks[0].get("Outputs", [])
        return {o["OutputKey"]: o["OutputValue"] for o in outputs}
    except ClientError as e:
        if "does not exist" in str(e):
            return {}
        raise


def deploy_stack(stack_name, template_body, parameters, region, capabilities=None):
    """Create or update a CloudFormation stack and wait for completion.

    Returns True on success, False on no-op (no update needed).
    Raises on failure.
    """
    cfn = client("cloudformation", region)
    caps = capabilities or ["CAPABILITY_NAMED_IAM"]

    params_list = [
        {"ParameterKey": k, "ParameterValue": v}
        for k, v in parameters.items()
    ]

    exists = stack_exists(stack_name, region)

    if exists:
        # Check if stack is in a failed state that needs cleanup
        status = get_stack_status(stack_name, region)
        if status in (
            "ROLLBACK_COMPLETE",
            "ROLLBACK_FAILED",
            "CREATE_FAILED",
            "DELETE_FAILED",
        ):
            print("    Stack {} is in {} state — deleting first...".format(
                stack_name, status))
            delete_stack(stack_name, region)
            exists = False

    if exists:
        # Check for in-progress operations
        status = get_stack_status(stack_name, region)
        if status and "IN_PROGRESS" in status:
            print("    Stack {} is {} — waiting for completion...".format(
                stack_name, status))
            _wait_for_stack(stack_name, region, status)

        # Update
        print("    Updating stack {} in {}...".format(stack_name, region))
        try:
            cfn.update_stack(
                StackName=stack_name,
                TemplateBody=template_body,
                Parameters=params_list,
                Capabilities=caps,
                Tags=[
                    {"Key": "Project", "Value": "fo-demo"},
                    {"Key": "ManagedBy", "Value": "deploy_scenarios.py"},
                ],
            )
        except ClientError as e:
            if "No updates are to be performed" in str(e):
                print("    No changes needed for {} in {}.".format(
                    stack_name, region))
                return False
            raise

        _wait_for_stack(stack_name, region, "UPDATE_IN_PROGRESS")
        print(green("    Stack {} updated in {}.".format(stack_name, region)))
        return True
    else:
        # Create
        print("    Creating stack {} in {}...".format(stack_name, region))
        cfn.create_stack(
            StackName=stack_name,
            TemplateBody=template_body,
            Parameters=params_list,
            Capabilities=caps,
            Tags=[
                {"Key": "Project", "Value": "fo-demo"},
                {"Key": "ManagedBy", "Value": "deploy_scenarios.py"},
            ],
            OnFailure="DELETE",
        )

        _wait_for_stack(stack_name, region, "CREATE_IN_PROGRESS")
        print(green("    Stack {} created in {}.".format(stack_name, region)))
        return True


def _wait_for_stack(stack_name, region, current_status):
    """Wait for a stack operation to complete. Polls every 15 seconds."""
    cfn = client("cloudformation", region)
    spinner = ["|", "/", "-", "\\"]
    tick = 0
    start = time.time()
    timeout = 1200  # 20 minutes

    while time.time() - start < timeout:
        time.sleep(15)
        status = get_stack_status(stack_name, region)
        elapsed = int(time.time() - start)

        if status is None:
            # Stack was deleted (CREATE failed with OnFailure=DELETE)
            raise RuntimeError(
                "Stack {} was deleted during creation (likely a template error). "
                "Check CloudFormation events in {} for details.".format(
                    stack_name, region))

        sys.stdout.write(
            "\r    {} {} in {} ({:d}s) — {}   ".format(
                spinner[tick % len(spinner)], stack_name, region, elapsed, status
            )
        )
        sys.stdout.flush()
        tick += 1

        if status.endswith("_COMPLETE"):
            sys.stdout.write("\n")
            if "ROLLBACK" in status:
                raise RuntimeError(
                    "Stack {} rolled back in {} (status: {}). "
                    "Check CloudFormation events for details.".format(
                        stack_name, region, status))
            return
        elif status.endswith("_FAILED"):
            sys.stdout.write("\n")
            raise RuntimeError(
                "Stack {} failed in {} (status: {}). "
                "Check CloudFormation events for details.".format(
                    stack_name, region, status))

    sys.stdout.write("\n")
    raise RuntimeError(
        "Timeout waiting for stack {} in {} (last status: {}).".format(
            stack_name, region, status))


def delete_stack(stack_name, region):
    """Delete a CFN stack and wait for completion."""
    cfn = client("cloudformation", region)

    if not stack_exists(stack_name, region):
        print("    Stack {} does not exist in {}, skipping.".format(
            stack_name, region))
        return

    # Check for in-progress operations first
    status = get_stack_status(stack_name, region)
    if status and "IN_PROGRESS" in status:
        print("    Stack {} is {} — waiting before delete...".format(
            stack_name, status))
        try:
            _wait_for_stack(stack_name, region, status)
        except RuntimeError:
            pass  # Proceed with delete even if the prior operation failed

    print("    Deleting stack {} in {}...".format(stack_name, region))
    try:
        cfn.delete_stack(StackName=stack_name)
    except ClientError as e:
        if "does not exist" in str(e):
            return
        raise

    # Wait for deletion
    spinner = ["|", "/", "-", "\\"]
    tick = 0
    start = time.time()
    timeout = 600  # 10 minutes

    while time.time() - start < timeout:
        time.sleep(15)
        status = get_stack_status(stack_name, region)
        elapsed = int(time.time() - start)

        if status is None or status == "DELETE_COMPLETE":
            sys.stdout.write("\n")
            print(green("    Stack {} deleted in {}.".format(stack_name, region)))
            return

        sys.stdout.write(
            "\r    {} Deleting {} in {} ({:d}s) — {}   ".format(
                spinner[tick % len(spinner)], stack_name, region, elapsed, status
            )
        )
        sys.stdout.flush()
        tick += 1

        if status == "DELETE_FAILED":
            sys.stdout.write("\n")
            raise RuntimeError(
                "Failed to delete stack {} in {}. "
                "Check for resources with DeletionPolicy=Retain or "
                "dependencies from other stacks.".format(stack_name, region))

    sys.stdout.write("\n")
    print(yellow("    Timeout waiting for deletion of {} in {}.".format(
        stack_name, region)))


# ── Lambda Code Deployment ────────────────────────────────────────────────────


def upload_lambda_code(function_name, zip_path, region):
    """Upload a zip file as Lambda function code."""
    lam = client("lambda", region)

    with open(zip_path, "rb") as f:
        zip_bytes = f.read()

    print("    Uploading code to {} in {} ({:.1f} KB)...".format(
        function_name, region, len(zip_bytes) / 1024))

    try:
        lam.update_function_code(
            FunctionName=function_name,
            ZipFile=zip_bytes,
        )
    except ClientError as e:
        if "ResourceNotFoundException" in str(e):
            print(yellow("    Lambda {} not found in {}, skipping code upload.".format(
                function_name, region)))
            return False
        raise

    # Wait for update to complete
    _wait_for_lambda_update(function_name, region)
    return True


def _wait_for_lambda_update(function_name, region):
    """Wait for Lambda function update to complete."""
    lam = client("lambda", region)
    start = time.time()
    timeout = 120

    while time.time() - start < timeout:
        try:
            resp = lam.get_function(FunctionName=function_name)
            config = resp.get("Configuration", {})
            state = config.get("State", "Unknown")
            last_update = config.get("LastUpdateStatus", "Unknown")

            if state == "Active" and last_update in ("Successful", "InProgress"):
                if last_update == "Successful":
                    return
            elif state == "Failed":
                raise RuntimeError(
                    "Lambda {} update failed: {}".format(
                        function_name,
                        config.get("LastUpdateStatusReasonCode", "unknown")))
        except ClientError:
            pass

        time.sleep(5)

    print(yellow("    Timeout waiting for Lambda {} update.".format(function_name)))


def set_lambda_env_vars(function_name, env_vars, region):
    """Update Lambda environment variables (merges with existing)."""
    lam = client("lambda", region)

    try:
        resp = lam.get_function_configuration(FunctionName=function_name)
        existing = resp.get("Environment", {}).get("Variables", {})
    except ClientError as e:
        if "ResourceNotFoundException" in str(e):
            print(yellow("    Lambda {} not found in {}, skipping env var update.".format(
                function_name, region)))
            return False
        raise

    # Merge: new vars override existing
    merged = dict(existing)
    merged.update(env_vars)

    print("    Setting env vars on {} in {} ({} vars)...".format(
        function_name, region, len(env_vars)))

    _wait_for_lambda_update(function_name, region)

    lam.update_function_configuration(
        FunctionName=function_name,
        Environment={"Variables": merged},
    )

    _wait_for_lambda_update(function_name, region)
    return True


# ── EventBridge Helpers ───────────────────────────────────────────────────────


def disable_eventbridge_rule(env, region):
    """Disable the EventBridge rule for a scenario."""
    rule = eventbridge_rule_name(env)
    try:
        client("events", region).disable_rule(Name=rule)
        print("    Disabled EventBridge rule {} in {}.".format(rule, region))
    except ClientError as e:
        if "ResourceNotFoundException" in str(e):
            print(dim("    EventBridge rule {} not found in {}, skipping.".format(
                rule, region)))
        else:
            raise


# ── ECS Helpers ───────────────────────────────────────────────────────────────


def scale_ecs_to_zero(env, region):
    """Scale ECS service to 0 tasks."""
    # The ECS service name from CFN is fo-{env}-app-svc
    svc_name = "fo-{}-app-svc".format(env)
    # The cluster name from CFN is fo-{env}-cluster
    cluster = "fo-{}-cluster".format(env)

    try:
        client("ecs", region).update_service(
            cluster=cluster,
            service=svc_name,
            desiredCount=0,
        )
        print("    Scaled {}/{} to 0 in {}.".format(cluster, svc_name, region))
    except ClientError as e:
        if "ServiceNotFoundException" in str(e) or "ClusterNotFoundException" in str(e):
            print(dim("    ECS service not found in {}, skipping.".format(region)))
        else:
            raise


# ── DynamoDB Helpers ──────────────────────────────────────────────────────────


def create_state_table(env, region):
    """Create the DynamoDB state table if it does not exist."""
    table = state_table_name(env)
    ddb = client("dynamodb", region)

    try:
        ddb.describe_table(TableName=table)
        print(dim("    DynamoDB table {} already exists in {}.".format(table, region)))
        return
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

    print("    Creating DynamoDB table {} in {}...".format(table, region))
    ddb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
        Tags=[
            {"Key": "Project", "Value": "fo-demo"},
            {"Key": "ManagedBy", "Value": "deploy_scenarios.py"},
        ],
    )

    # Wait for table to become active
    waiter = ddb.get_waiter("table_exists")
    waiter.wait(TableName=table, WaiterConfig={"Delay": 5, "MaxAttempts": 30})
    print(green("    DynamoDB table {} created in {}.".format(table, region)))


def delete_state_table(env, region):
    """Delete the DynamoDB state table."""
    table = state_table_name(env)
    ddb = client("dynamodb", region)

    try:
        ddb.delete_table(TableName=table)
        print("    Deleted DynamoDB table {} in {}.".format(table, region))
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            print(dim("    DynamoDB table {} not found in {}, skipping.".format(
                table, region)))
        else:
            raise


# ── Container Image Discovery ─────────────────────────────────────────────────


def discover_container_image(region):
    """Find the container image URI from the existing fo-demo-app stack or ECR."""
    # Try the existing fo-demo-app stack first
    outputs = get_stack_outputs("fo-demo-app", region)
    if outputs:
        # Some stacks expose the image as a parameter, not an output.
        # Fall through to ECR discovery.
        pass

    # Try to find a repo matching fo-demo in ECR
    ecr = client("ecr", region)
    try:
        repos = ecr.describe_repositories()
        for repo in repos.get("repositories", []):
            name = repo.get("repositoryName", "")
            if "fo-demo" in name or "fo-" in name:
                uri = repo.get("repositoryUri", "")
                # Get latest tag
                try:
                    images = ecr.list_images(
                        repositoryName=name,
                        filter={"tagStatus": "TAGGED"},
                        maxResults=10,
                    )
                    tags = [
                        img["imageTag"]
                        for img in images.get("imageIds", [])
                        if "imageTag" in img
                    ]
                    if "latest" in tags:
                        return "{}:latest".format(uri)
                    elif tags:
                        return "{}:{}".format(uri, tags[0])
                    else:
                        return "{}:latest".format(uri)
                except ClientError:
                    return "{}:latest".format(uri)
    except ClientError:
        pass

    return "placeholder"


# ── ALB ARN Suffix Extraction ─────────────────────────────────────────────────


def extract_alb_arn_suffix(outputs):
    """Extract the ALB ARN suffix from stack outputs.

    The ALB ARN looks like:
        arn:aws:elasticloadbalancing:region:acct:loadbalancer/app/name/hex
    The suffix for CloudWatch metrics is:
        app/name/hex
    """
    alb_arn = outputs.get("InternalAlbArn", "")
    if "/app/" in alb_arn:
        idx = alb_arn.index("/app/")
        return alb_arn[idx + 1:]  # "app/name/hex"
    return ""


# ── Deploy a Single Scenario ─────────────────────────────────────────────────


def deploy_scenario(env, regions=None):
    """Deploy all stacks for a single scenario."""
    if env not in SCENARIOS:
        print(red("Unknown scenario: {}".format(env)))
        return False

    scen = SCENARIOS[env]
    if regions is None:
        regions = BOTH_REGIONS

    print()
    print(bold("=" * 70))
    print(bold("Deploying: {} ({})".format(env, scen["description"])))
    print(bold("=" * 70))
    print()

    # Read templates
    app_template = read_template(APP_TEMPLATE)
    failover_template = read_template(FAILOVER_TEMPLATE)

    # Discover container image from primary region
    print(bold("[1/7] Discovering container image..."))
    container_image = discover_container_image(PRIMARY_REGION)
    print("    Container image: {}".format(container_image))
    print()

    # Deploy app stacks in both regions (parallel)
    print(bold("[2/7] Deploying app stacks..."))
    app_errors = {}

    def _deploy_app(region):
        params = {
            "Env": env,
            "NetworkStack": NETWORK_STACK,
            "SharedAppStack": SHARED_APP_STACK,
            "ContainerImage": container_image,
            "AuroraEndpoint": "placeholder",
            "AuroraPort": "5432",
            "AuroraDb": "appdb",
            "AuroraUser": "appuser",
            "AuroraPassword": "changeme",
            "RegionName": region,
            "DesiredCount": "0",
        }
        deploy_stack(app_stack_name(env), app_template, params, region)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {}
        for region in regions:
            f = pool.submit(_deploy_app, region)
            futures[f] = region

        for f in as_completed(futures):
            region = futures[f]
            try:
                f.result()
            except Exception as e:
                app_errors[region] = str(e)
                print(red("    ERROR deploying app stack in {}: {}".format(region, e)))

    if app_errors:
        print(red("App stack deployment failed. Cannot proceed with failover stacks."))
        return False

    print()

    # Get app stack outputs (needed for failover stack params)
    print(bold("[3/7] Reading app stack outputs..."))
    app_outputs = {}
    for region in regions:
        outputs = get_stack_outputs(app_stack_name(env), region)
        app_outputs[region] = outputs
        print("    {}: ALB DNS = {}".format(
            region, outputs.get("InternalAlbDns", "N/A")))
    print()

    # Create DynamoDB state tables
    print(bold("[4/7] Creating DynamoDB state tables..."))
    for region in regions:
        create_state_table(env, region)
    print()

    # Deploy failover stacks (sequential — depends on app outputs)
    print(bold("[5/7] Deploying failover stacks..."))
    for region in regions:
        outputs = app_outputs.get(region, {})
        alb_dns = outputs.get("InternalAlbDns", "placeholder")
        alb_arn_suffix = extract_alb_arn_suffix(outputs)
        ecs_cluster = outputs.get("EcsClusterName", "fo-{}-cluster".format(env))
        ecs_service = outputs.get("EcsServiceName", "fo-{}-app-svc".format(env))

        params = {
            "Env": env,
            "AppStack": app_stack_name(env),
            "NetworkStack": NETWORK_STACK,
            "PrimaryRegion": PRIMARY_REGION,
            "SecondaryRegion": SECONDARY_REGION,
            "StateTable": state_table_name(env),
            "InternalAlbDns": alb_dns,
            "AlbArnSuffix": alb_arn_suffix,
            "EcsClusterName": ecs_cluster,
            "EcsServiceName": ecs_service,
            "AuroraClusterId": aurora_cluster_id(env, region),
            "AuroraGlobalClusterId": aurora_global_cluster_id(env),
            "NotificationEmail": NOTIFICATION_EMAIL,
            "FailoverMode": "auto",
            "CooldownMinutes": "5",
            "ConsecutiveFailuresThreshold": "3",
        }

        try:
            deploy_stack(failover_stack_name(env), failover_template, params, region)
        except Exception as e:
            print(red("    ERROR deploying failover stack in {}: {}".format(region, e)))
            return False
    print()

    # Upload Lambda code
    print(bold("[6/7] Uploading Lambda code..."))
    version = scen["version"]

    orchestrator_zip = build_orchestrator_zip(version)
    failback_zip = build_failback_zip()

    try:
        for region in regions:
            # The Lambda names come from the CFN template: fo-{Env}-orchestrator
            orch_name = "fo-{}-orchestrator".format(env)
            fb_name = "fo-{}-failback".format(env)

            upload_lambda_code(orch_name, orchestrator_zip, region)
            upload_lambda_code(fb_name, failback_zip, region)
    finally:
        # Clean up temp files
        try:
            os.unlink(orchestrator_zip)
        except OSError:
            pass
        try:
            os.unlink(failback_zip)
        except OSError:
            pass

    # Set scenario-specific env vars on Lambda
    print()
    print("    Setting scenario-specific environment variables...")
    extra_env = {
        "ROUTING_MODE": scen["routing_mode"],
        "PASSIVE_PUBLISH_ZERO": str(scen["passive_publish_zero"]).lower(),
    }
    if scen["ai_rca_enabled"]:
        extra_env["AI_RCA_ENABLED"] = "true"
        extra_env["AI_RCA_PROVIDER"] = scen["ai_rca_provider"] or ""
    else:
        extra_env["AI_RCA_ENABLED"] = "false"

    # Set CW_NAMESPACE to scenario-specific namespace
    extra_env["CW_NAMESPACE"] = "Custom/{}".format(env)

    for region in regions:
        orch_name = "fo-{}-orchestrator".format(env)
        set_lambda_env_vars(orch_name, extra_env, region)
    print()

    # Park the scenario: disable EventBridge, scale ECS to 0
    print(bold("[7/7] Parking scenario (disable EventBridge, scale ECS to 0)..."))
    for region in regions:
        disable_eventbridge_rule(env, region)
        scale_ecs_to_zero(env, region)
    print()

    print(green("Scenario {} deployed and parked successfully.".format(bold(env))))
    print("  Use 'python3 tools/demo_manager.py activate {}' to start it.".format(env))
    print()
    return True


# ── Teardown a Single Scenario ────────────────────────────────────────────────


def teardown_scenario(env, regions=None):
    """Delete all stacks for a single scenario."""
    if env not in SCENARIOS:
        print(red("Unknown scenario: {}".format(env)))
        return False

    scen = SCENARIOS[env]
    if regions is None:
        regions = BOTH_REGIONS

    print()
    print(bold("=" * 70))
    print(bold("Tearing down: {} ({})".format(env, scen["description"])))
    print(bold("=" * 70))
    print()

    # Step 1: Delete failover stacks first (they depend on app stack exports)
    print(bold("[1/3] Deleting failover stacks..."))
    for region in regions:
        try:
            delete_stack(failover_stack_name(env), region)
        except Exception as e:
            print(red("    ERROR deleting failover stack in {}: {}".format(region, e)))
    print()

    # Step 2: Delete app stacks
    print(bold("[2/3] Deleting app stacks..."))
    for region in regions:
        try:
            delete_stack(app_stack_name(env), region)
        except Exception as e:
            print(red("    ERROR deleting app stack in {}: {}".format(region, e)))
    print()

    # Step 3: Clean up DynamoDB state tables
    print(bold("[3/3] Cleaning up DynamoDB state tables..."))
    for region in regions:
        delete_state_table(env, region)
    print()

    print(green("Scenario {} torn down.".format(bold(env))))
    print()
    return True


# ── Status for a Scenario ─────────────────────────────────────────────────────


def show_scenario_status(env):
    """Show detailed status of a scenario's stacks."""
    if env not in SCENARIOS:
        print(red("Unknown scenario: {}".format(env)))
        return

    scen = SCENARIOS[env]
    print()
    print(bold("{} ({})".format(env, scen["description"])))
    print("-" * 60)

    for region in BOTH_REGIONS:
        print()
        print(bold("  Region: {}".format(region)))

        # App stack
        app_status = get_stack_status(app_stack_name(env), region)
        if app_status:
            color = green if "COMPLETE" in app_status and "ROLLBACK" not in app_status else yellow
            print("    App stack ({}): {}".format(
                app_stack_name(env), color(app_status)))
        else:
            print("    App stack ({}): {}".format(
                app_stack_name(env), dim("NOT DEPLOYED")))

        # Failover stack
        fo_status = get_stack_status(failover_stack_name(env), region)
        if fo_status:
            color = green if "COMPLETE" in fo_status and "ROLLBACK" not in fo_status else yellow
            print("    Failover stack ({}): {}".format(
                failover_stack_name(env), color(fo_status)))
        else:
            print("    Failover stack ({}): {}".format(
                failover_stack_name(env), dim("NOT DEPLOYED")))

        # DynamoDB
        table = state_table_name(env)
        try:
            client("dynamodb", region).describe_table(TableName=table)
            print("    DynamoDB table ({}): {}".format(table, green("EXISTS")))
        except ClientError:
            print("    DynamoDB table ({}): {}".format(table, dim("NOT FOUND")))

    print()


# ── Commands ──────────────────────────────────────────────────────────────────


def cmd_deploy(args):
    """Deploy a single scenario."""
    env = args.env
    regions = [args.region] if args.region else None
    if not deploy_scenario(env, regions):
        sys.exit(1)


def cmd_deploy_all(args):
    """Deploy all 9 scenarios."""
    print()
    print(bold("Deploying ALL 9 demo scenarios"))
    print(bold("=" * 70))
    print()
    print(yellow("This will create 18 app stacks (9 scenarios x 2 regions)"))
    print(yellow("and 18 failover stacks. Each scenario gets:"))
    print(yellow("  - ALB, ECS cluster, ECS service, security groups"))
    print(yellow("  - Lambda functions, EventBridge rule, SNS topic"))
    print(yellow("  - CloudWatch alarm, Route 53 health check"))
    print()
    print(yellow("Estimated time: ~30-45 minutes"))
    print()

    if not args.yes:
        confirm = input("Continue? [y/N] ").strip().lower()
        if confirm != "y":
            print("Aborted.")
            return

    errors = {}
    for env in SCENARIOS:
        try:
            if not deploy_scenario(env):
                errors[env] = "Deployment returned failure"
        except Exception as e:
            print(red("ERROR deploying {}: {}".format(env, e)))
            errors[env] = str(e)

    print()
    print(bold("=" * 70))
    if errors:
        print(red("The following scenarios had errors:"))
        for env, err in errors.items():
            print(red("  {}: {}".format(env, err)))
        sys.exit(1)
    else:
        print(green("All 9 scenarios deployed and parked successfully."))
    print()


def cmd_teardown(args):
    """Teardown a single scenario."""
    env = args.env

    if not args.yes:
        confirm = input(
            "Delete all stacks for {}? This cannot be undone. [y/N] ".format(env)
        ).strip().lower()
        if confirm != "y":
            print("Aborted.")
            return

    if not teardown_scenario(env):
        sys.exit(1)


def cmd_teardown_all(args):
    """Teardown all 9 scenarios."""
    print()
    print(bold("Tearing down ALL 9 demo scenarios"))
    print(bold("=" * 70))
    print()
    print(red("This will DELETE all CloudFormation stacks and DynamoDB tables"))
    print(red("for all 9 scenarios in both regions."))
    print()

    if not args.yes:
        confirm = input("Type 'DELETE ALL' to confirm: ").strip()
        if confirm != "DELETE ALL":
            print("Aborted.")
            return

    errors = {}
    for env in SCENARIOS:
        try:
            if not teardown_scenario(env):
                errors[env] = "Teardown returned failure"
        except Exception as e:
            print(red("ERROR tearing down {}: {}".format(env, e)))
            errors[env] = str(e)

    print()
    print(bold("=" * 70))
    if errors:
        print(red("The following scenarios had errors:"))
        for env, err in errors.items():
            print(red("  {}: {}".format(env, err)))
        sys.exit(1)
    else:
        print(green("All 9 scenarios torn down."))
    print()


def cmd_list(args):
    """List all scenarios."""
    print()
    print(bold("Failover Orchestrator Demo Scenarios"))
    print(bold("=" * 80))
    print()

    header = "{:<14s} {:<45s} {:<8s} {:<15s}".format(
        "ENV", "DESCRIPTION", "VERSION", "ROUTING MODE"
    )
    print(bold(header))
    print("-" * 80)

    for env, scen in SCENARIOS.items():
        ai_tag = ""
        if scen["ai_rca_enabled"]:
            ai_tag = " +AI({})".format(scen["ai_rca_provider"])
        if scen["passive_publish_zero"]:
            ai_tag += " +ZeroCont"

        print("{:<14s} {:<45s} {:<8s} {:<15s}{}".format(
            cyan(env),
            scen["description"],
            scen["version"],
            scen["routing_mode"],
            dim(ai_tag) if ai_tag else "",
        ))

    print()
    print("Stack naming convention:")
    print("  App stack:      {env}-app")
    print("  Failover stack: {env}-failover")
    print("  State table:    {env}-state")
    print("  Network stack:  {} (shared)".format(NETWORK_STACK))
    print()


def cmd_status(args):
    """Show status of a scenario or all scenarios."""
    if args.env:
        show_scenario_status(args.env)
    else:
        for env in SCENARIOS:
            show_scenario_status(env)


# ── CLI Entry Point ──────────────────────────────────────────────────────────


def build_parser():
    parser = argparse.ArgumentParser(
        description="Deploy CloudFormation stacks for failover demo scenarios",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s deploy fo-v10-s1                 Deploy one scenario to both regions
  %(prog)s deploy fo-v10-s1 --region us-west-1  Deploy to one region only
  %(prog)s deploy-all                       Deploy all 9 scenarios
  %(prog)s deploy-all --yes                 Deploy all without confirmation
  %(prog)s teardown fo-v10-s1               Teardown one scenario
  %(prog)s teardown-all                     Teardown all 9 scenarios
  %(prog)s list                             List all scenario definitions
  %(prog)s status fo-v10-s1                 Show stack status for one scenario
  %(prog)s status                           Show stack status for all scenarios
""",
    )
    sub = parser.add_subparsers(dest="command", help="Command to run")
    sub.required = True

    # deploy
    p_deploy = sub.add_parser("deploy", help="Deploy a single scenario")
    p_deploy.add_argument(
        "env", choices=list(SCENARIOS.keys()), help="Scenario env name")
    p_deploy.add_argument(
        "--region", choices=BOTH_REGIONS,
        help="Deploy to one region only (default: both)")
    p_deploy.add_argument(
        "--yes", "-y", action="store_true",
        help="Skip confirmation prompts")

    # deploy-all
    p_deploy_all = sub.add_parser("deploy-all", help="Deploy all 9 scenarios")
    p_deploy_all.add_argument(
        "--yes", "-y", action="store_true",
        help="Skip confirmation prompts")

    # teardown
    p_td = sub.add_parser("teardown", help="Teardown a single scenario")
    p_td.add_argument(
        "env", choices=list(SCENARIOS.keys()), help="Scenario env name")
    p_td.add_argument(
        "--yes", "-y", action="store_true",
        help="Skip confirmation prompts")

    # teardown-all
    p_td_all = sub.add_parser("teardown-all", help="Teardown all 9 scenarios")
    p_td_all.add_argument(
        "--yes", "-y", action="store_true",
        help="Skip confirmation prompts")

    # list
    sub.add_parser("list", help="List all scenario definitions")

    # status
    p_status = sub.add_parser("status", help="Show stack status for scenarios")
    p_status.add_argument(
        "env", nargs="?", choices=list(SCENARIOS.keys()),
        help="Scenario env name (omit for all)")

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    # Validate that CFN templates exist
    for path, name in [
        (APP_TEMPLATE, "app.yaml"),
        (FAILOVER_TEMPLATE, "failover.yaml"),
    ]:
        if not os.path.isfile(path):
            print(red("ERROR: CFN template not found: {}".format(path)))
            sys.exit(1)

    # Validate Lambda source files exist (only for deploy commands)
    if args.command in ("deploy", "deploy-all"):
        for path, name in [
            (ORCHESTRATOR_SRC, "failover_orchestrator_v3.py"),
            (FAILBACK_SRC, "manual_failback_v2.py"),
            (STATE_BACKEND_SRC, "state_backend.py"),
        ]:
            if not os.path.isfile(path):
                print(red("ERROR: Lambda source not found: {}".format(path)))
                sys.exit(1)

    dispatch = {
        "deploy": cmd_deploy,
        "deploy-all": cmd_deploy_all,
        "teardown": cmd_teardown,
        "teardown-all": cmd_teardown_all,
        "list": cmd_list,
        "status": cmd_status,
    }

    cmd_fn = dispatch.get(args.command)
    if cmd_fn:
        cmd_fn(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
