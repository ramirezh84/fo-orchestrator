# Failover Orchestrator — Deployment Notes & Lessons Learned

This document covers issues found during the actual deployment of the failover orchestrator and the fixes applied. Read this alongside the main operational guide and console deployment guide.

---

## Critical Configuration Requirements

### 1. HEALTH_CHECK_URL Must Point to the Internal ALB

**Problem:** The HEALTH_CHECK_URL was initially set to the Route 53 domain (`https://mcc-data-management.ddtran.apps...`). This creates a circular dependency — the Lambda that decides whether to flip DNS is checking the endpoint that DNS controls. After failover, the URL resolves to the other region's NLB, so the Lambda checks the wrong region.

**Fix:** Each region's Lambda must point to its own internal ALB directly:

- us-east-1: `https://internal-<alb-name>.us-east-1.elb.amazonaws.com`
- us-east-2: `https://internal-<alb-name>.us-east-2.elb.amazonaws.com`

Find the ALB DNS under EC2 > Load Balancers. Use the internal ALB that sits in front of ECS, not the routable NLB that fronts API Gateway.

### 2. SSL Certificate Verification

**Problem:** The internal ALB uses a self-signed or internal CA certificate. Python's `urlopen` rejects it with `SSL: CERTIFICATE_VERIFY_FAILED`.

**Fix:** Set the environment variable `HEALTH_CHECK_DISABLE_SSL_VERIFY=true` on both the orchestrator and failback Lambdas in both regions. The code creates a custom SSL context that skips certificate chain validation.

### 3. Environment Variable: API_GW_NAME (not API_GW_ID)

**Problem:** The code was updated to read `API_GW_NAME` (matching the IaC tool's output) instead of `API_GW_ID`. The CloudWatch dimension is still `ApiId` (this is AWS's dimension name, not ours). The value should be the API Gateway's identifier as reported to CloudWatch.

### 4. CW_NAMESPACE Must Match Between Code, Alarms, and Both Regions

**Problem:** The team uses `MCC/RegionFailover` as the namespace. If even one component (Lambda env var, CloudWatch alarm, or the other region's Lambda) uses a different namespace, things silently break.

**Fix:** Verify all four match:
- us-east-1 Lambda `CW_NAMESPACE` env var
- us-east-2 Lambda `CW_NAMESPACE` env var
- us-east-1 CloudWatch alarm namespace
- us-east-2 CloudWatch alarm namespace

### 5. AURORA_CLUSTER_ID Is the Regional Cluster Identifier

**Problem:** The `AURORA_CLUSTER_ID` was initially set to the Aurora resource ID (like `cluster-m4jkeozzeekgzo4alfl55iygdq`) instead of the DB cluster identifier.

**Fix:** Use the human-readable DB cluster identifier from the RDS console (e.g., `perf-global-cluster`). In this deployment, both regions use the same identifier (`perf-global-cluster`), which is valid for Aurora Global Database.

`AURORA_GLOBAL_CLUSTER_ID` is the global cluster name (`mcc-perf-global`), which is separate from the regional cluster identifier.

---

## CloudWatch Alarm Configuration

### Alarm Dimensions Must Be Correct

**Problem (Critical):** The IaC tool generated alarm dimensions incorrectly, swapping Name/Value:

```json
// WRONG — IaC generated this
"Dimensions": [
    {"Name": "Value", "Value": "us-east-1"},
    {"Name": "Name", "Value": "Region"}
]

// CORRECT — should be this
"Dimensions": [
    {"Name": "Region", "Value": "us-east-1"}
]
```

The alarm watched a metric that didn't exist and stayed in ALARM permanently even though the actual metric had data.

**Detection:** Run `aws cloudwatch describe-alarms --alarm-names "<alarm-name>"` and verify the Dimensions block shows `"Name": "Region", "Value": "us-east-1"` — one dimension, not two.

**Fix:** Recreate the alarm from the CloudWatch metric browser (click "Create alarm" directly from the metric row) or use CLI:

```bash
aws cloudwatch put-metric-alarm \
  --alarm-name "mcc-region-active-status-use1" \
  --namespace "MCC/RegionFailover" \
  --metric-name "RegionActiveStatus" \
  --dimensions Name=Region,Value=us-east-1 \
  --statistic Minimum \
  --period 60 \
  --evaluation-periods 1 \
  --threshold 1 \
  --comparison-operator LessThanThreshold \
  --treat-missing-data breaching \
  --region us-east-1
```

### Alarm Settings Reference

| Setting | Value |
|---------|-------|
| Namespace | `MCC/RegionFailover` (must match Lambda's `CW_NAMESPACE`) |
| Metric name | `RegionActiveStatus` |
| Dimension | `Region` = `us-east-1` (or `us-east-2`) |
| Statistic | `Minimum` |
| Period | `60 seconds` |
| Threshold | `Less than 1` |
| Datapoints to alarm | `1 out of 1` |
| Missing data treatment | `Treat missing data as bad (breaching)` |

---

## Route 53 Health Checks

### Health Check Type Must Be CloudWatch Metric

The Route 53 health checks must be type **CloudWatch Metric** (also called "State of CloudWatch alarm"), NOT HTTP/HTTPS. The orchestrator controls Route 53 through the CloudWatch alarm, not through direct HTTP probing.

Create in Route 53 > Health checks > Create:
1. What to monitor: **State of CloudWatch alarm**
2. Region: `us-east-1` (or `us-east-2`)
3. Alarm: select the corresponding alarm
4. Health check status: **Unhealthy when alarm is in ALARM state**

### Both Health Checks Must Be Healthy During Normal Operation

During normal steady-state operation, both the us-east-1 and us-east-2 Route 53 health checks should show **Healthy**. If us-east-2 shows Unhealthy, Route 53 won't failover to it when us-east-1 fails.

### Evaluate Target Health Must Be Disabled

Set **Evaluate Target Health = No** on both failover records. The orchestrator Lambda is the single decision-maker for health — letting Route 53 also evaluate the NLB's health creates conflicting signals.

---

## VPC and Networking

### Interface Endpoints with Private DNS Override NAT Gateway

**Problem:** The us-east-2 Lambda timed out (120 seconds, zero output) even though the VPC had a NAT gateway and the route table was correct. The Lambda couldn't reach AWS services.

**Root cause:** The VPC had ~59 interface endpoints (STS, CloudWatch, SNS, etc.) with **Private DNS names enabled = Yes**. When private DNS is enabled, the Lambda resolves `sts.us-east-2.amazonaws.com` to the interface endpoint's private IP instead of going through the NAT gateway. If the endpoint's security group doesn't allow inbound 443 from the Lambda's security group, the connection hangs silently.

**Diagnosis:** If the Lambda shows "Found credentials in environment variables" and then nothing — zero output from the code — the Lambda can't reach STS. STS is the first AWS service call boto3 makes to assume the execution role.

**Fix:** The interface endpoint security groups must allow inbound HTTPS (443) from the Lambda's security group. Check the STS endpoint first (`com.amazonaws.<region>.sts`) as it's the most common blocker.

### Cross-Region CloudWatch Calls Require Timeout Configuration

**Problem:** The passive region Lambda (us-east-2) calls `monitoring.us-east-1.amazonaws.com` for staleness detection Method 2. In VPCs with restricted networking, this cross-region call may be blocked or extremely slow, causing the Lambda to hang.

**Fix (in code):** The cross-region CloudWatch client uses `BotoConfig(connect_timeout=5, read_timeout=10, retries=max_attempts=1)` so it fails fast instead of hanging. A timeout is treated as "stale" which is correct behavior.

**Fix (logic):** The staleness detection uses AND logic — both DynamoDB heartbeat AND cross-region CloudWatch must agree the active region is stale. This prevents false positives in VPCs where cross-region calls always fail. This behavior can be made more aggressive by setting the `STALENESS_DETECTION_MODE` environment variable to `DDB_ONLY`, which will rely solely on the DynamoDB heartbeat.

---

## Lambda Configuration

### Orchestrator Lambda

| Setting | Value |
|---------|-------|
| Function name | `app-failover-folambda` |
| Handler | `failover_orchestrator_v3.handler` |
| Runtime | Python 3.12 |
| Timeout | **120 seconds** (2 minutes) |
| Memory | **256 MB** |
| VPC | Same VPC as ECS/ALB |
| Subnets | Private subnets with routes to VPC endpoints or NAT |

### Failback Lambda

| Setting | Value |
|---------|-------|
| Function name | `app-mfolambda` |
| Handler | `manual_failback_v2.handler` |
| Runtime | Python 3.12 |
| Timeout | **300 seconds** (5 minutes) |
| Memory | **256 MB** |
| VPC | Same VPC as ECS/ALB |
| Subnets | Private subnets with routes to VPC endpoints or NAT |

### Required Environment Variables (Both Lambdas)

| Key | Example Value | Notes |
|-----|---------------|-------|
| `APP_NAME` | `MCC` | Included in all SNS notification subjects |
| `FAILBACK_FUNCTION_NAME` | `app-mfolambda` | Orchestrator only - used in Aurora promotion commands |
| `PRIMARY_REGION` | `us-east-1` | |
| `SECONDARY_REGION` | `us-east-2` | |
| `STATE_TABLE` | `app-failover-state` | DynamoDB Global Table name |
| `SNS_TOPIC_ARN` | `arn:aws:sns:us-east-1:...` | Region-specific ARN |
| `CW_NAMESPACE` | `MCC/RegionFailover` | Must match alarms exactly |
| `CW_METRIC_NAME` | `RegionActiveStatus` | |
| `HEALTH_CHECK_URL` | `https://internal-alb-...elb.amazonaws.com` | Region-specific internal ALB |
| `HEALTH_ENDPOINT` | `/health` | |
| `HEALTH_CHECK_TIMEOUT_SECONDS` | `5` | |
| `HEALTH_CHECK_DISABLE_SSL_VERIFY` | `true` | Required for internal CA certs |
| `ECS_CLUSTER_NAME` | `ecsf69b-fiftmcce1-v1` | Region-specific |
| `ECS_SERVICE_NAME` | `mcc-data-management-v1` | |
| `AURORA_CLUSTER_ID` | `perf-global-cluster` | Regional DB cluster identifier |
| `AURORA_GLOBAL_CLUSTER_ID` | `mcc-perf-global` | Global cluster name |

### Additional Orchestrator-Only Variables

| Key | Example Value | Notes |
|-----|---------------|-------|
| `ALB_ARN_SUFFIX` | `app/albb6885e-fiftmcce1-v1/...` | Region-specific |
| `TG_ARN_SUFFIX` | `targetgroup/ecs-.../...` | Region-specific |
| `API_GW_NAME` | `api21a44fcc-fiftmcce1-v1` | Region-specific |
| `FAILOVER_MODE` | `manual` | Start with `manual`, switch to `auto` |
| `AURORA_AUTO_PROMOTE` | `false` | Set to `true` to enable auto-promotion with guards. |
| `AURORA_PROMOTION_STRATEGY` | `SWITCHOVER_THEN_FAILOVER` | Use `FAILOVER_ONLY` if `SwitchoverGlobalCluster` is denied by IAM. |
| `COOLDOWN_MINUTES` | `30` | |
| `CONSECUTIVE_FAILURES_THRESHOLD` | `3` | |
| `ACTIVE_REGION_STALE_THRESHOLD_MINUTES` | `3` | |
| `STALENESS_DETECTION_MODE` | `DDB_AND_CLOUDWATCH` | `DDB_ONLY` is faster but riskier. See op guide. |
| `AURORA_PROMOTION_REMINDER_INTERVAL_MINUTES` | `5` | |
| `AURORA_MAX_REPLICATION_LAG_SECONDS` | `5` | Max Aurora lag for auto-promote pre-flight check. |
| `WARNING_NOTIFICATION_COOLDOWN_MINUTES` | `10` | |

---

## FAILOVER_MODE: Manual vs Auto

### Manual Mode (Initial Deployment)

Set `FAILOVER_MODE=manual` for initial deployment validation. The Lambda detects failures normally (health checks, consecutive failure counting, cooldown) but when the threshold is reached, instead of flipping DNS, it sends a notification:

> "FAILOVER RECOMMENDED: us-east-1 -> us-east-2 (manual mode)"

The notification includes a single command to execute the failover:

```bash
aws lambda invoke \
  --function-name app-failover-folambda \
  --payload '{"execute_failover": true}' \
  --region us-east-1 \
  response.json
```

### Auto Mode (Production Steady State)

Set `FAILOVER_MODE=auto` once the team has confidence. The Lambda executes failover automatically without operator intervention.

### Manual Mode Does NOT Apply to Region Failures

When the entire region goes down (Scenario 2), `FAILOVER_MODE` is bypassed. Route 53 moves traffic automatically via the CloudWatch alarm's missing data treatment, and the passive Lambda sends Aurora promotion commands without checking the mode. This is by design — there's no us-east-1 Lambda to invoke the `execute_failover` command.

---

## Operator Commands

### View Current State

Use the CLI script: `python3 failover_cli.py` → option 1

Or read DynamoDB directly:
```bash
aws dynamodb get-item \
  --table-name app-failover-state \
  --key '{"pk": {"S": "REGION_STATE"}}' \
  --region us-east-1 \
  --output json
```

### Execute Failover (Manual Mode)

```bash
aws lambda invoke \
  --function-name app-failover-folambda \
  --payload '{"execute_failover": true}' \
  --region us-east-1 \
  response.json
```

### Failback to us-east-1

Step 1 — Switchover Aurora:
```bash
aws rds switchover-global-cluster \
  --global-cluster-identifier mcc-perf-global \
  --target-db-cluster-identifier <us-east-1-cluster-arn> \
  --region us-east-1
```

Step 2 — Monitor until the target cluster is the writer (ReplicationSourceIdentifier is empty):
```bash
aws rds describe-db-clusters \
  --db-cluster-identifier <target-cluster-id> \
  --query 'DBClusters[0].{Status:Status,ReplicationSource:ReplicationSourceIdentifier}' \
  --region us-east-1
```

Step 3 — Execute failback:
```bash
aws lambda invoke \
  --function-name app-mfolambda \
  --payload '{"target_region": "us-east-1", "skip_health_check": false, "operator": "enrique", "aurora_confirmed": true}' \
  --region us-east-1 \
  response.json
```

### Reset State (Emergency Only)

```bash
aws lambda invoke \
  --function-name app-failover-folambda \
  --payload '{"reset_state": true}' \
  --region us-east-1 \
  response.json
```

---

## IAM Permissions

The Lambda role needs these permissions:



1.  **Managed policy:** `AWSLambdaVPCAccessExecutionRole`

2.  **DynamoDB:** `GetItem`, `PutItem`, `UpdateItem` on the state table in both regions

3.  **CloudWatch:** `GetMetricStatistics`, `GetMetricData`, `PutMetricData` (Resource: `*`)

4.  **ECS:** `DescribeServices` (scoped to cluster/service ARNs)

5.  **RDS:** `DescribeDBClusters` (Resource: `*` — this action doesn't support resource-level ARNs for clusters)

6.  **SNS:** `Publish` (scoped to topic ARNs in both regions)

7.  **KMS:** `GenerateDataKey`, `Decrypt` (if SNS topics are KMS-encrypted — scoped to the KMS key ARNs)



Note: When `AURORA_AUTO_PROMOTE` is enabled, the role also needs `rds:FailoverGlobalCluster`. The `rds:SwitchoverGlobalCluster` permission is only needed if the `AURORA_PROMOTION_STRATEGY` is `SWITCHOVER_THEN_FAILOVER`. The pre-flight checks for this feature also require cross-region access for `cloudwatch:GetMetricStatistics` and `rds:DescribeDBClusters`.



---

## Troubleshooting

### Lambda Times Out with Zero Output

**Symptom:** "Found credentials in environment variables" then nothing. Status: timeout.

**Cause:** Lambda can't reach AWS services. Usually STS is blocked by VPC interface endpoint security groups.

**Fix:** Check interface endpoint security groups allow inbound 443 from the Lambda's SG. STS endpoint is the most common blocker.

### Lambda Times Out After "Running as PASSIVE region"

**Symptom:** Logs show the handler running, passive region detected, then timeout.

**Cause:** Cross-region CloudWatch API call hanging. The code now has a 5-second timeout on this call, but older versions hang for the full Lambda timeout.

**Fix:** Deploy the latest code which includes `BotoConfig(connect_timeout=5)` on cross-region calls.

### Alarm In ALARM Even Though Metric Has Data

**Symptom:** CloudWatch metric browser shows data points but the alarm graph is empty.

**Cause:** Alarm dimensions don't match the metric dimensions. Run `describe-alarms` and verify the Dimensions block.

**Fix:** Recreate the alarm from the metric browser or use CLI with correct dimensions.

### "Target region is already the active region. No failback needed."

**Symptom:** Failback Lambda returns this message.

**Cause:** The state was already reset (via `reset_state`). There's nothing to fail back from.

**Explanation:** This is not an error. The system is already in the correct state.

### "Cannot failback while state is FAILBACK_IN_PROGRESS"

**Symptom:** A previous failback attempt timed out and left the state stuck.

**Fix:** Reset the state, then retry:
```bash
aws lambda invoke \
  --function-name app-failover-folambda \
  --payload '{"reset_state": true}' \
  --region us-east-1 \
  response.json
```

### SNS Publish Fails with KMSAccessDenied

**Symptom:** "kms:GenerateDataKey" error on SNS Publish.

**Cause:** SNS topic is KMS-encrypted and the Lambda role doesn't have KMS permissions.

**Fix:** Add `kms:GenerateDataKey` and `kms:Decrypt` on the KMS key ARN to the Lambda role.

---

## Secondary Region ECS Scaling

### When to Enable

Enable this for apps that run ECS desired count = 0 in the secondary region because the containers consume from Kafka or perform background processing that cannot run against an Aurora read replica.

When enabled, the orchestrator scales up ECS tasks in the secondary region BEFORE flipping DNS during failover. The failback Lambda scales them back to 0 when traffic returns to the primary.

### New Environment Variables

| Variable | Value | Where | Required |
|----------|-------|-------|----------|
| `SCALE_SECONDARY_ECS` | `true` | Orchestrator + Failback, both regions | Only for apps that need it |
| `ECS_SECONDARY_CLUSTER_NAME` | `<us-east-2 cluster name>` | Orchestrator + Failback, both regions | Only if SCALE_SECONDARY_ECS=true |
| `ECS_SECONDARY_SERVICE_NAME` | `<us-east-2 service name>` | Orchestrator + Failback, both regions | Only if SCALE_SECONDARY_ECS=true |
| `ECS_SECONDARY_DESIRED_COUNT` | `2` (production count) | Orchestrator only, both regions | Only if SCALE_SECONDARY_ECS=true |

For apps that don't need this, don't set `SCALE_SECONDARY_ECS`. It defaults to `false` and the feature is completely skipped.

### Additional IAM Permission

The Lambda role needs `ecs:UpdateService` on the secondary region's ECS service:

```json
{
  "Effect": "Allow",
  "Action": "ecs:UpdateService",
  "Resource": "arn:aws:ecs:us-east-2:<account-id>:service/<cluster-name>/<service-name>"
}
```

### Failover Sequence (with scaling enabled)

1. Orchestrator detects failure (3/3 threshold)
2. `ecs:UpdateService` sets desired count to N in us-east-2 (ECS starts provisioning)
3. `PutMetricData` publishes 0 for us-east-1 (Route 53 starts DNS change, ~30-60s TTL)
4. By the time DNS resolves to us-east-2, containers have had 30-60s head start
5. SNS notification sent with Aurora promotion commands

### Failback Sequence (with scaling enabled)

1. Operator runs failback Lambda
2. Lambda validates health, publishes metrics, releases latch
3. `ecs:UpdateService` sets desired count to 0 in us-east-2
4. Containers drain and stop consuming from Kafka
5. SNS confirmation notification sent

### Important: Kafka Consumer Behavior During Scale-Up

During the 30-60 seconds between ECS scale-up and Aurora promotion, the containers are running and consuming from Kafka. Writes to Aurora (read replica) will fail. The behavior depends on the app:

- If the app retries on write failure without committing offset: SAFE (messages reprocess after Aurora promotion)
- If the app commits offset before write succeeds: DATA LOSS (messages consumed but not persisted)

Ask the dev team: "If Aurora returns a read-only error, does the Kafka consumer commit the offset?" This is an existing app behavior question, not a code change.

---

## Aurora Writer Detection (DescribeDBClusters)

### Why DescribeGlobalClusters Is Not Used

`rds:DescribeGlobalClusters` is restricted by organizational policy. The orchestrator and failback Lambdas use `rds:DescribeDBClusters` instead, checking the `ReplicationSourceIdentifier` field:

- **Empty** = cluster is the primary writer
- **Set to another cluster ARN** = cluster is a secondary reader

The `rds:DescribeGlobalClusters` permission is not required anywhere in the system.

### ARN Construction

The target cluster ARN is now constructed from known values instead of looked up via API:

```
arn:aws:rds:<target-region>:<account-id>:cluster:<cluster-identifier>
```

The account ID is derived from the SNS_TOPIC_ARN environment variable (already required). No additional configuration needed.

### IAM Permissions Required

| Permission | Purpose |
|-----------|---------|
| `rds:DescribeDBClusters` | Aurora writer detection via `ReplicationSourceIdentifier` |

---

## Automated Aurora Promotion (Optional)

### Overview

When `AURORA_AUTO_PROMOTE=true`, the orchestrator automatically triggers Aurora promotion during failover instead of sending manual CLI commands via SNS. When disabled (default), the operator receives SNS notifications with copy-paste-ready commands.

### Behavior by Scenario

**App failure (region is reachable):**
1. Tries `SwitchoverGlobalCluster` first (planned, no data loss)
2. If switchover fails, falls back to `FailoverGlobalCluster` with `--allow-data-loss`
3. If both fail, sends manual notification as fallback

**Region failure (region is unreachable):**
1. Goes directly to `FailoverGlobalCluster` with `--allow-data-loss`
2. If it fails, sends manual notification as fallback

### New Environment Variable

| Variable | Default | Where |
|----------|---------|-------|
| `AURORA_AUTO_PROMOTE` | `false` | Orchestrator Lambda, both regions |

Not needed on the failback Lambda (failback always requires operator confirmation of Aurora status).

### Additional IAM Permissions (only if AURORA_AUTO_PROMOTE=true)

```json
{
  "Effect": "Allow",
  "Action": [
    "rds:SwitchoverGlobalCluster",
    "rds:FailoverGlobalCluster"
  ],
  "Resource": "arn:aws:rds::<account-id>:global-cluster:<global-cluster-id>"
}
```

For MCC specifically:
```json
{
  "Effect": "Allow",
  "Action": [
    "rds:SwitchoverGlobalCluster",
    "rds:FailoverGlobalCluster"
  ],
  "Resource": "arn:aws:rds::433607260168:global-cluster:mcc-perf-global"
}
```

### Graceful Fallback

If auto-promote is enabled but fails (IAM permission denied, Aurora API error, etc.), the orchestrator falls back to manual mode for that specific failover. The operator receives the standard notification with CLI commands. The DNS failover still proceeds regardless. This means enabling `AURORA_AUTO_PROMOTE=true` is always safe - worst case, it behaves like manual mode.
