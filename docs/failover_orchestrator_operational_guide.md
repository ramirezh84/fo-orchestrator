# Deposits 2.0 - Multi-Region Failover Orchestrator — Operational Guide

## Table of Contents

1. [Problem Statement](#problem-statement)
2. [Solution Overview](#solution-overview)
3. [Architecture](#architecture)
4. [Anti-Flip-Flop Mechanism](#anti-flip-flop-mechanism)
5. [Health Signal Evaluation](#health-signal-evaluation)
6. [Failure Scenarios](#failure-scenarios)
7. [State Machine](#state-machine)
8. [Component Inventory](#component-inventory)
9. [Operational Runbook](#operational-runbook)
10. [Automated Aurora Promotion](#automated-aurora-promotion)
11. [Aurora Writer Detection](#aurora-writer-detection)
12. [Zero-Container Secondary Region](#zero-container-secondary-region)
13. [IAM Permissions Reference](#iam-permissions-reference)
14. [Migrating to Deep Health Check](#migrating-to-deep-health-check)
15. [Monitoring and Alerting](#monitoring-and-alerting)
16. [Frequently Asked Questions](#frequently-asked-questions)

---

## Problem Statement

The Domestic Deposits platform runs in an active/passive configuration across us-east-1 (primary) and us-east-2 (secondary). The existing failover mechanism uses Route 53 failover records with health checks that directly probe the application's `/actuator/health` endpoint.

This approach has a critical flaw: Route 53 health checks are stateless. If the application in us-east-1 goes up, then down, then up, then down, Route 53 will flip traffic back and forth between regions on every state change. This "flip-flop" behavior is worse than being down in one region because it causes partial failures across both regions, risks Aurora split-brain scenarios, invalidates caches, and breaks in-flight transactions.

The Failover Orchestrator solves this by placing an intelligent decision layer between the application's real health and what Route 53 sees, implementing a latch mechanism that ensures automated failover is a one-way trip until an operator explicitly triggers failback.

---

## Solution Overview

The Failover Orchestrator is a Lambda-based system deployed in both us-east-1 and us-east-2 that evaluates multiple health signals across the application stack, publishes a synthetic CloudWatch metric, and uses that metric to control Route 53 routing decisions. The key design principles are:

**Automated failover, manual failback.** Once the system fails over to us-east-2, traffic stays there even if us-east-1 fully recovers. Returning to us-east-1 requires an operator to explicitly invoke the failback Lambda.

**Multi-signal evaluation.** A single health check blip cannot trigger failover. The system evaluates the HTTP health endpoint, ALB healthy host count, ECS running task count, API Gateway error rate, and Aurora cluster status. Multiple signals must agree that the region is unhealthy before failover is considered.

**Dual-scenario coverage.** The system handles both "app goes down but the region is fine" and "the entire region is lost" — two fundamentally different failure modes that require different detection and response mechanisms.

**No Step Functions, no ARC.** The entire system runs on Lambda, EventBridge, CloudWatch, DynamoDB Global Tables, and SNS. Aurora Global Database promotion is performed manually by default — the operator receives CLI commands in the SNS notification and runs them. Optionally, setting `AURORA_AUTO_PROMOTE=true` enables the Lambda to call `SwitchoverGlobalCluster` or `FailoverGlobalCluster` automatically. The promotion reminder handler verifies completion on subsequent cycles regardless of the mode.

---

## Architecture

### Component Layout (Both Regions)

Each region contains an identical set of resources deployed via the same CloudFormation template. The behavior of each resource changes depending on whether the region is currently active or passive.

```
┌─────────────────────────────────────────────────────────────┐
│                    Route 53 (Global)                        │
│                                                             │
│  Failover Record: api.deposits.example.com                  │
│    PRIMARY  → Health Check → CW Alarm (us-east-1)           │
│    SECONDARY → Health Check → CW Alarm (us-east-2)          │
└─────────────────────────────────────────────────────────────┘

┌──────────────────────────────┐   ┌──────────────────────────────┐
│      us-east-1 (Primary)     │   │     us-east-2 (Secondary)    │
│                              │   │                              │
│  EventBridge (1 min)         │   │  EventBridge (1 min)         │
│       │                      │   │       │                      │
│       ▼                      │   │       ▼                      │
│  Orchestrator Lambda         │   │  Orchestrator Lambda         │
│  (VPC-attached)              │   │  (VPC-attached)              │
│       │                      │   │       │                      │
│       ├─ /actuator/health    │   │       ├─ Check active region │
│       ├─ ALB HealthyHosts    │   │       │  metric staleness    │
│       ├─ ECS RunningTasks    │   │       ├─ Evaluate own health │
│       ├─ API GW 5xx          │   │       └─ Publish own metric  │
│       ├─ Aurora Status       │   │                              │
│       └─ Publish CW metric   │   │  Manual Failback Lambda      │
│                              │   │                              │
│  CW Alarm ← CW Metric        │   │  CW Alarm ← CW Metric        │
│                              │   │                              │
│  DynamoDB ◄──── Global ────► │   │  DynamoDB                    │
│             Table Replication│   │                              │
│                              │   │                              │
│  Aurora ◄──── Global ──────► │   │  Aurora                      │
│           DB Replication     │   │                              │
│                              │   │                              │
│  SNS Topic                   │   │  SNS Topic                   │
└──────────────────────────────┘   └──────────────────────────────┘
```

### How Route 53 is Controlled

Route 53 does NOT directly probe the application. Instead, each region has a CloudWatch alarm watching a custom metric called `Custom/RegionFailover/RegionActiveStatus`. This metric is published exclusively by the Orchestrator Lambda.

When the Lambda publishes `RegionActiveStatus = 1.0`, the alarm is in OK state, and Route 53 considers the region healthy.

When the Lambda publishes `RegionActiveStatus = 0.0`, or stops publishing entirely, the alarm enters ALARM state (because `TreatMissingData = breaching`), and Route 53 considers the region unhealthy and routes traffic to the other region.

This indirection gives the orchestrator complete control over routing decisions, including the ability to keep a recovered region marked as unhealthy (the latch).

---

## Anti-Flip-Flop Mechanism

Three layers work together to prevent traffic from bouncing between regions.

### Layer 1: Consecutive Failure Threshold

The orchestrator runs every minute. A single unhealthy evaluation does not trigger failover. The region must be unhealthy for N consecutive evaluations (default: 3, meaning 3 minutes of sustained degradation) before failover is even considered.

During the accumulation phase, the Lambda continues publishing `RegionActiveStatus = 1.0` so Route 53 does not react to transient issues. The team receives WARNING notifications at each step so they can intervene manually if the issue resolves.

If the region becomes healthy at any point during the accumulation, the consecutive failure counter resets to zero.

### Layer 2: Cooldown Window

Even after reaching the consecutive failure threshold, the Lambda checks whether a failover has already occurred within the cooldown window (default: 30 minutes). If it has, the Lambda does NOT trigger another failover. Instead, it continues publishing `RegionActiveStatus = 1.0` and sends a CRITICAL notification indicating that manual intervention may be required.

This prevents rapid successive failovers in scenarios where the secondary region also has issues after the first failover.

### Layer 3: The Latch

Once failover executes, the Lambda sets `latch_engaged = true` in the DynamoDB Global Table. From that point forward, even if the old primary region fully recovers, the Lambda in that region sees the latch and continues publishing `RegionActiveStatus = 0.0`. Route 53 never sees it as healthy again.

The latch can only be released by an operator explicitly invoking the Manual Failback Lambda. This ensures that failback is always a deliberate, validated action — never an automatic reaction to a recovering region.

### Layer 4: Dual-Region Circuit Breaker (v1.0.1+)

Before triggering an automated failover, the orchestrator checks the *target* region's health in the state backend. If the target region is ALSO unhealthy or its heartbeat is stale, the orchestrator aborts the failover and enters a "Dual-Region Outage" state.

In this state, the Lambda stays in the current region and sends a CRITICAL alert. This prevents the "infinite loop" where regions fail back and forth during a global outage (e.g., a bad deployment or widespread dependency failure). Failover only resumes once the target region is confirmed healthy.

---

## Health Signal Evaluation

### Signal Priority

The orchestrator evaluates up to six health signals. They are not all weighted equally.

**HTTP Health Check (`/actuator/health`) — PRIMARY SIGNAL.** This is the most important signal because it tests what actually matters: can a client reach the application and get a response? If this check fails, the region is marked unhealthy regardless of what the infrastructure metrics say. A failing HTTP check means the app is down from the consumer's perspective, period.

The HTTP check calls the configured endpoint on the private ALB. For Spring Boot Actuator, a 200 response with `{"status": "UP"}` means healthy. A 503 response, `{"status": "DOWN"}`, connection timeout, or connection refused all mean unhealthy.

**ALB Healthy Host Count — HIGH PRIORITY.** Checks the `AWS/ApplicationELB/HealthyHostCount` CloudWatch metric. If healthy hosts drop below the minimum threshold (default: 1), it means no containers are passing ALB health checks. With 3 AZs and a minimum of 1 container per AZ, you should normally see 3 healthy hosts.

**ECS Running Task Count — HIGH PRIORITY.** Queries the ECS `DescribeServices` API to check running vs. desired task count. The check passes if running tasks are at least 50% of desired (to allow for rolling deployments). If running tasks drop to zero, the application has no compute.

**API Gateway 5xx Error Rate — MEDIUM PRIORITY.** Checks the `AWS/ApiGateway/5XXError` metric relative to total request count. If the 5xx rate exceeds the threshold (default: 50%), it indicates the API Gateway layer is unable to serve most requests. If there is no traffic in the evaluation window, this signal is assumed healthy (no traffic means no errors, and the HTTP check is a better indicator).

**Aurora Cluster Status — HIGH PRIORITY.** Queries `DescribeDBClusters` to verify the local Aurora cluster has status `available`. Any other status (e.g., `failing-over`, `maintenance`, `stopped`) is unhealthy.

**ElastiCache Replication Group Status — HIGH PRIORITY (optional).** Queries `DescribeReplicationGroups` to verify the local ElastiCache replication group has status `available`. This signal is only evaluated when `ELASTICACHE_REPLICATION_GROUP_ID` is set; when the env var is empty, the signal is skipped entirely and does not count toward the quorum. Any status other than `available` (e.g., `modifying`, `snapshotting`, `deleting`) causes the signal to fail.

### Decision Logic

If the HTTP health check is configured and it fails, the region is immediately considered unhealthy. The HTTP check failure alone is sufficient — it does not require corroboration from infrastructure metrics.

If the HTTP health check passes (or is not yet configured), the infrastructure metrics are evaluated using a quorum model. The region is unhealthy only if at least 50% of the configured infrastructure signals agree it is unhealthy. This means a single infrastructure metric blipping will not trigger failover as long as the app is still responding to HTTP health checks.

---

## Failure Scenarios

### Scenario 1: Application Goes Down, Region is Fine

This covers situations like a bad deployment, application crash, database connection pool exhaustion, or a dependency failure that causes the Spring Boot actuator to report DOWN.

**What happens:**

1. EventBridge in us-east-1 fires every minute, invoking the Orchestrator Lambda.
2. The Lambda calls `GET /actuator/health` on the private ALB. The app returns HTTP 503 or `{"status": "DOWN"}`.
3. The Lambda marks the evaluation as unhealthy and increments the consecutive failure counter in DynamoDB.
4. The Lambda continues publishing `RegionActiveStatus = 1.0` (Route 53 sees no change yet) and sends a WARNING via SNS.
5. This repeats for each minute the app is down.
6. On the third consecutive failure (minute 3), the threshold is reached.
7. The Lambda checks the cooldown window — if no recent failover, it proceeds.
8. The Lambda publishes `RegionActiveStatus = 0.0` for us-east-1. The CloudWatch alarm fires. Route 53 routes traffic to us-east-2.
9. The Lambda updates DynamoDB: `active_region = us-east-2`, `latch_engaged = true`, `aurora_promotion_pending = true`, `state = WAITING_AURORA_PROMOTION`.
10. The Lambda sends an SNS notification with the exact CLI commands needed to promote Aurora in us-east-2. The notification includes both the planned switchover command (try first) and the unplanned failover command (if switchover fails).
11. **The operator receives the email and manually runs the Aurora promotion commands.** Until this step is completed, the app in us-east-2 cannot write to the database.
12. After Aurora is promoted, the orchestrator Lambda automatically detects the promotion within 60 seconds by calling `DescribeDBClusters` and checking the `ReplicationSourceIdentifier` field. When this field is empty, the local cluster is the writer. The Lambda clears `aurora_promotion_pending` and sends a confirmation notification.
13. Subsequent invocations: the Lambda sees the latch and keeps publishing `RegionActiveStatus = 0.0` even if the app in us-east-1 recovers. The reminders stop once Aurora promotion is detected.

**Recovery:** An operator validates that us-east-1 is healthy, then invokes the Manual Failback Lambda.

### Scenario 2: Entire Region Goes Down

This covers an AWS regional outage where EventBridge, Lambda, CloudWatch, and the application are all unavailable in us-east-1.

**What happens:**

1. The Orchestrator Lambda in us-east-1 stops running because the region is down.
2. The `RegionActiveStatus` metric in us-east-1 stops being published.
3. The CloudWatch alarm in us-east-1 is configured with `TreatMissingData = breaching`. After one evaluation period (~1 minute) with no metric data, the alarm enters ALARM state.
4. The Route 53 health check monitoring this alarm sees it go UNHEALTHY.
5. Route 53 fails over traffic to us-east-2 using the SECONDARY failover record.
6. **At this point, traffic is going to us-east-2 but Aurora is still a read-only replica.** The passive region Lambda detects this and sends the operator the Aurora promotion commands.
7. The Orchestrator Lambda in us-east-2 runs on its next minute-interval invocation.
8. It reads state from DynamoDB (the Global Table replica is in us-east-2, so it's accessible even though us-east-1 is down).
9. It sees that `active_region = us-east-1` and that it is the passive region.
10. It attempts to query the `RegionActiveStatus` metric in us-east-1's CloudWatch. If the region is truly down, this call either returns no data or fails with a connection error.
11. The passive Lambda concludes the active region is lost. The exact detection logic depends on the `STALENESS_DETECTION_MODE` environment variable. The default (`DDB_AND_CLOUDWATCH`) requires both the DynamoDB heartbeat to be old and the cross-region CloudWatch call to fail. `DDB_ONLY` mode will react faster based only on the DynamoDB heartbeat.
12. The passive Lambda updates DynamoDB: `active_region = us-east-2`, `latch_engaged = true`, `aurora_promotion_pending = true`, `state = WAITING_AURORA_PROMOTION`.
13. It sends an SNS notification to the us-east-2 SNS topic with the exact unplanned failover commands (including `--allow-data-loss` since the primary is unreachable).
14. **The operator receives the email and manually runs the Aurora failover commands.** Until this is done, the app in us-east-2 cannot write to the database.
15. After Aurora is promoted, the orchestrator Lambda automatically detects the promotion within 60 seconds (via `DescribeDBClusters` checking `ReplicationSourceIdentifier`) and clears `aurora_promotion_pending`.
16. On subsequent invocations, the Lambda in us-east-2 now sees itself as the active region and begins performing full health evaluations of its own stack.

**Recovery:** When us-east-1 comes back online, its Lambda starts running again. It reads DynamoDB, sees `active_region = us-east-2`, and operates in passive mode (evaluating its own health and publishing its own metric for readiness). The operator invokes the Manual Failback Lambda when ready.

### Scenario 3: Manual Failback to Primary

This is always operator-initiated.

**What happens:**

1. The operator first runs the Aurora switchover manually:
   ```bash
   aws rds switchover-global-cluster \
     --global-cluster-identifier <AURORA_GLOBAL_CLUSTER_ID> \
     --target-db-cluster-identifier <us-east-1-cluster-arn> \
     --region us-east-2
   ```
2. The operator monitors the switchover until us-east-1 shows empty `ReplicationSourceIdentifier` (meaning it is the writer):
   ```bash
   aws rds describe-db-clusters \
     --db-cluster-identifier <AURORA_CLUSTER_ID> \
     --query 'DBClusters[0].{Status:Status,ReplicationSource:ReplicationSourceIdentifier}' \
     --region us-east-1
   ```
3. Once Aurora switchover is confirmed, the operator invokes the Manual Failback Lambda in us-east-2:
   ```bash
   aws lambda invoke \
     --function-name failover-manual-failback-prod \
     --payload '{"target_region": "us-east-1", "skip_health_check": false, "operator": "enrique", "aurora_confirmed": true}' \
     --region us-east-1 \
     response.json
   ```
   Note: `--region us-east-1` is the target region, not us-east-2. The Lambda must run in the target region so it can reach the private ALB for HTTP health validation.
4. The Lambda reads current state from DynamoDB. It validates that `active_region` is NOT already the target (no-op guard).
5. It checks `aurora_confirmed = true` in the payload. If false or missing, the Lambda returns the Aurora switchover commands without doing anything else.
6. Unless `skip_health_check` is true, it validates the target region's health: calls `/actuator/health` on the private ALB to verify the app is responding, queries ECS in us-east-1 to confirm tasks are running at desired count, and verifies Aurora shows us-east-1 as the writer.
7. It updates DynamoDB to `state = FAILBACK_IN_PROGRESS`.
8. It publishes `RegionActiveStatus = 1.0` for BOTH regions (both are healthy, Route 53 PRIMARY takes precedence).
9. It updates DynamoDB: `active_region = us-east-1`, `latch_engaged = false`, `state = PRIMARY_ACTIVE`, `consecutive_failures = 0`, `aurora_promotion_pending = false`.
10. It sends a FAILBACK COMPLETE notification via SNS.
11. The Orchestrator Lambda in us-east-1 now sees itself as the active region and resumes normal health evaluation. The Lambda in us-east-2 reverts to passive mode.

---

## State Machine

The system has four states, tracked in the DynamoDB Global Table.

```
                    ┌──────────────────┐
                    │  PRIMARY_ACTIVE  │ ← Initial / normal state
                    │  (us-east-1)     │
                    └────────┬─────────┘
                             │
                 Auto failover triggered
                 (consecutive failures >= threshold
                  AND cooldown expired AND no latch)
                             │
                             ▼
                    ┌──────────────────────────────┐
                    │  WAITING_AURORA_PROMOTION     │ ← DNS points to us-east-2
                    │  aurora_promotion_pending     │   Aurora not yet promoted
                    │  = true                       │   (reminders every 5 min)
                    └──────────────┬───────────────┘
                                  │
                 Operator runs Aurora CLI commands
                 (or auto-promote if enabled),
                 orchestrator auto-detects promotion
                                  │
                                  ▼
                    ┌──────────────────┐
                    │ SECONDARY_ACTIVE │ ← Auto-transitioned by orchestrator
                    │  (us-east-2)     │   when aurora pending is cleared.
                    │                  │   Latch keeps us-east-1 marked down.
                    └────────┬─────────┘
                             │
                 Operator switchovers Aurora to us-east-1,
                 then invokes failback Lambda in us-east-1
                 with aurora_confirmed=true
                             │
                             ▼
                    ┌───────────────────────┐
                    │ FAILBACK_IN_PROGRESS  │
                    └───────────┬───────────┘
                                │
                  DNS moved back, latch released
                                │
                                ▼
                    ┌──────────────────┐
                    │  PRIMARY_ACTIVE  │ ← Back to normal
                    │  (us-east-1)     │
                    └──────────────────┘
```

### DynamoDB Record Schema

The Global Table stores a single record with partition key `REGION_STATE`:

| Field | Type | Description |
|-------|------|-------------|
| pk | String | Always `REGION_STATE` |
| active_region | String | `us-east-1` or `us-east-2` — which region is currently serving traffic |
| state | String | One of: `PRIMARY_ACTIVE`, `WAITING_AURORA_PROMOTION`, `SECONDARY_ACTIVE`, `FAILBACK_IN_PROGRESS` |
| last_failover_ts | String | ISO 8601 timestamp of the last failover event, used for cooldown calculation |
| cooldown_minutes | Number | Minimum minutes between failovers (default: 30) |
| initiated_by | String | Who triggered the last state change: `INIT`, `AUTO_ACTIVE`, `AUTO_PASSIVE`, `MANUAL` |
| reason | String | Human-readable description of why the last state change occurred |
| latch_engaged | Boolean | If true, the previously-active region is locked out of Route 53 until manual failback |
| consecutive_failures | Number | How many consecutive unhealthy evaluations have occurred (resets to 0 on recovery or failover) |
| last_active_metric_ts | String | ISO 8601 timestamp of the last time the active region Lambda published its metric (used by passive Lambda for staleness detection) |
| aurora_promotion_pending | Boolean | If true, DNS has been moved but Aurora has not been promoted yet. The Lambda sends periodic reminders and automatically clears this flag when it detects the local Aurora cluster has become the writer via `DescribeDBClusters` (checking `ReplicationSourceIdentifier`). |
| redis_promotion_pending | Boolean | If true, ElastiCache Global Datastore failover (promoting the secondary replication group to primary) is in progress. Only set to `True` when `ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID` is configured and `ELASTICACHE_AUTO_PROMOTE=true`. State transitions to `SECONDARY_ACTIVE` only when both `aurora_promotion_pending` and `redis_promotion_pending` are `False`. |
| last_warning_notification_ts | String | ISO 8601 timestamp of the last WARNING-level notification sent. Used for notification throttling to prevent inbox flooding during flapping scenarios. |

---

## Component Inventory

### Resources Deployed Per Region

| Resource | Purpose | Notes |
|----------|---------|-------|
| Orchestrator Lambda | Evaluates health, publishes metric, triggers failover | VPC-attached, runs every 1 min |
| Manual Failback Lambda | Operator-triggered failback to primary | VPC-attached, invoked manually |
| EventBridge Rule | Invokes Orchestrator Lambda on 1-minute schedule | `rate(1 minute)` |
| CloudWatch Alarm | Monitors `RegionActiveStatus` metric | `TreatMissingData: breaching` |
| Route 53 Health Check | Backed by the CW Alarm, consumed by failover records | Type: `CLOUDWATCH_METRIC` |
| SNS Topic | Sends email notifications for all failover events | One subscription per region |
| CloudWatch Dashboard | Visual overview of region health and metrics | Per-region dashboard |
| IAM Role | Shared by both Lambdas | Scoped to required resources; describe-only for RDS by default. If `AURORA_AUTO_PROMOTE=true`, add `rds:SwitchoverGlobalCluster` and `rds:FailoverGlobalCluster` |

### Resources Deployed Once (Global or Primary Only)

| Resource | Purpose | Notes |
|----------|---------|-------|
| DynamoDB Global Table | Stores failover state, replicated across regions | Created in primary, replica added to secondary |
| Route 53 Failover Records | PRIMARY and SECONDARY DNS records | Route 53 is global; records defined in primary stack |

---

## Deployment Guide

### Prerequisites

Before deploying, you need the following information from your existing infrastructure in each region.

**Per Region:**
- VPC ID where your ALB and ECS tasks run
- At least 2 private subnet IDs in different AZs (for Lambda VPC attachment)
- A security group ID for the Lambda (outbound to ALB SG on port 80/443, outbound to 0.0.0.0/0 on 443 for AWS APIs)
- Internal ALB DNS name (e.g., `http://internal-deposits-alb-1234567890.us-east-1.elb.amazonaws.com`)
- ALB ARN suffix (e.g., `app/deposits-alb/50dc6c495c0c9188`)
- Target Group ARN suffix
- ECS Cluster name and Service name
- Private API Gateway ID
- Aurora cluster identifier (regional)
- Routable NLB DNS name and hosted zone ID

**Global:**
- Aurora Global Database cluster identifier
- Route 53 hosted zone ID
- DNS record name (e.g., `api.deposits.example.com`)
- Notification email address

### Step-by-Step Deployment

**Step 1: Deploy CloudFormation in us-east-1.**

```bash
aws cloudformation deploy \
  --template-file failover_cfn_template_v2.yaml \
  --stack-name failover-orchestrator-prod \
  --parameter-overrides \
    Environment=prod \
    VpcId=vpc-0123456789abcdef0 \
    LambdaSubnetIds=subnet-aaa,subnet-bbb \
    LambdaSecurityGroupId=sg-0123456789abcdef0 \
    HealthCheckUrl=http://internal-deposits-alb-1234567890.us-east-1.elb.amazonaws.com \
    HealthEndpoint=/actuator/health \
    AlbArnSuffix=app/deposits-alb/50dc6c495c0c9188 \
    TargetGroupArnSuffix=targetgroup/deposits-tg/abcdef1234567890 \
    EcsClusterName=deposits-cluster \
    EcsServiceName=deposits-service \
    ApiGatewayId=abc123def4 \
    AuroraClusterId=deposits-aurora-use1 \
    AuroraGlobalClusterId=deposits-aurora-global \
    NotificationEmail=deposits-oncall@chase.com \
    Route53HostedZoneId=Z0123456789ABCDEFGHIJ \
    Route53RecordName=api.deposits.example.com \
    PrimaryNlbDnsName=deposits-nlb-use1-abcdef.elb.us-east-1.amazonaws.com \
    PrimaryNlbHostedZoneId=Z26RNL4JYFTOTI \
  --capabilities CAPABILITY_NAMED_IAM \
  --region us-east-1
```

**Step 2: Deploy CloudFormation in us-east-2.**

Use the same template but with us-east-2 resource identifiers.

```bash
aws cloudformation deploy \
  --template-file failover_cfn_template_v2.yaml \
  --stack-name failover-orchestrator-prod \
  --parameter-overrides \
    Environment=prod \
    VpcId=vpc-0987654321fedcba0 \
    LambdaSubnetIds=subnet-ccc,subnet-ddd \
    LambdaSecurityGroupId=sg-0987654321fedcba0 \
    HealthCheckUrl=http://internal-deposits-alb-9876543210.us-east-2.elb.amazonaws.com \
    HealthEndpoint=/actuator/health \
    AlbArnSuffix=app/deposits-alb-use2/abcdef1234567890 \
    TargetGroupArnSuffix=targetgroup/deposits-tg-use2/1234567890abcdef \
    EcsClusterName=deposits-cluster \
    EcsServiceName=deposits-service \
    ApiGatewayId=xyz789ghi0 \
    AuroraClusterId=deposits-aurora-use2 \
    AuroraGlobalClusterId=deposits-aurora-global \
    NotificationEmail=deposits-oncall@chase.com \
    Route53HostedZoneId=Z0123456789ABCDEFGHIJ \
    Route53RecordName=api.deposits.example.com \
    SecondaryNlbDnsName=deposits-nlb-use2-ghijkl.elb.us-east-2.amazonaws.com \
    SecondaryNlbHostedZoneId=ZLMOA37VPKANP \
  --capabilities CAPABILITY_NAMED_IAM \
  --region us-east-2
```

**Step 3: Create DynamoDB Global Table replica.**

```bash
aws dynamodb update-table \
  --table-name failover-state \
  --replica-updates 'Create={RegionName=us-east-2}' \
  --region us-east-1
```

Wait for the replica to become ACTIVE:

```bash
aws dynamodb describe-table --table-name failover-state --region us-east-1 \
  --query 'Table.Replicas'
```

**Step 4: Get the us-east-2 Health Check ID and wire up the secondary failover record.**

```bash
aws cloudformation describe-stacks \
  --stack-name failover-orchestrator-prod \
  --region us-east-2 \
  --query 'Stacks[0].Outputs[?OutputKey==`HealthCheckId`].OutputValue' \
  --output text
```

Take this Health Check ID and either uncomment the `SecondaryFailoverRecord` in the CloudFormation template (replacing the placeholder) and redeploy in us-east-1, or create the record manually:

```bash
aws route53 change-resource-record-sets \
  --hosted-zone-id Z0123456789ABCDEFGHIJ \
  --change-batch '{
    "Changes": [{
      "Action": "UPSERT",
      "ResourceRecordSet": {
        "Name": "api.deposits.example.com",
        "Type": "A",
        "SetIdentifier": "secondary",
        "Failover": "SECONDARY",
        "HealthCheckId": "<PASTE_US_EAST_2_HEALTH_CHECK_ID>",
        "AliasTarget": {
          "DNSName": "deposits-nlb-use2-ghijkl.elb.us-east-2.amazonaws.com",
          "HostedZoneId": "ZLMOA37VPKANP",
          "EvaluateTargetHealth": false
        }
      }
    }]
  }'
```

**Step 5: Deploy the actual Lambda code.**

The CloudFormation template creates the Lambdas with placeholder code. Deploy the real code:

```bash
# Package orchestrator
zip failover_orchestrator_v3.zip failover_orchestrator_v3.py

# Deploy to both regions
aws lambda update-function-code \
  --function-name failover-orchestrator-prod \
  --zip-file fileb://failover_orchestrator_v3.zip \
  --region us-east-1

aws lambda update-function-code \
  --function-name failover-orchestrator-prod \
  --zip-file fileb://failover_orchestrator_v3.zip \
  --region us-east-2

# Package failback
zip manual_failback_v2.zip manual_failback_v2.py

# Deploy to both regions
aws lambda update-function-code \
  --function-name failover-manual-failback-prod \
  --zip-file fileb://manual_failback_v2.zip \
  --region us-east-1

aws lambda update-function-code \
  --function-name failover-manual-failback-prod \
  --zip-file fileb://manual_failback_v2.zip \
  --region us-east-2
```

**Step 6: Confirm SNS subscription.**

Check the notification email inbox and confirm the subscription link for both regions.

**Step 7: Seed initial state.**

The Orchestrator Lambda auto-creates the initial `PRIMARY_ACTIVE` state on its first invocation. Within 1 minute of deployment, EventBridge will trigger the Lambda and the state will be initialized. Verify:

```bash
aws dynamodb get-item \
  --table-name failover-state \
  --key '{"pk": {"S": "REGION_STATE"}}' \
  --region us-east-1
```

**Step 8: Remove old Route 53 health checks.**

Once you've verified the new system is publishing metrics and the Route 53 failover records are using the new CloudWatch-alarm-backed health checks, delete the old health checks that directly probed `/actuator/health`.

---

## Configuration Reference

### Environment Variables — Orchestrator Lambda

| Variable | Default | Description |
|----------|---------|-------------|
| `PRIMARY_REGION` | `us-east-1` | The primary region |
| `SECONDARY_REGION` | `us-east-2` | The secondary region |
| `APP_NAME` | (empty) | Application name prepended to all SNS subjects as `[APP_NAME]`. Set this to identify which app is alerting when deploying across multiple applications. |
| `STATE_TABLE` | `failover-state` | DynamoDB Global Table name |
| `SNS_TOPIC_ARN` | (required) | SNS topic for notifications |
| `CW_NAMESPACE` | `Custom/RegionFailover` | CloudWatch namespace for synthetic metric |
| `CW_METRIC_NAME` | `RegionActiveStatus` | CloudWatch metric name |
| `FAILBACK_FUNCTION_NAME` | `failover-manual-failback` | Name of the failback Lambda function (used in SNS notification commands) |
| `HEALTH_CHECK_URL` | (required) | Internal ALB/NLB URL, e.g., `http://internal-my-alb.us-east-1.elb.amazonaws.com` |
| `HEALTH_ENDPOINT` | `/actuator/health` | Health endpoint path (change to `/actuator/deep-health` when ready) |
| `HEALTH_CHECK_TIMEOUT_SECONDS` | `5` | HTTP request timeout |
| `HEALTH_CHECK_DISABLE_SSL_VERIFY` | `false` | Set to `true` to skip SSL certificate verification. Required when ALB uses self-signed or internal CA certificates. |
| `ALB_ARN_SUFFIX` | (optional) | ALB ARN suffix for CW metrics |
| `TG_ARN_SUFFIX` | (optional) | Target Group ARN suffix |
| `ECS_CLUSTER_NAME` | (optional) | ECS cluster name |
| `ECS_SERVICE_NAME` | (optional) | ECS service name |
| `API_GW_NAME` | (optional) | Private API Gateway ID |
| `AURORA_CLUSTER_ID` | (required) | Aurora cluster ID in this region (same identifier in both regions) |
| `AURORA_GLOBAL_CLUSTER_ID` | (required) | Aurora Global Database cluster ID |
| `AURORA_AUTO_PROMOTE` | `false` | Set to `true` to automatically call `SwitchoverGlobalCluster` or `FailoverGlobalCluster` during failover. When `false`, operator receives CLI commands via SNS. See [Automated Aurora Promotion](#automated-aurora-promotion). |
| `AURORA_PROMOTION_STRATEGY` | `SWITCHOVER_THEN_FAILOVER` | Sets the auto-promotion method. Use `FAILOVER_ONLY` if your IAM policy denies `rds:SwitchoverGlobalCluster`. |
| `AURORA_MAX_REPLICATION_LAG_SECONDS` | `5` | If `AURORA_AUTO_PROMOTE` is true, this is the maximum replication lag in seconds allowed before an automated promotion is attempted. Prevents promotion if the secondary is too far behind. |
| `FAILOVER_MODE` | `auto` | `auto` = full automated failover. `manual` = detect and notify only, operator must run `execute_failover` command. `parked` = Lambda exits immediately without evaluating health or reading state — use during staged deployments to keep the orchestrator dormant while deploying new code. Switch to `auto` once deployment is verified. |
| `COOLDOWN_MINUTES` | `30` | Minimum minutes between automated failovers |
| `CONSECUTIVE_FAILURES_THRESHOLD` | `3` | Consecutive unhealthy evaluations before failover |
| `HEALTH_EVALUATION_WINDOW_MINUTES` | `5` | CloudWatch metric evaluation window |
| `MIN_HEALTHY_HOST_COUNT` | `1` | Minimum ALB healthy hosts |
| `API_GW_5XX_THRESHOLD_PERCENT` | `50` | Max API GW 5xx error rate before unhealthy |
| `ACTIVE_REGION_STALE_THRESHOLD_MINUTES` | `3` | How long the passive region waits before declaring the active region lost |
| `STALENESS_DETECTION_MODE` | `DDB_AND_CLOUDWATCH` | Configures the logic for passive region staleness detection. `DDB_AND_CLOUDWATCH` (default) is safest, requiring two signals. `DDB_ONLY` is faster but relies only on the DynamoDB heartbeat. |
| `AURORA_PROMOTION_REMINDER_INTERVAL_MINUTES` | `5` | How often the Lambda sends reminder notifications while Aurora promotion is pending |
| `WARNING_NOTIFICATION_COOLDOWN_MINUTES` | `10` | Minimum minutes between WARNING-level notifications. First alert always sends immediately. CRITICAL one-time events are never throttled. |

### Environment Variables — Failback Lambda

The failback Lambda shares most config with the orchestrator. These are all required:

| Variable | Description |
|----------|-------------|
| `PRIMARY_REGION`, `SECONDARY_REGION` | Same as orchestrator |
| `STATE_TABLE`, `SNS_TOPIC_ARN`, `CW_NAMESPACE`, `CW_METRIC_NAME` | Same as orchestrator |
| `AURORA_CLUSTER_ID`, `AURORA_GLOBAL_CLUSTER_ID` | Same as orchestrator |
| `APP_NAME` | Same as orchestrator |
| `HEALTH_CHECK_URL` | Region-specific internal ALB URL (must point to the local ALB, not Route 53) |
| `HEALTH_ENDPOINT`, `HEALTH_CHECK_TIMEOUT_SECONDS`, `HEALTH_CHECK_DISABLE_SSL_VERIFY` | Same as orchestrator |
| `ECS_CLUSTER_NAME`, `ECS_SERVICE_NAME` | Same as orchestrator |

### Tuning Recommendations

**Consecutive failure threshold:** 3 minutes is a good balance for a business-critical app. Setting it lower (e.g., 1-2) risks false positives during transient issues. Setting it higher (e.g., 5+) means longer downtime before failover.

**Cooldown:** 30 minutes is recommended to prevent cascading failovers and give the team time to assess. If your Aurora switchover takes 15-20 minutes to fully propagate, the cooldown should be at least that long.

**Stale threshold for passive region:** 3 minutes accounts for the 1-minute EventBridge interval plus potential CloudWatch metric publication delay plus one buffer cycle. Setting this lower risks false positive region-down detection.

---

## Networking and VPC Requirements

### Why the Lambda Must Be VPC-Attached

The Lambda needs to call `/actuator/health` on the private ALB. Since the ALB is in a private subnet with no public IP, the Lambda must be in the same VPC to reach it over the private network.

### Subnet Requirements

The Lambda subnets must have a route to the internet via a NAT Gateway (or VPC endpoints) for the following AWS API calls:

- DynamoDB (state table operations)
- CloudWatch (read metrics, publish custom metric)
- SNS (send notifications)
- ECS (describe services)
- RDS (describe clusters for health checks)

If your organization uses VPC endpoints for these services, the Lambda subnets need routes to those endpoints instead. The required VPC endpoint services are:

- `com.amazonaws.<region>.dynamodb`
- `com.amazonaws.<region>.monitoring` (CloudWatch)
- `com.amazonaws.<region>.sns`
- `com.amazonaws.<region>.ecs`
- `com.amazonaws.<region>.rds`

### Security Group Requirements

The Lambda security group needs:

**Outbound rules:**
- TCP 80 and/or 443 to the ALB security group (for `/actuator/health`)
- TCP 443 to `0.0.0.0/0` (for AWS API calls via NAT) — OR to VPC endpoint security groups if using endpoints

The ALB security group needs an **inbound rule** allowing traffic from the Lambda security group on the health check port (typically 80 or 443).

---

## Operational Runbook

### Performing a Manual Failover (Planned)

If you need to proactively move traffic to us-east-2 (e.g., for maintenance in us-east-1), the process mirrors failback but in reverse:

1. Switchover Aurora to us-east-2 first:
   ```bash
   aws rds switchover-global-cluster \
     --global-cluster-identifier <AURORA_GLOBAL_CLUSTER_ID> \
     --target-db-cluster-identifier <us-east-2-cluster-arn> \
     --region us-east-1
   ```

2. Wait until us-east-2 shows empty `ReplicationSourceIdentifier` (writer).

3. Invoke the failback Lambda in the target region (us-east-2):
   ```bash
   aws lambda invoke \
     --function-name failover-manual-failback-prod \
     --payload '{"target_region": "us-east-2", "skip_health_check": false, "operator": "enrique", "aurora_confirmed": true}' \
     --region us-east-2 \
     response.json
   ```

Note: The failback Lambda works bidirectionally — it moves traffic TO whatever `target_region` you specify. Always invoke it in the target region.

### Performing a Manual Failback

Failback is a three-step process when ElastiCache is configured: check ElastiCache readiness, promote Aurora, then move DNS.

**Step 0 (if ElastiCache is configured): Verify ElastiCache Global Datastore before failback.**

Confirm the current PRIMARY member is the secondary region and us-east-1 is SECONDARY — this is the state after failover:

```bash
aws elasticache describe-global-replication-groups \
  --global-replication-group-id <GLOBAL_RG_ID> \
  --show-member-info --region us-east-1
# Expect: us-east-2 member = PRIMARY, us-east-1 member = SECONDARY, both available
```

The failback Lambda automatically calls `failover_global_replication_group` to promote us-east-1's replication group back to PRIMARY during failback. This is handled by the Lambda if `ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID` is set — no manual ElastiCache API call is needed.

**Step 1: Switchover Aurora back to us-east-1:**

```bash
aws rds switchover-global-cluster \
  --global-cluster-identifier <AURORA_GLOBAL_CLUSTER_ID> \
  --target-db-cluster-identifier <us-east-1-cluster-arn> \
  --region us-east-2
```

Monitor until us-east-1 shows empty `ReplicationSourceIdentifier` (writer):

```bash
aws rds describe-db-clusters \
  --db-cluster-identifier <AURORA_CLUSTER_ID> \
  --query 'DBClusters[0].{Status:Status,ReplicationSource:ReplicationSourceIdentifier}' \
  --region us-east-1
```

**Step 2: Invoke the failback Lambda IN THE TARGET REGION (only after Aurora is confirmed):**

```bash
aws lambda invoke \
  --function-name failover-manual-failback-prod \
  --payload '{"target_region": "us-east-1", "skip_health_check": false, "operator": "enrique", "aurora_confirmed": true}' \
  --region us-east-1 \
  response.json

cat response.json
```

Note: `--region us-east-1` is intentional — the Lambda must run in the target region so it can reach the private ALB to verify the app is responding on `/actuator/health`.

If you invoke without `aurora_confirmed: true`, the Lambda returns the Aurora switchover commands without making any changes — useful if you need a reminder of the commands.

If the target region's health check fails (HTTP, ECS, or Aurora writer), the Lambda returns an error with specific issues. To override (use with caution):

```bash
aws lambda invoke \
  --function-name failover-manual-failback-prod \
  --payload '{"target_region": "us-east-1", "skip_health_check": true, "operator": "enrique", "aurora_confirmed": true}' \
  --region us-east-1 \
  response.json
```

### Checking Current State

```bash
aws dynamodb get-item \
  --table-name failover-state \
  --key '{"pk": {"S": "REGION_STATE"}}' \
  --region us-east-1 \
  --query 'Item' \
  --output json
```

Key fields to look at: `active_region`, `state`, `latch_engaged`, `consecutive_failures`.

### Manually Resetting the Latch (Emergency)

If you need to release the latch without running the full failback flow (emergency only):

```bash
aws dynamodb update-item \
  --table-name failover-state \
  --key '{"pk": {"S": "REGION_STATE"}}' \
  --update-expression "SET #l = :v, #c = :z" \
  --expression-attribute-names '{"#l": "latch_engaged", "#c": "consecutive_failures"}' \
  --expression-attribute-values '{":v": {"BOOL": false}, ":z": {"N": "0"}}' \
  --region us-east-1
```

**Warning:** This does NOT switchover Aurora. Only use this if Aurora has already been manually switched and you just need to reset the orchestrator's state.

### Running a Preflight Check (v1.4+)

Invoke the orchestrator with `{"preflight": true}` to validate connectivity to all configured services — state backend, CloudWatch, SNS, ElastiCache — without triggering any health evaluation or failover logic. Returns `ready: true/false` with per-check results.

Run this after any infrastructure change (new Lambda code, CFN stack update, ElastiCache deployment) before switching `FAILOVER_MODE` from `parked` to `auto`:

```bash
aws lambda invoke \
  --function-name <orchestrator-function-name> \
  --payload '{"preflight": true}' \
  --cli-binary-format raw-in-base64-out \
  --region us-east-1 \
  /dev/stdout
# Expect: {"ready": true, "checks": {"state_backend": "PASS", "cloudwatch": "PASS", "sns": "PASS", "elasticache": "CONFIGURED"}}
```

If any check returns `FAIL`, resolve the issue before enabling auto mode.

### Verifying Health Signal Evaluation

To test what the orchestrator sees without triggering a real failover, check the Lambda's CloudWatch Logs:

```bash
aws logs tail /aws/lambda/failover-orchestrator-prod \
  --region us-east-1 \
  --since 5m \
  --format short
```

Look for lines containing `Health evaluation:` — they include the full JSON of all signal results.

---

## Automated Aurora Promotion

By default (`AURORA_AUTO_PROMOTE=false`), Aurora promotion is manual. The operator receives an SNS notification with copy-paste CLI commands for `switchover-global-cluster` or `failover-global-cluster` and runs them. The orchestrator's promotion reminder handler verifies completion by checking `DescribeDBClusters` every minute.

When `AURORA_AUTO_PROMOTE=true`, the orchestrator calls the RDS API automatically during failover.

### Guarded Promotion: Pre-flight Checks
To make automated promotion safer, the orchestrator performs several "pre-flight checks" before attempting a switchover or failover. If any of these checks fail, the promotion is aborted and the system falls back to sending a manual notification to the operator for that failover event.

1.  **Replication Lag Check:** The Lambda queries the `AuroraGlobalDBReplicationLag` CloudWatch metric for the secondary cluster. It will only proceed if the lag is below the threshold configured by `AURORA_MAX_REPLICATION_LAG_SECONDS` (default: 5 seconds). This prevents data loss by ensuring the secondary is reasonably up-to-date.
2.  **Target Region Health Check:** The Lambda checks the target region's own `RegionActiveStatus` metric. This ensures the target region is reporting itself as healthy and ready to take over traffic.
3.  **Cluster Status Check:** The Lambda performs a cross-region check to verify that the Aurora clusters in *both* the source and target regions are in an `available` state. This prevents attempting a promotion while a cluster is busy with another operation (e.g., a backup or maintenance).

### Strategy Trade-offs and a Note on `FAILOVER_ONLY`

It is critical to understand the difference between the two promotion APIs to correctly configure your strategy.

*   `rds:SwitchoverGlobalCluster` is a **graceful, planned promotion**. It coordinates between the primary and secondary clusters to ensure there is **zero data loss**. This is the ideal method for failing over when the primary cluster is still healthy and reachable, such as during an application-only failure.

*   `rds:FailoverGlobalCluster` is a **forceful, unplanned promotion**. It is designed for disaster recovery when the primary cluster is unreachable. Because it cannot coordinate with the old primary, it carries a small but non-zero **risk of data loss** (typically sub-second, equal to the replication lag).

By setting `AURORA_PROMOTION_STRATEGY` to `FAILOVER_ONLY`, you are instructing the orchestrator to treat every automated promotion as a disaster recovery event. This is a valid and necessary configuration if your organization's security posture prohibits the `rds:SwitchoverGlobalCluster` permission, but you must understand and accept the trade-off: you are prioritizing IAM compliance over a zero-data-loss guarantee for automated, application-level failovers.

### Strategy Trade-offs and a Note on `FAILOVER_ONLY`

It is critical to understand the difference between the two promotion APIs to correctly configure your strategy.

*   `rds:SwitchoverGlobalCluster` is a **graceful, planned promotion**. It coordinates between the primary and secondary clusters to ensure there is **zero data loss**. This is the ideal method for failing over when the primary cluster is still healthy and reachable, such as during an application-only failure.

*   `rds:FailoverGlobalCluster` is a **forceful, unplanned promotion**. It is designed for disaster recovery when the primary cluster is unreachable. Because it cannot coordinate with the old primary, it carries a small but non-zero **risk of data loss** (typically sub-second, equal to the replication lag).

By setting `AURORA_PROMOTION_STRATEGY` to `FAILOVER_ONLY`, you are instructing the orchestrator to treat every automated promotion as a disaster recovery event. This is a valid and necessary configuration if your organization's security posture prohibits the `rds:SwitchoverGlobalCluster` permission, but you must understand and accept the trade-off: you are prioritizing IAM compliance over a zero-data-loss guarantee for all automated failover events.

### Strategy Trade-offs and a Note on `FAILOVER_ONLY`

It is critical to understand the difference between the two promotion APIs to correctly configure your strategy.

*   `rds:SwitchoverGlobalCluster` is a **graceful, planned promotion**. It coordinates between the primary and secondary clusters to ensure there is **zero data loss**. This is the ideal method for failing over when the primary cluster is still healthy and reachable, such as during an application-only failure.

*   `rds:FailoverGlobalCluster` is a **forceful, unplanned promotion**. It is designed for disaster recovery when the primary cluster is unreachable. Because it cannot coordinate with the old primary, it carries a small but non-zero **risk of data loss** (typically sub-second, equal to the replication lag).

By setting `AURORA_PROMOTION_STRATEGY` to `FAILOVER_ONLY`, you are instructing the orchestrator to treat every automated promotion as a disaster recovery event. This is a valid and necessary configuration if your organization's security posture prohibits the `rds:SwitchoverGlobalCluster` permission, but you must understand and accept the trade-off: you are prioritizing IAM compliance over a zero-data-loss guarantee for all automated failover events.

### Behavior by Scenario

The auto-promotion logic depends on the failure scenario and the `AURORA_PROMOTION_STRATEGY` environment variable.

**App failure (region is reachable):**
*   **If `AURORA_PROMOTION_STRATEGY` is `SWITCHOVER_THEN_FAILOVER` (Default):** The orchestrator first attempts a graceful, zero-data-loss promotion using `SwitchoverGlobalCluster`. If this fails (e.g., because the primary cluster is too unhealthy to respond), it falls back to using the more forceful `FailoverGlobalCluster`.
*   **If `AURORA_PROMOTION_STRATEGY` is `FAILOVER_ONLY`:** The orchestrator skips the switchover attempt and immediately calls `FailoverGlobalCluster`. This mode should be used if your organization's IAM policies deny the `rds:SwitchoverGlobalCluster` permission.

**Region failure (region is unreachable):**
*   The orchestrator always goes directly to `FailoverGlobalCluster` with `--allow-data-loss`, as a graceful switchover is not possible.

**Graceful fallback:** If the API call fails for any reason (IAM denied, Aurora API error, cluster in wrong state), the orchestrator falls back to manual mode for that specific failover and sends the operator the standard CLI commands via SNS. DNS still flips regardless. This means enabling `AURORA_AUTO_PROMOTE=true` is always safe.

**Asynchronous verification:** The `switchover_global_cluster` and `failover_global_cluster` APIs return immediately but the actual operation takes 30 seconds to 2 minutes. The `aurora_promotion_pending` flag stays `True` after the API call is initiated. The promotion reminder handler verifies completion on subsequent cycles by calling `DescribeDBClusters` and checking `ReplicationSourceIdentifier`. Only when the target region's cluster shows empty `ReplicationSourceIdentifier` (confirming it is the writer) does the flag clear.

**Additional IAM permissions required (only when AURORA_AUTO_PROMOTE=true):**

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

---

## Aurora Writer Detection

The orchestrator and failback Lambdas use `DescribeDBClusters` to determine which region holds the Aurora writer. The `rds:DescribeGlobalClusters` permission is not used or required.

**How it works:** Call `DescribeDBClusters` with the cluster identifier in the target region. The response includes a `ReplicationSourceIdentifier` field:

- **Empty string** = this cluster is the primary writer
- **Set to another cluster ARN** = this cluster is a secondary reader replicating from the source

The orchestrator's `_check_if_aurora_writer` function creates a regional RDS client for the target region and checks this field. The failback Lambda's health validation (Check 3) uses the same approach.

**ARN construction:** The target cluster ARN is constructed from known values rather than looked up via API: `arn:aws:rds:<target-region>:<account-id>:cluster:<cluster-id>`. The account ID is derived from the `SNS_TOPIC_ARN` environment variable. No additional API call is needed.

---

## Zero-Container Secondary Region

Some applications run with ECS desired count = 0 in the secondary region because the containers consume from Kafka or perform background processing that cannot run against an Aurora read replica. When failover occurs, these containers need to scale up automatically.

**The Lambda cannot call `ecs:UpdateService`** because the organization's IAM policies do not permit this action on Lambda execution roles. Instead, scaling is handled entirely by AWS infrastructure configuration using Application Auto Scaling.

### How It Works

The orchestrator already publishes a CloudWatch metric (`RegionActiveStatus`) and controls a CloudWatch alarm in each region. Application Auto Scaling policies attached to the us-east-1 alarm react to state changes:

```
Normal Operation:
  Orchestrator publishes 1.0 for us-east-1
  -> Alarm: OK -> Auto Scaling: no action -> us-east-2 ECS: 0 tasks

Failover Triggered:
  Orchestrator publishes 0.0 for us-east-1
  -> Alarm: ALARM -> Scale-up policy fires -> us-east-2 ECS: N tasks
  -> Route 53 moves DNS to us-east-2 (parallel, same alarm)

Failback Completed:
  Failback Lambda publishes 1.0 for us-east-1
  -> Alarm: OK -> Scale-down policy fires -> us-east-2 ECS: 0 tasks
```

### Setup Steps

**Step 1: Register ECS service as scalable target:**

```bash
aws application-autoscaling register-scalable-target \
  --service-namespace ecs \
  --resource-id "service/<CLUSTER_NAME>/<SERVICE_NAME>" \
  --scalable-dimension ecs:service:DesiredCount \
  --min-capacity 0 \
  --max-capacity <PRODUCTION_TASK_COUNT> \
  --region us-east-2
```

**Step 2: Create scale-up policy:**

```bash
aws application-autoscaling put-scaling-policy \
  --service-namespace ecs \
  --resource-id "service/<CLUSTER_NAME>/<SERVICE_NAME>" \
  --scalable-dimension ecs:service:DesiredCount \
  --policy-name "failover-scale-up" \
  --policy-type StepScaling \
  --step-scaling-policy-configuration '{
    "AdjustmentType": "ExactCapacity",
    "StepAdjustments": [{"MetricIntervalLowerBound": 0, "ScalingAdjustment": <PRODUCTION_TASK_COUNT>}],
    "Cooldown": 60
  }' \
  --region us-east-2
```

**Step 3: Create scale-down policy:**

```bash
aws application-autoscaling put-scaling-policy \
  --service-namespace ecs \
  --resource-id "service/<CLUSTER_NAME>/<SERVICE_NAME>" \
  --scalable-dimension ecs:service:DesiredCount \
  --policy-name "failover-scale-down" \
  --policy-type StepScaling \
  --step-scaling-policy-configuration '{
    "AdjustmentType": "ExactCapacity",
    "StepAdjustments": [{"MetricIntervalUpperBound": 0, "ScalingAdjustment": 0}],
    "Cooldown": 60
  }' \
  --region us-east-2
```

**Step 4: Attach policies to the CloudWatch alarm as actions:**

```bash
aws cloudwatch put-metric-alarm \
  --alarm-name "<alarm-name>" \
  --alarm-actions "<SCALE_UP_POLICY_ARN>" \
  --ok-actions "<SCALE_DOWN_POLICY_ARN>" \
  ... (rest of existing alarm config) \
  --region us-east-1
```

### Kafka Consumer Behavior During Scale-Up

During the window between ECS scale-up and Aurora promotion, containers are running and consuming from Kafka. Writes to Aurora (read replica) will fail. The behavior depends on the application: if the consumer retries on write failure without committing the offset, messages reprocess safely after Aurora promotion. If the consumer commits the offset before the write succeeds, those messages are lost. Verify this behavior with the application team.

### Timing

Total time from failure detection to containers serving traffic is approximately 2-3 minutes (alarm transition ~60s + Auto Scaling reaction ~10-30s + Fargate provisioning ~30-60s + ALB health check ~30s). DNS change happens in parallel.

### No Lambda Code Changes

The orchestrator and failback Lambdas do not need any modification for this feature. The scaling behavior is entirely controlled by Application Auto Scaling policies attached to the CloudWatch alarm.

For the full step-by-step setup guide with verification commands and rollback instructions, see `secondary_ecs_autoscaling_guide.md`.

---

## Multi-App Deployment Strategy

The recommended approach is **one set of resources per app** (isolated). Each app gets its own orchestrator Lambda, failback Lambda, DynamoDB table, CloudWatch alarms, Route 53 health checks, and EventBridge rules. The Python code is identical across all apps — only the environment variables change.

**Why isolated over shared:**

- **Blast radius:** A bug, timeout, or IAM change affects only one app. A shared orchestrator failure would take down failover coverage for all apps simultaneously.
- **Independent lifecycle:** Each team deploys, configures thresholds, and switches between manual/auto mode independently. One team can test aggressively while another runs steady-state production.
- **Different thresholds:** Some apps need 5 consecutive failures, others need 2. Some have 60-minute cooldowns. With isolated resources, each app's config is independent.
- **Lambda concurrency:** Each app's health check runs independently. A slow ALB in one app doesn't delay evaluation for others.
- **Debugging:** Separate CloudWatch log groups per app. When MCC's failover fires at 3am, you look at MCC's logs, not logs for 10 apps interleaved.
- **Cost is negligible:** Each additional app adds ~$5/month (Lambda invocations, DynamoDB on-demand single-row, CloudWatch alarms at $0.10/month each).

**How to deploy for a new app:** Create a reusable IaC module that takes app-specific inputs (app name, ECS cluster/service names, ALB/TG ARNs, health check URL, SNS topic ARN). The module creates all resources with consistent naming (`{app}-orchestrator`, `{app}-failback`, `{app}-failover-state`). The same Python zip file is used for every app's Lambda — only the environment variables differ.

---

## Operator CLI Tool

The `failover_cli.py` script provides a live health monitor with region detection, an interactive operations menu, and failure simulation capability.

### Features

- **Live health monitor:** Pings the application URL every 2 seconds showing UP/DOWN status, latency, and HTTP response.
- **DNS-based region detection:** Resolves NLB hostnames at startup to build IP-to-region mappings. On each ping, resolves the app hostname and matches IPs to show which region is serving traffic (`[us-east-1]` or `[us-east-2]`).
- **Region switch detection:** When DNS changes during failover, displays a `>>> REGION SWITCH: us-east-1 -> us-east-2` banner.
- **Failure simulation:** Continuously de-registers targets from the target group every 5 seconds to simulate application failure. ECS re-registers targets automatically when simulation stops.
- **Operations menu:** View state, execute failover, failback, reset state, start/stop failure simulation, show stats.

### Usage

```bash
python3 failover_cli.py
```

Press Enter at any time to open the operations menu. Press `h` for step-by-step demo instructions.

### Configuration

Edit the constants at the top of the file: `PRIMARY_REGION`, `SECONDARY_REGION`, `ORCHESTRATOR_FUNCTION_NAME`, `FAILBACK_FUNCTION_NAME`, `STATE_TABLE`, `MONITOR_URL`, `NLB_PRIMARY_HOSTNAME`, `NLB_SECONDARY_HOSTNAME`, `FAILURE_SIM_TARGET_GROUP_NAME`.

---

## Regression Testing

The `failover_tests.py` script provides automated regression testing across all failover scenarios.

### Tests

| Test | Description |
|------|-------------|
| 0 | SNS email delivery (manual console test, instructions provided) |
| 1 | Steady state validation: DDB state, app health, CW metrics, alarms, Lambda execution |
| 2 | Manual failover: execute_failover, state change, duplicate protection, wrong-region guard |
| 3 | Failback Lambda: aurora_confirmed guard, skip_health_check, state restoration, no-op |
| 4 | Health detection: de-register targets, wait for orchestrator detection, validate failure count |
| 5 | Full lifecycle: failover -> app reachable via us-east-2 -> failback -> app reachable via us-east-1 |
| 6 | Latch enforcement: verify us-east-1 publishes 0.0 and us-east-2 publishes 1.0 after failover |

### Usage

```bash
python3 failover_tests.py              # Run all tests (1-6)
python3 failover_tests.py --test 1     # Run a specific test
python3 failover_tests.py --test 5     # Full lifecycle only
python3 failover_tests.py --list       # List all tests
```

### Permissions

The test script only uses APIs available to limited IAM roles: DynamoDB `GetItem` (read state), CloudWatch `DescribeAlarms` and `GetMetricStatistics` (read), ELBv2 `DescribeTargetGroups`, `DescribeTargetHealth`, `DeregisterTargets` (failure sim), and Lambda `Invoke` (orchestrator + failback). No direct SNS publish, DynamoDB writes, or CloudWatch PutMetricData.

### Suite Mode vs Standalone

When running all tests (`python3 failover_tests.py`), the suite resets state once at the beginning and chains tests — saving approximately 6 minutes compared to each test resetting individually. When running a specific test (`--test N`), it resets independently to guarantee clean state.

---

## IAM Permissions Reference

### Required Permissions (Both Lambdas)

```
AWSLambdaVPCAccessExecutionRole (managed policy)
dynamodb:GetItem, dynamodb:PutItem, dynamodb:UpdateItem
cloudwatch:GetMetricStatistics, cloudwatch:GetMetricData, cloudwatch:PutMetricData
ecs:DescribeServices
rds:DescribeDBClusters
sns:Publish
```

### Conditional Permissions

```
kms:GenerateDataKey, kms:Decrypt                      — Only if SNS topic is KMS-encrypted
rds:FailoverGlobalCluster                             — Only if AURORA_AUTO_PROMOTE=true
rds:SwitchoverGlobalCluster                           — Only if AURORA_AUTO_PROMOTE=true and AURORA_PROMOTION_STRATEGY is SWITCHOVER_THEN_FAILOVER
elasticache:DescribeReplicationGroups                 — Only if ELASTICACHE_REPLICATION_GROUP_ID is set (health signal 6)
elasticache:DescribeGlobalReplicationGroups           — Only if ELASTICACHE_GLOBAL_REPLICATION_GROUP_ID is set (primary detection)
elasticache:FailoverGlobalReplicationGroup            — Only if ELASTICACHE_AUTO_PROMOTE=true (auto-promote on failover)
```

Note: When using guarded auto-promotion, the Lambda role will also need `cloudwatch:GetMetricStatistics` and `rds:DescribeDBClusters` permissions that can access resources in the secondary region to perform the pre-flight safety checks.

### Not Required

```
rds:DescribeGlobalClusters                — Not used; restricted by org policy
ecs:UpdateService                         — Not used (handled by Application Auto Scaling)
```

---

## Migrating to Deep Health Check

When your development team has the deep health check endpoint ready, the migration is a single environment variable change per region.

### What Changes

| Before | After |
|--------|-------|
| `HEALTH_ENDPOINT=/actuator/health` | `HEALTH_ENDPOINT=/actuator/deep-health` |

### What the Deep Health Check Should Include

For the deep health check to be maximally useful for failover decisions, it should verify:

- Application can connect to Aurora PostgreSQL and execute a lightweight query (e.g., `SELECT 1`)
- Application can connect to any critical caches (ElastiCache, etc.)
- Application can reach critical internal dependencies (other microservices, message queues)
- Application's connection pool is not exhausted

The deep health check should return `{"status": "UP"}` with HTTP 200 when all dependencies are reachable, and `{"status": "DOWN", "components": {...}}` with HTTP 503 when any critical dependency fails.

### Migration Steps

1. Deploy the deep health endpoint to your application in both regions.
2. Validate it returns correct responses: `curl http://<alb-dns>/actuator/deep-health`
3. Update the Lambda environment variable in us-east-1:
   ```bash
   aws lambda update-function-configuration \
     --function-name failover-orchestrator-prod \
     --environment "Variables={...,HEALTH_ENDPOINT=/actuator/deep-health,...}" \
     --region us-east-1
   ```
4. Monitor for 24 hours. Check CloudWatch Logs to verify the Lambda is calling the new endpoint and getting expected responses.
5. Repeat for us-east-2.

### Impact on Failover Behavior

Once the deep health check is active, the orchestrator gains much better visibility. For example, if Aurora becomes unreachable from the application (but the RDS API still reports the cluster as `available`), the deep health check will catch this immediately via the database connectivity test, whereas the infrastructure-only check would miss it.

This also means you could potentially reduce the `CONSECUTIVE_FAILURES_THRESHOLD` from 3 to 2, since the deep health check provides higher-confidence signals with fewer false positives.

---

## Monitoring and Alerting

### CloudWatch Dashboard

Each region deploys a dashboard named `failover-status-<region>-<environment>` with the following widgets:

- **Region Active Status:** Both regions' synthetic metric on a single graph. When a region is active, its line is at 1.0. When it fails over, the line drops to 0.0.
- **ALB Healthy Host Count:** Shows the number of healthy targets. Should normally be 3 (one per AZ).
- **API Gateway Requests and 5XX Errors:** Total requests vs. 5XX errors on the same graph.
- **ECS Running vs. Desired Tasks:** Running task count compared to desired. A gap indicates a problem.
- **Region Health Alarm Status:** Visual alarm state indicator.

### SNS Notifications

The system sends notifications at every stage of the failover lifecycle:

| Notification | When | Severity | Throttled? |
|-------------|------|----------|------------|
| `WARNING: <region> degraded (N/threshold)` | Each consecutive failure below threshold | Warning | Yes (10 min default) |
| `CRITICAL: <region> unhealthy, cooldown active` | Threshold reached but cooldown prevents failover | Critical | Yes (10 min default) |
| `FAILOVER: DNS moved to <region> — PROMOTE AURORA NOW` | DNS failover completed, includes Aurora CLI commands | Critical | No — always sends |
| `REGION FAILURE: DNS moved to <region> — PROMOTE AURORA NOW` | Passive region detected active region loss, includes Aurora CLI commands | Critical | No — always sends |
| `REMINDER: Aurora promotion still pending (Nm)` | Sent every 5 minutes while Aurora promotion is pending | Critical | Own interval (5 min) |
| `Aurora promotion confirmed — <region> is writer` | Orchestrator detected Aurora was promoted by operator | Info | No — always sends |
| `FAILOVER FAILED: <from> -> <to>` | DNS failover errored | Critical | No — always sends |
| `WARNING: Passive region <region> unhealthy` | Standby region not ready for failover | Warning | Yes (10 min default) |
| `FAILBACK COMPLETE: -> <region>` | Manual failback succeeded | Info | No — always sends |
| `FAILBACK BLOCKED: <region> not ready` | Failback target failed health validation or Aurora not promoted | Warning | No — always sends |

WARNING-level notifications are throttled to prevent inbox flooding during flapping scenarios. The first alert always sends immediately so the team knows something is wrong. Subsequent alerts of the same type are suppressed until `WARNING_NOTIFICATION_COOLDOWN_MINUTES` (default: 10) has elapsed. All throttle state is logged to CloudWatch even when the notification is suppressed, so you can always see the full timeline in the logs.

### Recommended CloudWatch Alarms (Additional)

Beyond the built-in `RegionActiveStatus` alarm, consider adding:

- Alarm on the Orchestrator Lambda error rate (`AWS/Lambda/Errors`) — if the Lambda itself is failing, the health evaluation stops.
- Alarm on the Orchestrator Lambda duration (`AWS/Lambda/Duration`) — if the Lambda is timing out (120s), it may not be completing its evaluation.
- Alarm on DynamoDB Global Table replication latency (`AWS/DynamoDB/ReplicationLatency`) — high replication lag could cause the passive region to read stale state.

---

## Frequently Asked Questions

**Q: What happens if both regions are unhealthy at the same time?**
The latch prevents flip-flopping. If us-east-1 fails over to us-east-2 and us-east-2 also degrades, the cooldown window prevents a second failover back. The team receives CRITICAL notifications indicating manual intervention is needed. Traffic stays on us-east-2 until the situation is resolved.

**Q: What if the DynamoDB Global Table replication is lagging?**
The system is designed to be tolerant of small replication delays. The active region writes state, and the passive region reads it. If there's a brief delay, the worst case is the passive region takes one additional cycle (1 minute) to detect a region failure. For the manual failback Lambda, it always reads from the local replica, which should be consistent within seconds.

**Q: Can I change the evaluation frequency from 1 minute?**
Yes, update the EventBridge rule's `ScheduleExpression`. However, 1 minute is the recommended minimum for production. Going faster increases Lambda invocations and CloudWatch API calls. Going slower increases time-to-detect. If you change it, adjust `CONSECUTIVE_FAILURES_THRESHOLD` and `ACTIVE_REGION_STALE_THRESHOLD_MINUTES` proportionally.

**Q: What happens during the gap between DNS failover and manual Aurora promotion?**
During this gap, Route 53 is sending traffic to us-east-2, but the Aurora cluster in us-east-2 is still a read-only replica. Your app will be able to handle read requests but all write operations will fail with read-replica errors. If your `/actuator/health` or future `/actuator/deep-health` checks database connectivity for writes, the health check in us-east-2 will report DOWN. This is expected — the system sends periodic reminders (every 5 minutes by default) until the operator promotes Aurora. Once Aurora is promoted, the orchestrator Lambda detects it automatically within 60 seconds by calling `DescribeDBClusters` and checking `ReplicationSourceIdentifier` and clears the `aurora_promotion_pending` flag — no manual DynamoDB update needed. The key design decision here is that a brief write outage during Aurora promotion is far better than the alternative of flip-flopping between regions.

**Q: Should I enable AURORA_AUTO_PROMOTE?**
If your IAM role allows `rds:SwitchoverGlobalCluster` and `rds:FailoverGlobalCluster`, enabling it reduces the time between DNS failover and Aurora writer availability from "whenever the operator runs the command" to "30-120 seconds." The feature has a graceful fallback — if the API call fails for any reason, the operator still receives the standard manual notification. Start with `false` during initial deployment, then enable once you've validated the manual flow works correctly.

**Q: What happens if my secondary region runs containers at desired count = 0?**
If the containers consume from Kafka or perform background processing, they cannot run against an Aurora read replica. Use the Application Auto Scaling approach documented in [Zero-Container Secondary Region](#zero-container-secondary-region). This uses the existing CloudWatch alarm to trigger scale-up/scale-down policies with no Lambda code changes and no additional IAM permissions on the Lambda role. Application Auto Scaling uses its own service-linked role that already has `ecs:UpdateService`.

**Q: Can I deploy this for multiple apps in the same account?**
Yes. The recommended approach is one isolated set of resources per app. See [Multi-App Deployment Strategy](#multi-app-deployment-strategy). The same Python code is used for every app — only the environment variables differ.

**Q: What happens during an Aurora switchover? Is there downtime?**
Aurora Global Database planned switchover typically completes in under 60 seconds with minimal downtime (usually 1-2 seconds of writer unavailability). Unplanned failover (when the primary is unreachable) can take longer and may involve some data loss depending on replication lag. Your Spring Boot application should have retry logic and connection pool recovery configured.

**Q: Can I test this without actually failing over?**
Yes. You can invoke the orchestrator Lambda manually and review the CloudWatch Logs to see what health signals it evaluates and what decision it would make. The Lambda only triggers failover when the consecutive failure threshold is met, so a single manual invocation during healthy operation will simply log `Region healthy` and publish `RegionActiveStatus = 1.0`.

To do a full dry run of the failover mechanics, deploy the system in a staging environment first and intentionally break the application (e.g., stop the ECS tasks) to observe the behavior end-to-end.

**Q: What if my Lambda can't reach the ALB for the health check?**
If the HTTP health check returns a connection error, it's treated as unhealthy — the same as the app being down. Make sure the Lambda security group, ALB security group, subnet routing, and NACLs are correctly configured before deploying. Test with a simple Lambda that just calls the ALB endpoint to verify connectivity.

**Q: How do I completely disable automated failover temporarily?**
Engage the latch manually without changing the active region:

```bash
aws dynamodb update-item \
  --table-name failover-state \
  --key '{"pk": {"S": "REGION_STATE"}}' \
  --update-expression "SET #l = :v" \
  --expression-attribute-names '{"#l": "latch_engaged"}' \
  --expression-attribute-values '{":v": {"BOOL": true}}' \
  --region us-east-1
```

With the latch engaged, the active region Lambda will keep publishing `RegionActiveStatus = 0.0`, but since Route 53 sees the active region as unhealthy AND the passive region as healthy, traffic will move to the passive region. To prevent this, instead disable the EventBridge rule:

```bash
aws events disable-rule \
  --name failover-orchestrator-schedule-prod \
  --region us-east-1
```

This stops the Lambda from running entirely. The CloudWatch alarm will fire on missing data after 1 minute (`TreatMissingData: breaching`), which would cause Route 53 to fail over. To prevent that, temporarily change the alarm's `TreatMissingData` to `notBreaching` while the rule is disabled.

Re-enable when ready:

```bash
aws events enable-rule \
  --name failover-orchestrator-schedule-prod \
  --region us-east-1
```
