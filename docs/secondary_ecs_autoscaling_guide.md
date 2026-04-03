# Secondary Region ECS Auto-Scaling via Application Auto Scaling

## Deposits 2.0 Failover Orchestrator - Companion Guide

---

## Problem

Some applications in the Domestic Deposits platform run with ECS desired count = 0 in the secondary region (us-east-2). These are typically Kafka consumers or background processors that cannot run against an Aurora read replica because they would consume messages and fail on writes, potentially causing data loss or duplicate processing.

When failover occurs, Route 53 moves DNS to us-east-2, but there are no containers running to serve traffic. The containers need to scale up automatically as part of the failover process.

## Why Not Lambda-Based Scaling

The orchestrator Lambda cannot call `ecs:UpdateService` because the organization's IAM policies do not permit this action on Lambda execution roles. This is a common enterprise guardrail that prevents Lambda functions from modifying infrastructure.

## Solution: Application Auto Scaling + CloudWatch Alarm

The failover orchestrator already publishes a CloudWatch metric (`RegionActiveStatus`) and controls a CloudWatch alarm in each region. Application Auto Scaling can react to these same alarm state changes to scale ECS tasks up and down, with zero Lambda code changes.

## How It Works

```
Normal Operation:
  Orchestrator publishes 1.0 for us-east-1
  -> Alarm: OK
  -> Auto Scaling: no action
  -> us-east-2 ECS: 0 tasks (idle)

Failover Triggered:
  Orchestrator publishes 0.0 for us-east-1
  -> Alarm: ALARM
  -> Alarm action triggers scale-up policy
  -> Auto Scaling sets us-east-2 ECS desired count to N
  -> ECS starts provisioning containers
  -> Route 53 moves DNS to us-east-2 (parallel, same alarm)

Failback Completed:
  Failback Lambda publishes 1.0 for us-east-1
  -> Alarm: OK
  -> OK action triggers scale-down policy
  -> Auto Scaling sets us-east-2 ECS desired count to 0
  -> Containers drain and stop
```

The key insight: the CloudWatch alarm already exists and already transitions between OK and ALARM as part of the failover mechanism. Application Auto Scaling policies can be attached as alarm actions, piggybacking on the same signal with zero additional infrastructure.

## Prerequisites

Before starting, confirm you have:

1. The orchestrator deployed and running in both regions
2. The CloudWatch alarm `mcc-region-active-status-use1` created in us-east-1
3. The alarm correctly watching the `RegionActiveStatus` metric
4. The ECS service name and cluster name in us-east-2
5. The desired production task count for us-east-2

## Step-by-Step Setup

All commands run against us-east-2 (where the ECS service lives) unless noted otherwise.

### Step 1: Register the ECS Service as a Scalable Target

This tells Application Auto Scaling that your ECS service can be scaled between 0 and N tasks.

```bash
aws application-autoscaling register-scalable-target \
  --service-namespace ecs \
  --resource-id "service/<CLUSTER_NAME>/<SERVICE_NAME>" \
  --scalable-dimension ecs:service:DesiredCount \
  --min-capacity 0 \
  --max-capacity <PRODUCTION_TASK_COUNT> \
  --region us-east-2
```

Replace:
- `<CLUSTER_NAME>` with the us-east-2 ECS cluster name (e.g., `ecsf69b-fiftmcce2-v1`)
- `<SERVICE_NAME>` with the us-east-2 ECS service name (e.g., `mcc-data-management-v1`)
- `<PRODUCTION_TASK_COUNT>` with the number of tasks to run during failover (e.g., `2`)

Verify:

```bash
aws application-autoscaling describe-scalable-targets \
  --service-namespace ecs \
  --resource-ids "service/<CLUSTER_NAME>/<SERVICE_NAME>" \
  --region us-east-2
```

### Step 2: Create the Scale-Up Policy

This policy sets the ECS desired count to the production value when triggered.

```bash
aws application-autoscaling put-scaling-policy \
  --service-namespace ecs \
  --resource-id "service/<CLUSTER_NAME>/<SERVICE_NAME>" \
  --scalable-dimension ecs:service:DesiredCount \
  --policy-name "failover-scale-up" \
  --policy-type StepScaling \
  --step-scaling-policy-configuration '{
    "AdjustmentType": "ExactCapacity",
    "StepAdjustments": [
      {
        "MetricIntervalLowerBound": 0,
        "ScalingAdjustment": <PRODUCTION_TASK_COUNT>
      }
    ],
    "Cooldown": 60
  }' \
  --region us-east-2
```

This command returns a response containing the policy ARN. Copy the `PolicyARN` value - you need it for Step 4.

Example response:
```json
{
  "PolicyARN": "arn:aws:autoscaling:us-east-2:433607260168:scalingPolicy:xxxxx:resource/ecs/service/<CLUSTER>/<SERVICE>:policyName/failover-scale-up",
  "Alarms": []
}
```

### Step 3: Create the Scale-Down Policy

This policy sets the ECS desired count back to 0 when the alarm returns to OK (after failback).

```bash
aws application-autoscaling put-scaling-policy \
  --service-namespace ecs \
  --resource-id "service/<CLUSTER_NAME>/<SERVICE_NAME>" \
  --scalable-dimension ecs:service:DesiredCount \
  --policy-name "failover-scale-down" \
  --policy-type StepScaling \
  --step-scaling-policy-configuration '{
    "AdjustmentType": "ExactCapacity",
    "StepAdjustments": [
      {
        "MetricIntervalUpperBound": 0,
        "ScalingAdjustment": 0
      }
    ],
    "Cooldown": 60
  }' \
  --region us-east-2
```

Copy the `PolicyARN` from this response as well.

### Step 4: Attach Policies to the CloudWatch Alarm

Update the us-east-1 alarm to trigger the scaling policies. You need the full existing alarm configuration plus the new actions.

First, get the current alarm config:

```bash
aws cloudwatch describe-alarms \
  --alarm-names "mcc-region-active-status-use1" \
  --region us-east-1 \
  --output json
```

Then update the alarm with the policy ARNs as actions:

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
  --alarm-actions "<SCALE_UP_POLICY_ARN>" \
  --ok-actions "<SCALE_DOWN_POLICY_ARN>" \
  --alarm-description "Monitors RegionActiveStatus. ALARM triggers failover DNS + ECS scale-up. OK triggers ECS scale-down." \
  --region us-east-1
```

Replace:
- `<SCALE_UP_POLICY_ARN>` with the ARN from Step 2
- `<SCALE_DOWN_POLICY_ARN>` with the ARN from Step 3

IMPORTANT: If the alarm already has other actions (like an SNS topic for notifications), include those too. The `--alarm-actions` and `--ok-actions` parameters replace existing actions, they don't append. To keep both:

```bash
  --alarm-actions "<SCALE_UP_POLICY_ARN>" "<EXISTING_SNS_TOPIC_ARN>" \
  --ok-actions "<SCALE_DOWN_POLICY_ARN>" \
```

### Step 5: Verify the Setup

Check the scalable target is registered:

```bash
aws application-autoscaling describe-scalable-targets \
  --service-namespace ecs \
  --resource-ids "service/<CLUSTER_NAME>/<SERVICE_NAME>" \
  --region us-east-2
```

Check both policies exist:

```bash
aws application-autoscaling describe-scaling-policies \
  --service-namespace ecs \
  --resource-id "service/<CLUSTER_NAME>/<SERVICE_NAME>" \
  --region us-east-2
```

Check the alarm has the actions attached:

```bash
aws cloudwatch describe-alarms \
  --alarm-names "mcc-region-active-status-use1" \
  --query 'MetricAlarms[0].{AlarmActions:AlarmActions,OKActions:OKActions}' \
  --region us-east-1
```

You should see the scale-up policy ARN in AlarmActions and the scale-down policy ARN in OKActions.

## Testing

### Test Scale-Up (Failover)

1. Run the failover CLI and execute failover (option 2), or invoke directly:

```bash
aws lambda invoke \
  --function-name app-folamdba \
  --payload '{"execute_failover": true}' \
  --region us-east-1 \
  response.json
```

2. This publishes `RegionActiveStatus=0.0` which puts the alarm in ALARM state
3. The alarm triggers the scale-up policy
4. Check ECS in us-east-2 - desired count should change from 0 to N:

```bash
aws ecs describe-services \
  --cluster <CLUSTER_NAME> \
  --services <SERVICE_NAME> \
  --query 'services[0].{desired:desiredCount,running:runningCount}' \
  --region us-east-2
```

### Test Scale-Down (Failback)

1. Run failback:

```bash
aws lambda invoke \
  --function-name app-mfolambda \
  --payload '{"target_region": "us-east-1", "skip_health_check": true, "operator": "test", "aurora_confirmed": true}' \
  --region us-east-1 \
  response.json
```

2. This publishes `RegionActiveStatus=1.0` which returns the alarm to OK state
3. The alarm triggers the scale-down policy
4. Check ECS in us-east-2 - desired count should return to 0

### Test Region-Level Failure

When the entire us-east-1 region goes down:

1. The orchestrator Lambda stops running, metric stops publishing
2. The alarm fires on missing data (TreatMissingData=breaching)
3. The alarm triggers the scale-up policy in us-east-2
4. ECS scales up automatically with zero human intervention

This is the most critical scenario and it works automatically because the alarm's missing data treatment handles it.

## Timing Analysis

The total time from failure detection to containers serving traffic:

| Step | Duration | Cumulative |
|------|----------|------------|
| Alarm transitions to ALARM | ~60s (one evaluation period) | 60s |
| Application Auto Scaling reacts | ~10-30s | ~90s |
| ECS provisions Fargate tasks | ~30-60s (depends on image size) | ~150s |
| ALB health check passes | ~30s (depends on health check interval) | ~180s |

Total: approximately 2-3 minutes from failure to containers serving traffic.

Route 53 DNS change happens in parallel (triggered by the same alarm via the Route 53 health check). DNS TTL is typically 60 seconds. So DNS and containers are both ready at roughly the same time.

## IAM Permissions Required

Application Auto Scaling uses a service-linked role (`AWSServiceRoleForApplicationAutoScaling_ECSService`) that AWS creates automatically. This role already has `ecs:UpdateService` permission. No changes to your Lambda IAM role are needed.

The only permissions needed are for whoever runs the setup commands above:
- `application-autoscaling:RegisterScalableTarget`
- `application-autoscaling:PutScalingPolicy`
- `application-autoscaling:DescribeScalableTargets`
- `application-autoscaling:DescribeScalingPolicies`
- `cloudwatch:PutMetricAlarm` (to update alarm actions)

## Deploying for Additional Apps

For each new app that needs secondary ECS scaling:

1. Register the app's us-east-2 ECS service as a scalable target
2. Create scale-up and scale-down policies for that service
3. Add the policy ARNs to the app's us-east-1 CloudWatch alarm

The orchestrator Lambda code is identical for all apps - no code changes needed. The scaling behavior is entirely controlled by AWS infrastructure configuration.

## Rollback

To remove auto-scaling from an app:

```bash
# Remove policies
aws application-autoscaling delete-scaling-policy \
  --service-namespace ecs \
  --resource-id "service/<CLUSTER_NAME>/<SERVICE_NAME>" \
  --scalable-dimension ecs:service:DesiredCount \
  --policy-name "failover-scale-up" \
  --region us-east-2

aws application-autoscaling delete-scaling-policy \
  --service-namespace ecs \
  --resource-id "service/<CLUSTER_NAME>/<SERVICE_NAME>" \
  --scalable-dimension ecs:service:DesiredCount \
  --policy-name "failover-scale-down" \
  --region us-east-2

# Deregister scalable target
aws application-autoscaling deregister-scalable-target \
  --service-namespace ecs \
  --resource-id "service/<CLUSTER_NAME>/<SERVICE_NAME>" \
  --scalable-dimension ecs:service:DesiredCount \
  --region us-east-2
```

Then update the CloudWatch alarm to remove the policy ARNs from `--alarm-actions` and `--ok-actions`.

## Important Notes

- The scale-down policy on the OK action means that if the alarm briefly goes to OK and back to ALARM (unlikely but possible), the containers would scale down and back up. The 60-second cooldown on both policies mitigates this.

- If the orchestrator is in manual mode (`FAILOVER_MODE=manual`), the alarm still fires when the operator runs `execute_failover` because the Lambda publishes 0.0 at that point. The ECS scaling happens automatically even in manual mode - the "manual" part is only the DNS decision, not the scaling.

- The ECS service must exist in us-east-2 with desired count = 0 before setting up auto-scaling. If the service doesn't exist, `register-scalable-target` will fail.

- Kafka consumer behavior during the scale-up window: containers start consuming before Aurora is promoted. If writes fail with read-only errors and the consumer does NOT commit offsets on failure, messages will be reprocessed after Aurora promotion. Verify this behavior with the app team.
