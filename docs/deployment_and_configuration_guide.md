# Failover Orchestrator - Deployment and Configuration Guide

This guide provides the necessary steps and reference materials for deploying and configuring the Multi-Region Failover Orchestrator.

## 1. Deployment Guide

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
  --change-batch 
{
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
  }
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

## 2. Configuration Reference

### Environment Variables -- Orchestrator Lambda

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
| `AURORA_AUTO_PROMOTE` | `false` | Set to `true` to automatically call `SwitchoverGlobalCluster` (for app failures) or `FailoverGlobalCluster` (for region failures) during failover. If the API call fails, falls back to manual notification. |
| `FAILOVER_MODE` | `auto` | `auto` = full automated failover. `manual` = detect and notify only, operator must run `execute_failover` command. |
| `COOLDOWN_MINUTES` | `30` | Minimum minutes between automated failovers |
| `CONSECUTIVE_FAILURES_THRESHOLD` | `3` | Consecutive unhealthy evaluations before failover |
| `HEALTH_EVALUATION_WINDOW_MINUTES` | `5` | CloudWatch metric evaluation window |
| `MIN_HEALTHY_HOST_COUNT` | `1` | Minimum ALB healthy hosts |
| `API_GW_5XX_THRESHOLD_PERCENT` | `50` | Max API GW 5xx error rate before unhealthy |
| `ACTIVE_REGION_STALE_THRESHOLD_MINUTES` | `3` | How long the passive region waits before declaring the active region lost. Uses AND logic: both DynamoDB heartbeat and cross-region CloudWatch must agree the region is stale. |
| `AURORA_PROMOTION_REMINDER_INTERVAL_MINUTES` | `5` | How often the Lambda sends reminder notifications while Aurora promotion is pending |
| `WARNING_NOTIFICATION_COOLDOWN_MINUTES` | `10` | Minimum minutes between WARNING-level notifications to prevent flooding. |

### Environment Variables -- Failback Lambda

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

## 3. Networking and VPC Requirements

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
- TCP 443 to `0.0.0.0/0` (for AWS API calls via NAT) -- OR to VPC endpoint security groups if using endpoints

The ALB security group needs an **inbound rule** allowing traffic from the Lambda security group on the health check port (typically 80 or 443).

```