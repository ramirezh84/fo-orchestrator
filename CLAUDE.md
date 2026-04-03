# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Multi-region failover orchestrator for AWS infrastructure (Deposits 2.0 platform). Lambda-based system that manages automated failover and manual failback between us-east-1 (primary) and us-east-2 (secondary), with a "latch" mechanism to prevent Route 53 from flip-flopping traffic.

**Core problem solved:** Route 53 failover records are stateless and can flip traffic back and forth rapidly. This system adds a decision layer with consecutive-failure thresholds, cooldowns, and an explicit latch that keeps the old region marked unhealthy until an operator manually runs failback.

## Commands

No build system. Deployment is manual via AWS CLI:

```bash
# Package and deploy orchestrator Lambda (include state_backend.py)
zip failover_orchestrator_v3.zip failover_orchestrator_v3.py state_backend.py
aws lambda update-function-code \
  --function-name failover-orchestrator-prod \
  --zip-file fileb://failover_orchestrator_v3.zip \
  --region us-east-1

# Package and deploy failback Lambda (include state_backend.py)
zip manual_failback_v2.zip manual_failback_v2.py state_backend.py
aws lambda update-function-code \
  --function-name failover-manual-failback-prod \
  --zip-file fileb://manual_failback_v2.zip \
  --region us-east-1

# Run the interactive CLI operator tool
python3 failover_cli.py

# Run the local monitoring dashboard (http://localhost:5000)
python3 failover_dashboard_local.py
```

Runtime dependencies: `boto3`, `botocore` (provided by Lambda runtime). Dashboard requires `flask`.

## Architecture

### Components

| File | Role |
|------|------|
| `failover_orchestrator_v3.py` | Main Lambda (~2,100 lines). Deployed in both regions. Evaluates health, publishes Route 53 metrics, triggers failover. |
| `manual_failback_v2.py` | Failback Lambda. Operator-triggered to return traffic to primary. |
| `state_backend.py` | State backend abstraction: DynamoDB Global Table or S3 CRR. |
| `failover_cli.py` | Interactive CLI for operators: live health monitoring, failure simulation, state inspection. |
| `failover_dashboard_local.py` | Flask web dashboard reading state from backend. |
| `tools/setup_s3_state_backend.py` | Infrastructure setup script for S3 CRR backend. |
| `tools/generate_dashboard.py` | CloudWatch dashboard generator. |
| `tests/test_state_backend.py` | Unit + integration + CRR replication tests for state backends. |
| `tests/test_e2e_s3_backend.py` | End-to-end scenario tests for S3 backend. |

### Supported Use Cases

| Use Case | `ROUTING_MODE` | Secondary Containers | Route 53 | Guide |
|----------|---------------|---------------------|----------|-------|
| Active/passive failover | `failover` (default) | Running (warm standby) | Failover records | This file |
| Zero-container secondary | `failover` + `PASSIVE_PUBLISH_ZERO=true` | 0 (auto-scaled on failover) | Failover records | `docs/zero_container_secondary_guide.md` |
| Active/active | `active-active` | Running in both regions | Latency-based records | `docs/active_active_guide.md` |

### Execution Flow

```
EventBridge (1 min) → Orchestrator Lambda
  Active region:  evaluate 5 health signals → publish RegionActiveStatus metric → may trigger failover
  Passive region: detect stale heartbeat → evaluate own readiness → publish metric

CloudWatch Alarm (TreatMissingData: breaching) → Route 53 Health Check → Route 53 Failover Record → NLB → App
```

### Health Signal Evaluation

Five signals evaluated with quorum logic (≥50% must fail to declare region unhealthy):
1. **HTTP** `/actuator/health` on private ALB — any failure = immediately unhealthy (bypasses quorum)
2. **ALB** HealthyHostCount ≥ `MIN_HEALTHY_HOST_COUNT`
3. **ECS** RunningTasks ≥ 50% of desired
4. **API Gateway** 5xx error rate < `API_GW_5XX_THRESHOLD_PERCENT`
5. **Aurora** cluster status must be "available"

### State Management

State is a single logical record replicated across both regions. Two backends are supported, selected via the `STATE_BACKEND` environment variable:

**Option 1: DynamoDB Global Table** (`STATE_BACKEND=dynamodb`, default)

Single item `pk: "REGION_STATE"` in a DynamoDB Global Table. Provides strong consistency within a region and sub-second cross-region replication. Requires DynamoDB Global Table provisioning.

**Option 2: S3 Cross-Region Replication** (`STATE_BACKEND=s3`)

JSON file at `s3://<bucket>/failover-state/REGION_STATE.json` with bidirectional CRR between region-specific buckets. Uses ETag-based optimistic concurrency control (S3 `If-Match` on PutObject) for conditional writes equivalent to DynamoDB ConditionExpressions. CRR replication is eventual (~15-60s typical). No DynamoDB required — useful when DynamoDB Global Tables require an exception process.

**State fields** (same for both backends):
- `active_region` — which region is serving traffic
- `state` — `PRIMARY_ACTIVE | WAITING_AURORA_PROMOTION | SECONDARY_ACTIVE | FAILBACK_IN_PROGRESS`
- `latch_engaged` — prevents flip-flopping; only cleared by failback Lambda
- `consecutive_failures` — must reach threshold before failover fires
- `last_failover_ts` — enforces cooldown window

### Anti-Flip-Flop Mechanisms

1. **Consecutive failure threshold** (default 3 min): sustained failure required, not a single blip
2. **Cooldown window** (default 30 min): one failover maximum per window
3. **Latch**: after failover, old region stays marked unhealthy in Route 53 even if it recovers — only released by explicit operator action via failback Lambda

## Configuration

All configuration is via Lambda environment variables. Key variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `PRIMARY_REGION` / `SECONDARY_REGION` | us-east-1 / us-east-2 | Region roles |
| `STATE_BACKEND` | dynamodb | State backend: `dynamodb` or `s3` |
| `STATE_TABLE` | failover-state | DynamoDB Global Table name (when `STATE_BACKEND=dynamodb`) |
| `STATE_BUCKET` | (required if s3) | S3 bucket name for state (when `STATE_BACKEND=s3`) |
| `STATE_PREFIX` | failover-state/ | S3 key prefix for state file (when `STATE_BACKEND=s3`) |
| `REMOTE_STATE_BUCKET` | (empty) | Other region's S3 bucket for cross-region writes (when `STATE_BACKEND=s3`) |
| `PASSIVE_PUBLISH_ZERO` | false | Passive region always publishes metric=0 (for zero-container secondary use case) |
| `ROUTING_MODE` | failover | `failover` (active/passive with latch) or `active-active` (both regions serve, auto-recovery) |
| `SNS_TOPIC_ARN` | (required) | Operator notifications |
| `HEALTH_CHECK_URL` | (empty) | Private ALB URL for HTTP health check |
| `FAILOVER_MODE` | auto | `auto` or `manual` (notify-only, no DNS change) |
| `COOLDOWN_MINUTES` | 30 | Minimum time between failovers |
| `CONSECUTIVE_FAILURES_THRESHOLD` | 3 | Sustained failures to trigger failover |
| `AURORA_AUTO_PROMOTE` | false | Auto-promote Aurora or wait for operator |
| `MIN_HEALTHY_HOST_COUNT` | 1 | Minimum ALB targets |
| `API_GW_5XX_THRESHOLD_PERCENT` | 50.0 | Error rate threshold |
| `ACTIVE_REGION_STALE_THRESHOLD_MINUTES` | 3 | Heartbeat age to declare region failed |

## Key Design Decisions

- **Automated failover, manual failback**: DNS failover fires automatically when thresholds are met, but returning to primary always requires operator action. This is intentional to prevent autonomous flip-flopping.
- **Aurora promotion is manual by default** (`AURORA_AUTO_PROMOTE=false`): Lambda sends CLI commands via SNS; operator runs `aws rds switchover-global-cluster`. Auto-promotion is available but disabled to prevent accidental data loss on unplanned failovers.
- **No Step Functions**: Entire orchestration runs inside a single Lambda on a 1-minute EventBridge schedule. Each invocation is stateless; all state is in the configured backend (DynamoDB or S3).
- **Passive region publishes its own health metric**: Secondary region demonstrates readiness to receive traffic, which is also used during failback validation.
- **Failback Lambda invoked in target region**: Must be invoked in us-east-1 when failing back to us-east-1, so it can verify Aurora writer status locally.

## Operational Notes

- Lambda must be VPC-attached to reach private ALB endpoints
- Both backends auto-create `PRIMARY_ACTIVE` state on first EventBridge invocation (no manual seeding needed)
- SNS notifications are throttled for WARNING level (every 10 min) but never throttled for CRITICAL level
- `FAILOVER_MODE=manual` is useful during deployments to suppress automatic DNS changes while still getting health alerts

## S3 State Backend Setup

Use this when DynamoDB Global Tables require an exception process. The S3 backend is a drop-in replacement that uses S3 Cross-Region Replication (CRR) instead.

### Quick Start

```bash
# 1. Provision S3 buckets, versioning, IAM roles, and bidirectional CRR
python3 tools/setup_s3_state_backend.py

# 2. Set Lambda environment variables (in BOTH regions):
#    STATE_BACKEND=s3
#    STATE_BUCKET=failover-state-<region>-<account-id>
#    STATE_PREFIX=failover-state/

# 3. Include state_backend.py in the Lambda deployment zip:
zip failover_orchestrator_v3.zip failover_orchestrator_v3.py state_backend.py
zip manual_failback_v2.zip manual_failback_v2.py state_backend.py
```

### Trade-offs vs DynamoDB

| Aspect | DynamoDB Global Table | S3 CRR |
|--------|----------------------|--------|
| Cross-region replication | Sub-second | 15-60s typical |
| Conditional writes | Native (ConditionExpression) | ETag-based optimistic locking |
| Consistency within region | Strongly consistent reads | Read-after-write consistent |
| Provisioning complexity | Requires DynamoDB exception | Standard S3 + IAM |
| Cost | DynamoDB WCU/RCU pricing | S3 PUT/GET pricing (very low) |
| Race condition window | Microseconds (DynamoDB lock) | Milliseconds (ETag check) |

### Teardown

```bash
python3 tools/setup_s3_state_backend.py --teardown
```

### Testing

```bash
# Unit tests (no AWS required)
python3 -m pytest tests/test_state_backend.py -v -k "not Integration"

# S3 integration tests (requires AWS credentials)
INTEGRATION_TEST=1 python3 -m pytest tests/test_state_backend.py -v -k "S3Integration"

# CRR replication test (requires pre-provisioned buckets)
CRR_TEST=1 \
  CRR_PRIMARY_BUCKET=<primary-bucket> \
  CRR_SECONDARY_BUCKET=<secondary-bucket> \
  python3 -m pytest tests/test_state_backend.py -v -k "CRR"
```
