# Zero-Container Secondary Region — Setup Guide

How to configure the failover orchestrator for applications that run **zero containers in the secondary region** during normal operation and scale up automatically during failover.

---

## How It Works

In the standard setup, both regions run containers at all times. The zero-container variant saves cost by running containers only in the primary region. When failover triggers, Application Auto Scaling spins up containers in the secondary automatically.

```
Normal:     us-west-1: 2 containers (active)     us-west-2: 0 containers (passive)
Failover:   us-west-1: 2 containers (latched)     us-west-2: 2 containers (active, auto-scaled)
Failback:   us-west-1: 2 containers (active)     us-west-2: 0 containers (scaled down)
```

### The Auto-Scaling Mechanism

The orchestrator publishes a `RegionActiveStatus` CloudWatch metric (1.0 = healthy, 0.0 = unhealthy) per region. A CloudWatch alarm watches this metric. Application Auto Scaling policies are attached to the alarm:

| Alarm State | Trigger | Auto Scaling Action | Result |
|-------------|---------|---------------------|--------|
| **ALARM** (metric < 1) | Region is passive or unhealthy | Scale **DOWN** to 0 | Containers removed |
| **OK** (metric >= 1) | Region claimed failover | Scale **UP** to N | Containers launched |

**Note**: This is reversed from typical alarm wiring. ALARM = scale down, OK = scale up. This is because metric=0 is the normal steady state for a passive region with no containers.

### Timeline (measured)

| Phase | Time |
|-------|------|
| Failover claimed, metric=1 published | 0s |
| CloudWatch alarm transitions ALARM → OK | ~50s |
| Application Auto Scaling fires scale-up | ~5s after alarm |
| ECS Fargate tasks provisioned and running | ~30-60s |
| ALB health checks pass | ~15s |
| **Total: containers healthy and serving traffic** | **~1.5-2 minutes** |

---

## What's Different from Standard Setup

| Component | Standard Setup | Zero-Container Setup |
|-----------|---------------|---------------------|
| Secondary ECS desired count | Same as primary (e.g., 2) | **0** |
| `PASSIVE_PUBLISH_ZERO` env var | Not set (default false) | **Set to `true`** on secondary orchestrator |
| Application Auto Scaling | Not required | **Required** on secondary ECS service |
| CloudWatch alarm actions | SNS only | SNS **+ scale-up/down policies** |
| Scale-down after failback | N/A (containers always running) | **Automatic** via alarm transition |

---

## Requirements

### 1. ECS Service in Secondary Region

Create the ECS service with `desiredCount: 0`. The service must be registered with an ALB target group so that when containers start, they are automatically registered and health-checked.

- Same task definition, VPC, subnets, and security groups as primary
- `desiredCount: 0`
- Load balancer target group attached

### 2. Application Auto Scaling

Register the secondary ECS service as a scalable target and create two step scaling policies.

**Scalable target:**

| Property | Value |
|----------|-------|
| Service namespace | ecs |
| Resource ID | `service/<cluster>/<service-name>` |
| Scalable dimension | `ecs:service:DesiredCount` |
| Min capacity | 0 |
| Max capacity | Production task count (e.g., 2) |

**Scale-UP policy** (attached to alarm **OK** action):

| Property | Value |
|----------|-------|
| Policy name | `<app>-scale-up` |
| Policy type | StepScaling |
| Adjustment type | ExactCapacity |
| Step adjustment | `MetricIntervalUpperBound: 0, ScalingAdjustment: <task count>` |
| Cooldown | 60 seconds |

**Scale-DOWN policy** (attached to alarm **ALARM** action):

| Property | Value |
|----------|-------|
| Policy name | `<app>-scale-down` |
| Policy type | StepScaling |
| Adjustment type | ExactCapacity |
| Step adjustment | `MetricIntervalUpperBound: 0, ScalingAdjustment: 0` |
| Cooldown | 60 seconds |

**Important: Step adjustment bounds**

The CloudWatch alarm uses `ComparisonOperator: LessThanThreshold` with `Threshold: 1`. When the alarm fires, the breach delta is **negative** (metric 0 - threshold 1 = -1). Both scale-up and scale-down policies must use `MetricIntervalUpperBound: 0` (not `LowerBound`) to match the negative breach delta.

### 3. CloudWatch Alarm Actions

The secondary region's CloudWatch alarm must have both auto-scaling policies attached:

| Alarm Action | Policy |
|-------------|--------|
| ALARM actions | Scale-down policy ARN + SNS topic ARN |
| OK actions | Scale-up policy ARN + SNS topic ARN |

### 4. Orchestrator Environment Variable

Set `PASSIVE_PUBLISH_ZERO=true` on the orchestrator Lambda **in the secondary region only**.

This makes the passive orchestrator always publish metric=0 for itself (Job 2), which:
- Keeps the alarm in ALARM state during normal operation → containers stay at 0
- After failback, triggers the alarm ALARM transition → containers scale back to 0

Without this flag, after failback the passive orchestrator evaluates real health (containers still running → healthy → metric=1), the alarm stays OK, and containers never scale down.

**Do NOT set this on the primary region orchestrator.** It is only needed on the region that starts with 0 containers.

### 5. Cross-Region State Writes (S3 backend only)

When using the S3 state backend, set `REMOTE_STATE_BUCKET` on **all** Lambdas (orchestrator + failback, both regions). This ensures state changes are written to both S3 buckets immediately, without waiting for CRR.

| Lambda Region | `REMOTE_STATE_BUCKET` value |
|--------------|----------------------------|
| Primary (e.g., us-west-1) | Secondary region's bucket name |
| Secondary (e.g., us-west-2) | Primary region's bucket name |

**Why this is required:** With S3 CRR, each region writes to its own bucket. After failover, the remote region's orchestrator continues writing stale state to its bucket. Without cross-region writes, CRR from the remote bucket can overwrite the failover/failback state. The `REMOTE_STATE_BUCKET` feature writes directly to both buckets, ensuring immediate consistency.

The Lambda IAM role must have `s3:GetObject`, `s3:PutObject`, and `s3:ListBucket` permissions on **both** buckets (local and remote).

### 6. Lambda Code

The `state_backend.py` module must be included in the Lambda deployment package. The `PASSIVE_PUBLISH_ZERO` and `REMOTE_STATE_BUCKET` features are in `failover_orchestrator_v3.py` and `manual_failback_v2.py` — no separate module needed.

---

## Operational Notes

### Scale-up timing adds to failover duration

With the standard setup, the secondary already has running containers — DNS change is the only delay (~60s TTL). With zero-container secondary, add ~1.5-2 minutes for containers to start and become healthy. Total failover time: ~2-3 minutes.

### The primary region is unaffected

All changes are in the secondary region only:
- Secondary ECS service: desired=0
- Secondary alarm: auto-scaling actions
- Secondary orchestrator: `PASSIVE_PUBLISH_ZERO=true`

The primary region orchestrator, alarm, and ECS service are identical to the standard setup.

### Health checks during scale-up

During the ~60s window between failover and containers being healthy, the ALB target group in the secondary has no healthy targets. The orchestrator in the secondary is in `WAITING_AURORA_PROMOTION` state and publishes metric=1 (it claimed failover), so Route 53 routes traffic there. Clients may see 502/503 from the ALB until containers are registered.

To mitigate: set a longer ALB `HealthCheckGracePeriodSeconds` on the ECS service and consider using Route 53 health checks with `FailureThreshold: 3` to give containers time to start.

### Failback scale-down takes ~3 minutes

After failback, the passive orchestrator publishes metric=0, but the CloudWatch alarm needs 2 evaluation periods (2 x 60s) to transition from OK to ALARM. Then Application Auto Scaling fires the scale-down. ECS drains and stops containers. Total: ~3 minutes from failback to 0 containers.

### The first app (fo-demo) is not affected

The second app uses:
- Separate ECS service (`fo-demo-app2-svc`)
- Separate ALB listener (port 8080)
- Separate CloudWatch namespace (`Custom/FoDemoApp2`)
- Separate CloudWatch alarms (`fo-demo-app2-region-inactive-*`)
- Separate Lambdas (`fo-demo-app2-orchestrator`, `fo-demo-app2-failback`)
- Separate S3 state prefix (`failover-state-app2/`)
- Separate SNS topics (`fo-demo-app2-alerts`)

No shared resources are modified.

---

## Test Results

All scenarios tested against live AWS infrastructure (us-west-1 / us-west-2) with the fo-demo-app2 deployment.

| # | Scenario | Trigger | Result | Auto-Scaling | SNS |
|---|----------|---------|--------|-------------|-----|
| 1 | **Auto failover (app outage)** | Health check fails, failures=3 | `AUTO_ACTIVE` failover | 0→2 in ~2 min, 2→0 after failback | Yes |
| 2 | **Manual failover mode** | `FAILOVER_MODE=manual`, health fails | Notified only, NO failover | Containers stayed at 0 | Yes (WARNING) |
| 3 | **Aurora auto-promote** | `AURORA_AUTO_PROMOTE=true`, failover | RDS switchover attempted | 0→2 | Yes |
| 4 | **Aurora manual mode** | `AURORA_AUTO_PROMOTE=false`, failover | CLI commands in SNS | 0→2 | Yes (with CLI) |
| 5 | **Region outage** | Heartbeat 801s stale, no CW data | `AUTO_PASSIVE` failover | 0→2 in ~70s, 2→0 after failback | Yes (CRITICAL) |
| 6 | **Cooldown enforcement** | Failures at threshold, cooldown active | Blocked, `Cooldown active` | No change | Yes (WARNING) |

### Key Findings

- **Cross-region S3 writes (`REMOTE_STATE_BUCKET`) are essential.** Without them, after failover or failback, the remote region's orchestrator continues writing stale state to its bucket, and CRR overwrites the correct state. Direct cross-region writes solve this.
- **`PASSIVE_PUBLISH_ZERO` is required for automatic scale-down after failback.** Without it, the passive orchestrator publishes metric=1 (containers still running) and the alarm never fires.
- **Scale-up timing:** ~70-120 seconds from failover claim to containers healthy and serving traffic.
- **Scale-down timing:** ~3 minutes from failback to 0 containers (2 alarm periods + ECS drain).
