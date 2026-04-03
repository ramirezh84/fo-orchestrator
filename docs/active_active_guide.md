# Active-Active Routing Mode — Setup Guide

How to configure the failover orchestrator for applications using Route 53 **latency-based routing** where both regions serve traffic simultaneously.

---

## How It Works

In active-active mode, each region independently evaluates its own health and publishes a CloudWatch metric. Route 53 uses this metric (via health checks) to decide whether to include the region in the latency-based routing pool.

```
Normal:     us-west-1: serving traffic, metric=1     us-west-2: serving traffic, metric=1
Degraded:   us-west-1: metric=0 (removed)            us-west-2: serving ALL traffic, metric=1
Recovered:  us-west-1: metric=1 (rejoined)            us-west-2: serving traffic, metric=1
```

There is no active/passive distinction, no latch, no manual failback. Recovery is automatic.

---

## What's Different from Failover Mode

| Behavior | Failover Mode | Active-Active Mode |
|----------|--------------|-------------------|
| Region roles | One active, one passive | Both active |
| Latch after unhealthy | Engaged, blocks recovery | No latch — auto-recovery |
| Manual failback required | Yes | No — automatic when healthy |
| Cross-region coordination | Required (who is active) | Minimal (only for Aurora) |
| State machine | PRIMARY_ACTIVE → WAITING_AURORA → etc. | Just healthy/unhealthy |
| Route 53 record type | Failover | Latency-based |
| Failback Lambda | Required | Not needed |
| `execute_failover` event | Supported | Not applicable (ignored) |

---

## Configuration

### Environment Variable

Set on the orchestrator Lambda in **both** regions:

| Variable | Value |
|----------|-------|
| `ROUTING_MODE` | `active-active` |

All other environment variables (`HEALTH_CHECK_URL`, `ECS_CLUSTER_NAME`, `SNS_TOPIC_ARN`, etc.) are configured the same as failover mode.

### Route 53 Records

Use **latency-based** records instead of failover records. Each record points to the regional NLB/ALB and is associated with a Route 53 health check linked to the CloudWatch alarm.

| Record | Type | Routing | Health Check |
|--------|------|---------|-------------|
| `api.example.com` (us-east-1) | A / Alias to NLB | Latency, region=us-east-1 | CW alarm for us-east-1 |
| `api.example.com` (us-east-2) | A / Alias to NLB | Latency, region=us-east-2 | CW alarm for us-east-2 |

When a region's health check fails (metric=0 → alarm ALARM → health check unhealthy), Route 53 stops routing traffic to that region. When it recovers, Route 53 resumes routing.

### Failback Lambda

Not needed. You can skip deploying `manual_failback_v2.py` for active-active apps. If deployed, it is inert — the orchestrator never enters states that require failback.

---

## How the Handler Works

In active-active mode, the handler:

1. Reads state (consecutive failures, cooldown timestamp)
2. Evaluates local health (same 5 signals: HTTP, ALB, ECS, API GW, Aurora)
3. If **healthy**:
   - Resets consecutive failures to 0
   - Publishes metric=1
   - If was previously unhealthy: emits `REGION_RECOVERED` Splunk event + SNS notification
4. If **unhealthy**:
   - Increments consecutive failures (with ETag concurrency control)
   - If below threshold: publishes metric=1 (still serving), sends WARNING
   - If at threshold + past cooldown: publishes metric=0 (removed from pool), sends CRITICAL, emits `REGION_REMOVED` Splunk event
   - If at threshold but cooldown active: publishes metric=1, sends WARNING

### Splunk Events

| Event Type | Trigger |
|-----------|---------|
| `REGION_REMOVED` | Region marked unhealthy and removed from traffic pool |
| `REGION_RECOVERED` | Region health restored and rejoining traffic pool |

Both include `app_name`, `timestamp`, `source_region`, `trigger: AUTO_ACTIVE_ACTIVE`, and `reason`.

---

## Operational Notes

### No operator intervention needed for recovery

Unlike failover mode, there is no latch to release and no failback Lambda to invoke. When the underlying issue is fixed (ECS tasks restart, ALB targets become healthy, etc.), the orchestrator automatically publishes metric=1 on the next cycle and Route 53 adds the region back.

### Cooldown prevents flapping

The cooldown window (default 5 min) prevents a region from being repeatedly removed and re-added to the pool. After marking a region unhealthy, the orchestrator waits for the cooldown to expire before it can mark the region unhealthy again. This prevents a flapping service from causing rapid DNS changes.

### Both regions share the same state backend

Each region writes to its own S3 bucket (or DynamoDB replica). The state is per-region (consecutive failures, last unhealthy timestamp). The `REMOTE_STATE_BUCKET` cross-region write ensures the other region sees state changes immediately.

### ECS service runs in both regions

Unlike the zero-container secondary use case, active-active requires containers running in both regions at all times. No Application Auto Scaling is needed — both regions maintain their desired count.

---

## Test Results

Tested against live AWS infrastructure (us-west-1 / us-west-2) with fo-demo-app3.

| # | Scenario | Result | SNS |
|---|----------|--------|-----|
| A | **Health failure removes region from pool** | metric=0, `AUTO_ACTIVE_ACTIVE`, failures=3, no latch, no state machine change | CRITICAL: removed |
| A | **Auto-recovery when health returns** | metric=1, failures reset to 0, region rejoins pool | RECOVERED: rejoining |
| A | **Other region unaffected** | us-west-2 independently healthy throughout | None |
| B | **Cooldown prevents flapping** | `Cooldown active`, metric=1 kept, no removal | WARNING |
