# Vigil Event Log Format

All structured events are emitted to CloudWatch Logs as JSON lines. The ingestion filter for P1 alerting is:

```
event_source = "failover-orchestrator"
```

---

## State Machine

```
PRIMARY_ACTIVE
  └─► WAITING_AURORA_PROMOTION  (failover triggered — DNS moved, data tier pending)
        └─► SECONDARY_ACTIVE    (both Aurora + ElastiCache promoted)
              └─► FAILBACK_IN_PROGRESS
                    └─► PRIMARY_ACTIVE
```

Additional transient marker (active-active mode only): each region independently publishes health with no shared state transitions.

---

## Canonical JSON Log Schema

Every structured event follows this envelope:

```json
{
  "event_source": "failover-orchestrator",
  "event_type": "<EVENT_TYPE>",
  "severity": "CRITICAL | WARNING | INFO",
  "app_name": "<APP_NAME env var or '(not set)'>",
  "timestamp": "<ISO 8601 UTC>",
  "source_region": "<aws-region>",
  "target_region": "<aws-region>",
  "trigger": "<AUTO_ACTIVE | AUTO_PASSIVE | MANUAL_EXECUTE | OPERATOR>",
  "reason": "<human-readable description>",
  "failover_mode": "<auto | manual | parked>",
  "aurora_auto_promote": "<true | false>",
  "aurora_global_cluster": "<global-cluster-id>",
  "cooldown_minutes": 30
}
```

Additional fields are merged at the top level per event type (see below).

---

## Event Types

### `FAILOVER_INITIATED`

Emitted exactly once per failover. DNS has been moved. This is the primary P1 trigger.

**Triggers:**
- `AUTO_ACTIVE` — active region Lambda detected sustained health check failures
- `AUTO_PASSIVE` — passive region Lambda detected active region heartbeat is stale
- `MANUAL_EXECUTE` — operator invoked `{"execute_failover": true}`

**Additional fields:**

| Field | Present when | Value |
|-------|-------------|-------|
| `consecutive_failures` | AUTO_ACTIVE | integer |
| `health_signals` | AUTO_ACTIVE | object with per-signal results |
| `detection_method` | AUTO_ACTIVE | `"active_health_evaluation"` |
| `heartbeat_stale` | AUTO_PASSIVE | boolean |
| `cw_stale` | AUTO_PASSIVE | boolean |
| `detection_method` | AUTO_PASSIVE | `"passive_staleness"` |

**Example:**

```json
{
  "event_source": "failover-orchestrator",
  "event_type": "FAILOVER_INITIATED",
  "severity": "CRITICAL",
  "app_name": "fo-demo",
  "timestamp": "2026-04-21T14:32:11.042Z",
  "source_region": "us-west-1",
  "target_region": "us-west-2",
  "trigger": "AUTO_ACTIVE",
  "reason": "3/6 health signals failed: ALB HealthyHostCount=0, ECS RunningTasks=0, HTTP 503",
  "failover_mode": "auto",
  "aurora_auto_promote": "true",
  "aurora_global_cluster": "fo-demo-global",
  "cooldown_minutes": 30,
  "consecutive_failures": 3,
  "detection_method": "active_health_evaluation",
  "health_signals": {
    "http": {"healthy": false, "detail": "HTTP 503"},
    "alb": {"healthy": false, "detail": "HealthyHostCount=0"},
    "ecs": {"healthy": false, "detail": "RunningTasks=0/2"},
    "api_gateway": {"healthy": true},
    "aurora": {"healthy": true},
    "elasticache": {"healthy": true}
  }
}
```

---

### `REGION_REMOVED`

Emitted in active-active mode when a region is removed from the traffic pool (metric published as 0).

```json
{
  "event_source": "failover-orchestrator",
  "event_type": "REGION_REMOVED",
  "severity": "CRITICAL",
  "app_name": "fo-demo",
  "timestamp": "2026-04-21T14:32:11.042Z",
  "source_region": "us-west-1",
  "target_region": "us-west-2",
  "trigger": "AUTO_ACTIVE",
  "reason": "<health decision reason>",
  "failover_mode": "auto",
  "aurora_auto_promote": "false",
  "aurora_global_cluster": "(not set)",
  "cooldown_minutes": 30
}
```

---

### `REGION_RECOVERED`

Emitted in active-active mode when a region re-enters the traffic pool after being removed.

```json
{
  "event_source": "failover-orchestrator",
  "event_type": "REGION_RECOVERED",
  "severity": "INFO",
  "app_name": "fo-demo",
  "timestamp": "2026-04-21T14:37:05.201Z",
  "source_region": "us-west-1",
  "target_region": "us-west-2",
  "trigger": "AUTO_ACTIVE",
  "reason": "Region us-west-1 recovered after 3 consecutive failures",
  "failover_mode": "auto",
  "aurora_auto_promote": "false",
  "aurora_global_cluster": "(not set)",
  "cooldown_minutes": 30
}
```

---

## SNS Notification Subjects (non-JSON, for email/ticketing match)

These are sent via SNS in addition to the structured log lines. Match on `Subject` if ingesting SNS delivery logs.

| Subject pattern | Severity | P1? | Condition |
|----------------|----------|-----|-----------|
| `WARNING: <region> degraded (N/M)` | WARNING | No | Failures below threshold; still serving |
| `WARNING: <region> unhealthy but cooldown active` | WARNING | No | Threshold reached but cooldown window blocks failover |
| `WARNING: <region> passive region degraded` | WARNING | No | Passive region health degraded (doesn't affect traffic) |
| `FAILOVER EXECUTED: DNS moved to <region> - ...` | CRITICAL | **Yes** | Auto or manual failover completed |
| `REGION FAILURE: DNS moved to <region> - ...` | CRITICAL | **Yes** | Region-level failure (passive Lambda detected dead active region) |
| `MANUAL FAILOVER FAILED: <region> -> <region>` | CRITICAL | **Yes** | Operator-triggered failover threw an exception |
| `REMINDER: Aurora promotion still pending (Nm)` | CRITICAL | **Yes** | Aurora not promoted N minutes after failover |
| `REMINDER: ElastiCache promotion still pending (Nm)` | CRITICAL | **Yes** | ElastiCache not promoted N minutes after failover |
| `Aurora promotion confirmed - <region> is writer` | INFO | No | Auto-detected Aurora promotion complete |
| `ElastiCache promotion confirmed - <region> is primary` | INFO | No | Auto-detected ElastiCache promotion complete |
| `FAILBACK BLOCKED: AI readiness assessment says NO GO` | WARNING | No | AI GO/NO-GO blocked failback; operator override required |
| `FAILBACK BLOCKED: <region> not ready` | WARNING | No | Target region failed health validation before failback |
| `FAILBACK COMPLETE: -> <region>` | INFO | No | Failback succeeded; latch released |
| `FAILBACK FAILED: -> <region>` | CRITICAL | **Yes** | Failback threw an exception |
| `CRITICAL: <region> removed from traffic pool` | CRITICAL | **Yes** | Active-active: region removed from pool |
| `RECOVERED: <region> healthy, rejoining traffic pool` | INFO | No | Active-active: region rejoined pool |

---

## State Fields (for context in alerts)

When a P1 alert fires, query the state backend for full context:

| Field | Type | Description |
|-------|------|-------------|
| `active_region` | string | Region currently serving traffic |
| `state` | string | `PRIMARY_ACTIVE \| WAITING_AURORA_PROMOTION \| SECONDARY_ACTIVE \| FAILBACK_IN_PROGRESS` |
| `latch_engaged` | boolean | When `true`, failed region stays unhealthy in Route 53 until operator runs failback |
| `consecutive_failures` | integer | Health check failure count since last reset |
| `last_failover_ts` | ISO 8601 | When the most recent failover occurred |
| `aurora_promotion_pending` | boolean | Aurora has not yet been promoted to the new active region |
| `redis_promotion_pending` | boolean | ElastiCache has not yet been promoted to the new active region |
| `initiated_by` | string | `AUTO_ACTIVE \| AUTO_PASSIVE \| MANUAL_EXECUTE \| MANUAL \| MANUAL_RESET` |
| `reason` | string | Human-readable reason for last state change |

---

## P1 Trigger Logic (recommended)

```
IF event_source = "failover-orchestrator"
AND event_type IN ("FAILOVER_INITIATED", "REGION_REMOVED")
  → Open P1: production traffic has been rerouted

IF SNS subject MATCHES "REMINDER: Aurora promotion still pending"
  → Escalate existing P1: write path to database is blocked

IF SNS subject MATCHES "FAILBACK FAILED"
  → Open P1: production recovery blocked, manual intervention required

IF SNS subject MATCHES "MANUAL FAILOVER FAILED"
  → Open P1: failover attempt failed, system may be in inconsistent state
```

---

## Log Line Examples by State Phase

### Phase 1 — Degraded (no action yet)

CloudWatch log (unstructured warning, no JSON event emitted):
```
WARNING: 2/3 consecutive failures. Health: ALB HealthyHostCount=1 (below threshold)
```
SNS subject: `WARNING: us-west-1 degraded (2/3)`

### Phase 2 — Failover triggered

CloudWatch log (structured JSON, prefixed `FAILOVER_EVENT`):
```
CRITICAL FAILOVER_EVENT {"event_source":"failover-orchestrator","event_type":"FAILOVER_INITIATED",...}
```
SNS subject: `FAILOVER EXECUTED: DNS moved to us-west-2 - PROMOTE DATA TIER NOW`

### Phase 3 — Waiting for data tier promotion

CloudWatch log (unstructured, per minute):
```
INFO Waiting for Aurora promotion (12 minutes since failover)
```
SNS subject (every N minutes): `REMINDER: Aurora promotion still pending (10m)`

### Phase 4 — Promotion confirmed

SNS subject: `Aurora promotion confirmed - us-west-2 is writer`
SNS subject: `ElastiCache promotion confirmed - us-west-2 is primary`

State transitions to `SECONDARY_ACTIVE`.

### Phase 5 — Failback

SNS subject on success: `FAILBACK COMPLETE: -> us-west-1`

State transitions to `PRIMARY_ACTIVE`. Latch released.

---

## CloudWatch Log Filter Pattern (for metric alarm or Insights query)

**Detect any failover initiation:**
```
{ $.event_source = "failover-orchestrator" && $.event_type = "FAILOVER_INITIATED" }
```

**Detect CRITICAL events:**
```
{ $.event_source = "failover-orchestrator" && $.severity = "CRITICAL" }
```

**Insights query — last 10 failover events:**
```sql
fields @timestamp, event_type, trigger, source_region, target_region, reason
| filter event_source = "failover-orchestrator"
| filter event_type in ["FAILOVER_INITIATED", "REGION_REMOVED", "REGION_RECOVERED"]
| sort @timestamp desc
| limit 10
```
