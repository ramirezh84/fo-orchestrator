# State Backend Comparison: DynamoDB Global Tables vs S3 Cross-Region Replication

This document compares the two state backends supported by the failover orchestrator and explains why the S3 backend is a viable stopgap while DynamoDB Global Table provisioning goes through the exception process.

---

## Executive Summary

The failover orchestrator stores a single state record replicated between two AWS regions. Both backends achieve the same goal — cross-region state replication with concurrency control — but with different consistency guarantees and provisioning requirements.

**DynamoDB Global Tables** is the production-grade backend with sub-second replication and native conditional writes. Use it when the exception process is complete.

**S3 Cross-Region Replication** is a drop-in alternative that requires no DynamoDB. It has ~23 seconds of replication lag but is safe for this workload because the orchestrator runs on a 60-second cycle.

---

## Comparison Matrix

| Aspect | DynamoDB Global Table | S3 CRR |
|--------|----------------------|--------|
| **Cross-region replication** | Sub-second (<1s) | ~23 seconds (measured) |
| **Consistency within region** | Strongly consistent reads | Read-after-write consistent |
| **Conditional writes** | Native `ConditionExpression` | ETag-based optimistic locking (S3 `If-Match`) |
| **Race condition window** | Microseconds (server-side lock) | Milliseconds (read-modify-write with ETag check) |
| **Provisioning** | Requires DynamoDB Global Table exception | Standard S3 buckets + IAM roles |
| **Cost** | DynamoDB WCU/RCU (pay per operation) | S3 PUT/GET pricing (~$0.005/1000 requests) |
| **Replication guarantee** | Synchronous replication, strongly consistent | Eventual, RTC guarantees 99.99% within 15 min |
| **Replication monitoring** | Built-in `ReplicationLatency` metric | RTC adds `s3:ReplicationLatency` metric |
| **Max object size** | 400 KB per item | Unlimited (state file is <1 KB) |
| **Encryption** | KMS or AWS-owned keys | SSE-S3, SSE-KMS, or SSE-C |
| **Backup/versioning** | Point-in-time recovery | S3 versioning (required for CRR) |

---

## Why the S3 Backend is a Safe Stopgap

### 1. The 23-second lag is within tolerance

The orchestrator runs on a **60-second EventBridge schedule**. CRR replication was measured at a consistent ~23 seconds across 20 stress-test rounds (range: 23.2s–23.6s). This means:

- After a state write in Region A, Region B sees the update within 23 seconds
- Since the Lambda fires every 60 seconds, Region B always has current state by its next invocation
- In the worst case, Region B acts on stale state for **one cycle** (60 seconds), then self-corrects

### 2. The replication lag does NOT affect failover correctness

The critical question: can the 23-second lag cause a bad failover?

**No.** Here's why for each scenario:

| Scenario | Lag impact | Why it's safe |
|----------|-----------|---------------|
| **Active region writes heartbeat** | Passive reads heartbeat 23s late | Staleness threshold is 3 minutes. 23s delay doesn't trigger false staleness |
| **Active region claims failover** | Passive sees old state for 23s | Passive might not know failover happened yet — but it would see it next cycle. The latch prevents flip-flop regardless |
| **Passive region claims failover (region-down)** | Active is down, so there's no writer | No conflict possible — the dead region isn't writing |
| **Failback executed** | Both regions see new state within 23s | Operator runs failback manually — 23s delay is imperceptible |

### 3. Concurrent write safety is preserved

The S3 backend uses **ETag-based optimistic concurrency control**:

1. Read the state object and capture its ETag
2. Modify the state in memory
3. Write back with `If-Match: <etag>` — S3 rejects the write if the ETag changed

This was tested with 3 simultaneous Lambda invocations — exactly 1 won, 2 yielded. This matches DynamoDB's `ConditionExpression` behavior.

### 4. The latch mechanism works identically

The latch is a boolean field in the state record. After failover, the old region reads `latch_engaged: true` and publishes metric=0, preventing Route 53 from routing traffic back. This works the same whether the state is in DynamoDB or S3 — it's just a JSON field.

---

## Where S3 CRR Falls Short vs DynamoDB

### 1. Replication lag creates a stale-read window

With DynamoDB Global Tables, Region B reads state that's <1 second old. With S3 CRR, it reads state that could be up to 23 seconds old. This means:

- **38% chance** (23/60) that a given passive-region invocation reads stale state
- The passive region might not detect an active-region failover for one extra cycle
- In practice, this adds up to **60 seconds of delay** to passive region awareness

This is acceptable for a system with a 3-minute staleness threshold, but would not be acceptable for sub-second failover requirements.

**Mitigation:** The `REMOTE_STATE_BUCKET` environment variable enables cross-region direct writes during failover and failback. When set, state changes are written to both the local and remote S3 buckets immediately, bypassing CRR lag for critical state transitions. CRR still provides the steady-state replication for heartbeat updates.

### 2. No server-side conditional writes

DynamoDB evaluates conditions server-side in a single atomic operation. S3 requires a read-modify-write cycle with ETag checking, which has a wider race window. In practice, this hasn't been a problem because:

- Lambda concurrency per region is low (1-3 instances)
- The orchestrator runs once per minute, not continuously
- The ETag mechanism was validated under concurrent load

### 3. No built-in item-level TTL or streams

DynamoDB offers TTL, Streams, and triggers. S3 has lifecycle rules and event notifications but they operate differently. The orchestrator doesn't use any of these features, so this gap doesn't apply.

---

## Recommendation

| Phase | Backend | Rationale |
|-------|---------|-----------|
| **Now (stopgap)** | S3 CRR | No DynamoDB exception needed. 23s lag is within tolerance. All scenarios validated end-to-end. |
| **After exception approved** | DynamoDB Global Table | Sub-second replication. Native conditional writes. Production-grade for latency-sensitive failover. |

The migration path is a single environment variable change (`STATE_BACKEND=dynamodb`) — no code changes required. Both backends are always available in the deployed Lambda.

---

## Test Evidence

All scenarios were validated against real AWS infrastructure in us-west-1/us-west-2:

| Test | Result |
|------|--------|
| State initialization (S3) | Pass |
| Healthy active region invocation | Pass — heartbeat updated, metric=1.0 published |
| Passive region invocation | Pass — read S3 state, evaluated own health |
| Manual failover (us-west-1 → us-west-2) | Pass — SNS notification sent, metric=0.0, latch engaged |
| Latch enforcement on old region | Pass — `Latched region, staying marked unhealthy` |
| Aurora promotion reminder | Pass — new active region sends reminders |
| Failback (us-west-2 → us-west-1) | Pass — latch released, SNS notification sent |
| Region-level failure (passive takeover) | Pass — `AUTO_PASSIVE` failover, CRITICAL SNS sent |
| Cooldown enforcement | Pass — failover blocked, WARNING SNS sent |
| Concurrent race (3 simultaneous Lambdas) | Pass — 1 won, 2 yielded, no duplicates |
| CRR replication lag | 23.3s average across 20 rounds (100% within 24s) |
| CRR replication with RTC | Same ~23s — RTC adds SLA guarantee, not speed |
