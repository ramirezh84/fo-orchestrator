# S3 State Backend — Setup and Migration Guide

Instructions for switching the failover orchestrator from DynamoDB Global Tables to S3 Cross-Region Replication as the state backend.

---

## 1. Infrastructure Requirements

### Resources Needed

Provision the following in your infrastructure tool of choice. All resources are required in **both** regions.

**Per region:**

| Resource | Configuration | Purpose |
|----------|--------------|---------|
| S3 bucket | Versioning enabled | State storage for that region's Lambda |
| S3 replication rule | Prefix filter: `failover-state/`, destination: the other region's bucket | Bidirectional cross-region replication |
| IAM role for replication | Trust: `s3.amazonaws.com` | Allows S3 to replicate objects between buckets |

**Global (one-time):**

| Resource | Configuration | Purpose |
|----------|--------------|---------|
| IAM policy on each Lambda execution role | S3 read/write to the regional bucket | Allows Lambda to read and write state |

### S3 Bucket Configuration

Both buckets must have:

- **Versioning**: `Enabled` (mandatory for cross-region replication)
- **Replication Time Control (RTC)**: `Enabled` with 15-minute threshold (provides SLA guarantee and CloudWatch replication metrics)
- **Replication scope**: Only objects under the `failover-state/` prefix
- **Replication direction**: Bidirectional — each bucket replicates to the other
- **Delete marker replication**: `Enabled`

### S3 Replication Rule

Each bucket needs one replication rule pointing to the other bucket:

| Property | Value |
|----------|-------|
| Status | Enabled |
| Priority | 1 |
| Filter prefix | `failover-state/` |
| Destination bucket | The other region's bucket ARN |
| Storage class | STANDARD |
| Replication Time Control | Enabled, 15 minutes |
| Metrics | Enabled, 15-minute threshold |
| Delete marker replication | Enabled |

### IAM Role for Replication

Create **two** roles — one per direction (primary → secondary, secondary → primary).

**Trust relationship** (same for both):

| Principal | Action |
|-----------|--------|
| `s3.amazonaws.com` | `sts:AssumeRole` |

**Permissions** (adjust source/destination per direction):

| Action | Resource | Purpose |
|--------|----------|---------|
| `s3:GetReplicationConfiguration` | Source bucket ARN | Read replication config |
| `s3:ListBucket` | Source bucket ARN | List objects for replication |
| `s3:GetObjectVersionForReplication` | Source bucket ARN/* | Read object versions |
| `s3:GetObjectVersionAcl` | Source bucket ARN/* | Read object ACLs |
| `s3:GetObjectVersionTagging` | Source bucket ARN/* | Read object tags |
| `s3:ReplicateObject` | Destination bucket ARN/* | Write replicated objects |
| `s3:ReplicateDelete` | Destination bucket ARN/* | Replicate deletions |
| `s3:ReplicateTags` | Destination bucket ARN/* | Replicate tags |

---

## 2. Lambda IAM Permissions

Add these permissions to each region's Lambda execution role. Each Lambda needs access to **both** its own bucket and the other region's bucket (for cross-region state writes).

| Action | Resource | Purpose |
|--------|----------|---------|
| `s3:GetObject` | Both regional bucket ARNs / `failover-state/*` | Read state |
| `s3:PutObject` | Both regional bucket ARNs / `failover-state/*` | Write state (local + remote) |
| `s3:DeleteObject` | Both regional bucket ARNs / `failover-state/*` | Delete state (reset) |
| `s3:ListBucket` | Both regional bucket ARNs | Distinguish "not found" from "access denied" |

**Why cross-region write access is required:** During failover and failback, the Lambda writes state changes to both buckets directly. Without this, the remote region's orchestrator may continue operating on stale state until CRR catches up (~23s), which can cause state conflicts.

**Why `s3:ListBucket` is required:** When the state file doesn't exist yet (first invocation), S3 returns `AccessDenied` instead of `NoSuchKey` unless the caller has `ListBucket` on the bucket.

---

## 3. Lambda Environment Variables

### Variables to add

Add these three environment variables to **all four Lambdas** (orchestrator + failback, in both regions). The existing `STATE_TABLE` variable can remain — it is ignored when `STATE_BACKEND=s3`.

| Variable | Value (primary region) | Value (secondary region) |
|----------|----------------------|-------------------------|
| `STATE_BACKEND` | `s3` | `s3` |
| `STATE_BUCKET` | `<primary-region-bucket-name>` | `<secondary-region-bucket-name>` |
| `STATE_PREFIX` | `failover-state/` | `failover-state/` |
| `REMOTE_STATE_BUCKET` | `<secondary-region-bucket-name>` | `<primary-region-bucket-name>` |

### Variables to remove (when switching back to DynamoDB)

Remove `STATE_BACKEND`, `STATE_BUCKET`, `STATE_PREFIX`, and `REMOTE_STATE_BUCKET`. The backend defaults to `dynamodb` when `STATE_BACKEND` is absent, and `STATE_TABLE` is already configured.

---

## 4. Lambda Code Deployment

The `state_backend.py` module must be included in both Lambda deployment packages alongside the existing handler file.

**Orchestrator package contents:**
- `failover_orchestrator_v3.py`
- `state_backend.py`

**Failback package contents:**
- `manual_failback_v2.py`
- `state_backend.py`

Deploy both packages to both regions.

---

## 5. Migration Procedure: DynamoDB to S3

### Step 1: Pause automated invocations

Disable the EventBridge schedule rule in both regions to prevent the orchestrator from running during the switch.

### Step 2: Deploy updated Lambda code

Deploy the new packages (with `state_backend.py` included) to all four Lambdas in both regions.

### Step 3: Set environment variables

Add `STATE_BACKEND`, `STATE_BUCKET`, and `STATE_PREFIX` to all four Lambdas as described in Section 3.

### Step 4: Initialize state

Invoke the orchestrator in the primary region with `{"reset_state": true}` to create the initial state file in S3. Verify the file exists in the primary bucket at `failover-state/REGION_STATE.json`.

### Step 5: Verify replication

Wait 30 seconds, then verify the state file exists in the secondary bucket. If it does not appear within 2 minutes, check the replication rule configuration.

### Step 6: Re-enable automated invocations

Re-enable the EventBridge schedule rule in both regions.

---

## 6. Migration Procedure: S3 Back to DynamoDB

### Step 1: Pause automated invocations

Disable the EventBridge schedule rule in both regions.

### Step 2: Update environment variables

Remove `STATE_BACKEND`, `STATE_BUCKET`, and `STATE_PREFIX` from all four Lambdas. The backend defaults to `dynamodb` and the existing `STATE_TABLE` variable takes effect.

### Step 3: Initialize DynamoDB state

Invoke the orchestrator in the primary region with `{"reset_state": true}` to write fresh state to DynamoDB.

### Step 4: Re-enable automated invocations

Re-enable the EventBridge schedule rule in both regions.

### Step 5: (Optional) Remove S3 resources

Remove the S3 buckets, replication IAM roles, and the `s3-state-backend-access` policy from the Lambda roles. The Lambda code does not need to be redeployed — `state_backend.py` is inert when `STATE_BACKEND` is absent or set to `dynamodb`.

---

## 7. Monitoring

### Replication lag

With Replication Time Control enabled, these CloudWatch metrics are available in the **source bucket's region** under the `AWS/S3` namespace:

| Metric | Description |
|--------|-------------|
| `ReplicationLatency` | Seconds for the most recent object to replicate |
| `OperationsPendingReplication` | Number of objects waiting to replicate |

### State file health check

Verify the state file exists and is valid JSON in both regions. The file path is:

```
s3://<BUCKET>/failover-state/REGION_STATE.json
```

Expected fields: `active_region`, `state`, `latch_engaged`, `consecutive_failures`, `last_failover_ts`, `aurora_promotion_pending`.

### Dashboard and CLI

The `failover_cli.py` and `failover_dashboard_local.py` tools support the S3 backend. Set the environment variables before running:

```
STATE_BACKEND=s3
STATE_BUCKET=<regional-bucket-name>
```

---

## 8. Troubleshooting

### Lambda returns `AccessDenied` on S3 operations

The Lambda role is missing S3 permissions. See Section 2. After adding the policy, the Lambda must cold-start to pick up the new credentials — update any Lambda configuration setting (e.g., description) to force this.

### State not replicating to secondary region

1. Verify the replication rule exists on the source bucket
2. Verify the object key starts with `failover-state/` (the replication filter prefix)
3. Verify versioning is enabled on both buckets
4. Check `ReplicationLatency` and `OperationsPendingReplication` metrics in CloudWatch

### First invocation fails with `AccessDenied` instead of creating state

The Lambda role needs `s3:ListBucket` on the bucket resource (not just the object path). Without it, S3 returns `403` instead of `404` for missing objects, and the Lambda cannot detect that the state file needs to be created.

### State diverges between regions

If both buckets have different state after a network partition, the most recent write wins when CRR catches up. To force convergence, invoke the orchestrator in the primary region with `{"reset_state": true}` and wait for replication.
