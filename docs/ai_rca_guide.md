# AI Root Cause Analysis (RCA) Guide

When a failover triggers, the orchestrator can automatically generate an AI-powered root cause analysis using Claude. The RCA is appended to the SNS failover notification, giving operators immediate incident context without manual log diving.

## How It Works

```
Failover Triggered
  → Collector gathers context (ECS, Aurora, ALB, CloudWatch Logs)
  → Claude API analyzes the incident
  → Formatted RCA appended to SNS notification
  → Operator receives actionable analysis within seconds
```

The RCA runs **after** the failover is claimed but **before** DNS is moved. It is completely non-blocking — if the AI call fails or times out, the failover proceeds normally and the SNS notification is sent without the RCA section.

## Setup

### 1. Store Anthropic API Key in Secrets Manager

```bash
aws secretsmanager create-secret \
  --name failover-orchestrator/anthropic-api-key \
  --secret-string "sk-ant-your-key-here" \
  --region us-east-1

# Replicate to secondary region
aws secretsmanager create-secret \
  --name failover-orchestrator/anthropic-api-key \
  --secret-string "sk-ant-your-key-here" \
  --region us-east-2
```

### 2. Add IAM Permissions

The Lambda execution role needs permission to read the secret:

```json
{
  "Effect": "Allow",
  "Action": "secretsmanager:GetSecretValue",
  "Resource": "arn:aws:secretsmanager:*:ACCOUNT_ID:secret:failover-orchestrator/anthropic-api-key-*"
}
```

### 3. Set Environment Variables

Add these to the Lambda configuration in **both regions**:

| Variable | Value | Required |
|----------|-------|----------|
| `AI_RCA_ENABLED` | `true` | Yes |
| `AI_RCA_MODEL` | `claude-haiku-4-5-20251001` | No (this is the default) |
| `APP_LOG_GROUP` | `/ecs/your-app-name` | No (enhances analysis) |
| `ALB_FULL_ARN` | `arn:aws:elasticloadbalancing:...` | No (enhances analysis) |

Optional tuning variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `AI_RCA_MAX_TOKENS` | `1024` | Max response tokens from Claude |
| `AI_RCA_TIMEOUT_SECONDS` | `15` | API call timeout — keeps failover fast |
| `AI_RCA_LOG_WINDOW_MINUTES` | `10` | How far back to collect logs/events |
| `AI_RCA_MAX_LOG_LINES` | `200` | Max log lines sent to Claude (controls token usage) |
| `ANTHROPIC_API_KEY_SECRET_NAME` | `failover-orchestrator/anthropic-api-key` | Custom Secrets Manager key name |

### 4. Include AI Module in Lambda Zip

```bash
zip failover_orchestrator_v3.zip \
  failover_orchestrator_v3.py \
  state_backend.py \
  ai/__init__.py \
  ai/config.py \
  ai/collector.py \
  ai/rca_analyzer.py
```

## RCA Output Format

The RCA is appended to the standard failover SNS notification:

```
FAILOVER: DNS moved to us-east-2 - PROMOTE AURORA NOW

Automated DNS failover triggered.
From: us-east-1
To: us-east-2
...

============================================================
AI ROOT CAUSE ANALYSIS
============================================================

## Timeline
- 15:26 ECS tasks began draining connections
- 15:27 ALB healthy host count dropped to 0
- 15:28 HTTP health check returned 503
- 15:29 Consecutive failure threshold (3) reached

## Root Cause
ECS deployment triggered a rolling update that deregistered all existing
tasks before new tasks became healthy. The deployment configuration
(minimumHealthyPercent=0) allowed all tasks to drain simultaneously.

## Affected Components
- HTTP health check: FAILED (503 Service Unavailable)
- ALB: FAILED (0 healthy hosts, minimum required: 1)
- ECS: FAILED (0 running tasks, 4 desired)

## Impact
All traffic to us-east-1 returned 503 errors for approximately 3 minutes
before failover was triggered.

## Recommended Actions
1. Verify the ECS deployment in us-east-2 is stable
2. Review deployment configuration — set minimumHealthyPercent >= 50
3. Monitor Aurora promotion status before initiating failback

------------------------------------------------------------
Analysis model: claude-haiku-4-5-20251001
Region: us-east-1
Log window: 10 minutes
Note: This is an AI-generated analysis. Verify findings before acting.
```

## Data Collected

The collector gathers the following at failover time:

| Source | Data | Used For |
|--------|------|----------|
| Health Signals | All 5 signal results from orchestrator evaluation | Primary analysis input |
| ECS | Service status, running/desired count, recent events, deployments | Deployment and task failure analysis |
| Aurora | Cluster status, writer/reader members, recent RDS events | Database-related failures |
| ALB | Target group health, individual target states | Network and instance analysis |
| CloudWatch Logs | Recent ERROR/WARN/Exception/FATAL lines | Application-level root cause |

If any source is unavailable (e.g., during a regional outage), the collector returns a partial result. The AI analysis adapts to whatever context is available.

## Cost Estimate

| Model | Input (~) | Output (~) | Cost per Failover |
|-------|-----------|------------|-------------------|
| claude-haiku-4-5-20251001 | ~2,000 tokens | ~500 tokens | ~$0.003 |
| claude-sonnet-4-5-20250514 | ~2,000 tokens | ~500 tokens | ~$0.012 |

At the expected failover frequency (rare — ideally never), AI costs are negligible.

## Model Selection

- **Haiku** (default): Fast, cheap, good for straightforward incidents. Recommended for most deployments.
- **Sonnet**: More detailed analysis, better at correlating subtle signals. Use for critical production environments where the extra cost is justified.

Change the model via the `AI_RCA_MODEL` environment variable.

## Testing

```bash
# Unit tests (no AWS or API key required)
python3 -m pytest tests/test_rca.py -v

# Integration test with real Claude API
AI_RCA_INTEGRATION_TEST=1 ANTHROPIC_API_KEY=sk-ant-your-key \
  python3 -m pytest tests/test_rca.py -v -k "RCAIntegration"
```

## Troubleshooting

| Issue | Cause | Fix |
|-------|-------|-----|
| No RCA in notification | `AI_RCA_ENABLED` not set to `true` | Set env var in Lambda config |
| `[RCA] Analysis unavailable: ClientError` | Missing Secrets Manager permissions | Add `secretsmanager:GetSecretValue` to Lambda role |
| `[RCA] Analysis unavailable: URLError` | Lambda can't reach `api.anthropic.com` | Ensure Lambda has internet access (NAT Gateway if VPC-attached) |
| RCA is slow / delaying failover | API latency > timeout | Reduce `AI_RCA_TIMEOUT_SECONDS` (default 15s) |
| Token limit exceeded | Too many log lines | Reduce `AI_RCA_MAX_LOG_LINES` |
