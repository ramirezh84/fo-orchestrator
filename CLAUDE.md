# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Multi-region failover orchestrator for AWS infrastructure (Deposits 2.0 platform). Lambda-based system that manages automated failover and manual failback between us-west-1 (primary) and us-west-2 (secondary), with a "latch" mechanism to prevent Route 53 from flip-flopping traffic.

**Core problem solved:** Route 53 failover records are stateless and can flip traffic back and forth rapidly. This system adds a decision layer with consecutive-failure thresholds, cooldowns, and an explicit latch that keeps the old region marked unhealthy until an operator manually runs failback.

## Development Process

All changes to this codebase MUST follow these practices:

### Git Workflow
- **`main`** branch is stable, tagged releases only (v1.0, v1.1, etc.)
- **`feature/*`** branches for all new work, merged via PR
- Every PR must reference a GitHub Issue
- Never commit directly to `main` without a PR (hotfixes are the only exception)

### Project Tracking
- **GitHub Project board:** [FO Orchestrator - AI Enhancements](https://github.com/users/ramirezh84/projects/1)
- Work is organized into **Epics** (labeled `epic`) with **Sub-tasks** (individual issues)
- Every piece of work needs a GitHub Issue before implementation starts
- Issues must be added to the project board
- Close issues via PR merge (use `Closes #N` in PR body)

### Testing Requirements
- **Run the full test suite before every commit:** `python3 -m pytest tests/ -v`
- **All tests must pass** before pushing or creating a PR
- New features require new tests — no untested code reaches `main`
- Regression test suite (180 tests) covers:
  - `tests/test_orchestrator.py` — Core orchestrator logic (52 tests)
  - `tests/test_rca.py` — AI RCA module (32 tests)
  - `tests/test_state_backend.py` — State backend (13 tests)

### Code Review Checklist
Before merging any PR:
1. All tests pass (`python3 -m pytest tests/ -v`)
2. No regressions in existing functionality
3. New code has test coverage
4. CLAUDE.md updated if architecture/config/components change
5. GitHub Issue linked and will auto-close

### Deployment
- Package Lambda zips with all required modules
- Deploy to BOTH regions (us-west-1 and us-west-2)
- Verify health after deployment (check orchestrator logs)

## Commands

No build system. Deployment is manual via AWS CLI:

```bash
# Package and deploy orchestrator Lambda (include state_backend.py + ai module)
zip failover_orchestrator_v3.zip failover_orchestrator_v3.py state_backend.py ai/__init__.py ai/config.py ai/llm_client.py ai/collector.py ai/rca_analyzer.py ai/stability_collector.py ai/aurora_advisor.py
aws lambda update-function-code \
  --function-name failover-orchestrator-prod \
  --zip-file fileb://failover_orchestrator_v3.zip \
  --region us-west-1
# Repeat for us-west-2

# Package and deploy failback Lambda (include state_backend.py)
zip manual_failback_v2.zip manual_failback_v2.py state_backend.py ai/__init__.py ai/config.py ai/llm_client.py ai/stability_collector.py ai/failback_readiness.py
aws lambda update-function-code \
  --function-name failover-manual-failback-prod \
  --zip-file fileb://manual_failback_v2.zip \
  --region us-west-1
# Repeat for us-west-2

# Run the full test suite (REQUIRED before every commit)
python3 -m pytest tests/ -v

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
| `ai/config.py` | AI configuration: provider, model, timeouts, feature toggles. |
| `ai/collector.py` | Collects incident context from ECS, Aurora, ALB, CloudWatch at failover time. |
| `ai/llm_client.py` | Shared LLM client: API key retrieval, Claude/Gemini HTTP calls, unified `call_llm()`. |
| `ai/rca_analyzer.py` | Multi-provider LLM integration (Claude/Gemini) for root cause analysis. |
| `ai/stability_collector.py` | Time-series stability data: Aurora replication lag, ECS task trends, ALB error rates. |
| `ai/failback_readiness.py` | LLM-powered GO/NO-GO/CAUTION assessment before failback. |
| `ai/aurora_advisor.py` | Progressive Aurora promotion advisor: advisory → guided → autonomous modes. |
| `tools/setup_s3_state_backend.py` | Infrastructure setup script for S3 CRR backend. |
| `tools/generate_dashboard.py` | CloudWatch dashboard generator. |
| `tests/test_orchestrator.py` | Regression tests for core orchestrator logic (52 tests). |
| `tests/test_state_backend.py` | Unit + integration + CRR replication tests for state backends. |
| `tests/test_e2e_s3_backend.py` | End-to-end scenario tests for S3 backend. |
| `tests/test_rca.py` | Unit + integration tests for AI RCA module (32 tests). |
| `tests/test_llm_client.py` | Unit tests for shared LLM client (14 tests). |
| `tests/test_stability_collector.py` | Unit tests for stability data collection (19 tests). |
| `tests/test_failback_readiness.py` | Unit tests for failback readiness assessment (18 tests). |
| `tests/test_aurora_advisor.py` | Unit tests for Aurora promotion advisor, all phases (32 tests). |
| `cfn/network.yaml` | CloudFormation: VPC, subnets, NAT Gateway. |
| `cfn/app.yaml` | CloudFormation: ECS, ALB, security groups, VPC endpoints. |
| `cfn/aurora.yaml` | CloudFormation: Aurora Global Database cluster. |
| `cfn/failover.yaml` | CloudFormation: Orchestrator Lambda, EventBridge, CloudWatch alarms, Route 53 health checks. |

### Deployed Infrastructure

| Stack | us-west-1 (primary) | us-west-2 (secondary) |
|-------|--------------------|-----------------------|
| `fo-demo-network` | VPC, subnets, NAT | VPC, subnets, NAT |
| `fo-demo-app` | ECS cluster, ALB, SGs | ECS cluster, ALB, SGs |
| `fo-demo-failover` | Orchestrator + Failback Lambdas | Orchestrator + Failback Lambdas |
| DynamoDB | `fo-demo-state` (Global Table) | `fo-demo-state` (replica) |
| Aurora | `fo-demo-aurora-w1` (writer) | `fo-demo-aurora-w2` (reader) |

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

On failover (if AI_RCA_ENABLED=true):
  Collector gathers ECS/Aurora/ALB/CloudWatch context → LLM analyzes → RCA appended to SNS notification

On failover (if AI_AURORA_ADVISOR_MODE != disabled):
  Stability collector gathers Aurora replication lag/topology/events → LLM advises promotion method →
  advisory: recommendation in SNS | guided: auto-execute if confident switchover | autonomous: auto-execute with guardrails

On failback (if AI_FAILBACK_READINESS_ENABLED=true):
  Stability collector gathers trends → LLM produces GO/NO-GO/CAUTION verdict →
  NO_GO blocks failback | CAUTION proceeds with warnings | GO proceeds normally
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
| `PRIMARY_REGION` / `SECONDARY_REGION` | us-west-1 / us-west-2 | Region roles |
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
| `AI_RCA_ENABLED` | false | Enable AI-powered root cause analysis on failover |
| `AI_RCA_PROVIDER` | claude | LLM provider: `claude` or `gemini` |
| `AI_RCA_MODEL` | (auto per provider) | Claude: `claude-haiku-4-5-20251001`, Gemini: `gemini-2.5-flash` |
| `AI_RCA_MAX_TOKENS` | 4096 | Max response tokens for RCA |
| `AI_RCA_TIMEOUT_SECONDS` | 15 | API call timeout (non-blocking) |
| `APP_LOG_GROUP` | (empty) | CloudWatch log group for app logs (enhances RCA) |
| `ALB_FULL_ARN` | (empty) | Full ALB ARN for target health collection (enhances RCA) |
| `ANTHROPIC_API_KEY_SECRET_NAME` | failover-orchestrator/anthropic-api-key | Secrets Manager key for Claude |
| `GEMINI_API_KEY_SECRET_NAME` | failover-orchestrator/gemini-api-key | Secrets Manager key for Gemini |
| `AI_FAILBACK_READINESS_ENABLED` | false | Enable AI failback readiness assessment (GO/NO-GO) |
| `AI_FAILBACK_STABILITY_WINDOW_MINUTES` | 15 | How far back to look at stability trends for failback |
| `AI_AURORA_ADVISOR_MODE` | disabled | Aurora advisor: `disabled`, `advisory`, `guided`, `autonomous` |
| `AI_AURORA_ADVISOR_CONFIDENCE_THRESHOLD` | 90 | Min LLM confidence for guided auto-execute |
| `AI_AURORA_ADVISOR_MAX_LAG_MS` | 100 | Hard guardrail: max acceptable replication lag (ms) |
| `AI_AURORA_STABILITY_WINDOW_MINUTES` | 10 | How far back to look at Aurora stability metrics |

## Key Design Decisions

- **Automated failover, manual failback**: DNS failover fires automatically when thresholds are met, but returning to primary always requires operator action. This is intentional to prevent autonomous flip-flopping.
- **Aurora promotion is manual by default** (`AURORA_AUTO_PROMOTE=false`): Lambda sends CLI commands via SNS; operator runs `aws rds switchover-global-cluster`. Auto-promotion is available but disabled to prevent accidental data loss on unplanned failovers.
- **No Step Functions**: Entire orchestration runs inside a single Lambda on a 1-minute EventBridge schedule. Each invocation is stateless; all state is in the configured backend (DynamoDB or S3).
- **Passive region publishes its own health metric**: Secondary region demonstrates readiness to receive traffic, which is also used during failback validation.
- **Failback Lambda invoked in target region**: Must be invoked in us-west-1 when failing back to us-west-1, so it can verify Aurora writer status locally.
- **AI RCA is non-blocking**: If the LLM API call fails or times out, failover proceeds normally. RCA is fire-and-forget.
- **Multi-provider LLM support**: Claude and Gemini are both supported via `AI_RCA_PROVIDER`. Uses urllib (no SDK dependency) for both providers.
- **Progressive Aurora automation**: Aurora promotion advisor has three modes (advisory → guided → autonomous) so operators can build trust incrementally. Hard guardrails (replication lag, sync status, cluster/instance state) run before the LLM call and cannot be overridden — deterministic safety beats AI confidence.
- **AI failback readiness is blocking**: Unlike RCA (fire-and-forget), the failback readiness assessment can block a failback with a NO_GO verdict. Operators can override with `skip_readiness_check=true`.

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
zip manual_failback_v2.zip manual_failback_v2.py state_backend.py ai/__init__.py ai/config.py ai/llm_client.py ai/stability_collector.py ai/failback_readiness.py
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
# Run FULL test suite (REQUIRED before every commit)
python3 -m pytest tests/ -v

# Orchestrator regression tests only
python3 -m pytest tests/test_orchestrator.py -v

# State backend unit tests (no AWS required)
python3 -m pytest tests/test_state_backend.py -v -k "not Integration"

# S3 integration tests (requires AWS credentials)
INTEGRATION_TEST=1 python3 -m pytest tests/test_state_backend.py -v -k "S3Integration"

# CRR replication test (requires pre-provisioned buckets)
CRR_TEST=1 \
  CRR_PRIMARY_BUCKET=<primary-bucket> \
  CRR_SECONDARY_BUCKET=<secondary-bucket> \
  python3 -m pytest tests/test_state_backend.py -v -k "CRR"

# AI module unit tests (no AWS or API key required)
python3 -m pytest tests/test_rca.py tests/test_llm_client.py tests/test_stability_collector.py tests/test_failback_readiness.py tests/test_aurora_advisor.py -v

# AI RCA integration test (requires API key)
AI_RCA_INTEGRATION_TEST=1 ANTHROPIC_API_KEY=sk-ant-your-key \
  python3 -m pytest tests/test_rca.py -v -k "RCAIntegration"
```
