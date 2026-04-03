# Multi-Region Failover Orchestrator

Automated multi-region failover for AWS infrastructure. Lambda-based system that evaluates application health across five signals, manages DNS failover via Route 53, and coordinates Aurora database promotion — with anti-flip-flop safeguards that prevent traffic from bouncing between regions.

## The Problem

Route 53 failover records are stateless. If Region A goes unhealthy and traffic moves to Region B, Route 53 will move it right back the moment Region A looks healthy again — even if the recovery is momentary. This causes traffic to flip-flop during partial outages, degrading the experience for everyone.

## The Solution

An EventBridge-triggered Lambda runs every 60 seconds in both regions. It evaluates five health signals using quorum logic, publishes a synthetic CloudWatch metric, and manages a state machine with three anti-flip-flop mechanisms:

1. **Consecutive failure threshold** — sustained failure required, not a single blip
2. **Cooldown window** — one failover per window (default 30 min)
3. **Latch** — after failover, the old region stays marked unhealthy until an operator explicitly runs failback

## Three Supported Architectures

| Architecture | How it works | When to use |
|---|---|---|
| **Active/passive failover** | One region serves traffic. Auto-failover on health failure, manual failback. | Standard DR setup with warm standby |
| **Zero-container secondary** | Secondary starts with 0 ECS tasks. Auto Scaling spins them up during failover, back to 0 on failback. | Cost optimization — no idle compute in secondary |
| **Active/active** | Both regions serve traffic via latency-based routing. Unhealthy region is pulled from the pool automatically. Auto-recovery when healthy. | Low-latency global apps |

All three use the same Lambda code — behavior is controlled by environment variables (`ROUTING_MODE`, `PASSIVE_PUBLISH_ZERO`).

## Health Signals

Five signals evaluated with quorum logic (>=50% must fail to declare unhealthy):

| Signal | Source | Behavior |
|--------|--------|----------|
| HTTP health check | Private ALB `/actuator/health` | Any failure = immediately unhealthy (bypasses quorum) |
| ALB healthy hosts | CloudWatch `HealthyHostCount` | Must meet minimum threshold |
| ECS running tasks | ECS `DescribeServices` | Must be >= 50% of desired |
| API Gateway errors | CloudWatch `5XXError` | Must be below threshold |
| Aurora status | RDS `DescribeDBClusters` | Must be "available" |

## State Backend

Pluggable state storage — choose based on your environment:

| Backend | Replication | Set via |
|---------|-------------|---------|
| **DynamoDB Global Table** (default) | Sub-second | `STATE_BACKEND=dynamodb` |
| **S3 Cross-Region Replication** | ~23 seconds | `STATE_BACKEND=s3` |

The S3 backend is a drop-in alternative when DynamoDB Global Tables require an exception process. Both backends support conditional writes for concurrency control.

## Project Structure

```
failover_orchestrator_v3.py    Lambda: health evaluation, failover, latch, metrics
manual_failback_v2.py          Lambda: operator-triggered failback
state_backend.py               Pluggable state: DynamoDB or S3 with ETag locking
failover_cli.py                Interactive operator CLI
failover_dashboard_local.py    Local Flask dashboard

app/                           Demo container (Dockerfile, Flask app)
cfn/                           CloudFormation templates (network, app, aurora, failover)
docs/                          Guides, architecture diagrams, operational runbooks
tests/                         Unit, integration, and end-to-end test suites
tools/                         S3 setup script, CloudWatch dashboard generator
```

## Quick Start

### Deploy

```bash
# Package Lambda (include state_backend.py in both zips)
zip failover_orchestrator_v3.zip failover_orchestrator_v3.py state_backend.py
zip manual_failback_v2.zip manual_failback_v2.py state_backend.py

# Deploy to both regions
for REGION in us-east-1 us-east-2; do
  aws lambda update-function-code \
    --function-name failover-orchestrator \
    --zip-file fileb://failover_orchestrator_v3.zip \
    --region $REGION
done
```

### Configure

Set environment variables on the Lambda. At minimum:

```
SNS_TOPIC_ARN          = arn:aws:sns:<region>:<account>:<topic>
HEALTH_CHECK_URL       = http://<internal-alb-dns>
ECS_CLUSTER_NAME       = <cluster>
ECS_SERVICE_NAME       = <service>
```

See `CLAUDE.md` for the full configuration reference.

### Operate

```bash
# Monitor via CLI
python3 failover_cli.py

# Manual failover (when FAILOVER_MODE=manual)
aws lambda invoke --function-name failover-orchestrator \
  --payload '{"execute_failover": true}' response.json

# Manual failback
aws lambda invoke --function-name failover-manual-failback \
  --payload '{"target_region": "us-east-1", "aurora_confirmed": true, "operator": "your-name"}' \
  --region us-east-1 response.json

# Reset state
aws lambda invoke --function-name failover-orchestrator \
  --payload '{"reset_state": true}' response.json
```

## Documentation

| Document | Description |
|----------|-------------|
| [CLAUDE.md](CLAUDE.md) | Architecture, configuration reference, design decisions |
| [Deployment Guide](docs/deployment_and_configuration_guide.md) | Step-by-step CloudFormation deployment |
| [Operational Guide](docs/failover_orchestrator_operational_guide.md) | Monitoring, runbooks, troubleshooting |
| [Deployment Notes](docs/deployment_notes.md) | Lessons learned and common pitfalls |
| [S3 Backend Setup](docs/s3_state_backend_setup_guide.md) | S3 CRR as DynamoDB alternative |
| [DynamoDB vs S3 Comparison](docs/state_backend_comparison.md) | Trade-offs and test evidence |
| [Zero-Container Guide](docs/zero_container_secondary_guide.md) | Auto-scaling from 0 on failover |
| [Active-Active Guide](docs/active_active_guide.md) | Latency-based routing with health gating |
| [Architecture Diagram](docs/architecture_diagram_v3.md) | Mermaid flowchart of the full system |

## Testing

```bash
# Unit tests (no AWS credentials needed)
python3 -m pytest tests/test_state_backend.py -v -k "not Integration"

# S3 integration tests (requires AWS credentials)
INTEGRATION_TEST=1 python3 -m pytest tests/test_state_backend.py -v -k "S3Integration"

# End-to-end scenario tests
INTEGRATION_TEST=1 python3 -m pytest tests/test_e2e_s3_backend.py -v
```

All three architectures have been validated end-to-end against live AWS infrastructure across 18 scenarios covering: auto failover, manual failover, region outage detection, latch enforcement, cooldown, failback, auto-scaling 0-to-N, active-active removal/recovery, concurrent race conditions, and Splunk event logging.
