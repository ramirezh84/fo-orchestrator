# SentinelFO — AI-Enhanced Failover Orchestration

Automated multi-region failover for AWS infrastructure with progressive AI intelligence. Lambda-based system that evaluates health, manages DNS failover via Route 53, coordinates Aurora database promotion, and uses LLM analysis to provide root cause analysis, failback readiness assessment, and Aurora promotion recommendations.

## Version Roadmap

| Version | Name | What it adds |
|---|---|---|
| **v1.0** | Core Failover | 5-signal health evaluation, DNS failover with latch, anti-flip-flop, manual failback |
| **v1.1** | AI Insights | + AI root cause analysis at failover time (Claude/Gemini) |
| **v1.2** | AI Safety | + Failback readiness (GO/NO-GO), Aurora promotion advisor (advisory/guided/autonomous) |
| **v2.0** | Platform | Shared infra, control portal, CI/CD, Lambda versioning, regression testing |
| v2.1+ | Predictive | Early warning, smart alarms, auto-heal, full automation (backlog) |

## Three Supported Architectures

| Architecture | How it works | When to use |
|---|---|---|
| **Active/Passive** | One region serves traffic. Auto-failover, manual failback. | Standard DR with warm standby |
| **Zero-Container Secondary** | Secondary has 0 ECS tasks, auto-scaled on failover. | Cost optimization |
| **Active/Active** | Both regions serve traffic. Unhealthy region pulled automatically. | Low-latency global apps |

## Two State Backends

| Backend | Replication | Use when |
|---|---|---|
| **DynamoDB Global Table** | Sub-second | Default choice |
| **S3 Cross-Region Replication** | ~15-60s | When DynamoDB Global Tables require exception process |

## Control Portal

```bash
pip install flask boto3
PYTHONPATH=. python3 portal/app.py
# Open http://localhost:5001
```

The portal lets you:
- Select version, architecture, backend, and LLM provider
- Start/stop test environments with one click
- Toggle Aurora instances on/off (the expensive part)
- Trigger failovers and run failback
- See live status of all components
- Prevents concurrent tests via locking

## Quick Start

```bash
# Run tests (180 unit tests, no AWS needed)
python3 -m pytest tests/ -v

# Publish a new Lambda version
python3 tools/publish_version.py --alias v1-2

# Switch the active alias
python3 tools/publish_version.py --alias active --copy-from v1-2
```

## Project Structure

```
failover_orchestrator_v3.py    Lambda: health evaluation, failover, latch, metrics
manual_failback_v2.py          Lambda: operator-triggered failback
state_backend.py               Pluggable state: DynamoDB or S3
ai/                            AI modules: RCA, stability collector, failback readiness, aurora advisor
portal/                        Control portal (Flask app)
tools/                         Publish versions, setup scripts
cfn/                           CloudFormation templates
tests/                         180 unit + integration tests
docs/                          Guides, architecture diagrams, runbooks
.github/                       CI/CD workflows, issue/PR templates
```

## Documentation

| Document | Description |
|---|---|
| [CLAUDE.md](CLAUDE.md) | Architecture, configuration, design decisions |
| [Operational Guide](docs/failover_orchestrator_operational_guide.md) | Monitoring, runbooks, troubleshooting |
| [AI RCA Guide](docs/ai_rca_guide.md) | AI root cause analysis setup |
| [S3 Backend Setup](docs/s3_state_backend_setup_guide.md) | S3 CRR alternative |
| [Architecture Diagram](docs/architecture_diagram_v3.md) | System flowchart |

## CI/CD

- **PR validation**: `python3 -m pytest tests/ -v` runs on every PR via GitHub Actions
- **Release**: Tag with `v*` to publish Lambda version and update alias
- **Branch protection**: Tests must pass, PR required for `main`
