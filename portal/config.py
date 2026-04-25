"""Vigil Portal configuration — version definitions, feature matrix, AWS resource names."""

# ── Regions ──────────────────────────────────────────────────────────────────

PRIMARY_REGION = "us-west-1"
SECONDARY_REGION = "us-west-2"
BOTH_REGIONS = [PRIMARY_REGION, SECONDARY_REGION]
ACCOUNT_ID = "597088043823"
ECS_CLUSTER = "fo-demo-cluster"  # Shared across both stacks
SNS_TOPIC_ARN = "arn:aws:sns:us-west-1:{}:fo-demo-alerts".format(ACCOUNT_ID)
REGION_SUFFIX = {PRIMARY_REGION: "w1", SECONDARY_REGION: "w2"}

# ── Stack Definitions ────────────────────────────────────────────────────────
# Two independent stacks — each with its own Lambdas, ECS service, Aurora, etc.
# Config is baked at deploy time. No runtime switching.

STACKS = {
    "ddb": {
        "name": "DynamoDB",
        "description": "DynamoDB Global Table state backend",
        "orchestrator_lambda": "fo-demo-orchestrator",
        "failback_lambda": "fo-demo-failback",
        "ecs_service": "fo-demo-app-svc",
        "eventbridge_rule": "fo-demo-orchestrator-schedule",
        "aurora_global": "fo-demo-aurora-global",
        "aurora_cluster_w1": "fo-demo-aurora-w1",
        "aurora_cluster_w2": "fo-demo-aurora-w2",
        "aurora_instance_w1": "fo-demo-aurora-w1-inst",
        "aurora_instance_w2": "fo-demo-aurora-w2-inst",
        "cw_namespace": "Custom/FoDemo",
        "state_table": "fo-demo-state",
        "state_backend": "dynamodb",
    },
    "s3": {
        "name": "S3 CRR",
        "description": "S3 Cross-Region Replication state backend",
        "orchestrator_lambda": "fo-demo-s3-orchestrator",
        "failback_lambda": "fo-demo-s3-failback",
        "ecs_service": "fo-demo-s3-app-svc",
        "eventbridge_rule": "fo-demo-s3-orchestrator-schedule",
        "aurora_global": "fo-demo-s3-aurora-global",
        "aurora_cluster_w1": "fo-demo-s3-aurora-w1",
        "aurora_cluster_w2": "fo-demo-s3-aurora-w2",
        "aurora_instance_w1": "fo-demo-s3-aurora-w1-inst",
        "aurora_instance_w2": "fo-demo-s3-aurora-w2-inst",
        "cw_namespace": "Custom/FoDemoS3",
        "s3_bucket_w1": "fo-demo-state-us-west-1-{}".format(ACCOUNT_ID),
        "s3_bucket_w2": "fo-demo-state-us-west-2-{}".format(ACCOUNT_ID),
        "state_backend": "s3",
    },
}

# Backward compat — default stack references for existing code
ORCHESTRATOR_LAMBDA = STACKS["ddb"]["orchestrator_lambda"]
FAILBACK_LAMBDA = STACKS["ddb"]["failback_lambda"]
ECS_SERVICE = STACKS["ddb"]["ecs_service"]
STATE_TABLE = STACKS["ddb"]["state_table"]
AURORA_GLOBAL_CLUSTER = STACKS["ddb"]["aurora_global"]
AURORA_CLUSTER_W1 = STACKS["ddb"]["aurora_cluster_w1"]
AURORA_CLUSTER_W2 = STACKS["ddb"]["aurora_cluster_w2"]
AURORA_INSTANCE_W1 = STACKS["ddb"]["aurora_instance_w1"]
AURORA_INSTANCE_W2 = STACKS["ddb"]["aurora_instance_w2"]
EVENTBRIDGE_RULE = STACKS["ddb"]["eventbridge_rule"]
S3_STATE_BUCKET_W1 = STACKS["s3"]["s3_bucket_w1"]
S3_STATE_BUCKET_W2 = STACKS["s3"]["s3_bucket_w2"]

# ── Portal Auth ───────────────────────────────────────────────────────────────

PORTAL_USERNAME = "eramirez"
PORTAL_PASSWORD = "1ns2deout"
SECRET_KEY = "sentinelfo-portal-session-key-2026"

# ── Version Definitions ─────���─────────────────────────────────────────────────

# Lambda alias names can't contain dots (AWS restriction).
# Use v1-0, v1-1, v1-2 as alias names; display as v1.0, v1.1, v1.2.
VERSION_TO_ALIAS = {"v1.0": "v1-0", "v1.1": "v1-1", "v1.2": "v1-2"}

VERSIONS = {
    "v1.0": {
        "name": "v1.0 — Core Failover",
        "alias": "v1-0",
        "description": "Automated DNS failover with latch, anti-flip-flop, manual failback",
        "features": [
            "5-signal health evaluation (HTTP, ALB, ECS, API GW, Aurora)",
            "Consecutive failure threshold + cooldown window",
            "Latch mechanism prevents DNS flip-flop",
            "Manual failback with health validation",
            "DynamoDB and S3 CRR state backends",
        ],
        "env_overrides": {
            "AI_RCA_ENABLED": "false",
            "AI_FAILBACK_READINESS_ENABLED": "false",
            "AI_AURORA_ADVISOR_MODE": "disabled",
        },
        "supports_provider": False,
    },
    "v1.1": {
        "name": "v1.1 — AI Insights",
        "alias": "v1-1",
        "description": "AI root cause analysis on failover (Claude or Gemini)",
        "features": [
            "Everything in v1.0",
            "AI-powered root cause analysis at failover time",
            "Multi-provider LLM support (Claude Haiku / Gemini Flash)",
            "Non-blocking: AI failure never delays failover",
            "RCA appended to SNS notification",
        ],
        "env_overrides": {
            "AI_RCA_ENABLED": "true",
            "AI_FAILBACK_READINESS_ENABLED": "false",
            "AI_AURORA_ADVISOR_MODE": "disabled",
        },
        "supports_provider": True,
    },
    "v1.2": {
        "name": "v1.2 — AI Safety",
        "alias": "v1-2",
        "description": "Failback readiness assessment + progressive Aurora promotion advisor",
        "features": [
            "Everything in v1.1",
            "AI failback readiness: GO/NO-GO/CAUTION before failback",
            "Aurora promotion advisor: advisory mode (LLM recommends method)",
            "Stability collector: time-series trends for Aurora, ECS, ALB",
            "Hard guardrails: replication lag, sync status, instance state",
        ],
        "env_overrides": {
            "AI_RCA_ENABLED": "true",
            "AI_FAILBACK_READINESS_ENABLED": "true",
            "AI_AURORA_ADVISOR_MODE": "advisory",
        },
        "supports_provider": True,
    },
}

# ── Architecture Definitions ──���───────────────────────────────────────────────

ARCHITECTURES = {
    "active-passive": {
        "name": "Active/Passive",
        "description": "Primary serves traffic, secondary on warm standby",
        "env_overrides": {
            "ROUTING_MODE": "failover",
            "PASSIVE_PUBLISH_ZERO": "false",
        },
    },
    "zero-container": {
        "name": "Zero-Container Secondary",
        "description": "Secondary has 0 ECS tasks, auto-scaled on failover",
        "env_overrides": {
            "ROUTING_MODE": "failover",
            "PASSIVE_PUBLISH_ZERO": "true",
        },
    },
    "active-active": {
        "name": "Active/Active",
        "description": "Both regions serve traffic, auto-recovery without latch",
        "env_overrides": {
            "ROUTING_MODE": "active-active",
            "PASSIVE_PUBLISH_ZERO": "false",
        },
    },
}

# ── Backend Definitions ───────��───────────────────────────────────────────────

BACKENDS = {
    "dynamodb": {
        "name": "DynamoDB Global Table",
        "description": "Sub-second replication, strong consistency",
        "env_overrides": {
            "STATE_BACKEND": "dynamodb",
            "STATE_TABLE": STATE_TABLE,
        },
    },
    "s3": {
        "name": "S3 Cross-Region Replication",
        "description": "15-60s replication, ETag-based locking, no DynamoDB needed",
        "env_overrides": {
            "STATE_BACKEND": "s3",
            "STATE_PREFIX": "failover-state/",
        },
    },
}

PROVIDERS = {
    "claude": {"name": "Claude (Haiku)", "env_key": "AI_RCA_PROVIDER", "env_value": "claude"},
    "gemini": {"name": "Gemini (Flash)", "env_key": "AI_RCA_PROVIDER", "env_value": "gemini"},
}
