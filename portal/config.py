"""SentinelFO Portal configuration — version definitions, feature matrix, AWS resource names."""

# ── AWS Resource Names ─────────���──────────────────────────────────────────────

PRIMARY_REGION = "us-west-1"
SECONDARY_REGION = "us-west-2"
BOTH_REGIONS = [PRIMARY_REGION, SECONDARY_REGION]
ACCOUNT_ID = "597088043823"

ORCHESTRATOR_LAMBDA = "fo-demo-orchestrator"
FAILBACK_LAMBDA = "fo-demo-failback"
ECS_CLUSTER = "fo-demo-cluster"
ECS_SERVICE = "fo-demo-app-svc"
STATE_TABLE = "fo-demo-state"
SNS_TOPIC_ARN = "arn:aws:sns:us-west-1:{}:fo-demo-alerts".format(ACCOUNT_ID)
AURORA_GLOBAL_CLUSTER = "fo-demo-aurora-global"
AURORA_CLUSTER_W1 = "fo-demo-aurora-w1"
AURORA_CLUSTER_W2 = "fo-demo-aurora-w2"
AURORA_INSTANCE_W1 = "fo-demo-aurora-w1-inst"
AURORA_INSTANCE_W2 = "fo-demo-aurora-w2-inst"
EVENTBRIDGE_RULE = "fo-demo-orchestrator-schedule"
S3_STATE_BUCKET_W1 = "fo-demo-state-us-west-1-{}".format(ACCOUNT_ID)
S3_STATE_BUCKET_W2 = "fo-demo-state-us-west-2-{}".format(ACCOUNT_ID)

REGION_SUFFIX = {PRIMARY_REGION: "w1", SECONDARY_REGION: "w2"}

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
