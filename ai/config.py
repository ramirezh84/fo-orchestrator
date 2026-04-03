"""Configuration for AI-powered RCA analysis."""

import os

# Feature toggle — must be explicitly enabled
AI_RCA_ENABLED = os.environ.get("AI_RCA_ENABLED", "false").lower() == "true"

# Anthropic API key — retrieved from environment (set via Secrets Manager in Lambda)
ANTHROPIC_API_KEY_SECRET_NAME = os.environ.get(
    "ANTHROPIC_API_KEY_SECRET_NAME", "failover-orchestrator/anthropic-api-key"
)

# Model selection: haiku for cost-sensitive, sonnet for detailed analysis
AI_RCA_MODEL = os.environ.get("AI_RCA_MODEL", "claude-haiku-4-5-20251001")

# Max tokens for the RCA response
AI_RCA_MAX_TOKENS = int(os.environ.get("AI_RCA_MAX_TOKENS", "1024"))

# Timeout for the Claude API call (seconds). RCA must not delay failover.
AI_RCA_TIMEOUT_SECONDS = int(os.environ.get("AI_RCA_TIMEOUT_SECONDS", "15"))

# How far back to look when collecting logs/events (minutes)
AI_RCA_LOG_WINDOW_MINUTES = int(os.environ.get("AI_RCA_LOG_WINDOW_MINUTES", "10"))

# Maximum log lines to include in the prompt (controls token usage)
AI_RCA_MAX_LOG_LINES = int(os.environ.get("AI_RCA_MAX_LOG_LINES", "200"))
