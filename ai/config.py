"""Configuration for AI-powered RCA analysis."""

import os

# Feature toggle — must be explicitly enabled
AI_RCA_ENABLED = os.environ.get("AI_RCA_ENABLED", "false").lower() == "true"

# LLM provider: "claude" or "gemini"
AI_RCA_PROVIDER = os.environ.get("AI_RCA_PROVIDER", "claude").lower()

# API key secret names in Secrets Manager
ANTHROPIC_API_KEY_SECRET_NAME = os.environ.get(
    "ANTHROPIC_API_KEY_SECRET_NAME", "failover-orchestrator/anthropic-api-key"
)
GEMINI_API_KEY_SECRET_NAME = os.environ.get(
    "GEMINI_API_KEY_SECRET_NAME", "failover-orchestrator/gemini-api-key"
)

# Model defaults per provider
_DEFAULT_MODELS = {
    "claude": "claude-haiku-4-5-20251001",
    "gemini": "gemini-2.5-flash",
}
AI_RCA_MODEL = os.environ.get("AI_RCA_MODEL", _DEFAULT_MODELS.get(AI_RCA_PROVIDER, "claude-haiku-4-5-20251001"))

# Max tokens for the RCA response
AI_RCA_MAX_TOKENS = int(os.environ.get("AI_RCA_MAX_TOKENS", "4096"))

# Timeout for the API call (seconds). RCA must not delay failover.
AI_RCA_TIMEOUT_SECONDS = int(os.environ.get("AI_RCA_TIMEOUT_SECONDS", "15"))

# How far back to look when collecting logs/events (minutes)
AI_RCA_LOG_WINDOW_MINUTES = int(os.environ.get("AI_RCA_LOG_WINDOW_MINUTES", "10"))

# Maximum log lines to include in the prompt (controls token usage)
AI_RCA_MAX_LOG_LINES = int(os.environ.get("AI_RCA_MAX_LOG_LINES", "200"))
